package viperblock

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// truncatingBackend wraps a real backend and shortens the answer to one
// specific ranged chunk read, reproducing what an S3-style backend does when a
// range starts inside an object but runs past its end: the server clamps the
// range and answers with a short 206 whose Content-Length matches the short
// body, so nothing in the HTTP stack reports an error.
//
// Verified against a live predastore: requesting 1024 bytes past the end of a
// chunk returns exactly the available bytes with err == nil. (A range whose
// START is past the end is a loud 416 instead, which needs no defending.)
type truncatingBackend struct {
	types.Backend
	trimChunk uint64
	trimBytes int
	fired     bool
}

func (b *truncatingBackend) ReadCtx(ctx context.Context, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	data, err := b.Backend.ReadCtx(ctx, fileType, objectID, offset, length)
	if err != nil {
		return nil, err
	}
	if fileType == types.FileTypeChunk && objectID == b.trimChunk && length > 0 &&
		len(data) > b.trimBytes && int(length) == len(data) {
		b.fired = true
		return data[:len(data)-b.trimBytes], nil
	}
	return data, nil
}

func (b *truncatingBackend) Read(fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	return b.ReadCtx(context.Background(), fileType, objectID, offset, length)
}

// TestShortBackendRead_IsRefusedNotZeroFilled pins that a silently short
// backend read is surfaced as an error rather than copied into a
// zero-initialised buffer and handed back (and cached) as real data.
//
// Before the length check this returned success with the trailing bytes of the
// run replaced by zeros — the exact shape of the corruption reported in
// siv-482: a 4096-byte block whose trailing 1024 bytes are zeroed, with no
// error surfaced anywhere. The encrypted path was already protected by
// openChunkRun's ciphertext-length check; only cleartext volumes were exposed.
func TestShortBackendRead_IsRefusedNotZeroFilled(t *testing.T) {
	root := t.TempDir()
	const vol = "vol-short-read"
	const volSize = 8 * 1024 * 1024

	backendConfig := file.FileConfig{VolumeName: vol, VolumeSize: volSize, BaseDir: root}
	vbconfig := VB{
		VolumeName:          vol,
		VolumeSize:          volSize,
		BaseDir:             fmt.Sprintf("%s/viperblock", root),
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		Cache:               Cache{Config: CacheConfig{Size: 0}},
	}

	vb, err := New(&vbconfig, FileBackend, backendConfig)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	bs := int(vb.BlockSize)
	const nBlocks = 8
	payload := make([]byte, nBlocks*bs)
	rnd := rand.New(rand.NewSource(4242)) //nolint:gosec // G404: deterministic test fixture
	_, err = rnd.Read(payload)
	require.NoError(t, err)

	require.NoError(t, vb.WriteAt(0, append([]byte(nil), payload...)))
	require.NoError(t, vb.DrainToBackend())

	objectID, _, _, err := vb.LookupBlockToObject(0)
	require.NoError(t, err)

	// Swap in the truncating backend and drop every in-memory copy so the read
	// has to come from the backend.
	trunc := &truncatingBackend{Backend: vb.Backend, trimChunk: objectID, trimBytes: 1024}
	vb.Backend = trunc
	vb.BlockStore.Clear()
	vb.Writes.mu.Lock()
	vb.Writes.Blocks = nil
	vb.Writes.mu.Unlock()
	vb.PendingBackendWrites.mu.Lock()
	vb.PendingBackendWrites.Blocks = nil
	vb.PendingBackendWrites.mu.Unlock()
	require.NoError(t, vb.LoadLiveCheckpoint())

	got, readErr := vb.ReadAt(0, uint64(nBlocks*bs))
	require.True(t, trunc.fired, "test did not actually inject a short read")

	if readErr == nil {
		lastTail := got[nBlocks*bs-1024:]
		allZero := true
		for _, b := range lastTail {
			if b != 0 {
				allZero = false
				break
			}
		}
		t.Fatalf("a short backend read was accepted silently: read succeeded, "+
			"trailing 1024 bytes all-zero=%v, data correct=%v",
			allZero, bytes.Equal(payload, got))
	}

	require.ErrorIs(t, readErr, types.ErrShortRead,
		"a short backend read must surface as ErrShortRead, not be zero-filled")
}
