package viperblock

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// readCountingBackend wraps a real backend and counts ranged chunk reads, so a
// test can assert that a read was answered from the plaintext cache rather than from
// storage. Only chunk reads are counted; state and checkpoint traffic is not
// part of the guest read path.
type readCountingBackend struct {
	types.Backend

	chunkReads atomic.Int64
}

func (b *readCountingBackend) ReadCtx(ctx context.Context, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	if fileType == types.FileTypeChunk {
		b.chunkReads.Add(1)
	}
	return b.Backend.ReadCtx(ctx, fileType, objectID, offset, length)
}

func (b *readCountingBackend) Read(fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	return b.ReadCtx(context.Background(), fileType, objectID, offset, length)
}

// newCachedVolume builds a file-backed volume with a real plaintext cache,
// writes nBlocks of recognisable data and drains them to the backend, so every
// block is Persisted with no in-memory copy left behind.
func newCachedVolume(t *testing.T, volume string, cacheBlocks, nBlocks int) (*VB, *readCountingBackend, []byte) {
	t.Helper()

	root := t.TempDir()
	const volSize = 8 * 1024 * 1024

	vb, err := New(&VB{
		VolumeName:          volume,
		VolumeSize:          volSize,
		BaseDir:             fmt.Sprintf("%s/viperblock", root),
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		Cache:               Cache{Config: CacheConfig{Size: cacheBlocks}},
	}, FileBackend, file.FileConfig{VolumeName: volume, VolumeSize: volSize, BaseDir: root})
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	bs := int(vb.BlockSize)
	payload := make([]byte, nBlocks*bs)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	require.NoError(t, vb.WriteAt(0, append([]byte(nil), payload...)))
	require.NoError(t, vb.DrainToBackend())
	dropInMemoryCopies(t, vb)

	// Count only reads issued after the data is already durable.
	counting := &readCountingBackend{Backend: vb.Backend}
	vb.Backend = counting

	return vb, counting, payload
}

// dropInMemoryCopies leaves the volume in the state a freshly reopened one
// would be in -- blocks resolvable as Persisted from the checkpoint, with
// nothing left in the write buffers or the block-store overlay to answer a read
// from. It deliberately does not touch vb.Cache, which is what the tests below
// are measuring.
func dropInMemoryCopies(t *testing.T, vb *VB) {
	t.Helper()
	vb.BlockStore.Clear()
	vb.Writes.mu.Lock()
	vb.Writes.Blocks = nil
	vb.Writes.mu.Unlock()
	vb.PendingBackendWrites.mu.Lock()
	vb.PendingBackendWrites.Blocks = nil
	vb.PendingBackendWrites.mu.Unlock()
	require.NoError(t, vb.LoadLiveCheckpoint())
}

// TestPersistedCache_SecondReadIssuesNoBackendRequest is the exit criterion for
// the cache repair: before it, MarkPersistedRange deleted the per-block shard
// entry that BlockStore.Cache() needed, so persisted data was never cached and
// every read of it was a backend round trip.
func TestPersistedCache_SecondReadIssuesNoBackendRequest(t *testing.T) {
	const nBlocks = 8
	vb, counting, payload := newCachedVolume(t, "vol-persisted-cache", 1024, nBlocks)
	length := uint64(len(payload))

	// Start from a cold cache. The chunk-upload path warms it (see
	// TestPersistedCache_UploadWarmsCache), which would mask a read-path miss.
	vb.Cache.purge()

	first, err := vb.ReadAt(0, length)
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload, first), "first read returned wrong data")

	afterFirst := counting.chunkReads.Load()
	require.Positive(t, afterFirst, "first read should have gone to the backend")

	second, err := vb.ReadAt(0, length)
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload, second), "second read returned wrong data")

	require.Equal(t, afterFirst, counting.chunkReads.Load(),
		"second read of persisted data must be served from cache without a backend request")
}

// TestPersistedCache_UploadWarmsCache pins the other half of the win: a block
// is cached as it is uploaded, so data that was just written stays readable
// without a backend round trip even after every in-memory write buffer is
// dropped.
func TestPersistedCache_UploadWarmsCache(t *testing.T) {
	const nBlocks = 8
	vb, counting, payload := newCachedVolume(t, "vol-persisted-cache-warm", 1024, nBlocks)

	got, err := vb.ReadAt(0, uint64(len(payload)))
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload, got), "read after upload returned wrong data")
	require.Zero(t, counting.chunkReads.Load(),
		"a block cached during upload must not be refetched")
}

// TestPersistedCache_StaleEntryIsNotServed pins the property that makes the
// cache safe without write-path invalidation. Entries are tagged with the
// sequence number they were cached under, so a copy that a re-persist has
// superseded fails the tag check and is refetched rather than served. The stale
// entry is planted directly because the upload path keeps the cache coherent on
// its own -- the tag is the defence for the cases that do not go through it,
// such as a volume reopened over data another instance has since rewritten.
func TestPersistedCache_StaleEntryIsNotServed(t *testing.T) {
	const nBlocks = 8
	vb, counting, payload := newCachedVolume(t, "vol-persisted-cache-stale", 1024, nBlocks)
	bs := uint64(vb.BlockSize)
	length := uint64(len(payload))

	entry, ok := vb.BlockStore.ReadEntry(3)
	require.True(t, ok, "block 3 should be resolvable")

	// Plant a copy of block 3 under a sequence number the index has moved past.
	stale := make([]byte, bs)
	for i := range stale {
		stale[i] = 0xAB
	}
	vb.Cache.put(3, entry.SeqNum-1, stale)

	before := counting.chunkReads.Load()

	got, err := vb.ReadAt(0, length)
	require.NoError(t, err)
	require.True(t, bytes.Equal(payload[3*bs:4*bs], got[3*bs:4*bs]),
		"a superseded cache entry was served instead of the current block")
	require.Positive(t, counting.chunkReads.Load()-before,
		"the superseded block should have been refetched from the backend")

	for i := range uint64(nBlocks) {
		require.True(t, bytes.Equal(payload[i*bs:(i+1)*bs], got[i*bs:(i+1)*bs]),
			"block %d returned wrong data", i)
	}
}
