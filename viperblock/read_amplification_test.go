package viperblock

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readAmpVolumeBytes and readAmpRequestBytes mirror the shape the live
// data-path measurement uses: a sequential stream of 64 KiB reads over a
// region several chunks wide.
const (
	readAmpVolumeBytes  = 16 << 20
	readAmpRequestBytes = 64 << 10
)

// chunkReadCounter counts backend reads of chunk objects, which is what a
// round trip to the object store costs on the read path.
type chunkReadCounter struct {
	*file.Backend

	reads atomic.Int64
}

var _ types.Backend = (*chunkReadCounter)(nil)

func (b *chunkReadCounter) ReadCtx(ctx context.Context, fileType types.FileType, objectId uint64, offset uint32, length uint32) ([]byte, error) {
	if fileType == types.FileTypeChunk {
		b.reads.Add(1)
	}
	return b.Backend.ReadCtx(ctx, fileType, objectId, offset, length)
}

// seedSequentialVolume writes readAmpVolumeBytes and pushes it all into
// chunks, returning the root the backend is stored under.
func seedSequentialVolume(t *testing.T, volumeName string) string {
	t.Helper()

	root := t.TempDir()

	vbconfig := VB{
		VolumeName:      volumeName,
		VolumeSize:      readAmpVolumeBytes,
		BaseDir:         filepath.Join(root, "viperblock"),
		WALSyncInterval: -1,
		GCEnabled:       false,
		GCInterval:      -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}
	writer, err := New(&vbconfig, FileBackend, file.FileConfig{
		VolumeName: volumeName,
		VolumeSize: readAmpVolumeBytes,
		BaseDir:    root,
	})
	require.NoError(t, err)
	require.NoError(t, writer.Backend.Init())
	require.NoError(t, writer.OpenWAL(&writer.WAL, filepath.Join(writer.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, writer.WAL.WallNum.Load(), writer.GetVolume()))))
	require.NoError(t, writer.OpenWAL(&writer.BlockToObjectWAL, filepath.Join(writer.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, writer.BlockToObjectWAL.WallNum.Load(), writer.GetVolume()))))

	blocksPerWrite := uint64(readAmpRequestBytes) / uint64(DefaultBlockSize)
	for block := uint64(0); block < uint64(readAmpVolumeBytes)/uint64(DefaultBlockSize); block += blocksPerWrite {
		require.NoError(t, writer.Write(block, randomBlockData(blocksPerWrite)))
	}
	require.NoError(t, writer.Flush())
	require.NoError(t, writer.WriteWALToChunk(true))
	// Close, not the individual saves: it persists the state and the numbered
	// checkpoint in the order a reopen expects to find them.
	require.NoError(t, writer.Close())

	return root
}

// coldReaderVB opens volumeName with an empty local directory and a cold
// cache, which is what every new NBD connection gets.
func coldReaderVB(t *testing.T, backendRoot, volumeName string) (*VB, *chunkReadCounter) {
	t.Helper()

	vbconfig := VB{
		VolumeName:      volumeName,
		VolumeSize:      readAmpVolumeBytes,
		BaseDir:         filepath.Join(t.TempDir(), "viperblock"),
		WALSyncInterval: -1,
		GCEnabled:       false,
		GCInterval:      -1,
		Cache:           Cache{Config: CacheConfig{Size: DefaultCacheBlocks}},
	}

	vb, err := New(&vbconfig, FileBackend, file.FileConfig{
		VolumeName: volumeName,
		VolumeSize: readAmpVolumeBytes,
		BaseDir:    backendRoot,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, vb.RemoveLocalFiles()) })

	fb, ok := vb.Backend.(*file.Backend)
	require.True(t, ok)
	require.NoError(t, fb.Init())
	require.NoError(t, vb.LoadState())
	require.NoError(t, vb.LoadBlockState())

	counter := &chunkReadCounter{Backend: fb}
	vb.Backend = counter

	return vb, counter
}

// TestSequentialRead_CostsOneBackendReadPerRequest records what the read path
// currently does, which the live data-path numbers only inferred from timing:
// a sequential stream costs one backend round trip per NBD request, not per
// chunk. Over the object store each of those is ~4.8 ms, so a depth-1
// sequential reader is bounded at one request per round trip however large
// the chunks are.
//
// The two numbers are the whole point of the test. Requests is what the
// client asked for; chunks is the lower bound any fetch strategy could
// achieve. Today they are 256 and 4.
func TestSequentialRead_CostsOneBackendReadPerRequest(t *testing.T) {
	ctx := context.Background()
	volumeName := "vol-readamp"

	root := seedSequentialVolume(t, volumeName)
	reader, counter := coldReaderVB(t, root, volumeName)

	requests := 0
	for offset := uint64(0); offset < readAmpVolumeBytes; offset += readAmpRequestBytes {
		data, err := reader.ReadAtCtx(ctx, offset, readAmpRequestBytes)
		require.NoErrorf(t, err, "read at offset %d", offset)
		require.Len(t, data, readAmpRequestBytes)
		requests++
	}

	chunksTouched := int64(readAmpVolumeBytes / reader.ObjBlockSize)
	backendReads := counter.reads.Load()

	require.Positive(t, backendReads, "no chunk was read from the backend, so this measures nothing")
	t.Logf("sequential stream: %d requests, %d chunks touched, %d backend chunk reads",
		requests, chunksTouched, backendReads)

	assert.Equal(t, int64(requests), backendReads,
		"read amplification changed: this test records one backend read per request, "+
			"so a fetch strategy that reads wider than the request must update it (lower bound is %d)", chunksTouched)
}
