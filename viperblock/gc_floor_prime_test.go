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

// checkpointReadCounter counts backend reads of the numbered block
// checkpoint. Over S3 each of those is a round trip, which is what makes
// fetching the same object twice per open worth removing.
type checkpointReadCounter struct {
	*file.Backend

	reads atomic.Int64
}

var _ types.Backend = (*checkpointReadCounter)(nil)

func (b *checkpointReadCounter) ReadCtx(ctx context.Context, fileType types.FileType, objectId uint64, offset uint32, length uint32) ([]byte, error) {
	if fileType == types.FileTypeBlockCheckpoint {
		b.reads.Add(1)
	}
	return b.Backend.ReadCtx(ctx, fileType, objectId, offset, length)
}

// checkpointReaderVB opens volumeName against an existing backend root with a
// private, empty local directory, so LoadBlockStateCtx has to go to the
// backend. That is the state a fresh process finds a volume in, and the one
// the seal on unmount runs in.
func checkpointReaderVB(t *testing.T, backendRoot, volumeName string, wallNum uint64) (*VB, *checkpointReadCounter) {
	t.Helper()

	vbconfig := VB{
		VolumeName:      volumeName,
		VolumeSize:      volumeSize,
		BaseDir:         filepath.Join(t.TempDir(), "viperblock"),
		WALSyncInterval: -1,
		GCEnabled:       true,
		GCInterval:      -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}

	vb, err := New(&vbconfig, FileBackend, file.FileConfig{
		VolumeName: volumeName,
		VolumeSize: volumeSize,
		BaseDir:    backendRoot,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, vb.RemoveLocalFiles()) })

	fb, ok := vb.Backend.(*file.Backend)
	require.True(t, ok)
	require.NoError(t, fb.Init())

	counter := &checkpointReadCounter{Backend: fb}
	vb.Backend = counter
	vb.BlockToObjectWAL.WallNum.Store(wallNum)

	return vb, counter
}

// seedCheckpoint writes two chunks and persists a numbered checkpoint
// referencing both, returning the checkpoint's WallNum and the highest chunk
// ObjectID it names.
func seedCheckpoint(t *testing.T, root, volumeName string) (wallNum, highestChunk uint64) {
	t.Helper()

	writer := newGCTestVB(t, root, volumeName, true)
	writeAndChunk(t, writer, 0, randomBlockData(4))
	writeAndChunk(t, writer, 64, randomBlockData(4))

	wallNum = writer.BlockToObjectWAL.WallNum.Load()
	highestChunk = writer.ObjectNum.Load() - 1
	require.NoError(t, writer.SaveBlockState())

	return wallNum, highestChunk
}

// TestGCFloor_ReusesTheCheckpointTheStateLoadFetched pins the fix for an open
// that fetched the numbered checkpoint twice: once to build the block map and
// again, unchanged, to compute the GC floor.
func TestGCFloor_ReusesTheCheckpointTheStateLoadFetched(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeName := "vol-gcfloor-prime"

	wallNum, highestChunk := seedCheckpoint(t, root, volumeName)
	reader, counter := checkpointReaderVB(t, root, volumeName, wallNum)

	require.NoError(t, reader.LoadBlockStateCtx(ctx))
	require.Equal(t, int64(1), counter.reads.Load(), "the block state load itself read the checkpoint more than once")

	floor := reader.ensureGCFloor(ctx)
	assert.Equal(t, highestChunk+1, floor, "the primed floor does not match the checkpoint's high water")
	assert.Equal(t, int64(1), counter.reads.Load(),
		"the GC floor re-read the numbered checkpoint the block state load had already fetched")
}

// TestGCFloor_IgnoresTheLocalCheckpoint is the safety half. The local
// checkpoint is not guaranteed to match the backend's, and a floor derived
// from an older local copy would be lower than the backend checkpoint needs,
// letting GC delete a chunk that checkpoint still references. Only a backend
// read may prime.
func TestGCFloor_IgnoresTheLocalCheckpoint(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	volumeName := "vol-gcfloor-local"

	// newGCTestVB shares root between the VB tree and the backend tree, so this
	// writer keeps its own local checkpoint alongside the backend's.
	writer := newGCTestVB(t, root, volumeName, true)
	writeAndChunk(t, writer, 0, randomBlockData(4))
	wallNum := writer.BlockToObjectWAL.WallNum.Load()
	require.NoError(t, writer.SaveBlockState())
	writer.BlockToObjectWAL.WallNum.Store(wallNum)

	fb, ok := writer.Backend.(*file.Backend)
	require.True(t, ok)
	counter := &checkpointReadCounter{Backend: fb}
	writer.Backend = counter

	require.NoError(t, writer.LoadBlockStateCtx(ctx))
	require.Equal(t, int64(0), counter.reads.Load(), "the local checkpoint was present but the backend was read anyway")
	assert.False(t, writer.gcFloorReady.Load(), "the GC floor was primed from the local checkpoint")

	// And the floor it does end up with comes from the backend.
	writer.ensureGCFloor(ctx)
	assert.Equal(t, int64(1), counter.reads.Load(), "the GC floor was not read from the backend")
}
