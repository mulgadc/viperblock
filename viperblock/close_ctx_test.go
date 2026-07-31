package viperblock

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockingBackend wraps a real types.Backend and makes every WriteCtx call
// block until its context is done, then return ctx.Err(). It stands in for a
// predastore that has stopped responding, so CloseCtx's own bound is what
// unwedges the caller instead of the backend ever completing the write.
type blockingBackend struct {
	types.Backend
}

func (b *blockingBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	<-ctx.Done()
	return ctx.Err()
}

// newCloseCtxTestVB builds a minimal file-backed VB, modeled on
// newEnospcTestVB (enospc_test.go): background WAL fsync and chunk upload are
// disabled so DrainToBackendCtx/CloseCtx are the only things touching the
// backend. Its Backend starts out real and unwrapped so setup (Init, WriteAt)
// never blocks; callers wrap vb.Backend in blockingBackend afterward.
func newCloseCtxTestVB(t *testing.T, root, volumeName string) (*VB, file.FileConfig) {
	t.Helper()

	backendConfig := file.FileConfig{
		VolumeName: volumeName,
		VolumeSize: 64 * 1024 * 1024,
		BaseDir:    root,
	}

	vbconfig := VB{
		VolumeName:          volumeName,
		VolumeSize:          64 * 1024 * 1024,
		BaseDir:             fmt.Sprintf("%s/%s", root, "viperblock"),
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}

	vb, err := New(&vbconfig, FileBackend, backendConfig)
	require.NoError(t, err)
	require.NotNil(t, vb)

	vb.UseShardedWAL = false
	vb.ShardedWAL = nil

	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	return vb, backendConfig
}

// TestCloseCtxExpiredContextReturnsPromptlyAndKeepsLocalFiles pins the first
// half of the shutdown-drain bound: CloseCtx given an already-expired context
// must not attempt to wait on the backend at all, and the #58 firstErr gate
// must keep the local WAL/state files since nothing reached the backend.
func TestCloseCtxExpiredContextReturnsPromptlyAndKeepsLocalFiles(t *testing.T) {
	root := t.TempDir()
	const vol = "vol-closectx-expired"

	vb, _ := newCloseCtxTestVB(t, root, vol)
	blockSize := int(vb.BlockSize)
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	// Wrap only after setup so New/Init/WriteAt above never touch the
	// blocking backend.
	vb.Backend = &blockingBackend{Backend: vb.Backend}

	localPath := filepath.Join(vb.BaseDir, vb.GetVolume())
	require.DirExists(t, localPath, "local state should exist before CloseCtx")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := vb.CloseCtx(ctx)
	elapsed := time.Since(start)

	require.Error(t, err, "CloseCtx must report the context error")
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, time.Second,
		"CloseCtx must return promptly on an already-done context, not attempt any backend I/O")

	require.DirExists(t, localPath, "CloseCtx deleted the only copy of the un-uploaded writes")
}

// TestCloseCtxLiveContextHealthyBackendCompletes pins the other half: against
// a backend that actually completes writes, a live (non-expired) context must
// not change CloseCtx's clean-path behavior, including removing local files.
func TestCloseCtxLiveContextHealthyBackendCompletes(t *testing.T) {
	root := t.TempDir()
	const vol = "vol-closectx-healthy"

	vb, _ := newCloseCtxTestVB(t, root, vol)
	blockSize := int(vb.BlockSize)
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	localPath := filepath.Join(vb.BaseDir, vb.GetVolume())
	require.DirExists(t, localPath, "local state should exist before CloseCtx")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, vb.CloseCtx(ctx))

	_, statErr := os.Stat(localPath)
	require.True(t, os.IsNotExist(statErr), "a clean CloseCtx should remove local files, stat gave %v", statErr)
}

// TestCloseCtxDeadlineAbortLeavesWALRecoverable is the property the whole
// bound rests on: a CloseCtx aborted by its deadline must leave the write
// recoverable from the local WAL. It reopens a fresh VB over the same
// BaseDir/backend, replaying the production boot order (LoadState ->
// LoadLiveCheckpoint -> RecoverLocalWALs -> OpenWAL, matching
// nbd/viperblock.go's connection-open sequence), and reads the data back.
func TestCloseCtxDeadlineAbortLeavesWALRecoverable(t *testing.T) {
	root := t.TempDir()
	const vol = "vol-closectx-deadline-abort"

	vb, backendConfig := newCloseCtxTestVB(t, root, vol)
	blockSize := int(vb.BlockSize)
	want := bytes.Repeat([]byte{0x7B}, blockSize)
	require.NoError(t, vb.WriteAt(0, append([]byte(nil), want...)))

	vb.Backend = &blockingBackend{Backend: vb.Backend}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := vb.CloseCtx(ctx)
	elapsed := time.Since(start)

	require.Error(t, err, "CloseCtx must report the deadline that aborted the backend chunk upload")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 5*time.Second, "CloseCtx must return once its deadline fires, not hang")

	localPath := filepath.Join(vb.BaseDir, vb.GetVolume())
	require.DirExists(t, localPath, "aborted CloseCtx must keep local files for recovery")

	reopenConfig := VB{
		VolumeName:      vol,
		VolumeSize:      64 * 1024 * 1024,
		BaseDir:         vb.BaseDir,
		WALSyncInterval: -1,
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}
	reopened, err := New(&reopenConfig, FileBackend, backendConfig)
	require.NoError(t, err)
	require.NoError(t, reopened.Backend.Init())
	require.NoError(t, reopened.LoadState())
	require.NoError(t, reopened.LoadLiveCheckpoint())
	require.NoError(t, reopened.RecoverLocalWALs())
	require.NoError(t, reopened.OpenWAL(&reopened.WAL, fmt.Sprintf("%s/%s", reopened.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, reopened.WAL.WallNum.Load(), reopened.GetVolume()))))
	require.NoError(t, reopened.OpenWAL(&reopened.BlockToObjectWAL, fmt.Sprintf("%s/%s", reopened.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, reopened.BlockToObjectWAL.WallNum.Load(), reopened.GetVolume()))))
	t.Cleanup(func() { assert.NoError(t, reopened.RemoveLocalFiles()) })

	got, err := reopened.ReadAt(0, uint64(blockSize))
	require.NoError(t, err)
	assert.Equal(t, want, got, "the aborted CloseCtx must not lose the write: WAL replay must reproduce it byte-for-byte")
}
