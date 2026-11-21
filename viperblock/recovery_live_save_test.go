package viperblock

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// walFiles lists every non-empty local WAL file under the volume's WAL dir.
func walFiles(t *testing.T, root, volumeName string) []string {
	t.Helper()
	var out []string
	dir := filepath.Join(root, "viperblock", volumeName, "wal")
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Size() > 0 {
			out = append(out, path)
		}
		return nil
	})
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	sort.Strings(out)
	return out
}

// openWithFailingLive opens a second process's view of volumeName over the
// same root, after a crash of the first, with live-checkpoint writes failing.
func openWithFailingLive(t *testing.T, root, volumeName string) (*VB, *failingCheckpointBackend) {
	t.Helper()
	vbconfig := VB{
		VolumeName:      volumeName,
		VolumeSize:      volumeSize,
		BaseDir:         filepath.Join(root, "viperblock"),
		WALSyncInterval: -1,
		GCEnabled:       true,
		GCInterval:      -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}
	vb, err := New(&vbconfig, FileBackend, file.FileConfig{VolumeName: volumeName, VolumeSize: volumeSize, BaseDir: root})
	require.NoError(t, err)
	t.Cleanup(vb.Detach)
	fb, ok := vb.Backend.(*file.Backend)
	require.True(t, ok)
	wrapped := &failingCheckpointBackend{Backend: fb}
	vb.Backend = wrapped
	vb.checkpointRetryBackoff = time.Microsecond
	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.LoadState())
	require.NoError(t, vb.LoadLiveCheckpoint())
	return vb, wrapped
}

// seedOlderLiveThenWALOverwrite leaves original durable in the live
// checkpoint and an overwrite of the same blocks only in the local WAL, then
// abandons the process as a crash would.
func seedOlderLiveThenWALOverwrite(t *testing.T, root, volumeName string) (original, overwrite []byte) {
	t.Helper()
	vb := newGCTestVB(t, root, volumeName, true)
	original = randomBlockData(2)
	writeAndChunk(t, vb, 0, original)
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
	require.NoError(t, vb.SaveState())

	overwrite = randomBlockData(2)
	require.NoError(t, vb.Write(0, overwrite))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.SaveState())
	vb.Detach()
	return original, overwrite
}

func assertReadsBack(t *testing.T, vb *VB, want []byte, msg string) {
	t.Helper()
	got, err := vb.ReadAt(0, uint64(len(want)))
	require.NoError(t, err)
	assert.Equal(t, want, got, msg)
}

// A recovery whose live-checkpoint save fails must keep the WAL it replayed:
// the next open prefers the older live map, so the WAL is the only copy.
func TestRecoverLocalWALs_FailedLiveSaveKeepsWAL(t *testing.T) {
	root := t.TempDir()
	const volumeName = "vol-recover-live-fail"
	_, overwrite := seedOlderLiveThenWALOverwrite(t, root, volumeName)
	before := walFiles(t, root, volumeName)
	require.NotEmpty(t, before)

	vb, wrapped := openWithFailingLive(t, root, volumeName)
	wrapped.failLiveCheckpoint.Store(true)
	assert.Error(t, vb.RecoverLocalWALs(), "a failed live save must fail recovery")
	assert.Subset(t, walFiles(t, root, volumeName), before, "no replayed WAL file may be removed")
	vb.Detach()

	// A second failed open changes nothing either.
	again, wrappedAgain := openWithFailingLive(t, root, volumeName)
	wrappedAgain.failLiveCheckpoint.Store(true)
	assert.Error(t, again.RecoverLocalWALs())
	assert.Subset(t, walFiles(t, root, volumeName), before)
	again.Detach()

	healthy := reopenGCTestVB(t, root, volumeName, true)
	assertReadsBack(t, healthy, overwrite, "the acknowledged overwrite must survive a failed recovery")
	fresh := reopenAfterClose(t, healthy, root, volumeName)
	assertReadsBack(t, fresh, overwrite, "and a further reopen")
}

// Close whose live-checkpoint save fails must fail and keep local files, for
// the same reason: the next open reads the older live map.
func TestCloseCtx_FailedLiveSaveKeepsWAL(t *testing.T) {
	root := t.TempDir()
	const volumeName = "vol-close-live-fail"
	_, overwrite := seedOlderLiveThenWALOverwrite(t, root, volumeName)

	vb, wrapped := openWithFailingLive(t, root, volumeName)
	require.NoError(t, vb.RecoverLocalWALs())
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	second := randomBlockData(2)
	require.NoError(t, vb.Write(0, second))
	require.NoError(t, vb.Flush())
	before := walFiles(t, root, volumeName)
	require.NotEmpty(t, before)

	wrapped.failLiveCheckpoint.Store(true)
	assert.Error(t, vb.CloseCtx(context.Background()), "a failed live save must fail Close")
	assert.Subset(t, walFiles(t, root, volumeName), before, "Close must keep the WAL when the live save failed")

	healthy := reopenGCTestVB(t, root, volumeName, true)
	assertReadsBack(t, healthy, second, "the write acknowledged before the failed Close must survive")
	assert.NotEqual(t, overwrite, second)
}

// reopenAfterClose closes vb cleanly and opens the volume again.
func reopenAfterClose(t *testing.T, vb *VB, root, volumeName string) *VB {
	t.Helper()
	require.NoError(t, vb.CloseCtx(context.Background()))
	return reopenGCTestVB(t, root, volumeName, true)
}
