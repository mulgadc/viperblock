package viperblock

import (
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

// newWALBaseDirTestVB builds a file-backend VB the same way newEnospcTestVB
// does, except BaseDir and the WAL base dir are two distinct roots under the
// test's tmpDir when separateWALDir is true. When false, WALBaseDir is left
// unset so the WAL/BlockToObjectWAL/ShardedWAL fall back to BaseDir exactly
// as before this field existed.
func newWALBaseDirTestVB(t *testing.T, sharded bool, separateWALDir bool) (vb *VB, baseDir, walBaseDir string) {
	t.Helper()

	tmpDir := t.TempDir()
	testVol := fmt.Sprintf("test_wal_base_dir_%d", time.Now().UnixNano())

	baseDir = filepath.Join(tmpDir, "viperblock")
	walBaseDir = baseDir
	if separateWALDir {
		walBaseDir = filepath.Join(tmpDir, "wal-device")
	}

	backendConfig := file.FileConfig{
		VolumeName: testVol,
		VolumeSize: 64 * 1024 * 1024,
		BaseDir:    tmpDir,
	}

	vbconfig := VB{
		VolumeName: testVol,
		VolumeSize: 64 * 1024 * 1024,
		BaseDir:    baseDir,
		// Deterministic: no background WAL fsync or ticker-driven chunk
		// upload racing the test; drains are driven explicitly.
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}
	if separateWALDir {
		vbconfig.WALBaseDir = walBaseDir
	}

	var err error
	vb, err = New(&vbconfig, FileBackend, backendConfig)
	require.NoError(t, err)
	require.NotNil(t, vb)

	t.Cleanup(func() {
		assert.NoError(t, vb.RemoveLocalFiles())
	})

	vb.UseShardedWAL = sharded
	if !sharded {
		vb.ShardedWAL = nil
	}

	require.NoError(t, vb.Backend.Init())

	if sharded {
		require.NoError(t, vb.OpenShardedWAL())
	} else {
		require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	}
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	return vb, baseDir, walBaseDir
}

// walTreeExists reports whether <root>/<volume>/wal exists and, if it does,
// contains at least one regular file somewhere underneath it.
func walTreeExists(t *testing.T, root, volume string) bool {
	t.Helper()

	walDir := filepath.Join(root, volume, "wal")
	info, err := os.Stat(walDir)
	if os.IsNotExist(err) {
		return false
	}
	require.NoError(t, err)
	require.True(t, info.IsDir(), "%s exists but is not a directory", walDir)

	found := false
	err = filepath.WalkDir(walDir, func(path string, d os.DirEntry, err error) error {
		require.NoError(t, err)
		if !d.IsDir() {
			found = true
		}
		return nil
	})
	require.NoError(t, err)
	return found
}

// TestWALBaseDirSeparatesWALFilesFromBaseDir pins the actual point of
// WALBaseDir: once set, every WAL-shaped file (legacy WAL segments, sharded
// WAL shard files, and the block-to-object WAL) lands only under WALBaseDir,
// never under BaseDir, and data written before a forced rotation and
// consolidation is still readable afterward.
func TestWALBaseDirSeparatesWALFilesFromBaseDir(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		t.Run(fmt.Sprintf("sharded=%v", sharded), func(t *testing.T) {
			vb, baseDir, walBaseDir := newWALBaseDirTestVB(t, sharded, true)

			require.NotEqual(t, baseDir, walBaseDir)
			require.Equal(t, walBaseDir, vb.WAL.BaseDir)
			require.Equal(t, walBaseDir, vb.BlockToObjectWAL.BaseDir)
			if sharded {
				require.Equal(t, walBaseDir, vb.ShardedWAL.BaseDir)
			}

			blockSize := uint64(vb.BlockSize)
			data := make([]byte, blockSize)
			copy(data, []byte("wal-base-dir-test"))
			require.NoError(t, vb.WriteAt(0, data))

			// Force a rotation and consolidation of the current WAL
			// generation into a chunk upload, exactly what a real
			// checkpoint/drain cycle does.
			require.NoError(t, vb.DrainToBackendCtx(context.Background()))

			got, err := vb.ReadAt(0, blockSize)
			require.NoError(t, err)
			assert.Equal(t, data, got, "data must survive a forced WAL rotation and consolidation")

			assert.False(t, walTreeExists(t, baseDir, vb.GetVolume()),
				"no WAL-shaped file must appear under BaseDir when WALBaseDir is set")
			assert.True(t, walTreeExists(t, walBaseDir, vb.GetVolume()),
				"WAL files must appear under WALBaseDir")

			// RemoveLocalFiles must clean up both trees, not just BaseDir's.
			require.NoError(t, vb.RemoveLocalFiles())
			_, err = os.Stat(filepath.Join(baseDir, vb.GetVolume()))
			assert.True(t, os.IsNotExist(err), "BaseDir volume tree must be removed")
			_, err = os.Stat(filepath.Join(walBaseDir, vb.GetVolume()))
			assert.True(t, os.IsNotExist(err), "WALBaseDir volume tree must be removed")
		})
	}
}

// TestWALBaseDirEmptyIsNoOp pins that an unset WALBaseDir behaves exactly as
// it did before the field existed: WAL, BlockToObjectWAL, and ShardedWAL all
// resolve to BaseDir, and WAL files land under BaseDir as before.
func TestWALBaseDirEmptyIsNoOp(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		t.Run(fmt.Sprintf("sharded=%v", sharded), func(t *testing.T) {
			vb, baseDir, walBaseDir := newWALBaseDirTestVB(t, sharded, false)

			require.Equal(t, baseDir, walBaseDir)
			assert.Equal(t, baseDir, vb.WAL.BaseDir)
			assert.Equal(t, baseDir, vb.BlockToObjectWAL.BaseDir)
			if sharded {
				assert.Equal(t, baseDir, vb.ShardedWAL.BaseDir)
			}

			blockSize := uint64(vb.BlockSize)
			data := make([]byte, blockSize)
			copy(data, []byte("wal-base-dir-empty-test"))
			require.NoError(t, vb.WriteAt(0, data))
			require.NoError(t, vb.DrainToBackendCtx(context.Background()))

			got, err := vb.ReadAt(0, blockSize)
			require.NoError(t, err)
			assert.Equal(t, data, got)

			assert.True(t, walTreeExists(t, baseDir, vb.GetVolume()),
				"WAL files must land under BaseDir when WALBaseDir is unset, same as before")
		})
	}
}

// TestWALBaseDirCrashRecovery pins that RecoverLocalWALs looks for orphaned
// WAL files under WALBaseDir, not BaseDir, when the two differ. Before this
// fix, RecoverLocalWALs scanned vb.BaseDir directly: with a separate
// WALBaseDir it would find no wal/chunks directory there, silently treat
// crash recovery as a no-op, and lose every block still sitting only in the
// WAL.
func TestWALBaseDirCrashRecovery(t *testing.T) {
	vb, baseDir, walBaseDir := newWALBaseDirTestVB(t, false, true)
	require.NotEqual(t, baseDir, walBaseDir)

	blockSize := uint64(vb.BlockSize)
	testData := make([][]byte, 3)
	for i := range 3 {
		testData[i] = make([]byte, blockSize)
		msg := fmt.Sprintf("recover_block_%d", i)
		copy(testData[i], []byte(msg))
		require.NoError(t, vb.WriteAt(uint64(i)*blockSize, testData[i]))
	}
	require.NoError(t, vb.Flush())

	// Blocks are now only in the WAL file under walBaseDir, not chunked to
	// the backend. Simulate a crash: reset in-memory state without going
	// through WriteWALToChunk/Close.
	vb.BlocksToObject.mu.Lock()
	vb.BlocksToObject.lookup.clear()
	vb.BlocksToObject.mu.Unlock()

	vb.PendingBackendWrites.mu.Lock()
	vb.PendingBackendWrites.Blocks = nil
	vb.PendingBackendWrites.mu.Unlock()

	if vb.UseBlockStore && vb.BlockStore != nil {
		vb.BlockStore = NewUnifiedBlockStore(vb.BlockSize)
	}

	require.NoError(t, vb.RecoverLocalWALs())

	for i := range 3 {
		data, err := vb.ReadAt(uint64(i)*blockSize, blockSize)
		require.NoError(t, err, "block %d should be recovered from the WAL under WALBaseDir", i)
		assert.Equal(t, testData[i], data, "block %d data mismatch after recovery", i)
	}
}
