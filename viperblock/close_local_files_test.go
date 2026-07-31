package viperblock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// errChunkWrite stands in for a backend that rejects a chunk upload — a full
// or unreachable predastore, say — without needing a real one to fail.
var errChunkWrite = errors.New("simulated chunk write rejection")

// chunkFailBackend fails FileTypeChunk writes only, leaving config and
// checkpoint writes working. That is the case Close has to survive: the WAL
// never reaches the backend while both state saves report success.
type chunkFailBackend struct {
	types.Backend

	failChunks atomic.Bool
}

func (b *chunkFailBackend) Write(fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	if fileType == types.FileTypeChunk && b.failChunks.Load() {
		return errChunkWrite
	}
	return b.Backend.Write(fileType, objectId, headers, data)
}

func (b *chunkFailBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	if fileType == types.FileTypeChunk && b.failChunks.Load() {
		return errChunkWrite
	}
	return b.Backend.WriteCtx(ctx, fileType, objectId, headers, data)
}

func newChunkFailTestVB(t *testing.T) (*VB, *chunkFailBackend) {
	t.Helper()

	tmpDir := t.TempDir()
	testVol := fmt.Sprintf("test_closewal_%d", time.Now().UnixNano())

	backendConfig := file.FileConfig{
		VolumeName: testVol,
		VolumeSize: 64 * 1024 * 1024,
		BaseDir:    tmpDir,
	}

	vbconfig := VB{
		VolumeName: testVol,
		VolumeSize: 64 * 1024 * 1024,
		BaseDir:    fmt.Sprintf("%s/%s", tmpDir, "viperblock"),
		// Deterministic: no background WAL fsync or ticker-driven chunk upload
		// racing Close, which is the only thing under test here.
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
	backend := &chunkFailBackend{Backend: vb.Backend}
	vb.Backend = backend

	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	return vb, backend
}

// TestCloseKeepsLocalFilesWhenChunkUploadFails is the durability guarantee: if
// the WAL never reached the backend, the local copy is the only one left, so
// Close must not delete it however well the state saves went.
func TestCloseKeepsLocalFilesWhenChunkUploadFails(t *testing.T) {
	vb, backend := newChunkFailTestVB(t)

	blockSize := int(vb.BlockSize)
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	localPath := filepath.Join(vb.BaseDir, vb.GetVolume())
	require.DirExists(t, localPath, "local state should exist before Close")

	backend.failChunks.Store(true)

	err := vb.Close()
	require.Error(t, err, "Close must report the failed chunk upload")
	require.ErrorIs(t, err, errChunkWrite)

	require.DirExists(t, localPath, "Close deleted the only copy of the un-uploaded writes")
}

// TestCloseRemovesLocalFilesOnSuccess pins the other half: a clean Close still
// reclaims the local files, so the test above is proving the failure gate and
// not merely that removal never happens.
func TestCloseRemovesLocalFilesOnSuccess(t *testing.T) {
	vb, _ := newChunkFailTestVB(t)

	blockSize := int(vb.BlockSize)
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	localPath := filepath.Join(vb.BaseDir, vb.GetVolume())
	require.DirExists(t, localPath, "local state should exist before Close")

	require.NoError(t, vb.Close())

	_, statErr := os.Stat(localPath)
	require.True(t, os.IsNotExist(statErr), "clean Close should remove local files, stat gave %v", statErr)
}

// TestCloseKeepsLocalFilesWhenFlushFails covers the other ungated failure: a
// partial flush leaves writes only in memory and in the local WAL.
func TestCloseKeepsLocalFilesWhenFlushFails(t *testing.T) {
	vb, _ := newChunkFailTestVB(t)

	localPath := filepath.Join(vb.BaseDir, vb.GetVolume())
	require.DirExists(t, localPath, "local state should exist before Close")

	// Stage a hot write, then close the WAL file underneath it so the flush
	// inside Close fails on write — a WAL that can no longer be appended to.
	vb.Writes.Blocks = append(vb.Writes.Blocks, Block{SeqNum: 1, Block: 0, Len: uint64(vb.BlockSize), Data: make([]byte, int(vb.BlockSize))})
	require.NotEmpty(t, vb.WAL.DB)
	require.NoError(t, vb.WAL.DB[len(vb.WAL.DB)-1].Close())

	err := vb.Close()
	require.Error(t, err, "Close must report the failed flush")

	require.DirExists(t, localPath, "Close deleted the local WAL after a failed flush")
}
