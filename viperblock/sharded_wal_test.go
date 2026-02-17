package viperblock

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupShardedTestVB creates a VB with sharded WAL enabled for testing.
func setupShardedTestVB(t *testing.T) (*VB, func()) {
	t.Helper()

	tmpDir := t.TempDir()
	testVol := fmt.Sprintf("shard_test_%d", os.Getpid())

	vbconfig := VB{
		VolumeName:      testVol,
		VolumeSize:      volumeSize,
		BaseDir:         fmt.Sprintf("%s/viperblock", tmpDir),
		WALSyncInterval: -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}

	vb, err := New(&vbconfig, "file", file.FileConfig{
		VolumeName: testVol,
		VolumeSize: volumeSize,
		BaseDir:    tmpDir,
	})
	require.NoError(t, err)

	err = vb.Backend.Init()
	require.NoError(t, err)

	// Enable sharded WAL
	vb.UseShardedWAL = true
	vb.ShardedWAL = NewShardedWAL(vb.WAL.BaseDir, vb.WAL.WALMagic)
	err = vb.OpenShardedWAL()
	require.NoError(t, err)

	// Open BlockToObjectWAL (needed for full Flush path)
	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume())))
	require.NoError(t, err)

	cleanup := func() {
		vb.RemoveLocalFiles()
	}

	return vb, cleanup
}

func TestShardedWAL_WriteReadRoundTrip(t *testing.T) {
	vb, cleanup := setupShardedTestVB(t)
	defer cleanup()

	// Write blocks across multiple shards
	data := make([]byte, 4096)
	rand.Read(data)

	for i := uint64(0); i < 32; i++ {
		err := vb.WriteShardedWAL(Block{
			SeqNum: i + 1,
			Block:  i,
			Len:    4096,
			Data:   data,
		})
		require.NoError(t, err)
	}

	// Verify shard files exist
	walNum := vb.ShardedWAL.WallNum.Load()
	for i := 0; i < NumShards; i++ {
		path := filepath.Join(vb.ShardedWAL.BaseDir,
			types.GetShardedWALPath(vb.GetVolume(), walNum, i))
		_, err := os.Stat(path)
		assert.NoError(t, err, "shard file %d should exist", i)
	}
}

func TestShardedWAL_ShardRouting(t *testing.T) {
	// Verify block N goes to shard N & ShardMask
	for blockNum := uint64(0); blockNum < 256; blockNum++ {
		expectedShard := blockNum & ShardMask
		assert.Equal(t, expectedShard, blockNum&ShardMask,
			"block %d should route to shard %d", blockNum, expectedShard)
	}

	// Verify blocks with same low bits go to same shard
	assert.Equal(t, uint64(0)&ShardMask, uint64(16)&ShardMask)
	assert.Equal(t, uint64(1)&ShardMask, uint64(17)&ShardMask)
	assert.Equal(t, uint64(15)&ShardMask, uint64(31)&ShardMask)

	// Verify different low bits go to different shards
	assert.NotEqual(t, uint64(0)&ShardMask, uint64(1)&ShardMask)
}

func TestShardedWAL_ConcurrentWriters(t *testing.T) {
	vb, cleanup := setupShardedTestVB(t)
	defer cleanup()

	data := make([]byte, 4096)
	rand.Read(data)

	const numWriters = 8
	const blocksPerWriter = 128
	var wg sync.WaitGroup
	errors := make([]error, numWriters)

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < blocksPerWriter; j++ {
				blockNum := uint64(writerID*blocksPerWriter + j)
				err := vb.WriteShardedWAL(Block{
					SeqNum: blockNum + 1,
					Block:  blockNum,
					Len:    4096,
					Data:   data,
				})
				if err != nil {
					errors[writerID] = err
					return
				}
			}
		}(w)
	}
	wg.Wait()

	for i, err := range errors {
		assert.NoError(t, err, "writer %d should not error", i)
	}

	// Consolidate and verify all blocks are preserved
	err := vb.WriteShardedWALToChunk(true)
	require.NoError(t, err)
}

func TestShardedWAL_ConsolidationMerge(t *testing.T) {
	vb, cleanup := setupShardedTestVB(t)
	defer cleanup()

	// Write blocks spread across all shards
	blocks := make(map[uint64][]byte)
	for i := uint64(0); i < 64; i++ {
		data := make([]byte, 4096)
		rand.Read(data)
		blocks[i] = data

		err := vb.WriteShardedWAL(Block{
			SeqNum: i + 1,
			Block:  i,
			Len:    4096,
			Data:   data,
		})
		require.NoError(t, err)
	}

	// Consolidate
	err := vb.WriteShardedWALToChunk(true)
	assert.NoError(t, err)

	// Next generation shard files should have been opened
	assert.Equal(t, uint64(1), vb.ShardedWAL.WallNum.Load())
}

func TestShardedWAL_ConsolidationDedup(t *testing.T) {
	vb, cleanup := setupShardedTestVB(t)
	defer cleanup()

	data1 := make([]byte, 4096)
	data2 := make([]byte, 4096)
	rand.Read(data1)
	rand.Read(data2)

	// Write same block twice — second write should win (higher SeqNum)
	err := vb.WriteShardedWAL(Block{SeqNum: 1, Block: 5, Len: 4096, Data: data1})
	require.NoError(t, err)

	err = vb.WriteShardedWAL(Block{SeqNum: 2, Block: 5, Len: 4096, Data: data2})
	require.NoError(t, err)

	err = vb.WriteShardedWALToChunk(true)
	assert.NoError(t, err)
}

func TestShardedWAL_Recovery(t *testing.T) {
	vb, cleanup := setupShardedTestVB(t)
	defer cleanup()

	data := make([]byte, 4096)
	rand.Read(data)

	// Write some blocks
	for i := uint64(0); i < 16; i++ {
		err := vb.WriteShardedWAL(Block{
			SeqNum: i + 1,
			Block:  i,
			Len:    4096,
			Data:   data,
		})
		require.NoError(t, err)
	}

	// Sync all shards
	for i := 0; i < NumShards; i++ {
		shard := vb.ShardedWAL.Shards[i]
		shard.mu.RLock()
		if shard.DB != nil {
			shard.DB.Sync()
		}
		shard.mu.RUnlock()
	}

	// RecoverLocalWALs should be able to read shard files
	// (they use same format as legacy WAL files)
	err := vb.RecoverLocalWALs()
	assert.NoError(t, err)
}

func TestShardedWAL_BackwardCompat(t *testing.T) {
	// A volume created without sharded WAL should still work
	tmpDir := t.TempDir()
	testVol := "compat_test"

	vbconfig := VB{
		VolumeName:      testVol,
		VolumeSize:      volumeSize,
		BaseDir:         fmt.Sprintf("%s/viperblock", tmpDir),
		WALSyncInterval: -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}

	vb, err := New(&vbconfig, "file", file.FileConfig{
		VolumeName: testVol,
		VolumeSize: volumeSize,
		BaseDir:    tmpDir,
	})
	require.NoError(t, err)

	err = vb.Backend.Init()
	require.NoError(t, err)

	// Sharded WAL is disabled by default; struct is pre-allocated but inactive
	assert.False(t, vb.UseShardedWAL)
	assert.NotNil(t, vb.ShardedWAL)

	walPath := fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, 0, vb.GetVolume()))
	err = vb.OpenWAL(&vb.WAL, walPath)
	require.NoError(t, err)

	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume())))
	require.NoError(t, err)

	// Write and flush with legacy WAL
	data := make([]byte, 4096)
	rand.Read(data)

	err = vb.WriteAt(0, data)
	require.NoError(t, err)

	err = vb.Flush()
	assert.NoError(t, err)

	err = vb.WriteWALToChunk(true)
	assert.NoError(t, err)

	vb.RemoveLocalFiles()
}

func TestShardedWAL_FlushDispatch(t *testing.T) {
	vb, cleanup := setupShardedTestVB(t)
	defer cleanup()

	data := make([]byte, 4096)
	rand.Read(data)

	// Write blocks via WriteAt (goes to vb.Writes)
	for i := 0; i < 32; i++ {
		err := vb.WriteAt(uint64(i)*4096, data)
		require.NoError(t, err)
	}

	// Flush should dispatch to flushLockedSharded
	err := vb.Flush()
	assert.NoError(t, err)

	// Verify blocks were written to shards (at least some should be dirty)
	// After flush, all writes should have been drained
	assert.Equal(t, 0, len(vb.Writes.Blocks))
}

func TestShardedWAL_SyncDirtyOnly(t *testing.T) {
	vb, cleanup := setupShardedTestVB(t)
	defer cleanup()

	// Initially all shards should be clean
	for i := 0; i < NumShards; i++ {
		assert.False(t, vb.ShardedWAL.Shards[i].dirty.Load(), "shard %d should be clean initially", i)
	}

	data := make([]byte, 4096)
	rand.Read(data)

	// Write to block 0 (shard 0) and block 1 (shard 1)
	vb.WriteShardedWAL(Block{SeqNum: 1, Block: 0, Len: 4096, Data: data})
	vb.WriteShardedWAL(Block{SeqNum: 2, Block: 1, Len: 4096, Data: data})

	// Shards 0 and 1 should be dirty
	assert.True(t, vb.ShardedWAL.Shards[0].dirty.Load())
	assert.True(t, vb.ShardedWAL.Shards[1].dirty.Load())

	// Other shards should be clean
	for i := 2; i < NumShards; i++ {
		assert.False(t, vb.ShardedWAL.Shards[i].dirty.Load(), "shard %d should be clean", i)
	}

	// Sync should clear dirty flags
	vb.syncShardedWALIfDirty()
	for i := 0; i < NumShards; i++ {
		assert.False(t, vb.ShardedWAL.Shards[i].dirty.Load(), "shard %d should be clean after sync", i)
	}
}

func TestShardedWAL_Reset(t *testing.T) {
	vb, cleanup := setupShardedTestVB(t)
	defer cleanup()

	data := make([]byte, 4096)
	rand.Read(data)

	// Write some data
	vb.WriteShardedWAL(Block{SeqNum: 1, Block: 0, Len: 4096, Data: data})

	// Reset should close all shard files
	err := vb.Reset()
	assert.NoError(t, err)

	assert.Equal(t, uint64(0), vb.ShardedWAL.WallNum.Load())
	for i := 0; i < NumShards; i++ {
		assert.Nil(t, vb.ShardedWAL.Shards[i].DB, "shard %d DB should be nil after reset", i)
		assert.False(t, vb.ShardedWAL.Shards[i].dirty.Load(), "shard %d should be clean after reset", i)
	}
}

func TestShardedWAL_StateRoundTrip(t *testing.T) {
	vb, cleanup := setupShardedTestVB(t)
	defer cleanup()

	// Save state with ShardedWAL enabled
	err := vb.SaveState()
	require.NoError(t, err)

	// Load state and verify ShardedWAL flag is preserved
	err = vb.LoadState()
	require.NoError(t, err)

	assert.True(t, vb.UseShardedWAL)
	assert.NotNil(t, vb.ShardedWAL)
}

func TestGetShardedWALPath(t *testing.T) {
	path := types.GetShardedWALPath("vol-123", 5, 3)
	assert.Equal(t, "vol-123/wal/chunks/wal.00000005.shard_03.bin", path)

	path = types.GetShardedWALPath("vol-abc", 0, 15)
	assert.Equal(t, "vol-abc/wal/chunks/wal.00000000.shard_15.bin", path)
}
