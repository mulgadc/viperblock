package viperblock

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupShardedSyncVB(t *testing.T) *VB {
	t.Helper()
	tmpDir := t.TempDir()
	name := fmt.Sprintf("psync_%d", os.Getpid())
	cfg := VB{VolumeName: name, VolumeSize: volumeSize, BaseDir: tmpDir + "/viperblock",
		WALSyncInterval: -1, ChunkUploadInterval: -1, GCInterval: -1, Cache: Cache{Config: CacheConfig{Size: 0}}}
	vb, err := New(&cfg, "file", file.FileConfig{VolumeName: name, VolumeSize: volumeSize, BaseDir: tmpDir})
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	vb.UseShardedWAL = true
	vb.ShardedWAL = NewShardedWAL(vb.WAL.BaseDir, vb.WAL.WALMagic)
	require.NoError(t, vb.OpenShardedWAL())
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))
	t.Cleanup(func() { vb.RemoveLocalFiles() })
	return vb
}

// A flush must not return while a sync that could cover its records is still
// in flight on that shard, even though the other shards sync concurrently.
func TestShardedFlushWaitsForAnInFlightShardSync(t *testing.T) {
	vb := setupShardedSyncVB(t)
	data := make([]byte, vb.BlockSize)
	_, _ = rand.Read(data)
	require.NoError(t, vb.Write(0, data))
	require.NoError(t, vb.flushWrites())

	shard := vb.ShardedWAL.Shards[0]
	shard.syncMu.Lock() // a sync of shard 0 is in flight
	flushed := make(chan error, 1)
	go func() { flushed <- vb.Flush() }()
	select {
	case err := <-flushed:
		t.Fatalf("Flush returned %v while shard 0's sync was in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	shard.syncMu.Unlock()
	require.NoError(t, <-flushed)
	assert.False(t, shard.dirty.Load(), "the flush synced shard 0 once the in-flight sync ended")
}

// A shard whose fsync fails fails the flush, and stays dirty for a retry.
func TestShardedFlushReturnsAShardSyncError(t *testing.T) {
	vb := setupShardedSyncVB(t)
	data := make([]byte, vb.BlockSize)
	_, _ = rand.Read(data)
	require.NoError(t, vb.Write(3, data))
	require.NoError(t, vb.flushWrites())

	shard := vb.ShardedWAL.Shards[3&ShardMask]
	require.True(t, shard.dirty.Load())
	require.NoError(t, shard.DB.Close()) // the next fsync on this shard fails
	err := vb.syncShardedWAL()
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("shard %d", 3&ShardMask))
	assert.True(t, shard.dirty.Load(), "a failed sync leaves the shard dirty")
}
