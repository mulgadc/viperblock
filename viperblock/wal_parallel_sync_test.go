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
// in flight: the leader's sync has cleared the flag and is mid-fsync.
func TestShardedFlushWaitsForAnInFlightShardSync(t *testing.T) {
	vb := setupShardedSyncVB(t)
	data := make([]byte, vb.BlockSize)
	_, _ = rand.Read(data)
	require.NoError(t, vb.Write(0, data))
	require.NoError(t, vb.flushWrites())

	shard := vb.ShardedWAL.Shards[0]
	shard.mu.Lock() // the leader sync stops after clearing dirty, before fsync
	leader := make(chan error, 1)
	go func() { leader <- shard.sync() }()
	require.Eventually(t, func() bool { return !shard.dirty.Load() }, 5*time.Second, time.Millisecond)

	follower := make(chan error, 1)
	go func() { follower <- vb.Flush() }()
	select {
	case err := <-follower:
		t.Fatalf("Flush returned %v while the leader's sync was in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	shard.mu.Unlock()
	require.NoError(t, <-leader)
	require.NoError(t, <-follower)
}

// Two failing shards and one healthy dirty shard: the flush returns the lower
// shard's error, the failed shards stay dirty and the healthy one is synced.
func TestShardedFlushReturnsTheFirstShardSyncError(t *testing.T) {
	vb := setupShardedSyncVB(t)
	data := make([]byte, vb.BlockSize)
	_, _ = rand.Read(data)
	for _, b := range []uint64{2, 5, 9} {
		require.NoError(t, vb.Write(b, data))
	}
	require.NoError(t, vb.flushWrites())

	sh := vb.ShardedWAL.Shards
	require.NoError(t, sh[5&ShardMask].DB.Close())
	require.NoError(t, sh[9&ShardMask].DB.Close())
	err := vb.Flush()
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("shard %d", 5&ShardMask))
	assert.True(t, sh[5&ShardMask].dirty.Load())
	assert.True(t, sh[9&ShardMask].dirty.Load())
	assert.False(t, sh[2&ShardMask].dirty.Load(), "the healthy shard finished its sync")
}
