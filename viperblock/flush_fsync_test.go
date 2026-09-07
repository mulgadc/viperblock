package viperblock

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFlushSyncsWAL pins the durability half of the NBD flush contract: a
// successful Flush must leave the WAL fsynced, not merely written into the
// page cache with the periodic syncer still to run.
func TestFlushSyncsWAL(t *testing.T) {
	vb, _ := newEnospcTestVB(t)

	// The write only reaches the WAL during the flush, so nothing outstanding
	// afterwards means the flush also synced what it appended.
	require.NoError(t, vb.WriteAt(0, make([]byte, vb.BlockSize)))
	require.NoError(t, vb.Flush())

	assert.False(t, vb.WAL.commit.pending(), "Flush must fsync the WAL, not leave it for the syncer tick")
}

// TestFlushReturnsSyncFailure pins that a failed fsync fails the barrier. A
// swallowed sync error would report success on a WAL that never reached
// stable storage, which is the same lie in a narrower window.
func TestFlushReturnsSyncFailure(t *testing.T) {
	vb, _ := newEnospcTestVB(t)

	// No buffered writes, so flushWrites is a no-op and the only thing that
	// can fail is the fsync -- a flush-write failure cannot be mistaken for it.
	require.NoError(t, vb.Flush())

	require.NotEmpty(t, vb.WAL.DB)
	require.NoError(t, vb.WAL.DB[len(vb.WAL.DB)-1].Close())
	vb.WAL.commit.appended()

	err := vb.Flush()
	require.Error(t, err, "Flush must report a failed fsync rather than swallowing it")
	assert.Contains(t, err.Error(), "WAL sync")
	assert.True(t, vb.WAL.commit.pending(), "a failed sync must leave the record outstanding so the syncer retries")
}

// TestSyncWALBackgroundSwallowsFailure pins that the periodic syncer keeps the
// old behaviour: one bad fsync logs and leaves the record outstanding rather
// than propagating out of a background tick.
func TestSyncWALBackgroundSwallowsFailure(t *testing.T) {
	vb, _ := newEnospcTestVB(t)

	require.NotEmpty(t, vb.WAL.DB)
	require.NoError(t, vb.WAL.DB[len(vb.WAL.DB)-1].Close())
	vb.WAL.commit.appended()

	vb.syncWALBackground()
	assert.True(t, vb.WAL.commit.pending(), "the syncer must leave the record outstanding so the next tick retries")
}

// TestFlushSyncsShardedWAL is TestFlushSyncsWAL for the sharded WAL, which
// carries its own per-shard dirty flags and sync path.
func TestFlushSyncsShardedWAL(t *testing.T) {
	vb := newShardedFlushTestVB(t)

	// As in the unsharded case, the write reaches its shard during the flush,
	// so clear flags afterwards mean the flush synced every shard it wrote.
	require.NoError(t, vb.WriteAt(0, make([]byte, vb.BlockSize)))
	require.NoError(t, vb.Flush())

	for i := range NumShards {
		assert.False(t, vb.ShardedWAL.Shards[i].dirty.Load(), "Flush must fsync shard %d", i)
	}
}

// TestFlushReturnsShardedSyncFailure is TestFlushReturnsSyncFailure for the
// sharded WAL.
func TestFlushReturnsShardedSyncFailure(t *testing.T) {
	vb := newShardedFlushTestVB(t)

	require.NoError(t, vb.Flush())

	shard := vb.ShardedWAL.Shards[0]
	require.NotNil(t, shard.DB)
	require.NoError(t, shard.DB.Close())
	shard.dirty.Store(true)

	err := vb.Flush()
	require.Error(t, err, "Flush must report a failed shard fsync")
	assert.Contains(t, err.Error(), "sharded WAL sync")
	assert.True(t, shard.dirty.Load(), "a failed shard sync must stay dirty for the next tick")
}

// newShardedFlushTestVB builds the enospc fixture over the sharded WAL, which
// the fixture otherwise disables.
func newShardedFlushTestVB(t *testing.T) *VB {
	t.Helper()

	vb, _ := newEnospcTestVB(t)
	vb.UseShardedWAL = true
	vb.ShardedWAL = NewShardedWAL(vb.WAL.BaseDir, vb.WAL.WALMagic)
	require.NoError(t, vb.OpenShardedWAL())

	return vb
}
