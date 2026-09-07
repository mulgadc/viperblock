package viperblock

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSyncWALDoesNotReportDurableDuringAnInFlightFsync pins the lost-write
// window. The dirty flag is cleared before the fsync, so a caller whose record
// was appended beforehand could read the cleared flag mid-flight and return --
// reporting the guest's write durable while the only fsync covering it was
// still running.
func TestSyncWALDoesNotReportDurableDuringAnInFlightFsync(t *testing.T) {
	vb, _ := newEnospcTestVB(t)

	// Both callers' records are in the WAL before either sync starts, so one
	// fsync covers both and the second is entitled to return once it ends --
	// but not before.
	require.NoError(t, vb.WriteAt(0, make([]byte, vb.BlockSize)))
	require.NoError(t, vb.flushWrites())
	require.True(t, vb.WAL.dirty.Load(), "the append must mark the WAL dirty")

	leaderOut := make(chan struct{})
	go func() {
		assert.NoError(t, vb.syncWAL())
		close(leaderOut)
	}()

	// Start the follower only once the leader has cleared the flag, which is
	// the exact window under test. Bounded so a leader that never runs fails
	// as a timeout rather than hanging.
	deadline := time.Now().Add(2 * time.Second)
	for vb.WAL.dirty.Load() {
		if time.Now().After(deadline) {
			t.Fatal("leader never entered its fsync")
		}
	}

	followerOut := make(chan struct{})
	go func() {
		assert.NoError(t, vb.syncWAL())
		close(followerOut)
	}()

	select {
	case <-followerOut:
		select {
		case <-leaderOut:
			// Leader finished first, so the follower's return is covered.
		default:
			t.Fatal("syncWAL returned while the fsync covering its record was still in flight")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("syncWAL deadlocked")
	}

	<-leaderOut
	assert.False(t, vb.WAL.dirty.Load(), "a completed sync must leave the WAL clean")
}

// TestSyncWALResyncsForAWriteThatArrivedDuringAnFsync pins the other half. The
// flag is cleared before the fsync, so a write landing during it re-marks the
// WAL and the next caller must sync again rather than trust the one that was
// already running.
func TestSyncWALResyncsForAWriteThatArrivedDuringAnFsync(t *testing.T) {
	vb, _ := newEnospcTestVB(t)

	require.NoError(t, vb.WriteAt(0, make([]byte, vb.BlockSize)))
	require.NoError(t, vb.flushWrites())
	require.NoError(t, vb.syncWAL())
	require.False(t, vb.WAL.dirty.Load())

	// A record appended after that sync must not be considered covered by it.
	require.NoError(t, vb.WriteAt(uint64(vb.BlockSize), make([]byte, vb.BlockSize)))
	require.NoError(t, vb.flushWrites())
	assert.True(t, vb.WAL.dirty.Load(), "a write after a sync must leave the WAL dirty")

	require.NoError(t, vb.syncWAL())
	assert.False(t, vb.WAL.dirty.Load())
}

// TestShardedWALSyncIsSafeUnderConcurrentCallers pins the same protocol on the
// sharded path, which carries one dirty flag per shard and had the identical
// defect.
func TestShardedWALSyncIsSafeUnderConcurrentCallers(t *testing.T) {
	vb := newShardedFlushTestVB(t)

	require.NoError(t, vb.WriteAt(0, make([]byte, vb.BlockSize)))
	require.NoError(t, vb.flushWrites())

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { assert.NoError(t, vb.syncShardedWAL()) })
	}
	wg.Wait()

	for i := range NumShards {
		assert.False(t, vb.ShardedWAL.Shards[i].dirty.Load(), "shard %d must be clean after sync", i)
	}
}
