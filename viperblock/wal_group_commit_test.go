package viperblock

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGroupCommitBatchesConcurrentFlushes is the point of the whole mechanism:
// flushes that arrive while an fsync is in flight join it instead of each
// paying for one of their own. Without batching this costs 8 fsyncs.
func TestGroupCommitBatchesConcurrentFlushes(t *testing.T) {
	var c walCommit
	var syncs atomic.Int64

	// Wide enough that every waiter is parked before the first sync returns.
	fsync := func() error {
		syncs.Add(1)
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	const waiters = 8
	for range waiters {
		c.appended()
	}

	var wg sync.WaitGroup
	for range waiters {
		wg.Go(func() {
			assert.NoError(t, c.await(fsync))
		})
	}
	wg.Wait()

	assert.Less(t, syncs.Load(), int64(waiters),
		"concurrent flushes must share an fsync, not queue one each")
	assert.False(t, c.pending(), "every waiter's record must be durable on return")
}

// TestGroupCommitWaitsForAnFsyncThatCoversTheCaller is the durability half,
// and the bug the dirty flag had: a record appended after an fsync started is
// not covered by it, so its flush must not return on the back of it.
func TestGroupCommitWaitsForAnFsyncThatCoversTheCaller(t *testing.T) {
	var c walCommit

	started := make(chan struct{})
	release := make(chan struct{})
	var syncs atomic.Int64

	fsync := func() error {
		if syncs.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}

	// One record, and a flush that claims the fsync and parks inside it.
	c.appended()
	first := make(chan struct{})
	go func() {
		defer close(first)
		assert.NoError(t, c.await(fsync))
	}()
	<-started

	// A second record lands while that fsync is in flight, so it is not on
	// disk when the fsync completes.
	c.appended()
	second := make(chan struct{})
	go func() {
		defer close(second)
		assert.NoError(t, c.await(fsync))
	}()

	// The second flush must still be blocked: the only fsync so far began
	// before its record existed.
	select {
	case <-second:
		t.Fatal("flush returned on an fsync that began before its record was appended")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	<-first
	<-second

	assert.Equal(t, int64(2), syncs.Load(), "the second record needs an fsync of its own")
	assert.False(t, c.pending())
}

// TestGroupCommitReportsFsyncFailureToTheWholeBatch pins that a failed fsync
// fails every flush it was covering, and leaves their records outstanding for
// the next attempt rather than silently marking them durable.
func TestGroupCommitReportsFsyncFailureToTheWholeBatch(t *testing.T) {
	var c walCommit

	started := make(chan struct{})
	release := make(chan struct{})
	var syncs atomic.Int64

	fsync := func() error {
		if syncs.Add(1) == 1 {
			close(started)
			<-release
		}
		return assert.AnError
	}

	for range 4 {
		c.appended()
	}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range 4 {
		wg.Go(func() {
			errs[i] = c.await(fsync)
		})
	}
	<-started
	// Give the other three time to park on the in-flight batch.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, err := range errs {
		assert.ErrorIs(t, err, assert.AnError, "waiter %d must see the fsync failure", i)
	}
	assert.True(t, c.pending(), "a failed fsync must not advance the durable point")
}

// TestGroupCommitIdleCostsNoFsync pins the fast path the dirty flag used to
// provide: nothing appended means nothing to sync.
func TestGroupCommitIdleCostsNoFsync(t *testing.T) {
	var c walCommit
	var syncs atomic.Int64

	fsync := func() error {
		syncs.Add(1)
		return nil
	}

	require.NoError(t, c.await(fsync))
	require.NoError(t, c.await(fsync))
	assert.Zero(t, syncs.Load(), "an idle WAL must not be fsynced")

	c.appended()
	require.NoError(t, c.await(fsync))
	require.NoError(t, c.await(fsync))
	assert.Equal(t, int64(1), syncs.Load(), "a synced WAL must not be fsynced again")
}

// TestWALRotationSettlesTheCommitDebt pins that the fsync WriteWALToChunkCtx
// performs before closing a generation counts, so the next flush does not
// fsync the empty successor to satisfy a debt already paid.
func TestWALRotationSettlesTheCommitDebt(t *testing.T) {
	var c walCommit
	var syncs atomic.Int64

	fsync := func() error {
		syncs.Add(1)
		return nil
	}

	c.appended()
	c.appended()
	c.rotated()

	assert.False(t, c.pending(), "rotation fsynced those records")
	require.NoError(t, c.await(fsync))
	assert.Zero(t, syncs.Load(), "no fsync should be needed after a rotation")
}

// TestFlushIsDurableUnderConcurrentWriters drives the real Flush path with
// concurrent writers and asserts the invariant that matters: when Flush
// returns, everything it appended is fsynced.
func TestFlushIsDurableUnderConcurrentWriters(t *testing.T) {
	vb, _ := newEnospcTestVB(t)

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 20 {
				block := uint64(w*20 + i)
				assert.NoError(t, vb.WriteAt(block*uint64(vb.BlockSize), make([]byte, vb.BlockSize)))
				assert.NoError(t, vb.Flush())
			}
		})
	}
	wg.Wait()

	assert.False(t, vb.WAL.commit.pending(), "no record may be left unsynced once every Flush has returned")
}
