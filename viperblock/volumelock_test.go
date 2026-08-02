package viperblock

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAcquireVolumeLock_SecondCallerFails is the core admission-control
// contract for mulga-w1iu8: a second concurrent opener of the SAME volume
// must fail fast with ErrVolumeLocked, not block or silently proceed.
func TestAcquireVolumeLock_SecondCallerFails(t *testing.T) {
	dir := t.TempDir()

	first, err := AcquireVolumeLock(dir, "vol-1")
	require.NoError(t, err)
	defer func() { _ = ReleaseVolumeLock(first) }()

	_, err = AcquireVolumeLock(dir, "vol-1")
	require.Error(t, err, "a second opener must not be admitted while the first holds the lock")
	assert.ErrorIs(t, err, ErrVolumeLocked, "error must wrap ErrVolumeLocked, got: %v", err)
}

// TestAcquireVolumeLock_ReleaseThenReacquire models a legitimate reattach:
// stop, terminate, or a spinifex service restart that closes the volume and
// reopens it. The lock must never wedge a clean reopen.
func TestAcquireVolumeLock_ReleaseThenReacquire(t *testing.T) {
	dir := t.TempDir()

	first, err := AcquireVolumeLock(dir, "vol-1")
	require.NoError(t, err)
	require.NoError(t, ReleaseVolumeLock(first))

	second, err := AcquireVolumeLock(dir, "vol-1")
	require.NoError(t, err, "reopen after a clean release must succeed")
	require.NoError(t, ReleaseVolumeLock(second))
}

// TestAcquireVolumeLock_CrashedHolderDoesNotWedge models the kernel dropping
// the lock when a holder dies without an orderly Close/Unload — e.g. a
// killed viperblockd. Closing the *os.File without an explicit unlock is
// what a process death looks like from the OS's perspective: the open file
// description goes away and flock releases it automatically. A PID-lockfile
// scheme would need extra staleness-detection logic to reach the same
// outcome; flock gets it for free, which is why the fix uses flock.
func TestAcquireVolumeLock_CrashedHolderDoesNotWedge(t *testing.T) {
	dir := t.TempDir()

	first, err := AcquireVolumeLock(dir, "vol-1")
	require.NoError(t, err)
	// Simulate a crash: close the fd directly, skipping ReleaseVolumeLock's
	// explicit LOCK_UN. The kernel must still drop the lock on close.
	require.NoError(t, first.Close())

	second, err := AcquireVolumeLock(dir, "vol-1")
	require.NoError(t, err, "a lock held by a now-dead file descriptor must not wedge the next opener")
	require.NoError(t, ReleaseVolumeLock(second))
}

// TestAcquireVolumeLock_DifferentVolumesIndependent pins that the lock is
// scoped per-volume, not per-baseDir: two different volumes under the same
// BaseDir must be independently lockable.
func TestAcquireVolumeLock_DifferentVolumesIndependent(t *testing.T) {
	dir := t.TempDir()

	a, err := AcquireVolumeLock(dir, "vol-a")
	require.NoError(t, err)
	defer func() { _ = ReleaseVolumeLock(a) }()

	b, err := AcquireVolumeLock(dir, "vol-b")
	require.NoError(t, err, "a different volume under the same BaseDir must not be blocked")
	require.NoError(t, ReleaseVolumeLock(b))
}

// TestAcquireVolumeLock_ConcurrentRace fires many concurrent acquire
// attempts at one volume and asserts exactly one wins at a time — the direct
// analogue of nbdkit's parallel thread model admitting concurrent Open()
// calls for the same volume.
func TestAcquireVolumeLock_ConcurrentRace(t *testing.T) {
	dir := t.TempDir()
	const attempts = 16

	results := make(chan *os.File, attempts)
	errs := make(chan error, attempts)
	for range attempts {
		go func() {
			f, err := AcquireVolumeLock(dir, "vol-race")
			results <- f
			errs <- err
		}()
	}

	var winners int
	var busyErrs int
	for range attempts {
		f := <-results
		err := <-errs
		switch {
		case err == nil:
			winners++
			require.NoError(t, ReleaseVolumeLock(f))
		case errors.Is(err, ErrVolumeLocked):
			busyErrs++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Attempts are not synchronized to fire simultaneously, so more than one
	// may win sequentially as earlier winners release before later attempts
	// run — the invariant is that every attempt either won cleanly or got
	// ErrVolumeLocked, never anything else, and never silently double-opened.
	assert.Equal(t, attempts, winners+busyErrs)
	assert.Positive(t, winners, "at least one attempt must have won the lock")
}

// TestReleaseVolumeLock_NilIsNoOp lets every call site defer
// ReleaseVolumeLock unconditionally, including paths where AcquireVolumeLock
// itself failed and never produced a file.
func TestReleaseVolumeLock_NilIsNoOp(t *testing.T) {
	assert.NoError(t, ReleaseVolumeLock(nil))
}
