package viperblock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCancelledWriteDoesNotRecordADrainStall pins that a caller giving up on
// its own write is not recorded as the backend failing. awaitBackpressure
// drives the drain on the caller's context, so a cancellation surfaces as a
// drain error -- but the backend never said anything was wrong, and the stall
// deadline it would be recorded against is shared by every writer on the
// volume.
func TestCancelledWriteDoesNotRecordADrainStall(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	// Force the backpressure gate to engage on the next write.
	vb.MaxPendingBytes = blockSize
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	// The backend now neither succeeds nor fails: it waits for the caller's
	// context, which is what a slow-but-healthy backend looks like from here.
	backend.blockUntilCancelled.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() { done <- vb.WriteAtCtx(ctx, blockSize, make([]byte, blockSize)) }()

	var err error
	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("WriteAtCtx did not return after its context was cancelled")
	}

	require.ErrorIs(t, err, context.Canceled,
		"a cancelled write must surface the cancellation, not a backend drain failure")

	stalled, lastErr := vb.drainStall.elapsed()
	assert.Zero(t, stalled, "a cancelled caller must not start the volume's shared stall deadline")
	assert.NoError(t, lastErr, "a cancelled caller must not be recorded as the backend's last failure")
}

// TestCancelledWriteDoesNotStallLaterWriters pins the consequence of the above:
// because drainStall is volume-scoped, one abandoned request recording itself
// there would abort every later writer against a backend that is working.
func TestCancelledWriteDoesNotStallLaterWriters(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	vb.MaxPendingBytes = blockSize
	// Short enough that any deadline the cancelled write below started has
	// certainly expired by the time the surviving write checks it.
	vb.BackpressureStallTimeout = time.Millisecond
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	backend.blockUntilCancelled.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	abandoned := make(chan error, 1)
	go func() { abandoned <- vb.WriteAtCtx(ctx, blockSize, make([]byte, blockSize)) }()

	select {
	case err := <-abandoned:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(30 * time.Second):
		t.Fatal("WriteAtCtx did not return after its context was cancelled")
	}

	// The backend was never unhealthy, so the next writer must be able to drain.
	backend.blockUntilCancelled.Store(false)
	time.Sleep(10 * time.Millisecond)

	err := runBlockingWriteWithHangTimeout(t, vb, 2*blockSize, make([]byte, blockSize), 30*time.Second)
	assert.NoError(t, err,
		"a write against a healthy backend must not inherit a stall deadline started by a cancelled caller")

	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
}
