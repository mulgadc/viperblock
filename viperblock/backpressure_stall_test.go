package viperblock

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mulgadc/viperblock/types"
)

// The first failure of a run starts the clock rather than reporting elapsed
// time, so one failure can never trip a deadline on its own.
func TestDrainStallFirstFailureReportsZero(t *testing.T) {
	var s drainStall
	assert.Zero(t, s.fail(errors.New("boom")), "the first failure starts the run, it does not measure it")

	time.Sleep(20 * time.Millisecond)
	elapsed, lastErr := s.elapsed()
	assert.Positive(t, elapsed, "a run in progress must report how long it has lasted")
	assert.EqualError(t, lastErr, "boom", "the run must carry its last cause for the error the writer returns")
}

// A drain that succeeds means the backend is answering, so the run ends and a
// later failure starts a fresh deadline rather than inheriting the old one.
func TestDrainStallSuccessEndsTheRun(t *testing.T) {
	var s drainStall
	s.fail(errors.New("first"))
	time.Sleep(30 * time.Millisecond)
	require.Positive(t, mustElapsed(t, &s))

	s.clear()
	elapsed, lastErr := s.elapsed()
	assert.Zero(t, elapsed, "a successful drain must end the run")
	assert.NoError(t, lastErr)

	assert.Zero(t, s.fail(errors.New("second")), "a failure after a success starts a new run, not a continuation")
}

// The bound is on the volume, so it holds however many writers are blocked.
// A per-writer counter divides the real tolerance by the number of writers,
// which is what made the old bound unreachable under concurrent load.
func TestDrainStallDeadlineIsSharedAcrossWriters(t *testing.T) {
	var s drainStall
	const writers = 16

	s.fail(errors.New("backend gone"))
	time.Sleep(30 * time.Millisecond)

	var wg sync.WaitGroup
	seen := make([]time.Duration, writers)
	for i := range writers {
		wg.Go(func() {
			// Every writer charges its own failure, exactly as concurrent
			// callers of awaitBackpressure do.
			s.fail(errors.New("backend gone"))
			seen[i], _ = s.elapsed()
		})
	}
	wg.Wait()

	for i, d := range seen {
		assert.Positive(t, d, "writer %d must observe the run that started before it, not a fresh one", i)
	}
}

func mustElapsed(t *testing.T, s *drainStall) time.Duration {
	t.Helper()
	d, _ := s.elapsed()
	return d
}

// The old bound was a local in awaitBackpressure, so with N writers blocked the
// effective tolerance was N times the intended one and, because only one writer
// at a time wins the drain CAS, in practice it was never reached at all. This
// is the case a single-writer test cannot see.
func TestAwaitBackpressureDeadlineHoldsWithConcurrentWriters(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	vb.MaxPendingBytes = blockSize
	vb.BackpressureStallTimeout = 500 * time.Millisecond
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	// Every checkpoint write fails from here on, and pendingBytes is pinned
	// back above the low-watermark so no writer can escape by the gate
	// releasing rather than by the deadline.
	backend.genericFail.Store(true)
	backend.afterWrite = func(fileType types.FileType, _ error) {
		if fileType == types.FileTypeBlockCheckpointLive {
			vb.pendingBytes.Store(int64(vb.maxPendingBytes()) * 4)
		}
	}

	const writers = 8
	errs := make([]error, writers)
	var wg sync.WaitGroup
	start := time.Now()
	for i := range writers {
		wg.Go(func() {
			errs[i] = vb.WriteAt(uint64(i+1)*blockSize, make([]byte, blockSize))
		})
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("writers did not return: the bound is still per-writer, so concurrency defeats it")
	}
	elapsed := time.Since(start)

	for i, err := range errs {
		require.Error(t, err, "writer %d must be bounded by the volume's deadline", i)
		assert.Contains(t, err.Error(), "has not drained for",
			"writer %d must fail on the stall deadline, not some other error", i)
	}
	assert.Less(t, elapsed, 15*time.Second,
		"the deadline is on the volume, so %d writers must not take %d times as long to reach it", writers, writers)
}
