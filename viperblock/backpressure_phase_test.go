package viperblock

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cancelOnDone is a real cancellable context that cancels itself the first
// time a caller asks for its Done channel, so a wait is cancelled exactly as
// it enters the select, with no sleep.
type cancelOnDone struct {
	context.Context

	cancel context.CancelFunc
	once   sync.Once
}

func newCancelOnDone(t *testing.T) *cancelOnDone {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &cancelOnDone{Context: ctx, cancel: cancel}
}

func (c *cancelOnDone) Done() <-chan struct{} {
	c.once.Do(c.cancel)
	return c.Context.Done()
}

// capturePhases replaces the phase recorder for the test and returns a
// function reporting the phases recorded so far, in order.
func capturePhases(t *testing.T, onPhase func(phase string)) func() []string {
	var mu sync.Mutex
	var phases []string
	prev := recordBackpressurePhase
	recordBackpressurePhase = func(_ context.Context, _, phase string, _ time.Duration, _ int64) {
		mu.Lock()
		phases = append(phases, phase)
		mu.Unlock()
		if onPhase != nil {
			onPhase(phase)
		}
	}
	t.Cleanup(func() { recordBackpressurePhase = prev })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), phases...)
	}
}

// A wait cancelled while polling on another writer's drain still records its
// poll phase, so the phases of a wait account for the whole of it.
func TestBackpressurePollPhaseRecordedOnCancel(t *testing.T) {
	vb := newGCTestVB(t, t.TempDir(), "vol-bp-cancel", false)
	phases := capturePhases(t, nil)

	vb.pendingBytes.Store(1 << 50)
	vb.drainInFlight.Store(true)
	t.Cleanup(func() { vb.pendingBytes.Store(0); vb.drainInFlight.Store(false) })

	err := vb.awaitBackpressure(newCancelOnDone(t))
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []string{"poll"}, phases())
}

// A lone writer whose own drain leaves the volume above the low watermark
// polls before retrying, and that sleep is recorded as poll too.
func TestBackpressurePollPhaseFollowsTheWritersOwnDrain(t *testing.T) {
	vb := newGCTestVB(t, t.TempDir(), "vol-bp-own", false)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// Cancelled once the drain is recorded, so the following poll is the
	// one that observes it.
	phases := capturePhases(t, func(phase string) {
		if phase == "drain" {
			cancel()
		}
	})

	vb.pendingBytes.Store(1 << 50)
	t.Cleanup(func() { vb.pendingBytes.Store(0) })

	err := vb.awaitBackpressure(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []string{"drain", "poll"}, phases())
}

type walOpSample struct{ phase, volume, outcome string }

// A map commit records one checkpoint sample per batch it writes, labelled by
// outcome, and none for a clean map.
func TestMapCommitRecordsItsOutcome(t *testing.T) {
	vb := newGCTestVB(t, t.TempDir(), "vol-ckpt-timer", false)
	ctx := context.Background()

	var mu sync.Mutex
	var samples []walOpSample
	prev := recordWALOp
	recordWALOp = func(_ context.Context, phase, volume, outcome string, _ time.Duration) {
		if phase != "checkpoint" {
			return
		}
		mu.Lock()
		samples = append(samples, walOpSample{phase, volume, outcome})
		mu.Unlock()
	}
	t.Cleanup(func() { recordWALOp = prev })
	got := func() []walOpSample {
		mu.Lock()
		defer mu.Unlock()
		return append([]walOpSample(nil), samples...)
	}

	installBlock := func(block uint64) {
		t.Helper()
		require.NoError(t, vb.Write(block, randomBlockData(1)))
		require.NoError(t, vb.Flush())
		require.NoError(t, vb.WriteWALToChunk(true))
	}

	require.NoError(t, vb.persistMapCtx(ctx))
	assert.Empty(t, got(), "a clean map is not a checkpoint")

	installBlock(0)
	require.NoError(t, vb.persistMapCtx(ctx))
	assert.Equal(t, []walOpSample{{"checkpoint", "vol-ckpt-timer", "success"}}, got())

	fb, ok := vb.Backend.(*file.Backend)
	require.True(t, ok)
	failing := &mapFaultBackend{Backend: fb}
	failing.failHead.Store(true)
	vb.Backend = failing
	t.Cleanup(func() { vb.Backend = fb })
	vb.checkpointRetryBackoff = time.Microsecond

	installBlock(1)
	require.Error(t, vb.persistMapCtx(ctx))
	assert.Equal(t, []walOpSample{
		{"checkpoint", "vol-ckpt-timer", "success"},
		{"checkpoint", "vol-ckpt-timer", "error"},
	}, got())
}
