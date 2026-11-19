package viperblock

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ctxCancelledInPoll reports no error on its first Err call and a closed Done
// thereafter, so a wait reaches its poll and is cancelled there, with no sleep.
type ctxCancelledInPoll struct {
	context.Context

	done  chan struct{}
	calls atomic.Int32
}

func (c *ctxCancelledInPoll) Done() <-chan struct{} { return c.done }

func (c *ctxCancelledInPoll) Err() error {
	if c.calls.Add(1) == 1 {
		return nil
	}
	return context.Canceled
}

// A wait cancelled while polling still records its poll phase, so the phases
// of a wait account for the whole of it.
func TestBackpressurePollPhaseRecordedOnCancel(t *testing.T) {
	vb := newGCTestVB(t, t.TempDir(), "vol-bp-cancel", false)

	var mu sync.Mutex
	var phases []string
	prev := recordBackpressurePhase
	recordBackpressurePhase = func(_ context.Context, _, phase string, _ time.Duration, _ int64) {
		mu.Lock()
		phases = append(phases, phase)
		mu.Unlock()
	}
	t.Cleanup(func() { recordBackpressurePhase = prev })

	vb.pendingBytes.Store(1 << 50)
	vb.drainInFlight.Store(true)
	t.Cleanup(func() { vb.pendingBytes.Store(0); vb.drainInFlight.Store(false) })

	done := make(chan struct{})
	close(done)
	ctx := &ctxCancelledInPoll{Context: context.Background(), done: done}

	err := vb.awaitBackpressure(ctx)
	require.ErrorIs(t, err, context.Canceled)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"poll"}, phases)
}
