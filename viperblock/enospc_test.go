package viperblock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// enospcBackend wraps a real types.Backend and can be toggled to fail every
// write with a types.ErrNoSpace-wrapped error, simulating a backend that has
// rejected a write as out-of-space (predastore 507/503, or a full local
// disk) without needing to actually exhaust a filesystem in this test.
type enospcBackend struct {
	types.Backend

	full atomic.Bool

	// genericFail, when true, makes every FileTypeBlockCheckpointLive write
	// return a persistent error that is NOT types.ErrNoSpace -- a degraded
	// backend failing for a reason other than being out of space (a corrupt
	// mount, a transient disk I/O error, ...). Used to prove
	// awaitBackpressure bounds consecutive drain failures instead of
	// retrying forever: only ErrNoSpace previously short-circuited the
	// retry loop.
	//
	// Scoped to the checkpoint write rather than every write: a chunk write
	// failure orphans that WAL segment (WriteWALToChunkCtx unconditionally
	// rotates the WAL before the write, so a failed chunk is never retried
	// without an explicit recovery pass), which makes a persistently
	// failing chunk collapse into a single real failure followed by
	// perpetual no-op "successes" -- not what these tests want to probe. A
	// chunk write is left to succeed for real so BlocksToObject.dirty stays
	// true and SaveLiveCheckpointCtx keeps being attempted every drain
	// round, which is the realistic way a backend keeps rejecting writes
	// across many consecutive drains without a continuous stream of fresh
	// guest writes behind it.
	genericFail atomic.Bool

	// mu guards checkpointScript/checkpointScriptIdx below.
	mu sync.Mutex

	// checkpointScript, if non-empty, overrides genericFail for successive
	// FileTypeBlockCheckpointLive writes only: consumed in order, a nil
	// entry succeeds (delegates to the wrapped backend), a non-nil entry
	// fails with that error. Once exhausted, checkpoint writes fall back to
	// the genericFail toggle.
	checkpointScript    []error
	checkpointScriptIdx int

	// afterWrite, if set, runs after every Write/WriteCtx call (any
	// fileType) returns, once any production pendingBytes bookkeeping for
	// that call (e.g. createChunkFile's decrement on a successful chunk
	// write) has already happened. Tests use it to pin vb.pendingBytes
	// above the low-watermark for as long as they want an awaitBackpressure
	// retry loop to keep iterating, decoupling loop iteration count from
	// this fake's simplistic single-shot real chunk drain, and to re-mark
	// BlocksToObject.dirty after a scripted checkpoint success so
	// SaveLiveCheckpointCtx keeps being attempted on later drain rounds --
	// standing in for a concurrent guest write that would otherwise dirty
	// the checkpoint again in a real deployment.
	afterWrite func(fileType types.FileType, err error)
}

// enospcErr is what backends/s3 and backends/file actually return once
// classifyWriteErr recognizes a 507/503 or syscall.ENOSPC: the underlying
// cause wrapped alongside types.ErrNoSpace so errors.Is still finds it under
// whatever fmt.Errorf wrapping the drain path adds on top.
func enospcErr() error {
	return fmt.Errorf("%w: simulated backend-full write rejection", types.ErrNoSpace)
}

// genericBackendErr simulates a persistent backend failure that is NOT
// out-of-space, so errors.Is(err, ErrNoSpace) must be false for it — the
// class of error that used to make awaitBackpressure retry forever.
func genericBackendErr() error {
	return errors.New("simulated persistent backend I/O error (not out-of-space)")
}

// dispatch centralizes this fake's fail/succeed decision so Write and
// WriteCtx (the sync and ctx-aware halves of types.Backend) behave
// identically. doWrite performs (or is skipped in place of) the actual
// delegating write.
func (b *enospcBackend) dispatch(fileType types.FileType, doWrite func() error) error {
	if b.full.Load() {
		return enospcErr()
	}

	if fileType == types.FileTypeBlockCheckpointLive {
		b.mu.Lock()
		var scriptedErr error
		scripted := b.checkpointScriptIdx < len(b.checkpointScript)
		if scripted {
			scriptedErr = b.checkpointScript[b.checkpointScriptIdx]
			b.checkpointScriptIdx++
		}
		b.mu.Unlock()

		if scripted {
			if scriptedErr != nil {
				return scriptedErr
			}
			return doWrite()
		}
		if b.genericFail.Load() {
			return genericBackendErr()
		}
	}

	return doWrite()
}

func (b *enospcBackend) Write(fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	err := b.dispatch(fileType, func() error { return b.Backend.Write(fileType, objectId, headers, data) })
	if b.afterWrite != nil {
		b.afterWrite(fileType, err)
	}
	return err
}

func (b *enospcBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	err := b.dispatch(fileType, func() error { return b.Backend.WriteCtx(ctx, fileType, objectId, headers, data) })
	if b.afterWrite != nil {
		b.afterWrite(fileType, err)
	}
	return err
}

// newEnospcTestVB builds a minimal file-backed VB (legacy single-file WAL,
// no LRU cache, periodic syncers disabled for deterministic behavior) with
// its Backend wrapped in enospcBackend, so tests can toggle backend-full
// behavior on and off without touching an actual filesystem's free space.
func newEnospcTestVB(t *testing.T) (*VB, *enospcBackend) {
	t.Helper()

	tmpDir := t.TempDir()
	testVol := fmt.Sprintf("test_enospc_%d", time.Now().UnixNano())

	backendConfig := file.FileConfig{
		VolumeName: testVol,
		VolumeSize: 64 * 1024 * 1024,
		BaseDir:    tmpDir,
	}

	vbconfig := VB{
		VolumeName: testVol,
		VolumeSize: 64 * 1024 * 1024,
		BaseDir:    fmt.Sprintf("%s/%s", tmpDir, "viperblock"),
		// Deterministic: no background WAL fsync or ticker-driven chunk
		// upload racing the test; DrainToBackendCtx is driven explicitly.
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}

	vb, err := New(&vbconfig, FileBackend, backendConfig)
	require.NoError(t, err)
	require.NotNil(t, vb)

	t.Cleanup(func() {
		assert.NoError(t, vb.RemoveLocalFiles())
	})

	vb.UseShardedWAL = false
	vb.ShardedWAL = nil

	require.NoError(t, vb.Backend.Init())
	backend := &enospcBackend{Backend: vb.Backend}
	vb.Backend = backend

	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	return vb, backend
}

// TestDrainLatchesBackendFullAndWriteFailsFast is the core regression test
// for this backpressure mechanism: a drain that hits a backend-full error
// must latch backendFull, and a subsequent WriteAtCtx must fail fast with
// ErrNoSpace instead of buffering the write and blocking forever in
// awaitBackpressure.
func TestDrainLatchesBackendFullAndWriteFailsFast(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	// Buffer one write so the drain below actually has a WAL segment to
	// upload — DrainToBackendCtx is a no-op (and cannot latch anything) when
	// there is nothing pending.
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	backend.full.Store(true)

	err := vb.DrainToBackendCtx(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoSpace, "drain error must classify as ErrNoSpace")
	assert.True(t, vb.backendFull.Load(), "backendFull latch must be set after a backend-full drain error")

	pendingBefore := vb.PendingBytes()
	err = vb.WriteAt(blockSize, make([]byte, blockSize))
	assert.ErrorIs(t, err, ErrNoSpace, "WriteAtCtx must fail fast with ErrNoSpace while backendFull is latched")
	assert.Equal(t, pendingBefore, vb.PendingBytes(),
		"a fast-failed write must not have buffered anything into Writes.Blocks or bumped pendingBytes")
}

// TestBackendFullLatchClearsAfterSuccessfulDrain proves the latch is not
// sticky forever: once the backend recovers (predastore frees space, or the
// full local disk gets cleaned up), the next successful drain must clear
// backendFull, and writes must succeed again.
func TestBackendFullLatchClearsAfterSuccessfulDrain(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	backend.full.Store(true)
	err := vb.DrainToBackendCtx(context.Background())
	require.Error(t, err)
	require.True(t, vb.backendFull.Load(), "latch must be set before we can test it clearing")

	// Backend recovers. Drain directly (bypassing WriteAtCtx's gate, which
	// would otherwise refuse to buffer anything new while the latch is
	// still set) so a clean drain can observe/clear the latch.
	backend.full.Store(false)
	err = vb.DrainToBackendCtx(context.Background())
	require.NoError(t, err, "drain against a recovered backend must succeed")
	assert.False(t, vb.backendFull.Load(), "backendFull latch must clear after a successful drain")

	// Writes must succeed again now that the latch is clear.
	err = vb.WriteAt(blockSize, make([]byte, blockSize))
	assert.NoError(t, err, "writes must succeed once the backend-full latch has cleared")

	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
}

// TestAwaitBackpressureStopsRetryingOnNoSpace proves the fix for the
// "thrash forever" bug: once a backend-full error is observed while blocked
// in the backpressure gate, awaitBackpressure must return ErrNoSpace
// immediately instead of backing off and retrying against a backend that
// has already said it is full.
func TestAwaitBackpressureStopsRetryingOnNoSpace(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	// Force the backpressure gate to engage on the very next write.
	vb.MaxPendingBytes = blockSize

	// First write fills up to the watermark without crossing it -- must not
	// touch the backend at all, so leave it in place before flipping full.
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	backend.full.Store(true)

	start := time.Now()
	err := vb.WriteAt(blockSize, make([]byte, blockSize))
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoSpace, "a write that trips awaitBackpressure against a full backend must surface ErrNoSpace")
	assert.True(t, vb.backendFull.Load(), "backendFull latch must be set by the drain awaitBackpressure drove")
	// A retrying implementation would have spent at least one backoff
	// interval (10ms, doubling) before giving up; a fixed implementation
	// returns as soon as the first drain attempt reports ErrNoSpace.
	assert.Less(t, elapsed, 200*time.Millisecond,
		"awaitBackpressure must stop on the first ErrNoSpace instead of backing off and retrying")
}

// TestWriteAtCtxGateIsLoadBearing directly performs the buffering half of
// WriteAtCtx (append to Writes.Blocks, bump pendingBytes) that runs
// immediately after the up-front backendFull check, to prove that check is
// load-bearing: without it, a write against a full backend would buffer
// into memory and grow pendingBytes instead of failing before touching
// either — this is the "fail without" counter-test the up-front gate exists
// to fix (see TestDrainLatchesBackendFullAndWriteFailsFast for the gate
// itself).
func TestWriteAtCtxGateIsLoadBearing(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))
	backend.full.Store(true)
	require.Error(t, vb.DrainToBackendCtx(context.Background()))
	require.True(t, vb.backendFull.Load())

	pendingBefore := vb.PendingBytes()

	// Simulate "no up-front gate" by calling the buffering half of
	// WriteAtCtx directly: append to Writes.Blocks and bump pendingBytes,
	// exactly as WriteAtCtx does immediately after its up-front check. If
	// that check did not exist, this is what every subsequent write would
	// do while the backend is full — proving the gate is load-bearing for
	// "the disk stops growing".
	vb.Writes.mu.Lock()
	vb.Writes.Blocks = append(vb.Writes.Blocks, Block{SeqNum: 1, Block: 999, Len: uint64(vb.BlockSize), Data: make([]byte, blockSize)})
	vb.Writes.mu.Unlock()
	vb.pendingBytes.Add(int64(blockSize))

	assert.Greater(t, vb.PendingBytes(), pendingBefore,
		"without the up-front gate, a write against a full backend would buffer and grow pendingBytes unbounded")

	// Reset back to a clean state for t.Cleanup's drain/RemoveLocalFiles.
	vb.Writes.mu.Lock()
	vb.Writes.Blocks = vb.Writes.Blocks[:len(vb.Writes.Blocks)-1]
	vb.Writes.mu.Unlock()
	vb.pendingBytes.Add(-int64(blockSize))
	backend.full.Store(false)
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
}

// TestErrNoSpaceIsTypesErrNoSpace is a lightweight guard on the plumbing
// errors.Is relies on: ErrNoSpace re-exports types.ErrNoSpace (the sentinel
// backends/file and backends/s3 actually return), and DrainToBackendCtx's
// wrapping ("drain chunk upload: %w") must not break errors.Is against
// either name.
func TestErrNoSpaceIsTypesErrNoSpace(t *testing.T) {
	assert.ErrorIs(t, ErrNoSpace, types.ErrNoSpace)
	wrapped := fmt.Errorf("drain chunk upload: %w", enospcErr())
	assert.ErrorIs(t, wrapped, ErrNoSpace)
	assert.ErrorIs(t, wrapped, types.ErrNoSpace)
}

// runBlockingWriteWithHangTimeout drives vb.WriteAt(offset, data) on a
// goroutine and waits up to timeout for it to return. A buggy
// awaitBackpressure that retries a non-ErrNoSpace drain failure forever
// would never send on done, so this is the hang-safety net for the two
// tests below: without it, a regression here would wedge `go test` (and
// everything after it in the same run) instead of failing promptly.
func runBlockingWriteWithHangTimeout(t *testing.T, vb *VB, offset uint64, data []byte, timeout time.Duration) error {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- vb.WriteAt(offset, data)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		t.Fatalf("WriteAt did not return within %s -- awaitBackpressure is hanging instead of bounding consecutive drain failures", timeout)
		return nil // unreachable, t.Fatalf stops the goroutine
	}
}

// TestAwaitBackpressureBoundsConsecutiveNonNoSpaceFailures is the direct
// regression test for the "retries forever" bug: a backend that keeps
// rejecting the live checkpoint write with an error that is NOT
// types.ErrNoSpace (a degraded backend, not an out-of-space one) must not
// keep awaitBackpressure spinning indefinitely. Without maxDrainFailures
// bounding the consecutive-failure count, this test hangs until
// runBlockingWriteWithHangTimeout's safety net trips and fails it; with the
// bound in place it returns a wrapped, non-ErrNoSpace error after exactly
// maxDrainFailures (10) consecutive failed drains.
func TestAwaitBackpressureBoundsConsecutiveNonNoSpaceFailures(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	// Force the backpressure gate to engage on the very next write, exactly
	// as TestAwaitBackpressureStopsRetryingOnNoSpace does.
	vb.MaxPendingBytes = blockSize
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	// Every checkpoint write fails from here on, with a persistent,
	// non-out-of-space error. Pin pendingBytes back above the low-watermark
	// after each attempt: the first (and only) buffered chunk write drains
	// and frees its bytes for real after round one, and without re-pinning,
	// pendingBytes would fall under the low-watermark and let
	// awaitBackpressure return nil having silently swallowed the checkpoint
	// failure -- exactly the "loop exit can mask the last drain's error"
	// trap this backend must be wired to avoid triggering here.
	backend.genericFail.Store(true)
	backend.afterWrite = func(fileType types.FileType, err error) {
		if fileType == types.FileTypeBlockCheckpointLive {
			vb.pendingBytes.Store(int64(vb.maxPendingBytes()) * 4)
		}
	}

	start := time.Now()
	// Each failing round costs SaveLiveCheckpointCtx's own internal 1s+2s
	// retry backoff (~3s) before awaitBackpressure counts it as one failed
	// drain, so 10 rounds is a real ~30s of wall clock -- generous but
	// bounded, unlike the unfixed behavior this guards against.
	err := runBlockingWriteWithHangTimeout(t, vb, blockSize, make([]byte, blockSize), 90*time.Second)
	elapsed := time.Since(start)

	require.Error(t, err, "a backend that never recovers must eventually surface an error instead of hanging")
	assert.NotErrorIs(t, err, ErrNoSpace, "a generic, non-out-of-space drain failure must not be misreported as ErrNoSpace")
	assert.Contains(t, err.Error(), "consecutive drains failed", "error must be the maxDrainFailures-bounded error, not some other failure")
	assert.False(t, vb.backendFull.Load(), "a generic drain failure must not latch backendFull -- that latch is reserved for real out-of-space errors")
	assert.Less(t, elapsed, 90*time.Second, "must return once the bound trips, not hang until the test's own safety-net timeout")
}

// TestAwaitBackpressureFailureCounterResetsAfterInterleavedSuccess proves
// assertion (b) of the maxDrainFailures fix: a single successful drain
// resets the consecutive-failure counter, so failures separated by a
// success do not accumulate toward the cap. The script below fails 1 round
// (3 internal SaveLiveCheckpointCtx attempts), succeeds once, then fails 9
// more rounds (27 attempts) -- 1+9 = maxDrainFailures if the counter never
// reset, which would trip the cap and return an error; with the reset in
// place the run instead completes successfully once the script is
// exhausted and a real checkpoint write finally lands.
func TestAwaitBackpressureFailureCounterResetsAfterInterleavedSuccess(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	vb.MaxPendingBytes = blockSize
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	script := make([]error, 0, 31)
	for range 3 {
		script = append(script, genericBackendErr())
	}
	script = append(script, nil)
	for range 27 {
		script = append(script, genericBackendErr())
	}
	backend.checkpointScript = script

	// Drive dirty/pendingBytes from an independent goroutine rather than
	// from the backend's afterWrite hook: SaveLiveCheckpointCtx
	// unconditionally clears BlocksToObject.dirty immediately after a
	// successful write returns (see the `if err == nil` branch), which
	// would clobber a redirty attempted from inside that same call. Ticking
	// this out-of-band, like a concurrent guest write landing between drain
	// rounds, avoids that ordering trap: for as long as scripted checkpoint
	// attempts remain, keep the checkpoint dirty and pendingBytes pinned
	// above the low-watermark so awaitBackpressure keeps calling
	// DrainToBackendCtx; once the script is exhausted, stop pinning and let
	// pendingBytes settle so the loop can observe it fall back under the
	// low-watermark and return.
	stop := make(chan struct{})
	driverDone := make(chan struct{})
	go func() {
		defer close(driverDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				backend.mu.Lock()
				exhausted := backend.checkpointScriptIdx >= len(backend.checkpointScript)
				backend.mu.Unlock()
				if exhausted {
					vb.pendingBytes.Store(0)
					return
				}
				vb.BlocksToObject.dirty.Store(true)
				vb.pendingBytes.Store(int64(vb.maxPendingBytes()) * 4)
			}
		}
	}()
	defer func() {
		close(stop)
		<-driverDone
	}()

	start := time.Now()
	err := runBlockingWriteWithHangTimeout(t, vb, blockSize, make([]byte, blockSize), 90*time.Second)
	elapsed := time.Since(start)

	assert.NoError(t, err, "an interleaved success must reset the consecutive-failure counter so 9 more failures (well under the cap of 10) do not trip it")
	assert.False(t, vb.backendFull.Load())
	assert.Less(t, elapsed, 90*time.Second, "must complete once the script settles, not hang")
}
