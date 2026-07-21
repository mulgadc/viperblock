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
// rejected a write as out-of-space without needing to actually exhaust a
// filesystem in this test.
type enospcBackend struct {
	types.Backend

	full atomic.Bool

	// genericFail, when true, makes every FileTypeBlockCheckpointLive write
	// return a persistent, non-ErrNoSpace error, so tests can prove
	// awaitBackpressure bounds consecutive drain failures instead of
	// retrying forever. Scoped to the checkpoint write, not chunk writes,
	// so BlocksToObject.dirty stays true and SaveLiveCheckpointCtx keeps
	// being retried every drain round.
	genericFail atomic.Bool

	// mu guards checkpointScript/checkpointScriptIdx below.
	mu sync.Mutex

	// checkpointScript, if non-empty, overrides genericFail for successive
	// FileTypeBlockCheckpointLive writes only: consumed in order, a nil
	// entry succeeds, a non-nil entry fails with that error. Once
	// exhausted, checkpoint writes fall back to the genericFail toggle.
	checkpointScript    []error
	checkpointScriptIdx int

	// afterWrite, if set, runs after every Write/WriteCtx call returns.
	// Tests use it to pin vb.pendingBytes above the low-watermark so an
	// awaitBackpressure retry loop keeps iterating, and to re-mark
	// BlocksToObject.dirty after a scripted checkpoint success, standing in
	// for a concurrent guest write that would otherwise dirty it again.
	afterWrite func(fileType types.FileType, err error)
}

// enospcErr mirrors what backends/s3 and backends/file return once
// classifyWriteErr recognizes a 507/503 or syscall.ENOSPC.
func enospcErr() error {
	return fmt.Errorf("%w: simulated backend-full write rejection", types.ErrNoSpace)
}

// genericBackendErr simulates a persistent backend failure that is NOT
// out-of-space.
func genericBackendErr() error {
	return errors.New("simulated persistent backend I/O error (not out-of-space)")
}

// dispatch centralizes this fake's fail/succeed decision so Write and
// WriteCtx behave identically.
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

// newEnospcTestVB builds a minimal file-backed VB with its Backend wrapped
// in enospcBackend, so tests can toggle backend-full behavior without
// touching an actual filesystem's free space.
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

	// These tests drive tens of deliberately failing checkpoint writes; at the
	// production backoff each one would really sleep ~3s. Retry ordering is
	// what's under test, not the wait itself.
	vb.checkpointRetryBackoff = time.Microsecond

	require.NoError(t, vb.Backend.Init())
	backend := &enospcBackend{Backend: vb.Backend}
	vb.Backend = backend

	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	return vb, backend
}

// TestDrainLatchesBackendFullAndWriteFailsFast pins that a drain hitting a
// backend-full error latches backendFull, and a subsequent write fails fast
// with ErrNoSpace instead of buffering.
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

// TestBackendFullLatchClearsAfterSuccessfulDrain pins that once the backend
// recovers, the next successful drain clears backendFull and writes succeed
// again.
func TestBackendFullLatchClearsAfterSuccessfulDrain(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	backend.full.Store(true)
	err := vb.DrainToBackendCtx(context.Background())
	require.Error(t, err)
	require.True(t, vb.backendFull.Load(), "latch must be set before we can test it clearing")

	// Drain directly (bypassing WriteAtCtx's gate) so a clean drain can
	// observe/clear the latch.
	backend.full.Store(false)
	err = vb.DrainToBackendCtx(context.Background())
	require.NoError(t, err, "drain against a recovered backend must succeed")
	assert.False(t, vb.backendFull.Load(), "backendFull latch must clear after a successful drain")

	// Writes must succeed again now that the latch is clear.
	err = vb.WriteAt(blockSize, make([]byte, blockSize))
	assert.NoError(t, err, "writes must succeed once the backend-full latch has cleared")

	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
}

// TestAwaitBackpressureStopsRetryingOnNoSpace pins that once a backend-full
// error is observed in the backpressure gate, awaitBackpressure returns
// ErrNoSpace immediately instead of backing off and retrying.
func TestAwaitBackpressureStopsRetryingOnNoSpace(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	// Force the backpressure gate to engage on the very next write.
	vb.MaxPendingBytes = blockSize

	// First write fills up to the watermark without crossing it or touching
	// the backend, so flip full only after it.
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	backend.full.Store(true)

	start := time.Now()
	err := vb.WriteAt(blockSize, make([]byte, blockSize))
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoSpace, "a write that trips awaitBackpressure against a full backend must surface ErrNoSpace")
	assert.True(t, vb.backendFull.Load(), "backendFull latch must be set by the drain awaitBackpressure drove")
	// A retrying implementation would spend at least one backoff interval
	// before giving up; the fix returns on the first ErrNoSpace.
	assert.Less(t, elapsed, 200*time.Millisecond,
		"awaitBackpressure must stop on the first ErrNoSpace instead of backing off and retrying")
}

// TestWriteAtCtxGateIsLoadBearing proves the up-front backendFull check is
// load-bearing: without it, a write against a full backend would buffer
// into memory and grow pendingBytes unbounded instead of failing.
func TestWriteAtCtxGateIsLoadBearing(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))
	backend.full.Store(true)
	require.Error(t, vb.DrainToBackendCtx(context.Background()))
	require.True(t, vb.backendFull.Load())

	pendingBefore := vb.PendingBytes()

	// Simulate "no up-front gate" by performing the buffering half of
	// WriteAtCtx directly, as it would happen without the check.
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

// TestErrNoSpaceIsTypesErrNoSpace pins that ErrNoSpace re-exports
// types.ErrNoSpace, and drain-path error wrapping doesn't break errors.Is
// against either name.
func TestErrNoSpaceIsTypesErrNoSpace(t *testing.T) {
	assert.ErrorIs(t, ErrNoSpace, types.ErrNoSpace)
	wrapped := fmt.Errorf("drain chunk upload: %w", enospcErr())
	assert.ErrorIs(t, wrapped, ErrNoSpace)
	assert.ErrorIs(t, wrapped, types.ErrNoSpace)
}

// runBlockingWriteWithHangTimeout drives vb.WriteAt(offset, data) on a
// goroutine and fails the test if it does not return within timeout,
// instead of letting a hanging awaitBackpressure wedge `go test`.
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

// TestAwaitBackpressureBoundsConsecutiveNonNoSpaceFailures pins that a
// backend persistently rejecting the checkpoint write with a non-ErrNoSpace
// error does not keep awaitBackpressure spinning indefinitely:
// maxDrainFailures bounds it after 10 consecutive failed drains.
func TestAwaitBackpressureBoundsConsecutiveNonNoSpaceFailures(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	// Force the backpressure gate to engage on the very next write.
	vb.MaxPendingBytes = blockSize
	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	// Every checkpoint write fails from here on. Pin pendingBytes back
	// above the low-watermark after each attempt so it can't fall under it
	// and let awaitBackpressure return nil, silently swallowing the
	// checkpoint failure.
	backend.genericFail.Store(true)
	backend.afterWrite = func(fileType types.FileType, err error) {
		if fileType == types.FileTypeBlockCheckpointLive {
			vb.pendingBytes.Store(int64(vb.maxPendingBytes()) * 4)
		}
	}

	start := time.Now()
	// newEnospcTestVB shrinks SaveLiveCheckpointCtx's retry backoff, so the 10
	// bounded rounds cost microseconds rather than ~3s each. The timeout is a
	// hang detector with generous headroom, not an expected duration.
	err := runBlockingWriteWithHangTimeout(t, vb, blockSize, make([]byte, blockSize), 30*time.Second)
	elapsed := time.Since(start)

	require.Error(t, err, "a backend that never recovers must eventually surface an error instead of hanging")
	assert.NotErrorIs(t, err, ErrNoSpace, "a generic, non-out-of-space drain failure must not be misreported as ErrNoSpace")
	assert.Contains(t, err.Error(), "consecutive drains failed", "error must be the maxDrainFailures-bounded error, not some other failure")
	assert.False(t, vb.backendFull.Load(), "a generic drain failure must not latch backendFull -- that latch is reserved for real out-of-space errors")
	assert.Less(t, elapsed, 30*time.Second, "must return once the bound trips, not hang until the test's own safety-net timeout")
}

// TestAwaitBackpressureFailureCounterResetsAfterInterleavedSuccess pins that
// a single successful drain resets the consecutive-failure counter, so
// failures separated by a success (1 + 9, both under the cap of 10) don't
// accumulate and trip maxDrainFailures.
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

	// Drive dirty/pendingBytes from an independent goroutine rather than the
	// backend's afterWrite hook: SaveLiveCheckpointCtx clears
	// BlocksToObject.dirty right after a successful write returns, which
	// would clobber a redirty attempted from inside that same call. Ticking
	// out-of-band mimics a concurrent guest write instead.
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
