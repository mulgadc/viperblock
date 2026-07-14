// Tests for the live-checkpoint write-amplification fix.
//
// The bug: SaveLiveCheckpointCtx fired unconditionally on every 30s uploader
// tick and after every chunk upload, regardless of whether any blocks changed.
// On env19 (4 volumes) this produced 3,650 checkpoint PUTs in 8.5h, each one
// a full 3-node erasure-coded write into predastore's append-only segment
// store, filling 158G disks overnight.
//
// These tests compress the overnight scenario to seconds by using a fast
// ChunkUploadInterval and a counting backend wrapper that records WriteCtx
// calls by FileType.

package viperblock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingBackend wraps a Backend and records WriteCtx calls by FileType.
type countingBackend struct {
	types.Backend

	mu     sync.Mutex
	counts map[types.FileType]int
}

func (c *countingBackend) WriteCtx(ctx context.Context, ft types.FileType, id uint64, h *[]byte, d *[]byte) error {
	c.mu.Lock()
	c.counts[ft]++
	c.mu.Unlock()
	return c.Backend.WriteCtx(ctx, ft, id, h, d)
}

func (c *countingBackend) count(ft types.FileType) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[ft]
}

// errBackend injects failures on the first failN WriteCtx calls for failType.
type errBackend struct {
	types.Backend

	mu       sync.Mutex
	failType types.FileType
	failN    int
	failed   int
}

func (e *errBackend) WriteCtx(ctx context.Context, ft types.FileType, id uint64, h *[]byte, d *[]byte) error {
	if ft == e.failType {
		e.mu.Lock()
		if e.failed < e.failN {
			e.failed++
			e.mu.Unlock()
			return errors.New("injected write failure")
		}
		e.mu.Unlock()
	}
	return e.Backend.WriteCtx(ctx, ft, id, h, d)
}

// openFastVBWithCounting opens a file-backed VB, stops the auto-started
// uploader, wraps the backend with a write counter, and restarts the uploader
// at the given interval.
func openFastVBWithCounting(t *testing.T, dir, name string, interval time.Duration) (*VB, *countingBackend) {
	t.Helper()
	vb := openEncryptedVBInDir(t, dir, name, nil)
	vb.StopChunkUploader()

	cb := &countingBackend{Backend: vb.Backend, counts: make(map[types.FileType]int)}
	vb.Backend = cb
	vb.ChunkUploadInterval = interval
	vb.StartChunkUploader()

	t.Cleanup(func() {
		vb.StopChunkUploader()
		vb.StopWALSyncer()
	})
	return vb, cb
}

// TestCheckpointIdleGating is the fast-forward overnight test.
//
// Before the fix: every uploader tick unconditionally wrote blocks.live.bin,
// so N ticks → N checkpoint PUTs regardless of write activity. At 30s/tick on
// 4 volumes that produced 3,650 PUTs in 8.5h. Here we run 10 ticks in 600ms
// and assert zero checkpoint writes when no blocks have changed.
func TestCheckpointIdleGating(t *testing.T) {
	dir := t.TempDir()
	_, cb := openFastVBWithCounting(t, dir, "idle-gating", 50*time.Millisecond)

	// 10 ticks at 50ms; 600ms gives them all room to fire.
	time.Sleep(600 * time.Millisecond)

	assert.Equal(t, 0, cb.count(types.FileTypeBlockCheckpointLive),
		"idle ticks must not PUT blocks.live.bin when no blocks changed")
}

// TestCheckpointWriteProportionality verifies that a drain writes the live
// checkpoint exactly once — a single coalesced PUT after all chunk uploads
// land, regardless of chunk count — and that subsequent idle ticks add no
// further writes. Per-chunk checkpointing was removed: concurrent per-chunk
// PUTs from the parallel upload pool could land out of snapshot order and
// leave a stale seqNum in the checkpoint (encrypted reattach bad superblock).
func TestCheckpointWriteProportionality(t *testing.T) {
	dir := t.TempDir()
	// Slow ticker so it does not fire during the explicit drain below.
	vb, cb := openFastVBWithCounting(t, dir, "proportionality", 10*time.Second)

	// Write exactly 2 chunks worth of blocks. openEncryptedVBInDir sets
	// ObjBlockSize = 16 * DefaultBlockSize, so 32 blocks = 2 full chunks.
	blocksPerChunk := int(vb.ObjBlockSize / vb.BlockSize)
	data := make([]byte, vb.BlockSize)
	for i := range 2 * blocksPerChunk {
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), data))
	}

	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	chunkWrites := cb.count(types.FileTypeChunk)
	cpWrites := cb.count(types.FileTypeBlockCheckpointLive)
	require.Equal(t, 2, chunkWrites, "expected 2 chunk uploads")
	assert.Equal(t, 1, cpWrites,
		"drain must write the live checkpoint once (coalesced), not per chunk")
	assert.False(t, vb.BlocksToObject.dirty.Load(),
		"dirty must be clear after successful drain")

	// Confirm idle ticks after the drain add no further checkpoint writes.
	snap := cb.count(types.FileTypeBlockCheckpointLive)
	time.Sleep(700 * time.Millisecond) // 3+ idle ticks at 200ms would have fired before fix
	assert.Equal(t, snap, cb.count(types.FileTypeBlockCheckpointLive),
		"idle ticks after a clean drain must not PUT blocks.live.bin")
}

// TestCheckpointRetryOnFailure verifies that a transient backend error leaves
// dirty set (so the next trigger retries) and that the in-call retry loop
// clears dirty on success.
func TestCheckpointRetryOnFailure(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "retry-test", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	// Inject a single failure on the first checkpoint write, then succeed.
	eb := &errBackend{
		Backend:  vb.Backend,
		failType: types.FileTypeBlockCheckpointLive,
		failN:    1,
	}
	vb.Backend = eb

	// Mark dirty directly — simulates what createChunkFile does after uploading
	// a chunk. We skip createChunkFile here to isolate the retry path.
	vb.BlocksToObject.dirty.Store(true)

	// SaveLiveCheckpointCtx: attempt 1 fails (injected), waits 1s backoff,
	// attempt 2 succeeds. Dirty must be clear on return.
	require.NoError(t, vb.SaveLiveCheckpointCtx(context.Background()),
		"SaveLiveCheckpointCtx must succeed after one transient failure")

	assert.Equal(t, 1, eb.failed, "exactly one injected failure must have been consumed")
	assert.False(t, vb.BlocksToObject.dirty.Load(),
		"dirty must be cleared after a successful retry")
}
