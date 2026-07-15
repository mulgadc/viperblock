package viperblock

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowBackend wraps a real types.Backend and adds artificial latency to every
// write, standing in for a backend (e.g. S3/predastore over the network)
// that can't keep up with a fast local guest write. Everything else proxies
// straight through via the embedded interface.
type slowBackend struct {
	types.Backend

	delay time.Duration
}

func (s *slowBackend) Write(fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	time.Sleep(s.delay)
	return s.Backend.Write(fileType, objectId, headers, data)
}

func (s *slowBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	time.Sleep(s.delay)
	return s.Backend.WriteCtx(ctx, fileType, objectId, headers, data)
}

// newBackpressureTestVB builds a minimal file-backed VB (legacy single-file
// WAL, no LRU cache, periodic syncers disabled for deterministic behavior)
// and swaps in a slowBackend so drains take drainDelay per backend write.
func newBackpressureTestVB(t *testing.T, maxPendingBytes uint64, drainDelay time.Duration) *VB {
	t.Helper()

	tmpDir := t.TempDir()
	testVol := fmt.Sprintf("test_backpressure_%d", time.Now().UnixNano())

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
		// upload racing the test; the backpressure gate itself is what we're
		// exercising.
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		MaxPendingBytes:     maxPendingBytes,
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}

	vb, err := New(&vbconfig, FileBackend, backendConfig)
	require.NoError(t, err)
	require.NotNil(t, vb)

	// Registered before the setup below can call FailNow, which would otherwise
	// skip cleanup and leave the VB tree behind.
	t.Cleanup(func() {
		assert.NoError(t, vb.RemoveLocalFiles())
	})

	vb.UseShardedWAL = false
	vb.ShardedWAL = nil

	require.NoError(t, vb.Backend.Init())
	vb.Backend = &slowBackend{Backend: vb.Backend, delay: drainDelay}

	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	return vb
}

// TestWriteAtBackpressureBoundsPendingBytes drives many WriteAt calls, single
// threaded and back to back (i.e. faster than the artificially slowed backend
// drain), and asserts that outstanding buffered bytes (Writes.Blocks +
// PendingBackendWrites.Blocks) stay bounded near MaxPendingBytes instead of
// growing to the full amount written — the core guest-write-balloons-memory
// regression this backpressure gate fixes.
func TestWriteAtBackpressureBoundsPendingBytes(t *testing.T) {
	const maxPendingBytes = 256 * 1024 // 64 blocks @ 4KB
	const numBlocks = 2000             // 2000 * 4KB ~= 8MB total written

	vb := newBackpressureTestVB(t, maxPendingBytes, 15*time.Millisecond)

	blockSize := uint64(vb.BlockSize)
	totalWritten := uint64(numBlocks) * blockSize

	var maxObserved atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if v := vb.PendingBytes(); v > maxObserved.Load() {
					maxObserved.Store(v)
				}
			case <-stop:
				return
			}
		}
	}()

	// Write each block with content identifying its block number, so we can
	// verify no block was lost or reordered by the backpressure gate.
	expected := make(map[uint64][]byte, numBlocks)
	for i := range uint64(numBlocks) {
		data := make([]byte, blockSize)
		msg := fmt.Sprintf("block-%d-payload", i)
		copy(data, msg)
		expected[i] = data

		err := vb.WriteAt(i*blockSize, data)
		assert.NoError(t, err)
	}

	close(stop)
	<-done

	// Drain whatever remains under the watermark so a final readback can
	// verify every block, including the tail that never crossed the gate.
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	t.Logf("total written = %d bytes, max observed pending = %d bytes, MaxPendingBytes = %d",
		totalWritten, maxObserved.Load(), maxPendingBytes)

	// The gate must have actually engaged (buffer filled at least past the
	// low watermark) — otherwise this test would trivially pass on a no-op
	// implementation.
	assert.Greater(t, maxObserved.Load(), uint64(maxPendingBytes/2),
		"backpressure gate never appeared to engage; test is not exercising it")

	// The real assertion: pending bytes stayed near MaxPendingBytes, not the
	// full amount written. One in-flight WriteAt's worth of overshoot above
	// the watermark is expected (the counter is bumped before the gate is
	// checked), so allow a small margin.
	assert.LessOrEqual(t, maxObserved.Load(), uint64(maxPendingBytes)+blockSize,
		"outstanding buffered bytes exceeded MaxPendingBytes + one block; backpressure gate did not bound memory")
	assert.Less(t, maxObserved.Load(), totalWritten/4,
		"outstanding buffered bytes grew proportionally to total written; backpressure gate is not bounding memory")

	assert.Equal(t, uint64(0), vb.PendingBytes(), "pending bytes should be fully drained after final DrainToBackendCtx")

	// Correctness: every block must read back exactly what was written, in
	// spite of being throttled through the gate.
	for i := range uint64(numBlocks) {
		got, err := vb.ReadAt(i*blockSize, blockSize)
		if !assert.NoError(t, err, "block %d", i) {
			continue
		}
		assert.Equal(t, expected[i], got, "block %d data mismatch", i)
	}
}

// TestWriteAtBackpressureBlocksThenReleases directly measures that a single
// WriteAt call which pushes pendingBytes over MaxPendingBytes blocks for
// roughly the drain latency, and that pendingBytes drops back to the low
// watermark (MaxPendingBytes/2) by the time the call returns.
func TestWriteAtBackpressureBlocksThenReleases(t *testing.T) {
	const maxPendingBytes = 64 * 1024 // 16 blocks @ 4KB
	const drainDelay = 50 * time.Millisecond

	vb := newBackpressureTestVB(t, maxPendingBytes, drainDelay)
	blockSize := uint64(vb.BlockSize)

	// Fill up to (but not past) the high watermark: these calls must not block.
	fillBlocks := maxPendingBytes / blockSize
	fillStart := time.Now()
	for i := range fillBlocks {
		data := make([]byte, blockSize)
		err := vb.WriteAt(i*blockSize, data)
		assert.NoError(t, err)
	}
	fillElapsed := time.Since(fillStart)
	assert.Less(t, fillElapsed, drainDelay, "filling up to the watermark should not have blocked on a drain")
	assert.Equal(t, uint64(maxPendingBytes), vb.PendingBytes(), "pending bytes should equal exactly what was written so far")

	// This next write pushes pendingBytes past MaxPendingBytes and must block
	// until a drain (paced by drainDelay) brings it back under the low
	// watermark.
	triggerStart := time.Now()
	err := vb.WriteAt(fillBlocks*blockSize, make([]byte, blockSize))
	triggerElapsed := time.Since(triggerStart)
	assert.NoError(t, err)

	assert.GreaterOrEqual(t, triggerElapsed, drainDelay,
		"write crossing the high watermark should have blocked for at least one drain cycle")
	assert.LessOrEqual(t, vb.PendingBytes(), uint64(maxPendingBytes/2),
		"pending bytes should be at or below the low watermark once the blocking write returns")

	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
	assert.Equal(t, uint64(0), vb.PendingBytes())
}
