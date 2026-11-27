package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"libguestfs.org/nbdkit"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const zeroTestVolumeSize = 1 << 20 // 1 MiB
const zeroTestBlockSize = 64 * 1024

// newZeroTestConnection builds a connection over a small file-backed volume
// with a small object block size, so chunk boundaries are cheap to cross.
func newZeroTestConnection(t *testing.T) *ViperBlockConnection {
	t.Helper()
	vb, err := viperblock.New(
		&viperblock.VB{VolumeName: "zero-test", VolumeSize: zeroTestVolumeSize, ObjBlockSize: zeroTestBlockSize},
		"file",
		file.FileConfig{VolumeName: "zero-test", VolumeSize: zeroTestVolumeSize, BaseDir: t.TempDir()},
	)
	require.NoError(t, err)
	return &ViperBlockConnection{vb: vb}
}

// TestZero_RejectsOutOfBoundsWithoutAllocating pins the ordering: the bounds
// check runs before the buffer exists, so a rejected request costs nothing
// like its own size.
func TestZero_RejectsOutOfBoundsWithoutAllocating(t *testing.T) {
	conn := newZeroTestConnection(t)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := conn.Zero(1<<28, 0, 0) // 256 MiB request against a 1 MiB volume
	runtime.ReadMemStats(&after)

	pluginErr, ok := err.(nbdkit.PluginError)
	require.True(t, ok, "the rejection must be a plugin error, got %T", err)
	assert.Equal(t, syscall.EINVAL, pluginErr.Errno, "the guest must see a rejected request, not an I/O error")

	alloc := after.TotalAlloc - before.TotalAlloc
	assert.Less(t, alloc, uint64(1<<20),
		"a rejected zero must not allocate anything like its requested size")
}

// TestZero_RejectsMaxUint32 pins the extreme request: a full uint32 count is
// rejected on bounds, before any allocation, on any volume.
func TestZero_RejectsMaxUint32(t *testing.T) {
	conn := newZeroTestConnection(t)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := conn.Zero(^uint32(0), 0, 0)
	runtime.ReadMemStats(&after)

	pluginErr, ok := err.(nbdkit.PluginError)
	require.True(t, ok, "the rejection must be a plugin error, got %T", err)
	assert.Equal(t, syscall.EINVAL, pluginErr.Errno)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20),
		"a rejected max-size zero must not allocate its own size")
}

// TestZero_ValidatesAgainstTheEngineSize pins that bounds come from the same
// size GetSize reports, not a copy taken at Open: after a resize reconciled
// into the engine, the grown range zeroes and the grown bound still holds.
func TestZero_ValidatesAgainstTheEngineSize(t *testing.T) {
	conn := newZeroTestConnection(t)

	// Simulate the LoadState resize reconciliation: the engine's size grows
	// past the size the connection was opened with.
	conn.vb.VolumeSize = zeroTestVolumeSize + zeroTestBlockSize
	assert.Equal(t, conn.vb.VolumeSize, mustSize(t, conn))

	// A range inside the grown size is valid, and the request that reaches
	// past the grown size is still refused.
	require.NoError(t, conn.Zero(zeroTestBlockSize, zeroTestVolumeSize, 0))
	err := conn.Zero(1, zeroTestVolumeSize+zeroTestBlockSize, 0)
	pluginErr, ok := err.(nbdkit.PluginError)
	require.True(t, ok, "expected a plugin error, got %T", err)
	assert.Equal(t, syscall.EINVAL, pluginErr.Errno)
}

func mustSize(t *testing.T, conn *ViperBlockConnection) uint64 {
	t.Helper()
	size, err := conn.GetSize()
	require.NoError(t, err)
	return size
}

// TestZero_ZeroesAcrossChunkBoundary pins that a range spanning chunk
// boundaries zeroes exactly the requested bytes and nothing beside them.
func TestZero_ZeroesAcrossChunkBoundary(t *testing.T) {
	conn := newZeroTestConnection(t)

	pattern := make([]byte, 4*zeroTestBlockSize)
	for i := range pattern {
		pattern[i] = 0xA5
	}
	require.NoError(t, conn.PWrite(pattern, 0, 0))

	// One byte before the first boundary through one past the second: the
	// range crosses two chunk edges.
	const start = zeroTestBlockSize - 1
	const count = zeroTestBlockSize + 2
	require.NoError(t, conn.Zero(count, start, 0))

	assertZeroedExactly(t, conn, 4*zeroTestBlockSize, start, count, 0xA5)
}

// TestZero_SmallUnalignedRange pins the small case: a few bytes mid-block
// zero exactly, and the surrounding data survives.
func TestZero_SmallUnalignedRange(t *testing.T) {
	conn := newZeroTestConnection(t)

	pattern := make([]byte, 2*zeroTestBlockSize)
	for i := range pattern {
		pattern[i] = 0x5A
	}
	require.NoError(t, conn.PWrite(pattern, 0, 0))

	require.NoError(t, conn.Zero(3, 17, 0))
	assertZeroedExactly(t, conn, 2*zeroTestBlockSize, 17, 3, 0x5A)
}

// failingBackend arms a backend to refuse writes with ErrNoSpace, so a test
// can fail a drain deterministically.
type failingBackend struct {
	types.Backend
	failWrites atomic.Bool
}

func (f *failingBackend) Write(fileType types.FileType, objectId uint64, headers, data *[]byte) error {
	if f.failWrites.Load() {
		return types.ErrNoSpace
	}
	return f.Backend.Write(fileType, objectId, headers, data)
}

func (f *failingBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers, data *[]byte) error {
	if f.failWrites.Load() {
		return types.ErrNoSpace
	}
	return f.Backend.WriteCtx(ctx, fileType, objectId, headers, data)
}

// TestZero_FailedZeroLeavesModifiedPrefix pins the failure shape of a
// chunked zero: when a chunk's drain fails with ErrNoSpace, the chunks before
// it stay zeroed, the rest keeps its fill, and the error reaches the guest.
func TestZero_FailedZeroLeavesModifiedPrefix(t *testing.T) {
	const (
		volumeSize uint64 = 4 << 20
		objBlock   uint64 = 64 << 10
		watermark  uint64 = 256 << 10
	)
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "local"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "backend"), 0o700))
	config := viperblock.VB{
		VolumeName: "enospc", VolumeSize: volumeSize, ObjBlockSize: uint32(objBlock),
		BaseDir: filepath.Join(root, "local"), LeaseEpoch: 1, MaxPendingBytes: watermark,
	}
	vb, err := viperblock.New(&config, "file", file.FileConfig{
		VolumeName: config.VolumeName, VolumeSize: volumeSize, BaseDir: filepath.Join(root, "backend"),
	})
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	t.Cleanup(func() { detachWriteBufferVolume(vb) })
	openWriteBufferWAL(t, vb)

	failing := &failingBackend{Backend: vb.Backend}
	vb.Backend = failing

	pattern := make([]byte, volumeSize)
	for i := range pattern {
		pattern[i] = 0x7E
	}
	require.NoError(t, vb.WriteAt(0, pattern))
	require.NoError(t, vb.DrainToBackend())

	// Arm the failure, then zero: the chunks fill the pending window, the
	// gate drives a drain, and the drain refuses with ErrNoSpace.
	failing.failWrites.Store(true)
	conn := &ViperBlockConnection{vb: vb}
	err = conn.Zero(uint32(volumeSize), 0, 0)
	pluginErr, ok := err.(nbdkit.PluginError)
	require.True(t, ok, "the guest must see a plugin error, got %T", err)
	assert.Equal(t, syscall.ENOSPC, pluginErr.Errno, "the drain's ErrNoSpace must reach the guest as ENOSPC")

	// The shape: the chunks before the failure are zeroed, and everything
	// past the failure keeps the pattern. The boundary is wherever the gate
	// tripped, so find it.
	got, readErr := vb.ReadAt(0, volumeSize)
	require.NoError(t, readErr)
	firstNonZero := -1
	for i, b := range got {
		if b != 0 {
			firstNonZero = i
			break
		}
	}
	require.Greater(t, firstNonZero, 0, "the chunks before the failure must be zeroed")
	for i := firstNonZero; i < len(got); i++ {
		if got[i] != 0x7E {
			t.Fatalf("byte %d is %#x, want the untouched pattern past the failure", i, got[i])
		}
	}
}

// TestZero_BackpressureBoundedBetweenChunks pins that a large zero submits
// its chunks one at a time against a low watermark: pending bytes cross the
// watermark (the gate engages), never reach the request size, and the zero
// resumes to completion with correct bytes.
func TestZero_BackpressureBoundedBetweenChunks(t *testing.T) {
	root := t.TempDir()
	vb := newWriteBufferVolume(t, root, 1, nil)
	t.Cleanup(func() { detachWriteBufferVolume(vb) })
	openWriteBufferWAL(t, vb)

	const watermark uint64 = 256 << 10
	vb.MaxPendingBytes = watermark

	const total uint64 = 8 << 20
	// Seed the range with data so the read-back proves the zero overwrote it
	// rather than reading a range that was never written.
	pattern := make([]byte, total)
	for i := range pattern {
		pattern[i] = 0x7E
	}
	require.NoError(t, vb.WriteAt(0, pattern))
	require.NoError(t, vb.DrainToBackend())

	var peak atomic.Uint64
	stop := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if p := vb.PendingBytes(); p > peak.Load() {
				peak.Store(p)
			}
			time.Sleep(500 * time.Microsecond)
		}
	}()

	zeroErr := (&ViperBlockConnection{vb: vb}).Zero(uint32(total), 0, 0)
	close(stop)
	<-pollDone
	require.NoError(t, zeroErr)

	t.Logf("peak pending bytes: %d (watermark %d, request %d)", peak.Load(), watermark, total)
	assert.Greater(t, peak.Load(), watermark, "the backpressure gate must have engaged")
	assert.Less(t, peak.Load(), total,
		"pending bytes must never reach the request size: chunks are submitted one at a time")

	got, err := vb.ReadAt(0, total)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, total), got, "the zero resumes to completion with correct bytes")
}

// gatedBackend blocks the first write after it is armed until released, so a
// test can hold a drain open and observe what the writer does while it waits.
type gatedBackend struct {
	types.Backend
	armed   atomic.Bool
	blocked chan struct{}
	gate    chan struct{}
}

func (g *gatedBackend) Write(fileType types.FileType, objectId uint64, headers, data *[]byte) error {
	return g.WriteCtx(context.Background(), fileType, objectId, headers, data)
}

func (g *gatedBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers, data *[]byte) error {
	if g.armed.CompareAndSwap(true, false) {
		close(g.blocked)
		<-g.gate
	}
	return g.Backend.WriteCtx(ctx, fileType, objectId, headers, data)
}

// TestZero_HoldsBackNextChunkWhileDrainOpen pins the pause: with the first
// chunk's drain held open at a low watermark, the zero submits no second
// chunk — pending bytes plateau at one chunk — and completes once released.
func TestZero_HoldsBackNextChunkWhileDrainOpen(t *testing.T) {
	const (
		volumeSize uint64 = 8 << 20
		watermark  uint64 = 256 << 10
	)
	root := t.TempDir()
	vb := newWriteBufferVolume(t, root, 1, nil)
	t.Cleanup(func() { detachWriteBufferVolume(vb) })
	openWriteBufferWAL(t, vb)
	vb.MaxPendingBytes = watermark

	// Seed so the read-back proves the zero overwrote the range.
	pattern := make([]byte, volumeSize)
	for i := range pattern {
		pattern[i] = 0x7E
	}
	require.NoError(t, vb.WriteAt(0, pattern))
	require.NoError(t, vb.DrainToBackend())

	gated := &gatedBackend{Backend: vb.Backend, blocked: make(chan struct{}), gate: make(chan struct{})}
	vb.Backend = gated
	gated.armed.Store(true)

	done := make(chan error, 1)
	go func() { done <- (&ViperBlockConnection{vb: vb}).Zero(uint32(volumeSize), 0, 0) }()

	// Wait for the first drain to enter the held write.
	select {
	case <-gated.blocked:
	case <-time.After(30 * time.Second):
		t.Fatal("the zero never reached its first drain")
	}

	// While the drain is held, pending must plateau at one chunk: no second
	// chunk is submitted behind an open drain.
	var plateau atomic.Uint64
	for i := 0; i < 400; i++ { // 200ms of samples
		p := vb.PendingBytes()
		if p > plateau.Load() {
			plateau.Store(p)
		}
		time.Sleep(500 * time.Microsecond)
	}
	select {
	case err := <-done:
		t.Fatalf("the zero completed while the drain was held open: %v", err)
	default:
	}
	t.Logf("pending while drain held: plateau %d (chunk %d, watermark %d)", plateau.Load(), 4<<20, watermark)
	assert.Less(t, plateau.Load(), uint64(6<<20),
		"no second chunk may be submitted while the first drain is open")
	assert.Greater(t, plateau.Load(), watermark, "the first chunk's buffered bytes must be visible")

	// Release: the zero resumes to completion with correct bytes.
	close(gated.gate)
	require.NoError(t, <-done)

	got, err := vb.ReadAt(0, volumeSize)
	require.NoError(t, err)
	assert.Equal(t, make([]byte, volumeSize), got, "the zero overwrote the seeded pattern")
}

// assertZeroedExactly reads the whole volume and asserts the requested range
// is zero and every byte outside it keeps its fill.
func assertZeroedExactly(t *testing.T, conn *ViperBlockConnection, total, start, count int, fill byte) {
	t.Helper()
	buf := make([]byte, total)
	require.NoError(t, conn.PRead(buf, 0, 0))
	for i := 0; i < total; i++ {
		if i >= start && i < start+count {
			assert.Equal(t, byte(0), buf[i], "byte %d inside the zeroed range must be zero", i)
		} else {
			assert.Equal(t, fill, buf[i], "byte %d outside the zeroed range must be untouched", i)
		}
	}
}

// TestNextZeroChunk_SplitsOnBlockEdges pins the segmentation contract: with
// an unaligned start and a chunk that is a block multiple, every interior
// boundary lands on a block edge, so the engine never rebuilds a fully
// covered block from two partial writes; a request that fits the buffer stays
// a single write, so a small unaligned zero is never split across one block;
// and the ranges cover exactly the request.
func TestNextZeroChunk_SplitsOnBlockEdges(t *testing.T) {
	const block uint64 = 4096
	const chunk uint64 = 4 * block

	tests := []struct {
		name   string
		offset uint64
		count  uint64
		ranges int
		bound  uint64 // expected first interior boundary, 0 when one range
	}{
		{"small request stays one write", 17, 3, 1, 0},
		{"aligned start", 0, 8 * block, 2, 0},
		{"unaligned start", 3*block + 7, 8*block - 7, 3, 4 * block},
		{"one byte over a boundary", block - 1, block + 2, 1, 0},
		{"exact fit", block, 4 * block, 1, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var covered uint64
			var boundaries []uint64
			for offset, remaining := tc.offset, tc.count; remaining > 0; {
				n := nextZeroChunk(offset, remaining, chunk, block)
				assert.LessOrEqual(t, n, chunk, "range exceeds the chunk size")
				covered += n
				boundaries = append(boundaries, offset)
				offset += n
				remaining -= n
			}
			assert.Equal(t, tc.count, covered, "the ranges must cover exactly the request")
			assert.Len(t, boundaries, tc.ranges)
			// Every boundary except the request's own start is a block edge.
			for i, b := range boundaries[1:] {
				assert.Zero(t, b%block, "interior boundary %d (%d) is not block aligned", i+1, b)
			}
			if tc.bound != 0 {
				assert.Equal(t, tc.bound, boundaries[1], "first interior boundary")
			}
		})
	}
}

// TestNextZeroChunk_DegradesWithoutBlockMultiples pins the defensive
// fallback: a chunk that is not a block multiple still splits into covering
// ranges that never exceed the buffer.
func TestNextZeroChunk_DegradesWithoutBlockMultiples(t *testing.T) {
	var covered uint64
	for offset, remaining := uint64(3), uint64(1000); remaining > 0; {
		n := nextZeroChunk(offset, remaining, 300, 4096)
		assert.LessOrEqual(t, n, uint64(300))
		covered += n
		offset += n
		remaining -= n
	}
	assert.Equal(t, uint64(1000), covered)
}

// TestChunkSize_Bounded pins the buffer sizing: a fixed ceiling regardless of
// the persisted object size, rounded down to a whole engine block, then capped
// by the request so a request below the limit stays one write.
func TestChunkSize_Bounded(t *testing.T) {
	const block = uint64(4096)
	const objBlock = uint64(4 << 20)
	assert.Equal(t, objBlock, chunkSize(64<<20, objBlock, block), "large request uses the object block size")
	assert.Equal(t, uint64(1000), chunkSize(1000, objBlock, block), "small request is capped by the request")
	assert.Equal(t, uint64(viperblock.DefaultObjBlockSize), chunkSize(64<<20, 0, block), "zero object block falls back to the engine default")
	assert.Equal(t, block, chunkSize(64<<20, 1024, block), "an object block below the engine block is raised")
	assert.Equal(t, uint64(8192), chunkSize(64<<20, 10000, block),
		"a non-multiple object size rounds down to whole engine blocks")
	assert.Equal(t, block, chunkSize(6000, 5000, block), "a small non-multiple request still rounds to the block")
	assert.Equal(t, uint64(4<<20), chunkSize(6<<20, 8<<20, block),
		"an object block above the ceiling is capped, whatever the persisted state says")
	assert.Equal(t, uint64(1<<20)+7, chunkSize((1<<20)+7, objBlock, block),
		"an unaligned request below the limit stays a single write")
	assert.Equal(t, uint64(4<<20), chunkSize(64<<20, 8<<20, 8<<20),
		"a persisted engine block above the ceiling keeps the buffer at the ceiling")
	assert.Equal(t, uint64(4<<20), chunkSize(4<<20, 8<<20, 8<<20),
		"a request at the ceiling stays one chunk even with a larger block")
}

// --- benchmarks ---

// BenchmarkZero measures the zero path across the common, large, multi-chunk,
// advertised-maximum and largest-benchmarked shapes, on WAL-initialized
// volumes with a bounded write window, mirroring the BenchmarkPWrite pattern.
// The plugin itself accepts any uint32 count within the volume size; 256 MiB
// is the largest case the harness measures, eight times the 32 MiB maximum
// the deployment's blocksize policy advertises.
func BenchmarkZero(b *testing.B) {
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(logger)

	for _, size := range []uint32{4 << 10, 1 << 20, 12 << 20, 32 << 20, 256 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			root := filepath.Join(b.TempDir(), "volume")
			// The window never sits below the case itself, and the volume
			// holds the window plus one case, with headroom for the WAL's
			// chunk growth.
			window := max(64<<20, int(size))
			volSize := uint64(window) + uint64(size)*3
			newVolume := func() *viperblock.VB {
				backendDir := filepath.Join(root, "backend")
				require.NoError(b, os.MkdirAll(backendDir, 0o700))
				config := viperblock.VB{
					VolumeName: "zero-bench", VolumeSize: volSize,
					BaseDir: filepath.Join(root, "local"), LeaseEpoch: 1,
					WALSyncInterval: -1, ChunkUploadInterval: -1,
				}
				vb, err := viperblock.New(&config, "file", file.FileConfig{
					VolumeName: config.VolumeName, VolumeSize: volSize, BaseDir: backendDir,
				})
				require.NoError(b, err)
				require.NoError(b, vb.Backend.Init())
				openWriteBufferWAL(b, vb)
				return vb
			}
			conn := &ViperBlockConnection{vb: newVolume()}
			defer func() { detachWriteBufferVolume(conn.vb) }()
			offset := 0
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				if offset+int(size) > window {
					b.StopTimer()
					detachWriteBufferVolume(conn.vb)
					conn.vb = nil
					require.NoError(b, os.RemoveAll(root))
					conn.vb = newVolume()
					offset = 0
					b.StartTimer()
				}
				if err := conn.Zero(size, uint64(offset), 0); err != nil {
					b.Fatal(err)
				}
				offset += int(size)
			}
		})
	}
}
