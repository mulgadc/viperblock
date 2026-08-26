package viperblock

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testArenaBlockSize = 4096

// TestArena_AllocsDoNotOverlap is the core allocator contract: every Alloc
// hands back a distinct, block-sized region, so writing through one buffer
// can never corrupt another.
func TestArena_AllocsDoNotOverlap(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 4*testArenaBlockSize)

	const n = 4
	bufs := make([][]byte, n)
	for i := range bufs {
		bufs[i] = a.Alloc()
		require.Len(t, bufs[i], testArenaBlockSize)
		for j := range bufs[i] {
			bufs[i][j] = byte(i + 1)
		}
	}

	for i, buf := range bufs {
		want := bytes.Repeat([]byte{byte(i + 1)}, testArenaBlockSize)
		assert.Equal(t, want, buf, "buffer %d was overwritten by another allocation", i)
	}

	// All four fit in one slab, and the stats reflect exactly what was handed out.
	assert.Equal(t, 1, a.SlabCount())
	numSlabs, totalAllocs, totalBytes := a.Stats()
	assert.Equal(t, 1, numSlabs)
	assert.Equal(t, uint64(n), totalAllocs)
	assert.Equal(t, uint64(n*testArenaBlockSize), totalBytes)
}

// TestArena_AllocIsZeroed guards the guarantee AllocCopy depends on: a fresh
// block is zero, so copying short data leaves no stale bytes in the tail.
func TestArena_AllocIsZeroed(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 4*testArenaBlockSize)

	buf := a.Alloc()
	assert.Equal(t, make([]byte, testArenaBlockSize), buf)
}

// TestArena_AllocRollsOverToNewSlab checks the bump pointer starts a new slab
// once the active one is exhausted rather than returning a short buffer.
func TestArena_AllocRollsOverToNewSlab(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 2*testArenaBlockSize)

	first := a.Alloc()
	second := a.Alloc()

	used, total := a.ActiveSlabUsage()
	assert.Equal(t, uint32(2*testArenaBlockSize), used)
	assert.Equal(t, uint32(2*testArenaBlockSize), total)
	require.Equal(t, 1, a.SlabCount())

	third := a.Alloc()
	require.Len(t, third, testArenaBlockSize)
	assert.Equal(t, 2, a.SlabCount(), "exhausted slab should trigger a new one")

	used, _ = a.ActiveSlabUsage()
	assert.Equal(t, uint32(testArenaBlockSize), used)

	// The rolled-over block must be independent of the full slab's blocks.
	for i := range third {
		third[i] = 0xAA
	}
	assert.Equal(t, make([]byte, testArenaBlockSize), first)
	assert.Equal(t, make([]byte, testArenaBlockSize), second)
}

// TestArena_AllocCopy verifies the returned block owns its bytes: mutating the
// source afterwards must not be visible through the arena buffer.
func TestArena_AllocCopy(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 4*testArenaBlockSize)

	src := bytes.Repeat([]byte{0x5A}, 512)
	buf := a.AllocCopy(src)
	require.Len(t, buf, testArenaBlockSize)

	src[0] = 0xFF
	assert.Equal(t, byte(0x5A), buf[0], "arena buffer aliases the source")
	assert.Equal(t, bytes.Repeat([]byte{0x5A}, 512), buf[:512])
	assert.Equal(t, make([]byte, testArenaBlockSize-512), buf[512:], "tail beyond the copy must stay zero")
}

// TestArena_AllocNContiguous checks a multi-block request comes back as one
// contiguous region of exactly n blocks.
func TestArena_AllocNContiguous(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 8*testArenaBlockSize)

	buf := a.AllocN(3)
	require.Len(t, buf, 3*testArenaBlockSize)

	used, _ := a.ActiveSlabUsage()
	assert.Equal(t, uint32(3*testArenaBlockSize), used)

	next := a.Alloc()
	for i := range buf {
		buf[i] = 0x11
	}
	assert.Equal(t, make([]byte, testArenaBlockSize), next, "AllocN region overlaps the following block")
}

// TestArena_AllocNRollsOverToNewSlab covers a multi-block request that no
// longer fits the active slab but is still smaller than a whole slab.
func TestArena_AllocNRollsOverToNewSlab(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 4*testArenaBlockSize)

	a.AllocN(3)
	require.Equal(t, 1, a.SlabCount())

	buf := a.AllocN(2)
	require.Len(t, buf, 2*testArenaBlockSize)
	assert.Equal(t, 2, a.SlabCount())

	used, _ := a.ActiveSlabUsage()
	assert.Equal(t, uint32(2*testArenaBlockSize), used)
}

// TestArena_AllocNOversizedUsesDedicatedSlab checks a request larger than a
// slab is satisfied exactly and leaves the active slab's bump pointer alone.
func TestArena_AllocNOversizedUsesDedicatedSlab(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 4*testArenaBlockSize)

	a.Alloc()
	usedBefore, _ := a.ActiveSlabUsage()

	buf := a.AllocN(6)
	require.Len(t, buf, 6*testArenaBlockSize)
	assert.Equal(t, 2, a.SlabCount())

	usedAfter, _ := a.ActiveSlabUsage()
	assert.Equal(t, usedBefore, usedAfter, "oversized allocation must not consume the active slab")

	_, totalAllocs, totalBytes := a.Stats()
	assert.Equal(t, uint64(2), totalAllocs)
	assert.Equal(t, uint64(7*testArenaBlockSize), totalBytes)
}

// TestArena_CompactReclaimsReleasedSlabs exercises the refcount contract:
// a slab is only reclaimed once every block handed out from it is released.
func TestArena_CompactReclaimsReleasedSlabs(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 2*testArenaBlockSize)

	a.Alloc()
	a.Alloc()
	a.Alloc() // rolls over, leaving the first slab full and still referenced
	require.Equal(t, 2, a.SlabCount())

	first := a.slabs[0]
	active := a.active

	assert.Equal(t, 0, a.Compact(), "slab with live allocations must be kept")
	assert.Equal(t, 2, a.SlabCount())

	a.Release(first)
	assert.Equal(t, 0, a.Compact(), "slab with one live allocation must be kept")

	a.Release(first)
	assert.Equal(t, 1, a.Compact())
	assert.Equal(t, 1, a.SlabCount())
	assert.Same(t, active, a.active, "the active slab must never be reclaimed")
}

// TestArena_CompactKeepsEmptyActiveSlab pins the exemption for the active
// slab, which has no allocations yet but must survive Compact.
func TestArena_CompactKeepsEmptyActiveSlab(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 4*testArenaBlockSize)

	assert.Equal(t, 0, a.Compact())
	assert.Equal(t, 1, a.SlabCount())

	require.Len(t, a.Alloc(), testArenaBlockSize)
}

// TestArena_ReleaseNilIsNoOp covers the guard that lets callers release a slab
// they never resolved without panicking.
func TestArena_ReleaseNilIsNoOp(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 4*testArenaBlockSize)

	assert.NotPanics(t, func() { a.Release(nil) })
	assert.Equal(t, 1, a.SlabCount())
}

// TestArena_ResetDropsSlabsButKeepsStats checks Reset returns the arena to a
// single empty slab while leaving the cumulative counters intact.
func TestArena_ResetDropsSlabsButKeepsStats(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 2*testArenaBlockSize)

	a.Alloc()
	a.Alloc()
	a.Alloc()
	require.Equal(t, 2, a.SlabCount())

	a.Reset()

	assert.Equal(t, 1, a.SlabCount())
	used, _ := a.ActiveSlabUsage()
	assert.Zero(t, used)

	_, totalAllocs, totalBytes := a.Stats()
	assert.Equal(t, uint64(3), totalAllocs, "Reset frees memory, it does not rewind stats")
	assert.Equal(t, uint64(3*testArenaBlockSize), totalBytes)

	assert.Equal(t, make([]byte, testArenaBlockSize), a.Alloc())
}

// TestArena_ConcurrentAllocsAreDisjoint is the reason Alloc holds a mutex:
// racing callers must never be handed the same region. Run with -race.
func TestArena_ConcurrentAllocsAreDisjoint(t *testing.T) {
	a := NewArenaWithSlabSize(testArenaBlockSize, 4*testArenaBlockSize)

	const workers = 8
	const perWorker = 16

	bufs := make([][][]byte, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bufs[w] = make([][]byte, perWorker)
			for i := range perWorker {
				buf := a.Alloc()
				for j := range buf {
					buf[j] = byte(w + 1)
				}
				bufs[w][i] = buf
			}
		}()
	}
	wg.Wait()

	want := workers * perWorker
	for w := range workers {
		for i, buf := range bufs[w] {
			require.Len(t, buf, testArenaBlockSize)
			expected := bytes.Repeat([]byte{byte(w + 1)}, testArenaBlockSize)
			require.Equal(t, expected, buf, "worker %d buffer %d was shared with another worker", w, i)
		}
	}

	_, totalAllocs, _ := a.Stats()
	assert.Equal(t, uint64(want), totalAllocs)
}

// TestPooledArena_PutClearsBuffer is a data-leak guard: a recycled block must
// not carry a previous volume's plaintext into its next user.
func TestPooledArena_PutClearsBuffer(t *testing.T) {
	pa := NewPooledArena(testArenaBlockSize)

	buf := pa.Get()
	require.Len(t, buf, testArenaBlockSize)

	for i := range buf {
		buf[i] = 0xC3
	}
	pa.Put(buf)

	assert.Equal(t, make([]byte, testArenaBlockSize), buf, "Put must zero the buffer before pooling it")
	assert.Len(t, pa.Get(), testArenaBlockSize)
}

// TestPooledArena_PermanentAllocsComeFromArena checks the permanent path
// bypasses the pool, so those buffers are never recycled under the caller.
func TestPooledArena_PermanentAllocsComeFromArena(t *testing.T) {
	pa := NewPooledArena(testArenaBlockSize)

	buf := pa.AllocPermanent()
	require.Len(t, buf, testArenaBlockSize)

	src := bytes.Repeat([]byte{0x7E}, 128)
	copied := pa.AllocCopyPermanent(src)
	require.Len(t, copied, testArenaBlockSize)
	assert.Equal(t, src, copied[:128])
	assert.Equal(t, make([]byte, testArenaBlockSize-128), copied[128:])

	_, totalAllocs, totalBytes := pa.arena.Stats()
	assert.Equal(t, uint64(2), totalAllocs)
	assert.Equal(t, uint64(2*testArenaBlockSize), totalBytes)
}
