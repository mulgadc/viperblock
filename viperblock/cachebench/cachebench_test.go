// Package cachebench isolates the read-cache retention behaviour of the
// viperblock NBD plugin. It models the exact construct at
// viperblock/viperblock.go:3925 & :4036 — the read cache stores a 4 KiB
// subslice of the (large) per-NBD-request read buffer:
//
//	data := make([]byte, blockLen)               // viperblock.go:3254/3753
//	...
//	vb.Cache.lru.Add(block, data[blockStart:blockEnd])   // subslice -> pins data
//
// In Go a subslice retains its whole backing array, so a single live 4 KiB
// entry pins the entire request buffer. This benchmark drives the SAME LRU
// dependency (hashicorp/golang-lru/v2, viperblock.go:31/615) and measures
// retained heap for the current code (subslice) vs the proposed fix (clone),
// across three access patterns:
//
//   - clustered:  large reads at well-separated aligned offsets. Each buffer's
//     blocks enter and leave the LRU together, so aliasing barely bites — best
//     case for the current code.
//   - random:     overlapping unaligned reads over a hot region. Older buffers
//     are partially overwritten; modest retention overhead — typical case.
//   - streaming:  a sliding read window advancing one block at a time (e.g.
//     sequential I/O with overlapping prefetch). Each read overwrites all but
//     one of the previous buffer's block-entries, so EVERY cached entry ends up
//     aliasing its own distinct backing buffer. With the subslice Add this pins
//     bufLen/blockSize (24x here) the intended bytes — the worst case, and the
//     mechanism behind the observed nbdkit RSS blowup / host OOM.
//
// Lives in its own package so it does not inherit the viperblock package's
// TestMain (predastore server); avoids the WAL/backend path so it is fast and
// deterministic.
package cachebench

import (
	"math/rand"
	"runtime"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	cacheSize = 8192 // 1/4 of production nbd cache_size (32768)
	blockSize = 4096 // viperblock DefaultBlockSize
	largeReq  = 24   // 96 KiB read buffer (bounds worst-case retention < 1 GiB)
	prodScale = 4    // cacheSize * prodScale == production 32768
)

func TestCacheRetentionMemory(t *testing.T) {
	intended := float64(cacheSize*blockSize) / (1024 * 1024)

	var streamBefore, streamAfter float64
	for _, w := range []struct {
		name string
		fn   func(useClone bool) float64
	}{
		{"clustered (aligned, separated)", runClustered},
		{"random    (unaligned overlap) ", runRandom},
		{"streaming (sliding prefetch)  ", runStreaming},
	} {
		before := w.fn(false)
		after := w.fn(true)
		t.Logf("=== %s ===", w.name)
		t.Logf("  intended ceiling : %7.1f MiB", intended)
		t.Logf("  BEFORE (subslice): %7.1f MiB  (%5.1fx intended)", before, before/intended)
		t.Logf("  AFTER  (clone)   : %7.1f MiB  (%5.1fx intended)", after, after/intended)
		t.Logf("  reduction        : %7.1f MiB  (%5.1fx smaller)", before-after, before/after)
		t.Logf("  @prod 32768 (x%d) : BEFORE %.0f MiB -> AFTER %.0f MiB", prodScale, before*prodScale, after*prodScale)
		if w.name[:9] == "streaming" {
			streamBefore, streamAfter = before, after
		}
	}

	// Headline guard: under the streaming worst case the subslice Add must pin
	// dramatically more than the clone fix (each entry pins a whole buffer).
	if streamBefore < streamAfter*5 {
		t.Fatalf("streaming worst case did not show the leak: before=%.1f after=%.1f (want before >= 5x after)",
			streamBefore, streamAfter)
	}
}

// runClustered: large reads at block-aligned offsets over a keyspace 4x the
// cache. A request's blocks share recency and evict together, so buffers stay
// fully resident while cached — aliasing has little to pin. Best case.
func runClustered(useClone bool) float64 {
	const (
		keyspace = cacheSize * 4
		numReads = 12000
	)
	rng := rand.New(rand.NewSource(42))
	maxStart := (keyspace - largeReq) / largeReq
	return drive(useClone, numReads, func() uint64 {
		return uint64(rng.Intn(maxStart) * largeReq) // aligned, well-separated
	})
}

// runRandom: overlapping unaligned reads over a hot region ~3x the cache.
// Successive reads share blocks with earlier ones and overwrite those entries,
// eroding older buffers. Modest overhead — typical case.
func runRandom(useClone bool) float64 {
	const (
		region   = cacheSize * 3
		numReads = 30000
	)
	rng := rand.New(rand.NewSource(42))
	maxStart := region - largeReq
	return drive(useClone, numReads, func() uint64 {
		return uint64(rng.Intn(maxStart)) // unaligned -> overlap
	})
}

// runStreaming: a read window advancing one block per read. Read s caches keys
// [s, s+largeReq); the next read overwrites keys [s+1, s+largeReq), leaving
// buffer s referenced only by key s. Every live cache entry therefore aliases a
// distinct backing buffer — maximal pinning. Worst case.
func runStreaming(useClone bool) float64 {
	const numReads = 40000
	var s uint64
	return drive(useClone, numReads, func() uint64 {
		cur := s
		s++ // slide by one block each read
		return cur
	})
}

// drive runs numReads large reads, each allocating a fresh backing buffer and
// caching its blocks at the offset returned by start(). Returns retained heap.
func drive(useClone bool, numReads int, start func() uint64) float64 {
	base := heapInuseMiB()
	cache, _ := lru.New[uint64, []byte](cacheSize)
	for range numReads {
		buf := newBuf(largeReq * blockSize)
		s := start()
		for b := range largeReq {
			addBlock(cache, s+uint64(b), buf, b, useClone)
		}
	}
	retained := heapInuseMiB() - base
	runtime.KeepAlive(cache)
	return retained
}

// addBlock mirrors viperblock.go:3925/4036: store the b-th 4 KiB block of buf.
// useClone=false stores a subslice (aliases buf); true stores an independent copy.
func addBlock(cache *lru.Cache[uint64, []byte], key uint64, buf []byte, b int, useClone bool) {
	val := buf[b*blockSize : (b+1)*blockSize]
	if useClone {
		val = clone(val)
	}
	cache.Add(key, val)
}

// newBuf allocates a per-request read buffer and faults both ends in, matching
// read()'s data = make([]byte, blockLen).
func newBuf(n int) []byte {
	buf := make([]byte, n)
	buf[0] = 1
	buf[n-1] = 1
	return buf
}

// clone matches viperblock's clone() helper (viperblock.go:3703): a fresh copy
// that does not alias the source's backing array.
func clone(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func heapInuseMiB() float64 {
	runtime.GC()
	runtime.GC() // second pass: finish sweep so freed buffers are not counted
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapInuse) / (1024 * 1024)
}
