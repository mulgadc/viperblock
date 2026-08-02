package viperblock

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// Asymptotic benchmarks for the two extent indexes. Both answer range queries
// ("which extent contains block X", "which extents overlap [s,e)"), so their
// cost must grow no worse than logarithmically in the number of extents. A
// linear scan shows up here as ns/op rising in proportion to extentCount.
//
// Setup builds the containers directly rather than through SetPersistedRange /
// insertCoalescedLocked: those are themselves O(N) per insert, so a 1M-extent
// fixture would take O(N^2) to construct and the benchmark could not run at all.

// extentCounts is the sweep. Point lookups run the full range; mutation
// benchmarks stop earlier because a linear implementation takes minutes per
// operation at the top end.
var extentCounts = []int{1_000, 10_000, 100_000, 1_000_000}

const benchRunLen = 16 // blocks per extent, typical of a coalesced random-write volume

// buildPersistedFixture returns a store holding n disjoint extents of runLen
// blocks each, laid down back to back from block 0.
func buildPersistedFixture(n int, runLen uint16) *UnifiedBlockStore {
	ubs := NewUnifiedBlockStore(4096)
	for i := range n {
		start := uint64(i) * uint64(runLen)
		ubs.persistedExtents.set(persistedExtent{
			startBlock:   start,
			numBlocks:    runLen,
			objectID:     uint64(i / 64),
			objectOffset: uint32(i%64) * 4096,
			stride:       4096,
			seqNums:      make([]uint64, runLen),
		})
	}
	return ubs
}

// buildBlockLookupFixture is buildPersistedFixture's counterpart for the
// BlocksToObject index.
func buildBlockLookupFixture(n int, runLen uint16) *BlocksToObject {
	b := &BlocksToObject{}
	for i := range n {
		start := uint64(i) * uint64(runLen)
		b.lookup.set(BlockLookup{
			StartBlock:   start,
			NumBlocks:    runLen,
			ObjectID:     uint64(i / 64),
			ObjectOffset: uint32(i%64) * 4096,
			SeqNums:      make([]uint64, runLen),
		})
	}
	return b
}

// blockSpan is the highest block number covered by a fixture of n extents.
func blockSpan(n int, runLen uint16) uint64 {
	return uint64(n) * uint64(runLen)
}

// BenchmarkPersistedPointLookup measures the read path: resolving one block to
// its persisted location. This is the hottest single operation in the system —
// readBlockStore calls it once per 4 KiB block of every guest read.
func BenchmarkPersistedPointLookup(b *testing.B) {
	for _, n := range extentCounts {
		b.Run(fmt.Sprintf("extents=%d", n), func(b *testing.B) {
			ubs := buildPersistedFixture(n, benchRunLen)
			span := blockSpan(n, benchRunLen)
			rng := rand.New(rand.NewPCG(1, 1))

			// Draw the probe sequence up front so the RNG is not part of the
			// measurement, and reuse it across iterations.
			probes := make([]uint64, 1024)
			for i := range probes {
				probes[i] = rng.Uint64() % span
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				if _, ok := ubs.readPersisted(probes[i%len(probes)]); !ok {
					b.Fatal("probe missed a block that must be present")
				}
			}
		})
	}
}

// BenchmarkPersistedRandomOverwrite measures the write path: fracturing the
// index for a one-block overwrite. The overwrite covers an existing extent
// exactly, so the entry count stays constant across iterations and later
// iterations measure the same index size as the first.
func BenchmarkPersistedRandomOverwrite(b *testing.B) {
	for _, n := range extentCounts {
		b.Run(fmt.Sprintf("extents=%d", n), func(b *testing.B) {
			ubs := buildPersistedFixture(n, benchRunLen)
			rng := rand.New(rand.NewPCG(2, 2))

			starts := make([]uint64, 1024)
			for i := range starts {
				starts[i] = uint64(rng.IntN(n)) * uint64(benchRunLen)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				start := starts[i%len(starts)]
				ubs.persistedMu.Lock()
				ubs.fractureOverlapsLocked(start, benchRunLen)
				ubs.persistedExtents.set(persistedExtent{
					startBlock: start, numBlocks: benchRunLen,
					stride: 4096, seqNums: make([]uint64, benchRunLen),
				})
				ubs.persistedMu.Unlock()
			}
		})
	}
}

// BenchmarkPersistedRangeOverwrite fractures a multi-extent range, the shape a
// large sequential write produces. It replaces the extents it spans with one
// covering entry, so the index shrinks; the fixture is rebuilt periodically to
// keep the measured size honest.
func BenchmarkPersistedRangeOverwrite(b *testing.B) {
	const spanExtents = 8
	const resetEvery = 512

	for _, n := range extentCounts {
		b.Run(fmt.Sprintf("extents=%d", n), func(b *testing.B) {
			ubs := buildPersistedFixture(n, benchRunLen)
			rng := rand.New(rand.NewPCG(3, 3))
			width := uint16(benchRunLen * spanExtents)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				if i > 0 && i%resetEvery == 0 {
					b.StopTimer()
					ubs = buildPersistedFixture(n, benchRunLen)
					b.StartTimer()
				}
				start := uint64(rng.IntN(n-spanExtents)) * uint64(benchRunLen)
				ubs.persistedMu.Lock()
				ubs.fractureOverlapsLocked(start, width)
				ubs.persistedExtents.set(persistedExtent{
					startBlock: start, numBlocks: width,
					stride: 4096, seqNums: make([]uint64, width),
				})
				ubs.persistedMu.Unlock()
			}
		})
	}
}

// BenchmarkPersistedBulkLoad measures recovery: replaying a checkpoint through
// SetPersistedRange. Capped well below the other sweeps because a linear
// implementation makes this O(N^2) overall and 100k would not complete.
func BenchmarkPersistedBulkLoad(b *testing.B) {
	for _, n := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("extents=%d", n), func(b *testing.B) {
			blocks := make([]uint64, benchRunLen)
			seqNums := make([]uint64, benchRunLen)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				ubs := NewUnifiedBlockStore(4096)
				b.StartTimer()
				for i := range n {
					base := uint64(i) * uint64(benchRunLen)
					for k := range blocks {
						blocks[k] = base + uint64(k)
					}
					ubs.SetPersistedRange(blocks, uint64(i/64), 0, 4096, seqNums)
				}
			}
		})
	}
}

// BenchmarkBlockLookupResolve is BenchmarkPersistedPointLookup's counterpart on
// the BlocksToObject index.
func BenchmarkBlockLookupResolve(b *testing.B) {
	for _, n := range extentCounts {
		b.Run(fmt.Sprintf("extents=%d", n), func(b *testing.B) {
			bt := buildBlockLookupFixture(n, benchRunLen)
			span := blockSpan(n, benchRunLen)
			rng := rand.New(rand.NewPCG(4, 4))

			probes := make([]uint64, 1024)
			for i := range probes {
				probes[i] = rng.Uint64() % span
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				if _, _, ok := bt.resolveBlockLookup(probes[i%len(probes)]); !ok {
					b.Fatal("probe missed a block that must be present")
				}
			}
		})
	}
}

// BenchmarkBlockLookupInsertCoalesced measures insertCoalescedLocked, which
// runs once per consecutive run per chunk upload under a global write lock —
// so its cost is also the time every other writer and reader spends blocked.
func BenchmarkBlockLookupInsertCoalesced(b *testing.B) {
	for _, n := range extentCounts {
		b.Run(fmt.Sprintf("extents=%d", n), func(b *testing.B) {
			bt := buildBlockLookupFixture(n, benchRunLen)
			rng := rand.New(rand.NewPCG(5, 5))

			starts := make([]uint64, 1024)
			for i := range starts {
				starts[i] = uint64(rng.IntN(n)) * uint64(benchRunLen)
			}
			gcRefcount := make(map[uint64]uint64)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				start := starts[i%len(starts)]
				bt.mu.Lock()
				bt.insertCoalescedLocked(BlockLookup{
					StartBlock: start,
					NumBlocks:  benchRunLen,
					SeqNums:    make([]uint64, benchRunLen),
				}, 4096, gcRefcount)
				bt.mu.Unlock()
			}
		})
	}
}
