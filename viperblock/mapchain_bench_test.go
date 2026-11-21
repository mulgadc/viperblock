package viperblock

import (
	"context"
	"fmt"
	"testing"
)

// mapBenchShapes are the maps a drain persists against: a small sequential
// volume, a large sequential one, and a small one written fully at random.
var mapBenchShapes = []struct {
	name     string
	runs     int
	runBlock uint16
}{
	{"small-1k-runs-4GiB", 1_000, 1024},
	{"large-10k-runs-40GiB", 10_000, 1024},
	{"fragmented-1M-runs-4GiB-random", 1_000_000, 1},
}

func fillBenchMap(vb *VB, runs int, runBlocks uint16) {
	vb.BlocksToObject.mu.Lock()
	defer vb.BlocksToObject.mu.Unlock()
	for i := range runs {
		seqs := make([]uint64, runBlocks)
		for j := range seqs {
			seqs[j] = uint64(i)*uint64(runBlocks) + uint64(j) + 1
		}
		vb.BlocksToObject.lookup.set(BlockLookup{
			StartBlock: uint64(i) * uint64(runBlocks) * 2, NumBlocks: runBlocks,
			ObjectID: uint64(i), ObjectOffset: 64, SeqNum: seqs[0], SeqNums: seqs,
		})
	}
	vb.ObjectNum.Store(uint64(runs) + 1)
}

// benchChange installs one changed run, as a drain of a small write does.
func benchChange(vb *VB, i int) {
	vb.BlocksToObject.mu.Lock()
	defer vb.BlocksToObject.mu.Unlock()
	r := BlockLookup{StartBlock: uint64(i%1000) * 3, NumBlocks: 1, ObjectID: vb.ObjectNum.Add(1), ObjectOffset: 64, SeqNum: uint64(1<<40 + i)}
	vb.BlocksToObject.insertCoalescedLocked(r, vb.blockStride(), nil)
	if vb.BlocksToObject.trackChanges {
		vb.BlocksToObject.changes = append(vb.BlocksToObject.changes, r)
	}
	vb.BlocksToObject.dirty.Store(true)
}

func BenchmarkMapPersistPerDrain(b *testing.B) {
	for _, shape := range mapBenchShapes {
		for _, format := range []string{"v1-full-checkpoint", "v2-delta"} {
			b.Run(fmt.Sprintf("%s/%s", shape.name, format), func(b *testing.B) {
				mv := newMapVolume(b, 1<<50, nil)
				var vb *VB
				if format == "v2-delta" {
					vb, _ = mv.create(b, 5)
				} else {
					vb, _ = mv.engine(b, 5)
				}
				fillBenchMap(vb, shape.runs, shape.runBlock)
				ctx := context.Background()
				// Settle the first commit outside the timer.
				benchChange(vb, 0)
				if err := vb.persistMapCtx(ctx); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					benchChange(vb, i+1)
					if err := vb.persistMapCtx(ctx); err != nil {
						b.Fatal(err)
					}
					// Keep the format-2 head bounded as the background base would.
					if format == "v2-delta" && i%256 == 255 {
						b.StopTimer()
						vb.checkpointMu.Lock()
						vb.BlocksToObject.mu.Lock()
						runs := vb.mapRunsLocked()
						vb.BlocksToObject.mu.Unlock()
						_ = vb.commitMapBaseLocked(ctx, runs)
						vb.checkpointMu.Unlock()
						b.StartTimer()
					}
				}
			})
		}
	}
}

func BenchmarkMapOpenFromBaseAndDeltas(b *testing.B) {
	for _, deltas := range []int{0, 64, 512} {
		b.Run(fmt.Sprintf("10k-run-base/%d-deltas", deltas), func(b *testing.B) {
			mv := newMapVolume(b, 1<<50, nil)
			vb, _ := mv.create(b, 5)
			fillBenchMap(vb, 10_000, 1024)
			ctx := context.Background()
			vb.checkpointMu.Lock()
			vb.BlocksToObject.mu.Lock()
			runs := vb.mapRunsLocked()
			vb.BlocksToObject.mu.Unlock()
			if err := vb.commitMapBaseLocked(ctx, runs); err != nil {
				b.Fatal(err)
			}
			vb.checkpointMu.Unlock()
			for i := range deltas {
				for j := range 32 {
					benchChange(vb, i*32+j)
				}
				if err := vb.persistMapCtx(ctx); err != nil {
					b.Fatal(err)
				}
			}
			if err := vb.SaveState(); err != nil {
				b.Fatal(err)
			}
			vb.Detach()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				re, _ := mv.engine(b, 6)
				if err := re.LoadState(); err != nil {
					b.Fatal(err)
				}
				if err := re.LoadLiveCheckpoint(); err != nil {
					b.Fatal(err)
				}
				re.Detach()
			}
		})
	}
}

// BenchmarkMapDrainDuringBase times drains while background bases run: every
// 64 drains the base falls due by age and is written concurrently, with the
// timer running, from the copy-on-write snapshot the drain takes.
func BenchmarkMapDrainDuringBase(b *testing.B) {
	for _, shape := range mapBenchShapes[1:] {
		b.Run(shape.name, func(b *testing.B) {
			mv := newMapVolume(b, 1<<50, nil)
			vb, _ := mv.create(b, 5)
			fillBenchMap(vb, shape.runs, shape.runBlock)
			ctx := context.Background()
			benchChange(vb, 0)
			if err := vb.persistMapCtx(ctx); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%64 == 0 {
					mv.clock.advance(mapBaseMaxAge)
				}
				benchChange(vb, i+1)
				if err := vb.persistMapCtx(ctx); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			vb.waitMapBackground()
		})
	}
}
