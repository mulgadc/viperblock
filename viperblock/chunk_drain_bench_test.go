package viperblock

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// chunkBlocks is one 4 MiB chunk at the 4 KiB block size: the unit
// createChunkFile classifies in one pass.
const chunkBlocks = 1024

// setupDrainBenchVB is setupBenchVB with a volume large enough that successive
// iterations can each drain a region no earlier one touched.
func setupDrainBenchVB(b *testing.B, size uint64) *VB {
	b.Helper()

	tmpDir := b.TempDir()
	testVol := fmt.Sprintf("bench_drain_%d", b.N)

	vbconfig := VB{
		VolumeName:      testVol,
		VolumeSize:      size,
		BaseDir:         fmt.Sprintf("%s/viperblock", tmpDir),
		WALSyncInterval: -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}

	vb, err := New(&vbconfig, "file", file.FileConfig{
		VolumeName: testVol,
		VolumeSize: size,
		BaseDir:    tmpDir,
	})
	require.NoError(b, err)
	require.NoError(b, vb.Backend.Init())

	require.NoError(b, vb.OpenWAL(&vb.WAL,
		fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, 0, vb.GetVolume()))))
	require.NoError(b, vb.OpenWAL(&vb.BlockToObjectWAL,
		fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	b.Cleanup(func() { _ = vb.RemoveLocalFiles() })

	return vb
}

// mappedVB returns a VB whose block map covers blocks [0, blocks) in extents of
// extentLen blocks: what a volume of that size looks like once written at that
// average extent length.
func mappedVB(blocks, extentLen uint64) *VB {
	vb := &VB{}
	for start := uint64(0); start < blocks; start += extentLen {
		vb.BlocksToObject.lookup.set(BlockLookup{
			StartBlock: start,
			NumBlocks:  uint16(extentLen),
			SeqNum:     start,
		})
	}
	return vb
}

// BenchmarkRunClassify isolates the block-map work createChunkFile does for one
// 4 MiB chunk landing entirely on already-mapped blocks, across volume size and
// fragmentation. Everything else in a drain -- encrypt, backend write, WAL --
// is excluded, because that I/O is what hides this cost end to end.
func BenchmarkRunClassify(b *testing.B) {
	sizes := []struct {
		name   string
		blocks uint64
	}{
		{"8GiB", 8 << 30 / 4096},
		{"250GiB", 250 << 30 / 4096},
	}
	// 32 KiB and 512 KiB average extents: how fragmented the map is decides how
	// many entries the ordered walk covers, where the per-block path pays one
	// tree descent regardless.
	extentLens := []uint64{8, 128}

	for _, size := range sizes {
		for _, extentLen := range extentLens {
			vb := mappedVB(size.blocks, extentLen)
			name := fmt.Sprintf("%s/extent%dblk", size.name, extentLen)

			// A chunk-sized run mid-volume, every candidate newer than what is
			// mapped, so neither implementation can exit early.
			base := size.blocks / 2
			run := make([]Block, chunkBlocks)
			for i := range run {
				run[i] = Block{Block: base + uint64(i), SeqNum: 1 << 40}
			}

			b.Run(name+"/perBlockLookup", func(b *testing.B) {
				for range b.N {
					for _, candidate := range run {
						if !vb.supersedesLocked(candidate) {
							b.Fatal("candidate must supersede")
						}
					}
				}
			})

			b.Run(name+"/orderedMerge", func(b *testing.B) {
				var (
					overlaps []BlockLookup
					verdict  []bool
				)
				for range b.N {
					overlaps = vb.BlocksToObject.lookup.collectOverlaps(overlaps[:0], run[0].Block, run[len(run)-1].Block+1)
					verdict = classifyRun(overlaps, run, verdict[:0])
					if len(verdict) != len(run) {
						b.Fatal("short verdict")
					}
				}
			})
		}
	}
}

// BenchmarkChunkDrain_FreshSequential drains one chunk per iteration into a
// region no earlier iteration touched, against a block map that keeps growing.
// This is what a guest filling a new volume does, and the case the whole-run
// no-overlap check exists for: every candidate is new, so a per-block
// classifier pays 1,024 tree descents to reach the verdict one range query
// gives.
func BenchmarkChunkDrain_FreshSequential(b *testing.B) {
	vb := setupDrainBenchVB(b, uint64(b.N+1)*chunkBlocks*4096)

	data := make([]byte, 4096)
	_, err := rand.Read(data)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		base := uint64(i) * chunkBlocks
		for j := range chunkBlocks {
			require.NoError(b, vb.WriteAt((base+uint64(j))*4096, data))
		}
		require.NoError(b, vb.Flush())
		b.StartTimer()

		require.NoError(b, vb.WriteWALToChunk(true))
	}
}

// BenchmarkChunkDrain_FullOverwrite drains the same region every iteration, so
// the map always covers it and the per-block classifier always runs. It is the
// control for FreshSequential: the no-overlap check must not cost anything
// measurable on the path it cannot take.
func BenchmarkChunkDrain_FullOverwrite(b *testing.B) {
	vb := setupDrainBenchVB(b, chunkBlocks*4096*2)

	data := make([]byte, 4096)
	_, err := rand.Read(data)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := range chunkBlocks {
			require.NoError(b, vb.WriteAt(uint64(j)*4096, data))
		}
		require.NoError(b, vb.Flush())
		b.StartTimer()

		require.NoError(b, vb.WriteWALToChunk(true))
	}
}
