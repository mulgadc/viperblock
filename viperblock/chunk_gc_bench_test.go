package viperblock

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// setupGCBenchVB is setupDrainBenchVB with chunk GC on or off and no periodic
// sweep, so a benchmark decides when a sweep runs.
func setupGCBenchVB(b *testing.B, size uint64, gc bool) *VB {
	b.Helper()

	tmpDir := b.TempDir()
	testVol := fmt.Sprintf("bench_gc_%d", b.N)

	vbconfig := VB{
		VolumeName:      testVol,
		VolumeSize:      size,
		BaseDir:         fmt.Sprintf("%s/viperblock", tmpDir),
		WALSyncInterval: -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
		GCEnabled:       gc,
		GCInterval:      -1,
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

// overwriteChunk writes one full chunk of blocks from block 0 and flushes it,
// outside the timer; the timed part is the drain that repoints the map.
func overwriteChunk(b *testing.B, vb *VB, data []byte) {
	b.Helper()
	b.StopTimer()
	for j := range chunkBlocks {
		require.NoError(b, vb.WriteAt(uint64(j)*4096, data))
	}
	require.NoError(b, vb.Flush())
	b.StartTimer()
	require.NoError(b, vb.WriteWALToChunk(true))
}

// BenchmarkChunkDrain_FullOverwriteGC measures the refcount work chunk GC adds
// to a drain that supersedes a whole chunk.
func BenchmarkChunkDrain_FullOverwriteGC(b *testing.B) {
	for _, gc := range []bool{false, true} {
		b.Run(fmt.Sprintf("gc=%t", gc), func(b *testing.B) {
			vb := setupGCBenchVB(b, chunkBlocks*4096*2, gc)
			data := make([]byte, 4096)
			_, err := rand.Read(data)
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				overwriteChunk(b, vb, data)
			}
		})
	}
}

// BenchmarkChunkDrain_FullOverwriteDuringSweep runs the same drains while a
// goroutine sweeps in a loop, so drains contend with sweeps for drainMu.
func BenchmarkChunkDrain_FullOverwriteDuringSweep(b *testing.B) {
	vb := setupGCBenchVB(b, chunkBlocks*4096*2, true)
	data := make([]byte, 4096)
	_, err := rand.Read(data)
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		for ctx.Err() == nil {
			vb.runGCSweep(ctx)
		}
	})
	b.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		overwriteChunk(b, vb, data)
	}
	b.StopTimer()
	cancel()
	wg.Wait()
}
