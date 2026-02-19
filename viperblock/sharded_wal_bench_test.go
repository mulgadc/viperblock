package viperblock

import (
	"crypto/rand"
	"fmt"
	"sync"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// setupBenchVB creates a VB instance with file backend for benchmarking.
// Uses real file I/O to measure actual WAL write contention.
func setupBenchVB(b *testing.B, sharded bool) *VB {
	b.Helper()

	tmpDir := b.TempDir()
	testVol := fmt.Sprintf("bench_vol_%d", b.N)

	vbconfig := VB{
		VolumeName:      testVol,
		VolumeSize:      volumeSize,
		BaseDir:         fmt.Sprintf("%s/viperblock", tmpDir),
		WALSyncInterval: -1, // Disable syncer in benchmarks
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}

	vb, err := New(&vbconfig, "file", file.FileConfig{
		VolumeName: testVol,
		VolumeSize: volumeSize,
		BaseDir:    tmpDir,
	})
	require.NoError(b, err)

	err = vb.Backend.Init()
	require.NoError(b, err)

	if sharded {
		vb.UseShardedWAL = true
		vb.ShardedWAL = NewShardedWAL(vb.WAL.BaseDir, vb.WAL.WALMagic)
		err = vb.OpenShardedWAL()
		require.NoError(b, err)
	} else {
		walPath := fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, 0, vb.GetVolume()))
		err = vb.OpenWAL(&vb.WAL, walPath)
		require.NoError(b, err)
	}

	// Open BlockToObjectWAL too (needed by Flush path)
	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume())))
	require.NoError(b, err)

	return vb
}

func BenchmarkWrite_Sequential_LegacyWAL(b *testing.B) {
	vb := setupBenchVB(b, false)

	data := make([]byte, 4096)
	rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vb.WriteWAL(Block{
			SeqNum: uint64(i),
			Block:  uint64(i),
			Len:    4096,
			Data:   data,
		})
	}
}

func BenchmarkWrite_Sequential_ShardedWAL(b *testing.B) {
	vb := setupBenchVB(b, true)

	data := make([]byte, 4096)
	rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vb.WriteShardedWAL(Block{
			SeqNum: uint64(i),
			Block:  uint64(i),
			Len:    4096,
			Data:   data,
		})
	}
}

func BenchmarkWrite_Concurrent8_LegacyWAL(b *testing.B) {
	vb := setupBenchVB(b, false)

	data := make([]byte, 4096)
	rand.Read(data)

	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			i++
			vb.WriteWAL(Block{
				SeqNum: i,
				Block:  i,
				Len:    4096,
				Data:   data,
			})
		}
	})
}

func BenchmarkWrite_Concurrent8_ShardedWAL(b *testing.B) {
	vb := setupBenchVB(b, true)

	data := make([]byte, 4096)
	rand.Read(data)

	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i uint64
		for pb.Next() {
			i++
			vb.WriteShardedWAL(Block{
				SeqNum: i,
				Block:  i,
				Len:    4096,
				Data:   data,
			})
		}
	})
}

func BenchmarkFlush_LegacyWAL(b *testing.B) {
	vb := setupBenchVB(b, false)

	data := make([]byte, 4096)
	rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range 64 {
			vb.WriteAt(uint64(j)*4096, data)
		}
		vb.Flush()
	}
}

func BenchmarkFlush_ShardedParallel(b *testing.B) {
	vb := setupBenchVB(b, true)

	data := make([]byte, 4096)
	rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range 64 {
			vb.WriteAt(uint64(j)*4096, data)
		}
		vb.Flush()
	}
}

func BenchmarkWriteWALToChunk_Sharded(b *testing.B) {
	vb := setupBenchVB(b, true)

	data := make([]byte, 4096)
	rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var wg sync.WaitGroup
		for t := range 8 {
			wg.Add(1)
			go func(threadID int) {
				defer wg.Done()
				for j := range 128 {
					blockNum := uint64(threadID*128 + j)
					vb.WriteShardedWAL(Block{
						SeqNum: blockNum,
						Block:  blockNum,
						Len:    4096,
						Data:   data,
					})
				}
			}(t)
		}
		wg.Wait()
		b.StartTimer()

		vb.WriteShardedWALToChunk(true)
	}
}
