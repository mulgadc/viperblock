package viperblock

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

func BenchmarkBufferedWALFlush(b *testing.B) {
	silenceVBLogs()
	for _, workload := range []struct {
		records int
		callers int
	}{
		{1, 1},
		{63, 1},
		{64, 1},
		{256, 1},
		{64, 16},
	} {
		b.Run(fmt.Sprintf("records=%d/callers=%d", workload.records, workload.callers), func(b *testing.B) {
			vb := setupBenchVB(b, false)
			vb.StopChunkUploader()
			b.Cleanup(func() {
				for _, f := range vb.WAL.DB {
					_ = f.Close()
				}
				for _, f := range vb.BlockToObjectWAL.DB {
					_ = f.Close()
				}
			})
			blockSize := int(vb.BlockSize)
			payload := bytes.Repeat([]byte{0x92}, workload.records/workload.callers*blockSize)
			b.SetBytes(int64(workload.records * blockSize))
			b.ReportAllocs()
			latencies := make([]time.Duration, b.N*workload.callers)
			b.ResetTimer()
			for i := range b.N {
				var wg sync.WaitGroup
				for w := range workload.callers {
					run := func() {
						start := time.Now()
						if err := vb.WriteAt(uint64(w*len(payload)), payload); err != nil {
							b.Error(err)
							return
						}
						if err := vb.Flush(); err != nil {
							b.Error(err)
							return
						}
						latencies[i*workload.callers+w] = time.Since(start)
					}
					if workload.callers == 1 {
						run()
					} else {
						wg.Go(run)
					}
				}
				wg.Wait()
			}
			b.StopTimer()
			slices.Sort(latencies)
			b.ReportMetric(float64(latencies[(len(latencies)-1)*99/100].Nanoseconds()), "cycle-p99-ns")
		})
	}
}
