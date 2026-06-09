// Package writebench drives the REAL viperblock write/flush path (file
// backend, legacy single-file WAL) to quantify heavy-write latency and the
// flush-path memory growth described in
// docs/development/bugs/nbd-write-latency-and-flush-oom.md, and to compare the
// proposed fixes:
//
//	#1 demote hot-path INFO logging to Debug
//	#2 move chunk consolidation/upload off the guest-fsync path (async drain)
//	#3 bound the in-flight write buffer with backpressure
//
// It models the NBD plugin's per-op logging (one slog.Info per write, like
// PREAD/PWRITE at nbd/viperblock.go:258/277) and a slow object backend
// (S3/predastore RTT) so the synchronous-drain stall is visible. Lives in its
// own package so it does not inherit the viperblock package's predastore
// TestMain, and uses the legacy WAL (UseShardedWAL=false, WALSyncInterval=-1)
// which is the deadlock-free standalone recipe.
package writebench

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	vblib "github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
)

const (
	blockSize       = 4096
	volBytes        = 256 * 1024 * 1024 // 256 MiB volume
	numWrites       = 12000             // 12000 x 4 KiB = ~47 MiB written
	guestFlushEvery = 3000              // RARE guest fsync -> buffer grows (OOM regime)
	capBytes        = 4 * 1024 * 1024   // #3 backpressure threshold (4 MiB)
	backendDelayMs  = 3                 // simulated object-backend (S3) write RTT
)

type variant struct {
	name        string
	demoteLogs  bool // #1
	asyncDrain  bool // #2
	boundBuffer bool // #3
}

type result struct {
	totalMs     float64
	flushP50    float64
	flushP99    float64
	peakHeapMiB float64 // heap growth above post-setup baseline
	drains      int
}

func TestWritePathLatencyMemory(t *testing.T) {
	variants := []variant{
		{"baseline (current code)      ", false, false, false},
		{"#1 logs demoted              ", true, false, false},
		{"#2 async drain               ", false, true, false},
		{"#3 bounded buffer            ", false, false, true},
		{"#2+#3 async + bounded        ", false, true, true},
		{"#1+#2+#3 all fixes           ", true, true, true},
	}
	t.Logf("workload: %d x 4KiB writes, guest fsync every %d ops, backend RTT %dms, cap %dMiB",
		numWrites, guestFlushEvery, backendDelayMs, capBytes/(1024*1024))
	measureLoggingTax(t)
	for _, v := range variants {
		r := runVariant(t, v)
		t.Logf("%s total=%7.0fms  flush p50=%6.2f p99=%7.2fms  peakHeap=%6.1fMiB  drains=%d",
			v.name, r.totalMs, r.flushP50, r.flushP99, r.peakHeapMiB, r.drains)
	}
}

// measureLoggingTax isolates fix #1: the cost of one structured log per I/O op
// at INFO (current) vs Error (demoted). Logs to a temp file (real write
// syscalls); journald is slower still, so this understates the production win.
func measureLoggingTax(t *testing.T) {
	const n = 200000
	run := func(level slog.Level) time.Duration {
		lf, err := os.Create(filepath.Join(t.TempDir(), "tax.log"))
		if err != nil {
			t.Fatal(err)
		}
		defer lf.Close()
		slog.SetDefault(slog.New(slog.NewTextHandler(lf, &slog.HandlerOptions{Level: level})))
		start := time.Now()
		for i := range n {
			slog.Info("PWRITE", "offset", uint64(i)*blockSize, "len", blockSize)
		}
		return time.Since(start)
	}
	info := run(slog.LevelInfo)
	errl := run(slog.LevelError) // calls become no-ops below the handler level
	t.Logf("logging tax (#1): %d ops  INFO=%6.1fms (%4.0f ns/op)  Error=%5.1fms (%3.0f ns/op)  -> %.0fx cheaper",
		n, float64(info.Microseconds())/1000, float64(info.Nanoseconds())/n,
		float64(errl.Microseconds())/1000, float64(errl.Nanoseconds())/n,
		float64(info.Nanoseconds())/float64(errl.Nanoseconds()))
}

func runVariant(t *testing.T, v variant) result {
	// #1: per-op logging level. Logs go to a temp file (real write syscalls,
	// approximating journald) so the cost is realistic.
	lf, err := os.Create(filepath.Join(t.TempDir(), "vb.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer lf.Close()
	level := slog.LevelInfo
	if v.demoteLogs {
		level = slog.LevelError // fix: hot-path logs become no-ops
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(lf, &slog.HandlerOptions{Level: level})))

	vb := setupVB(t)
	defer vb.StopWALSyncer()

	// baseline heap after setup, before the workload
	base := heapInuse()

	// #2: background drainer. Foreground Flush only does the (fast, local) WAL
	// write; chunk consolidation/upload runs here, off the fsync path.
	drainCh := make(chan struct{}, 4096)
	var drainWG sync.WaitGroup
	if v.asyncDrain {
		drainWG.Go(func() {
			for range drainCh {
				_ = vb.WriteWALToChunk(false)
			}
		})
	}

	// peak-heap sampler
	stop := make(chan struct{})
	var peak uint64
	var sampleWG sync.WaitGroup
	sampleWG.Go(func() {
		tk := time.NewTicker(5 * time.Millisecond)
		defer tk.Stop()
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			case <-tk.C:
				runtime.ReadMemStats(&m)
				if m.HeapInuse > peak {
					peak = m.HeapInuse
				}
			}
		}
	})

	var flushLat []float64
	pending := 0
	drains := 0
	doFlush := func() {
		start := time.Now()
		_ = vb.Flush() // drains write buffer -> local WAL (fast)
		if v.asyncDrain {
			select {
			case drainCh <- struct{}{}: // hand chunk drain to background
			default:
			}
		} else {
			_ = vb.WriteWALToChunk(false) // synchronous: blocks the fsync
		}
		flushLat = append(flushLat, float64(time.Since(start).Microseconds())/1000)
		drains++
		pending = 0
	}

	t0 := time.Now()
	off := uint64(0)
	buf := make([]byte, blockSize)
	for i := range numWrites {
		buf[0] = byte(i)
		buf[blockSize-1] = byte(i)
		if err := vb.WriteAt(off, buf); err != nil {
			t.Fatal(err)
		}
		// models the NBD plugin's per-op INFO log (nbd/viperblock.go:258/277)
		slog.Info("PWRITE", "offset", off, "len", blockSize)
		pending += blockSize
		off += blockSize
		if off+blockSize > volBytes {
			off = 0
		}
		if (i+1)%guestFlushEvery == 0 {
			doFlush()
		}
		if v.boundBuffer && pending >= capBytes { // #3 backpressure
			doFlush()
		}
	}
	doFlush()

	if v.asyncDrain {
		close(drainCh)
		drainWG.Wait()
	}
	_ = vb.WriteWALToChunk(true) // persist remainder

	total := time.Since(t0)
	close(stop)
	sampleWG.Wait()

	sort.Float64s(flushLat)
	return result{
		totalMs:     float64(total.Microseconds()) / 1000,
		flushP50:    pct(flushLat, 50),
		flushP99:    pct(flushLat, 99),
		peakHeapMiB: float64(peak-base) / (1024 * 1024),
		drains:      drains,
	}
}

func setupVB(t *testing.T) *vblib.VB {
	dir := t.TempDir()
	vol := fmt.Sprintf("wbench_%d", time.Now().UnixNano())

	bcfg := file.FileConfig{VolumeName: vol, VolumeSize: volBytes, BaseDir: dir}
	cfg := &vblib.VB{
		VolumeName:      vol,
		VolumeSize:      volBytes,
		BaseDir:         filepath.Join(dir, "viperblock"),
		WALSyncInterval: -1, // legacy recipe: no background syncer deadlock
	}
	vb, err := vblib.New(cfg, "file", bcfg)
	if err != nil {
		t.Fatal(err)
	}
	vb.UseShardedWAL = false
	vb.ShardedWAL = nil

	if err := vb.Backend.Init(); err != nil {
		t.Fatal(err)
	}
	// Wrap the backend to simulate object-store (S3/predastore) write latency
	// on the chunk-persist path. WAL writes go straight to the local file and
	// are unaffected.
	vb.Backend = &slowBackend{Backend: vb.Backend, delay: backendDelayMs * time.Millisecond}

	walPath := fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))
	if err := vb.OpenWAL(&vb.WAL, walPath); err != nil {
		t.Fatal(err)
	}
	blkPath := fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))
	if err := vb.OpenWAL(&vb.BlockToObjectWAL, blkPath); err != nil {
		t.Fatal(err)
	}
	return vb
}

// slowBackend wraps a real backend and adds a fixed delay to object writes,
// modelling S3/predastore round-trip latency on the chunk-persist path.
type slowBackend struct {
	types.Backend

	delay time.Duration
}

func (s *slowBackend) Write(ft types.FileType, id uint64, headers *[]byte, data *[]byte) error {
	time.Sleep(s.delay)
	return s.Backend.Write(ft, id, headers, data)
}

func (s *slowBackend) WriteTo(vol string, ft types.FileType, id uint64, headers *[]byte, data *[]byte) error {
	time.Sleep(s.delay)
	return s.Backend.WriteTo(vol, ft, id, headers, data)
}

func pct(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := p * len(sorted) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func heapInuse() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}
