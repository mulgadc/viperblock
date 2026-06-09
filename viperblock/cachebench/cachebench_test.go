// Package cachebench drives the REAL viperblock read path (file backend,
// UseBlockStore=true — the production NBD default) to guard against the
// read-cache buffer-aliasing leak fixed at viperblock.go:3925 & :4036.
//
// The bug: readBlockStore allocates one large per-request buffer
//
//	data := make([]byte, blockLen)                       // viperblock.go:3753
//
// and previously cached each 4 KiB block as a SUBSLICE of it
//
//	vb.Cache.lru.Add(block, data[blockStart:blockEnd])   // pins all of data
//
// In Go a subslice retains its whole backing array, so a single live cache
// entry pins the entire request buffer. Under streaming/prefetch I/O (a read
// window sliding one block at a time) every surviving LRU entry ends up
// aliasing its own distinct request buffer, so retained heap balloons to
// roughly cacheSize*requestBytes instead of cacheSize*blockSize — the nbdkit
// RSS blowup / host OOM. The fix wraps each insert in clone() so the LRU owns
// an independent BlockSize buffer.
//
// Unlike a synthetic model, this test exercises vb.ReadAt -> read ->
// readBlockStore against a real persisted volume, so it FAILS if the clone()
// fix is reverted: it asserts the post-streaming retained heap stays within a
// multiple of the intended cache footprint, a bound the subslice variant
// (~16x larger here) cannot satisfy.
//
// Lives in its own package so it does not inherit the viperblock package's
// TestMain (predastore server); the file backend keeps it hermetic.
package cachebench

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	vblib "github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
)

const (
	blockSize = 4096 // viperblock DefaultBlockSize
	cacheSize = 4096 // LRU entry cap -> intended ceiling = cacheSize*blockSize = 16 MiB
	largeReq  = 32   // blocks per ReadAt -> 128 KiB request buffer (the thing a subslice pins)
	numReads  = cacheSize + 512
	allBlocks = numReads + largeReq // distinct blocks written/persisted

	// retainFactor bounds legitimate retained heap (clone LRU + never-evicted
	// BlockStore Cached copies) as a multiple of (cacheSize+allBlocks) blocks.
	// The reverted subslice variant retains ~cacheSize*largeReq*blockSize
	// (512 MiB here), far beyond this bound.
	retainFactor = 2.5
)

// TestReadCacheDoesNotPinRequestBuffers persists a volume, then issues a
// sliding-window stream of multi-block ReadAt calls and checks that the read
// cache did not pin the per-request buffers. Guards viperblock.go:3925 (and the
// identical pattern at :3455 legacy / :4036 snapshot-base).
func TestReadCacheDoesNotPinRequestBuffers(t *testing.T) {
	vb := setupVB(t)

	// Write every block once and force it out to the backend so reads come back
	// as BlockStatePersisted (BlockStore data freed) and populate the cache.
	buf := make([]byte, blockSize)
	for b := range allBlocks {
		bn := uint64(b)
		buf[0], buf[blockSize-1] = byte(bn), byte(bn>>8)
		if err := vb.WriteAt(bn*blockSize, buf); err != nil {
			t.Fatalf("WriteAt block %d: %v", bn, err)
		}
	}
	if err := vb.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := vb.WriteWALToChunk(true); err != nil {
		t.Fatalf("WriteWALToChunk: %v", err)
	}

	// Baseline after persist: LRU empty, BlockStore entries Persisted (no data).
	base := heapInuseMiB()

	// Streaming read: window slides one block per read. Each read pulls exactly
	// one new leading block from the backend (the rest are already Cached), so
	// each surviving LRU entry maps to a distinct request buffer — maximal
	// pinning if the cache stored subslices.
	for s := range numReads {
		sn := uint64(s)
		if _, err := vb.ReadAt(sn*blockSize, largeReq*blockSize); err != nil && !errors.Is(err, vblib.ErrZeroBlock) {
			t.Fatalf("ReadAt offset-block %d: %v", sn, err)
		}
	}

	retained := heapInuseMiB() - base
	runtime.KeepAlive(vb)

	intended := float64(cacheSize*blockSize) / (1024 * 1024)
	// Legitimate footprint with the clone fix: the bounded LRU plus the
	// (currently never-evicted) BlockStore Cached copies of every block read.
	legit := float64((cacheSize+allBlocks)*blockSize) / (1024 * 1024)
	threshold := legit * retainFactor
	subsliceWorst := float64(cacheSize*largeReq*blockSize) / (1024 * 1024)

	t.Logf("intended LRU ceiling : %7.1f MiB (cacheSize=%d x %d B)", intended, cacheSize, blockSize)
	t.Logf("retained after stream: %7.1f MiB", retained)
	t.Logf("clone threshold      : %7.1f MiB (legit %.1f x %.1f)", threshold, legit, retainFactor)
	t.Logf("subslice worst case  : %7.1f MiB (what a revert would retain)", subsliceWorst)

	if retained > threshold {
		t.Fatalf("read cache retained %.1f MiB > %.1f MiB threshold: entries are pinning whole "+
			"request buffers — the clone() fix at viperblock.go:3925/4036 is missing or reverted",
			retained, threshold)
	}
}

func setupVB(t *testing.T) *vblib.VB {
	t.Helper()
	dir := t.TempDir()
	vol := fmt.Sprintf("cbench_%d", time.Now().UnixNano())
	volBytes := uint64(allBlocks+largeReq) * blockSize

	bcfg := file.FileConfig{VolumeName: vol, VolumeSize: volBytes, BaseDir: dir}
	cfg := &vblib.VB{
		VolumeName:      vol,
		VolumeSize:      volBytes,
		BaseDir:         filepath.Join(dir, "viperblock"),
		WALSyncInterval: -1, // standalone recipe: no background syncer
		Cache:           vblib.Cache{Config: vblib.CacheConfig{Size: cacheSize}},
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

func heapInuseMiB() float64 {
	runtime.GC()
	runtime.GC() // second pass: finish sweep so freed buffers are not counted
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapInuse) / (1024 * 1024)
}
