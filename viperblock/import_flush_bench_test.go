package viperblock

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/require"
)

// These benchmarks reproduce the spinifex image-import-and-flush-to-disk path
// (v_utils.ImportDiskImage) so flush performance can be compared across commits
// with benchstat. Two variants are provided:
//
//   - BenchmarkImportImage_SpinifexStyle drives the file backend, isolating
//     viperblock's WAL->chunk flush to local disk with no network in the path.
//   - BenchmarkImportImage_Predastore drives the S3 backend against the shared
//     predastore test server started in TestMain. This is the real production
//     path: every flush uploads 4 MiB chunk objects that predastore then
//     erasure-codes and replicates over raft, so it captures upload cost, which
//     is the likely end-to-end bottleneck the file backend cannot see.
//
// Set VB_IMPORT_MB to tune the synthetic image size (default 256 MiB).

// silenceVBLogs discards all slog output for the remainder of the process.
// TestMain starts a predastore test server that installs a JSON logger on the
// process-wide slog default; left in place it interleaves with and corrupts the
// benchmark result lines on stdout. Re-asserting a discard handler at benchmark
// start (after TestMain has run) suppresses it without touching measured work.
func silenceVBLogs() {
	slog.SetDefault(slog.New(slog.DiscardHandler))
}

// importVolumeSeq gives each import iteration a unique volume name so chunks and
// state never collide, especially on the shared predastore server.
var importVolumeSeq atomic.Uint64

// importImageBytes is the synthetic raw-image size imported per iteration.
// Defaults to 256 MiB (roughly a minimal cloud image expanded to raw); override
// with VB_IMPORT_MB to model larger AMIs.
func importImageBytes(b *testing.B) uint64 {
	b.Helper()
	mib := 256
	if v := os.Getenv("VB_IMPORT_MB"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(b, err)
		require.Positive(b, n)
		mib = n
	}
	return uint64(mib) * 1024 * 1024
}

// makeSyntheticImage returns a deterministic, fully non-zero raw image. A fixed
// seed keeps the payload identical across the two compared commits, and
// non-zero content means every block is flushed (no zero-block skipping) so the
// benchmark measures the flush path at maximum load.
func makeSyntheticImage(size uint64) []byte {
	img := make([]byte, size)
	// math/rand/v2 has no Read, so fill 8 bytes at a time from a fixed-seed PCG
	// source; the trailing copy tolerates a size that is not a multiple of 8.
	src := rand.NewPCG(1, 2)
	for i := 0; i < len(img); i += 8 {
		var word [8]byte
		binary.LittleEndian.PutUint64(word[:], src.Uint64())
		copy(img[i:], word[:])
	}
	return img
}

// openImportWALs opens the chunk and block WALs the way ImportDiskImage does
// before its write loop. Shared by both backend variants.
func openImportWALs(b *testing.B, vb *VB) {
	b.Helper()

	walPath := fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))
	require.NoError(b, vb.OpenWAL(&vb.WAL, walPath))

	blockWALPath := fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))
	require.NoError(b, vb.OpenWAL(&vb.BlockToObjectWAL, blockWALPath))
}

// newImportVBFile builds a fresh file-backed volume under dir.
func newImportVBFile(b *testing.B, dir string, size uint64) *VB {
	b.Helper()

	vol := fmt.Sprintf("import_vol_%d", importVolumeSeq.Add(1))
	volSize := size + 64*1024*1024 // headroom past the image

	vbconfig := VB{
		VolumeName:      vol,
		VolumeSize:      volSize,
		BaseDir:         fmt.Sprintf("%s/viperblock", dir),
		WALSyncInterval: -1, // disable the async syncer; the import loop flushes explicitly
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}

	vb, err := New(&vbconfig, "file", file.FileConfig{
		VolumeName: vol,
		VolumeSize: volSize,
		BaseDir:    dir,
	})
	require.NoError(b, err)
	require.NoError(b, vb.Backend.Init())

	openImportWALs(b, vb)
	return vb
}

// newImportVBPredastore builds a fresh S3-backed volume against the shared
// predastore test server. The WAL is still staged on local disk (dir); Flush +
// WriteWALToChunk upload the sealed chunks to predastore.
func newImportVBPredastore(b *testing.B, dir string, size uint64) *VB {
	b.Helper()

	if sharedServerHost == "" {
		b.Skip("predastore test server not started (sharedServerHost empty)")
	}

	vol := fmt.Sprintf("import_vol_%d", importVolumeSeq.Add(1))
	volSize := size + 64*1024*1024

	vbconfig := VB{
		VolumeName:      vol,
		VolumeSize:      volSize,
		BaseDir:         fmt.Sprintf("%s/viperblock", dir),
		WALSyncInterval: -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}

	vb, err := New(&vbconfig, "s3", s3.S3Config{
		VolumeName: vol,
		VolumeSize: volSize,
		Region:     "ap-southeast-2",
		Bucket:     sharedBucket,
		AccessKey:  AccessKey,
		SecretKey:  SecretKey,
		Host:       fmt.Sprintf("https://%s", sharedServerHost),
		HTTPClient: testHTTPClient,
	})
	require.NoError(b, err)
	require.NoError(b, vb.Backend.Init())

	openImportWALs(b, vb)
	return vb
}

// importImage replays ImportDiskImage's core loop: sequential 4 KiB WriteAt,
// then Flush + WriteWALToChunk every 4096 blocks, with a final flush for the
// remainder. Zero blocks would be skipped by the real importer; the synthetic
// image is fully non-zero, so every block is written and flushed.
func importImage(b *testing.B, vb *VB, img []byte) {
	b.Helper()

	blockSize := uint64(vb.BlockSize)
	nblocks := uint64(len(img)) / blockSize

	for block := range nblocks {
		off := block * blockSize
		require.NoError(b, vb.WriteAt(off, img[off:off+blockSize]))

		// Flush every 4096 blocks, matching ImportDiskImage's cadence.
		if (block+1)%uint64(vb.BlockSize) == 0 {
			require.NoError(b, vb.Flush())
			require.NoError(b, vb.WriteWALToChunk(true))
		}
	}

	// Flush the trailing partial batch, completing the import.
	require.NoError(b, vb.Flush())
	require.NoError(b, vb.WriteWALToChunk(true))
}

// runImportBenchmark imports a full synthetic image into a fresh volume per
// iteration, flushing to disk as spinifex does. newVB selects the backend.
func runImportBenchmark(b *testing.B, newVB func(b *testing.B, dir string, size uint64) *VB) {
	silenceVBLogs()
	size := importImageBytes(b)
	img := makeSyntheticImage(size)
	b.SetBytes(int64(size))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		vb := newVB(b, dir, size)
		b.StartTimer()

		importImage(b, vb, img)

		b.StopTimer()
		_ = vb.Close()
		_ = os.RemoveAll(dir)
		b.StartTimer()
	}
}

// BenchmarkImportImage_SpinifexStyle measures the import-and-flush path on the
// file backend, isolating viperblock's flush logic from the network.
func BenchmarkImportImage_SpinifexStyle(b *testing.B) {
	runImportBenchmark(b, newImportVBFile)
}

// BenchmarkImportImage_Predastore measures the real end-to-end import path,
// including chunk upload to the predastore test cluster.
func BenchmarkImportImage_Predastore(b *testing.B) {
	runImportBenchmark(b, newImportVBPredastore)
}
