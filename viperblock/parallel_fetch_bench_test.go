package viperblock

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// The benchmark volume: blocks below pfbFragBlocks alternate between chunks,
// so every block is its own run; the rest are one contiguous chunk.
const (
	pfbVolumeBytes = 64 << 20
	pfbBlocks      = pfbVolumeBytes / 4096
	pfbFragBlocks  = pfbBlocks / 2
	pfbWindow      = 256
	pfbWindows     = pfbFragBlocks / pfbWindow
	pfbWritesPerOp = 256
)

// pfbDelayBackend adds a fixed delay to every chunk read, so the benchmark
// compares how fetches are scheduled rather than local disk speed.
type pfbDelayBackend struct {
	types.Backend

	delay time.Duration

	// soloIO, when set, receives one signal per payload read made for a
	// context carrying pfbSoloKey.
	soloIO chan struct{}
}

type pfbSoloKey struct{}

var _ types.Backend = (*pfbDelayBackend)(nil)

func (d *pfbDelayBackend) wait(ctx context.Context) error {
	if d.soloIO != nil && ctx.Value(pfbSoloKey{}) != nil {
		select {
		case d.soloIO <- struct{}{}:
		default:
		}
	}
	if d.delay <= 0 {
		return nil
	}
	timer := time.NewTimer(d.delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *pfbDelayBackend) Read(fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	return d.ReadCtx(context.Background(), fileType, objectID, offset, length)
}

func (d *pfbDelayBackend) ReadCtx(ctx context.Context, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	if fileType == types.FileTypeChunk {
		if err := d.wait(ctx); err != nil {
			return nil, err
		}
	}
	return d.Backend.ReadCtx(ctx, fileType, objectID, offset, length)
}

func (d *pfbDelayBackend) ReadFromCtx(ctx context.Context, volume string, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	if fileType == types.FileTypeChunk {
		if err := d.wait(ctx); err != nil {
			return nil, err
		}
	}
	return d.Backend.ReadFromCtx(ctx, volume, fileType, objectID, offset, length)
}

func pfbConfig(name, baseDir string, cacheBlocks int) VB {
	return VB{
		VolumeName:      name,
		VolumeSize:      pfbVolumeBytes,
		BaseDir:         baseDir,
		WALSyncInterval: -1,
		GCEnabled:       false,
		GCInterval:      -1,
		Cache:           Cache{Config: CacheConfig{Size: cacheBlocks}},
	}
}

func pfbOpenWALs(b *testing.B, vb *VB) {
	b.Helper()
	require.NoError(b, vb.OpenWAL(&vb.WAL, filepath.Join(vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(b, vb.OpenWAL(&vb.BlockToObjectWAL, filepath.Join(vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))
}

// pfbSeed writes the even then the odd fragmented blocks, then the contiguous
// half, each as its own chunk set, and returns the backend root.
func pfbSeed(b *testing.B, name string) string {
	b.Helper()
	root := b.TempDir()
	cfg := pfbConfig(name, filepath.Join(root, "viperblock"), 0)
	writer, err := New(&cfg, FileBackend, file.FileConfig{VolumeName: name, VolumeSize: pfbVolumeBytes, BaseDir: root})
	require.NoError(b, err)
	require.NoError(b, writer.Backend.Init())
	pfbOpenWALs(b, writer)

	bs := uint64(DefaultBlockSize)
	data := randomBlockData(pfbBlocks)
	phases := [][2]uint64{{0, 2}, {1, 2}, {pfbFragBlocks, 1}}
	for _, phase := range phases {
		end := uint64(pfbFragBlocks)
		if phase[0] >= pfbFragBlocks {
			end = pfbBlocks
		}
		for block := phase[0]; block < end; block += phase[1] {
			require.NoError(b, writer.Write(block, data[block*bs:(block+1)*bs]))
		}
		require.NoError(b, writer.Flush())
		require.NoError(b, writer.WriteWALToChunk(true))
	}
	require.NoError(b, writer.Close())
	return root
}

// pfbOpen opens the seeded volume cold behind a delayed backend.
func pfbOpen(b *testing.B, root, name string, cacheBlocks int, delay time.Duration, writable bool) *VB {
	b.Helper()
	cfg := pfbConfig(name, filepath.Join(b.TempDir(), "viperblock"), cacheBlocks)
	vb, err := New(&cfg, FileBackend, file.FileConfig{VolumeName: name, VolumeSize: pfbVolumeBytes, BaseDir: root})
	require.NoError(b, err)
	b.Cleanup(func() { _ = vb.RemoveLocalFiles() })
	fb, ok := vb.Backend.(*file.Backend)
	require.True(b, ok)
	require.NoError(b, fb.Init())
	require.NoError(b, vb.LoadState())
	require.NoError(b, vb.LoadBlockState())
	if writable {
		pfbOpenWALs(b, vb)
	}
	vb.Backend = &pfbDelayBackend{Backend: fb, delay: delay}
	return vb
}

// pfbConcurrent runs n reads at once and fails the benchmark on any error.
func pfbConcurrent(b *testing.B, n int, read func(j int) error) {
	var wg sync.WaitGroup
	errs := make([]error, n)
	for j := range n {
		wg.Go(func() { errs[j] = read(j) })
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParallelFetch compares read scheduling on the default block store
// for warmed, single-run, contiguous and fragmented reads, concurrent misses,
// and partial writes under fragmented read load.
func BenchmarkParallelFetch(b *testing.B) {
	const name = "vol-pfb"
	root := pfbSeed(b, name)
	bs := uint64(DefaultBlockSize)
	window := uint64(pfbWindow) * bs
	ctx := context.Background()

	for _, delay := range []time.Duration{0, time.Millisecond} {
		prefix := fmt.Sprintf("delay=%v/", delay)

		b.Run(prefix+"warm-4KiB", func(b *testing.B) {
			vb := pfbOpen(b, root, name, 1024, delay, false)
			offset := uint64(pfbFragBlocks) * bs
			_, err := vb.ReadAtCtx(ctx, offset, bs)
			require.NoError(b, err)
			b.SetBytes(int64(bs))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := vb.ReadAtCtx(ctx, offset, bs); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(prefix+"miss-4KiB", func(b *testing.B) {
			vb := pfbOpen(b, root, name, 0, delay, false)
			b.SetBytes(int64(bs))
			b.ReportAllocs()
			i := uint64(0)
			for b.Loop() {
				block := pfbFragBlocks + i%pfbFragBlocks
				i++
				if _, err := vb.ReadAtCtx(ctx, block*bs, bs); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(prefix+"contig-1MiB", func(b *testing.B) {
			vb := pfbOpen(b, root, name, 0, delay, false)
			b.SetBytes(int64(window))
			b.ReportAllocs()
			i := uint64(0)
			for b.Loop() {
				offset := uint64(pfbFragBlocks)*bs + (i%pfbWindows)*window
				i++
				if _, err := vb.ReadAtCtx(ctx, offset, window); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(prefix+"frag-1MiB-qd1", func(b *testing.B) {
			vb := pfbOpen(b, root, name, 0, delay, false)
			b.SetBytes(int64(window))
			b.ReportAllocs()
			i := uint64(0)
			for b.Loop() {
				offset := (i % pfbWindows) * window
				i++
				if _, err := vb.ReadAtCtx(ctx, offset, window); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(prefix+"frag-1MiB-qd16", func(b *testing.B) {
			vb := pfbOpen(b, root, name, 0, delay, false)
			b.SetBytes(16 * int64(window))
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				pfbConcurrent(b, 16, func(j int) error {
					_, err := vb.ReadAtCtx(ctx, uint64((i*16+j)%pfbWindows)*window, window)
					return err
				})
				i++
			}
		})

		b.Run(prefix+"miss-4KiB-x32", func(b *testing.B) {
			vb := pfbOpen(b, root, name, 0, delay, false)
			b.SetBytes(32 * int64(bs))
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				pfbConcurrent(b, 32, func(j int) error {
					block := uint64(pfbFragBlocks + (i*32+j)%pfbFragBlocks)
					_, err := vb.ReadAtCtx(ctx, block*bs, bs)
					return err
				})
				i++
			}
		})

		b.Run(prefix+"frag-32MiB", func(b *testing.B) {
			vb := pfbOpen(b, root, name, 0, delay, false)
			length := uint64(pfbFragBlocks) * bs
			b.SetBytes(int64(length))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := vb.ReadAtCtx(ctx, 0, length); err != nil {
					b.Fatal(err)
				}
			}
		})

		// Each op is pfbWritesPerOp partial writes to distinct persisted blocks,
		// so every write's RMW read misses, while 16 readers keep fragmented
		// 1 MiB reads in flight on the same volume.
		b.Run(prefix+"partial-write-under-frag-load", func(b *testing.B) {
			b.ReportAllocs()
			patch := make([]byte, 512)
			var vb *VB
			var stop chan struct{}
			var wg sync.WaitGroup
			var bgReads atomic.Int64
			start := func() {
				vb = pfbOpen(b, root, name, 0, delay, true)
				stop = make(chan struct{})
				for j := range 16 {
					wg.Go(func() {
						for k := j; ; k += 16 {
							select {
							case <-stop:
								return
							default:
							}
							if _, err := vb.ReadAtCtx(ctx, uint64(k%pfbWindows)*window, window); err != nil {
								b.Error(err)
								return
							}
							bgReads.Add(1)
						}
					})
				}
			}
			halt := func() {
				close(stop)
				wg.Wait()
			}
			groups := pfbFragBlocks / pfbWritesPerOp
			start()
			op := 0
			began := time.Now()
			b.ResetTimer()
			for b.Loop() {
				if op > 0 && op%groups == 0 {
					b.StopTimer()
					halt()
					start()
					b.StartTimer()
				}
				base := uint64(pfbFragBlocks + (op%groups)*pfbWritesPerOp)
				for w := range uint64(pfbWritesPerOp) {
					if err := vb.WriteAt((base+w)*bs+512, patch); err != nil {
						b.Fatal(err)
					}
				}
				op++
			}
			b.StopTimer()
			elapsed := time.Since(began)
			halt()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*pfbWritesPerOp), "ns/write")
			b.ReportMetric(float64(bgReads.Load())/elapsed.Seconds(), "bg-reads/s")
		})

		// Each op starts a sole cold fragmented 1 MiB read, waits until it is in
		// backend I/O, then times one arriving operation on another block.
		arrival := func(caseName string, writable bool, arrive func(vb *VB, op int) error) {
			b.Run(prefix+caseName, func(b *testing.B) {
				b.ReportAllocs()
				open := func() (*VB, *pfbDelayBackend) {
					vb := pfbOpen(b, root, name, 0, delay, writable)
					db := vb.Backend.(*pfbDelayBackend)
					db.soloIO = make(chan struct{}, 1)
					return vb, db
				}
				vb, db := open()
				soloCtx := context.WithValue(ctx, pfbSoloKey{}, true)
				var arrivalTotal time.Duration
				op := 0
				for b.Loop() {
					if op > 0 && op%pfbFragBlocks == 0 {
						b.StopTimer()
						vb, db = open()
						b.StartTimer()
					}
					select {
					case <-db.soloIO:
					default:
					}
					soloDone := make(chan error, 1)
					go func() {
						_, err := vb.ReadAtCtx(soloCtx, uint64(op%pfbWindows)*window, window)
						soloDone <- err
					}()
					<-db.soloIO
					began := time.Now()
					if err := arrive(vb, op); err != nil {
						b.Fatal(err)
					}
					arrivalTotal += time.Since(began)
					if err := <-soloDone; err != nil {
						b.Fatal(err)
					}
					op++
				}
				b.ReportMetric(float64(arrivalTotal.Nanoseconds())/float64(b.N), "arrival-ns/op")
			})
		}
		arrival("arrival-miss-4KiB", false, func(vb *VB, op int) error {
			block := uint64(pfbFragBlocks + op%pfbFragBlocks)
			_, err := vb.ReadAtCtx(ctx, block*bs, bs)
			return err
		})
		patch := make([]byte, 512)
		arrival("arrival-partial-write", true, func(vb *VB, op int) error {
			block := uint64(pfbFragBlocks + op%pfbFragBlocks)
			return vb.WriteAt(block*bs+512, patch)
		})
	}
}
