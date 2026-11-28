package viperblock

import (
	"context"
	"fmt"
	"io/fs"
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

	// pfbSoloWidth is the most fetches a sole read keeps in flight on any arm.
	pfbSoloWidth = 8
	pfbQuiet     = 25 * time.Millisecond
	pfbStall     = 10 * time.Second
)

// pfbDelayBackend adds a fixed delay to every chunk read, so the benchmark
// compares how fetches are scheduled rather than local disk speed.
type pfbDelayBackend struct {
	types.Backend

	delay time.Duration
	gate  atomic.Pointer[pfbSoloGate]
}

type pfbSoloKey struct{}

// pfbSoloGate holds every chunk payload read of the tagged sole read until
// release closes, and signals each held read and each untagged read.
type pfbSoloGate struct {
	held    chan struct{}
	other   chan struct{}
	release chan struct{}
}

func newPfbSoloGate() *pfbSoloGate {
	return &pfbSoloGate{
		held:    make(chan struct{}, pfbBlocks),
		other:   make(chan struct{}, pfbBlocks),
		release: make(chan struct{}),
	}
}

var _ types.Backend = (*pfbDelayBackend)(nil)

func (d *pfbDelayBackend) wait(ctx context.Context, length uint32) error {
	if g := d.gate.Load(); g != nil && length != 4 {
		if ctx.Value(pfbSoloKey{}) == nil {
			g.other <- struct{}{}
		} else {
			g.held <- struct{}{}
			select {
			case <-g.release:
			case <-ctx.Done():
				return ctx.Err()
			}
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
		if err := d.wait(ctx, length); err != nil {
			return nil, err
		}
	}
	return d.Backend.ReadCtx(ctx, fileType, objectID, offset, length)
}

func (d *pfbDelayBackend) ReadFromCtx(ctx context.Context, volume string, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	if fileType == types.FileTypeChunk {
		if err := d.wait(ctx, length); err != nil {
			return nil, err
		}
	}
	return d.Backend.ReadFromCtx(ctx, volume, fileType, objectID, offset, length)
}

// pfbConfig disables every background task, so no engine uploads to the
// backend while it is open or after it is retired.
func pfbConfig(name, baseDir string, cacheBlocks int) VB {
	return VB{
		VolumeName:          name,
		VolumeSize:          pfbVolumeBytes,
		BaseDir:             baseDir,
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		GCEnabled:           false,
		GCInterval:          -1,
		Cache:               Cache{Config: CacheConfig{Size: cacheBlocks}},
	}
}

func pfbOpenWALs(b *testing.B, vb *VB) {
	b.Helper()
	require.NoError(b, vb.OpenWAL(&vb.WAL, filepath.Join(vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(b, vb.OpenWAL(&vb.BlockToObjectWAL, filepath.Join(vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))
}

// pfbRetire closes a writable engine's WAL files without a drain, so its
// buffered writes never reach the backend.
func pfbRetire(vb *VB) {
	for _, w := range []*WAL{&vb.WAL, &vb.BlockToObjectWAL} {
		w.mu.Lock()
		for _, f := range w.DB {
			_ = f.Close()
		}
		w.DB = nil
		w.mu.Unlock()
	}
}

// pfbFixture records the size and modification time of every backend file.
func pfbFixture(b *testing.B, root string) map[string]string {
	b.Helper()
	files := map[string]string{}
	require.NoError(b, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files[path] = fmt.Sprintf("%d %d", info.Size(), info.ModTime().UnixNano())
		return nil
	}))
	return files
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
	require.NoError(b, vb.LoadLiveCheckpoint())
	if writable {
		pfbOpenWALs(b, vb)
		b.Cleanup(func() { pfbRetire(vb) })
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
		writeRoot := pfbSeed(b, name)
		b.Run(prefix+"partial-write-under-frag-load", func(b *testing.B) {
			b.ReportAllocs()
			fixture := pfbFixture(b, writeRoot)
			patch := make([]byte, 512)
			var vb *VB
			var stop chan struct{}
			var wg sync.WaitGroup
			var bgReads atomic.Int64
			start := func() {
				vb = pfbOpen(b, writeRoot, name, 0, delay, true)
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
				pfbRetire(vb)
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
			require.Equal(b, fixture, pfbFixture(b, writeRoot), "a writable engine changed the backend fixture")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*pfbWritesPerOp), "ns/write")
			b.ReportMetric(float64(bgReads.Load())/elapsed.Seconds(), "bg-reads/s")
		})

		// Each op starts a sole cold fragmented 1 MiB read and holds its chunk
		// payload reads. Once the held group is complete, it starts one timed
		// operation and releases the group when that operation reaches the backend.
		arrival := func(caseName string, opRoot string, writable bool, arrive func(vb *VB, op int) error) {
			b.Run(prefix+caseName, func(b *testing.B) {
				b.ReportAllocs()
				fixture := pfbFixture(b, opRoot)
				open := func() (*VB, *pfbDelayBackend) {
					vb := pfbOpen(b, opRoot, name, 0, delay, writable)
					return vb, vb.Backend.(*pfbDelayBackend)
				}
				vb, db := open()
				soloCtx, cancelSolo := context.WithCancel(context.WithValue(ctx, pfbSoloKey{}, true))
				defer cancelSolo()
				var arrivalTotal time.Duration
				widths := map[int]int{}

				// Every failure releases and cancels the held solo fetches and joins
				// both callers before the benchmark stops and its fixture is retired.
				var gate *pfbSoloGate
				var soloDone, arrived chan error
				released := false
				release := func() {
					if !released {
						released = true
						close(gate.release)
					}
				}
				abort := func(format string, args ...any) {
					release()
					cancelSolo()
					stall := time.After(pfbStall)
					for soloDone != nil || arrived != nil {
						select {
						case <-soloDone:
							soloDone = nil
						case <-arrived:
							arrived = nil
						case <-stall:
							b.Fatalf(format+" (a caller did not stop within %v)", append(args, pfbStall)...)
						}
					}
					db.gate.Store(nil)
					b.Fatalf(format, args...)
				}

				op := 0
				for b.Loop() {
					if op > 0 && op%pfbFragBlocks == 0 {
						b.StopTimer()
						if writable {
							pfbRetire(vb)
						}
						vb, db = open()
						b.StartTimer()
					}
					gate, released, arrived = newPfbSoloGate(), false, nil
					db.gate.Store(gate)
					soloDone = make(chan error, 1)
					go func() {
						_, err := vb.ReadAtCtx(soloCtx, uint64(op%pfbWindows)*window, window)
						soloDone <- err
					}()

					b.StopTimer()
					select {
					case <-gate.held:
					case err := <-soloDone:
						soloDone = nil
						abort("the sole read returned %v before its first held fetch", err)
					case <-time.After(pfbStall):
						abort("no held solo fetch within %v", pfbStall)
					}
					width := 1
				settle:
					for width < pfbSoloWidth {
						select {
						case <-gate.held:
							width++
						case <-time.After(pfbQuiet):
							break settle
						}
					}
					if width != 1 && width != pfbSoloWidth {
						abort("the sole read held %d fetches; want 1 or %d", width, pfbSoloWidth)
					}
					widths[width]++
					b.StartTimer()

					began := time.Now()
					arrived = make(chan error, 1)
					go func() { arrived <- arrive(vb, op) }()
					// A fast arrival can reach the backend and finish before this
					// select runs, so a finished arrival checks for its backend signal.
					var arriveErr error
					finished := false
					select {
					case <-gate.other:
					case arriveErr = <-arrived:
						arrived, finished = nil, true
						select {
						case <-gate.other:
						default:
							abort("the arriving operation returned %v before it reached the backend", arriveErr)
						}
					case <-time.After(pfbStall):
						abort("no arriving backend read within %v", pfbStall)
					}
					release()
					if !finished {
						select {
						case arriveErr = <-arrived:
							arrived = nil
						case <-time.After(pfbStall):
							abort("the arriving operation did not finish within %v", pfbStall)
						}
					}
					if arriveErr != nil {
						abort("arriving operation: %v", arriveErr)
					}
					arrivalTotal += time.Since(began)
					select {
					case err := <-soloDone:
						soloDone = nil
						if err != nil {
							abort("sole read: %v", err)
						}
					case <-time.After(pfbStall):
						abort("the sole read did not finish within %v", pfbStall)
					}
					db.gate.Store(nil)
					op++
				}
				if writable {
					pfbRetire(vb)
				}
				require.Equal(b, fixture, pfbFixture(b, opRoot), "a writable engine changed the backend fixture")
				require.Len(b, widths, 1, "the sole read changed width between ops: %v", widths)
				for width := range widths {
					b.ReportMetric(float64(width), "solo-width")
				}
				b.ReportMetric(float64(arrivalTotal.Nanoseconds())/float64(b.N), "arrival-ns/op")
			})
		}
		arrival("arrival-miss-4KiB", root, false, func(vb *VB, op int) error {
			block := uint64(pfbFragBlocks + op%pfbFragBlocks)
			_, err := vb.ReadAtCtx(ctx, block*bs, bs)
			return err
		})
		patch := make([]byte, 512)
		arrival("arrival-partial-write", pfbSeed(b, name), true, func(vb *VB, op int) error {
			block := uint64(pfbFragBlocks + op%pfbFragBlocks)
			return vb.WriteAt(block*bs+512, patch)
		})
	}
}
