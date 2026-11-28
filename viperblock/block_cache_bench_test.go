package viperblock

import (
	"context"
	"sync/atomic"
	"testing"
)

// BenchmarkBlockCache measures reads that go through the block cache: hits
// from one and from many readers, and 1 MiB misses that fill it concurrently.
func BenchmarkBlockCache(b *testing.B) {
	const name = "vol-bcb"
	root := pfbSeed(b, name)
	bs := uint64(DefaultBlockSize)
	window := uint64(pfbWindow) * bs
	ctx := context.Background()
	hot := uint64(pfbBlocks - pfbFragBlocks)

	warm := func(b *testing.B, vb *VB) {
		b.Helper()
		for w := uint64(0); w < hot/pfbWindow; w++ {
			if _, err := vb.ReadAtCtx(ctx, uint64(pfbFragBlocks)*bs+w*window, window); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("hit-4KiB", func(b *testing.B) {
		vb := pfbOpen(b, root, name, DefaultCacheBlocks, 0, false)
		warm(b, vb)
		b.SetBytes(int64(bs))
		b.ReportAllocs()
		i := uint64(0)
		for b.Loop() {
			block := pfbFragBlocks + (i*7919)%hot
			i++
			if _, err := vb.ReadAtCtx(ctx, block*bs, bs); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("hit-4KiB-parallel", func(b *testing.B) {
		vb := pfbOpen(b, root, name, DefaultCacheBlocks, 0, false)
		warm(b, vb)
		var next atomic.Uint64
		b.SetBytes(int64(bs))
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				block := pfbFragBlocks + (next.Add(1)*7919)%hot
				if _, err := vb.ReadAtCtx(ctx, block*bs, bs); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})

	b.Run("hit-1MiB-parallel", func(b *testing.B) {
		vb := pfbOpen(b, root, name, DefaultCacheBlocks, 0, false)
		warm(b, vb)
		var next atomic.Uint64
		b.SetBytes(int64(window))
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				w := next.Add(1) % (hot / pfbWindow)
				if _, err := vb.ReadAtCtx(ctx, uint64(pfbFragBlocks)*bs+w*window, window); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})

	// Each read is a miss that caches its blocks, so readers contend on
	// inserts; a cache smaller than the region keeps every read a miss.
	b.Run("fill-1MiB-parallel", func(b *testing.B) {
		vb := pfbOpen(b, root, name, 4096, 0, false)
		var next atomic.Uint64
		b.SetBytes(int64(window))
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				w := next.Add(1) % (hot / pfbWindow)
				if _, err := vb.ReadAtCtx(ctx, uint64(pfbFragBlocks)*bs+w*window, window); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})

	// The plugin's path: ReadAtInto fills the caller's buffer, as PRead does.
	b.Run("into-hit-1MiB-parallel", func(b *testing.B) {
		vb := pfbOpen(b, root, name, DefaultCacheBlocks, 0, false)
		warm(b, vb)
		var next atomic.Uint64
		b.SetBytes(int64(window))
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			dst := make([]byte, window)
			for pb.Next() {
				w := next.Add(1) % (hot / pfbWindow)
				if err := vb.ReadAtInto(uint64(pfbFragBlocks)*bs+w*window, dst); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})

	b.Run("into-miss-1MiB-parallel", func(b *testing.B) {
		vb := pfbOpen(b, root, name, 4096, 0, false)
		var next atomic.Uint64
		b.SetBytes(int64(window))
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			dst := make([]byte, window)
			for pb.Next() {
				w := next.Add(1) % (hot / pfbWindow)
				if err := vb.ReadAtInto(uint64(pfbFragBlocks)*bs+w*window, dst); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})

	// Mostly hits with a few misses: each 1 MiB read finds 4 of its 256 blocks
	// uncached. The pattern is restored outside the timer after every pass.
	b.Run("sparse-miss-1MiB", func(b *testing.B) {
		vb := pfbOpen(b, root, name, DefaultCacheBlocks, 0, false)
		windows := hot / pfbWindow
		sparse := func() {
			vb.Cache.purge()
			for w := uint64(0); w < windows; w++ {
				base := uint64(pfbFragBlocks) + w*pfbWindow
				for blk := uint64(0); blk < pfbWindow; blk += 64 {
					if _, err := vb.ReadAtCtx(ctx, (base+blk+1)*bs, 63*bs); err != nil {
						b.Fatal(err)
					}
				}
			}
		}
		sparse()
		dst := make([]byte, window)
		b.SetBytes(int64(window))
		b.ReportAllocs()
		b.ResetTimer()
		w := uint64(0)
		for b.Loop() {
			if w == windows {
				b.StopTimer()
				sparse()
				w = 0
				b.StartTimer()
			}
			if err := vb.ReadAtInto(uint64(pfbFragBlocks)*bs+w*window, dst); err != nil {
				b.Fatal(err)
			}
			w++
		}
	})
}
