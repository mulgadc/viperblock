package viperblock

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// These tests pin the durability half of the stale-drain repoint hazard
// (mulga-siv-482). TestChunkGC_StaleDrainNeverClobbersNewerChunk already
// covers the in-memory BlocksToObject map, but it deliberately sets
// UseBlockStore = false and never reopens the volume. The field corruption
// only became visible on a COLD read — after the live checkpoint was
// persisted and a fresh process loaded it — so the property that actually
// matters is that the newest bytes survive a checkpoint round-trip, with
// UseBlockStore left at its production default.

// TestStaleDrain_ColdReopenPreservesNewestBytes drives the exact out-of-order
// boundary deterministically: two chunk uploads for the same block where the
// OLDER drain's block-map write lands last. The volume is then checkpointed
// and reopened from scratch. Without createChunkFile's monotonic SeqNum
// guard, the persisted checkpoint points the block at the stale chunk and the
// cold read hands back superseded data with no error anywhere.
func TestStaleDrain_ColdReopenPreservesNewestBytes(t *testing.T) {
	root := t.TempDir()
	const vol = "vol-stale-drain-cold-reopen"

	vb := newGCTestVB(t, root, vol, false)
	ctx := context.Background()
	bs := int(vb.BlockSize)

	newer := bytes.Repeat([]byte{0xA5}, bs)
	stale := bytes.Repeat([]byte{0x5C}, bs)

	// The newer write (higher SeqNum) is uploaded and mapped first.
	newerBuf := append([]byte(nil), newer...)
	newerMatched := []Block{{Block: 0, SeqNum: 20, Len: uint64(bs)}}
	require.NoError(t, vb.createChunkFile(ctx, 0, &newerBuf, &newerMatched))

	// A slower, OLDER drain for the same block completes second. Its map
	// write must be rejected, not applied.
	staleBuf := append([]byte(nil), stale...)
	staleMatched := []Block{{Block: 0, SeqNum: 10, Len: uint64(bs)}}
	require.NoError(t, vb.createChunkFile(ctx, 0, &staleBuf, &staleMatched))

	// Publish volume config + the live checkpoint, then boot a fresh VB.
	require.NoError(t, vb.SaveState())
	require.NoError(t, vb.SaveLiveCheckpoint())

	reopened := reopenGCTestVB(t, root, vol, false)
	got, err := reopened.ReadAt(0, uint64(bs))
	require.NoError(t, err)

	require.Equalf(t, newer, got,
		"cold read returned superseded data: the stale drain's map write was persisted into the live checkpoint (first byte got=%#x want=%#x)",
		got[0], newer[0])
}

// TestSequentialPartialWrites_ColdReopenByteExact is the field-shaped
// regression: ONE sequential writer, no concurrency in the guest, issuing
// sub-block (3072-byte) writes so that every 4096-byte block is assembled by
// the WriteAtCtx read-modify-write path across two successive writes. Chunk
// drains run in the background underneath, exactly as they do under nbdkit.
// After a clean Close the volume is reopened cold and compared byte-for-byte.
//
// This is the shape that produced the reported signature: a block whose tail
// bytes revert to a previous generation (zeros on a never-written region,
// otherwise whatever the superseded chunk held at that offset — which is how
// deleted-file contents leaked back into readable data).
func TestSequentialPartialWrites_ColdReopenByteExact(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-megabyte durability round-trip in -short mode")
	}

	const volSize = 128 * 1024 * 1024
	const total = 32 * 1024 * 1024
	const piece = 3072 // sub-block: forces RMW on every block boundary

	root := t.TempDir()
	const vol = "vol-sequential-partial-writes"

	backendConfig := file.FileConfig{VolumeName: vol, VolumeSize: volSize, BaseDir: root}
	vbconfig := VB{
		VolumeName:      vol,
		VolumeSize:      volSize,
		BaseDir:         fmt.Sprintf("%s/viperblock", root),
		WALSyncInterval: -1,
		// Drain often and in parallel so chunk uploads interleave with the
		// writer, as they do in production.
		ChunkUploadInterval: time.Millisecond,
		MaxPendingBytes:     2 * 1024 * 1024,
		UploadWorkers:       4,
		GCEnabled:           true,
		GCInterval:          5 * time.Millisecond,
		Cache:               Cache{Config: CacheConfig{Size: 0}},
	}

	vb, err := New(&vbconfig, FileBackend, backendConfig)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	src := make([]byte, total)
	rnd := rand.New(rand.NewPCG(20260721, 20260721))
	for i := range src {
		src[i] = byte(rnd.IntN(256))
	}

	// Pass 1: strictly sequential fill.
	for off := 0; off < total; off += piece {
		end := min(off+piece, total)
		require.NoError(t, vb.WriteAt(uint64(off), append([]byte(nil), src[off:end]...)))
	}

	// Pass 2: rewrite a scattered quarter so blocks are superseded and the
	// block map fractures across chunk generations.
	for range total / piece / 4 {
		off := rnd.IntN(total/piece) * piece
		end := min(off+piece, total)
		for i := off; i < end; i++ {
			src[i] = byte(rnd.IntN(256))
		}
		require.NoError(t, vb.WriteAt(uint64(off), append([]byte(nil), src[off:end]...)))
	}

	vb.StopChunkUploader()
	require.NoError(t, vb.Close())

	// Cold reopen: everything must come from the persisted checkpoint+chunks.
	reopenConfig := VB{
		VolumeName:          vol,
		VolumeSize:          volSize,
		BaseDir:             fmt.Sprintf("%s/viperblock", root),
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		Cache:               Cache{Config: CacheConfig{Size: 0}},
	}
	reopened, err := New(&reopenConfig, FileBackend, backendConfig)
	require.NoError(t, err)
	require.NoError(t, reopened.Backend.Init())
	require.NoError(t, reopened.LoadState())
	require.NoError(t, reopened.LoadLiveCheckpoint())

	got, err := reopened.ReadAt(0, total)
	require.NoError(t, err)

	if !bytes.Equal(src, got) {
		bs := int(reopened.BlockSize)
		bad := 0
		detail := ""
		for b := 0; b*bs < total; b++ {
			s, e := b*bs, (b+1)*bs
			if bytes.Equal(src[s:e], got[s:e]) {
				continue
			}
			bad++
			if bad <= 5 {
				lo, hi := -1, -1
				for i := s; i < e; i++ {
					if src[i] != got[i] {
						if lo < 0 {
							lo = i - s
						}
						hi = i - s
					}
				}
				allZero := true
				for i := s + lo; i <= s+hi; i++ {
					if got[i] != 0 {
						allZero = false
						break
					}
				}
				detail += fmt.Sprintf("\n  block %d (offset %#x): bytes [%d,%d] wrong (%d bytes), replacement all-zero=%v",
					b, s, lo, hi, hi-lo+1, allZero)
			}
		}
		t.Fatalf("cold read differs from what was written: %d of %d blocks corrupt (%.4f%%)%s",
			bad, total/bs, 100*float64(bad)/float64(total/bs), detail)
	}
}
