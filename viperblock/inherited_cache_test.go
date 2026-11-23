package viperblock

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/require"
)

type inheritedReadCounter struct {
	types.Backend

	payloadReads int
	truncateRead int
}

func (backend *inheritedReadCounter) ReadFromCtx(ctx context.Context, volume string, typ types.FileType, object uint64, offset, length uint32) ([]byte, error) {
	if typ == types.FileTypeChunk && offset != 0 {
		backend.payloadReads++
		if backend.payloadReads == backend.truncateRead {
			data, err := backend.Backend.ReadFromCtx(ctx, volume, typ, object, offset, length)
			if err != nil || len(data) == 0 {
				return data, err
			}
			return data[:len(data)-1], nil
		}
	}
	return backend.Backend.ReadFromCtx(ctx, volume, typ, object, offset, length)
}

func createInheritedCacheFixture(t testing.TB) (parent, child *VB, original []byte) {
	t.Helper()
	rawKey := bytes.Repeat([]byte{0x42}, masterkey.MasterKeySize)
	aead, err := masterkey.NewAEAD(rawKey)
	require.NoError(t, err)
	key := &masterkey.Key{AEAD: aead, Fingerprint: masterkey.Fingerprint(rawKey)}
	volume := newMapVolume(t, 128<<20, key)
	parent, _ = volume.create(t, 1)
	original = bytes.Repeat([]byte{0x5a}, int(parent.BlockSize))
	require.NoError(t, parent.WriteAt(0, original))
	require.NoError(t, parent.WriteAt(uint64(parent.BlockSize), original))
	require.NoError(t, parent.WriteAt(2*uint64(parent.BlockSize), original))
	require.NoError(t, parent.DrainToBackend())
	require.NoError(t, parent.WriteAt(3*uint64(parent.BlockSize), original))
	_, err = parent.CreateSnapshot("snap-cache-source")
	require.NoError(t, err)
	volume.name = "vol-cache-child"
	child, _ = volume.create(t, 1)
	require.NoError(t, child.OpenFromSnapshot("snap-cache-source"))
	return parent, child, original
}

func TestInheritedReadReusesSnapshotCache(t *testing.T) {
	for _, blockstore := range []bool{true, false} {
		mode := "blockstore"
		if !blockstore {
			mode = "legacy"
		}
		t.Run(mode, func(t *testing.T) {
			_, child, original := createInheritedCacheFixture(t)
			child.UseBlockStore = blockstore
			require.NoError(t, child.SetCacheSize(1, 0))
			backend := &inheritedReadCounter{Backend: child.Backend}
			child.Backend = backend
			dst := make([]byte, len(original))
			for range 3 {
				require.NoError(t, child.ReadAtInto(0, dst))
				require.Equal(t, original, dst)
			}
			require.Equal(t, 1, backend.payloadReads, "repeated reads should reuse the authenticated source bytes")

			require.NoError(t, child.ReadAtInto(uint64(child.BlockSize), dst))
			require.NoError(t, child.ReadAtInto(0, dst))
			require.Equal(t, original, dst)
			require.Equal(t, 3, backend.payloadReads, "the other block evicted the first")

			overwrite := bytes.Repeat([]byte{0xb4}, len(original))
			_, _, sourceSeq, err := child.LookupBaseBlockToObject(0)
			require.NoError(t, err)
			require.Positive(t, sourceSeq)
			child.SeqNum.Store(sourceSeq - 1)
			require.NoError(t, child.WriteAt(0, overwrite))
			require.NoError(t, child.ReadAtInto(0, dst))
			require.Equal(t, overwrite, dst)
			// Upload normally warms the child's cache. Keep the inherited entry
			// so this test exercises the origin check on a persisted read.
			child.Cache.Config.Size = 0
			require.NoError(t, child.DrainToBackend())
			child.Cache.Config.Size = 1
			_, _, ownSeq, err := child.LookupBlockToObject(0)
			require.NoError(t, err)
			require.Equal(t, sourceSeq, ownSeq)
			require.NoError(t, child.ReadAtInto(0, dst))
			require.Equal(t, overwrite, dst)
			require.Equal(t, 3, backend.payloadReads, "a child write must take precedence")
		})
	}
}

func TestInheritedCacheDetectsBadPayloadOnMiss(t *testing.T) {
	_, child, original := createInheritedCacheFixture(t)
	require.NoError(t, child.SetCacheSize(1, 0))
	backend := &inheritedReadCounter{Backend: child.Backend}
	child.Backend = backend
	dst := make([]byte, len(original))
	require.NoError(t, child.ReadAtInto(0, dst))
	backend.truncateRead = 2
	clear(dst)
	require.NoError(t, child.ReadAtInto(0, dst))
	require.Equal(t, original, dst)
	require.Equal(t, 1, backend.payloadReads)
	child.Cache.purge()
	require.ErrorContains(t, child.ReadAtInto(0, dst), "short read")
	require.Zero(t, child.Cache.lru.Len())
	require.NoError(t, child.ReadAtInto(0, dst))
	require.Equal(t, original, dst)
}

func TestInheritedMixedFailureLeavesCacheEmpty(t *testing.T) {
	_, child, original := createInheritedCacheFixture(t)
	require.NoError(t, child.SetCacheSize(8, 0))
	backend := &inheritedReadCounter{Backend: child.Backend, truncateRead: 2}
	child.Backend = backend
	dst := make([]byte, 4*len(original))
	require.ErrorContains(t, child.ReadAtInto(0, dst), "short read")
	require.Equal(t, 2, backend.payloadReads)
	require.Zero(t, child.Cache.lru.Len())
}

func TestInheritedAncestorCacheAndDisabled(t *testing.T) {
	_, child, original := createInheritedCacheFixture(t)
	require.NoError(t, child.WriteAt(4*uint64(child.BlockSize), original))
	_, err := child.CreateSnapshot("snap-cache-intermediate")
	require.NoError(t, err)
	volume := newMapVolume(t, child.VolumeSize, child.MasterKey)
	volume.root = filepath.Dir(child.BaseDir)
	volume.name = "vol-cache-grandchild"
	grandchild, _ := volume.create(t, 1)
	require.NoError(t, grandchild.OpenFromSnapshot("snap-cache-intermediate"))
	require.NotEmpty(t, grandchild.ancestors)
	backend := &inheritedReadCounter{Backend: grandchild.Backend}
	grandchild.Backend = backend
	dst := make([]byte, len(original))

	for _, useBlockStore := range []bool{true, false} {
		grandchild.UseBlockStore = useBlockStore
		for _, cacheSize := range []int{0, 8} {
			require.NoError(t, grandchild.SetCacheSize(cacheSize, 0))
			backend.payloadReads = 0
			for range 2 {
				require.NoError(t, grandchild.ReadAtInto(0, dst))
				require.Equal(t, original, dst)
			}
			if cacheSize == 0 {
				require.Equal(t, 2, backend.payloadReads)
			} else {
				require.Equal(t, 1, backend.payloadReads)
			}
		}
	}
}

func BenchmarkSnapshotPlaintextRead(b *testing.B) {
	for _, name := range []string{"inherited-warm", "inherited-disabled", "own-warm", "inherited-partly-warm-12KiB"} {
		b.Run(name, func(b *testing.B) {
			parent, child, original := createInheritedCacheFixture(b)
			volume := child
			if name == "own-warm" {
				volume = parent
			}
			if name != "inherited-disabled" {
				require.NoError(b, volume.SetCacheSize(128, 0))
			}
			backend := &inheritedReadCounter{Backend: volume.Backend}
			volume.Backend = backend
			dst := make([]byte, len(original))
			require.NoError(b, volume.ReadAtInto(0, dst))
			backend.payloadReads = 0
			if name == "inherited-partly-warm-12KiB" {
				dst = make([]byte, 3*len(original))
			}
			b.SetBytes(int64(len(dst)))
			b.ReportAllocs()
			for b.Loop() {
				if name == "inherited-partly-warm-12KiB" {
					b.StopTimer()
					volume.Cache.purge()
					require.NoError(b, volume.ReadAtInto(uint64(len(original)), dst[:len(original)]))
					backend.payloadReads--
					b.StartTimer()
				}
				if err := volume.ReadAtInto(0, dst); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(backend.payloadReads)/float64(b.N), "source-payload/op")
			if name == "inherited-partly-warm-12KiB" {
				require.Equal(b, b.N, backend.payloadReads)
			}
			require.Equal(b, bytes.Repeat(original, len(dst)/len(original)), dst)
		})
	}
}
