package viperblock

import (
	"bytes"
	"context"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/require"
)

type inheritedReadCounter struct {
	types.Backend
	payloadReads int
}

func (backend *inheritedReadCounter) ReadFromCtx(ctx context.Context, volume string, typ types.FileType, object uint64, offset, length uint32) ([]byte, error) {
	if typ == types.FileTypeChunk && offset != 0 {
		backend.payloadReads++
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
			require.NoError(t, child.WriteAt(0, overwrite))
			require.NoError(t, child.ReadAtInto(0, dst))
			require.Equal(t, overwrite, dst)
			require.NoError(t, child.DrainToBackend())
			require.NoError(t, child.ReadAtInto(0, dst))
			require.Equal(t, overwrite, dst)
			require.Equal(t, 3, backend.payloadReads, "a child write must take precedence")
		})
	}
}

func BenchmarkSnapshotPlaintextRead(b *testing.B) {
	for _, name := range []string{"inherited-warm", "inherited-disabled", "own-warm"} {
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
			b.SetBytes(int64(len(dst)))
			b.ReportAllocs()
			for b.Loop() {
				if err := volume.ReadAtInto(0, dst); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(backend.payloadReads)/float64(b.N), "source-payload/op")
			require.Equal(b, original, dst)
		})
	}
}
