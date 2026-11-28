// Pinning tests for extent-based block-map coalescing: createChunkFile must
// emit one BlockLookup entry per consecutive run of blocks instead of one
// entry per physical block, and BlockStore's Persisted state must mirror
// that in persistedExtents.

package viperblock

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// newCoalesceTestVB creates a file-backed VB with a caller-chosen
// ObjBlockSize so tests can control how many blocks land in one chunk (and
// therefore one coalesced extent). key == nil gives an unencrypted volume.
func newCoalesceTestVB(t *testing.T, volume string, objBlockSizeBlocks int, key *masterkey.Key) *VB {
	t.Helper()
	dir := t.TempDir()
	cfg := file.FileConfig{BaseDir: dir, VolumeName: volume}
	vb, err := New(&VB{
		VolumeName:        volume,
		VolumeSize:        64 * 1024 * 1024,
		BaseDir:           dir,
		MasterKey:         key,
		EncryptionEnabled: key != nil,
		WALSyncInterval:   -1,
	}, "file", cfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())

	vb.BlockSize = DefaultBlockSize
	vb.ObjBlockSize = uint32(objBlockSizeBlocks) * DefaultBlockSize
	require.NoError(t, vb.SaveState())

	require.NoError(t, vb.OpenWAL(&vb.WAL,
		fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL,
		fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	return vb
}

// randomBlocks returns n*BlockSize bytes of random data.
func randomBlocks(t *testing.T, vb *VB, n int) []byte {
	t.Helper()
	buf := make([]byte, uint64(n)*uint64(vb.BlockSize))
	_, err := rand.Read(buf)
	require.NoError(t, err)
	return buf
}

// TestBlockMapCoalesce_SequentialWrite pins that a single consecutive write
// fitting in one chunk produces exactly one BlockLookup entry, and every
// block reads back correctly, including ones that aren't the StartBlock key.
func TestBlockMapCoalesce_SequentialWrite(t *testing.T) {
	const numBlocks = 50
	vb := newCoalesceTestVB(t, "vol-seq-write", 4096 /* huge, single chunk */, nil)

	data := randomBlocks(t, vb, numBlocks)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.BlocksToObject.mu.RLock()
	require.Equal(t, 1, vb.BlocksToObject.lookup.len(), "one consecutive run must coalesce into one BlockLookup entry")
	entry := lookupMap(&vb.BlocksToObject)[0]
	vb.BlocksToObject.mu.RUnlock()
	require.Equal(t, uint16(numBlocks), entry.NumBlocks)

	require.Equal(t, 1, vb.BlockStore.PersistedExtentCount(), "one consecutive run must coalesce into one persistedExtent")

	got, err := vb.ReadAt(0, uint64(numBlocks)*uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got), "sequential write readback must be byte-identical")

	// Read individual blocks too, to prove range resolution (not just the
	// map-key hit) works for non-StartBlock blocks.
	for _, block := range []uint64{0, 1, numBlocks / 2, numBlocks - 1} {
		want := data[block*uint64(vb.BlockSize) : (block+1)*uint64(vb.BlockSize)]
		got, err := vb.ReadAt(block*uint64(vb.BlockSize), uint64(vb.BlockSize))
		require.NoError(t, err)
		require.True(t, bytes.Equal(want, got), "block %d readback mismatch", block)
	}
}

// TestBlockMapCoalesce_OverwriteFracturesExtent pins that an overwrite
// landing inside an existing coalesced extent fractures it into its
// surviving head/tail, with every block reading back correctly afterwards.
func TestBlockMapCoalesce_OverwriteFracturesExtent(t *testing.T) {
	const numBlocks = 20
	vb := newCoalesceTestVB(t, "vol-overwrite-fracture", 4096, nil)

	original := randomBlocks(t, vb, numBlocks)
	require.NoError(t, vb.WriteAt(0, original))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.BlocksToObject.mu.RLock()
	require.Equal(t, 1, vb.BlocksToObject.lookup.len())
	vb.BlocksToObject.mu.RUnlock()

	// Overwrite [8, 12) -- strictly inside the [0, 20) extent, leaving
	// surviving blocks on both sides.
	overwrite := randomBlocks(t, vb, 4)
	require.NoError(t, vb.WriteAt(8*uint64(vb.BlockSize), overwrite))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.BlocksToObject.mu.RLock()
	require.Equal(t, 3, vb.BlocksToObject.lookup.len(), "overwrite must fracture the original extent into head/new/tail")
	head, headOK := lookupMap(&vb.BlocksToObject)[0]
	mid, midOK := lookupMap(&vb.BlocksToObject)[8]
	tail, tailOK := lookupMap(&vb.BlocksToObject)[12]
	vb.BlocksToObject.mu.RUnlock()

	require.True(t, headOK && midOK && tailOK, "expected entries keyed at 0, 8, 12")
	require.Equal(t, uint16(8), head.NumBlocks)
	require.Equal(t, uint16(4), mid.NumBlocks)
	require.Equal(t, uint16(8), tail.NumBlocks)
	require.Equal(t, uint64(0), head.ObjectID, "head must still point at the original chunk")
	require.Equal(t, uint64(1), mid.ObjectID, "overwritten range must point at the new chunk")
	require.Equal(t, uint64(0), tail.ObjectID, "tail must still point at the original chunk")

	want := make([]byte, 0, numBlocks*int(vb.BlockSize))
	want = append(want, original[:8*vb.BlockSize]...)
	want = append(want, overwrite...)
	want = append(want, original[12*vb.BlockSize:]...)

	got, err := vb.ReadAt(0, uint64(numBlocks)*uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(want, got), "readback after fracture must reflect head/overwrite/tail exactly")

	// Block 12 (the tail's first block) is what breaks if the fractured
	// tail's ObjectOffset isn't recomputed relative to the original extent.
	wantTailBlock := original[12*vb.BlockSize : 13*vb.BlockSize]
	gotTailBlock, err := vb.ReadAt(12*uint64(vb.BlockSize), uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(wantTailBlock, gotTailBlock), "tail block 12 readback mismatch")
}

// TestBlockMapCoalesce_MultiChunkBoundary pins that a sequential write
// spanning multiple chunks produces exactly one extent per chunk, since
// cross-chunk coalescing is architecturally impossible.
func TestBlockMapCoalesce_MultiChunkBoundary(t *testing.T) {
	const blocksPerChunk = 8
	const numBlocks = 20 // spans 3 chunks: 8 + 8 + 4
	vb := newCoalesceTestVB(t, "vol-multi-chunk", blocksPerChunk, nil)

	data := randomBlocks(t, vb, numBlocks)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.BlocksToObject.mu.RLock()
	require.Equal(t, 3, vb.BlocksToObject.lookup.len(), "20 blocks at 8 blocks/chunk must produce 3 extents")
	e0, ok0 := lookupMap(&vb.BlocksToObject)[0]
	e1, ok1 := lookupMap(&vb.BlocksToObject)[8]
	e2, ok2 := lookupMap(&vb.BlocksToObject)[16]
	vb.BlocksToObject.mu.RUnlock()

	require.True(t, ok0 && ok1 && ok2, "expected extents keyed at 0, 8, 16")
	require.Equal(t, uint16(8), e0.NumBlocks)
	require.Equal(t, uint16(8), e1.NumBlocks)
	require.Equal(t, uint16(4), e2.NumBlocks)
	require.NotEqual(t, e0.ObjectID, e1.ObjectID, "each chunk boundary must be a different backend object")
	require.NotEqual(t, e1.ObjectID, e2.ObjectID)

	require.Equal(t, 3, vb.BlockStore.PersistedExtentCount())

	got, err := vb.ReadAt(0, uint64(numBlocks)*uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got), "readback across chunk boundaries must be byte-identical")
}

// TestBlockMapCoalesce_EncryptedReadback pins that per-block SeqNum
// fidelity survives coalescing on an encrypted volume: blocks in a
// coalesced run aren't guaranteed to share a SeqNum, and AEAD nonce
// reconstruction on read depends on each block keeping its own.
func TestBlockMapCoalesce_EncryptedReadback(t *testing.T) {
	var raw [masterkey.MasterKeySize]byte
	for i := range raw {
		raw[i] = 0x7A
	}
	aead, err := masterkey.NewAEAD(raw[:])
	require.NoError(t, err)
	key := &masterkey.Key{AEAD: aead, Fingerprint: masterkey.Fingerprint(raw[:])}

	const blocksPerChunk = 8
	const numBlocks = 20
	vb := newCoalesceTestVB(t, "vol-encrypted-coalesce", blocksPerChunk, key)

	data := randomBlocks(t, vb, numBlocks)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.BlocksToObject.mu.RLock()
	require.Equal(t, 3, vb.BlocksToObject.lookup.len(), "encrypted volumes coalesce the same way as plaintext ones")
	vb.BlocksToObject.mu.RUnlock()

	got, err := vb.ReadAt(0, uint64(numBlocks)*uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got), "encrypted sequential readback must be byte-identical")

	// Overwrite a range straddling a chunk boundary, forcing per-block
	// SeqNum tracking across a fracture on an encrypted volume.
	overwrite := randomBlocks(t, vb, 4)
	require.NoError(t, vb.WriteAt(6*uint64(vb.BlockSize), overwrite))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	want := make([]byte, 0, numBlocks*int(vb.BlockSize))
	want = append(want, data[:6*vb.BlockSize]...)
	want = append(want, overwrite...)
	want = append(want, data[10*vb.BlockSize:]...)

	got, err = vb.ReadAt(0, uint64(numBlocks)*uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(want, got), "encrypted readback after cross-chunk-boundary overwrite must be byte-identical")

	// Cold-reload the block map from a commit, exercising the map load's
	// recoalescing and BlockStore's restore path.
	require.NoError(t, vb.persistMapCtx(t.Context()))
	vb.BlockStore = NewUnifiedBlockStore(vb.BlockSize)
	require.NoError(t, vb.LoadLiveCheckpoint())

	got, err = vb.ReadAt(0, uint64(numBlocks)*uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(want, got), "cold encrypted readback after reload must still be byte-identical")
}

// TestBlockMapCoalesce_EntryCountIsExtentsNotBlocks pins that a large
// sequential write grows the block map by O(extents), not O(blocks).
func TestBlockMapCoalesce_EntryCountIsExtentsNotBlocks(t *testing.T) {
	const blocksPerChunk = 1024 // matches the production default (4MB / 4KB)
	const numBlocks = 5000      // spans ceil(5000/1024) = 5 chunks
	vb := newCoalesceTestVB(t, "vol-entry-count", blocksPerChunk, nil)

	data := randomBlocks(t, vb, numBlocks)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.BlocksToObject.mu.RLock()
	entryCount := vb.BlocksToObject.lookup.len()
	vb.BlocksToObject.mu.RUnlock()

	wantExtents := (numBlocks + blocksPerChunk - 1) / blocksPerChunk // ceil
	require.Equal(t, wantExtents, entryCount,
		"map must grow by O(extents) (%d chunks), not O(blocks) (%d blocks)", wantExtents, numBlocks)
	require.Equal(t, wantExtents, vb.BlockStore.PersistedExtentCount())
	require.Less(t, entryCount, numBlocks/10, "sanity: coalesced entry count must be far smaller than the block count")
}
