// Pinning tests for extent-based block-map coalescing: createChunkFile must
// emit one BlockLookup entry per consecutive run of blocks instead of one
// entry per physical block, and BlockStore's Persisted state must mirror
// that in persistedExtents. These tests pin the correctness properties that
// make coalescing safe:
//   - sequential writes produce O(extents) map entries, not O(blocks)
//   - an overwrite landing inside an existing extent fractures it into its
//     surviving head/tail rather than corrupting or losing data
//   - reads resolve any block in a coalesced run -- including blocks that
//     are not the run's StartBlock map key -- to byte-identical data
//   - per-block SeqNum fidelity survives coalescing, so AEAD nonce/AAD
//     reconstruction on encrypted volumes stays correct after coalescing
//   - chunk boundaries (ObjBlockSize) produce one extent per chunk, since
//     cross-chunk coalescing is architecturally impossible

package viperblock

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// newCoalesceTestVB stands up a file-backed VB with WAL files already open
// and a caller-chosen ObjBlockSize, so tests can control exactly how many
// blocks land in one chunk (and therefore one coalesced extent). key == nil
// gives an unencrypted volume.
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

// TestBlockMapCoalesce_SequentialWrite pins the core fix: a single
// consecutive write that fits in one chunk must produce exactly one
// BlockLookup entry and one BlockStore persisted extent, not one entry per
// block -- and every block must still read back byte-identical, including
// blocks that are not the extent's StartBlock map key.
func TestBlockMapCoalesce_SequentialWrite(t *testing.T) {
	const numBlocks = 50
	vb := newCoalesceTestVB(t, "vol-seq-write", 4096 /* huge, single chunk */, nil)

	data := randomBlocks(t, vb, numBlocks)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.BlocksToObject.mu.RLock()
	require.Len(t, vb.BlocksToObject.BlockLookup, 1, "one consecutive run must coalesce into one BlockLookup entry")
	entry := vb.BlocksToObject.BlockLookup[0]
	vb.BlocksToObject.mu.RUnlock()
	require.Equal(t, uint16(numBlocks), entry.NumBlocks)

	require.Equal(t, 1, vb.BlockStore.PersistedExtentCount(), "one consecutive run must coalesce into one persistedExtent")

	// Read back every block, not just the run's StartBlock, to prove range
	// resolution (not just the map-key hit) works.
	got, err := vb.ReadAt(0, uint64(numBlocks)*uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got), "sequential write readback must be byte-identical")

	// Single-block reads from the middle and end of the run must also
	// resolve correctly via the range lookup, not just a whole-range read.
	for _, block := range []uint64{0, 1, numBlocks / 2, numBlocks - 1} {
		want := data[block*uint64(vb.BlockSize) : (block+1)*uint64(vb.BlockSize)]
		got, err := vb.ReadAt(block*uint64(vb.BlockSize), uint64(vb.BlockSize))
		require.NoError(t, err)
		require.True(t, bytes.Equal(want, got), "block %d readback mismatch", block)
	}
}

// TestBlockMapCoalesce_OverwriteFracturesExtent pins the partial-range
// invalidation constraint: an overwrite landing inside an existing
// coalesced extent must fracture it into its surviving head/tail, and every
// block -- head, overwritten middle, and tail -- must read back correctly
// afterwards.
func TestBlockMapCoalesce_OverwriteFracturesExtent(t *testing.T) {
	const numBlocks = 20
	vb := newCoalesceTestVB(t, "vol-overwrite-fracture", 4096, nil)

	original := randomBlocks(t, vb, numBlocks)
	require.NoError(t, vb.WriteAt(0, original))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.BlocksToObject.mu.RLock()
	require.Len(t, vb.BlocksToObject.BlockLookup, 1)
	vb.BlocksToObject.mu.RUnlock()

	// Overwrite blocks [8, 12) -- strictly inside the [0, 20) extent, with
	// surviving blocks on both sides.
	overwrite := randomBlocks(t, vb, 4)
	require.NoError(t, vb.WriteAt(8*uint64(vb.BlockSize), overwrite))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.BlocksToObject.mu.RLock()
	require.Len(t, vb.BlocksToObject.BlockLookup, 3, "overwrite must fracture the original extent into head/new/tail")
	head, headOK := vb.BlocksToObject.BlockLookup[0]
	mid, midOK := vb.BlocksToObject.BlockLookup[8]
	tail, tailOK := vb.BlocksToObject.BlockLookup[12]
	vb.BlocksToObject.mu.RUnlock()

	require.True(t, headOK && midOK && tailOK, "expected entries keyed at 0, 8, 12")
	require.Equal(t, uint16(8), head.NumBlocks)
	require.Equal(t, uint16(4), mid.NumBlocks)
	require.Equal(t, uint16(8), tail.NumBlocks)
	require.Equal(t, uint64(0), head.ObjectID, "head must still point at the original chunk")
	require.Equal(t, uint64(1), mid.ObjectID, "overwritten range must point at the new chunk")
	require.Equal(t, uint64(0), tail.ObjectID, "tail must still point at the original chunk")

	// Build the expected composite buffer: original head + overwrite + original tail.
	want := make([]byte, 0, numBlocks*int(vb.BlockSize))
	want = append(want, original[:8*vb.BlockSize]...)
	want = append(want, overwrite...)
	want = append(want, original[12*vb.BlockSize:]...)

	got, err := vb.ReadAt(0, uint64(numBlocks)*uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(want, got), "readback after fracture must reflect head/overwrite/tail exactly")

	// Explicitly check the tail's first block (block 12): this is the block
	// most likely to break if the fractured tail's ObjectOffset isn't
	// recomputed relative to the original extent's start.
	wantTailBlock := original[12*vb.BlockSize : 13*vb.BlockSize]
	gotTailBlock, err := vb.ReadAt(12*uint64(vb.BlockSize), uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(wantTailBlock, gotTailBlock), "tail block 12 readback mismatch")
}

// TestBlockMapCoalesce_MultiChunkBoundary pins that coalescing respects
// chunk boundaries: cross-chunk coalescing is architecturally impossible
// (different chunks are different backend objects), so a sequential write
// spanning multiple chunks must produce exactly one extent per chunk.
func TestBlockMapCoalesce_MultiChunkBoundary(t *testing.T) {
	const blocksPerChunk = 8
	const numBlocks = 20 // spans 3 chunks: 8 + 8 + 4
	vb := newCoalesceTestVB(t, "vol-multi-chunk", blocksPerChunk, nil)

	data := randomBlocks(t, vb, numBlocks)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.BlocksToObject.mu.RLock()
	require.Len(t, vb.BlocksToObject.BlockLookup, 3, "20 blocks at 8 blocks/chunk must produce 3 extents")
	e0, ok0 := vb.BlocksToObject.BlockLookup[0]
	e1, ok1 := vb.BlocksToObject.BlockLookup[8]
	e2, ok2 := vb.BlocksToObject.BlockLookup[16]
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

// TestBlockMapCoalesce_EncryptedReadback pins per-block SeqNum fidelity
// through coalescing on an encrypted volume: blocks in a coalesced run are
// not guaranteed to share a SeqNum (blocks are sorted by number, not write
// order, before coalescing), and AEAD nonce/AAD reconstruction on read
// depends on each block's own SeqNum surviving the coalesce.
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
	require.Len(t, vb.BlocksToObject.BlockLookup, 3, "encrypted volumes coalesce the same way as plaintext ones")
	vb.BlocksToObject.mu.RUnlock()

	got, err := vb.ReadAt(0, uint64(numBlocks)*uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(data, got), "encrypted sequential readback must be byte-identical")

	// Now overwrite a range straddling a chunk boundary (blocks [6, 10)),
	// which spans the first two chunks and forces per-block SeqNum tracking
	// across a fracture on an encrypted volume.
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

	// Force a cold reload of the block map from a saved checkpoint --
	// exercising parseBlockCheckpoint's recoalescing (coalesceBlockLookup)
	// and BlockStore's SetPersistedRange restore path -- then confirm the
	// reloaded, recoalesced map still decrypts every block correctly.
	require.NoError(t, vb.SaveLiveCheckpoint())
	vb.BlockStore = NewUnifiedBlockStore(vb.BlockSize)
	require.NoError(t, vb.LoadLiveCheckpoint())

	got, err = vb.ReadAt(0, uint64(numBlocks)*uint64(vb.BlockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(want, got), "cold encrypted readback after reload must still be byte-identical")
}

// TestBlockMapCoalesce_EntryCountIsExtentsNotBlocks is the bead's headline
// acceptance criterion: a large sequential write must grow the block map by
// O(extents), not O(blocks). Before this fix, createChunkFile wrote one
// BlockLookup entry per physical block, so nbdkit RSS scaled linearly with
// total bytes ever written.
func TestBlockMapCoalesce_EntryCountIsExtentsNotBlocks(t *testing.T) {
	const blocksPerChunk = 1024 // matches the production default (4MB / 4KB)
	const numBlocks = 5000      // spans ceil(5000/1024) = 5 chunks
	vb := newCoalesceTestVB(t, "vol-entry-count", blocksPerChunk, nil)

	data := randomBlocks(t, vb, numBlocks)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.BlocksToObject.mu.RLock()
	entryCount := len(vb.BlocksToObject.BlockLookup)
	vb.BlocksToObject.mu.RUnlock()

	wantExtents := (numBlocks + blocksPerChunk - 1) / blocksPerChunk // ceil
	require.Equal(t, wantExtents, entryCount,
		"map must grow by O(extents) (%d chunks), not O(blocks) (%d blocks)", wantExtents, numBlocks)
	require.Equal(t, wantExtents, vb.BlockStore.PersistedExtentCount())
	require.Less(t, entryCount, numBlocks/10, "sanity: coalesced entry count must be far smaller than the block count")
}
