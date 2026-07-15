package viperblock

import (
	"crypto/rand"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateSnapshot writes blocks, creates a snapshot, then verifies the
// snapshot checkpoint and config exist on the backend.
func TestCreateSnapshot(t *testing.T) {
	runWithBackends(t, "create_snapshot", func(t *testing.T, vb *VB) {
		// Write some blocks
		data := make([]byte, DefaultBlockSize*4)
		rand.Read(data)

		err := vb.Write(0, data)
		require.NoError(t, err)

		err = vb.Flush()
		require.NoError(t, err)

		err = vb.WriteWALToChunk(true)
		require.NoError(t, err)

		// Create snapshot
		snapshotID := fmt.Sprintf("snap-%s", vb.VolumeName)
		snap, err := vb.CreateSnapshot(snapshotID)
		require.NoError(t, err)
		require.NotNil(t, snap)

		assert.Equal(t, snapshotID, snap.SnapshotID)
		assert.Equal(t, vb.VolumeName, snap.SourceVolumeName)
		assert.Equal(t, vb.BlockSize, snap.BlockSize)
		assert.Equal(t, vb.ObjBlockSize, snap.ObjBlockSize)

		// Verify we can load the snapshot block map back
		baseMap, ident, err := vb.LoadSnapshotBlockMap(snapshotID)
		require.NoError(t, err)
		assert.Equal(t, vb.VolumeName, ident.SourceVolumeName)
		assert.Len(t, baseMap.BlockLookup, len(vb.BlocksToObject.BlockLookup))

		// Verify each block mapping matches
		for block, lookup := range vb.BlocksToObject.BlockLookup {
			baseLookup, ok := baseMap.BlockLookup[block]
			assert.True(t, ok, "block %d missing from snapshot", block)
			assert.Equal(t, lookup.ObjectID, baseLookup.ObjectID)
			assert.Equal(t, lookup.ObjectOffset, baseLookup.ObjectOffset)
		}
	})
}

// TestSnapshotReadFallback creates a snapshot, creates a clone, and verifies
// that reading unmodified blocks returns the snapshot data.
func TestSnapshotReadFallback(t *testing.T) {
	runWithBackends(t, "snapshot_read_fallback", func(t *testing.T, vb *VB) {
		// Write data to the source volume
		blockCount := uint64(8)
		data := make([]byte, uint64(DefaultBlockSize)*blockCount)
		rand.Read(data)

		err := vb.Write(0, data)
		require.NoError(t, err)

		err = vb.Flush()
		require.NoError(t, err)

		err = vb.WriteWALToChunk(true)
		require.NoError(t, err)

		// Create snapshot
		snapshotID := fmt.Sprintf("snap-%s", vb.VolumeName)
		_, err = vb.CreateSnapshot(snapshotID)
		require.NoError(t, err)

		// Create a "clone" by loading the snapshot's base map onto a fresh VB
		// that uses the same backend (shares the same storage)
		clone := createCloneVB(t, vb, snapshotID)

		// Read all blocks from the clone -- should return snapshot data
		for i := range blockCount {
			readData, err := clone.ReadAt(i*uint64(clone.BlockSize), uint64(clone.BlockSize))
			require.NoError(t, err)

			expected := data[i*uint64(DefaultBlockSize) : (i+1)*uint64(DefaultBlockSize)]
			assert.Equal(t, expected, readData, "block %d mismatch", i)
		}
	})
}

// TestSnapshotCopyOnWrite verifies that after cloning, writes to the clone
// shadow the snapshot data, while unmodified blocks still return original data.
func TestSnapshotCopyOnWrite(t *testing.T) {
	runWithBackends(t, "snapshot_cow", func(t *testing.T, vb *VB) {
		// Write data to source volume
		blockCount := uint64(8)
		originalData := make([]byte, uint64(DefaultBlockSize)*blockCount)
		rand.Read(originalData)

		err := vb.Write(0, originalData)
		require.NoError(t, err)
		err = vb.Flush()
		require.NoError(t, err)
		err = vb.WriteWALToChunk(true)
		require.NoError(t, err)

		// Create snapshot
		snapshotID := fmt.Sprintf("snap-%s", vb.VolumeName)
		_, err = vb.CreateSnapshot(snapshotID)
		require.NoError(t, err)

		// Create clone
		clone := createCloneVB(t, vb, snapshotID)

		// Overwrite block 2 in the clone
		newData := make([]byte, DefaultBlockSize)
		rand.Read(newData)

		err = clone.Write(2, newData)
		require.NoError(t, err)
		err = clone.Flush()
		require.NoError(t, err)
		err = clone.WriteWALToChunk(true)
		require.NoError(t, err)

		// Block 2 should return the new data
		readData, err := clone.ReadAt(2*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err)
		assert.Equal(t, newData, readData, "overwritten block should return new data")

		// Block 0 should return original snapshot data
		readData, err = clone.ReadAt(0, uint64(clone.BlockSize))
		require.NoError(t, err)
		assert.Equal(t, originalData[:DefaultBlockSize], readData, "unmodified block should return snapshot data")

		// Block 5 should also return original snapshot data
		readData, err = clone.ReadAt(5*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err)
		expected := originalData[5*uint64(DefaultBlockSize) : 6*uint64(DefaultBlockSize)]
		assert.Equal(t, expected, readData, "unmodified block 5 should return snapshot data")
	})
}

// TestSnapshotZeroBlocks verifies that blocks never written in either the
// snapshot or the clone return zero bytes.
func TestSnapshotZeroBlocks(t *testing.T) {
	runWithBackends(t, "snapshot_zero_blocks", func(t *testing.T, vb *VB) {
		// Write data to blocks 0-3 only
		data := make([]byte, DefaultBlockSize*4)
		rand.Read(data)

		err := vb.Write(0, data)
		require.NoError(t, err)
		err = vb.Flush()
		require.NoError(t, err)
		err = vb.WriteWALToChunk(true)
		require.NoError(t, err)

		// Create snapshot
		snapshotID := fmt.Sprintf("snap-%s", vb.VolumeName)
		_, err = vb.CreateSnapshot(snapshotID)
		require.NoError(t, err)

		// Create clone
		clone := createCloneVB(t, vb, snapshotID)

		// Block 100 was never written -- should return zeros
		readData, err := clone.ReadAt(100*uint64(clone.BlockSize), uint64(clone.BlockSize))
		assert.ErrorIs(t, err, ErrZeroBlock)
		assert.Equal(t, make([]byte, clone.BlockSize), readData)

		// Block 2 was written -- should return data
		readData, err = clone.ReadAt(2*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err)
		assert.Equal(t, data[2*int(DefaultBlockSize):3*int(DefaultBlockSize)], readData)
	})
}

// TestSnapshotMultiChunk tests that snapshots work correctly when the source
// volume's data spans multiple chunk files.
func TestSnapshotMultiChunk(t *testing.T) {
	runWithBackends(t, "snapshot_multi_chunk", func(t *testing.T, vb *VB) {
		// Write enough data to span multiple 4MB chunks
		// ObjBlockSize is 4MB = 1024 blocks of 4KB each
		// Write 2048 blocks to get at least 2 chunks
		blocksPerChunk := vb.ObjBlockSize / vb.BlockSize
		totalBlocks := blocksPerChunk * 2

		data := make([]byte, uint64(totalBlocks)*uint64(vb.BlockSize))
		rand.Read(data)

		err := vb.Write(0, data)
		require.NoError(t, err)
		err = vb.Flush()
		require.NoError(t, err)
		err = vb.WriteWALToChunk(true)
		require.NoError(t, err)

		// Should have created at least 2 chunks
		assert.GreaterOrEqual(t, vb.ObjectNum.Load(), uint64(2), "should have multiple chunks")

		// Create snapshot
		snapshotID := fmt.Sprintf("snap-%s", vb.VolumeName)
		_, err = vb.CreateSnapshot(snapshotID)
		require.NoError(t, err)

		// Create clone
		clone := createCloneVB(t, vb, snapshotID)

		// Read blocks from different chunks
		// Block 0 (first chunk)
		readData, err := clone.ReadAt(0, uint64(clone.BlockSize))
		require.NoError(t, err)
		assert.Equal(t, data[:DefaultBlockSize], readData)

		// Block in second chunk
		secondChunkBlock := uint64(blocksPerChunk + 10)
		readData, err = clone.ReadAt(secondChunkBlock*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err)
		expected := data[secondChunkBlock*uint64(DefaultBlockSize) : (secondChunkBlock+1)*uint64(DefaultBlockSize)]
		assert.Equal(t, expected, readData)

		// Last block
		lastBlock := uint64(totalBlocks - 1)
		readData, err = clone.ReadAt(lastBlock*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err)
		expected = data[lastBlock*uint64(DefaultBlockSize) : (lastBlock+1)*uint64(DefaultBlockSize)]
		assert.Equal(t, expected, readData)
	})
}

// TestSnapshotWithUnopenedShardedWAL simulates the viperblockd scenario where
// a VB instance has UseShardedWAL=true but never opened the WAL files (because
// the NBD plugin process owns them). Calling CreateSnapshot on such an instance
// must not corrupt the WAL directory, and a second snapshot must also succeed.
// This is a regression test for the shard WAL corruption bug where
// WriteShardedWALToChunk would rotate into WAL files it didn't own.
func TestSnapshotWithUnopenedShardedWAL(t *testing.T) {
	runWithBackends(t, "snapshot_unopened_wal", func(t *testing.T, vb *VB) {
		if !vb.UseShardedWAL {
			t.Skip("test only applies to sharded WAL mode")
		}

		// Write data and consolidate using the "owner" VB (simulates NBD plugin)
		data := make([]byte, DefaultBlockSize*4)
		rand.Read(data)

		err := vb.Write(0, data)
		require.NoError(t, err)
		err = vb.Flush()
		require.NoError(t, err)
		err = vb.WriteWALToChunk(true)
		require.NoError(t, err)

		// Create a second VB that loads state but does NOT open WAL files
		// (simulates viperblockd's snapshot VB)
		snapshotVB := &VB{
			VolumeName:    vb.VolumeName,
			VolumeSize:    vb.VolumeSize,
			BlockSize:     vb.BlockSize,
			ObjBlockSize:  vb.ObjBlockSize,
			Version:       vb.Version,
			BaseDir:       vb.BaseDir,
			UseShardedWAL: true,
			ShardedWAL:    NewShardedWAL(vb.BaseDir, vb.WAL.WALMagic),
			Backend:       vb.Backend,
			ChunkMagic:    vb.ChunkMagic,
			BlocksToObject: BlocksToObject{
				BlockLookup: make(map[uint64]BlockLookup),
			},
			BlockStore:    NewUnifiedBlockStore(vb.BlockSize),
			UseBlockStore: true,
		}
		// Copy block-to-object map (simulates LoadState + LoadBlockState)
		vb.BlocksToObject.mu.RLock()
		maps.Copy(snapshotVB.BlocksToObject.BlockLookup, vb.BlocksToObject.BlockLookup)
		vb.BlocksToObject.mu.RUnlock()
		snapshotVB.SeqNum.Store(vb.SeqNum.Load())
		snapshotVB.ObjectNum.Store(vb.ObjectNum.Load())
		snapshotVB.ShardedWAL.WallNum.Store(vb.ShardedWAL.WallNum.Load())

		// First snapshot on the unopened-WAL VB should succeed
		snap1, err := snapshotVB.CreateSnapshot("snap-first")
		require.NoError(t, err)
		require.NotNil(t, snap1)

		// Write more data through the owner VB (simulates NBD writes continuing)
		moreData := make([]byte, DefaultBlockSize*4)
		rand.Read(moreData)
		err = vb.Write(4, moreData)
		require.NoError(t, err)
		err = vb.Flush()
		require.NoError(t, err)
		err = vb.WriteWALToChunk(true)
		require.NoError(t, err)

		// Second snapshot should also succeed (the bug caused checksum mismatch here)
		snap2, err := snapshotVB.CreateSnapshot("snap-second")
		require.NoError(t, err)
		require.NotNil(t, snap2)

		// Verify the owner VB's WAL is not corrupted — it can still write and consolidate
		err = vb.Write(8, data)
		require.NoError(t, err)
		err = vb.Flush()
		require.NoError(t, err)
		err = vb.WriteWALToChunk(true)
		require.NoError(t, err, "owner VB WAL should not be corrupted by snapshot VB")
	})
}

// TestLoadFromSnapshot verifies that a clone can be closed and reopened,
// with the base map restored from the saved state.
func TestLoadFromSnapshot(t *testing.T) {
	runWithBackends(t, "load_from_snapshot", func(t *testing.T, vb *VB) {
		// Write data to source
		data := make([]byte, DefaultBlockSize*4)
		rand.Read(data)

		err := vb.Write(0, data)
		require.NoError(t, err)
		err = vb.Flush()
		require.NoError(t, err)
		err = vb.WriteWALToChunk(true)
		require.NoError(t, err)

		// Create snapshot
		snapshotID := fmt.Sprintf("snap-%s", vb.VolumeName)
		_, err = vb.CreateSnapshot(snapshotID)
		require.NoError(t, err)

		// Create clone and write some new data
		clone := createCloneVB(t, vb, snapshotID)

		newData := make([]byte, DefaultBlockSize)
		rand.Read(newData)
		err = clone.Write(10, newData)
		require.NoError(t, err)
		err = clone.Flush()
		require.NoError(t, err)
		err = clone.WriteWALToChunk(true)
		require.NoError(t, err)

		// Save clone state
		err = clone.SaveState()
		require.NoError(t, err)
		err = clone.SaveBlockState()
		require.NoError(t, err)

		// Simulate reopening: reset and reload
		clone.BaseBlockMap = nil
		clone.SourceVolumeName = ""
		clone.SnapshotID = ""
		clone.Reset()

		err = clone.LoadState()
		require.NoError(t, err)

		// Verify snapshot fields restored
		assert.Equal(t, snapshotID, clone.SnapshotID)
		assert.Equal(t, vb.VolumeName, clone.SourceVolumeName)
		assert.NotNil(t, clone.BaseBlockMap)

		// Reload clone's own block state
		err = clone.LoadBlockState()
		require.NoError(t, err)

		// Read snapshot block (block 0) -- should work via base map
		readData, err := clone.ReadAt(0, uint64(clone.BlockSize))
		require.NoError(t, err)
		assert.Equal(t, data[:DefaultBlockSize], readData)

		// Read clone's own block (block 10)
		readData, err = clone.ReadAt(10*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err)
		assert.Equal(t, newData, readData)
	})
}

// TestSnapshotCOWChainDeep exercises the buildFlatSection ancestors loop by
// creating a 3-generation chain (base→cloneA→cloneB→cloneC). cloneB's snapshot
// must encode 2 inherited layers, and cloneC must resolve reads from all three.
func TestSnapshotCOWChainDeep(t *testing.T) {
	runWithBackends(t, "snapshot_cow_chain_deep", func(t *testing.T, base *VB) {
		blockSize := uint64(base.BlockSize)

		// Layer 0 (base): block 0 only.
		baseData := make([]byte, blockSize)
		rand.Read(baseData)
		require.NoError(t, base.Write(0, baseData))
		require.NoError(t, base.Flush())
		require.NoError(t, base.WriteWALToChunk(true))

		// Use short snapshot IDs to prevent the cascading clone-name from exceeding
		// the filesystem path length limit at 3+ generations.
		baseSnapID := "s0"
		_, err := base.CreateSnapshot(baseSnapID)
		require.NoError(t, err)

		// Layer 1 (cloneA): block 1 only; snapshot has 1 inherited layer (base).
		cloneA := createCloneVB(t, base, baseSnapID)
		cloneAData := make([]byte, blockSize)
		rand.Read(cloneAData)
		require.NoError(t, cloneA.Write(1, cloneAData))
		require.NoError(t, cloneA.Flush())
		require.NoError(t, cloneA.WriteWALToChunk(true))

		cloneASnapID := "s1"
		snapA, err := cloneA.CreateSnapshot(cloneASnapID)
		require.NoError(t, err)
		assert.True(t, snapA.HasFlatSection)

		// Layer 2 (cloneB): block 2 only; snapshot must encode 2 inherited layers
		// (cloneA delta + base) because cloneB.ancestors is non-empty.
		cloneB := createCloneVB(t, cloneA, cloneASnapID)
		cloneBData := make([]byte, blockSize)
		rand.Read(cloneBData)
		require.NoError(t, cloneB.Write(2, cloneBData))
		require.NoError(t, cloneB.Flush())
		require.NoError(t, cloneB.WriteWALToChunk(true))

		cloneBSnapID := "s2"
		snapB, err := cloneB.CreateSnapshot(cloneBSnapID)
		require.NoError(t, err)
		assert.True(t, snapB.HasFlatSection)

		_, ident, err := cloneB.LoadSnapshotBlockMap(cloneBSnapID)
		require.NoError(t, err)
		assert.Len(t, ident.InheritedLayers, 2, "flat section must encode cloneA delta and base blocks as separate layers")

		// Layer 3 (cloneC): read-only; resolves each block through a different ancestor.
		cloneC := createCloneVB(t, cloneB, cloneBSnapID)

		got, err := cloneC.ReadAt(0, blockSize)
		require.NoError(t, err)
		assert.Equal(t, baseData, got, "block 0 must come from base (deepest ancestor)")

		got, err = cloneC.ReadAt(blockSize, blockSize)
		require.NoError(t, err)
		assert.Equal(t, cloneAData, got, "block 1 must come from cloneA (ancestors[0])")

		got, err = cloneC.ReadAt(2*blockSize, blockSize)
		require.NoError(t, err)
		assert.Equal(t, cloneBData, got, "block 2 must come from cloneB (BaseBlockMap)")

		got, err = cloneC.ReadAt(3*blockSize, blockSize)
		assert.ErrorIs(t, err, ErrZeroBlock)
		assert.Equal(t, make([]byte, blockSize), got)
	})
}

// TestFlatSectionBinaryRoundtrip calls buildFlatSection directly on a COW clone
// and verifies the wire format (magic, version, numSources, source name, block
// entries) before confirming parseFlatSection decodes it back with fidelity.
func TestFlatSectionBinaryRoundtrip(t *testing.T) {
	runWithBackends(t, "flat_section_roundtrip", func(t *testing.T, vb *VB) {
		blockSize := uint64(vb.BlockSize)

		data := make([]byte, blockSize*2)
		rand.Read(data)
		require.NoError(t, vb.Write(0, data))
		require.NoError(t, vb.Flush())
		require.NoError(t, vb.WriteWALToChunk(true))

		baseSnapID := fmt.Sprintf("snap-%s", vb.VolumeName)
		_, err := vb.CreateSnapshot(baseSnapID)
		require.NoError(t, err)

		// Clone has vb's blocks as BaseBlockMap and no own writes or ancestors.
		// buildFlatSection must encode exactly 1 source group.
		clone := createCloneVB(t, vb, baseSnapID)

		raw, err := clone.buildFlatSection()
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(raw), 6, "flat section must be at least 6 bytes")

		assert.Equal(t, []byte("FLAT"), raw[0:4], "magic")
		assert.Equal(t, uint8(1), raw[4], "version")
		assert.Equal(t, uint8(1), raw[5], "numSources must be 1")

		layers, err := parseFlatSection(raw)
		require.NoError(t, err)
		require.Len(t, layers, 1)

		assert.Equal(t, vb.VolumeName, layers[0].SourceVolumeName, "decoded source name")

		vb.BlocksToObject.mu.RLock()
		defer vb.BlocksToObject.mu.RUnlock()

		assert.Len(t, layers[0].Blocks.BlockLookup, len(vb.BlocksToObject.BlockLookup),
			"decoded block count must match parent")

		for blockNum, want := range vb.BlocksToObject.BlockLookup {
			got, ok := layers[0].Blocks.BlockLookup[blockNum]
			assert.True(t, ok, "block %d missing from decoded flat section", blockNum)
			if ok {
				assert.Equal(t, want.ObjectID, got.ObjectID, "block %d ObjectID", blockNum)
				assert.Equal(t, want.ObjectOffset, got.ObjectOffset, "block %d ObjectOffset", blockNum)
				assert.Equal(t, want.SeqNum, got.SeqNum, "block %d SeqNum", blockNum)
			}
		}
	})
}

// TestLiveCheckpointFallback verifies that when no live checkpoint exists,
// LoadLiveCheckpoint falls back to LoadBlockState and recovers the block map.
func TestLiveCheckpointFallback(t *testing.T) {
	runWithBackends(t, "live_checkpoint_fallback", func(t *testing.T, vb *VB) {
		data := make([]byte, DefaultBlockSize*3)
		rand.Read(data)

		require.NoError(t, vb.Write(0, data))
		require.NoError(t, vb.Flush())

		if vb.UseShardedWAL {
			require.NoError(t, vb.WriteShardedWALToChunk(true))
		} else {
			require.NoError(t, vb.WriteWALToChunk(true))
		}

		// SaveBlockState (not SaveLiveCheckpoint) — no live checkpoint will exist.
		require.NoError(t, vb.SaveBlockState())

		reader := &VB{
			VolumeName:   vb.VolumeName,
			VolumeSize:   vb.VolumeSize,
			BlockSize:    vb.BlockSize,
			ObjBlockSize: vb.ObjBlockSize,
			Version:      vb.Version,
			BaseDir:      vb.BaseDir,
			Backend:      vb.Backend,
			BlockToObjectWAL: WAL{
				WALMagic: vb.BlockToObjectWAL.WALMagic,
			},
			BlocksToObject: BlocksToObject{
				BlockLookup: make(map[uint64]BlockLookup),
			},
		}

		// No live checkpoint exists: must fall back to LoadBlockState.
		require.NoError(t, reader.LoadLiveCheckpoint())

		vb.BlocksToObject.mu.RLock()
		defer vb.BlocksToObject.mu.RUnlock()

		assert.Len(t, reader.BlocksToObject.BlockLookup, len(vb.BlocksToObject.BlockLookup),
			"fallback must restore the full block map")

		for blockNum, want := range vb.BlocksToObject.BlockLookup {
			got, ok := reader.BlocksToObject.BlockLookup[blockNum]
			assert.True(t, ok, "block %d missing after fallback", blockNum)
			if ok {
				assert.Equal(t, want.ObjectID, got.ObjectID, "block %d ObjectID", blockNum)
				assert.Equal(t, want.ObjectOffset, got.ObjectOffset, "block %d ObjectOffset", blockNum)
			}
		}
	})
}

// TestLiveCheckpointRoundtrip verifies that SaveLiveCheckpoint serialises the
// block map to a fixed backend key and LoadLiveCheckpoint restores it exactly
// in a separate VB instance, including small writes below the 4 MB WAL flush threshold.
func TestLiveCheckpointRoundtrip(t *testing.T) {
	runWithBackends(t, "live_checkpoint", func(t *testing.T, vb *VB) {
		// 3 blocks = 12 KiB — well below the 4 MB WAL consolidation threshold.
		data := make([]byte, DefaultBlockSize*3)
		rand.Read(data)

		require.NoError(t, vb.Write(0, data))
		require.NoError(t, vb.Flush())

		// Force-flush WAL to the backend before checkpointing; without force=true
		// blocks below the threshold stay in the local WAL and are absent from
		// the checkpoint.
		if vb.UseShardedWAL {
			require.NoError(t, vb.WriteShardedWALToChunk(true))
		} else {
			require.NoError(t, vb.WriteWALToChunk(true))
		}

		require.NoError(t, vb.SaveLiveCheckpoint())

		// Reader VB shares the same backend but has no WAL files open —
		// this mirrors the snapshotRunningVolume path in spinifex.
		reader := &VB{
			VolumeName:   vb.VolumeName,
			VolumeSize:   vb.VolumeSize,
			BlockSize:    vb.BlockSize,
			ObjBlockSize: vb.ObjBlockSize,
			Version:      vb.Version,
			BaseDir:      vb.BaseDir,
			Backend:      vb.Backend,
			BlockToObjectWAL: WAL{
				WALMagic: vb.BlockToObjectWAL.WALMagic,
			},
			BlocksToObject: BlocksToObject{
				BlockLookup: make(map[uint64]BlockLookup),
			},
		}

		require.NoError(t, reader.LoadLiveCheckpoint())

		vb.BlocksToObject.mu.RLock()
		defer vb.BlocksToObject.mu.RUnlock()

		assert.Len(t, reader.BlocksToObject.BlockLookup, len(vb.BlocksToObject.BlockLookup),
			"live checkpoint must contain all written blocks")

		for blockNum, want := range vb.BlocksToObject.BlockLookup {
			got, ok := reader.BlocksToObject.BlockLookup[blockNum]
			assert.True(t, ok, "block %d missing from live checkpoint", blockNum)
			if ok {
				assert.Equal(t, want.ObjectID, got.ObjectID, "block %d: ObjectID mismatch", blockNum)
				assert.Equal(t, want.ObjectOffset, got.ObjectOffset, "block %d: ObjectOffset mismatch", blockNum)
				assert.Equal(t, want.NumBlocks, got.NumBlocks, "block %d: NumBlocks mismatch", blockNum)
				assert.Equal(t, want.SeqNum, got.SeqNum, "block %d: SeqNum mismatch", blockNum)
			}
		}
	})
}

// TestObjectNumReconcileOnLoad verifies that loading a block map reconciles
// ObjectNum to max(ObjectID)+1. Runtime chunk drains do not persist config.json
// ObjectNum, so a re-Open can load a stale-low value; without reconciliation the
// next createChunkFile would reuse a live chunk ID and corrupt it (durable AEAD
// tag failure). A pre-set higher ObjectNum must not be lowered.
func TestObjectNumReconcileOnLoad(t *testing.T) {
	runWithBackends(t, "objectnum_reconcile", func(t *testing.T, vb *VB) {
		// Three separate write+drain cycles produce three chunks with distinct
		// ObjectIDs 0,1,2, leaving vb.ObjectNum at 3.
		for i := range uint64(3) {
			data := make([]byte, DefaultBlockSize)
			rand.Read(data)
			require.NoError(t, vb.Write(i, data))
			require.NoError(t, vb.Flush())
			if vb.UseShardedWAL {
				require.NoError(t, vb.WriteShardedWALToChunk(true))
			} else {
				require.NoError(t, vb.WriteWALToChunk(true))
			}
		}
		require.NoError(t, vb.SaveLiveCheckpoint())

		var maxObjectID uint64
		vb.BlocksToObject.mu.RLock()
		for _, bl := range vb.BlocksToObject.BlockLookup {
			if bl.ObjectID > maxObjectID {
				maxObjectID = bl.ObjectID
			}
		}
		vb.BlocksToObject.mu.RUnlock()
		require.Positive(t, maxObjectID, "expected multiple chunks")

		newReader := func() *VB {
			return &VB{
				VolumeName:   vb.VolumeName,
				VolumeSize:   vb.VolumeSize,
				BlockSize:    vb.BlockSize,
				ObjBlockSize: vb.ObjBlockSize,
				Version:      vb.Version,
				BaseDir:      vb.BaseDir,
				Backend:      vb.Backend,
				BlockToObjectWAL: WAL{
					WALMagic: vb.BlockToObjectWAL.WALMagic,
				},
				BlocksToObject: BlocksToObject{
					BlockLookup: make(map[uint64]BlockLookup),
				},
			}
		}

		// Stale-low reader (ObjectNum defaults to 0) is bumped to max+1.
		stale := newReader()
		require.NoError(t, stale.LoadLiveCheckpoint())
		assert.Equal(t, maxObjectID+1, stale.ObjectNum.Load(),
			"stale ObjectNum must reconcile to max chunk ID + 1")

		// Already-ahead reader is left untouched (max semantics, never lowered).
		ahead := newReader()
		ahead.ObjectNum.Store(maxObjectID + 100)
		require.NoError(t, ahead.LoadLiveCheckpoint())
		assert.Equal(t, maxObjectID+100, ahead.ObjectNum.Load(),
			"reconcile must never lower an already-higher ObjectNum")
	})
}

// TestRecoverLocalWALsNoChunkReuse reproduces the durable AEAD corruption where
// a re-Open with a stale-low ObjectNum lets WAL recovery reuse a chunk ID that
// the loaded map still references, overwriting live chunk data. It drives the
// legacy-WAL file path (shardwal=false) that nbdkit runs in production. Without
// the ObjectNum reconcile in RecoverLocalWALs the recovered chunks collide with
// the existing ones and the pre-existing blocks fail to read back.
func TestRecoverLocalWALsNoChunkReuse(t *testing.T) {
	backend := BackendTest{
		Name:          "file_legacywal",
		BackendType:   FileBackend,
		CacheConfig:   CacheConfig{Size: 0},
		UseShardedWAL: false,
	}
	vb, _, shutdown, err := setupTestVB(t, TestVB{name: "recover_no_reuse"}, backend)
	require.NoError(t, err)
	defer shutdown(vb.GetVolume())

	// Three write+drain cycles produce chunks with ObjectIDs 0,1,2 (ObjectNum=3),
	// each block widely separated so recovery re-buffers them into fresh chunks.
	seeded := []uint64{100, 200, 300}
	want := make(map[uint64][]byte, len(seeded))
	for _, blk := range seeded {
		data := make([]byte, DefaultBlockSize)
		rand.Read(data)
		want[blk] = data
		require.NoError(t, vb.Write(blk, data))
		require.NoError(t, vb.Flush())
		require.NoError(t, vb.WriteWALToChunk(true))
	}

	var maxObjectID uint64
	vb.BlocksToObject.mu.RLock()
	for _, bl := range vb.BlocksToObject.BlockLookup {
		if bl.ObjectID > maxObjectID {
			maxObjectID = bl.ObjectID
		}
	}
	vb.BlocksToObject.mu.RUnlock()
	require.Equal(t, uint64(2), maxObjectID, "expected three chunks with ObjectIDs 0,1,2")

	// Delete the drained WAL files (keeping the highest-numbered active one) so
	// the seeded blocks stay mapped to chunks 0,1,2 but are no longer replayable.
	// This is the orphaned-map-entry state a killed VB leaves after consolidation:
	// the chunk objects and their map entries are live, their source WALs are not.
	walDir := filepath.Join(vb.BaseDir, vb.GetVolume(), "wal", "chunks")
	walEntries, err := os.ReadDir(walDir)
	require.NoError(t, err)
	var walNames []string
	for _, e := range walEntries {
		if !e.IsDir() {
			walNames = append(walNames, e.Name())
		}
	}
	require.Greater(t, len(walNames), 1, "expected drained WALs to be retained")
	sort.Strings(walNames)
	for _, name := range walNames[:len(walNames)-1] {
		require.NoError(t, os.Remove(filepath.Join(walDir, name)))
	}

	// A fourth block flushed to the surviving active WAL but never chunked — the
	// only block recovery will replay.
	orphan := make([]byte, DefaultBlockSize)
	rand.Read(orphan)
	want[400] = orphan
	require.NoError(t, vb.Write(400, orphan))
	require.NoError(t, vb.Flush())

	// Inject the stale-low ObjectNum a re-Open would load from config.json, then
	// recover. Without the reconcile, replaying block 400 reuses chunk ID 0 and
	// overwrites the chunk still mapped for block 100 — a durable corruption.
	vb.ObjectNum.Store(0)
	require.NoError(t, vb.RecoverLocalWALs())

	assert.Greater(t, vb.ObjectNum.Load(), maxObjectID,
		"RecoverLocalWALs must reconcile ObjectNum past every mapped chunk before allocating")

	for blk, data := range want {
		got, err := vb.ReadAt(blk*uint64(vb.BlockSize), uint64(vb.BlockSize))
		require.NoError(t, err, "read block %d", blk)
		assert.Equal(t, data, got, "block %d corrupted after WAL recovery", blk)
	}
}

// createCloneVB creates a new VB instance configured as a clone of the given
// snapshot. It creates its own backend so that writes target the clone's
// namespace, while ReadFrom can still access the source volume's chunks.
func createCloneVB(t *testing.T, source *VB, snapshotID string) *VB {
	t.Helper()

	cloneName := fmt.Sprintf("clone-%s-%s", source.VolumeName, snapshotID)

	// Create a separate backend for the clone with its own volume name
	// but pointing to the same storage location (BaseDir/Bucket)
	var cloneBackendConfig any
	btype := source.Backend.GetBackendType()

	switch btype {
	case "file":
		// Derive the backend root from the source rather than naming it, so the
		// clone follows its source wherever that is rooted. setupTestVB roots
		// the VB at "{tmpDir}/viperblock" and the file backend at tmpDir, so
		// the source's parent is the storage root both must share for ReadFrom
		// and OpenFromSnapshot to resolve the source's chunks.
		cloneBackendConfig = file.FileConfig{
			VolumeName: cloneName,
			VolumeSize: source.VolumeSize,
			BaseDir:    filepath.Dir(source.BaseDir),
		}
	case "s3":
		// Reconstruct S3 config with the clone's volume name
		cloneBackendConfig = s3.S3Config{
			VolumeName: cloneName,
			VolumeSize: source.VolumeSize,
			Region:     "ap-southeast-2",
			Bucket:     "predastore",
			AccessKey:  AccessKey,
			SecretKey:  SecretKey,
			Host:       source.Backend.GetHost(),
			HTTPClient: testHTTPClient,
		}
	}

	vbconfig := VB{
		VolumeName:      cloneName,
		VolumeSize:      source.VolumeSize,
		BaseDir:         source.BaseDir,
		WALSyncInterval: -1, // Disable syncer in tests
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}

	clone, err := New(&vbconfig, btype, cloneBackendConfig)
	require.NoError(t, err)

	// Registered before the setup below can call FailNow, which would otherwise
	// skip cleanup and leave the clone's VB tree behind.
	t.Cleanup(func() {
		assert.NoError(t, clone.RemoveLocalFiles())
	})

	err = clone.Backend.Init()
	require.NoError(t, err)

	// Open WAL files for the clone
	if clone.UseShardedWAL {
		err = clone.OpenShardedWAL()
	} else {
		err = clone.OpenWAL(&clone.WAL, fmt.Sprintf("%s/%s", clone.WAL.BaseDir, fmt.Sprintf("%s/wal/chunks/wal.%08d.bin", cloneName, 0)))
	}
	require.NoError(t, err)

	err = clone.OpenWAL(&clone.BlockToObjectWAL, fmt.Sprintf("%s/%s", clone.BlockToObjectWAL.BaseDir, fmt.Sprintf("%s/wal/blocks/blocks.%08d.bin", cloneName, 0)))
	require.NoError(t, err)

	// Load snapshot base map
	err = clone.OpenFromSnapshot(snapshotID)
	require.NoError(t, err)

	return clone
}

// TestSnapshotCOWChain verifies that a snapshot taken from a COW clone carries
// the parent chain reference and that a second-generation clone can read blocks
// from all three layers: own delta, parent delta, and grandparent (base image).
func TestSnapshotCOWChain(t *testing.T) {
	runWithBackends(t, "snapshot_cow_chain", func(t *testing.T, base *VB) {
		blockSize := uint64(base.BlockSize)

		// Layer 0 (base image): write known data to blocks 0–1 only; block 2
		// is intentionally left unwritten to test the zero-read path.
		baseData := make([]byte, blockSize*2)
		rand.Read(baseData)
		require.NoError(t, base.Write(0, baseData))
		require.NoError(t, base.Flush())
		require.NoError(t, base.WriteWALToChunk(true))

		baseSnapID := fmt.Sprintf("snap-%s-base", base.VolumeName)
		_, err := base.CreateSnapshot(baseSnapID)
		require.NoError(t, err)

		// Layer 1 (instance A clone): only write to block 1
		cloneA := createCloneVB(t, base, baseSnapID)
		cloneAData := make([]byte, blockSize)
		rand.Read(cloneAData)
		require.NoError(t, cloneA.Write(1, cloneAData))
		require.NoError(t, cloneA.Flush())
		require.NoError(t, cloneA.WriteWALToChunk(true))

		// Snapshot the clone — flat snapshot: HasFlatSection=true, all ancestor
		// blocks embedded in the checkpoint with no chain to walk.
		cloneASnapID := fmt.Sprintf("snap-%s-clone", cloneA.VolumeName)
		snap, err := cloneA.CreateSnapshot(cloneASnapID)
		require.NoError(t, err)
		assert.True(t, snap.HasFlatSection, "snapshot of COW clone must embed inherited blocks (flat section)")

		// Layer 2 (instance B): clone from the clone's snapshot; ancestors populated from flat section
		cloneB := createCloneVB(t, cloneA, cloneASnapID)

		// Block 0: never written by cloneA — must come from grandparent (base image)
		got, err := cloneB.ReadAt(0, blockSize)
		require.NoError(t, err)
		assert.Equal(t, baseData[:blockSize], got, "block 0 must be read from grandparent")

		// Block 1: written by cloneA — must come from parent delta
		got, err = cloneB.ReadAt(blockSize, blockSize)
		require.NoError(t, err)
		assert.Equal(t, cloneAData, got, "block 1 must be read from parent delta")

		// Block 2: never written by anyone — ReadAt returns ErrZeroBlock and zeros
		got, err = cloneB.ReadAt(2*blockSize, blockSize)
		assert.ErrorIs(t, err, ErrZeroBlock, "unwritten block must return ErrZeroBlock")
		assert.Equal(t, make([]byte, blockSize), got, "unwritten block must read as zeros")
	})
}
