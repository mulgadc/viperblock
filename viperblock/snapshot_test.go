package viperblock

import (
	"crypto/rand"
	"fmt"
	"os"
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
		baseMap, sourceVol, err := vb.LoadSnapshotBlockMap(snapshotID)
		require.NoError(t, err)
		assert.Equal(t, vb.VolumeName, sourceVol)
		assert.Equal(t, len(vb.BlocksToObject.BlockLookup), len(baseMap.BlockLookup))

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
		for i := uint64(0); i < blockCount; i++ {
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
		// Use the same base dir as setupTestVB uses for the file backend.
		// The VB.BaseDir is "{tmpDir}/viperblock" while the file backend's
		// BaseDir is just tmpDir. Both must share the same storage root.
		cloneBackendConfig = file.FileConfig{
			VolumeName: cloneName,
			VolumeSize: source.VolumeSize,
			BaseDir:    os.TempDir(),
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

	err = clone.Backend.Init()
	require.NoError(t, err)

	// Open WAL files for the clone
	err = clone.OpenWAL(&clone.WAL, fmt.Sprintf("%s/%s", clone.WAL.BaseDir, fmt.Sprintf("%s/wal/chunks/wal.%08d.bin", cloneName, 0)))
	require.NoError(t, err)

	err = clone.OpenWAL(&clone.BlockToObjectWAL, fmt.Sprintf("%s/%s", clone.BlockToObjectWAL.BaseDir, fmt.Sprintf("%s/wal/blocks/blocks.%08d.bin", cloneName, 0)))
	require.NoError(t, err)

	// Load snapshot base map
	err = clone.OpenFromSnapshot(snapshotID)
	require.NoError(t, err)

	t.Cleanup(func() {
		clone.RemoveLocalFiles()
	})

	return clone
}

// createCloneDirs creates the local directory structure needed for a clone volume.
func createCloneDirs(vb *VB) error {
	dirs := []string{
		fmt.Sprintf("%s/%s", vb.BaseDir, vb.VolumeName),
		fmt.Sprintf("%s/%s/checkpoints", vb.BaseDir, vb.VolumeName),
		fmt.Sprintf("%s/%s/wal/chunks", vb.BaseDir, vb.VolumeName),
		fmt.Sprintf("%s/%s/wal/blocks", vb.BaseDir, vb.VolumeName),
	}
	for _, dir := range dirs {
		if err := mkdirAll(dir); err != nil {
			return err
		}
	}
	return nil
}

func mkdirAll(path string) error {
	return os.MkdirAll(path, 0750)
}
