package simplefs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/viperblock/viperblock"
	"github.com/stretchr/testify/assert"
)

// TestSimpleFS is a helper struct to hold test cases
type TestSimpleFS struct {
	name       string
	volumeSize uint64
}

// setupTestSimpleFS creates a new SimpleFS instance for testing
func setupTestSimpleFS(t *testing.T, testCase TestSimpleFS) *SimpleFS {
	// Create a temporary directory for test data
	tmpDir, err := os.MkdirTemp("", "simplefs_test_*")
	assert.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	sfs := New()
	assert.NotNil(t, sfs)
	return sfs
}

func TestNew(t *testing.T) {
	sfs := New()
	assert.NotNil(t, sfs)
	//assert.Equal(t, uint64(len(sfs.Blocks.Blocks)), sfs.Blocksize)
	assert.NotNil(t, sfs.Blocks)
}

func TestCreateVolume(t *testing.T) {
	testCases := []TestSimpleFS{
		{
			name:       "create valid volume",
			volumeSize: uint64(viperblock.DefaultBlockSize) * 10, // 10 blocks
		},
		{
			name:       "create volume with invalid size",
			volumeSize: uint64(viperblock.DefaultBlockSize)*10 + 1, // Not a multiple of blocksize
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sfs := setupTestSimpleFS(t, tc)

			err := sfs.CreateVolume("test_volume", tc.volumeSize)
			if tc.volumeSize%uint64(viperblock.DefaultBlockSize) != 0 {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, "test_volume", sfs.Volume.Name)
			assert.Equal(t, tc.volumeSize/uint64(viperblock.DefaultBlockSize), uint64(len(sfs.Volume.Free)))
		})
	}
}

func TestFormatVolume(t *testing.T) {
	sfs := setupTestSimpleFS(t, TestSimpleFS{name: "test_volume"})

	// First create a volume
	err := sfs.CreateVolume("test_volume", uint64(viperblock.DefaultBlockSize)*10)
	assert.NoError(t, err)

	// Allocate some blocks
	blocks, err := sfs.AllocateBlocks(5)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(blocks))

	// Test FormatVolume
	err = sfs.FormatVolume()
	assert.NoError(t, err)

	// Verify all blocks are free
	assert.Equal(t, 10, len(sfs.Volume.Free))
	assert.Equal(t, 0, len(sfs.Volume.Used))
}

func TestAllocateAndFreeBlocks(t *testing.T) {
	sfs := setupTestSimpleFS(t, TestSimpleFS{name: "allocate_free_blocks"})

	// Create a volume
	err := sfs.CreateVolume("test_volume", uint64(viperblock.DefaultBlockSize)*10)
	assert.NoError(t, err)

	t.Run("Allocate Blocks", func(t *testing.T) {
		// Test allocating blocks
		blocks, err := sfs.AllocateBlocks(5)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(blocks))

		// Verify blocks are marked as used
		for _, block := range blocks {
			assert.True(t, sfs.Volume.Used[block])
		}

		// Test allocating more blocks than available
		_, err = sfs.AllocateBlocks(10)
		assert.Error(t, err)
	})

	t.Run("Free Blocks", func(t *testing.T) {
		// Allocate some blocks first
		blocks, err := sfs.AllocateBlocks(3)
		assert.NoError(t, err)

		// Test freeing blocks
		err = sfs.FreeBlocks(blocks)
		assert.NoError(t, err)

		// Verify blocks are marked as free
		for _, block := range blocks {
			assert.False(t, sfs.Volume.Used[block])
		}
	})
}

func TestFileOperations(t *testing.T) {
	sfs := setupTestSimpleFS(t, TestSimpleFS{name: "file_operations"})

	// Create a volume
	err := sfs.CreateVolume("test_volume", uint64(viperblock.DefaultBlockSize)*10)
	assert.NoError(t, err)

	t.Run("Create File", func(t *testing.T) {
		// Test creating a file
		blocks, err := sfs.CreateFile("test.txt", uint64(viperblock.DefaultBlockSize)*2) // 2 blocks
		assert.NoError(t, err)
		assert.Equal(t, 2, len(blocks))

		// Verify file exists in blocks map
		block, exists := sfs.Blocks["test.txt"]
		assert.True(t, exists)
		assert.Equal(t, "test.txt", block.Filename)
		assert.Equal(t, uint64(viperblock.DefaultBlockSize)*2, block.Size)
		assert.Equal(t, 2, len(block.Blocks))
	})

	t.Run("Delete File", func(t *testing.T) {
		// Create a file first
		_, err := sfs.CreateFile("test.txt", uint64(viperblock.DefaultBlockSize))
		assert.NoError(t, err)

		// Test deleting the file
		err = sfs.DeleteFile("test.txt")
		assert.NoError(t, err)

		// Verify file is removed
		_, exists := sfs.Blocks["test.txt"]
		assert.False(t, exists)

		// Test deleting non-existent file
		err = sfs.DeleteFile("nonexistent.txt")
		assert.Error(t, err)
	})
}

func TestStateOperations(t *testing.T) {
	sfs := setupTestSimpleFS(t, TestSimpleFS{name: "state_operations"})

	// Create a volume and some files
	err := sfs.CreateVolume("test_volume", uint64(viperblock.DefaultBlockSize)*10)
	assert.NoError(t, err)
	_, err = sfs.CreateFile("test.txt", uint64(viperblock.DefaultBlockSize))
	assert.NoError(t, err)

	t.Run("Save and Load State", func(t *testing.T) {
		// Create temporary state file
		tmpDir, err := os.MkdirTemp("", "simplefs_state_*")
		assert.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		stateFile := filepath.Join(tmpDir, "state.json")

		// Test SaveState
		err = sfs.SaveState(stateFile)
		assert.NoError(t, err)

		// Create new instance and test LoadState
		newSFS := New()
		err = newSFS.LoadState(stateFile)
		assert.NoError(t, err)

		// Verify state was loaded correctly
		assert.Equal(t, sfs.Volume.Name, newSFS.Volume.Name)
		assert.Equal(t, len(sfs.Blocks), len(newSFS.Blocks))
	})
}
