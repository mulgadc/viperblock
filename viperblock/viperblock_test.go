package viperblock

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/assert"
)

// TestVB is a helper struct to hold test cases
type TestVB struct {
	name      string
	config    interface{}
	blockSize uint32
}

// setupTestVB creates a new VB instance for testing
func setupTestVB(t *testing.T, testCase TestVB) *VB {
	// Create a temporary directory for test data
	//tmpDir, err := os.MkdirTemp("tmp/", "")
	//assert.NoError(t, err)

	// Get the last directory pathname
	//testVol := filepath.Base(tmpDir)
	tmpDir := os.TempDir()
	testVol := fmt.Sprintf("test_volume_%d", time.Now().UnixNano())

	//tmpDir := "tmp/"

	t.Cleanup(func() {
		os.RemoveAll(fmt.Sprintf("%s/%s", tmpDir, testVol))
	})

	// Create test configuration
	config := file.FileConfig{
		BaseDir:    tmpDir,
		VolumeName: testVol,
	}

	vb := New("file", config)
	vb.WAL.BaseDir = tmpDir
	assert.NotNil(t, vb)

	err := vb.OpenWAL(fmt.Sprintf("%s/%s/wal.%08d.bin", tmpDir, vb.Backend.GetVolume(), vb.WAL.WallNum.Load()))

	assert.NoError(t, err)

	return vb
}

func TestNew(t *testing.T) {
	testCases := []TestVB{
		{
			name:      "file",
			config:    file.FileConfig{BaseDir: "test_data"},
			blockSize: 4 * 1024,
		},

		{
			name: "s3",
			config: s3.S3Config{
				VolumeName: "test_s3",
				Region:     "ap-southeast-2",
				Bucket:     "test_bucket",
				AccessKey:  "test_access_key",
				SecretKey:  "test_secret_key",
				Host:       "https://localhost:8443/",
			},
			blockSize: 4 * 1024,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vb := New(tc.name, tc.config)
			assert.NotNil(t, vb)
			assert.Equal(t, tc.blockSize, vb.BlockSize)
			assert.Equal(t, uint32(128*1024), vb.ObjBlockSize)
			assert.Equal(t, 5*time.Second, vb.FlushInterval)
			assert.Equal(t, uint32(64*1024*1024), vb.FlushSize)

			if tc.name == "s3" {
				assert.IsType(t, &s3.Backend{}, vb.Backend)
			} else {
				assert.IsType(t, &file.Backend{}, vb.Backend)
			}
		})
	}
}

func TestWriteAndRead(t *testing.T) {
	testCases := []struct {
		name      string
		blockID   uint64
		data      []byte
		expectErr bool
	}{
		{
			name:      "write valid block",
			blockID:   0,
			data:      []byte("test data"),
			expectErr: false,
		},
		{
			name:      "write empty data",
			blockID:   1,
			data:      []byte{},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vb := setupTestVB(t, TestVB{name: tc.name})

			// Test Write
			err := vb.Write(tc.blockID, tc.data)
			if tc.expectErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// Test Read
			readData, err := vb.Read(tc.blockID)
			assert.NoError(t, err)
			assert.Equal(t, tc.data, readData)

			// Next test a WAL write and read
			/*
				err = vb.WriteWAL(Block{SeqNum: 1, Block: 1, Data: tc.data})
				assert.NoError(t, err)

				readData, err = vb.ReadWAL()
				assert.NoError(t, err)
				assert.Equal(t, tc.data, readData)
			*/
		})
	}
}

func TestWALOperations(t *testing.T) {
	vb := setupTestVB(t, TestVB{name: "wal_operations"})

	// Test WAL file operations
	t.Run("WAL file operations", func(t *testing.T) {
		walFile := filepath.Join(vb.WAL.BaseDir, "test.wal")

		// Test OpenWAL
		err := vb.OpenWAL(walFile)
		assert.NoError(t, err)

		// Test WriteWAL
		block := Block{
			SeqNum: 1,
			Block:  1,
			Data:   []byte("test data"),
		}
		err = vb.WriteWAL(block)
		assert.NoError(t, err)

		// Test ReadWAL
		err = vb.ReadWAL()
		assert.NoError(t, err)
	})
}

func TestStateOperations(t *testing.T) {
	vb := setupTestVB(t, TestVB{name: "state_operations"})

	t.Run("Save and Load State", func(t *testing.T) {
		stateFile := filepath.Join(vb.WAL.BaseDir, "state.json")

		// Test SaveState
		err := vb.SaveState(stateFile)
		assert.NoError(t, err)

		// Test LoadState
		err = vb.LoadState(stateFile)
		assert.NoError(t, err)
	})
}

func TestBlockLookup(t *testing.T) {
	vb := setupTestVB(t, TestVB{name: "block_lookup"})

	t.Run("Block Lookup Operations", func(t *testing.T) {

		// Write a test block
		blockID := uint64(0)
		data := make([]byte, 4096)
		msg := "test data"
		copy(data[:len(msg)], msg)

		err := vb.Write(blockID, data)
		assert.NoError(t, err)

		// Next block
		blockID = uint64(1)
		data = make([]byte, 4096)
		msg = "hello world"
		copy(data[:len(msg)], msg)

		err = vb.Write(blockID, data)
		assert.NoError(t, err)

		// Next block
		blockID = uint64(2)
		data = make([]byte, 4096)
		msg = "say hi in japanese"
		copy(data[:len(msg)], msg)

		err = vb.Write(blockID, data)
		assert.NoError(t, err)

		// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
		// Test LookupBlockToObject

		for _, block := range []uint64{0, 1, 2} {
			objectID, objectOffset, err := vb.LookupBlockToObject(block)
			assert.Error(t, err)
			assert.Equal(t, uint64(0), objectID)
			assert.Equal(t, uint32(0), objectOffset)
		}

		vb.Flush()

		vb.WriteWALToChunk()

		// Test LookupBlockToObject
		for _, block := range []uint64{0, 1, 2} {
			objectID, objectOffset, err := vb.LookupBlockToObject(block)
			assert.NoError(t, err)
			assert.Equal(t, objectID, uint64(0))
			assert.NotZero(t, objectOffset)
		}

		// Next, write a new block and flush
		blockID = uint64(3)
		data = make([]byte, 4096)
		msg = "new block"
		copy(data[:len(msg)], msg)

		err = vb.Write(blockID, data)

		// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
		objectID, objectOffset, err := vb.LookupBlockToObject(3)
		assert.Error(t, err)
		assert.Equal(t, uint64(0), objectID)
		assert.Equal(t, uint32(0), objectOffset)

		vb.Flush()

		vb.WriteWALToChunk()

		// Lookup the block in the WAL, should be the 2nd chunk
		objectID, objectOffset, err = vb.LookupBlockToObject(3)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), objectID)
		assert.NotZero(t, objectOffset)

	})
}

func TestFlushOperations(t *testing.T) {
	vb := setupTestVB(t, TestVB{name: "flush_operations"})

	t.Run("Flush Operations", func(t *testing.T) {
		// Write multiple blocks
		for i := uint64(1); i <= 5; i++ {
			buffer := make([]byte, 4096)
			msg := fmt.Sprintf("test data %d", i)
			copy(buffer[:len(msg)], msg)
			err := vb.Write(i, buffer)
			assert.NoError(t, err)
		}

		// Test Flush
		err := vb.Flush()
		assert.NoError(t, err)

		// Test WriteWALToChunk
		err = vb.WriteWALToChunk()
		assert.NoError(t, err)
	})
}

func TestCacheConfiguration(t *testing.T) {
	vb := setupTestVB(t, TestVB{name: "cache_config"})

	t.Run("Set Cache Size", func(t *testing.T) {
		// Test setting cache size
		err := vb.SetCacheSize(1000, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1000, vb.Cache.config.Size)
		assert.False(t, vb.Cache.config.UseSystemMemory)
		assert.Equal(t, 0, vb.Cache.config.SystemMemoryPercent)

		// Test invalid cache size
		err = vb.SetCacheSize(0, 0)
		assert.Error(t, err)
		err = vb.SetCacheSize(-1, 0)
		assert.Error(t, err)
	})

	t.Run("Set Cache System Memory", func(t *testing.T) {
		// Test setting cache size based on system memory
		err := vb.SetCacheSystemMemory(50)
		assert.NoError(t, err)
		assert.True(t, vb.Cache.config.UseSystemMemory)
		assert.Equal(t, 50, vb.Cache.config.SystemMemoryPercent)
		assert.Greater(t, vb.Cache.config.Size, 0)

		// Test invalid percentages
		err = vb.SetCacheSystemMemory(0)
		assert.Error(t, err)
		err = vb.SetCacheSystemMemory(101)
		assert.Error(t, err)
	})

	t.Run("Cache Operations", func(t *testing.T) {
		// Set a small cache size for testing
		err := vb.SetCacheSize(5, 0)
		assert.NoError(t, err)

		// Write some blocks
		for i := uint64(0); i < 10; i++ {
			err := vb.Write(i, []byte(fmt.Sprintf("test data %d", i)))
			assert.NoError(t, err)
		}

		// Verify only the last 5 blocks are in cache
		for i := uint64(0); i < 10; i++ {
			data, err := vb.Read(i)
			if i < 5 {
				// First 5 blocks should be evicted
				assert.Error(t, err)
			} else {
				// Last 5 blocks should be in cache
				assert.NoError(t, err)
				assert.Equal(t, []byte(fmt.Sprintf("test data %d", i)), data)
			}
		}
	})
}
