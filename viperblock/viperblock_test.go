package viperblock

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	predastore "github.com/mulgadc/predastore/s3"
)

// TestVB is a helper struct to hold test cases
type TestVB struct {
	name      string
	config    interface{}
	blockSize uint32
}

// BackendType represents the type of backend to use in tests
type BackendTest struct {
	Name        string
	BackendType string
	Config      interface{}
	CacheConfig CacheConfig
}

const (
	FileBackend string = "file"
	S3Backend   string = "s3"

	AccessKey string = "AKIAIOSFODNN7EXAMPLE"
	SecretKey string = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	volumeSize uint64 = 8 * 1024 * 1024
)

//var s3server predastore.Config

// startTestServer starts the Fiber app and returns:
// - the base URL (e.g., "http://127.0.0.1:3000")
// - a shutdown function to cleanly stop the server
// - an error if startup failed
func startTestServer(t *testing.T, host string) (shutdown func(volName string), err error) {

	// Create and configure the S3 server
	s3server := predastore.New()

	// Get the current directory
	dir, err := os.Getwd()
	require.NoError(t, err, "Failed to get current directory")

	// Go down one directory from the current directory
	dir = filepath.Join(dir, "..")

	err = s3server.ReadConfig(filepath.Join(dir, "tests/config/server.toml"), dir)
	require.NoError(t, err, "Failed to read config file")

	// Setup routes
	app := s3server.SetupRoutes()

	// Channel to track when server is running
	started := make(chan struct{})

	require.NoError(t, err, "Failed to get free port")

	go func() {
		close(started) // notify that listener is active

		// Start the Fiber app directly with ListenTLS
		if err := app.ListenTLS(fmt.Sprintf("%s", host), "../config/server.pem", "../config/server.key"); err != nil {
			// Only log, don't panic, to avoid crashing test
			assert.NoError(t, err)

			t.Logf("fiber server exited: %v\n", err)

		}

	}()

	// Wait for listener to bind
	<-started

	shutdown = func(volName string) {
		_ = app.Shutdown()

		// Remove the test volume
		tmpVolume := fmt.Sprintf("%s/%s", s3server.Buckets[0].Pathname, volName)

		t.Log(fmt.Sprintf("Removing %s", tmpVolume))
		os.RemoveAll(tmpVolume)

	}

	return shutdown, nil
}

// setupTestVB creates a new VB instance for testing with the specified backend
func setupTestVB(t *testing.T, testCase TestVB, backendType BackendTest) (vb *VB, baseURL string, shutdown func(volName string), err error) {

	// Create a temporary directory for test data
	//tmpDir, err := os.MkdirTemp("", "viperblock_test_*")
	tmpDir := os.TempDir()
	testVol := fmt.Sprintf("test_volume_%d", time.Now().UnixNano())

	t.Cleanup(func() {
		if vb != nil {
			t.Log("Removing VB WAL files: ", vb.WAL.BaseDir, testVol)
			os.RemoveAll(fmt.Sprintf("%s/%s", tmpDir, testVol))
		}

	})

	var config interface{}
	switch backendType.BackendType {

	case FileBackend:
		config = file.FileConfig{
			BaseDir:    tmpDir,
			VolumeName: testVol,
			VolumeSize: volumeSize,
		}

		shutdown = func(volName string) {
			t.Logf("Shutdown for file handler %s", volName)
		}

	case S3Backend:

		t.Log("S3 backend not found, setting up S3 server")

		host, err := getFreePort()
		assert.NoError(t, err)

		config = s3.S3Config{
			VolumeName: testVol,
			VolumeSize: volumeSize,
			Region:     "ap-southeast-2",
			Bucket:     "predastore",
			AccessKey:  AccessKey,
			SecretKey:  SecretKey,
			Host:       fmt.Sprintf("https://%s", host),
		}

		shutdown, err = startTestServer(t, host)
		if err != nil {
			t.Fatalf("failed to start server: %v", err)
		}

		// Wait until server is responsive
		ok := waitForServer(config.(s3.S3Config).Host, 2*time.Second)
		if !ok {
			t.Fatalf("server did not respond in time")
		}

		assert.NoError(t, err)

	default:
		t.Fatalf("unsupported backend type: %s", backendType.BackendType)
	}

	// Create a new Viperblock
	vb = New(backendType.BackendType, config)
	assert.NotNil(t, vb)

	err = vb.SetCacheSize(backendType.CacheConfig.Size, backendType.CacheConfig.SystemMemoryPercent)
	assert.NoError(t, err)

	vb.WAL.BaseDir = tmpDir

	err = vb.OpenWAL(fmt.Sprintf("%s/%s/wal.%08d.bin", tmpDir, vb.Backend.GetVolume(), vb.WAL.WallNum.Load()))

	assert.NoError(t, err)

	err = vb.Backend.Init()

	assert.NoError(t, err)

	return vb, baseURL, shutdown, nil
}

// runWithBackends runs a test function with both file and S3 backends
func runWithBackends(t *testing.T, testName string, testFunc func(t *testing.T, vb *VB)) {
	backends := []BackendTest{
		{
			Name:        "file",
			BackendType: FileBackend,
			Config:      file.FileConfig{BaseDir: "test_data"},
			CacheConfig: CacheConfig{Size: 1024 * 1024 * 1024, SystemMemoryPercent: 0},
		},

		{
			Name:        "file_nocache",
			BackendType: FileBackend,
			Config:      file.FileConfig{BaseDir: "test_data"},
			CacheConfig: CacheConfig{Size: 0, SystemMemoryPercent: 0},
		},

		{
			Name:        "s3",
			BackendType: S3Backend,
			Config: s3.S3Config{
				VolumeName: "test_s3",
				VolumeSize: volumeSize,
				Region:     "ap-southeast-2",
				Bucket:     "predastore",
				AccessKey:  AccessKey,
				SecretKey:  SecretKey,
				Host:       "https://127.0.0.1:8443/",
			},
			CacheConfig: CacheConfig{Size: 1024 * 1024 * 1024, SystemMemoryPercent: 0},
		},

		{
			Name:        "s3_nocache",
			BackendType: S3Backend,
			Config: s3.S3Config{
				VolumeName: "test_s3",
				VolumeSize: volumeSize,
				Region:     "ap-southeast-2",
				Bucket:     "predastore",
				AccessKey:  AccessKey,
				SecretKey:  SecretKey,
				Host:       "https://127.0.0.1:8443/",
			},
			CacheConfig: CacheConfig{Size: 0, SystemMemoryPercent: 0},
		},
	}

	for _, backendType := range backends {
		t.Run(fmt.Sprintf("%s_%s", testName, backendType.Name), func(t *testing.T) {
			t.Log("Running test ", backendType.Name, " with backend:", backendType.BackendType)

			vb, baseURL, shutdown, err := setupTestVB(t, TestVB{name: testName}, backendType)
			if err != nil {
				t.Fatalf("failed to setup test VB: %v baseURL: %s", err, baseURL)
			}

			testFunc(t, vb)

			defer shutdown(vb.Backend.GetVolume())

		})
	}
}

// setupTestVB creates a new VB instance for testing
/*
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
*/

func TestNew(t *testing.T) {
	testCases := []TestVB{
		{
			name:      "file",
			config:    file.FileConfig{BaseDir: "test_data"},
			blockSize: DefaultBlockSize,
		},

		{
			name: "s3",
			config: s3.S3Config{
				VolumeName: "test_s3",
				VolumeSize: volumeSize,
				Region:     "ap-southeast-2",
				Bucket:     "test_bucket",
				AccessKey:  AccessKey,
				SecretKey:  SecretKey,
				Host:       "https://127.0.0.1:8443/",
			},
			blockSize: DefaultBlockSize,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vb := New(tc.name, tc.config)
			assert.NotNil(t, vb)
			assert.Equal(t, tc.blockSize, vb.BlockSize)
			assert.Equal(t, uint32(1024*1024*4), vb.ObjBlockSize)
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

	// Test data block
	dataSingleBlock := make([]byte, DefaultBlockSize)
	msg := "test data"
	copy(dataSingleBlock[:len(msg)], msg)

	dataDoubleBlock := make([]byte, DefaultBlockSize*2)
	msg2 := "hello first block"
	copy(dataDoubleBlock[:len(msg2)], msg2)
	msg3 := "hello second block"
	copy(dataDoubleBlock[DefaultBlockSize:], msg3)

	dataTenBlock := make([]byte, DefaultBlockSize*10)
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("test data %d", i)
		copy(dataTenBlock[i*int(DefaultBlockSize):(i+1)*int(DefaultBlockSize)], msg)
	}

	// Generate a random number between 20 and 50 with random data
	randomDataBlockSize, _ := rand.Int(rand.Reader, big.NewInt(30))
	randomDataBlock := make([]byte, int(DefaultBlockSize)*int(randomDataBlockSize.Int64()))
	for i := 0; i < int(randomDataBlockSize.Int64()); i++ {
		rand.Read(randomDataBlock[i*int(DefaultBlockSize) : (i+1)*int(DefaultBlockSize)])
	}

	testCases := []struct {
		name        string
		blockID     uint64
		data        []byte
		expectErr   bool
		endOfVolume bool
	}{
		{
			name:        "write single valid block",
			blockID:     0,
			data:        dataSingleBlock,
			expectErr:   false,
			endOfVolume: false,
		},

		{
			name:        "write single valid block end of volume",
			blockID:     0,
			data:        dataSingleBlock,
			expectErr:   false,
			endOfVolume: true,
		},

		{
			name:        "write empty data",
			blockID:     1,
			data:        make([]byte, DefaultBlockSize),
			expectErr:   false,
			endOfVolume: false,
		},

		{
			name:        "write 10x empty data",
			blockID:     40,
			data:        make([]byte, DefaultBlockSize*10),
			expectErr:   false,
			endOfVolume: false,
		},
		// Write double block

		{
			name:        "write double valid block",
			blockID:     2,
			data:        dataDoubleBlock,
			expectErr:   false,
			endOfVolume: false,
		},

		{
			name:        "write 10 valid blocks",
			blockID:     10,
			data:        dataTenBlock,
			expectErr:   false,
			endOfVolume: false,
		},

		{

			name:        "write various random valid blocks",
			blockID:     200,
			data:        randomDataBlock,
			expectErr:   false,
			endOfVolume: false,
		},

		{

			name:        "write various random valid blocks",
			blockID:     400,
			data:        randomDataBlock,
			expectErr:   false,
			endOfVolume: true,
		},
	}

	runWithBackends(t, "write_and_read", func(t *testing.T, vb *VB) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {

				if tc.endOfVolume {
					// Change the block size to write at the end of the volume
					volumeEndBlock := vb.Backend.GetVolumeSize()/uint64(vb.BlockSize) - 1
					t.Log("volumeEndBlock", volumeEndBlock)
					tc.blockID = volumeEndBlock - uint64(len(tc.data)/int(vb.BlockSize))
					t.Log("verfiy", tc.blockID*uint64(vb.BlockSize), ">", vb.Backend.GetVolumeSize())
					t.Log("New BlockID", tc.blockID)
				}

				// Check for errors on invalid reads first
				// Test Read error for invalid block, beyond the volume size

				readData, err := vb.ReadAt(vb.Backend.GetVolumeSize()+(2*uint64(vb.BlockSize)), uint64(vb.BlockSize))
				assert.ErrorIs(t, err, RequestTooLarge)
				assert.Nil(t, readData)

				// Test Read error for invalid block, beyond the volume size
				readData, err = vb.ReadAt(vb.Backend.GetVolumeSize()-(1*uint64(vb.BlockSize)), uint64(vb.BlockSize*2))
				assert.ErrorIs(t, err, RequestOutOfRange)
				assert.Nil(t, readData)

				// Test Read for a block that exists, but null (unallocated) data
				readData, err = vb.ReadAt((vb.Backend.GetVolumeSize())-(1*uint64(vb.BlockSize)), uint64(vb.BlockSize))
				assert.ErrorIs(t, err, ZeroBlock)
				assert.Equal(t, make([]byte, vb.BlockSize), readData)

				// Test invalid writes
				err = vb.WriteAt(vb.Backend.GetVolumeSize()+10, tc.data)
				assert.ErrorIs(t, err, RequestTooLarge)

				err = vb.WriteAt(0, make([]byte, 0))
				assert.ErrorIs(t, err, RequestBufferEmpty)

				err = vb.WriteAt(vb.Backend.GetVolumeSize(), tc.data)
				assert.ErrorIs(t, err, RequestOutOfRange)

				// NO FLUSH TEST
				// Test Write (no flush)
				err = vb.WriteAt(tc.blockID*uint64(vb.BlockSize), tc.data)
				assert.NoError(t, err)

				// Test Read
				readData, err = vb.ReadAt(tc.blockID*uint64(vb.BlockSize), uint64(len(tc.data)))
				assert.NoError(t, err)
				assert.Equal(t, tc.data, readData)

				// Test Read
				readData, err = vb.ReadAt(tc.blockID*uint64(vb.BlockSize), uint64(len(tc.data)))
				assert.NoError(t, err)
				assert.Equal(t, tc.data, readData)

				// Test Read from 1024 bytes in the block
				readData, err = vb.ReadAt((tc.blockID*uint64(vb.BlockSize))+(uint64(vb.BlockSize)/4), uint64(len(tc.data)-(int(vb.BlockSize)/4)))
				assert.NoError(t, err)
				assert.Equal(t, tc.data[(uint64(vb.BlockSize)/4):], readData)

				// Test Read for last 1024 bytes in the block
				readData, err = vb.ReadAt((tc.blockID*uint64(vb.BlockSize))+(uint64(len(tc.data))-(uint64(vb.BlockSize)/4)), uint64((vb.BlockSize)/4))
				assert.NoError(t, err)
				assert.Equal(t, tc.data[uint64(len(tc.data))-(uint64(vb.BlockSize)/4):], readData)

				// FLUSH TEST, prior to backend write (inflight cache should hit)
				err = vb.Flush()
				assert.NoError(t, err)

				readData, err = vb.ReadAt(tc.blockID*uint64(vb.BlockSize), uint64(len(tc.data)))
				assert.NoError(t, err)
				assert.Equal(t, tc.data, readData)

				// Confirm inflight cache has the correct number of blocks
				expectedInflight := len(tc.data) / int(vb.BlockSize)
				assert.Equal(t, expectedInflight, len(vb.PendingBackendWrites.Blocks))

				// Write a new request while inflight, should be stored in our HOT cache
				inflightWrite := make([]byte, DefaultBlockSize)
				msg := "inflight HOT write"
				copy(inflightWrite[:len(msg)], msg)

				var inflightBlock uint64 = 0

				if tc.endOfVolume {
					inflightBlock = tc.blockID - 200
				} else {
					inflightBlock = tc.blockID + 200
				}

				err = vb.WriteAt(inflightBlock*uint64(vb.BlockSize), inflightWrite)
				assert.NoError(t, err)

				// Next, upload chunk to the backend
				err = vb.WriteWALToChunk(true)
				assert.NoError(t, err)

				// Confirm HOT cache has the one write after the flush
				assert.Equal(t, 1, len(vb.Writes.Blocks))

				// Flush again, so not to interfere with the next test
				err = vb.Flush()
				assert.NoError(t, err)

				// Confirm single inflight write exists
				assert.Equal(t, 1, len(vb.PendingBackendWrites.Blocks))

				// Next, upload chunk to the backend
				err = vb.WriteWALToChunk(true)
				assert.NoError(t, err)

				//spew.Dump(vb.Writes.Blocks)

				// Test read path again, should be removed from inflight cache
				readData, err = vb.ReadAt(tc.blockID*uint64(vb.BlockSize), uint64(len(tc.data)))
				assert.NoError(t, err)
				assert.Equal(t, tc.data, readData)

				// If cache enabled, check the LRU cache
				if vb.Cache.config.Size > 0 {

					var blockCount uint64 = 0

					// Loop through each 4096 (default) block, confirm in the cache
					for i := uint64(0); i < uint64(len(tc.data))/uint64(DefaultBlockSize); i++ {

						t.Log("Checking cache for block", tc.blockID+i)

						if cachedData, ok := vb.Cache.lru.Get(tc.blockID + i); ok {
							slog.Info("CACHE HIT:", "block", tc.blockID+i)

							assert.Equal(t, tc.data[blockCount:blockCount+uint64(vb.BlockSize)], cachedData)

						}

						blockCount += uint64(vb.BlockSize)

					}

				}

				err = vb.SetCacheSize(1, 0)
				assert.NoError(t, err)

				// Next, validate each read is successful for each block size
				var blockCount uint64 = 0

				// Loop through each 4096 block, confirm in the cache

				for i := uint64(0); i < uint64(len(tc.data))/uint64(vb.BlockSize); i++ {

					//readData, err = vb.Read(tc.blockID+i, uint64(vb.BlockSize))

					readData, err = vb.ReadAt((tc.blockID+i)*uint64(vb.BlockSize), uint64(vb.BlockSize))

					assert.NoError(t, err)

					assert.Equal(t, tc.data[blockCount:blockCount+uint64(vb.BlockSize)], readData)

					blockCount += uint64(vb.BlockSize)

				}

				// Test smaller blocksize write, e.g simulate GRUB writing 512 blocks for bootloader

				smallBlock := make([]byte, 512)
				rand.Read(smallBlock)

				copy(tc.data[:512], smallBlock)

				err = vb.WriteAt(tc.blockID*uint64(vb.BlockSize), tc.data[:512])
				assert.NoError(t, err)

				// Flush again, so not to interfere with the next test
				err = vb.Flush()
				assert.NoError(t, err)

				// Next, upload chunk to the backend
				err = vb.WriteWALToChunk(true)
				assert.NoError(t, err)

				// Test Read
				//readData, err = vb.ReadAt(tc.blockID*uint64(vb.BlockSize), uint64(len(tc.data)))
				//assert.NoError(t, err)
				//assert.Equal(t, tc.data[:512], readData)

			})
		}
	})
}

func TestWALOperations(t *testing.T) {

	runWithBackends(t, "write_and_read", func(t *testing.T, vb *VB) {

		// Test WAL file operations
		t.Run("WAL file operations", func(t *testing.T) {

			// Create a new WAL file with current timestamp
			walFile := filepath.Join(vb.WAL.BaseDir, fmt.Sprintf("test.%s.wal", time.Now().Format("20060102150405")))

			// Test OpenWAL
			err := vb.OpenWAL(walFile)
			assert.NoError(t, err)

			data := make([]byte, DefaultBlockSize)
			msg := "test data"
			copy(data[:len(msg)], msg)

			// Test WriteWAL
			block := Block{
				SeqNum: 1,
				Block:  1,
				Data:   data,
			}
			err = vb.WriteWAL(block)
			assert.NoError(t, err)

			// Test ReadWAL
			err = vb.ReadWAL()
			assert.NoError(t, err)

			// Delete the temp WAL file
			os.Remove(walFile)
		})

	})
}

func TestStateOperations(t *testing.T) {

	runWithBackends(t, "write_and_read", func(t *testing.T, vb *VB) {
		t.Run("Save and Load State", func(t *testing.T) {

			// Create with unique timestamp
			stateFile := filepath.Join(vb.WAL.BaseDir, fmt.Sprintf("state.%s.json", time.Now().Format("20060102150405")))

			// Test SaveState
			err := vb.SaveState(stateFile)
			assert.NoError(t, err)

			// Test LoadState
			err = vb.LoadState(stateFile)
			assert.NoError(t, err)

			// Remove the state file
			os.Remove(stateFile)
		})

	})
}

func TestBlockLookup(t *testing.T) {

	runWithBackends(t, "write_and_read", func(t *testing.T, vb *VB) {
		t.Run("Block Lookup Operations", func(t *testing.T) {

			// Write a test block
			blockID := uint64(0)
			data := make([]byte, DefaultBlockSize)
			msg := "test data"
			copy(data[:len(msg)], msg)

			err := vb.WriteAt(blockID*uint64(vb.BlockSize), data)
			assert.NoError(t, err)

			// Next block
			blockID = uint64(1)
			data = make([]byte, DefaultBlockSize)
			msg = "hello world"
			copy(data[:len(msg)], msg)

			err = vb.WriteAt(blockID*uint64(vb.BlockSize), data)
			assert.NoError(t, err)

			// Next block
			blockID = uint64(2)
			data = make([]byte, DefaultBlockSize)
			msg = "say hi in japanese"
			copy(data[:len(msg)], msg)

			err = vb.WriteAt(blockID*uint64(vb.BlockSize), data)
			assert.NoError(t, err)

			// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
			// Test LookupBlockToObject

			for _, block := range []uint64{0, 1, 2} {
				objectID, objectOffset, err := vb.LookupBlockToObject(block)
				assert.Error(t, err)
				assert.Equal(t, uint64(0), objectID)
				assert.Equal(t, uint32(0), objectOffset)
			}

			err = vb.Flush()
			assert.NoError(t, err)

			err = vb.WriteWALToChunk(true)
			assert.NoError(t, err)

			headersLen := vb.WALHeaderSize()

			// Test LookupBlockToObject
			for _, block := range []uint64{0, 1, 2} {
				objectID, objectOffset, err := vb.LookupBlockToObject(block)
				assert.NoError(t, err)
				assert.Equal(t, objectID, uint64(0))
				offset := uint32(vb.BlockSize)*uint32(block) + uint32(headersLen)
				assert.Equal(t, offset, objectOffset)
				t.Log("offset", offset, "objectOffset", objectOffset)

				assert.NotZero(t, objectOffset)
			}

			// Next, write a new block and flush
			blockID = uint64(3)
			data = make([]byte, DefaultBlockSize)
			msg = "new block"
			copy(data[:len(msg)], msg)

			err = vb.WriteAt(blockID*uint64(vb.BlockSize), data)

			assert.NoError(t, err)

			// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
			objectID, objectOffset, err := vb.LookupBlockToObject(3)
			assert.Error(t, err)
			assert.Equal(t, uint64(0), objectID)
			assert.Equal(t, uint32(0), objectOffset)

			err = vb.Flush()
			assert.NoError(t, err)

			err = vb.WriteWALToChunk(true)
			assert.NoError(t, err)

			// Lookup the block in the WAL, should be the 2nd chunk
			objectID, objectOffset, err = vb.LookupBlockToObject(3)
			assert.NoError(t, err)
			assert.Equal(t, uint64(1), objectID)
			offset := uint32(vb.BlockSize)*uint32(0) + uint32(headersLen)
			assert.Equal(t, int(offset), int(objectOffset))

		})

	})
}

func TestFlushOperations(t *testing.T) {

	runWithBackends(t, "write_and_read", func(t *testing.T, vb *VB) {

		t.Run("Flush Operations", func(t *testing.T) {
			// Write multiple blocks
			for i := uint64(1); i <= 5; i++ {
				buffer := make([]byte, DefaultBlockSize)
				msg := fmt.Sprintf("test data %d", i)
				copy(buffer[:len(msg)], msg)
				err := vb.WriteAt(i*uint64(vb.BlockSize), buffer)
				assert.NoError(t, err)
			}

			// Test Flush
			err := vb.Flush()
			assert.NoError(t, err)

			// Test WriteWALToChunk
			err = vb.WriteWALToChunk(true)
			assert.NoError(t, err)
		})

	})
}

func TestConsecutiveBlockRead(t *testing.T) {

	runWithBackends(t, "consecutive_block_read", func(t *testing.T, vb *VB) {

		t.Run("Consecutive Block Read", func(t *testing.T) {

			err := vb.SetCacheSize(0, 0)
			assert.NoError(t, err)

			// Write multiple blocks together
			buffer := make([]byte, DefaultBlockSize*10)

			for i := 0; i < 10; i++ {
				// Add random data to the buffer
				rand.Read(buffer[i*int(DefaultBlockSize) : (i+1)*int(DefaultBlockSize)])
			}

			err = vb.WriteAt(0, buffer)
			assert.NoError(t, err)

			// Test Flush
			err = vb.Flush()
			assert.NoError(t, err)

			// Test WriteWALToChunk
			err = vb.WriteWALToChunk(true)
			assert.NoError(t, err)

			// Read 4 blocks, confirm we get the correct data
			data := make([]byte, vb.BlockSize*4)
			readData, err := vb.ReadAt(0, uint64(len(data)))
			assert.NoError(t, err)
			assert.Equal(t, buffer[0:len(data)], readData)

		})

	})
}

func TestInvalidS3Host(t *testing.T) {

	runWithBackends(t, "invalid_s3_write_and_read", func(t *testing.T, vb *VB) {

		// Skip if file backend
		if vb.Backend.GetBackendType() == "file" {
			t.Skip("Skipping test for file backend")
		}

		t.Run("Use Invalid S3 Bucket", func(t *testing.T) {

			// Get a temp free port
			tempPort, err := getFreePort()
			assert.NoError(t, err)

			vb.Backend.SetConfig(s3.S3Config{
				VolumeName: vb.Backend.GetVolume(),
				VolumeSize: volumeSize,
				Region:     "ap-southeast-2",
				Bucket:     "bad_bucket",
				AccessKey:  AccessKey,
				SecretKey:  SecretKey,
				Host:       fmt.Sprintf("https://%s", tempPort),
			})

			err = vb.Backend.Init()
			assert.Error(t, err)

			// Write a block
			// Next, write a new block and flush
			blockID := uint64(5)
			data := make([]byte, DefaultBlockSize)
			msg := "bad bucket block"
			copy(data[:len(msg)], msg)

			err = vb.WriteAt(blockID*uint64(vb.BlockSize), data)
			assert.NoError(t, err)

			// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
			objectID, objectOffset, err := vb.LookupBlockToObject(5)
			assert.Error(t, err)
			assert.Equal(t, uint64(0), objectID)
			assert.Equal(t, uint32(0), objectOffset)

			err = vb.Flush()
			assert.NoError(t, err)

			err = vb.WriteWALToChunk(true)
			assert.Error(t, err)

			// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
			objectID, objectOffset, err = vb.LookupBlockToObject(4)
			assert.Error(t, err)
			assert.Equal(t, uint64(0), objectID)
			assert.Equal(t, uint32(0), objectOffset)

		})

	})
}

func TestInvalidS3Bucket(t *testing.T) {

	runWithBackends(t, "invalid_s3_write_and_read", func(t *testing.T, vb *VB) {

		// Skip if file backend
		if vb.Backend.GetBackendType() == "file" {
			t.Skip("Skipping test for file backend")
		}

		t.Run("Use Invalid S3 Bucket", func(t *testing.T) {

			vb.Backend.SetConfig(s3.S3Config{
				VolumeName: vb.Backend.GetVolume(),
				VolumeSize: volumeSize,
				Region:     "ap-southeast-2",
				Bucket:     "bad_bucket",
				AccessKey:  AccessKey,
				SecretKey:  SecretKey,
				Host:       vb.Backend.GetHost(),
			})

			err := vb.Backend.Init()
			assert.Error(t, err)

			// Write a block
			// Next, write a new block and flush
			blockID := uint64(5)
			data := make([]byte, DefaultBlockSize)
			msg := "bad bucket block"
			copy(data[:len(msg)], msg)

			err = vb.WriteAt(blockID*uint64(vb.BlockSize), data)
			assert.NoError(t, err)

			// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
			objectID, objectOffset, err := vb.LookupBlockToObject(5)
			assert.Error(t, err)
			assert.Equal(t, uint64(0), objectID)
			assert.Equal(t, uint32(0), objectOffset)

			err = vb.Flush()
			assert.NoError(t, err)

			err = vb.WriteWALToChunk(true)
			assert.Error(t, err)

			// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
			objectID, objectOffset, err = vb.LookupBlockToObject(4)
			assert.Error(t, err)
			assert.Equal(t, uint64(0), objectID)
			assert.Equal(t, uint32(0), objectOffset)

		})

	})
}

func TestInvalidS3Auth(t *testing.T) {

	runWithBackends(t, "invalid_s3_write_and_read", func(t *testing.T, vb *VB) {

		// Skip if file backend
		if vb.Backend.GetBackendType() == "file" {
			t.Skip("Skipping test for file backend")
		}

		t.Run("Use Invalid S3 Auth", func(t *testing.T) {

			vb.Backend.SetConfig(s3.S3Config{
				VolumeName: vb.Backend.GetVolume(),
				VolumeSize: volumeSize,
				Region:     "ap-southeast-2",
				Bucket:     "test_bucket",
				AccessKey:  "INVALIDACCESSKEY",
				SecretKey:  "BADSECRET/K7MDENG/bPxRfiCYEXAMPLEKEY",
				Host:       vb.Backend.GetHost(),
			})

			// Write a block
			// Next, write a new block and flush
			blockID := uint64(4)
			data := make([]byte, DefaultBlockSize)
			msg := "bad auth block"
			copy(data[:len(msg)], msg)

			err := vb.WriteAt(blockID*uint64(vb.BlockSize), data)
			assert.NoError(t, err)

			// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
			objectID, objectOffset, err := vb.LookupBlockToObject(4)
			assert.Error(t, err)
			assert.Equal(t, uint64(0), objectID)
			assert.Equal(t, uint32(0), objectOffset)

			err = vb.Flush()
			assert.NoError(t, err)

			err = vb.WriteWALToChunk(true)
			assert.Error(t, err)

			// Confirm the block does not exist in the object/chunk file (since not flushed/Write to WAL)
			objectID, objectOffset, err = vb.LookupBlockToObject(4)
			assert.Error(t, err)
			assert.Equal(t, uint64(0), objectID)
			assert.Equal(t, uint32(0), objectOffset)

		})

	})

}
func TestCacheConfiguration(t *testing.T) {

	runWithBackends(t, "write_and_read", func(t *testing.T, vb *VB) {
		t.Run("Set Cache Size", func(t *testing.T) {
			// Test setting cache size
			err := vb.SetCacheSize(1000, 0)
			assert.NoError(t, err)
			assert.Equal(t, 1000, vb.Cache.config.Size)
			assert.False(t, vb.Cache.config.UseSystemMemory)
			assert.Equal(t, 0, vb.Cache.config.SystemMemoryPercent)

			// Test invalid cache size
			err = vb.SetCacheSize(0, 0)
			assert.NoError(t, err)
			assert.Equal(t, 0, vb.Cache.config.Size)
			assert.False(t, vb.Cache.config.UseSystemMemory)
			assert.Equal(t, 0, vb.Cache.config.SystemMemoryPercent)

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

			t.Skip("Skipping cache operations test")
			// Set a small cache size for testing
			err := vb.SetCacheSize(5, 0)
			assert.NoError(t, err)

			// Write some blocks
			for i := uint64(0); i < 10; i++ {
				data := make([]byte, DefaultBlockSize)
				msg := fmt.Sprintf("test data %d", i)
				copy(data[:len(msg)], msg)

				err := vb.Write(i, data)
				assert.NoError(t, err)
			}

			// Verify only the last 5 blocks are in cache
			for i := uint64(0); i < 10; i++ {
				data, err := vb.ReadAt(i*uint64(vb.BlockSize), uint64(vb.BlockSize))
				if i < 5 {
					// First 5 blocks should be evicted, and returned ZeroBlock error
					assert.ErrorIs(t, err, ZeroBlock)
					//assert.Error(t, err)
				} else {
					// Last 5 blocks should be in cache
					assert.NoError(t, err)
					compare := make([]byte, DefaultBlockSize)
					msg := fmt.Sprintf("test data %d", i)
					copy(compare[:len(msg)], msg)
					assert.Equal(t, compare, data)
				}
			}
		})

	})
}

// getFreePort allocates a free TCP port from the OS
func getFreePort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

// waitForServer polls the URL until it responds or times out
func waitForServer(url string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	client := http.Client{Timeout: 100 * time.Millisecond, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // only for test
		},
	}}
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
