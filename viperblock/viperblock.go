// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package viperblock

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

type VBState struct {
	BlockSize    uint32
	ObjBlockSize uint32

	SeqNum    uint64
	ObjectNum uint64
	WALNum    uint64
}

type VB struct {
	BlockSize    uint32
	ObjBlockSize uint32

	FlushInterval time.Duration
	FlushSize     uint32

	// Sequence number for the next block to be written
	SeqNum atomic.Uint64

	// Chunk number for the next chunk to be written
	ObjectNum atomic.Uint64

	// Object map, maps block IDs to block objects (e.g request block 1,000,000, stored as object 43 at offset 256)
	BlocksToObject BlocksToObject

	// Writes, incoming data stored in Blocks in main memory
	Writes Blocks

	// Periodically writes (Blocks) are flushed to the WAL log (default 5s, or when 64MB written, or OS flushes)
	WAL WAL

	// In-memory cache of recently used blocks from read/write operations
	Cache Cache

	Version uint16

	WALMagic   [4]byte
	ChunkMagic [4]byte

	Backend types.Backend
}

// CacheConfig holds configuration for the LRU cache
type CacheConfig struct {
	// Size in number of blocks
	Size int
	// Whether to use system memory percentage
	UseSystemMemory bool
	// Percentage of system memory to use (0-100)
	SystemMemoryPercent int
}

type Cache struct {
	mu     sync.RWMutex
	lru    *lru.Cache[uint64, []byte]
	config CacheConfig
}

type ObjectMap struct {
	mu sync.RWMutex

	Objects map[uint64]Block
}

type BlockCache struct {
	Data []byte
}

type Block struct {
	SeqNum uint64
	Block  uint64
	Offset uint64
	Len    uint64
	Data   []byte
}

type Blocks struct {
	Blocks []Block
	mu     sync.RWMutex
}

type BlocksToObject struct {
	mu sync.RWMutex

	BlockLookup map[uint64]BlockLookup
}

type BlockLookup struct {
	StartBlock   uint64
	NumBlocks    uint16
	ObjectID     uint64
	ObjectOffset uint32
}

type BlocksMap map[uint64]Block

type WAL struct {
	DB      []*os.File
	WallNum atomic.Uint64
	BaseDir string
	mu      sync.RWMutex
}

// getSystemMemory returns the total system memory in bytes
func getSystemMemory() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys
}

// calculateCacheSize calculates the number of blocks that can fit in the cache
// based on the system memory and block size
func calculateCacheSize(blockSize uint32, percent int) int {
	if percent <= 0 || percent > 100 {
		percent = 30 // default to 30%
	}

	systemMemory := getSystemMemory()
	cacheMemory := (systemMemory * uint64(percent)) / 100

	return int(cacheMemory / uint64(blockSize))
}

// SetCacheSize sets the size of the LRU cache in number of blocks
func (vb *VB) SetCacheSize(size int, percentage int) error {
	if size <= 0 {
		return fmt.Errorf("cache size must be greater than 0")
	}

	vb.Cache.mu.Lock()
	defer vb.Cache.mu.Unlock()

	// Create new LRU cache with specified size
	newCache, err := lru.New[uint64, []byte](size)
	if err != nil {
		return fmt.Errorf("failed to create new LRU cache: %v", err)
	}

	// Replace old cache with new one
	vb.Cache.lru = newCache
	vb.Cache.config.Size = size

	if percentage > 0 {
		vb.Cache.config.UseSystemMemory = true
		vb.Cache.config.SystemMemoryPercent = percentage
	} else {
		vb.Cache.config.UseSystemMemory = false
		vb.Cache.config.SystemMemoryPercent = 0
	}

	return nil
}

// SetCacheSystemMemory sets the cache size based on a percentage of system memory
func (vb *VB) SetCacheSystemMemory(percent int) error {
	if percent <= 0 || percent > 100 {
		return fmt.Errorf("system memory percentage must be between 1 and 100")
	}

	size := calculateCacheSize(vb.BlockSize, percent)

	return vb.SetCacheSize(size, percent)
}

func New(btype string, config interface{}) (vb *VB) {
	var defaultBlockSize uint32 = 4 * 1024
	var defaultObjBlockSize uint32 = 128 * 1024
	var backend types.Backend
	switch btype {
	case "file":
		backend = file.New(config)
	case "s3":
		backend = s3.New(config)
	}

	// Calculate initial cache size based on 30% of system memory
	initialCacheSize := calculateCacheSize(defaultBlockSize, 30)

	// Create LRU cache with calculated size
	lruCache, err := lru.New[uint64, []byte](initialCacheSize)
	if err != nil {
		panic(fmt.Sprintf("failed to create LRU cache: %v", err))
	}

	vb = &VB{
		BlockSize:     defaultBlockSize,
		ObjBlockSize:  defaultObjBlockSize,
		FlushInterval: 5 * time.Second,
		FlushSize:     64 * 1024 * 1024,
		Writes:        Blocks{},
		WAL:           WAL{BaseDir: "tmp/"},
		Cache: Cache{
			lru: lruCache,
			config: CacheConfig{
				Size:                initialCacheSize,
				UseSystemMemory:     true,
				SystemMemoryPercent: 30,
			},
		},
		Version:        1,
		WALMagic:       [4]byte{'V', 'B', 'W', 'L'},
		ChunkMagic:     [4]byte{'V', 'B', 'C', 'H'},
		BlocksToObject: BlocksToObject{},
		Backend:        backend,
	}

	vb.BlocksToObject.BlockLookup = make(map[uint64]BlockLookup)

	return vb
}

func (vb *VB) SetWALBaseDir(baseDir string) {
	vb.WAL.BaseDir = baseDir
}

// WAL functions
func (vb *VB) OpenWAL(filename string) (err error) {
	// Lock operations on the WAL
	vb.WAL.mu.Lock()
	defer vb.WAL.mu.Unlock()

	// Create the directory if it doesn't exist
	os.MkdirAll(filepath.Dir(filename), 0755)

	// Create the file if it doesn't exist
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0640)

	if err != nil {
		return err
	}

	// Append the latest "hot" WAL file to the DB
	vb.WAL.DB = append(vb.WAL.DB, file)

	return err

}

func (vb *VB) Close(file *os.File) error {

	return file.Close()

}

func (vb *VB) Write(block uint64, data []byte) (err error) {
	// Validate length of data
	if len(data) > int(vb.BlockSize) {
		return fmt.Errorf("data length exceeds block size")
	}

	// Write raw data received, first to the main memory Block, which will be flushed to the WAL
	seqNum := vb.SeqNum.Add(1)

	vb.Writes.mu.Lock()
	vb.Writes.Blocks = append(vb.Writes.Blocks, Block{
		SeqNum: seqNum,
		Block:  block,
		Data:   data,
	})
	vb.Writes.mu.Unlock()

	// Update the cache
	vb.Cache.mu.Lock()
	vb.Cache.lru.Add(block, data)
	vb.Cache.mu.Unlock()

	return nil
}

// Flush the main memory (writes) to the WAL
func (vb *VB) Flush() (err error) {

	vb.Writes.mu.Lock()

	for _, block := range vb.Writes.Blocks {

		// Write the block to the WAL
		vb.WriteWAL(block)

	}

	// Once flushed, clear the main memory
	vb.Writes.Blocks = nil

	vb.Writes.mu.Unlock()

	return err

}

func (vb *VB) WriteWAL(block Block) (err error) {

	vb.WAL.mu.Lock()

	// Get the current WAL file
	currentWAL := vb.WAL.DB[len(vb.WAL.DB)-1]

	// Format for each block in the WAL
	// [seq_number, uint64][block_number, uint64][block_length, uint64][checksum, uint32][block_data, []byte]
	// big endian

	// Write the block number as big endian
	blockSeq := make([]byte, 8)
	binary.BigEndian.PutUint64(blockSeq, block.SeqNum)
	currentWAL.Write(blockSeq)

	// Write the block number as big endian
	blockNumber := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumber, block.Block)
	currentWAL.Write(blockNumber)

	// TODO: Optimise
	blockLength := make([]byte, 8)
	binary.BigEndian.PutUint64(blockLength, uint64(len(block.Data)))
	currentWAL.Write(blockLength)

	// Calculate a CRC32 checksum of the block data and headers
	checksum := crc32.ChecksumIEEE(append(blockSeq, blockNumber...))
	checksum = crc32.Update(checksum, crc32.IEEETable, blockLength)
	checksum = crc32.Update(checksum, crc32.IEEETable, block.Data)

	// Write the checksum as big endian
	checksumBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumBytes, checksum)
	currentWAL.Write(checksumBytes)

	// Next, write the block data
	currentWAL.Write(block.Data)

	vb.WAL.mu.Unlock()
	return err
}

func (vb *VB) ReadWAL() (err error) {

	block := Block{}
	vb.WAL.mu.RLock()

	// Scan through the file, reading the block number, offset, length, checksum, and block data
	currentWAL := vb.WAL.DB[len(vb.WAL.DB)-1]

	defer vb.WAL.mu.RUnlock()

	for {

		// Read the block number
		headers := make([]byte, 28)
		n, err := currentWAL.Read(headers)

		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		block.SeqNum = binary.BigEndian.Uint64(headers[:8])
		block.Block = binary.BigEndian.Uint64(headers[8:16])
		block.Len = binary.BigEndian.Uint64(headers[16:24])
		checksum := binary.BigEndian.Uint32(headers[24:28])

		// Read the block data
		block.Data = make([]byte, block.Len)

		// TODO: Optimise, read entire block at once, from the header magic that tells us the length
		n, err = currentWAL.Read(block.Data)

		if n != int(block.Len) {
			return fmt.Errorf("incomplete read: got %d bytes, expected %d", n, block.Len)
		}

		// Calculate a CRC32 checksum of the block data and headers
		checksum_validated := crc32.ChecksumIEEE(headers[:8])
		checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, headers[8:16])
		checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, headers[16:24])
		checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, block.Data[:n])

		if checksum_validated != checksum {
			err2 := errors.New("checksum mismatch for block " + strconv.FormatUint(block.Block, 10) + " offset: " + strconv.FormatUint(block.Offset, 10))
			slog.Error("checksum mismatch", "error", err2)
			return err2
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

	}

	return nil

}

func (vb *VB) WriteWALToChunk() (err error) {

	// First, lock, and close the current WAL file
	vb.WAL.mu.Lock()

	currentWALNum := vb.WAL.WallNum.Load()

	// Scan through the file, reading the block number, offset, length, checksum, and block data
	pendingWAL := vb.WAL.DB[len(vb.WAL.DB)-1]
	vb.WAL.mu.Unlock()

	defer pendingWAL.Close()

	// Increment the WAL number
	nextWalNum := vb.WAL.WallNum.Add(1)

	// Create the next WAL file, and unlock, so other threads can continue to write to the new WAL, while we read from the pending WAL
	vb.OpenWAL(fmt.Sprintf("%s/%s/wal.%08d.bin", vb.WAL.BaseDir, vb.Backend.GetVolume(), nextWalNum))

	// First seek to the beginning of the pending WAL file
	pendingWAL.Close()

	filename := fmt.Sprintf("%s/%s/wal.%08d.bin", vb.WAL.BaseDir, vb.Backend.GetVolume(), currentWALNum)
	pendingWAL2, err := os.OpenFile(filename, os.O_RDWR, 0640)
	//fmt.Sprintf("%s/%s/wal.%08d.bin", vb.WAL.BaseDir, vb.Backend.GetVolume(), currentWALNum), os.O_RDWR, 0640)

	if err != nil {
		slog.Error("ERROR OPENING WAL:", "filename", filename, "error", err)
		return err
	}

	//pendingWAL.Seek(0, 0)

	blocks := []Block{}

	// Next, read the pending WAL file, and write to the chunk file
	for {

		// Read the headers + data in one chunk
		data := make([]byte, 28+4096)
		_, err := pendingWAL2.Read(data)

		if err != nil {
			if err == io.EOF {
				break
			}
			slog.Error("ERROR READING WAL:", "error", err)
		}

		// Validate the checksum
		checksum := binary.BigEndian.Uint32(data[24:28])

		// Calculate a CRC32 checksum of the block data and headers
		checksum_validated := crc32.ChecksumIEEE(data[:8])
		checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, data[8:16])
		checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, data[16:24])
		checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, data[28:])

		if checksum_validated != checksum {
			slog.Error("checksum mismatch in WriteWALToChunk", "checksum_validated", checksum_validated, "checksum", checksum)
		}

		// Add the block to the list

		blocks = append(blocks, Block{
			SeqNum: binary.BigEndian.Uint64(data[:8]),
			Block:  binary.BigEndian.Uint64(data[8:16]),
			Len:    binary.BigEndian.Uint64(data[16:24]),
			Data:   data[28:],
		})

	}

	// View the blocks
	// Find any blocks that are the same, let the latest seqnum win
	blocksMap := make(BlocksMap, len(blocks))

	for _, block := range blocks {

		//fmt.Println("SEQNUM:", block.SeqNum, "BLOCK:", block.Block, "LEN:", block.Len)

		if _, ok := blocksMap[block.Block]; !ok {
			//fmt.Println("BLOCK ADDED TO MAP:", block.Block)
			blocksMap[block.Block] = block
		} else {
			//fmt.Println("BLOCK EXISTS IN MAP:", blocksMap[block.Block].SeqNum, "vs", block.SeqNum)
			if blocksMap[block.Block].SeqNum < block.SeqNum {
				//fmt.Println("BLOCK EXISTS, LATEST SEQNUM WINS:", block.SeqNum)
				blocksMap[block.Block] = block
			}
		}

	}

	var sortedBlocks []Block

	for _, block := range blocksMap {
		sortedBlocks = append(sortedBlocks, block)
	}

	sort.Slice(sortedBlocks, func(i, j int) bool { return sortedBlocks[i].Block < sortedBlocks[j].Block })

	// Next, given the blocks are deduplicated, and now sorted, write each block to the chunk file in ObjBlockSize chunk sizes
	var chunkBuffer = make([]byte, 0, vb.ObjBlockSize)
	var matchedBlocks = make([]Block, 0)

	for _, block := range sortedBlocks {

		//fmt.Println("LOOKUP:", k, "BLOCK:", block.Block, "LEN:", block.Len)
		// Append block data to buffer
		chunkBuffer = append(chunkBuffer, block.Data...)
		matchedBlocks = append(matchedBlocks, Block{
			SeqNum: block.SeqNum,
			Block:  block.Block,
		})

		// If buffer is full, write to file
		if len(chunkBuffer) >= int(vb.ObjBlockSize) {
			slog.Debug("WRITING CHUNK:", "chunkIndex", vb.ObjectNum.Load())

			err := vb.createChunkFile(currentWALNum, vb.ObjectNum.Load(), &chunkBuffer, &matchedBlocks)
			if err != nil {
				slog.Error("Failed to create chunk file", "error", err)
				return err
			}

			chunkBuffer = chunkBuffer[:0] // Reset buffer
			matchedBlocks = make([]Block, 0)
		}
	}

	// Write any remaining data as the last chunk
	if len(chunkBuffer) > 0 {
		err := vb.createChunkFile(currentWALNum, vb.ObjectNum.Load(), &chunkBuffer, &matchedBlocks)
		if err != nil {
			slog.Error("Failed to create chunk file", "error", err)
			return err
		}

	}

	// Next, read the pending WAL file, and write to the chunk file

	return err

}

func (vb *VB) createChunkFile(currentWALNum uint64, chunkIndex uint64, chunkBuffer *[]byte, matchedBlocks *[]Block) (err error) {

	//
	slog.Info("Creating chunk file", "chunkIndex", chunkIndex, "currentWALNum", currentWALNum)

	// Generate headers
	headers := make([]byte, 10)
	copy(headers[:3], vb.ChunkMagic[:])

	binary.BigEndian.PutUint16(headers[3:5], vb.Version)
	binary.BigEndian.PutUint32(headers[5:9], vb.BlockSize)

	//vb.Backend.Open()
	err = vb.Backend.Write(chunkIndex, &headers, chunkBuffer)
	if err != nil {
		return err
	}

	headerLen := len(headers)

	// Loop through the chunk buffer, and write each block to the file
	i := 0

	vb.BlocksToObject.mu.Lock()
	for k, block := range *matchedBlocks {

		// Find out how many consecutive blocks there are
		numBlocks := 1
		for j := k + 1; j < len(*matchedBlocks); j++ {

			if (*matchedBlocks)[j].Block == (*matchedBlocks)[j-1].Block+1 {
				numBlocks++
			} else {
				break
			}

		}

		// TODO: Optimise for number of consecutive blocks to reduce the memory size
		vb.BlocksToObject.BlockLookup[block.Block] = BlockLookup{
			StartBlock:   block.Block,
			NumBlocks:    uint16(numBlocks),
			ObjectID:     chunkIndex,
			ObjectOffset: uint32(headerLen + (i * int(vb.BlockSize))),
		}

		i++

	}

	vb.BlocksToObject.mu.Unlock()

	// Increment the object number
	vb.ObjectNum.Add(1)

	return nil
}

func (vb *VB) SerializeBlocksToObject(filename string) (err error) {

	vb.BlocksToObject.mu.RLock()

	// Write the BlocksToObject to a file as a binary file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the BlocksToObject to the file as JSON
	json.NewEncoder(file).Encode(vb.BlocksToObject.BlockLookup)

	defer vb.BlocksToObject.mu.RUnlock()

	return err

}

// Lookup Block to Object
func (vb *VB) LookupBlockToObject(block uint64) (objectID uint64, objectOffset uint32, err error) {

	vb.BlocksToObject.mu.RLock()

	blockLookup, ok := vb.BlocksToObject.BlockLookup[block]

	vb.BlocksToObject.mu.RUnlock()

	if ok {
		return blockLookup.ObjectID, blockLookup.ObjectOffset, nil
	} else {
		return 0, 0, fmt.Errorf("block not found")
	}

}

// Save the block tracking state to disk
func (vb *VB) SaveState(filename string) error {
	vb.BlocksToObject.mu.Lock()
	defer vb.BlocksToObject.mu.Unlock()

	state := VBState{
		BlockSize:    vb.BlockSize,
		ObjBlockSize: vb.ObjBlockSize,
		SeqNum:       vb.SeqNum.Load(),
		ObjectNum:    vb.ObjectNum.Load(),
		WALNum:       vb.WAL.WallNum.Load(),
	}

	// Save as JSON
	jsonData, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// Write to file
	err = os.WriteFile(filename, jsonData, 0644)
	if err != nil {
		return err
	}

	return nil
}

// Load the block tracking state from disk
func (vb *VB) LoadState(filename string) error {

	state := VBState{}
	// Read from file
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	// Parse JSON
	err = json.Unmarshal(jsonData, &state)
	if err != nil {
		return err
	}

	vb.BlockSize = state.BlockSize
	vb.ObjBlockSize = state.ObjBlockSize
	vb.SeqNum.Store(state.SeqNum)
	vb.ObjectNum.Store(state.ObjectNum)
	vb.WAL.WallNum.Store(state.WALNum)

	return nil
}

// Read reads a block from the storage backend
func (vb *VB) Read(block uint64) (data []byte, err error) {
	// First check the cache
	vb.Cache.mu.RLock()
	if cachedData, ok := vb.Cache.lru.Get(block); ok {
		vb.Cache.mu.RUnlock()
		return cachedData, nil
	}
	vb.Cache.mu.RUnlock()

	// Look up the block in the object map
	objectID, objectOffset, err := vb.LookupBlockToObject(block)
	if err != nil {
		return nil, err
	}

	// Read from the backend
	data, err = vb.Backend.Read(objectID, uint32(objectOffset), vb.BlockSize)
	if err != nil {
		return nil, err
	}

	// Update the cache with the read data
	vb.Cache.mu.Lock()
	vb.Cache.lru.Add(block, data)
	vb.Cache.mu.Unlock()

	return data, nil
}

func (vb *VB) WALHeader(data []byte) []byte {
	header := make([]byte, vb.WALHeaderSize())
	copy(header[:len(vb.ChunkMagic)], vb.ChunkMagic[:])
	binary.BigEndian.PutUint16(header[3:5], vb.Version)
	binary.BigEndian.PutUint32(header[5:9], vb.BlockSize)
	return header
}

// WALHeaderSize returns the size of the WAL header in bytes
func (vb *VB) WALHeaderSize() int {
	// Magic bytes (4) + Version (2) + BlockSize (4)
	return len(vb.ChunkMagic) + binary.Size(vb.Version) + binary.Size(vb.BlockSize)
}
