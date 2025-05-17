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
	"syscall"
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

	// Pending writes to backend, when data flushed, WAL appended, and prior to backend upload completion (to avoid race conditions)
	PendingBackendWrites Blocks

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

type ConsecutiveBlock struct {
	BlockPosition uint64
	StartBlock    uint64
	NumBlocks     uint16
	OffsetStart   uint64
	OffsetEnd     uint64
	ObjectID      uint64
	ObjectOffset  uint32
}

type ConsecutiveBlocks []ConsecutiveBlock

type BlocksMap map[uint64]Block

type WAL struct {
	DB      []*os.File
	WallNum atomic.Uint64
	BaseDir string
	mu      sync.RWMutex
}

// Error messages

var ZeroBlock = errors.New("zero block")
var RequestTooLarge = errors.New("request too large")
var RequestOutOfRange = errors.New("request out of range")
var RequestBlockSize = errors.New("request must be a multiple of block size")

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

	if size < 0 {
		return fmt.Errorf("cache size must be greater than 0")
	}

	if size == 0 {
		// Disable the cache
		vb.Cache.config.Size = 0
		vb.Cache.config.UseSystemMemory = false
		vb.Cache.config.SystemMemoryPercent = 0
		return nil
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
	var defaultObjBlockSize uint32 = 1024 * 1024 * 4
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

	// Create the file if it doesn't exist, make sure writes and committed immediately
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR|syscall.O_SYNC, 0640)

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

	blockLen := uint64(len(data))

	// First check the block exists in our volume size
	if block*uint64(vb.BlockSize) > vb.Backend.GetVolumeSize() {
		return RequestTooLarge
	}

	// Check if the request is within range
	if block*uint64(vb.BlockSize)+blockLen > vb.Backend.GetVolumeSize() {
		return RequestOutOfRange
	}

	// Check blockLen a multiple of a blocksize
	if blockLen%uint64(vb.BlockSize) != 0 {
		return RequestBlockSize
	}

	blockRequests := blockLen / uint64(vb.BlockSize)

	slog.Info("\tVBWRITE:", "blockRequests", blockRequests, "block", block, "blockLen", blockLen)

	vb.Writes.mu.Lock()

	// Loop through each block request
	for i := uint64(0); i < blockRequests; i++ {

		currentBlock := block + i

		start := i * uint64(vb.BlockSize)
		end := start + uint64(vb.BlockSize)

		blockCopy := make([]byte, vb.BlockSize)
		copy(blockCopy, data[start:end])

		slog.Error("\t\tBLOCKWRITE:", "currentBlock", currentBlock, "start", start, "end", end, "i", i)

		// Write raw data received, first to the main memory Block, which will be flushed to the WAL
		seqNum := vb.SeqNum.Add(1)

		vb.Writes.Blocks = append(vb.Writes.Blocks, Block{
			SeqNum: seqNum,
			Block:  currentBlock,
			Data:   blockCopy,
		})

		slog.Error("WRITE:", "seqNum", seqNum, "BLOCK:", currentBlock, "start", start, "end", end)

	}

	vb.Writes.mu.Unlock()

	return nil
}

// Flush the main memory (writes) to the WAL
func (vb *VB) Flush() error {
	vb.Writes.mu.Lock()
	flushBlocks := make([]Block, len(vb.Writes.Blocks))
	copy(flushBlocks, vb.Writes.Blocks)
	vb.Writes.mu.Unlock()

	// Map to track successfully flushed blocks
	flushed := make(map[uint64]uint64) // block -> seqnum

	for _, block := range flushBlocks {
		slog.Info("FLUSH:", "block", block.Block, "seqnum", block.SeqNum)

		if err := vb.WriteWAL(block); err != nil {
			slog.Error("ERROR FLUSHING:", "block", block.Block, "error", err)
			break
		}

		// Record successful flush
		flushed[block.Block] = block.SeqNum
	}

	// Filter vb.Writes.Blocks to keep only blocks NOT successfully flushed
	if len(flushed) > 0 {
		vb.Writes.mu.Lock()

		remaining := make([]Block, 0)
		for _, b := range vb.Writes.Blocks {
			if _, ok := flushed[b.Block]; !ok {
				slog.Error("REMAINING:", "block", b.Block, "seqnum", b.SeqNum)
				// Either this block wasn't flushed, or it was rewritten after flush started
				remaining = append(remaining, b)
			}
		}

		vb.Writes.Blocks = remaining
		vb.Writes.mu.Unlock()
	}

	// Append successful flushed blocks to the PendingBackendWrites
	vb.PendingBackendWrites.mu.Lock()
	vb.PendingBackendWrites.Blocks = append(vb.PendingBackendWrites.Blocks, flushBlocks...)

	slog.Info("PENDING BACKEND WRITES:", "len", len(vb.PendingBackendWrites.Blocks))
	//spew.Dump(vb.PendingBackendWrites.Blocks)

	/*
		for _, block := range flushBlocks {
			vb.PendingBackendWrites.Blocks = append(vb.PendingBackendWrites.Blocks, block)
		}
	*/

	vb.PendingBackendWrites.mu.Unlock()

	if len(flushed) < len(flushBlocks) {
		return fmt.Errorf("partial flush: %d of %d blocks flushed", len(flushed), len(flushBlocks))
	}

	return nil
}

func (vb *VB) Flush2() (err error) {

	vb.Writes.mu.Lock()
	flushBlocks := make([]Block, len(vb.Writes.Blocks))
	copy(flushBlocks, vb.Writes.Blocks)
	vb.Writes.Blocks = nil
	vb.Writes.mu.Unlock()

	for _, block := range flushBlocks {
		slog.Info("FLUSH:", "block", block.Block, "seqnum", block.SeqNum)

		// Write the block to the WAL
		err = vb.WriteWAL(block)
		if err != nil {
			slog.Error("ERROR FLUSHING:", "error", err)
			return err
		}

	}

	return nil

}

func (vb *VB) WriteWAL(block Block) (err error) {

	vb.WAL.mu.Lock()

	// Get the current WAL file
	currentWAL := vb.WAL.DB[len(vb.WAL.DB)-1]

	slog.Info("WRITE WAL:", "currentWAL", currentWAL)

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
	slog.Info("WriteWAL > blockNumber", "original", block.Block, "converted", binary.BigEndian.Uint64(blockNumber))

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

	if err != nil {
		slog.Error("ERROR OPENING WAL:", "filename", filename, "error", err)
		return err
	}

	blocks := []Block{}

	// Next, read the pending WAL file, and write to the chunk file
	for {

		// Read the headers + data in one chunk
		data := make([]byte, 28+4096)
		_, err := pendingWAL2.Read(data)

		// Check n is the size of the data expected
		/*
			if n != len(data) {
				slog.Error("ERROR READING WAL:", "n", n, "expected", len(data))
				return fmt.Errorf("incomplete read: got %d bytes, expected %d", n, len(data))
			}
		*/

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

		slog.Info("\t\tWAL BLOCK:", "SeqNum", binary.BigEndian.Uint64(data[:8]), "Block", binary.BigEndian.Uint64(data[8:16]), "Len", binary.BigEndian.Uint64(data[16:24]))

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

		if _, ok := blocksMap[block.Block]; !ok {
			blocksMap[block.Block] = block
		} else {
			if blocksMap[block.Block].SeqNum < block.SeqNum {
				blocksMap[block.Block] = block
			}
		}

	}

	// Sort the blocks to write consecutive blocks together in the chunk file
	var sortedBlocks []Block

	for _, block := range blocksMap {
		sortedBlocks = append(sortedBlocks, block)
	}

	sort.Slice(sortedBlocks, func(i, j int) bool { return sortedBlocks[i].Block < sortedBlocks[j].Block })

	// Next, given the blocks are deduplicated, and now sorted, write each block to the chunk file in ObjBlockSize chunk sizes
	var chunkBuffer = make([]byte, 0, vb.ObjBlockSize)
	var matchedBlocks = make([]Block, 0)

	for _, block := range sortedBlocks {

		// Append block data to buffer
		chunkBuffer = append(chunkBuffer, block.Data...)
		matchedBlocks = append(matchedBlocks, Block{
			SeqNum: block.SeqNum,
			Block:  block.Block,
		})

		// If buffer is full (default 4MB), write to file
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

	err = vb.Backend.Write(chunkIndex, &headers, chunkBuffer)
	if err != nil {
		return err
	}

	// After upload completion, remove from PendingBackendWrites
	// Clone a copy of the blocks
	vb.PendingBackendWrites.mu.Lock()
	pendingBackendWrites := make([]Block, len(vb.PendingBackendWrites.Blocks))
	copy(pendingBackendWrites, vb.PendingBackendWrites.Blocks)

	// Reset the pending backend writes
	vb.PendingBackendWrites.Blocks = make([]Block, 0)

	// Loop through the pending backend writes, and remove the blocks that have been written
	for _, block := range pendingBackendWrites {
		var matched bool = false
		for _, matchedBlock := range *matchedBlocks {
			if block.Block == matchedBlock.Block {
				matched = true
				break
			}
		}

		// If the block is not in the matched blocks, append to the pending backend writes
		if !matched {
			vb.PendingBackendWrites.Blocks = append(vb.PendingBackendWrites.Blocks, block)
		} else {
			// Update the cache with the block data on successful write

			if vb.Cache.config.Size > 0 {
				vb.Cache.lru.Add(block.Block, block.Data)
			}
		}
	}

	vb.PendingBackendWrites.mu.Unlock()

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

	// Increment the object number
	vb.ObjectNum.Add(1)

	vb.BlocksToObject.mu.Unlock()

	return nil
}

func (vb *VB) SaveHotState(filename string) (err error) {

	vb.Writes.mu.RLock()

	// Write the BlocksToObject to a file as a binary file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the BlocksToObject to the file as JSON
	json.NewEncoder(file).Encode(vb.Writes.Blocks)

	defer vb.Writes.mu.RUnlock()

	return err

}

func (vb *VB) SaveBlockState(filename string) (err error) {

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

	slog.Info("\tLOOKUP BLOCK TO OBJECT:", "block", block, "blockLookup", blockLookup)

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

// Private function to read a block from the storage backend, use ReadAt for public access
func (vb *VB) read(block uint64, blockLen uint64) (data []byte, err error) {

	// Check blockLen a multiple of a blocksize
	if blockLen%uint64(vb.BlockSize) != 0 {
		return nil, RequestBlockSize
	}

	var zeroBlockErr error
	data = make([]byte, blockLen)

	// Preprocess latest writes
	vb.Writes.mu.RLock()
	writesCopy := make([]Block, len(vb.Writes.Blocks))
	copy(writesCopy, vb.Writes.Blocks)
	vb.Writes.mu.RUnlock()

	latestWrites := make(BlocksMap)
	for _, wr := range writesCopy {
		if prev, ok := latestWrites[wr.Block]; !ok || wr.SeqNum > prev.SeqNum {
			latestWrites[wr.Block] = wr
		}
	}

	// Preprocess pending writes (after WAL write to backend upload success/completion)
	vb.PendingBackendWrites.mu.RLock()
	pendingWritesCopy := make([]Block, len(vb.PendingBackendWrites.Blocks))
	copy(pendingWritesCopy, vb.PendingBackendWrites.Blocks)
	vb.PendingBackendWrites.mu.RUnlock()

	latestPendingWrites := make(BlocksMap)
	for _, wr := range pendingWritesCopy {
		latestPendingWrites[wr.Block] = wr
	}

	blockRequests := blockLen / uint64(vb.BlockSize)

	var consecutiveBlocks ConsecutiveBlocks

	for i := uint64(0); i < blockRequests; i++ {
		currentBlock := block + i
		start := i * uint64(vb.BlockSize)
		end := start + uint64(vb.BlockSize)

		// If matched in our HOT writes, copy the data
		if wr, ok := latestWrites[currentBlock]; ok {
			slog.Info("[READ] HOT BLOCK:", "block", wr.Block, "seqnum", wr.SeqNum)

			copy(data[start:end], clone(wr.Data))
			continue
		}

		// Next, check the pending backend writes buffer
		if lp, ok := latestPendingWrites[currentBlock]; ok {
			slog.Info("[READ] PENDING BLOCK:", "block", lp.Block, "seqnum", lp.SeqNum)

			copy(data[start:end], clone(lp.Data))
			continue
		}

		// Next query the LRU cache if the data does not exist in the HOT write path, or pending write buffer.
		if vb.Cache.config.Size > 0 {
			if cachedData, ok := vb.Cache.lru.Get(currentBlock); ok {
				slog.Info("[READ] LRU CACHE BLOCK:", "block", currentBlock)

				copy(data[start:end], cachedData)
				continue
			}
		}

		// Next, fetch which object and offset the block is within
		objectID, objectOffset, err := vb.LookupBlockToObject(currentBlock)
		if err != nil {
			zeroBlockErr = ZeroBlock
			slog.Info("[READ] ZERO BLOCK:", "block", currentBlock)

			copy(data[start:end], make([]byte, vb.BlockSize)) // zero
			continue
		}

		slog.Info("[READ] OBJECT ID:", "objectID", objectID, "objectOffset", objectOffset)

		consecutiveBlocks = append(consecutiveBlocks, ConsecutiveBlock{
			BlockPosition: i,
			StartBlock:    currentBlock,
			NumBlocks:     1,
			OffsetStart:   start,
			OffsetEnd:     end,
			ObjectID:      objectID,
			ObjectOffset:  uint32(objectOffset),
		})
		// Lastly, query the backend for the block data
		/*
			blockData, err := vb.Backend.Read(objectID, objectOffset, vb.BlockSize)
			if err != nil {
				return nil, err
			}

			slog.Info("[READ] BACKEND BLOCK:", "block", currentBlock, "objectID", objectID, "objectOffset", objectOffset)

			copy(data[start:end], blockData)

			// Update the cache with the read data
			if vb.Cache.config.Size > 0 {
				vb.Cache.lru.Add(currentBlock, blockData)
			}
		*/

	}

	// Loop through all consecutive blocks that are required to fetch from the backend
	var consecutiveBlocksToRead ConsecutiveBlocks

	// Store which consecutive blocks we have already read
	consecutiveBlocksRead := make(map[uint64]bool)

	for i := 0; i < len(consecutiveBlocks); i++ {
		slog.Info("[READ] CONSECUTIVE BLOCK:", "startBlock", consecutiveBlocks[i].StartBlock, "numBlocks", consecutiveBlocks[i].NumBlocks, "offsetStart", consecutiveBlocks[i].OffsetStart, "offsetEnd", consecutiveBlocks[i].OffsetEnd, "objectID", consecutiveBlocks[i].ObjectID, "objectOffset", consecutiveBlocks[i].ObjectOffset)

		// Skip if this blocks belongs to a previous consecutive block
		if _, ok := consecutiveBlocksRead[consecutiveBlocks[i].StartBlock]; ok {
			slog.Info("[READ] SKIPPING CONSECUTIVE BLOCK READ:", "startBlock", consecutiveBlocks[i].StartBlock)
			continue
		}

		// Find out how many consecutive blocks there are
		numBlocks := 1
		for j := i + 1; j < len(consecutiveBlocks); j++ {

			// If our StartBlock is consecutive, and the ObjectID is the same, then we have a consecutive block to read from our backend
			if (consecutiveBlocks[j].StartBlock == consecutiveBlocks[j-1].StartBlock+1) && (consecutiveBlocks)[j].ObjectID == (consecutiveBlocks)[j-1].ObjectID {
				numBlocks++
				consecutiveBlocksRead[consecutiveBlocks[j].StartBlock] = true
			} else {
				break
			}

		}

		consecutiveBlocksToRead = append(consecutiveBlocksToRead, ConsecutiveBlock{
			BlockPosition: consecutiveBlocks[i].BlockPosition,
			StartBlock:    consecutiveBlocks[i].StartBlock,
			NumBlocks:     uint16(numBlocks),
			OffsetStart:   consecutiveBlocks[i].OffsetStart,
			OffsetEnd:     consecutiveBlocks[i].OffsetEnd,
			ObjectID:      consecutiveBlocks[i].ObjectID,
			ObjectOffset:  consecutiveBlocks[i].ObjectOffset,
		})
	}

	// Next, read our consecutive blocks from the backend
	for _, cb := range consecutiveBlocksToRead {
		slog.Info("[READ] READING CONSECUTIVE BLOCK:", "startBlock", cb.StartBlock, "numBlocks", cb.NumBlocks, "offsetStart", cb.OffsetStart, "offsetEnd", cb.OffsetEnd, "objectID", cb.ObjectID, "objectOffset", cb.ObjectOffset)

		// Account for our extra 10 bytes of metadata per block
		//consecutiveBlockStart := cb.BlockPosition * uint64(vb.BlockSize)
		consecutiveBlockOffset := (uint32(cb.NumBlocks) * uint32(vb.BlockSize))

		start := cb.BlockPosition * uint64(vb.BlockSize)
		end := start + uint64(cb.NumBlocks)*uint64(vb.BlockSize)

		blockData, err := vb.Backend.Read(cb.ObjectID, cb.ObjectOffset, consecutiveBlockOffset)
		if err != nil {
			return nil, err
		}

		slog.Info("[READ] COPYING BLOCK DATA:", "start", start, "end", end)
		slog.Info("[READ] BLOCK DATA:", "blockData", blockData[:32])
		slog.Info("[READ] DATA:", "data len", len(data))
		copy(data[start:end], blockData)
		//copy(data[consecutiveBlockStart:consecutiveBlockOffset], blockData)

		// Update the cache with the read data
		if vb.Cache.config.Size > 0 {
			for i := uint64(0); i < uint64(cb.NumBlocks); i++ {
				currentBlock := cb.StartBlock + uint64(i)
				vb.Cache.lru.Add(currentBlock, blockData[i*uint64(vb.BlockSize):(i+1)*uint64(vb.BlockSize)])
			}

		}

	}

	return data, zeroBlockErr

}

func (vb *VB) ReadAt(offset uint64, length uint64) ([]byte, error) {

	// First check the block exists in our volume size
	if offset > vb.Backend.GetVolumeSize() {
		return nil, RequestTooLarge
	}

	if offset+length > vb.Backend.GetVolumeSize() {
		return nil, RequestOutOfRange
	}

	blockSize := uint64(vb.BlockSize)

	// Calculate first and last block numbers
	firstBlock := offset / blockSize
	lastBlock := (offset + length - 1) / blockSize
	blockCount := lastBlock - firstBlock + 1

	// Read entire range of needed blocks
	fullData, err := vb.read(firstBlock, blockCount*blockSize)
	if err != nil && err != ZeroBlock {
		return nil, err
	}

	// Compute offset within the first block
	innerOffset := offset % blockSize
	return fullData[innerOffset : innerOffset+length], err
}

// Read reads a block from the storage backend for desired length

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

func clone(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
