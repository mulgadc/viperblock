// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package viperblock

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
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

const DefaultBlockSize uint32 = 4096
const DefaultObjBlockSize uint32 = 1024 * 1024 * 4
const DefaultFlushInterval time.Duration = 5 * time.Second
const DefaultFlushSize uint32 = 64 * 1024 * 1024

type VBState struct {
	VolumeName string
	VolumeSize uint64

	BlockSize    uint32
	ObjBlockSize uint32

	SeqNum    uint64
	ObjectNum uint64
	WALNum    uint64

	BlockToObjectWALNum uint64

	Version uint16

	VolumeConfig VolumeConfig
}

type VB struct {
	VolumeName string
	VolumeSize uint64

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

	// BlockToObject WAL, stored on persistent storage for redundancy and periodic checkpointing
	BlockToObjectWAL WAL

	// In-memory cache of recently used blocks from read/write operations
	Cache Cache

	Version uint16

	ChunkMagic [4]byte

	Backend types.Backend

	BaseDir string

	VolumeConfig VolumeConfig
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
	Config CacheConfig
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

type BlockOptimised struct {
	SeqNum uint64
	Index  int
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

type BlocksMapOptimised map[uint64]BlockOptimised

type WAL struct {
	DB       []*os.File
	WallNum  atomic.Uint64
	BaseDir  string
	WALMagic [4]byte

	mu sync.RWMutex
}

// ChunkWork represents a unit of work for the worker pool
type ChunkWork struct {
	currentWALNum uint64
	chunkBuffer   []byte
	matchedBlocks []Block
}

type VolumeConfig struct {
	VolumeMetadata VolumeMetadata `json:"VolumeMetadata"`
	AMIMetadata    AMIMetadata    `json:"AMIMetadata"`
}

// Meta-data
type VolumeMetadata struct {
	VolumeID         string // e.g. "vol-0abcd1234ef567890"
	VolumeName       string // Optional name for UI or tagging
	TenantID         string // For multi-tenant support
	SizeGiB          uint64 // Volume size in GiB
	State            string // "creating", "available", "in-use", "deleted"
	CreatedAt        time.Time
	AvailabilityZone string            // Optional: "us-west-1a"
	AttachedInstance string            // Instance ID (if any)
	DeviceName       string            // e.g. "/dev/nbd1"
	VolumeType       string            // e.g. "gp3", "io1"
	IOPS             int               // For provisioned volumes
	Tags             map[string]string // User-defined metadata
	SnapshotID       string            // If created from a snapshot
	IsEncrypted      bool              // Encryption flag
}

type AMIMetadata struct {
	ImageID         string // e.g. "ami-0fbce8adcf7e5166f"
	Name            string // e.g. "debian-12-cloud"
	Description     string
	Architecture    string // "x86_64", "arm64"
	PlatformDetails string // "Linux/UNIX"
	CreationDate    time.Time
	RootDeviceType  string            // "ebs"
	Virtualization  string            // "hvm"
	ImageOwnerAlias string            // e.g. "hive"
	VolumeSizeGiB   uint64            // Size of the root image
	Tags            map[string]string // Metadata tags
}

// Error messages

var ZeroBlock = errors.New("zero block")
var RequestTooLarge = errors.New("request too large")
var RequestOutOfRange = errors.New("request out of range")
var RequestBlockSize = errors.New("request must be a multiple of block size")
var RequestBufferEmpty = errors.New("request requires a buffer > 0")

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
		vb.Cache.Config.Size = 0
		vb.Cache.Config.UseSystemMemory = false
		vb.Cache.Config.SystemMemoryPercent = 0
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
	vb.Cache.Config.Size = size

	if percentage > 0 {
		vb.Cache.Config.UseSystemMemory = true
		vb.Cache.Config.SystemMemoryPercent = percentage
	} else {
		vb.Cache.Config.UseSystemMemory = false
		vb.Cache.Config.SystemMemoryPercent = 0
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

func New(config VB, btype string, backendConfig interface{}) (vb *VB, err error) {
	var backend types.Backend

	// Volume name and size are set by the backend
	if config.VolumeName == "" || config.VolumeSize == 0 {
		return nil, fmt.Errorf("volume name and size must be set")
	}

	switch btype {
	case "file":
		//volumeName = backendConfig.(file.FileConfig).VolumeName
		//volumeSize = backendConfig.(file.FileConfig).VolumeSize
		backend = file.New(backendConfig)
	case "s3":
		//volumeName = backendConfig.(s3.S3Config).VolumeName
		//volumeSize = backendConfig.(s3.S3Config).VolumeSize
		backend = s3.New(backendConfig)
	}

	if config.BlockSize == 0 {
		config.BlockSize = DefaultBlockSize
	}

	if config.ObjBlockSize == 0 {
		config.ObjBlockSize = DefaultObjBlockSize
	}

	if config.BaseDir == "" {
		config.BaseDir = "/tmp/viperblock"
	}

	if config.FlushInterval == 0 {
		config.FlushInterval = DefaultFlushInterval
	}

	if config.FlushSize == 0 {
		config.FlushSize = DefaultFlushSize
	}

	var lruCache *lru.Cache[uint64, []byte]

	if config.Cache.Config.Size == 0 {
		//config.Cache.Config.Size = calculateCacheSize(config.BlockSize, 30)
		//config.Cache.Config.UseSystemMemory = true
		//config.Cache.Config.SystemMemoryPercent = 30
		config.Cache.Config.UseSystemMemory = false
		config.Cache.Config.SystemMemoryPercent = 0

	} else {

		// Create LRU cache with calculated size
		lruCache, err = lru.New[uint64, []byte](config.Cache.Config.Size)
		if err != nil {
			panic(fmt.Sprintf("failed to create LRU cache: %v", err))
		}

	}

	// Calculate initial cache size based on 30% of system memory
	//initialCacheSize := calculateCacheSize(config.BlockSize, 30)

	vb = &VB{
		VolumeName:       config.VolumeName,
		VolumeSize:       config.VolumeSize,
		BlockSize:        config.BlockSize,
		ObjBlockSize:     config.ObjBlockSize,
		FlushInterval:    config.FlushInterval,
		FlushSize:        config.FlushSize,
		Writes:           Blocks{},
		WAL:              WAL{BaseDir: config.BaseDir, WALMagic: [4]byte{'V', 'B', 'W', 'L'}},
		BlockToObjectWAL: WAL{BaseDir: config.BaseDir, WALMagic: [4]byte{'V', 'B', 'W', 'B'}},
		Cache: Cache{
			lru: lruCache,
			Config: CacheConfig{
				Size:                config.Cache.Config.Size,
				UseSystemMemory:     config.Cache.Config.UseSystemMemory,
				SystemMemoryPercent: config.Cache.Config.SystemMemoryPercent,
			},
		},
		Version: 1,

		ChunkMagic:     [4]byte{'V', 'B', 'C', 'H'},
		BlocksToObject: BlocksToObject{},
		Backend:        backend,
		BaseDir:        config.BaseDir,
		VolumeConfig:   config.VolumeConfig,
	}

	vb.BlocksToObject.BlockLookup = make(map[uint64]BlockLookup)

	vb.SetDebug(false)

	// Create the base directory if it doesn't exist
	os.MkdirAll(filepath.Join(vb.BaseDir, vb.GetVolume()), 0750)

	// Create the checkpoint directory if it doesn't exist
	os.MkdirAll(filepath.Join(vb.BaseDir, vb.GetVolume(), "checkpoints"), 0750)

	return vb, nil
}

func (vb *VB) SetDebug(debug bool) {

	var level slog.Level
	if debug {
		level = slog.LevelDebug
	} else {
		level = slog.LevelError
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))
	slog.SetDefault(logger)
}

func (vb *VB) SetWALBaseDir(baseDir string) {
	vb.WAL.BaseDir = baseDir
}

func (vb *VB) SetBlockWALBaseDir(baseDir string) {
	vb.BlockToObjectWAL.BaseDir = baseDir
}

// WAL functions
func (vb *VB) OpenWAL(wal *WAL, filename string) (err error) {

	// Lock operations on the WAL
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Create the directory if it doesn't exist
	os.MkdirAll(filepath.Dir(filename), 0750)

	// Create the file if it doesn't exist, make sure writes and committed immediately
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR|syscall.O_SYNC, 0640)

	// Append the WAL header, format
	// Check our type
	var headers []byte

	if wal.WALMagic == vb.WAL.WALMagic {
		// Write the WAL header
		headers = vb.WALHeader()
	} else if wal.WALMagic == vb.BlockToObjectWAL.WALMagic {
		// Write the BlockToObjectWAL header
		headers = vb.BlockToObjectWALHeader()
	} else {
		return fmt.Errorf("invalid WAL magic")
	}

	_, err = file.Write(headers)

	if err != nil {
		return err
	}

	// Append the latest "hot" WAL file to the DB
	wal.DB = append(wal.DB, file)

	slog.Debug("OpenWAL complete, new WAL", "file", *file)

	return err

}

func (vb *VB) WriteAt(offset uint64, data []byte) error {

	// First check the block exists in our volume size
	if offset > vb.GetVolumeSize() {
		return RequestTooLarge
	}

	// Check if the request is within range
	if offset+uint64(len(data)) > vb.GetVolumeSize() {
		return RequestOutOfRange
	}

	blockSize := uint64(vb.BlockSize)
	dataLen := uint64(len(data))

	// Request buffer must be > 0
	if dataLen == 0 {
		return RequestBufferEmpty
	}

	// Check blockLen a multiple of a blocksize
	// No longer required, WriteAt can handle different block sizes (default 4096)
	// Issue was with GRUB which requires 512 blocksize to write bootloader, ignorning the block size specified for the volume.
	//if blockLen%uint64(vb.BlockSize) != 0 {
	//	return RequestBlockSize
	//}

	startBlock := offset / blockSize
	endOffset := offset + dataLen
	endBlock := (endOffset - 1) / blockSize

	var writes []Block

	for b := startBlock; b <= endBlock; b++ {

		blockStart := b * blockSize
		blockEnd := blockStart + blockSize

		// Slice the range of data to write into this block
		writeStart := uint64(0)
		writeEnd := uint64(0)

		if offset > blockStart {
			// Support different blocksizes that do not match
			writeStart = offset - blockStart
		} else {
			writeStart = 0
		}

		if endOffset < blockEnd {
			writeEnd = endOffset - blockStart
		} else {
			writeEnd = blockSize
		}

		// Read existing block if partial write, else skip
		var blockData []byte
		if writeStart > 0 || writeEnd < blockSize {

			existing, err := vb.ReadAt(b*blockSize, blockSize)

			if err != nil && err != ZeroBlock {
				return fmt.Errorf("failed to read block %d for RMW: %w", b, err)
			}
			blockData = make([]byte, blockSize)
			copy(blockData, existing)
		} else {
			blockData = make([]byte, blockSize) // full overwrite
		}

		// Copy the relevant data into block buffer
		copy(blockData[writeStart:writeEnd], data[blockStart+writeStart-offset:blockStart+writeEnd-offset])

		writes = append(writes, Block{
			SeqNum: vb.SeqNum.Add(1),
			Block:  b,
			Len:    blockSize,
			Data:   blockData,
		})
	}

	// Thread-safe write into memory buffer
	vb.Writes.mu.Lock()
	vb.Writes.Blocks = append(vb.Writes.Blocks, writes...)
	vb.Writes.mu.Unlock()

	// Optionally update cache
	/*
		if vb.Cache.config.Size > 0 {
			for _, block := range writes {
				vb.Cache.lru.Add(block.Block, block.Data)
			}
		}
	*/

	return nil
}

func (vb *VB) Write(block uint64, data []byte) (err error) {

	blockLen := uint64(len(data))

	// First check the block exists in our volume size
	if block*uint64(vb.BlockSize) > vb.GetVolumeSize() {
		return RequestTooLarge
	}

	// Check if the request is within range
	if block*uint64(vb.BlockSize)+blockLen > vb.GetVolumeSize() {
		return RequestOutOfRange
	}

	// Check blockLen a multiple of a blocksize
	if blockLen%uint64(vb.BlockSize) != 0 {
		return RequestBlockSize
	}

	blockRequests := blockLen / uint64(vb.BlockSize)

	//slog.Info("\tVBWRITE:", "blockRequests", blockRequests, "block", block, "blockLen", blockLen)

	vb.Writes.mu.Lock()

	// Loop through each block request
	for i := uint64(0); i < blockRequests; i++ {

		currentBlock := block + i

		start := i * uint64(vb.BlockSize)
		end := start + uint64(vb.BlockSize)

		blockCopy := make([]byte, vb.BlockSize)
		copy(blockCopy, data[start:end])

		//slog.Info("\t\tBLOCKWRITE:", "currentBlock", currentBlock, "start", start, "end", end, "i", i)

		// Write raw data received, first to the main memory Block, which will be flushed to the WAL
		seqNum := vb.SeqNum.Add(1)

		vb.Writes.Blocks = append(vb.Writes.Blocks, Block{
			SeqNum: seqNum,
			Block:  currentBlock,
			Data:   blockCopy,
		})

		//slog.Info("WRITE:", "seqNum", seqNum, "BLOCK:", currentBlock, "start", start, "end", end)

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
				//slog.Info("REMAINING:", "block", b.Block, "seqnum", b.SeqNum)
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

	//slog.Info("PENDING BACKEND WRITES:", "len", len(vb.PendingBackendWrites.Blocks))
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
		//slog.Info("FLUSH:", "block", block.Block, "seqnum", block.SeqNum)

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

	// Pre-allocate record buffer (28 byte header + data)
	recordSize := 28 + len(block.Data)
	record := make([]byte, recordSize)

	vb.WAL.mu.Lock()

	// Get the current WAL file
	currentWAL := vb.WAL.DB[len(vb.WAL.DB)-1]

	//slog.Info("WRITE WAL:", "currentWAL", currentWAL)

	// Format for each block in the WAL
	// [seq_number, uint64][block_number, uint64][block_length, uint64][checksum, uint32][block_data, []byte]
	// big endian

	// Write the block number as big endian
	binary.BigEndian.PutUint64(record[0:8], block.SeqNum)

	//blockSeq := make([]byte, 8)
	//binary.BigEndian.PutUint64(blockSeq, block.SeqNum)
	//currentWAL.Write(blockSeq)

	// Write the block number as big endian
	binary.BigEndian.PutUint64(record[8:16], block.Block)

	//blockNumber := make([]byte, 8)
	//binary.BigEndian.PutUint64(blockNumber, block.Block)
	//slog.Info("WriteWAL > blockNumber", "original", block.Block, "converted", binary.BigEndian.Uint64(blockNumber))
	//currentWAL.Write(blockNumber)

	// TODO: Optimise
	// Write the block length as big endian
	binary.BigEndian.PutUint64(record[16:24], block.Len)

	//blockLength := make([]byte, 8)
	//binary.BigEndian.PutUint64(blockLength, uint64(len(block.Data)))
	//currentWAL.Write(blockLength)

	// Calculate a CRC32 checksum of the block data and headers
	checksum := crc32.ChecksumIEEE(record[0:24])
	checksum = crc32.Update(checksum, crc32.IEEETable, block.Data)

	// Write the checksum as big endian
	binary.BigEndian.PutUint32(record[24:28], checksum)

	//checksumBytes := make([]byte, 4)
	//binary.BigEndian.PutUint32(checksumBytes, checksum)
	//currentWAL.Write(checksumBytes)

	// Next, write the block data
	copy(record[28:], block.Data)

	// Optimistation, write a single time to reduce O_SYNC calls (slow) per finished block
	n, err := currentWAL.Write(record)

	vb.WAL.mu.Unlock()

	if n != recordSize {
		slog.Error("ERROR WRITING BLOCK TO WAL: incomplete write", "n", n, "expected", recordSize)
		return fmt.Errorf("incomplete write to WAL: wrote %d of %d bytes", n, recordSize)
	}

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

func (vb *VB) WriteBlockWAL(blocks *[]BlockLookup) (err error) {

	return

	vb.BlockToObjectWAL.mu.Lock()

	// Get the current WAL file
	currentWAL := vb.BlockToObjectWAL.DB[len(vb.BlockToObjectWAL.DB)-1]

	//slog.Info("Writing to Block WAL file", "filename", currentWAL.Name())

	// Format for each block in the BlockWAL
	// [start_block, uint64][num_blocks, uint16][object_id, uint64][object_offset, uint32][checksum, uint32]
	// big endian

	for _, block := range *blocks {
		//slog.Info("Writing block to BlockWAL", "block", block)

		data := vb.writeBlockWalChunk(&block)

		_, err := currentWAL.Write(data)

		if err != nil {
			slog.Error("ERROR WRITING BLOCK TO BLOCK WAL:", "error", err)
			vb.BlockToObjectWAL.mu.Unlock()
			return err
		}

	}

	vb.BlockToObjectWAL.mu.Unlock()

	// Cycle to the next Block WAL file
	// Create the Block WAL
	nextBlockWalNum := vb.BlockToObjectWAL.WallNum.Add(1)
	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, nextBlockWalNum, vb.GetVolume())))
	//	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s/wal/blocks/blocks.%08d.bin", vb.BlockToObjectWAL.BaseDir, vb.GetVolume(), nextBlockWalNum))
	if err != nil {
		slog.Error("ERROR OPENING BLOCK WAL:", "error", err)
		return err
	}

	return nil
}

func (vb *VB) writeBlockWalChunk(block *BlockLookup) (data []byte) {
	data = make([]byte, 26)

	// Write the start block
	binary.BigEndian.PutUint64(data[0:8], block.StartBlock)
	binary.BigEndian.PutUint16(data[8:10], block.NumBlocks)
	binary.BigEndian.PutUint64(data[10:18], block.ObjectID)
	binary.BigEndian.PutUint32(data[18:22], block.ObjectOffset)

	// Calculate a CRC32 checksum of the block data and headers
	checksum := crc32.ChecksumIEEE(data[0:22])

	binary.BigEndian.PutUint32(data[22:26], checksum)

	return data
}

func (vb *VB) readBlockWalChunk(data []byte) (block BlockLookup, err error) {

	block.StartBlock = binary.BigEndian.Uint64(data[:8])
	block.NumBlocks = binary.BigEndian.Uint16(data[8:10])
	block.ObjectID = binary.BigEndian.Uint64(data[10:18])
	block.ObjectOffset = binary.BigEndian.Uint32(data[18:22])

	checksum := binary.BigEndian.Uint32(data[22:26])

	// Validate checksum
	checksum_validated := crc32.ChecksumIEEE(data[:8])
	checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, data[8:10])
	checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, data[10:18])
	checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, data[18:22])

	if checksum_validated != checksum {
		slog.Error("Checksum mismatch", "checksum", checksum, "checksum_validated", checksum_validated)
		return block, fmt.Errorf("checksum mismatch")
	}

	return block, nil
}

func (vb *VB) WriteWALToChunk(force bool) error {
	// First, lock, and close the current WAL file
	vb.WAL.mu.Lock()
	currentWALNum := vb.WAL.WallNum.Load()
	pendingWAL := vb.WAL.DB[len(vb.WAL.DB)-1]

	// Check if we should write the chunk based on size
	if !force {
		fstat, err := pendingWAL.Stat()
		if err != nil {
			vb.WAL.mu.Unlock()
			return fmt.Errorf("could not validate WAL size: %v", err)
		}
		if fstat.Size() < int64(vb.ObjBlockSize) {
			vb.WAL.mu.Unlock()
			slog.Info("WAL is less than 4MB, skipping chunk write")
			return nil
		}
	}

	// Increment the WAL number and unlock
	nextWalNum := vb.WAL.WallNum.Add(1)
	vb.WAL.mu.Unlock()

	// Create the next WAL file
	err := vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, nextWalNum, vb.GetVolume())))
	//err := vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s/wal/chunks/wal.%08d.bin", vb.WAL.BaseDir, vb.GetVolume(), nextWalNum))
	if err != nil {
		return err
	}

	// Close and reopen the pending WAL for reading
	pendingWAL.Close()

	filename := fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, currentWALNum, vb.GetVolume()))
	//filename := fmt.Sprintf("%s/%s/wal/chunks/wal.%08d.bin", vb.WAL.BaseDir, vb.GetVolume(), currentWALNum)
	pendingWAL2, err := os.OpenFile(filename, os.O_RDWR, 0640)
	if err != nil {
		return err
	}
	defer pendingWAL2.Close()

	// Read and validate WAL header (magic, version, timestamp)
	headers := make([]byte, vb.WALHeaderSize())
	if _, err := pendingWAL2.Read(headers); err != nil {
		return fmt.Errorf("error reading WAL headers: %v", err)
	}

	if !bytes.Equal(headers[:4], vb.WAL.WALMagic[:]) {
		return fmt.Errorf("magic mismatch")
	}

	if binary.BigEndian.Uint16(headers[4:6]) != vb.Version {
		return fmt.Errorf("version mismatch")
	}

	// Read blocks from WAL, pre-allocate the estimated number of blocks that could be in the chunk
	blocks := make([]Block, 0, (vb.ObjBlockSize/vb.BlockSize)+1)

	for {
		data := make([]byte, 28+vb.BlockSize)
		_, err := pendingWAL2.Read(data)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error reading WAL data: %v", err)
		}

		// Validate checksum
		checksum := binary.BigEndian.Uint32(data[24:28])

		checksumValidated := crc32.ChecksumIEEE(data[:24])
		// Skip the checksum (24:28), just the data next
		checksumValidated = crc32.Update(checksumValidated, crc32.IEEETable, data[28:])

		if checksumValidated != checksum {
			return fmt.Errorf("checksum mismatch in WriteWALToChunk")
		}

		blocks = append(blocks, Block{
			SeqNum: binary.BigEndian.Uint64(data[:8]),
			Block:  binary.BigEndian.Uint64(data[8:16]),
			Len:    binary.BigEndian.Uint64(data[16:24]),
			Data:   data[28:],
		})
	}

	// Deduplicate and sort blocks
	blocksMap := make(BlocksMapOptimised, len(blocks))
	for index, block := range blocks {
		if existing, ok := blocksMap[block.Block]; !ok || existing.SeqNum < block.SeqNum {
			blocksMap[block.Block] = BlockOptimised{
				SeqNum: block.SeqNum,
				Index:  index,
			}
		}
	}

	sortedBlocks := make([]*Block, 0, len(blocksMap))
	for _, block := range blocksMap {
		sortedBlocks = append(sortedBlocks, &blocks[block.Index])
	}
	sort.Slice(sortedBlocks, func(i, j int) bool { return sortedBlocks[i].Block < sortedBlocks[j].Block })

	var chunkBuffer = make([]byte, 0, vb.ObjBlockSize)
	var matchedBlocks = make([]Block, 0)

	for _, block := range sortedBlocks {
		chunkBuffer = append(chunkBuffer, block.Data...)
		matchedBlocks = append(matchedBlocks, Block{
			SeqNum: block.SeqNum,
			Block:  block.Block,
		})

		// If buffer is full (default 4MB), write to file
		if len(chunkBuffer) >= int(vb.ObjBlockSize) {
			err := vb.createChunkFile(currentWALNum, vb.ObjectNum.Load(), &chunkBuffer, &matchedBlocks)
			if err != nil {
				slog.Error("Failed to create chunk file", "error", err)
				//vb.WAL.mu.Unlock()

				return err
			}

			chunkBuffer = chunkBuffer[:0] // Reset bufferAdd commentMore actions
			matchedBlocks = make([]Block, 0)
		}

	}

	// Write any remaining data as the last chunkAdd commentMore actions
	if len(chunkBuffer) > 0 {
		err := vb.createChunkFile(currentWALNum, vb.ObjectNum.Load(), &chunkBuffer, &matchedBlocks)
		if err != nil {
			slog.Error("Failed to create chunk file", "error", err)

			return err
		}

	}

	return nil
}

// Create work channel and result channel
/*
		workChan := make(chan ChunkWork, runtime.NumCPU())
		resultChan := make(chan error, runtime.NumCPU())

		// Create worker pool
		numWorkers := runtime.NumCPU()
		var wg sync.WaitGroup
		wg.Add(numWorkers)

		// Start workers
		for i := 0; i < numWorkers; i++ {
			go func() {
				defer wg.Done()
				for work := range workChan {
					err := vb.createChunkFile(work.currentWALNum, vb.ObjectNum.Load(), &work.chunkBuffer, &work.matchedBlocks)
					resultChan <- err
				}
			}()
		}

		// Prepare chunks and send to workers
		chunkBuffer := make([]byte, 0, vb.ObjBlockSize)
		matchedBlocks := make([]Block, 0, len(sortedBlocks))
		chunkIndex := uint64(0)

		for _, block := range sortedBlocks {
			chunkBuffer = append(chunkBuffer, block.Data...)
			matchedBlocks = append(matchedBlocks, Block{
				SeqNum: block.SeqNum,
				Block:  block.Block,
			})

			if len(chunkBuffer) >= int(vb.ObjBlockSize) {
				// Create copies for the worker
				chunkBufferCopy := make([]byte, len(chunkBuffer))
				copy(chunkBufferCopy, chunkBuffer)
				matchedBlocksCopy := make([]Block, len(matchedBlocks))
				copy(matchedBlocksCopy, matchedBlocks)

				workChan <- ChunkWork{
					currentWALNum: currentWALNum,
					chunkBuffer:   chunkBufferCopy,
					matchedBlocks: matchedBlocksCopy,
				}

				chunkBuffer = chunkBuffer[:0]
				matchedBlocks = matchedBlocks[:0]
				chunkIndex++
			}
		}

		// Handle remaining data
		if len(chunkBuffer) > 0 {
			chunkBufferCopy := make([]byte, len(chunkBuffer))
			copy(chunkBufferCopy, chunkBuffer)
			matchedBlocksCopy := make([]Block, len(matchedBlocks))
			copy(matchedBlocksCopy, matchedBlocks)

			workChan <- ChunkWork{
				currentWALNum: currentWALNum,
				chunkBuffer:   chunkBufferCopy,
				matchedBlocks: matchedBlocksCopy,
			}
		}

		// Close work channel and wait for workers
		close(workChan)
		wg.Wait()
		close(resultChan)

		// Check for errors
		for err := range resultChan {
			if err != nil {
				return err
			}
		}


	return nil
}
*/

func (vb *VB) createChunkFile(currentWALNum uint64, chunkIndex uint64, chunkBuffer *[]byte, matchedBlocks *[]Block) (err error) {

	//runtime.LockOSThread()
	//defer runtime.UnlockOSThread()

	//slog.Info("Creating chunk file", "chunkIndex", chunkIndex, "currentWALNum", currentWALNum)

	headers := vb.ChunkHeader()

	err = vb.Backend.Write(types.FileTypeChunk, chunkIndex, &headers, chunkBuffer)
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

			if vb.Cache.Config.Size > 0 {
				vb.Cache.lru.Add(block.Block, block.Data)
			}
		}
	}

	vb.PendingBackendWrites.mu.Unlock()

	headerLen := len(headers)

	// Loop through the chunk buffer, and write each block to the file
	i := 0

	BlockObjectsToWAL := make([]BlockLookup, 0, len(*matchedBlocks))

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

		newBlock := BlockLookup{
			StartBlock:   block.Block,
			NumBlocks:    uint16(numBlocks),
			ObjectID:     chunkIndex,
			ObjectOffset: uint32(headerLen + (i * int(vb.BlockSize))),
		}

		// TODO: Optimise for number of consecutive blocks to reduce the memory size
		vb.BlocksToObject.BlockLookup[block.Block] = newBlock

		//slog.Info("Added block to BlockLookup", "block", block.Block, "newBlock", newBlock)

		BlockObjectsToWAL = append(BlockObjectsToWAL, newBlock)

		i++

	}

	vb.BlocksToObject.mu.Unlock()

	// Lastly, write the Block objects to it's own WAL for redundancy and checkpointing.
	err = vb.WriteBlockWAL(&BlockObjectsToWAL)
	if err != nil {
		return err
	}

	// Increment the object number
	vb.ObjectNum.Add(1)

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

func (vb *VB) SaveBlockState() (err error) {

	vb.BlocksToObject.mu.RLock()
	defer vb.BlocksToObject.mu.RUnlock()

	//checkpoint := []byte{}

	// Write the BlocksToObject to a file as a binary file
	/*
		file, err := os.Create(filename)
		if err != nil {
			return err
		}
		defer file.Close()
	*/

	// Write the BlocksToObject to the file as binary
	// Loop through each block

	checkpoint := vb.BlockToObjectWALHeader()

	//file.Write(header)

	for _, block := range vb.BlocksToObject.BlockLookup {

		checkpoint = append(checkpoint, vb.writeBlockWalChunk(&block)...)

		if err != nil {
			slog.Error("ERROR WRITING BLOCK TO BLOCK WAL:", "error", err)
			return err
		}

	}

	filepath := fmt.Sprintf("%s/%s", vb.BaseDir, types.GetFilePath(types.FileTypeBlockCheckpoint, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))
	file, err := os.Create(filepath)

	if err != nil {
		return err
	}

	defer file.Close()

	// Write the file locally
	file.Write(checkpoint)

	headers := []byte{}

	// Next, upload the file to the backend
	err = vb.Backend.Write(types.FileTypeBlockCheckpoint, vb.BlockToObjectWAL.WallNum.Load(), &headers, &checkpoint)
	if err != nil {
		return err
	}

	// Increment the Block WAL sequence number
	vb.BlockToObjectWAL.WallNum.Add(1)

	return err

}

// Load the previous blockstate from disk
func (vb *VB) LoadBlockState() (err error) {

	var checkpoint []byte

	// Step 1. Validate the local persistant disk contains the state
	filename := fmt.Sprintf("%s/%s", vb.BaseDir, types.GetFilePath(types.FileTypeBlockCheckpoint, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))

	_, err = os.Stat(filename)
	if err != nil {
		slog.Info("No state found in local file, using backend state", "error", err)

		// Open the latest checkpoint from the backend
		checkpoint, err = vb.Backend.Read(types.FileTypeBlockCheckpoint, vb.BlockToObjectWAL.WallNum.Load(), 0, 0)

		if err != nil {
			// If no file found, volume is empty, return nil
			return nil
		}

	} else {
		checkpoint, err = os.ReadFile(filename)
		if err != nil {
			return err
		}
	}

	// TODO: Rebuild the checkpoint if corrupted or missing based on previous WAL

	// Step 2. Build the BlockLookup from the binary checkpoint

	vb.BlocksToObject.mu.Lock()
	defer vb.BlocksToObject.mu.Unlock()

	vb.BlocksToObject.BlockLookup = make(map[uint64]BlockLookup, 0)

	slog.Debug("Loaded checkpoint", "checkpoint", checkpoint)

	headers := checkpoint[:vb.BlockToObjectWALHeaderSize()]

	// Validate the header
	//if !bytes.Equal(headers, vb.BlockToObjectWALHeader()) {
	//	return fmt.Errorf("invalid header")
	//}

	// Check the magic
	if !bytes.Equal(headers[:4], vb.BlockToObjectWAL.WALMagic[:]) {
		return fmt.Errorf("magic mismatch")
	}

	if binary.BigEndian.Uint16(headers[4:6]) != vb.Version {
		return fmt.Errorf("version mismatch")
	}

	// Check file written within the last 2 seconds
	// TODO: Move to _test.go
	//now := time.Now().Unix()
	//writtenAt := binary.BigEndian.Uint64(headers[6:14])
	//if now-int64(writtenAt) > 2 {
	//	return fmt.Errorf("file written more than 2 seconds ago")
	//}

	// Check the wall number

	// Read each block from the checkpoint buffer (26 bytes each)
	offset := vb.BlockToObjectWALHeaderSize()

	for offset < len(checkpoint) {

		// TODO: Improve offset calculation, not specific to 26 bytes
		block, err := vb.readBlockWalChunk(checkpoint[offset : offset+26])

		if err != nil {
			slog.Error("Error reading block", "error", err)
			return err
		}

		vb.BlocksToObject.BlockLookup[block.StartBlock] = block

		offset += 26

	}

	return err
}

// Lookup Block to Object
func (vb *VB) LookupBlockToObject(block uint64) (objectID uint64, objectOffset uint32, err error) {

	slog.Debug("LookupBlockToObject", "block", block)

	vb.BlocksToObject.mu.RLock()

	blockLookup, ok := vb.BlocksToObject.BlockLookup[block]

	vb.BlocksToObject.mu.RUnlock()

	slog.Debug("\tLOOKUP BLOCK TO OBJECT:", "block", block, "blockLookup", blockLookup)

	if ok {
		return blockLookup.ObjectID, blockLookup.ObjectOffset, nil
	} else {
		return 0, 0, ZeroBlock
	}

}

// Save the block tracking state to disk
func (vb *VB) SaveState() error {
	vb.BlocksToObject.mu.Lock()
	defer vb.BlocksToObject.mu.Unlock()

	state := VBState{
		VolumeName:          vb.VolumeName,
		VolumeSize:          vb.VolumeSize,
		BlockSize:           vb.BlockSize,
		ObjBlockSize:        vb.ObjBlockSize,
		SeqNum:              vb.SeqNum.Load(),
		ObjectNum:           vb.ObjectNum.Load(),
		WALNum:              vb.WAL.WallNum.Load(),
		BlockToObjectWALNum: vb.BlockToObjectWAL.WallNum.Load(),
		Version:             vb.Version,
		VolumeConfig:        vb.VolumeConfig,
	}

	// Save as JSON
	jsonData, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// First, write to the local persistant disk
	filename := fmt.Sprintf("%s/%s", vb.BaseDir, types.GetFilePath(types.FileTypeConfig, 0, vb.GetVolume()))
	err = os.WriteFile(filename, jsonData, 0640)

	if err != nil {
		return err
	}

	// Next, write to the backend
	headers := []byte{}
	err = vb.Backend.Write(types.FileTypeConfig, 0, &headers, &jsonData)
	if err != nil {
		return err
	}

	return nil
}

// Load the block tracking state from disk
func (vb *VB) LoadState() error {

	// Step 1. Query the state locally
	state, err := vb.LoadStateRequest(fmt.Sprintf("%s/%s", vb.BaseDir, types.GetFilePath(types.FileTypeConfig, 0, vb.GetVolume())))
	if err != nil {
		slog.Info("No state found in local file, using backend state", "error", err)
	}

	//slog.Info("Loaded state", "state", state)

	// Step 2. Query the state from the backend
	stateBackend, err := vb.LoadStateRequest("")

	//slog.Info("Loaded backend state", "state", stateBackend)

	if err != nil {
		slog.Warn("No state found in backend, using local state", "error", err)
	}

	if stateBackend.BlockSize == 0 && state.BlockSize == 0 {
		errMsg := "Invalid state, block size or object block size is 0. Not syncing config"
		slog.Error(errMsg)
		err = errors.New(errMsg)
		return err
	}

	// Step 3. Compare the two states, the state with the highest SeqNum is the correct state
	if stateBackend.SeqNum > state.SeqNum {
		state = stateBackend
	}

	vb.VolumeName = state.VolumeName
	vb.VolumeSize = state.VolumeSize
	vb.BlockSize = state.BlockSize
	vb.ObjBlockSize = state.ObjBlockSize
	vb.SeqNum.Store(state.SeqNum)
	vb.ObjectNum.Store(state.ObjectNum)
	vb.WAL.WallNum.Store(state.WALNum)
	vb.BlockToObjectWAL.WallNum.Store(state.BlockToObjectWALNum)

	vb.Version = state.Version

	slog.Debug("Loaded state", "state", state)

	return nil
}

// Query the local state from file or the backend
func (vb *VB) LoadStateRequest(filename string) (state VBState, err error) {

	var jsonData []byte

	// Read from file
	if filename != "" {
		jsonData, err = os.ReadFile(filename)
		if err != nil {
			return state, err
		}
	} else {

		jsonData, err = vb.Backend.Read(types.FileTypeConfig, 0, 0, 0)
		if err != nil {
			return state, err
		}

	}

	// Parse JSON
	err = json.Unmarshal(jsonData, &state)

	return state, err

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
			//slog.Info("[READ] HOT BLOCK:", "block", wr.Block, "seqnum", wr.SeqNum)

			copy(data[start:end], clone(wr.Data))
			continue
		}

		// Next, check the pending backend writes buffer
		if lp, ok := latestPendingWrites[currentBlock]; ok {
			//slog.Info("[READ] PENDING BLOCK:", "block", lp.Block, "seqnum", lp.SeqNum)

			copy(data[start:end], clone(lp.Data))
			continue
		}

		// Next query the LRU cache if the data does not exist in the HOT write path, or pending write buffer.
		if vb.Cache.Config.Size > 0 {
			if cachedData, ok := vb.Cache.lru.Get(currentBlock); ok {
				//slog.Info("[READ] LRU CACHE BLOCK:", "block", currentBlock)

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

		blockData, err := vb.Backend.Read(types.FileTypeChunk, cb.ObjectID, cb.ObjectOffset, consecutiveBlockOffset)
		if err != nil {
			return nil, err
		}

		slog.Info("[READ] COPYING BLOCK DATA:", "start", start, "end", end)
		slog.Info("[READ] BLOCK DATA:", "blockData", blockData[:32])
		slog.Info("[READ] DATA:", "data len", len(data))
		copy(data[start:end], blockData)
		//copy(data[consecutiveBlockStart:consecutiveBlockOffset], blockData)

		// Update the cache with the read data
		if vb.Cache.Config.Size > 0 {
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
	if offset > vb.GetVolumeSize() {
		return nil, RequestTooLarge
	}

	if offset+length > vb.GetVolumeSize() {
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

func (vb *VB) Close() error {

	slog.Info("VB Close, flushing block state to disk")

	vb.Flush()
	err := vb.WriteWALToChunk(true)

	if err != nil {
		slog.Error("Could not Write WAL to Chunk", "err", err)
		return err
	}

	path := fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, vb.GetVolume())

	slog.Debug("Saving Close state to", "path", path)

	// Upload the state to the backend
	err = vb.SaveState()

	if err != nil {
		slog.Error("Could not save state", "err", err)
		return err
	}

	// Should not be required with Flush and WriteWALToChunk prior
	/*
		err = vb.SaveHotState(fmt.Sprintf("%s/hotstate.json", path))

		if err != nil {
			slog.Error("Could not save hot state", "err", err)
			return err
		}
	*/

	err = vb.SaveBlockState()

	if err != nil {
		slog.Error("Could not save block state", "err", err)
		return err
	}

	return nil

}

// Remove local WAL and block state files, connection must be closed first.
func (vb *VB) RemoveLocalFiles() (err error) {

	localPath := filepath.Join(vb.BaseDir, vb.GetVolume())

	slog.Info("Removing local files", "path", localPath)

	vb.WAL.mu.Lock()
	err = os.RemoveAll(localPath)
	vb.WAL.mu.Unlock()

	return err

}

func (vb *VB) GetVolumeSize() uint64 {
	return vb.VolumeSize
}

func (vb *VB) GetVolume() string {
	return vb.VolumeName
}

func (vb *VB) Reset() error {

	vb.BlocksToObject.mu.Lock()
	vb.BlocksToObject.BlockLookup = make(map[uint64]BlockLookup, 0)
	vb.BlocksToObject.mu.Unlock()

	vb.Writes.mu.Lock()
	vb.Writes.Blocks = make([]Block, 0)
	vb.Writes.mu.Unlock()

	vb.PendingBackendWrites.mu.Lock()
	vb.PendingBackendWrites.Blocks = make([]Block, 0)
	vb.PendingBackendWrites.mu.Unlock()

	vb.Cache.lru.Purge()

	// Reset SeqNum and ObjectNum
	vb.SeqNum.Store(0)
	vb.ObjectNum.Store(0)

	// Reset WAL
	vb.WAL.mu.Lock()
	vb.WAL.WallNum.Store(0)
	vb.WAL.mu.Unlock()

	// Reset BlockWAL
	vb.BlockToObjectWAL.mu.Lock()
	vb.BlockToObjectWAL.WallNum.Store(0)
	vb.BlockToObjectWAL.mu.Unlock()

	return nil
}

// Read reads a block from the storage backend for desired length

func (vb *VB) WALHeader() []byte {
	header := make([]byte, vb.WALHeaderSize())
	copy(header[:len(vb.WAL.WALMagic)], vb.WAL.WALMagic[:])
	binary.BigEndian.PutUint16(header[4:6], vb.Version)
	binary.BigEndian.PutUint32(header[6:10], vb.BlockSize)
	binary.BigEndian.PutUint64(header[10:18], uint64(time.Now().Unix()))
	return header
}

// WALHeaderSize returns the size of the WAL header in bytes
func (vb *VB) WALHeaderSize() int {
	// Magic bytes (4) + Version (2) + BlockSize (4) + Timestamp (8)
	return len(vb.WAL.WALMagic) + binary.Size(vb.Version) + binary.Size(vb.BlockSize) + binary.Size(time.Now().Unix())
}

func (vb *VB) BlockToObjectWALHeader() []byte {
	header := make([]byte, vb.BlockToObjectWALHeaderSize())

	slog.Info("Writing BlockToObjectWALHeader", "header", header, "size", vb.BlockToObjectWALHeaderSize())
	copy(header[:len(vb.BlockToObjectWAL.WALMagic)], vb.BlockToObjectWAL.WALMagic[:])
	binary.BigEndian.PutUint16(header[4:6], vb.Version)
	binary.BigEndian.PutUint64(header[6:14], uint64(time.Now().Unix()))
	return header
}

func (vb *VB) BlockToObjectWALHeaderSize() int {
	// Magic bytes (4) + Version (2) + Timestamp (8)
	return len(vb.BlockToObjectWAL.WALMagic) + binary.Size(vb.Version) + binary.Size(time.Now().Unix())
}

func (vb *VB) ChunkHeader() []byte {
	header := make([]byte, vb.ChunkHeaderSize())
	copy(header[:len(vb.ChunkMagic)], vb.ChunkMagic[:])
	binary.BigEndian.PutUint16(header[4:6], vb.Version)
	binary.BigEndian.PutUint32(header[6:10], vb.BlockSize)
	return header
}

func (vb *VB) ChunkHeaderSize() int {
	return len(vb.ChunkMagic) + binary.Size(vb.Version) + binary.Size(vb.BlockSize)
}

func clone(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func GenerateVolumeID(voltype, name, bucket string, timestamp int64) string {
	// Combine the fields
	input := fmt.Sprintf("%-s-%s-%s-%d", voltype, name, bucket, timestamp)

	// Create SHA-256 hash
	hash := sha256.Sum256([]byte(input))

	// Convert first 17 characters of hex
	shortHash := hex.EncodeToString(hash[:])[:17]

	return fmt.Sprintf("%s-%s", voltype, shortHash)
}

// FindFreePort allocates a free TCP port from the OS
func FindFreePort() (string, error) {
	l, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}
