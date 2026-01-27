// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package viperblock

import (
	"errors"
	"sync"
	"sync/atomic"
)

// BlockState represents the lifecycle state of a block in the unified store
type BlockState uint8

const (
	// BlockStateEmpty indicates a block that has never been written (zero block)
	BlockStateEmpty BlockState = iota
	// BlockStateHot indicates a block in the active write buffer (not yet WAL'd)
	BlockStateHot
	// BlockStatePending indicates a block that's WAL'd but awaiting backend upload
	BlockStatePending
	// BlockStatePersisted indicates a block stored on the backend storage
	BlockStatePersisted
	// BlockStateCached indicates a clean copy from backend in LRU cache
	BlockStateCached
)

// String returns a human-readable representation of the block state
func (s BlockState) String() string {
	switch s {
	case BlockStateEmpty:
		return "Empty"
	case BlockStateHot:
		return "Hot"
	case BlockStatePending:
		return "Pending"
	case BlockStatePersisted:
		return "Persisted"
	case BlockStateCached:
		return "Cached"
	default:
		return "Unknown"
	}
}

// BlockEntry represents a single block's metadata and data in the unified store
type BlockEntry struct {
	SeqNum       uint64     // Sequence number for ordering writes
	State        BlockState // Current state of the block
	Data         []byte     // Block data for Hot/Pending/Cached states
	ObjectID     uint64     // Object ID for Persisted state (which chunk file)
	ObjectOffset uint32     // Offset within the chunk file for Persisted state
}

// IndexShard is a single shard of the block index with its own lock
type IndexShard struct {
	mu      sync.RWMutex
	entries map[uint64]*BlockEntry
}

const (
	// NumShards is the number of shards in the index (must be power of 2)
	NumShards = 16
	// ShardMask is used for fast modulo operation (NumShards - 1)
	ShardMask = NumShards - 1
)

// UnifiedBlockStore provides O(1) block lookups with sharded locking
// to reduce contention. It replaces the 4 separate data structures
// (Writes, PendingBackendWrites, Cache, BlocksToObject) with a single
// unified index that is updated incrementally on writes.
type UnifiedBlockStore struct {
	shards    [NumShards]*IndexShard
	blockSize uint32
	seqNum    atomic.Uint64

	// Stats for monitoring
	stats BlockStoreStats
}

// BlockStoreStats tracks operational statistics
type BlockStoreStats struct {
	Reads      atomic.Uint64
	Writes     atomic.Uint64
	CacheHits  atomic.Uint64
	CacheMiss  atomic.Uint64
	HotReads   atomic.Uint64
	PendReads  atomic.Uint64
	BackendReads atomic.Uint64
}

// ErrBlockNotFound is returned when a block doesn't exist in the store
var ErrBlockNotFound = errors.New("block not found")

// NewUnifiedBlockStore creates a new unified block store with the given block size
func NewUnifiedBlockStore(blockSize uint32) *UnifiedBlockStore {
	ubs := &UnifiedBlockStore{
		blockSize: blockSize,
	}

	// Initialize all shards
	for i := 0; i < NumShards; i++ {
		ubs.shards[i] = &IndexShard{
			entries: make(map[uint64]*BlockEntry),
		}
	}

	return ubs
}

// getShard returns the shard for a given block number
func (ubs *UnifiedBlockStore) getShard(blockNum uint64) *IndexShard {
	return ubs.shards[blockNum&ShardMask]
}

// ReadBlock returns the block entry for a given block number
// This is the core O(1) lookup operation
func (ubs *UnifiedBlockStore) ReadBlock(blockNum uint64) (*BlockEntry, bool) {
	shard := ubs.getShard(blockNum)
	shard.mu.RLock()
	entry, exists := shard.entries[blockNum]
	shard.mu.RUnlock()
	ubs.stats.Reads.Add(1)
	return entry, exists
}

// ReadSingle is the fast path for single-block reads (most common case)
// Returns the block data, state, and any error
func (ubs *UnifiedBlockStore) ReadSingle(blockNum uint64) ([]byte, BlockState, error) {
	shard := ubs.getShard(blockNum)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	ubs.stats.Reads.Add(1)

	entry, exists := shard.entries[blockNum]
	if !exists {
		ubs.stats.CacheMiss.Add(1)
		return nil, BlockStateEmpty, ErrZeroBlock
	}

	switch entry.State {
	case BlockStateHot:
		ubs.stats.HotReads.Add(1)
		return entry.Data, entry.State, nil
	case BlockStatePending:
		ubs.stats.PendReads.Add(1)
		return entry.Data, entry.State, nil
	case BlockStateCached:
		ubs.stats.CacheHits.Add(1)
		return entry.Data, entry.State, nil
	case BlockStatePersisted:
		ubs.stats.BackendReads.Add(1)
		return nil, BlockStatePersisted, nil // Caller fetches from backend
	}

	ubs.stats.CacheMiss.Add(1)
	return nil, BlockStateEmpty, ErrZeroBlock
}

// Write stores a block in the Hot state and returns the assigned sequence number
// The index is updated immediately (not rebuilt on read)
func (ubs *UnifiedBlockStore) Write(blockNum uint64, data []byte) uint64 {
	seqNum := ubs.seqNum.Add(1)
	shard := ubs.getShard(blockNum)

	// Make copy of data to ensure ownership
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	shard.mu.Lock()
	entry, exists := shard.entries[blockNum]
	if !exists {
		entry = &BlockEntry{}
		shard.entries[blockNum] = entry
	}

	// Only update if this is a newer write (higher sequence number)
	if seqNum > entry.SeqNum {
		entry.SeqNum = seqNum
		entry.State = BlockStateHot
		entry.Data = dataCopy
		// Clear persisted info since we have new data
		entry.ObjectID = 0
		entry.ObjectOffset = 0
	}
	shard.mu.Unlock()

	ubs.stats.Writes.Add(1)
	return seqNum
}

// WriteWithSeqNum stores a block with a specific sequence number
// Used for WAL replay and state recovery
func (ubs *UnifiedBlockStore) WriteWithSeqNum(blockNum uint64, data []byte, seqNum uint64) {
	shard := ubs.getShard(blockNum)

	// Make copy of data
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	shard.mu.Lock()
	entry, exists := shard.entries[blockNum]
	if !exists {
		entry = &BlockEntry{}
		shard.entries[blockNum] = entry
	}

	// Only update if this is a newer write
	if seqNum > entry.SeqNum {
		entry.SeqNum = seqNum
		entry.State = BlockStateHot
		entry.Data = dataCopy
		entry.ObjectID = 0
		entry.ObjectOffset = 0
	}
	shard.mu.Unlock()
}

// MarkPending transitions a block from Hot to Pending state
// Called by Flush() after WAL write succeeds
func (ubs *UnifiedBlockStore) MarkPending(blockNum uint64) bool {
	shard := ubs.getShard(blockNum)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.entries[blockNum]
	if !ok {
		return false
	}

	if entry.State == BlockStateHot {
		entry.State = BlockStatePending
		return true
	}
	return false
}

// MarkPersisted transitions a block from Pending to Persisted state
// Called by createChunkFile() after backend upload succeeds
func (ubs *UnifiedBlockStore) MarkPersisted(blockNum, objectID uint64, offset uint32) bool {
	shard := ubs.getShard(blockNum)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.entries[blockNum]
	if !ok {
		return false
	}

	if entry.State == BlockStatePending {
		entry.State = BlockStatePersisted
		entry.ObjectID = objectID
		entry.ObjectOffset = offset
		entry.Data = nil // Release memory - data is now on backend
		return true
	}
	return false
}

// SetPersisted directly sets a block to Persisted state with object info
// Used for loading block mappings from checkpoints
func (ubs *UnifiedBlockStore) SetPersisted(blockNum, objectID uint64, offset uint32, seqNum uint64) {
	shard := ubs.getShard(blockNum)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.entries[blockNum]
	if !exists {
		entry = &BlockEntry{}
		shard.entries[blockNum] = entry
	}

	// Only update if this sequence number is newer or entry doesn't have data
	if seqNum >= entry.SeqNum || entry.State == BlockStateEmpty {
		entry.SeqNum = seqNum
		entry.State = BlockStatePersisted
		entry.ObjectID = objectID
		entry.ObjectOffset = offset
		entry.Data = nil
	}
}

// Cache transitions a block from Persisted to Cached state after backend read
func (ubs *UnifiedBlockStore) Cache(blockNum uint64, data []byte) bool {
	shard := ubs.getShard(blockNum)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.entries[blockNum]
	if !ok {
		return false
	}

	// Only cache if currently Persisted (not Hot or Pending which have newer data)
	if entry.State == BlockStatePersisted {
		// Make copy of data
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)

		entry.State = BlockStateCached
		entry.Data = dataCopy
		return true
	}
	return false
}

// EvictCache transitions a block from Cached back to Persisted state
// Used for LRU eviction
func (ubs *UnifiedBlockStore) EvictCache(blockNum uint64) bool {
	shard := ubs.getShard(blockNum)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.entries[blockNum]
	if !ok {
		return false
	}

	if entry.State == BlockStateCached {
		entry.State = BlockStatePersisted
		entry.Data = nil
		return true
	}
	return false
}

// GetBlocksByState returns all block numbers in a given state
// Used by Flush() to get all Hot blocks
func (ubs *UnifiedBlockStore) GetBlocksByState(state BlockState) []uint64 {
	var blocks []uint64

	for i := 0; i < NumShards; i++ {
		shard := ubs.shards[i]
		shard.mu.RLock()
		for blockNum, entry := range shard.entries {
			if entry.State == state {
				blocks = append(blocks, blockNum)
			}
		}
		shard.mu.RUnlock()
	}

	return blocks
}

// GetHotBlocks returns all blocks in Hot state with their data
// This is more efficient than GetBlocksByState + individual reads
func (ubs *UnifiedBlockStore) GetHotBlocks() []Block {
	var blocks []Block

	for i := 0; i < NumShards; i++ {
		shard := ubs.shards[i]
		shard.mu.RLock()
		for blockNum, entry := range shard.entries {
			if entry.State == BlockStateHot {
				blocks = append(blocks, Block{
					SeqNum: entry.SeqNum,
					Block:  blockNum,
					Len:    uint64(len(entry.Data)),
					Data:   entry.Data,
				})
			}
		}
		shard.mu.RUnlock()
	}

	return blocks
}

// GetPendingBlocks returns all blocks in Pending state with their data
func (ubs *UnifiedBlockStore) GetPendingBlocks() []Block {
	var blocks []Block

	for i := 0; i < NumShards; i++ {
		shard := ubs.shards[i]
		shard.mu.RLock()
		for blockNum, entry := range shard.entries {
			if entry.State == BlockStatePending {
				blocks = append(blocks, Block{
					SeqNum: entry.SeqNum,
					Block:  blockNum,
					Len:    uint64(len(entry.Data)),
					Data:   entry.Data,
				})
			}
		}
		shard.mu.RUnlock()
	}

	return blocks
}

// GetPersistedInfo returns object location info for a persisted block
func (ubs *UnifiedBlockStore) GetPersistedInfo(blockNum uint64) (objectID uint64, offset uint32, ok bool) {
	shard := ubs.getShard(blockNum)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entry, exists := shard.entries[blockNum]
	if !exists {
		return 0, 0, false
	}

	if entry.State == BlockStatePersisted || entry.State == BlockStateCached {
		return entry.ObjectID, entry.ObjectOffset, true
	}
	return 0, 0, false
}

// Count returns the total number of blocks across all shards
func (ubs *UnifiedBlockStore) Count() int {
	total := 0
	for i := 0; i < NumShards; i++ {
		shard := ubs.shards[i]
		shard.mu.RLock()
		total += len(shard.entries)
		shard.mu.RUnlock()
	}
	return total
}

// CountByState returns the count of blocks in each state
func (ubs *UnifiedBlockStore) CountByState() map[BlockState]int {
	counts := make(map[BlockState]int)

	for i := 0; i < NumShards; i++ {
		shard := ubs.shards[i]
		shard.mu.RLock()
		for _, entry := range shard.entries {
			counts[entry.State]++
		}
		shard.mu.RUnlock()
	}

	return counts
}

// Clear removes all entries from the store
func (ubs *UnifiedBlockStore) Clear() {
	for i := 0; i < NumShards; i++ {
		shard := ubs.shards[i]
		shard.mu.Lock()
		shard.entries = make(map[uint64]*BlockEntry)
		shard.mu.Unlock()
	}
}

// Delete removes a specific block from the store
func (ubs *UnifiedBlockStore) Delete(blockNum uint64) bool {
	shard := ubs.getShard(blockNum)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	_, exists := shard.entries[blockNum]
	if exists {
		delete(shard.entries, blockNum)
		return true
	}
	return false
}

// GetSeqNum returns the current sequence number
func (ubs *UnifiedBlockStore) GetSeqNum() uint64 {
	return ubs.seqNum.Load()
}

// SetSeqNum sets the sequence number (used for recovery)
func (ubs *UnifiedBlockStore) SetSeqNum(seqNum uint64) {
	ubs.seqNum.Store(seqNum)
}

// Stats returns a copy of the current statistics
func (ubs *UnifiedBlockStore) Stats() BlockStoreStats {
	return BlockStoreStats{
		Reads:        atomic.Uint64{},
		Writes:       atomic.Uint64{},
		CacheHits:    atomic.Uint64{},
		CacheMiss:    atomic.Uint64{},
		HotReads:     atomic.Uint64{},
		PendReads:    atomic.Uint64{},
		BackendReads: atomic.Uint64{},
	}
}

// GetStats returns the statistics values
func (ubs *UnifiedBlockStore) GetStats() (reads, writes, cacheHits, cacheMiss uint64) {
	return ubs.stats.Reads.Load(),
		ubs.stats.Writes.Load(),
		ubs.stats.CacheHits.Load(),
		ubs.stats.CacheMiss.Load()
}
