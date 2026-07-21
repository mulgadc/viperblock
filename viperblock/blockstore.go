package viperblock

import (
	"errors"
	"sync"
	"sync/atomic"
)

// BlockState represents the lifecycle state of a block in the unified store.
type BlockState uint8

const (
	// BlockStateEmpty indicates a block that has never been written (zero block).
	BlockStateEmpty BlockState = iota
	// BlockStateHot indicates a block in the active write buffer (not yet WAL'd).
	BlockStateHot
	// BlockStatePending indicates a block that's WAL'd but awaiting backend upload.
	BlockStatePending
	// BlockStatePersisted indicates a block stored on the backend storage.
	BlockStatePersisted
	// BlockStateCached indicates a clean copy from backend in LRU cache.
	BlockStateCached
)

// String returns a human-readable representation of the block state.
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

// BlockEntry represents a single block's metadata and data in the unified store.
type BlockEntry struct {
	SeqNum       uint64     // Sequence number for ordering writes
	State        BlockState // Current state of the block
	Data         []byte     // Block data for Hot/Pending/Cached states
	ObjectID     uint64     // Object ID for Persisted state (which chunk file)
	ObjectOffset uint32     // Offset within the chunk file for Persisted state
}

// IndexShard is a single shard of the block index with its own lock.
type IndexShard struct {
	mu      sync.RWMutex
	entries map[uint64]*BlockEntry
}

const (
	// NumShards is the number of shards in the index (must be power of 2).
	NumShards = 16
	// ShardMask is used for fast modulo operation (NumShards - 1).
	ShardMask = NumShards - 1
)

// UnifiedBlockStore provides O(1) block lookups with sharded locking
// to reduce contention. It replaces the 4 separate data structures
// (Writes, PendingBackendWrites, Cache, BlocksToObject) with a single
// unified index that is updated incrementally on writes.
//
// Persisted blocks are the exception to per-block sharding: to avoid growing
// the sharded index linearly with total volume data written, MarkPersistedRange
// removes the per-block shard entries for a run uploaded in one chunk and
// records a single coalesced persistedExtent covering it instead. Hot/Pending/
// Cached blocks stay in the sharded index, since those states are already
// bounded (WriteAtCtx backpressure, LRU cache size).
type UnifiedBlockStore struct {
	shards    [NumShards]*IndexShard
	blockSize uint32
	seqNum    atomic.Uint64

	// persistedMu guards persistedExtents. Separate from the per-shard locks:
	// a run's blocks are spread across many shards (shard = blockNum &
	// ShardMask), so no single shard lock could cover a whole extent.
	persistedMu      sync.RWMutex
	persistedExtents map[uint64]persistedExtent

	// Stats for monitoring
	stats BlockStoreStats
}

// persistedExtent mirrors BlockLookup for the UnifiedBlockStore's own
// coalesced index of Persisted-state blocks: one entry per consecutive run
// uploaded together in a chunk, instead of one BlockEntry per block. stride
// is the on-disk byte distance between two consecutive blocks in the run's
// chunk (BlockSize, or BlockSize+16 for encrypted volumes). seqNums carries
// each block's own write-generation SeqNum (index 0 == startBlock), since
// numerically consecutive blocks are not necessarily from the same guest
// write and the exact per-block value is needed to reconstruct the AEAD nonce.
type persistedExtent struct {
	startBlock   uint64
	numBlocks    uint16
	objectID     uint64
	objectOffset uint32
	stride       uint32
	seqNums      []uint64
}

// end returns the exclusive end block of the extent.
func (pe persistedExtent) end() uint64 {
	return pe.startBlock + uint64(pe.numBlocks)
}

// seqNumAt returns the SeqNum of the block at position i (0-based) within the extent.
func (pe persistedExtent) seqNumAt(i uint16) uint64 {
	if int(i) < len(pe.seqNums) {
		return pe.seqNums[i]
	}
	return 0
}

// offsetAt returns the ObjectOffset of the block at position i (0-based) within the extent.
func (pe persistedExtent) offsetAt(i uint16) uint32 {
	return pe.objectOffset + uint32(i)*pe.stride
}

// BlockStoreStats tracks operational statistics.
type BlockStoreStats struct {
	Reads        atomic.Uint64
	Writes       atomic.Uint64
	CacheHits    atomic.Uint64
	CacheMiss    atomic.Uint64
	HotReads     atomic.Uint64
	PendReads    atomic.Uint64
	BackendReads atomic.Uint64
}

// ErrBlockNotFound is returned when a block doesn't exist in the store.
var ErrBlockNotFound = errors.New("block not found")

// NewUnifiedBlockStore creates a new unified block store with the given block size.
func NewUnifiedBlockStore(blockSize uint32) *UnifiedBlockStore {
	ubs := &UnifiedBlockStore{
		blockSize:        blockSize,
		persistedExtents: make(map[uint64]persistedExtent),
	}

	// Initialize all shards
	for i := range NumShards {
		ubs.shards[i] = &IndexShard{
			entries: make(map[uint64]*BlockEntry),
		}
	}

	return ubs
}

// getShard returns the shard for a given block number.
func (ubs *UnifiedBlockStore) getShard(blockNum uint64) *IndexShard {
	return ubs.shards[blockNum&ShardMask]
}

// findPersistedExtentLocked returns the persistedExtent covering blockNum, if
// any. Callers must hold persistedMu (read or write). Checks the fast path
// first (blockNum is itself an extent's startBlock) before falling back to a
// scan, which is cheap since the index is O(extents), not O(blocks).
func (ubs *UnifiedBlockStore) findPersistedExtentLocked(blockNum uint64) (persistedExtent, bool) {
	if pe, ok := ubs.persistedExtents[blockNum]; ok {
		return pe, true
	}
	for _, pe := range ubs.persistedExtents {
		if blockNum > pe.startBlock && blockNum < pe.end() {
			return pe, true
		}
	}
	return persistedExtent{}, false
}

// readPersisted resolves blockNum against the persisted-extent index and
// returns it as a BlockEntry, matching the shape ReadEntry/ReadBlock hand
// back for shard-resident entries.
func (ubs *UnifiedBlockStore) readPersisted(blockNum uint64) (BlockEntry, bool) {
	ubs.persistedMu.RLock()
	pe, ok := ubs.findPersistedExtentLocked(blockNum)
	ubs.persistedMu.RUnlock()
	if !ok {
		return BlockEntry{}, false
	}
	idx := uint16(blockNum - pe.startBlock) //nolint:gosec // G115: findPersistedExtentLocked only returns pe when blockNum < pe.end(), so blockNum-pe.startBlock < pe.numBlocks (uint16)
	return BlockEntry{
		SeqNum:       pe.seqNumAt(idx),
		State:        BlockStatePersisted,
		ObjectID:     pe.objectID,
		ObjectOffset: pe.offsetAt(idx),
	}, true
}

// fractureOverlapsLocked removes or splits existing persisted extents that
// overlap [newStart, newStart+newNum), since the caller is about to insert a
// fresher extent covering that range. An overwrite can land anywhere inside a
// previously-coalesced extent, so every existing extent must be checked for
// range overlap and any surviving head/tail re-inserted under its own
// startBlock key. Callers must hold persistedMu (write).
func (ubs *UnifiedBlockStore) fractureOverlapsLocked(newStart uint64, newNum uint16) {
	newEnd := newStart + uint64(newNum)

	keys := make([]uint64, 0, len(ubs.persistedExtents))
	for k := range ubs.persistedExtents {
		keys = append(keys, k)
	}

	for _, key := range keys {
		existing := ubs.persistedExtents[key]
		exStart, exEnd := existing.startBlock, existing.end()
		if exEnd <= newStart || exStart >= newEnd {
			continue // no overlap
		}
		delete(ubs.persistedExtents, key)

		// Surviving head: [exStart, newStart)
		if exStart < newStart {
			headNum := newStart - exStart
			head := existing
			head.numBlocks = uint16(headNum) //nolint:gosec // G115: headNum = newStart-exStart < existing.numBlocks (uint16) since exStart < newStart < exEnd
			if len(existing.seqNums) > 0 {
				head.seqNums = append([]uint64(nil), existing.seqNums[:headNum]...)
			}
			ubs.persistedExtents[head.startBlock] = head
		}

		// Surviving tail: [newEnd, exEnd)
		if exEnd > newEnd {
			tailOffsetBlocks := newEnd - exStart
			tailNum := exEnd - newEnd
			tail := existing
			tail.startBlock = newEnd
			tail.numBlocks = uint16(tailNum)                                //nolint:gosec // G115: tailNum = exEnd-newEnd < existing.numBlocks (uint16) since newStart < newEnd < exEnd
			tail.objectOffset = existing.offsetAt(uint16(tailOffsetBlocks)) //nolint:gosec // G115: tailOffsetBlocks = newEnd-exStart < existing.numBlocks (uint16) since exStart < newEnd < exEnd
			if len(existing.seqNums) > 0 {
				tail.seqNums = append([]uint64(nil), existing.seqNums[tailOffsetBlocks:]...)
			}
			ubs.persistedExtents[tail.startBlock] = tail
		}
	}
}

// ReadBlock returns the block entry for a given block number
// This is the core O(1) lookup operation.
func (ubs *UnifiedBlockStore) ReadBlock(blockNum uint64) (*BlockEntry, bool) {
	shard := ubs.getShard(blockNum)
	shard.mu.RLock()
	entry, exists := shard.entries[blockNum]
	// Return an immutable snapshot, not the live pointer: the caller reads
	// (ObjectID, ObjectOffset, SeqNum) as a unit to rebuild the chunk nonce,
	// and a concurrent MarkPersisted mutating the live entry would otherwise
	// hand back a torn triple (old location, new seqNum) that fails AEAD.
	var snapshot BlockEntry
	if exists {
		snapshot = *entry
	}
	shard.mu.RUnlock()
	ubs.stats.Reads.Add(1)
	if exists {
		return &snapshot, true
	}

	// Not in the sharded index -- it may have been coalesced into the
	// persisted-extent index after upload. Resolve by range instead of
	// treating a miss here as "never written".
	if pe, ok := ubs.readPersisted(blockNum); ok {
		return &pe, true
	}
	return nil, false
}

// ReadSingle is the fast path for single-block reads (most common case)
// Returns the block data, state, and any error.
func (ubs *UnifiedBlockStore) ReadSingle(blockNum uint64) ([]byte, BlockState, error) {
	shard := ubs.getShard(blockNum)
	shard.mu.RLock()
	entry, exists := shard.entries[blockNum]
	var snapshot BlockEntry
	if exists {
		snapshot = *entry
	}
	shard.mu.RUnlock()

	ubs.stats.Reads.Add(1)

	if !exists {
		// Not in the sharded index -- it may have been coalesced into the
		// persisted-extent index after upload. Resolve by range instead of
		// treating a miss here as "never written".
		if _, ok := ubs.readPersisted(blockNum); ok {
			ubs.stats.BackendReads.Add(1)
			return nil, BlockStatePersisted, nil // Caller fetches from backend
		}
		ubs.stats.CacheMiss.Add(1)
		return nil, BlockStateEmpty, ErrZeroBlock
	}

	switch snapshot.State {
	case BlockStateHot:
		ubs.stats.HotReads.Add(1)
		return snapshot.Data, snapshot.State, nil
	case BlockStatePending:
		ubs.stats.PendReads.Add(1)
		return snapshot.Data, snapshot.State, nil
	case BlockStateCached:
		ubs.stats.CacheHits.Add(1)
		return snapshot.Data, snapshot.State, nil
	case BlockStatePersisted:
		ubs.stats.BackendReads.Add(1)
		return nil, BlockStatePersisted, nil // Caller fetches from backend
	}

	ubs.stats.CacheMiss.Add(1)
	return nil, BlockStateEmpty, ErrZeroBlock
}

// ReadEntry returns a full snapshot of a block's entry (state, data, and
// persisted location) captured under a single lock acquisition. Callers that
// need more than just the state — in particular the Persisted location — must
// use this instead of ReadSingle followed by a separate ReadBlock call: two
// separate lock acquisitions leave a window where a concurrent rewrite (guest
// WriteAt racing a chunk upload) changes the entry between the two reads,
// handing back a state from before the rewrite paired with a location/seqNum
// from after it. The read then reconstructs the wrong AEAD nonce from that
// torn pair and fails integrity ("cipher: message authentication failed").
// Higher chunk-upload concurrency widens this window, so a single atomic
// snapshot is required rather than narrowing it and hoping.
func (ubs *UnifiedBlockStore) ReadEntry(blockNum uint64) (BlockEntry, bool) {
	shard := ubs.getShard(blockNum)
	shard.mu.RLock()
	entry, exists := shard.entries[blockNum]
	var snapshot BlockEntry
	if exists {
		snapshot = *entry
	}
	shard.mu.RUnlock()

	ubs.stats.Reads.Add(1)
	if !exists {
		// Not in the sharded index -- may have been coalesced into the
		// persisted-extent index after upload. Resolve by range instead of
		// treating a miss as "never written".
		if pe, ok := ubs.readPersisted(blockNum); ok {
			ubs.stats.BackendReads.Add(1)
			return pe, true
		}
		ubs.stats.CacheMiss.Add(1)
		return snapshot, false
	}

	switch snapshot.State {
	case BlockStateHot:
		ubs.stats.HotReads.Add(1)
	case BlockStatePending:
		ubs.stats.PendReads.Add(1)
	case BlockStateCached:
		ubs.stats.CacheHits.Add(1)
	case BlockStatePersisted:
		ubs.stats.BackendReads.Add(1)
	default:
		ubs.stats.CacheMiss.Add(1)
	}
	return snapshot, true
}

// Write stores a block in the Hot state and returns the assigned sequence number
// The index is updated immediately (not rebuilt on read).
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
// Used for WAL replay and state recovery.
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
// Called by Flush() after WAL write succeeds.
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
// Called by createChunkFile() after backend upload succeeds. seqNum is the
// sequence number the chunk sealed this block under: the location and seqNum
// must be bound as a unit so the read path rebuilds the correct nonce.
func (ubs *UnifiedBlockStore) MarkPersisted(blockNum, objectID uint64, offset uint32, seqNum uint64) bool {
	shard := ubs.getShard(blockNum)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, ok := shard.entries[blockNum]
	if !ok {
		return false
	}

	// Only bind the location for the exact write this chunk sealed. A newer
	// re-write bumps entry.SeqNum (back to Hot); a stale drain completing here
	// must not staple its old chunk location onto the newer seqNum, or the
	// read reconstructs the wrong nonce and fails AEAD ("message
	// authentication failed"). seqNum can only equal the head or be older, so
	// an exact match means no newer write has superseded this one.
	if entry.State == BlockStatePending && seqNum == entry.SeqNum {
		entry.State = BlockStatePersisted
		entry.ObjectID = objectID
		entry.ObjectOffset = offset
		entry.SeqNum = seqNum
		entry.Data = nil // Release memory - data is now on backend
		return true
	}
	return false
}

// MarkPersistedRange transitions a consecutive run of blocks from Pending to
// Persisted in one call, coalescing eligible sub-runs into persistedExtent
// entries instead of leaving one BlockEntry per block, so the index stays
// O(extents) rather than O(blocks written). blocks and seqNums must be the
// same length, in ascending block-number order; seqNums[i] is the SeqNum
// blocks[i] was sealed under. objectOffset is the ObjectOffset of blocks[0];
// stride is the on-disk byte distance between consecutive blocks in the
// chunk. A block is only transitioned if still Pending with a matching
// SeqNum, so one superseded by a newer write is left untouched. Extent
// entries are inserted (pass 2) before shard entries are removed (pass 3),
// so no reader ever observes a block as unwritten mid-transition. Returns
// the number of blocks transitioned.
func (ubs *UnifiedBlockStore) MarkPersistedRange(blocks []uint64, objectID uint64, objectOffset uint32, stride uint32, seqNums []uint64) int {
	if len(blocks) == 0 || len(blocks) != len(seqNums) {
		return 0
	}

	// Pass 1 (read-only): predict which blocks are still eligible. A
	// concurrent write can invalidate a prediction before pass 3 finalizes
	// it; pass 3 re-checks under lock before actually removing anything.
	eligible := make([]bool, len(blocks))
	anyEligible := false
	for i, b := range blocks {
		shard := ubs.getShard(b)
		shard.mu.RLock()
		entry, ok := shard.entries[b]
		if ok && entry.State == BlockStatePending && entry.SeqNum == seqNums[i] {
			eligible[i] = true
			anyEligible = true
		}
		shard.mu.RUnlock()
	}
	if !anyEligible {
		return 0
	}

	// Pass 2: make the persisted location resolvable via the extent index
	// for every predicted-eligible, block-number-consecutive sub-run, before
	// pass 3 removes anything from the sharded index.
	ubs.persistedMu.Lock()
	i := 0
	for i < len(blocks) {
		if !eligible[i] {
			i++
			continue
		}
		j := i
		for j < len(blocks) && eligible[j] && j-i < 65535 && blocks[j] == blocks[i]+uint64(j-i) { //nolint:gosec // G115: j-i bounded by the 65535 loop guard before conversion
			j++
		}
		run := persistedExtent{
			startBlock:   blocks[i],
			numBlocks:    uint16(j - i), //nolint:gosec // G115: j-i < 65535 by the loop guard above
			objectID:     objectID,
			objectOffset: objectOffset + uint32(i)*stride,
			stride:       stride,
			seqNums:      append([]uint64(nil), seqNums[i:j]...),
		}
		ubs.fractureOverlapsLocked(run.startBlock, run.numBlocks)
		ubs.persistedExtents[run.startBlock] = run
		i = j
	}
	ubs.persistedMu.Unlock()

	// Pass 3: finalize by removing the per-block shard entries, re-checking
	// eligibility under lock since a concurrent rewrite may have superseded a
	// block after pass 1 sampled it. A raced block is left in the sharded
	// index as-is (newer write wins); its stale pass-2 extent entry is
	// corrected next time it is persisted.
	transitioned := 0
	for i, b := range blocks {
		if !eligible[i] {
			continue
		}
		shard := ubs.getShard(b)
		shard.mu.Lock()
		entry, ok := shard.entries[b]
		if ok && entry.State == BlockStatePending && entry.SeqNum == seqNums[i] {
			delete(shard.entries, b)
			transitioned++
		}
		shard.mu.Unlock()
	}

	ubs.stats.Writes.Add(uint64(transitioned))
	return transitioned
}

// SetPersisted directly sets a block to Persisted state with object info
// Used for loading block mappings from checkpoints.
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

// SetPersistedRange is SetPersisted's bulk, extent-coalescing counterpart: it
// installs a consecutive run of blocks as Persisted as a single
// persistedExtent, instead of one shard entry per block. Used to reconstruct
// BlockStore state from an on-disk checkpoint at load time. Unlike
// MarkPersistedRange, there is no prior shard state to validate — the
// checkpoint is authoritative, so this unconditionally installs the run,
// fracturing away any overlapping extent.
func (ubs *UnifiedBlockStore) SetPersistedRange(blocks []uint64, objectID uint64, objectOffset uint32, stride uint32, seqNums []uint64) {
	if len(blocks) == 0 || len(blocks) != len(seqNums) {
		return
	}

	ubs.persistedMu.Lock()
	defer ubs.persistedMu.Unlock()

	i := 0
	for i < len(blocks) {
		j := i + 1
		for j < len(blocks) && j-i < 65535 && blocks[j] == blocks[i]+uint64(j-i) { //nolint:gosec // G115: j-i bounded by the 65535 loop guard before conversion
			j++
		}
		run := persistedExtent{
			startBlock:   blocks[i],
			numBlocks:    uint16(j - i), //nolint:gosec // G115: j-i < 65535 by the loop guard above
			objectID:     objectID,
			objectOffset: objectOffset + uint32(i)*stride,
			stride:       stride,
			seqNums:      append([]uint64(nil), seqNums[i:j]...),
		}
		ubs.fractureOverlapsLocked(run.startBlock, run.numBlocks)
		ubs.persistedExtents[run.startBlock] = run
		i = j
	}
}

// Cache transitions a block from Persisted to Cached state after backend read.
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
// Used for LRU eviction.
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
// Used by Flush() to get all Hot blocks.
func (ubs *UnifiedBlockStore) GetBlocksByState(state BlockState) []uint64 {
	var blocks []uint64

	for i := range NumShards {
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
// This is more efficient than GetBlocksByState + individual reads.
func (ubs *UnifiedBlockStore) GetHotBlocks() []Block {
	var blocks []Block

	for i := range NumShards {
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

// GetPendingBlocks returns all blocks in Pending state with their data.
func (ubs *UnifiedBlockStore) GetPendingBlocks() []Block {
	var blocks []Block

	for i := range NumShards {
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

// GetPersistedInfo returns object location info for a persisted block.
func (ubs *UnifiedBlockStore) GetPersistedInfo(blockNum uint64) (objectID uint64, offset uint32, ok bool) {
	shard := ubs.getShard(blockNum)
	shard.mu.RLock()
	entry, exists := shard.entries[blockNum]
	var snapshot BlockEntry
	if exists {
		snapshot = *entry
	}
	shard.mu.RUnlock()

	if exists {
		if snapshot.State == BlockStatePersisted || snapshot.State == BlockStateCached {
			return snapshot.ObjectID, snapshot.ObjectOffset, true
		}
		return 0, 0, false
	}

	// Not in the sharded index -- resolve via the coalesced persisted-extent
	// index instead of treating a miss as "not persisted".
	if pe, ok := ubs.readPersisted(blockNum); ok {
		return pe.ObjectID, pe.ObjectOffset, true
	}
	return 0, 0, false
}

// Count returns the total number of blocks tracked, across the sharded index
// and the coalesced persisted-extent index.
func (ubs *UnifiedBlockStore) Count() int {
	total := 0
	for i := range NumShards {
		shard := ubs.shards[i]
		shard.mu.RLock()
		total += len(shard.entries)
		shard.mu.RUnlock()
	}

	ubs.persistedMu.RLock()
	for _, pe := range ubs.persistedExtents {
		total += int(pe.numBlocks)
	}
	ubs.persistedMu.RUnlock()

	return total
}

// PersistedExtentCount returns the number of coalesced extent entries in the
// persisted-extent index -- O(extents), not O(blocks persisted). Exposed for
// tests/metrics that need to observe the coalescing directly.
func (ubs *UnifiedBlockStore) PersistedExtentCount() int {
	ubs.persistedMu.RLock()
	defer ubs.persistedMu.RUnlock()
	return len(ubs.persistedExtents)
}

// CountByState returns the count of blocks in each state, across the sharded
// index and the coalesced persisted-extent index.
func (ubs *UnifiedBlockStore) CountByState() map[BlockState]int {
	counts := make(map[BlockState]int)

	for i := range NumShards {
		shard := ubs.shards[i]
		shard.mu.RLock()
		for _, entry := range shard.entries {
			counts[entry.State]++
		}
		shard.mu.RUnlock()
	}

	ubs.persistedMu.RLock()
	for _, pe := range ubs.persistedExtents {
		counts[BlockStatePersisted] += int(pe.numBlocks)
	}
	ubs.persistedMu.RUnlock()

	return counts
}

// Clear removes all entries from the store, including the persisted-extent index.
func (ubs *UnifiedBlockStore) Clear() {
	for i := range NumShards {
		shard := ubs.shards[i]
		shard.mu.Lock()
		shard.entries = make(map[uint64]*BlockEntry)
		shard.mu.Unlock()
	}

	ubs.persistedMu.Lock()
	ubs.persistedExtents = make(map[uint64]persistedExtent)
	ubs.persistedMu.Unlock()
}

// Delete removes a specific block from the store, whether it lives in the
// sharded index or as part of a coalesced persisted extent (in the latter
// case, fracturing that one block out of its extent).
func (ubs *UnifiedBlockStore) Delete(blockNum uint64) bool {
	shard := ubs.getShard(blockNum)
	shard.mu.Lock()
	_, exists := shard.entries[blockNum]
	if exists {
		delete(shard.entries, blockNum)
	}
	shard.mu.Unlock()
	if exists {
		return true
	}

	ubs.persistedMu.Lock()
	defer ubs.persistedMu.Unlock()
	if _, ok := ubs.findPersistedExtentLocked(blockNum); ok {
		ubs.fractureOverlapsLocked(blockNum, 1)
		return true
	}
	return false
}

// GetSeqNum returns the current sequence number.
func (ubs *UnifiedBlockStore) GetSeqNum() uint64 {
	return ubs.seqNum.Load()
}

// SetSeqNum sets the sequence number (used for recovery).
func (ubs *UnifiedBlockStore) SetSeqNum(seqNum uint64) {
	ubs.seqNum.Store(seqNum)
}

// Stats returns a copy of the current statistics.
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

// GetStats returns the statistics values.
func (ubs *UnifiedBlockStore) GetStats() (reads, writes, cacheHits, cacheMiss uint64) {
	return ubs.stats.Reads.Load(),
		ubs.stats.Writes.Load(),
		ubs.stats.CacheHits.Load(),
		ubs.stats.CacheMiss.Load()
}
