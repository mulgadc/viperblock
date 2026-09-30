# Viperblock Performance Optimisations

## Problem Statement

Viperblock exhibits poor write performance in two specific scenarios:

1. **WSL (Windows Subsystem for Linux)**: 4KB writes are extremely slow due to filesystem passthrough overhead
2. **NVIDIA Jetson with SD Card**: Slow random I/O on flash storage with poor 4KB write performance

The problematic code pattern:
```go
vb.WriteAt(block*uint64(vb.BlockSize), buf[:n])
block++

// Flush every 4MB
if block%uint64(vb.BlockSize) == 0 {
    fmt.Println("Flush", "block", block)
    vb.Flush()
    vb.WriteWALToChunk(true)
}
```

---

## Root Cause Analysis

### Current Write Path (I/O Operations Per Block)

| Stage | Operation | Sync Behaviour | I/O Count |
|-------|-----------|----------------|-----------|
| WriteAt() | Memory buffer only | None | 0 |
| Flush() → WriteWAL() | 5 separate write() calls | **O_SYNC per call** | 5 |
| WriteWALToChunk() | Read WAL + Write chunk | Backend-dependent | 2+ |

**Critical Issue: `O_SYNC` on WAL File (viperblock.go:442)**

```go
file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR|syscall.O_SYNC, 0640)
```

With `O_SYNC`, every `write()` system call blocks until data hits the physical storage. For each block written to WAL:

1. Write sequence number (8 bytes) → **disk sync**
2. Write block number (8 bytes) → **disk sync**
3. Write block length (8 bytes) → **disk sync**
4. Write CRC32 checksum (4 bytes) → **disk sync**
5. Write block data (4096 bytes) → **disk sync**

**Result**: 5 forced disk syncs per 4KB block = ~50-200ms per block on slow storage

### Why This Hurts WSL and SD Cards

| Platform | Issue | Impact |
|----------|-------|--------|
| **WSL** | 9P filesystem passthrough adds ~5-10ms per sync | 5 syncs × 10ms = 50ms/block minimum |
| **SD Card** | Flash write latency 2-10ms, wear leveling overhead | Random 4KB writes are worst-case scenario |
| **Both** | No write coalescing or batching | Every block = separate I/O storm |

---

## Proposed Optimisations

### Optimisation 1: Buffered WAL with Explicit Fsync (HIGH IMPACT)

**Problem**: O_SYNC forces sync on every write() call
**Solution**: Use buffered I/O with periodic fsync()

**Current (viperblock.go:442)**:
```go
file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR|syscall.O_SYNC, 0640)
```

**Proposed**:
```go
// Option A: Remove O_SYNC, add explicit flush control
file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0640)

// Option B: Use bufio.Writer for write coalescing
type WAL struct {
    DB          []*os.File
    Writers     []*bufio.Writer  // NEW: Buffered writers
    DirtyBlocks uint64           // NEW: Track unflushed blocks
    // ...
}
```

**New WriteWAL with batching**:
```go
func (vb *VB) WriteWAL(block Block) error {
    // ... existing header construction ...

    // Single buffered write instead of 5 separate writes
    var buf bytes.Buffer
    binary.Write(&buf, binary.BigEndian, block.SeqNum)
    binary.Write(&buf, binary.BigEndian, block.Block)
    binary.Write(&buf, binary.BigEndian, block.Len)
    binary.Write(&buf, binary.BigEndian, checksum)
    buf.Write(block.Data)

    vb.WAL.Writers[currentWALNum].Write(buf.Bytes())
    vb.WAL.DirtyBlocks++

    return nil
}

func (vb *VB) SyncWAL() error {
    // Explicit sync when durability is needed
    for _, writer := range vb.WAL.Writers {
        writer.Flush()
    }
    for _, file := range vb.WAL.DB {
        file.Sync()
    }
    vb.WAL.DirtyBlocks = 0
    return nil
}
```

**Expected Impact**:
- Reduces 5 sync operations per block to 1 sync per batch
- 10-50x improvement for small writes

---

### Optimisation 2: Configurable Durability Modes (MEDIUM IMPACT)

Add durability configuration to trade safety for speed:

```go
type DurabilityMode int

const (
    DurabilityStrict    DurabilityMode = iota  // Current: O_SYNC every write
    DurabilityBatched                          // Sync every N blocks or M milliseconds
    DurabilityRelaxed                          // Sync only on explicit Flush()
    DurabilityAsync                            // Background sync (maximum performance)
)

type VB struct {
    // ...
    DurabilityMode      DurabilityMode
    BatchSyncBlocks     uint64         // Sync after N blocks (for Batched mode)
    BatchSyncInterval   time.Duration  // Sync after N milliseconds (for Batched mode)
}
```

**Usage scenarios**:
- **Strict**: Production with critical data
- **Batched**: Good balance for most use cases (recommended default)
- **Relaxed**: Development/testing, data can be regenerated
- **Async**: Maximum throughput, acceptable data loss window

---

### Optimisation 3: Write Coalescing in Memory (HIGH IMPACT)

**Problem**: Current code calls `Flush()` frequently, triggering I/O
**Solution**: Smarter flush triggers based on actual conditions

**Current pattern** (caller code):
```go
// Flush every 4MB worth of blocks
if block%uint64(vb.BlockSize) == 0 {
    vb.Flush()
    vb.WriteWALToChunk(true)
}
```

**Proposed: Automatic Background Flushing**:
```go
type VB struct {
    // ... existing fields ...

    // Flush configuration (these constants exist but are unused!)
    FlushInterval   time.Duration  // Already defined: 5 * time.Second
    FlushSize       uint32         // Already defined: 64MB

    // New: Background flush control
    flushTicker     *time.Ticker
    flushChan       chan struct{}
    autoFlush       bool
}

func (vb *VB) StartAutoFlush() {
    vb.autoFlush = true
    vb.flushTicker = time.NewTicker(vb.FlushInterval)
    go func() {
        for {
            select {
            case <-vb.flushTicker.C:
                vb.flushIfNeeded()
            case <-vb.flushChan:
                return
            }
        }
    }()
}

func (vb *VB) flushIfNeeded() {
    vb.Writes.mu.RLock()
    pendingBytes := len(vb.Writes.Blocks) * int(vb.BlockSize)
    vb.Writes.mu.RUnlock()

    if pendingBytes >= int(vb.FlushSize) {
        vb.Flush()
        vb.SyncWAL()  // Using new sync method
    }
}
```

**Caller code becomes**:
```go
vb.StartAutoFlush()  // Once at startup

for {
    vb.WriteAt(block*uint64(vb.BlockSize), buf[:n])
    block++
    // No manual flush needed - handled automatically
}

vb.Close()  // Flushes remaining data
```

---

### Optimisation 4: Single Write per WAL Block (MEDIUM IMPACT)

**Problem**: WriteWAL makes 5 separate write() calls per block
**Solution**: Construct full record in memory, single write

**Current (viperblock.go:711-756)**:
```go
func (vb *VB) WriteWAL(block Block) error {
    // 5 separate writes:
    currentWAL.Write(seqBuf)      // 8 bytes
    currentWAL.Write(blockBuf)    // 8 bytes
    currentWAL.Write(lenBuf)      // 8 bytes
    currentWAL.Write(checksumBuf) // 4 bytes
    currentWAL.Write(block.Data)  // 4096 bytes
}
```

**Proposed**:
```go
func (vb *VB) WriteWAL(block Block) error {
    // Pre-allocate record buffer (28 byte header + data)
    recordSize := 28 + len(block.Data)
    record := make([]byte, recordSize)

    // Pack header
    binary.BigEndian.PutUint64(record[0:8], block.SeqNum)
    binary.BigEndian.PutUint64(record[8:16], block.Block)
    binary.BigEndian.PutUint64(record[16:24], block.Len)

    // Copy data
    copy(record[28:], block.Data)

    // Calculate checksum over header + data
    checksum := crc32.ChecksumIEEE(record[0:24])
    checksum = crc32.Update(checksum, crc32.IEEETable, block.Data)
    binary.BigEndian.PutUint32(record[24:28], checksum)

    // SINGLE write call
    _, err := currentWAL.Write(record)
    return err
}
```

**Expected Impact**: 5x reduction in system calls per block

---

### Optimisation 5: Memory-Mapped WAL (ADVANCED)

For extreme performance, use memory-mapped files:

```go
type MappedWAL struct {
    file     *os.File
    data     []byte        // mmap'd region
    offset   int64         // Current write position
    capacity int64         // Total mapped size
}

func (wal *MappedWAL) WriteBlock(record []byte) error {
    if wal.offset + int64(len(record)) > wal.capacity {
        return ErrWALFull
    }

    copy(wal.data[wal.offset:], record)
    wal.offset += int64(len(record))
    return nil  // No syscall!
}

func (wal *MappedWAL) Sync() error {
    return unix.Msync(wal.data, unix.MS_SYNC)
}
```

**Benefits**:
- Writes go directly to page cache (no syscalls)
- OS handles writeback efficiently
- Single msync() for durability

**Drawbacks**:
- More complex error handling
- Platform-specific code needed
- Fixed WAL size or remapping required

---

### Optimisation 6: Compress Blocks Before WAL (MEDIUM IMPACT)

SD cards and slow storage benefit from reduced I/O volume:

```go
import "github.com/klauspost/compress/lz4"

type VB struct {
    // ...
    CompressWAL     bool
    MinCompressSize int  // Don't compress blocks smaller than this
}

func (vb *VB) WriteWAL(block Block) error {
    data := block.Data
    compressed := false

    if vb.CompressWAL && len(block.Data) >= vb.MinCompressSize {
        var buf bytes.Buffer
        w := lz4.NewWriter(&buf)
        w.Write(block.Data)
        w.Close()

        if buf.Len() < len(block.Data) * 90 / 100 {  // >10% savings
            data = buf.Bytes()
            compressed = true
        }
    }

    // Write with compression flag in header
    // ...
}
```

**Expected Impact**:
- Typical text/config files: 50-70% size reduction
- Binary/media files: minimal benefit
- Net result: fewer bytes to slow storage

---

### Optimisation 7: Batch Multiple Blocks per WAL Write (HIGH IMPACT)

Instead of writing blocks individually to WAL, batch them:

```go
type WALBatch struct {
    blocks    []Block
    totalSize int
    maxSize   int  // e.g., 1MB batch size
}

func (vb *VB) WriteAtBatched(offset uint64, data []byte) error {
    // ... convert to Block ...

    vb.walBatch.mu.Lock()
    vb.walBatch.blocks = append(vb.walBatch.blocks, block)
    vb.walBatch.totalSize += len(block.Data) + 28  // data + header

    shouldFlush := vb.walBatch.totalSize >= vb.walBatch.maxSize
    vb.walBatch.mu.Unlock()

    if shouldFlush {
        return vb.FlushWALBatch()
    }
    return nil
}

func (vb *VB) FlushWALBatch() error {
    vb.walBatch.mu.Lock()
    blocks := vb.walBatch.blocks
    vb.walBatch.blocks = make([]Block, 0, 256)
    vb.walBatch.totalSize = 0
    vb.walBatch.mu.Unlock()

    // Build single large buffer
    var buf bytes.Buffer
    for _, block := range blocks {
        writeBlockRecord(&buf, block)
    }

    // Single write + single sync
    vb.WAL.DB[currentWAL].Write(buf.Bytes())
    vb.WAL.DB[currentWAL].Sync()

    return nil
}
```

**Expected Impact**:
- 100 blocks batched = 1 sync instead of 500
- Dramatic improvement for sequential writes

---

## Recommended Implementation Order

### Phase 1: Quick Wins (1-2 hours each)

1. **Remove O_SYNC, add explicit Sync()** - Biggest single impact
2. **Single write per WAL block** - Simple refactor, immediate benefit
3. **Add DurabilityMode configuration** - Allows users to choose their tradeoff

### Phase 2: Structural Improvements (half day each)

4. **Implement batch WAL writes** - Major performance gain
5. **Background auto-flush goroutine** - Better caller experience
6. **Add buffered writers** - Smoother I/O patterns

### Phase 3: Advanced Optimisations (1+ days)

7. **Memory-mapped WAL** - Maximum performance
8. **Block compression** - Benefits slow storage specifically
9. **Async write pipeline** - Fully decoupled I/O

---

## Performance Expectations

| Scenario | Current | After Phase 1 | After Phase 2 |
|----------|---------|---------------|---------------|
| WSL 4KB writes | ~50ms/block | ~5ms/block | ~0.5ms/block |
| SD Card sequential | ~20ms/block | ~2ms/block | ~0.2ms/block |
| SD Card random | ~30ms/block | ~3ms/block | ~0.5ms/block |
| NVMe (baseline) | ~0.1ms/block | ~0.05ms/block | ~0.02ms/block |

**Note**: Actual performance depends on hardware, but order-of-magnitude improvements are expected.

---

## API Changes Summary

### New Configuration Options

```go
type VBConfig struct {
    // Existing
    BlockSize     uint32
    ObjBlockSize  uint32

    // New durability options
    DurabilityMode    DurabilityMode  // Strict, Batched, Relaxed, Async
    WALBatchSize      int             // Bytes to accumulate before WAL write
    WALSyncInterval   time.Duration   // Time-based sync trigger

    // New performance options
    EnableAutoFlush   bool            // Background flush goroutine
    CompressWAL       bool            // LZ4 compress blocks
    UseBufferedIO     bool            // bufio.Writer for WAL
}
```

### New Methods

```go
// Explicit sync control
func (vb *VB) SyncWAL() error
func (vb *VB) SetDurabilityMode(mode DurabilityMode)

// Background flush
func (vb *VB) StartAutoFlush()
func (vb *VB) StopAutoFlush()

// Batch operations
func (vb *VB) WriteAtBatched(offset uint64, data []byte) error
func (vb *VB) FlushWALBatch() error
```

### Backward Compatibility

- Default `DurabilityMode = DurabilityStrict` maintains current behaviour
- All new features are opt-in
- Existing API unchanged

---

## Testing Requirements

1. **Durability tests**: Verify data survives crash/power loss in each mode
2. **Performance benchmarks**: Measure throughput on WSL, SD card, NVMe
3. **Concurrent write tests**: Ensure batching doesn't cause race conditions
4. **WAL recovery tests**: Verify batched WAL can be replayed correctly
5. **Memory pressure tests**: Ensure batching doesn't cause OOM

---

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Data loss in relaxed modes | Clear documentation, explicit opt-in required |
| Increased memory usage (batching) | Configurable batch sizes, memory limits |
| Complexity increase | Phased rollout, extensive testing |
| Platform-specific issues (mmap) | Feature flags, fallback to standard I/O |

---

## Additional Optimisations (WAL Reliability Preserved)

The following optimisations reduce CPU overhead, memory allocations, and lock contention **without compromising WAL durability**. These are safe to implement regardless of durability mode.

---

### Optimisation 8: sync.Pool for WAL Record Buffers (HIGH IMPACT)

**Problem**: `WriteWAL()` allocates a new buffer for every block written.

**Current (viperblock.go:715)**:
```go
func (vb *VB) WriteWAL(block Block) error {
    recordSize := 28 + len(block.Data)
    record := make([]byte, recordSize)  // NEW allocation every call!
    // ...
}
```

**Impact**: Writing 1GB at 4KB blocks = 262,144 allocations, each hitting Go's allocator.

**Solution**: Use `sync.Pool` to reuse buffers:
```go
var walRecordPool = sync.Pool{
    New: func() interface{} {
        // Pre-allocate for typical 4KB block + 28 byte header
        buf := make([]byte, 4096+28)
        return &buf
    },
}

func (vb *VB) WriteWAL(block Block) error {
    recordSize := 28 + len(block.Data)

    // Get buffer from pool
    bufPtr := walRecordPool.Get().(*[]byte)
    record := *bufPtr

    // Grow if needed (rare case for blocks > 4KB)
    if cap(record) < recordSize {
        record = make([]byte, recordSize)
    } else {
        record = record[:recordSize]
    }

    // ... pack header and data ...

    _, err := currentWAL.Write(record)

    // Return to pool
    *bufPtr = record
    walRecordPool.Put(bufPtr)

    return err
}
```

**Expected Impact**: Near-zero allocations in hot path, significant GC pressure reduction.

---

### Optimisation 9: Fix O(n²) Block Deduplication (CRITICAL)

**Problem**: `createChunkFile()` uses O(n²) search to filter pending writes.

**Current (viperblock.go:1198-1205)**:
```go
for _, block := range pendingBackendWrites {
    var matched bool = false
    for _, matchedBlock := range *matchedBlocks {  // O(n) per block!
        if block.Block == matchedBlock.Block {
            matched = true
            break
        }
    }
    // ...
}
```

**Impact**: 1000 pending + 256 matched = **256,000 comparisons** with lock held.

**Solution**: Use hash map for O(1) lookup:
```go
func (vb *VB) createChunkFile(...) error {
    // Build lookup map ONCE - O(n)
    matchedMap := make(map[uint64]struct{}, len(*matchedBlocks))
    for _, mb := range *matchedBlocks {
        matchedMap[mb.Block] = struct{}{}
    }

    vb.PendingBackendWrites.mu.Lock()
    pendingBackendWrites := make([]Block, len(vb.PendingBackendWrites.Blocks))
    copy(pendingBackendWrites, vb.PendingBackendWrites.Blocks)

    // Filter with O(1) lookup per block
    remaining := pendingBackendWrites[:0]  // Reuse slice
    for _, block := range pendingBackendWrites {
        if _, matched := matchedMap[block.Block]; !matched {
            remaining = append(remaining, block)
        }
    }
    vb.PendingBackendWrites.Blocks = remaining
    vb.PendingBackendWrites.mu.Unlock()
}
```

**Expected Impact**: O(n²) → O(n), lock held for microseconds instead of milliseconds.

---

### Optimisation 10: Single-Allocation Block Lookup Serialization (HIGH IMPACT)

**Problem**: `writeBlockWalChunk()` creates 5 separate allocations per block lookup entry.

**Current (viperblock.go:890-912)**:
```go
func (vb *VB) writeBlockWalChunk(block *BlockLookup) (data []byte) {
    data = make([]byte, 0)                      // Alloc 1
    startBlock := make([]byte, 8)               // Alloc 2
    numBlocks := make([]byte, 2)                // Alloc 3
    objectID := make([]byte, 8)                 // Alloc 4
    objectOffset := make([]byte, 4)             // Alloc 5
    checksumBytes := make([]byte, 4)            // Alloc 6
    // ... append each one ...
}
```

**Impact**: 4MB chunk with 4KB blocks = 1024 blocks × 6 allocations = **6,144 allocations**.

**Solution**: Single buffer with direct encoding:
```go
func (vb *VB) writeBlockWalChunk(block *BlockLookup) []byte {
    // Single allocation: 8 + 2 + 8 + 4 + 4 = 26 bytes
    data := make([]byte, 26)

    binary.BigEndian.PutUint64(data[0:8], block.StartBlock)
    binary.BigEndian.PutUint16(data[8:10], block.NumBlocks)
    binary.BigEndian.PutUint64(data[10:18], block.ObjectID)
    binary.BigEndian.PutUint32(data[18:22], block.ObjectOffset)

    // Checksum over first 22 bytes
    checksum := crc32.ChecksumIEEE(data[0:22])
    binary.BigEndian.PutUint32(data[22:26], checksum)

    return data
}
```

**Expected Impact**: 6x reduction in allocations during chunk creation.

---

### Optimisation 11: Consolidate Double-Lock in Flush() (MEDIUM IMPACT)

**Problem**: `Flush()` acquires and releases the same lock twice.

**Current (viperblock.go:629-662)**:
```go
func (vb *VB) Flush() error {
    vb.Writes.mu.Lock()                    // Lock #1 acquire
    flushBlocks := make([]Block, len(vb.Writes.Blocks))
    copy(flushBlocks, vb.Writes.Blocks)
    vb.Writes.mu.Unlock()                  // Lock #1 release

    // ... write to WAL ...

    vb.Writes.mu.Lock()                    // Lock #2 acquire
    remaining := make([]Block, 0)
    for _, block := range vb.Writes.Blocks {
        // filter logic
    }
    vb.Writes.Blocks = remaining
    vb.Writes.mu.Unlock()                  // Lock #2 release
}
```

**Solution**: Track flushed indices and consolidate:
```go
func (vb *VB) Flush() error {
    vb.Writes.mu.Lock()
    defer vb.Writes.mu.Unlock()

    if len(vb.Writes.Blocks) == 0 {
        return nil
    }

    // Track which blocks we successfully flush
    flushedIndices := make([]bool, len(vb.Writes.Blocks))

    for i, block := range vb.Writes.Blocks {
        if err := vb.WriteWAL(block); err != nil {
            slog.Error("WriteWAL failed", "block", block.Block, "err", err)
            continue
        }
        flushedIndices[i] = true
    }

    // Filter in-place using flushedIndices
    n := 0
    for i, block := range vb.Writes.Blocks {
        if !flushedIndices[i] {
            vb.Writes.Blocks[n] = block
            n++
        }
    }
    vb.Writes.Blocks = vb.Writes.Blocks[:n]

    return nil
}
```

**Trade-off**: Lock held during WAL writes, but eliminates race window between copy and filter. For high-throughput scenarios, the original pattern may be preferred.

---

### Optimisation 12: Reduce Lock Scope in createChunkFile() (MEDIUM IMPACT)

**Problem**: `BlocksToObject.mu` lock held during entire block mapping loop.

**Current (viperblock.go:1228-1261)**:
```go
vb.BlocksToObject.mu.Lock()
for i, block := range *matchedBlocks {
    // Build BlockLookup entries - many iterations
    lookup := BlockLookup{...}
    vb.BlocksToObject.Blocks[block.Block] = lookup
}
vb.BlocksToObject.mu.Unlock()
```

**Solution**: Build entries first, then bulk insert:
```go
// Build entries WITHOUT lock
lookups := make([]BlockLookup, 0, len(*matchedBlocks))
for i, block := range *matchedBlocks {
    lookup := BlockLookup{
        StartBlock:   block.Block,
        NumBlocks:    1,
        ObjectID:     objectID,
        ObjectOffset: uint32(i * int(vb.BlockSize)),
    }
    lookups = append(lookups, lookup)
}

// Bulk insert WITH lock - minimal hold time
vb.BlocksToObject.mu.Lock()
for _, lookup := range lookups {
    vb.BlocksToObject.Blocks[lookup.StartBlock] = lookup
}
vb.BlocksToObject.mu.Unlock()
```

**Expected Impact**: Lock held for map insertions only, not computation.

---

### Optimisation 13: Fixed-Size WAL Read Buffer (MEDIUM IMPACT)

**Problem**: `WriteWALToChunk()` allocates new buffer for every WAL entry read.

**Current (viperblock.go:1005)**:
```go
for {
    data := make([]byte, 28+vb.BlockSize)  // Allocation per entry!
    _, err := pendingWAL2.Read(data)
    // ...
}
```

**Solution**: Reuse single buffer:
```go
func (vb *VB) WriteWALToChunk(force bool) error {
    // ...

    // Single allocation, reused for all reads
    readBuffer := make([]byte, 28+vb.BlockSize)

    for {
        _, err := pendingWAL2.Read(readBuffer)
        if err == io.EOF {
            break
        }

        // Parse header from buffer
        seqNum := binary.BigEndian.Uint64(readBuffer[0:8])
        blockNum := binary.BigEndian.Uint64(readBuffer[8:16])
        blockLen := binary.BigEndian.Uint64(readBuffer[16:24])

        // Copy only the data portion we need to keep
        blockData := make([]byte, blockLen)
        copy(blockData, readBuffer[28:28+blockLen])

        block := Block{
            SeqNum: seqNum,
            Block:  blockNum,
            Len:    blockLen,
            Data:   blockData,
        }
        blocks = append(blocks, block)
    }
}
```

**Expected Impact**: One allocation per entry (for Data) instead of two.

---

### Optimisation 14: Eliminate Unnecessary clone() in Read Path (LOW IMPACT)

**Problem**: `read()` clones block data even when returning immutable data.

**Current (viperblock.go:1628, 1636)**:
```go
return clone(block.Data), nil  // Copies 4KB unnecessarily
```

**Analysis**: The clone is defensive - prevents caller from mutating cached data. However, if callers treat returned data as read-only, this is wasteful.

**Solution**: Document immutability contract OR use copy-on-write:
```go
// Option A: Trust callers (document that returned data must not be modified)
return block.Data, nil

// Option B: Return wrapper that copies on first modification (complex)

// Option C: Only clone if block is from mutable source (Cache entries are safe)
if fromCache {
    return block.Data, nil  // Cache data is already a copy
}
return clone(block.Data), nil
```

**Expected Impact**: Eliminates 4KB copy per read from cache.

---

### Optimisation 15: Batch State Saves (MEDIUM IMPACT)

**Problem**: `SaveBlockState()` called after every chunk creation.

**Current (viperblock.go:1264 in createChunkFile)**:
```go
err = vb.WriteBlockWAL(blockLookup)  // Called per chunk
```

This triggers state persistence for every 4MB chunk. Writing 1GB = 256 state saves.

**Solution**: Batch state saves:
```go
type VB struct {
    // ...
    pendingBlockLookups []BlockLookup
    stateSaveThreshold  int  // e.g., 64 (save every 64 chunks = 256MB)
}

func (vb *VB) WriteBlockWAL(lookup *BlockLookup) error {
    vb.pendingBlockLookups = append(vb.pendingBlockLookups, *lookup)

    if len(vb.pendingBlockLookups) >= vb.stateSaveThreshold {
        return vb.flushBlockLookups()
    }
    return nil
}

func (vb *VB) flushBlockLookups() error {
    // Write all pending lookups in single operation
    for _, lookup := range vb.pendingBlockLookups {
        // ... write to WAL checkpoint ...
    }
    vb.pendingBlockLookups = vb.pendingBlockLookups[:0]
    return nil
}
```

**Expected Impact**: 256 state saves → 4 state saves for 1GB write.

---

### Optimisation 16: Pre-allocate Slice Capacities (LOW IMPACT)

**Problem**: Multiple slices grow via `append()` without pre-allocation.

**Examples**:
```go
// viperblock.go:1002
blocks := make([]Block, 0, (ObjBlockSize/BlockSize)+1)  // Good - has capacity

// viperblock.go:1071
matchedBlocks = make([]Block, 0)  // Bad - no capacity, will reallocate

// viperblock.go:652
remaining := make([]Block, 0)  // Bad - no capacity
```

**Solution**: Pre-allocate with expected capacity:
```go
// Estimate based on chunk size
expectedBlocks := int(vb.ObjBlockSize / vb.BlockSize)
matchedBlocks = make([]Block, 0, expectedBlocks)

// For remaining, estimate based on failure rate (usually 0)
remaining := make([]Block, 0, len(vb.Writes.Blocks)/10)
```

**Expected Impact**: Fewer slice reallocations during append operations.

---

## Summary: Safe Optimisations (No Durability Trade-off)

| Optimisation | Location | Impact | Complexity |
|--------------|----------|--------|------------|
| sync.Pool for WAL buffers | WriteWAL() | High | Low |
| O(n²) → O(n) dedup | createChunkFile() | Critical | Low |
| Single-alloc block lookup | writeBlockWalChunk() | High | Low |
| Consolidate Flush() locks | Flush() | Medium | Medium |
| Reduce BlocksToObject lock scope | createChunkFile() | Medium | Low |
| Fixed-size WAL read buffer | WriteWALToChunk() | Medium | Low |
| Eliminate clone() in reads | read() | Low | Low |
| Batch state saves | WriteBlockWAL() | Medium | Medium |
| Pre-allocate slice capacities | Various | Low | Low |

**Recommended order**: Start with #9 (O(n²) fix) and #8 (sync.Pool) for biggest impact with lowest risk.

---

## Conclusion

The current performance issues stem primarily from **O_SYNC on every WAL write** combined with **5 separate write() calls per block**.

**Minimum viable optimisation**: Remove O_SYNC and batch WAL writes with explicit sync points. This alone should provide 10-50x improvement for the problematic scenarios.

**For CPU/memory improvements without touching durability**: Implement sync.Pool for WAL buffers, fix the O(n²) deduplication, and use single-allocation serialization. These provide significant gains with no durability trade-off.

For maximum benefit, implement the full batch write pipeline with configurable durability modes, allowing users to choose their performance/safety tradeoff based on their use case.

---

## Unified Block Store (IMPLEMENTED)

### Problem Statement

The original read path consumed 46% CPU in `mapassign_fast64` because it rebuilt lookup maps on every read operation. With 4 separate data structures (Writes, PendingBackendWrites, Cache, BlocksToObject), each read performed O(n) preprocessing of all pending writes:

```go
// OLD CODE - O(n) map rebuild on every read
latestWrites := make(BlocksMap, writesLen)
for _, wr := range writesCopy {
    if prev, ok := latestWrites[wr.Block]; !ok || wr.SeqNum > prev.SeqNum {
        latestWrites[wr.Block] = wr
    }
}
```

### Solution

The Unified Block Store (`blockstore.go`) replaces the 4 separate data structures with a single sharded index that provides O(1) lookups and is updated incrementally on writes (not rebuilt on reads).

### Block States

```
Empty -> Hot -> Pending -> Persisted <-> Cached
         ^                    |
         |____________________|  (new write)
```

- **Empty**: Block has never been written (zero block)
- **Hot**: In active write buffer (not yet WAL'd)
- **Pending**: WAL'd, awaiting backend upload
- **Persisted**: On backend storage
- **Cached**: Clean copy from backend in LRU cache

### Key Design Decisions

1. **Sharded Index**: 16 shards with independent RWMutex reduces lock contention by 16x
2. **Persistent Write Index**: Updated on write, not rebuilt on read
3. **Fast Path**: Single-block reads bypass batch processing via `ReadSingle()`
4. **State Machine**: Clear transitions between block states with atomic updates

### Implementation Files

- `viperblock/viperblock/blockstore.go` - Core UnifiedBlockStore implementation
- `viperblock/viperblock/blockstore_test.go` - Tests and benchmarks
- `viperblock/viperblock/arena.go` - Memory arena for write data (reduces GC)
- `viperblock/viperblock/viperblock.go` - Integration with VB struct

### API

```go
// Create a new block store
bs := NewUnifiedBlockStore(4096) // 4KB block size

// O(1) write - updates index immediately
seqNum := bs.Write(blockNum, data)

// O(1) read - single lookup, no map rebuilding
data, state, err := bs.ReadSingle(blockNum)

// State transitions
bs.MarkPending(blockNum)                        // Hot -> Pending
bs.MarkPersisted(blockNum, objectID, offset)    // Pending -> Persisted
bs.Cache(blockNum, data)                        // Persisted -> Cached
bs.EvictCache(blockNum)                         // Cached -> Persisted
```

### Enabling the Block Store

The BlockStore can be enabled/disabled at runtime for safe migration:

```go
// Enable optimized path
vb.EnableBlockStore()

// Disable and use legacy path
vb.DisableBlockStore()

// Sync state from legacy data structures
vb.SyncBlockStoreFromLegacy()
```

Or enable during VB creation:

```go
config := &VB{
    VolumeName:    "test-vol",
    VolumeSize:    1024 * 1024 * 1024,
    UseBlockStore: true,
}
vb, _ := New(config, "file", backendConfig)
```

### Expected Performance

| Metric | Before | After |
|--------|--------|-------|
| `mapassign_fast64` CPU | 46% | < 5% |
| Read latency | O(n) | O(1) |
| Per-read allocations | 2 maps + copies | 1 result copy |
| Lock contention | Single global | 16 shards |

### Memory Arena

The `arena.go` provides a bump-pointer allocator for write data to reduce GC pressure:

```go
arena := NewArena(4096) // 4KB blocks

// Allocate from arena (O(1), no GC until slab full)
buf := arena.Alloc()

// Or allocate and copy in one step
buf := arena.AllocCopy(data)
```

Features:
- 4MB slabs (matches ObjBlockSize)
- Reference counting for slab reuse
- Compaction of empty slabs
- Optional pooling via `PooledArena` for frequently reused buffers

### Verification

```bash
# Run unit tests
cd viperblock && LOG_IGNORE=1 go test -v ./viperblock/... -run BlockStore

# Run benchmarks
cd viperblock && go test -bench=BlockStore -benchmem ./viperblock/...

# Compare old vs new approach
go test -bench=OldVsNew -benchmem ./viperblock/...
```

### Rollback Plan

The implementation uses a phased approach for safe rollback:

1. BlockStore runs alongside existing structures
2. `UseBlockStore` flag controls which path is used
3. Can disable BlockStore and revert to legacy at any time
4. Legacy data structures are always kept in sync

```go
// Disable BlockStore and use legacy path
vb.DisableBlockStore()
```
