# Viperblock Architecture

Viperblock is a high-performance, WAL-backed block storage engine that provides EBS-compatible volumes for [Hive](https://github.com/mulgadc/hive). It combines an in-memory write buffer, a write-ahead log on fast local storage, and batched chunk uploads to a pluggable backend (local filesystem, S3, or [Predastore](https://github.com/mulgadc/predastore)). Volumes are exposed to QEMU/KVM virtual machines via an nbdkit plugin over the NBD protocol.

---

## Table of Contents

1. [High-Level Architecture](#1-high-level-architecture)
2. [Data Model](#2-data-model)
3. [Write Path](#3-write-path)
4. [Read Path](#4-read-path)
5. [Write-Ahead Log (WAL)](#5-write-ahead-log-wal)
6. [Sharded WAL](#6-sharded-wal)
7. [Chunk Storage](#7-chunk-storage)
8. [Block Mapping](#8-block-mapping)
9. [Unified Block Store](#9-unified-block-store)
10. [Snapshots and Clones](#10-snapshots-and-clones)
11. [Storage Backends](#11-storage-backends)
12. [NBD Plugin](#12-nbd-plugin)
13. [NATS Integration](#13-nats-integration)
14. [Memory Management](#14-memory-management)
15. [Binary Formats](#15-binary-formats)
16. [Configuration Reference](#16-configuration-reference)
17. [File Structure](#17-file-structure)
18. [References](#18-references)

---

# 1. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       QEMU/KVM Virtual Machine                              │
│                                                                             │
│   Guest OS sees /dev/vda (or /dev/sda) — a normal block device              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ NBD Protocol
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       nbdkit + viperblock plugin                            │
│                                                                             │
│   lib/nbdkit-viperblock-plugin.so                                           │
│   • PRead / PWrite / Zero / Trim / Flush                                    │
│   • Translates NBD requests into Viperblock Read/Write/Flush calls          │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Viperblock Engine                                    │
│                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐   │
│  │ Write Buffer │  │  BlockStore  │  │  LRU Cache   │  │ Block Mapping │   │
│  │ (in-memory)  │  │ (16 shards)  │  │              │  │ (extents)     │   │
│  └──────┬───────┘  └──────────────┘  └──────────────┘  └───────────────┘   │
│         │                                                                   │
│  ┌──────▼───────┐  ┌──────────────┐                                        │
│  │     WAL      │  │ Sharded WAL  │   NVMe / fast local storage            │
│  │  (single)    │  │ (16 shards)  │                                        │
│  └──────┬───────┘  └──────┬───────┘                                        │
│         │                 │                                                 │
│  ┌──────▼─────────────────▼──┐                                             │
│  │   Chunk Consolidation     │   Dedup → Sort → Pack into 4 MB chunks      │
│  └──────────────┬────────────┘                                             │
│                 ▼                                                           │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                    Storage Backend                               │       │
│  │  ┌──────────┐   ┌──────────┐   ┌──────────────────────────┐     │       │
│  │  │   File   │   │  Memory  │   │  S3 (Predastore / AWS)   │     │       │
│  │  └──────────┘   └──────────┘   └──────────────────────────┘     │       │
│  └──────────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Component Overview

| Component | Purpose |
|-----------|---------|
| **Write Buffer** | In-memory block cache for fast write acknowledgment |
| **WAL** | Durability layer on fast local storage (NVMe recommended) |
| **Chunk Consolidation** | Deduplicates and packs WAL entries into 4 MB chunk files |
| **Block Mapping** | Extent-based index mapping logical blocks to chunk file + offset |
| **Unified Block Store** | O(1) sharded index tracking block state across all lifecycle phases |
| **LRU Cache** | Caches clean blocks read from the backend |
| **Storage Backend** | Pluggable durable storage (file, memory, S3) |
| **NBD Plugin** | Exposes volumes as network block devices for QEMU/KVM |

---

# 2. Data Model

## Constants

| Constant | Value | Description |
|----------|-------|-------------|
| Block size | 4 KB (4,096 bytes) | Standard filesystem/disk alignment |
| Chunk size | 4 MB (4,194,304 bytes) | Batched upload unit (`ObjBlockSize`) |
| Flush interval | 5 seconds | Write buffer → WAL flush period |
| Flush size | 64 MB | Write buffer → WAL flush threshold |
| WAL sync interval | 200 ms | Periodic `fsync` for WAL durability |
| Index shards | 16 | Block store sharding factor (power of 2) |
| Arena slab size | 4 MB | Bump-pointer allocator slab |

## Block Lifecycle

Every block passes through a well-defined state machine:

```
                Write()
                  │
                  ▼
┌─────────┐   ┌─────────┐   Flush()    ┌─────────────┐   Consolidate   ┌─────────────┐
│  Empty  │──▶│   Hot   │────────────▶│   Pending   │──────────────▶│  Persisted  │
│ (never  │   │ (memory │              │ (WAL'd,     │               │ (on backend │
│ written)│   │  only)  │              │  awaiting   │               │  storage)   │
└─────────┘   └─────────┘              │  upload)    │               └──────┬──────┘
                                       └─────────────┘                      │
                                                                     Read from
                                                                      backend
                                                                            │
                                                                     ┌──────▼──────┐
                                                                     │   Cached    │
                                                                     │ (LRU copy)  │
                                                                     └─────────────┘
```

| State | Description |
|-------|-------------|
| `Empty` | Block has never been written; reads return zeros |
| `Hot` | In the write buffer, not yet WAL'd |
| `Pending` | Flushed to WAL, awaiting chunk consolidation and backend upload |
| `Persisted` | Stored on the backend; block mapping records its location |
| `Cached` | Clean copy loaded from backend into the LRU cache |

---

# 3. Write Path

```
Client Write(block, data)
        │
        ▼
┌─────────────────────────────────────────────┐
│  1. Assign monotonic sequence number        │
│  2. Store in BlockStore as Hot              │
│  3. Return immediately (fast ack)           │
└──────────────────┬──────────────────────────┘
                   │
           FlushInterval (5s)
           or FlushSize (64 MB)
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  4. Flush(): write Hot blocks to WAL        │
│     - Write WAL header + block records      │
│     - Mark blocks as Pending                │
│  5. WAL syncer fsyncs every 200ms           │
└──────────────────┬──────────────────────────┘
                   │
            WAL accumulates
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  6. WriteWALToChunk(): consolidate          │
│     - Read WAL file, validate checksums     │
│     - Deduplicate (highest SeqNum wins)     │
│     - Sort blocks by block number           │
│     - Pack into 4 MB chunk files            │
│     - Upload chunks to backend              │
│     - Update BlocksToObject mapping         │
│     - Mark blocks as Persisted              │
└─────────────────────────────────────────────┘
```

The write path prioritizes latency: writes are acknowledged as soon as data reaches the in-memory buffer. Durability is layered — the WAL ensures crash recovery, and chunk consolidation moves data to durable backend storage.

---

# 4. Read Path

```
Client Read(block)
        │
        ▼
┌─────────────────────────────────────────────┐
│  1. Check UnifiedBlockStore (O(1) lookup)   │
│     ├─ Hot/Pending/Cached → return data     │
│     └─ Persisted → need backend fetch       │
└──────────────────┬──────────────────────────┘
                   │ (Persisted)
                   ▼
┌─────────────────────────────────────────────┐
│  2. Lookup BlocksToObject                   │
│     → ObjectID + ObjectOffset               │
│                                             │
│  3. If not found, check BaseBlockMap        │
│     (for snapshot clones — reads fall       │
│      through to source volume)              │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  4. Backend.Read(objectID, offset, length)  │
│  5. Cache result in LRU (Cached state)      │
│  6. Return data                             │
└─────────────────────────────────────────────┘
```

Reads check the fastest storage tier first (memory), then fall through to the backend. For snapshot clones, an additional `BaseBlockMap` lookup provides copy-on-write semantics without duplicating data.

---

# 5. Write-Ahead Log (WAL)

The WAL provides crash durability for writes that have left the in-memory buffer but have not yet been consolidated into chunk files on the backend.

## WAL File Format

```
┌───────────────────────────────────────────────────────────────┐
│                        WAL Header (18 bytes)                   │
├───────────┬───────────┬────────────┬──────────────────────────┤
│ Magic (4) │ Version(2)│ BlockSize(4)│ Timestamp (8)           │
│  "VBWL"   │ uint16 BE │ uint32 BE  │ uint64 BE (UnixNano)    │
└───────────┴───────────┴────────────┴──────────────────────────┘
┌───────────────────────────────────────────────────────────────┐
│                   Block Record (28 bytes + data)               │
├──────────────┬──────────────┬──────────────┬──────────────────┤
│ SeqNum (8)   │ BlockNum (8) │ BlockLen (8) │ CRC32 (4)        │
│ uint64 BE    │ uint64 BE    │ uint64 BE    │ uint32 BE        │
├──────────────┴──────────────┴──────────────┴──────────────────┤
│                     Block Data (BlockLen bytes)                │
└───────────────────────────────────────────────────────────────┘
│                         ... more block records                 │
```

All multi-byte fields are big-endian. Each block record includes a CRC32 checksum for integrity verification during WAL replay and chunk consolidation.

## WAL Lifecycle

1. **Open**: A new WAL file is created at `{volume}/wal/chunks/wal.{walNum:08d}.bin`
2. **Write**: `Flush()` appends block records from the write buffer
3. **Sync**: A background goroutine calls `fsync` every 200ms if the dirty flag is set
4. **Consolidate**: `WriteWALToChunk()` reads the WAL, deduplicates, and packs blocks into chunks
5. **Rotate**: After consolidation, WALNum increments and a new WAL file is opened

## WAL Recovery

On startup, `RecoverLocalWALs()` scans for unconsolidated WAL files:

1. Read each WAL file, validate headers and checksums
2. Replay valid block records into the block store
3. Rebuild the in-memory state from recovered blocks
4. Resume normal operation with consolidated state

---

# 6. Sharded WAL

The sharded WAL reduces write contention by splitting the WAL into 16 independent files, each with its own mutex. This eliminates lock contention when concurrent writes target different blocks.

```
                      Write(blockNum, data)
                              │
                              ▼
                    shardID = blockNum & 0xF
                              │
               ┌──────────────┼──────────────┐
               ▼              ▼              ▼
        ┌──────────┐   ┌──────────┐   ┌──────────┐
        │ Shard 0  │   │ Shard 1  │   │ Shard N  │
        │ (mutex)  │   │ (mutex)  │   │ (mutex)  │
        │ wal.shard│   │ wal.shard│   │ wal.shard│
        │ _00.bin  │   │ _01.bin  │   │ _15.bin  │
        └──────────┘   └──────────┘   └──────────┘
               │              │              │
               └──────────────┼──────────────┘
                              ▼
                   Consolidation merges all
                   shards into unified 4 MB
                   chunk files
```

| Property | Value |
|----------|-------|
| Shard count | 16 (matches UnifiedBlockStore shard count) |
| Shard routing | `blockNum & 0xF` (bitwise AND with mask) |
| Lock granularity | Per-shard mutex (zero contention between shards) |
| Dirty tracking | Per-shard dirty flag for selective `fsync` |
| File path | `{volume}/wal/chunks/wal.{walNum:08d}.shard_{shardID:02d}.bin` |

During consolidation, `WriteShardedWALToChunk()` reads all 16 shards, merges them, deduplicates by sequence number, and produces unified chunk files identical to the single-file WAL path.

---

# 7. Chunk Storage

WAL entries are consolidated into 4 MB chunk files for efficient backend storage.

## Chunk File Format

```
┌───────────────────────────────────────────────────────────────┐
│                      Chunk Header (10 bytes)                   │
├───────────┬───────────┬───────────────────────────────────────┤
│ Magic (4) │ Version(2)│ BlockSize (4)                          │
│  "VBCH"   │ uint16 BE │ uint32 BE                              │
└───────────┴───────────┴───────────────────────────────────────┘
┌───────────────────────────────────────────────────────────────┐
│                Block Data (sequential, packed)                  │
│                                                                │
│  Block 0 data (4096 bytes)                                     │
│  Block 1 data (4096 bytes)                                     │
│  Block 2 data (4096 bytes)                                     │
│  ...                                                           │
│  Block N data (4096 bytes)                                     │
│                                                                │
│  Total ≤ 4 MB (ObjBlockSize)                                   │
└───────────────────────────────────────────────────────────────┘
```

## Consolidation Process

1. **Read WAL**: Parse all block records, validate CRC32 checksums
2. **Deduplicate**: For blocks written multiple times, the highest sequence number wins
3. **Sort**: Order blocks by block number for sequential access patterns
4. **Pack**: Fill 4 MB chunk buffers with sorted block data
5. **Upload**: Write chunk files to the backend via `Backend.Write()`
6. **Index**: Update `BlocksToObject` mapping with extent entries

Chunk files are stored at `{volume}/chunks/chunk.{objectId:08d}.bin` and are immutable once written.

---

# 8. Block Mapping

Viperblock uses an extent-based block mapping inspired by ext4 extents. Instead of storing a separate entry per block, consecutive blocks in the same chunk are merged into a single extent.

```go
type BlockLookup struct {
    StartBlock   uint64  // First logical block in the extent
    NumBlocks    uint16  // Number of consecutive blocks
    ObjectID     uint64  // Chunk file containing the data
    ObjectOffset uint32  // Byte offset within the chunk file
}
```

## Extent Example

If blocks 100–109 are stored consecutively starting at offset 10 in chunk 5:

```
BlockLookup{
    StartBlock:   100,
    NumBlocks:    10,
    ObjectID:     5,
    ObjectOffset: 10,
}
```

A single extent entry replaces 10 individual block entries, reducing memory usage and improving lookup efficiency.

## Block-to-Object WAL

The block mapping is itself persisted via a dedicated WAL to survive restarts:

```
┌───────────────────────────────────────────────────────────────┐
│              Block-to-Object WAL Header (6 bytes)              │
├───────────┬───────────────────────────────────────────────────┤
│ Magic (4) │ Version (2)                                        │
│  "VBWB"   │ uint16 BE                                          │
└───────────┴───────────────────────────────────────────────────┘
┌───────────────────────────────────────────────────────────────┐
│              Entry (26 bytes each)                              │
├──────────────┬────────────┬──────────┬─────────────┬──────────┤
│ StartBlock(8)│NumBlocks(2)│ObjectID(8)│ObjOffset(4)│Padding(4)│
│ uint64 BE    │ uint16 BE  │uint64 BE │ uint32 BE   │          │
└──────────────┴────────────┴──────────┴─────────────┴──────────┘
```

On startup, `LoadBlockState()` replays this WAL to rebuild the `BlocksToObject` index.

---

# 9. Unified Block Store

The `UnifiedBlockStore` provides O(1) block lookups with 16-way sharded locking. It consolidates all block state tracking into a single data structure, replacing separate maps for hot, pending, persisted, and cached blocks.

```
┌─────────────────────────────────────────────────────────────────┐
│                    UnifiedBlockStore                             │
│                                                                 │
│  shardID = blockNum & 0xF                                       │
│                                                                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐     ┌──────────┐      │
│  │ Shard 0  │ │ Shard 1  │ │ Shard 2  │ ... │ Shard 15 │      │
│  │ (RWMutex)│ │ (RWMutex)│ │ (RWMutex)│     │ (RWMutex)│      │
│  │          │ │          │ │          │     │          │      │
│  │ map[     │ │ map[     │ │ map[     │     │ map[     │      │
│  │  uint64  │ │  uint64  │ │  uint64  │     │  uint64  │      │
│  │  ]*Block │ │  ]*Block │ │  ]*Block │     │  ]*Block │      │
│  │  Entry   │ │  Entry   │ │  Entry   │     │  Entry   │      │
│  └──────────┘ └──────────┘ └──────────┘     └──────────┘      │
│                                                                 │
│  Atomic counters: Reads, Writes, CacheHits, CacheMiss,         │
│                   HotReads, PendReads, BackendReads              │
└─────────────────────────────────────────────────────────────────┘
```

Each `BlockEntry` tracks:

```go
type BlockEntry struct {
    SeqNum       uint64     // Monotonic write sequence number
    State        BlockState // Empty | Hot | Pending | Persisted | Cached
    Data         []byte     // Block data (nil for Persisted state)
    ObjectID     uint64     // Chunk file ID (Persisted state)
    ObjectOffset uint32     // Offset within chunk (Persisted state)
}
```

State transitions are explicit: `Write()` → Hot, `MarkPending()` → Pending, `MarkPersisted()` → Persisted, `Cache()` → Cached. The block store never allows backward transitions.

---

# 10. Snapshots and Clones

Viperblock supports point-in-time snapshots and copy-on-write volume clones, enabling EBS-compatible `CreateSnapshot` and `CreateVolume` (from snapshot) operations.

## CreateSnapshot

```
CreateSnapshot(snapshotID)
        │
        ▼
┌─────────────────────────────────────────────┐
│  1. Flush all Hot blocks to WAL             │
│  2. Consolidate WAL → chunk files           │
│  3. Serialize BlocksToObject mapping        │
│     → checkpoint file on backend            │
│  4. Write SnapshotState metadata            │
│     → config.json on backend                │
│  5. Return SnapshotState                    │
└─────────────────────────────────────────────┘
```

No data is copied. The snapshot records a frozen reference to the source volume's existing chunk files plus a block-to-object checkpoint.

```go
type SnapshotState struct {
    SnapshotID       string
    SourceVolumeName string
    SeqNum           uint64
    ObjectNum        uint64
    BlockSize        uint32
    ObjBlockSize     uint32
    BlockCount       uint64
    CreatedAt        time.Time
}
```

## Copy-on-Write Clones

`OpenFromSnapshot()` creates a new volume backed by the snapshot's block map:

```
Clone Volume Read(block)
        │
        ▼
  ┌─ Check clone's own BlocksToObject
  │   (writes since clone creation)
  │
  ├─ Found → read from clone's chunks
  │
  └─ Not found → check BaseBlockMap
                   (frozen snapshot mapping)
                        │
                        ▼
              Read from source volume's
              chunk files (no data copy)
```

Writes always go to the clone's own storage. Only blocks that have been overwritten since cloning consume additional space.

---

# 11. Storage Backends

All backends implement the `Backend` interface:

```go
type Backend interface {
    Init() error
    Open(fname string) error
    Read(fileType FileType, objectId uint64, offset uint32, length uint32) ([]byte, error)
    Write(fileType FileType, objectId uint64, headers *[]byte, data *[]byte) error
    ReadFrom(volumeName string, fileType FileType, objectId uint64, offset uint32, length uint32) ([]byte, error)
    WriteTo(volumeName string, fileType FileType, objectId uint64, headers *[]byte, data *[]byte) error
    Sync()
    GetBackendType() string
    SetConfig(config any)
}
```

`ReadFrom` / `WriteTo` enable cross-volume operations required for snapshot clones (reading the source volume's chunks from a different volume context).

## File Backend

Local filesystem storage. Chunks are stored as individual files:

```
{baseDir}/{volumeName}/chunks/chunk.00000001.bin
{baseDir}/{volumeName}/wal/chunks/wal.00000001.bin
{baseDir}/{volumeName}/checkpoints/blocks.00000001.bin
{baseDir}/{volumeName}/config.json
```

Suitable for development, testing, and single-node deployments.

## Memory Backend

In-memory storage for unit tests and benchmarks. All data is lost on process exit.

## S3 Backend

Stores chunks in any S3-compatible object storage. Designed to work with [Predastore](https://github.com/mulgadc/predastore) as part of the Hive stack, but compatible with AWS S3 or other implementations.

Key optimizations:
- **HTTP/2**: Connection multiplexing reduces TLS handshake overhead
- **Connection pooling**: 200 max idle connections per host, 120s idle timeout
- **TLS session resumption**: LRU cache of 256 sessions
- **Timeouts**: TLS handshake 10s, response header 60s, expect-continue 1s
- **Self-signed cert support**: For on-premise Predastore deployments

---

# 12. NBD Plugin

Viperblock volumes are exposed to QEMU/KVM via an [nbdkit](https://gitlab.com/nbdkit/nbdkit) plugin compiled as a CGO shared library.

```
QEMU VM
  │
  │ NBD protocol (TCP or Unix socket)
  ▼
nbdkit server
  │
  │ Plugin API
  ▼
lib/nbdkit-viperblock-plugin.so
  │
  │ Go function calls
  ▼
Viperblock engine
```

The plugin implements the full nbdkit interface:

| Operation | Description |
|-----------|-------------|
| `PRead` | Read data at offset → `vb.ReadAt()` |
| `PWrite` | Write data at offset → `vb.WriteAt()` |
| `Flush` | Flush write buffer + WAL → `vb.Flush()` |
| `Zero` | Write zeros to a range |
| `Trim` | Discard a block range |

### Plugin Configuration Parameters

| Parameter | Description |
|-----------|-------------|
| `size` | Volume size in bytes |
| `volume` | Volume name |
| `bucket` | S3 bucket name |
| `region` | AWS region |
| `access_key` / `secret_key` | S3 credentials |
| `host` | S3 endpoint URL |
| `base_dir` | Local storage directory |
| `cache_size` | LRU cache size (percentage of system memory) |
| `shardwal` | Enable sharded WAL (`true`/`false`) |

---

# 13. NATS Integration

When deployed with [Hive](https://github.com/mulgadc/hive), Viperblock subscribes to NATS topics for EBS-compatible volume lifecycle management:

| Topic | Purpose |
|-------|---------|
| `ebs.createvolume` | Create a new volume |
| `ebs.describevolumes` | List volume metadata |
| `ebs.attachvolume` | Attach volume to an instance |
| `ebs.detachvolume` | Detach volume from an instance |
| `ebs.mount` | Mount volume via NBD — returns NBD URI for QEMU |
| `ebs.unmount` | Unmount volume, flush, and close |

The Hive daemon sends a mount request over NATS; Viperblock starts an nbdkit process with the plugin and returns the NBD URI. QEMU uses this URI to back a virtual disk.

---

# 14. Memory Management

## Arena Allocator

The arena allocator reduces GC pressure for write-path allocations by using bump-pointer allocation within fixed-size slabs.

```
┌──────────────────────────────────────┐
│            Arena                      │
│                                      │
│  ┌────────────────────────────────┐  │
│  │ Active Slab (4 MB)            │  │
│  │ [████████████░░░░░░░░░░░░░░░] │  │
│  │  ↑ allocated    ↑ free        │  │
│  └────────────────────────────────┘  │
│  ┌────────────────────────────────┐  │
│  │ Full Slab (4 MB)              │  │
│  │ [████████████████████████████] │  │
│  │  refs: 847                    │  │
│  └────────────────────────────────┘  │
│                                      │
│  Compact() removes slabs with        │
│  zero references                     │
└──────────────────────────────────────┘
```

| Property | Value |
|----------|-------|
| Slab size | 4 MB (configurable) |
| Allocation | O(1) bump pointer |
| Lifecycle | Reference-counted slabs |
| Cleanup | `Compact()` frees empty slabs |

## LRU Cache

An LRU cache (`github.com/hashicorp/golang-lru`) stores clean blocks read from the backend. The cache can be sized by:

- **Fixed count**: Specific number of blocks
- **System memory percentage**: Auto-sizes based on available RAM

---

# 15. Binary Formats

## Magic Bytes

| Format | Magic | ASCII |
|--------|-------|-------|
| Chunk data | `0x56 0x42 0x43 0x48` | `VBCH` |
| WAL data | `0x56 0x42 0x57 0x4C` | `VBWL` |
| Block mapping WAL | `0x56 0x42 0x57 0x42` | `VBWB` |

## File Types and Storage Paths

| FileType | Path Pattern |
|----------|--------------|
| Config | `{volume}/config.json` |
| Chunk | `{volume}/chunks/chunk.{objectId:08d}.bin` |
| Block Checkpoint | `{volume}/checkpoints/blocks.{objectId:08d}.bin` |
| WAL Chunk | `{volume}/wal/chunks/wal.{walNum:08d}.bin` |
| WAL Block Map | `{volume}/wal/blocks/blocks.{walNum:08d}.bin` |
| Sharded WAL | `{volume}/wal/chunks/wal.{walNum:08d}.shard_{shardID:02d}.bin` |

---

# 16. Configuration Reference

## VBState (Persisted Volume State)

| Field | Type | Description |
|-------|------|-------------|
| `VolumeName` | string | Unique volume identifier |
| `VolumeSize` | uint64 | Volume size in bytes |
| `BlockSize` | uint32 | Block size (default 4096) |
| `ObjBlockSize` | uint32 | Chunk size (default 4 MB) |
| `SeqNum` | uint64 | Next sequence number |
| `ObjectNum` | uint64 | Next chunk object number |
| `WALNum` | uint64 | Current WAL file number |
| `Version` | uint16 | Format version |
| `ShardedWAL` | bool | Whether sharded WAL is enabled |
| `SnapshotID` | string | Source snapshot ID (if cloned) |
| `SourceVolumeName` | string | Source volume (if cloned) |

## Volume Metadata (EBS-compatible)

| Field | Description |
|-------|-------------|
| `VolumeID` | AWS-compatible volume ID (vol-xxxxx) |
| `SizeGiB` | Volume size in GiB |
| `State` | available, in-use, creating, deleting |
| `VolumeType` | gp2, io1, etc. |
| `AvailabilityZone` | Placement zone |
| `AttachedInstance` | EC2 instance ID |
| `SnapshotID` | Source snapshot |

---

# 17. File Structure

```
viperblock/
├── viperblock/              # Core library
│   ├── viperblock.go        # VB struct, New(), Write, Read, Flush, state management
│   ├── blockstore.go        # UnifiedBlockStore (16-shard O(1) index)
│   ├── wal.go               # WAL and ShardedWAL operations
│   ├── snapshot.go          # CreateSnapshot, OpenFromSnapshot, LoadSnapshotBlockMap
│   ├── arena.go             # Bump-pointer allocator (Arena, PooledArena)
│   ├── cache.go             # LRU cache wrapper
│   └── backends/            # Storage backend implementations
│       ├── file/file.go     # Local filesystem backend
│       ├── memory/memory.go # In-memory backend (testing)
│       └── s3/s3.go         # S3-compatible backend
├── types/                   # Shared type definitions, Backend interface, FileType
├── nbd/                     # nbdkit plugin (CGO shared library)
│   └── viperblock.go        # Plugin entry point, PRead/PWrite/Flush
├── cmd/
│   ├── sfs/                 # Simple File System demo
│   └── vblock/              # Volume management CLI
└── tests/                   # Integration test suites
```

---

# 18. References

The following research aided the design and implementation of Viperblock:

- Hajkazemi, Mohammad Hossein, et al. "Beating the I/O bottleneck: a case for log-structured virtual disks." *Proceedings of the Seventeenth European Conference on Computer Systems.* 2022. https://doi.org/10.1145/3492321.3524271
- Zhou, Diyu, et al. "Enabling high-performance and secure userspace NVM file systems with the trio architecture." *Proceedings of the 29th Symposium on Operating Systems Principles.* 2023. https://doi.org/10.1145/3600006.3613171
- Li, Huiba, et al. "Ursa: Hybrid block storage for cloud-scale virtual disks." *Proceedings of the Fourteenth EuroSys Conference 2019.* 2019. https://doi.org/10.1145/3302424.3303967
