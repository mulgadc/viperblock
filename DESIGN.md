# Viperblock Architecture

Viperblock is a high-performance, WAL-backed block storage engine that provides EBS-compatible volumes for [Spinifex](https://github.com/mulgadc/spinifex). It combines an in-memory write buffer, a write-ahead log on fast local storage, and batched chunk uploads to a pluggable backend (local filesystem, S3, or [Predastore](https://github.com/mulgadc/predastore)). Volumes are exposed to QEMU/KVM virtual machines via an nbdkit plugin over the NBD protocol.

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
18. [Encryption at Rest](#18-encryption-at-rest)
19. [References](#19-references)

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

## Chunk Garbage Collection

A chunk holds many blocks (`ObjBlockSize`, multi-MB) behind one `BlockLookup` entry per logical block (`ObjectID` field). Overwriting a block repoints its map entry at a new chunk; the old chunk is left on the backend. A chunk is **wholly unreferenced** once no live map entry names its `ObjectID` anywhere — every block it ever held has been superseded. Because chunk IDs are minted from a monotonic counter (`vb.ObjectNum`) and never reused, once a chunk is wholly unreferenced it stays that way for the rest of the volume's life: no future write can ever re-reference it. Chunk GC deletes exactly this class of object. It does not compact **partially**-garbage chunks (some blocks live, some superseded) — that requires read-modify-write repacking and is a separate, future phase.

**Reference tracking.** `VB` maintains `gcRefcount map[uint64]uint64`, one entry per chunk `ObjectID` currently named by at least one `BlockLookup`, counting how many block entries name it. `createChunkFile` increments the new chunk's count and decrements the superseded chunk's count for every block it repoints; a count reaching zero marks the chunk swept-candidate (the entry is left in the map at zero rather than deleted, so a sweep can find it). `parseBlockCheckpoint` rebuilds this table from scratch on every checkpoint load, so it is always derivable from the current live map and needs no separate persistence. Only maintained when `GCEnabled` is true.

**Sweep timing.** Deleting a chunk is only safe once the durable checkpoint that stops referencing it has landed — otherwise a crash regresses recovery to an older checkpoint that still points at a now-deleted chunk. The sweep therefore only ever runs immediately after a successful `SaveLiveCheckpointCtx` (as the last step of `DrainToBackendCtx`), never from `createChunkFile` (which runs under the parallel chunk-upload pool and cannot see a checkpoint-durable, coalesced view of the map — the same reason per-chunk live-checkpoint writes were removed from that path). It runs from the existing chunk-uploader goroutine on its own `GCInterval` ticker (default 5 minutes, decoupled from the 30-second `ChunkUploadInterval`; `<= 0` disables the periodic sweep), plus once more on `Close`.

**Watermark clamp.** A sweep captures `watermark := vb.ObjectNum.Load()` once, at mark time, and only considers `ObjectID < watermark`. A chunk minted after that point is structurally excluded, without needing a mutex between writers and the sweep: unreferenced-is-terminal means a chunk that was live at mark time is still correctly excluded (nonzero refcount) at sweep time, and a chunk minted after mark time fails the watermark check outright.

**The snapshot-ancestry hazard.** Snapshots do not copy data — `CreateSnapshot` freezes a checkpoint that references the source volume's existing chunks in place, and clone reads fetch those chunks directly from the source volume's prefix. A GC that only consults the live map would delete exactly the chunks a snapshot (or any clone/AMI descended from it) still needs the moment the source volume supersedes them — silent, cross-volume data loss. Two guards close this:

- **Own-prefix only, structurally.** `Backend.Delete` has no `DeleteTo`/`DeleteFrom` cross-volume form; the interface makes it impossible for a volume to delete another volume's chunk, by construction.
- **Binary snapshot-existence guard.** Before a volume's first sweep, `ensureGCSnapshotSafe` lists every top-level `snap-*` prefix in the backend (snapshots are NOT nested under the source volume's own prefix — they are independently-keyed, top-level siblings) and reads each candidate's `config.json` (via `StateBody`, which needs no decryption key — the metadata envelope authenticates, it does not encrypt) to compare `SourceVolumeName`. If **any** existing snapshot references this volume, GC is disabled entirely for it — no arithmetic floor derived from snapshot state, just a binary yes/no. The result is cached for the process lifetime once a scan succeeds. If `CreateSnapshot` is ever called on a GC-enabled instance afterward, GC is latched off permanently for that instance regardless of scan timing.

This guard is deliberately conservative and does not attempt to distinguish "a snapshot exists but doesn't reference the chunks about to be swept" — any existing snapshot of the volume disables GC for it outright. It also does not cover a **different process** creating a snapshot of this volume after the scan already ran and cached "safe" — that cross-process window has no fix in this phase, which is why `GCEnabled` defaults to `false` and is opt-in per volume, only for volumes an operator can establish have no snapshot descendants (an AMI-derived boot volume is typically such a volume: it has a parent, but no snapshots *of itself*).

**Numbered-checkpoint floor.** `LoadLiveCheckpointCtx` falls back to the older, numbered checkpoint (`checkpoints/blocks.%08d.bin`) when the live checkpoint is unreadable. That numbered checkpoint references chunks that may already be zero-refcount in the current live map. `ensureGCFloor` reads the current `WallNum`'s numbered checkpoint once per process lifetime and sets `GCFloor` to one past its highest referenced `ObjectID`, so GC never deletes anything that fallback path could still need. Net candidate predicate: **`GCFloor <= ObjectID < watermark` AND `refcount == 0`**.

`Reset()` (which re-issues `ObjectID` 0) and `CreateSnapshot` both latch GC off permanently for the instance they run on, since both invalidate an invariant the design depends on (ID reuse, and snapshot ancestry, respectively).

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

Stores chunks in any S3-compatible object storage. Designed to work with [Predastore](https://github.com/mulgadc/predastore) as part of the Spinifex stack, but compatible with AWS S3 or other implementations.

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
| `gc_enabled` | Enable chunk garbage collection (`true`/`false`, default `false`) |

---

# 13. NATS Integration

When deployed with [Spinifex](https://github.com/mulgadc/spinifex), Viperblock subscribes to NATS topics for EBS-compatible volume lifecycle management:

| Topic | Purpose |
|-------|---------|
| `ebs.createvolume` | Create a new volume |
| `ebs.describevolumes` | List volume metadata |
| `ebs.attachvolume` | Attach volume to an instance |
| `ebs.detachvolume` | Detach volume from an instance |
| `ebs.mount` | Mount volume via NBD — returns NBD URI for QEMU |
| `ebs.unmount` | Unmount volume, flush, and close |

The Spinifex daemon sends a mount request over NATS; Viperblock starts an nbdkit process with the plugin and returns the NBD URI. QEMU uses this URI to back a virtual disk.

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

| Format | Magic | ASCII | Notes |
|--------|-------|-------|-------|
| Chunk data (plaintext) | `0x56 0x42 0x43 0x48` | `VBCH` | Pre-encryption layout |
| Chunk data (encrypted) | `0x56 0x42 0x43 0x45` | `VBCE` | AES-256-GCM sealed per block, see §18 |
| WAL data (plaintext) | `0x56 0x42 0x57 0x4C` | `VBWL` | Pre-encryption layout; sharded WAL retains this |
| WAL data (encrypted) | `0x56 0x42 0x57 0x45` | `VBWE` | AES-256-GCM sealed per record (single-file WAL only) |
| Block mapping WAL | `0x56 0x42 0x57 0x42` | `VBWB` | Plaintext + CRC32 in both modes (metadata, not FCI) |

Volumes are mode-locked at creation: an encrypted volume never sees `VBCH`/`VBWL`, and an unencrypted volume never sees `VBCE`/`VBWE`. A magic mismatch on read is a migration error — there is no in-place upgrade path.

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
| `EncryptionEnabled` | bool | Volume is sealed under the operator master key (see §18) |
| `VolumeUUID` | [4]byte | Per-volume nonce subspace, minted via `crypto/rand` on first SaveState |
| `SeqNumHighWater` | uint64 | Crash-safe nonce-uniqueness reservation; SeqNum resumes here on Open |
| `StateSeqNum` | uint64 | Monotonic SaveState generation, bound into the VBState metadata HMAC |
| `KeyFingerprint` | string | 16 hex chars of the master key fingerprint; mismatched key fails Open |

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

# 18. Encryption at Rest

Viperblock seals every byte of FCI to its live code paths under AES-256-GCM using a node-shared master key, and HMACs `VBState` and `SnapshotState` metadata so a backend writer cannot pivot one volume onto another's blocks. This closes the CMMC L1 practices MP.L1-3.8.3 (cryptographic erase at node-decommission granularity), SI.L1-3.14.2 (chunk + WAL integrity via the 16-byte AEAD tag), and SC.L1-3.13.1 (backend payload is opaque ciphertext).

## Scope

In scope: single-file WAL, chunk write/read, `VBState` / `SnapshotState` metadata HMAC. The encryption-enabled `Open` refuses `UseShardedWAL=true`; the sharded path stays plaintext until a follow-up bead lifts the gate. `WriteBlockWAL` (the block-to-object streaming path) is a dead stub and is not encrypted. The block-to-object checkpoint stays plaintext (it is metadata, not FCI).

## Master key

The master key is a 32-byte raw key file, loaded via `predastore/pkg/masterkey.LoadShared` (group-readable 0640 OK, world-accessible refused). The same file serves predastore and viperblock under different unix users via a shared group. Viperblock receives the file path at startup (`-encryption-key-file` / `ENCRYPTION_KEY_FILE`); raw key bytes are never retained — `LoadShared` returns `*Key{AEAD, Fingerprint}` and viperblock caches the AEAD on `VB.aead` for the hot path.

**Crypto-erase granularity is the node, not the volume.** Destroying the master key file renders every viperblock volume and every predastore object on that node unrecoverable. Per-volume `DeleteVolume` is a logical delete; whole-node decommission is the operator path for MP.L1-3.8.3. Per-volume crypto-erase via envelope encryption is a follow-up.

## Nonce construction

GCM nonces are 12 bytes (96 bits). Reuse with the same key is catastrophic (NIST SP 800-38D §8.3), so the construction uses three disjoint subspaces under the shared master key:

```
nonce[0:7]   = BE(SeqNum)         56 bits of monotonic counter
nonce[7:11]  = VolumeUUID         4 random bytes per volume, persisted in VBState
nonce[11]    = domain             0x00 chunk | 0x01 WAL | 0x02 vbstate-meta | 0x03 snapshot-meta
```

The 56-bit SeqNum gives ~2,283 years at 1M writes/sec per volume; `reserveSeqNum` refuses past `MaxSeqNum = (1<<56)-1`. `VolumeUUID` is minted via `crypto/rand` on first `SaveState` of an encrypted volume and persisted before `Open` returns. The domain byte keeps a WAL record sealed at `(SeqNum=N, VolumeUUID=V)` from colliding with the chunk block built from it at the same SeqNum / UUID after consolidation.

`SeqNumHighWater` is the crash-safe nonce-uniqueness mechanism: `reserveSeqNum` hands out values lock-free until the high-water is crossed, then takes a mutex, advances by `seqNumReservation = 2^20`, and `SaveState`s before releasing. A kill -9 loses up to one reservation window of values, but every SeqNum handed out before the crash sits below the persisted high-water and cannot be re-issued.

## AAD construction

Data-domain AAD is 48 bytes — `SHA256(VolumeName) || BE(blockNum_8) || BE(seqNum_8)`. The volume hash defeats cross-volume swap; the blockNum defeats positional shuffle; the seqNum defeats in-place rollback.

Metadata AAD substitutes a domain tag for the block slot: `SHA256(VolumeName) || domainTag || BE(StateSeqNum_8)`, where `domainTag` is `"vbstate"` for `VBState` and `"snap:"||snapshotID` for `SnapshotState`. The literal separator stops a snapshot blob from being accepted as a `VBState` blob and vice versa.

## Encrypted WAL record (single-file WAL only, magic `VBWE`)

```
[SeqNum(8) | BlockNum(8) | BlockLen(8) | ciphertext(BlockLen) | tag(16)]
= 40 + BlockLen bytes per record
```

CRC32 is deleted; the 16-byte GCM tag subsumes it. Nonce: `(SeqNum, VolumeUUID, 0x01)`. AAD: `(volumeNameHash, BlockNum, SeqNum)`. The WAL file header is `[VBWE(4) | version(2) | BlockSize(4) | timestamp(8)]` — same 18-byte shape as `VBWL`.

## Encrypted chunk block (chunk magic `VBCE`)

```
[Chunk header(10): VBCE(4) | version(2) | BlockSize(4)]
[block 0 ciphertext(BlockSize) | tag(16)]
[block 1 ciphertext(BlockSize) | tag(16)]
...
```

Stride = `BlockSize + 16` = 4112 bytes at default 4 KiB. Per-block nonce: `(block.SeqNum, VolumeUUID, 0x00)`. AAD: `(volumeNameHash, block.Block, block.SeqNum)`. No on-disk SeqNum, no on-disk nonce — both reconstructible from `BlockLookup.SeqNum` (which Stage 2 added to the persisted checkpoint) and `vb.VolumeUUID`.

`BlockLookup` grew from 22 + 4 (CRC32) = 26 bytes per entry to 30 + 4 = 34 bytes to carry the SeqNum needed for nonce reconstruction.

## Metadata HMAC — AES-GCM as MAC

`VBState` and `SnapshotState` ship plaintext-readable on disk (operators can `cat config.json`) but gain a 16-byte trailing tag. `sealMeta` calls `aead.Seal(nil, nonce, nil, aad || jsonBytes)` — the JSON content is part of the covered AAD even though it ships in the clear, so any byte modification breaks verification.

```
saveMeta:
  jsonBytes := json.Marshal(state)
  nonce := makeNonce(StateSeqNum, VolumeUUID, 0x02 or 0x03)
  aad   := makeMetaAAD(volumeNameHash, "vbstate" or "snap:"||snapshotID, StateSeqNum)
  tag   := aead.Seal(nil, nonce, nil, aad || jsonBytes)
  write: jsonBytes || tag

loadMeta:
  blob := read
  jsonBytes, tag := blob[:len-16], blob[len-16:]
  peek StateSeqNum + VolumeUUID from jsonBytes (and KeyFingerprint for clear errors)
  nonce, aad := reconstruct
  aead.Open(nil, nonce, tag, aad || jsonBytes)  → ErrIntegrity on mismatch
```

Rollback protection composes with the existing `LoadState` tie-break (highest `SeqNum` between local-fsync and backend wins): HMAC blocks cross-volume substitution and bit-flips, the SeqNum comparison blocks replay of an older authentic backend blob. The local copy is written atomically via tmp + fsync + rename + fsync(parent dir).

## Snapshot identity

`CreateSnapshot` persists the source volume's `VolumeUUID` (hex string, 8 chars) and `volumeNameHash` (hex string, 64 chars) into `SnapshotState`, plus the snapshot's own `StateSeqNum` for the snapshot-meta HMAC nonce. `OpenFromSnapshot` decodes these into `vb.SourceVolumeUUID` and `vb.sourceVolumeNameHash`; `fetchBaseBlocksFromBackend` uses them (not the clone's own identity) when decrypting source chunks. The clone's own writes seal under the clone's identity. This makes snapshots self-describing: a snapshot is readable as long as the source's chunks exist on the backend, independent of whether the source volume's `VBState` is still loadable.

## Threat model

**In scope:** confidentiality of guest block data against an attacker reading raw chunk files on the backend or raw WAL files on local NVMe; integrity/authenticity of WAL records and chunk blocks against an attacker who can modify them in place; cross-volume metadata pivot (a `VBState` or `SnapshotState` blob authentic for volume B spliced into volume A's location); within-volume positional shuffle; in-place rollback.

**Out of scope:** compromise of a running viperblock host with the master key resident in memory; per-volume cryptographic erase (envelope encryption is the follow-up); migration of existing unencrypted volumes (hard cutover); encryption of the block-to-object checkpoint (metadata, not FCI); encryption of `VBState`/`SnapshotState` JSON itself (HMAC only — operators must still be able to `cat config.json`); the sharded WAL path; inbound NBD authentication.

## Migration

There is no in-place upgrade. A post-cutover binary refuses to open a volume with `VBCH` chunk magic or `VBWL` WAL magic under `EncryptionEnabled=true` (magic-mismatch error, then a migration message). Operators convert by creating a new encrypted volume, copying data across via guest-side tooling (`dd`, filesystem clone), and deleting the old volume. The decision is per-volume — an encrypted volume is encrypted end-to-end; an unencrypted volume runs the pre-encryption code path unchanged.

---

# 19. References

The following research aided the design and implementation of Viperblock:

- Hajkazemi, Mohammad Hossein, et al. "Beating the I/O bottleneck: a case for log-structured virtual disks." *Proceedings of the Seventeenth European Conference on Computer Systems.* 2022. https://doi.org/10.1145/3492321.3524271
- Zhou, Diyu, et al. "Enabling high-performance and secure userspace NVM file systems with the trio architecture." *Proceedings of the 29th Symposium on Operating Systems Principles.* 2023. https://doi.org/10.1145/3600006.3613171
- Li, Huiba, et al. "Ursa: Hybrid block storage for cloud-scale virtual disks." *Proceedings of the Fourteenth EuroSys Conference 2019.* 2019. https://doi.org/10.1145/3302424.3303967
