package viperblock

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"maps"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/telemetry"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/utils"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

const DefaultBlockSize uint32 = 4096
const DefaultObjBlockSize uint32 = 1024 * 1024 * 4
const DefaultFlushInterval time.Duration = 5 * time.Second
const DefaultFlushSize uint32 = 64 * 1024 * 1024
const DefaultWALSyncInterval time.Duration = 200 * time.Millisecond
const DefaultChunkUploadInterval time.Duration = 30 * time.Second

// DefaultGCInterval is how often the background chunk uploader also runs a
// chunk GC sweep when GCEnabled is true and GCInterval is left at its zero
// value. Deliberately much longer than DefaultChunkUploadInterval — a sweep
// is a bucket-wide-scan-adjacent operation the first time it runs per VB
// lifetime (see ensureGCSnapshotSafe) and superseded chunks are not urgent
// to reclaim the moment they go stale.
const DefaultGCInterval time.Duration = 5 * time.Minute

// DefaultMaxPendingBytes bounds the combined size of Writes.Blocks and
// PendingBackendWrites.Blocks — the guest-write backpressure high-watermark.
// WriteAtCtx blocks the caller once this is crossed, draining synchronously
// until back under MaxPendingBytes/2, so a sustained guest write self-throttles
// to backend ingest rate instead of ballooning unbounded anonymous memory.
const DefaultMaxPendingBytes uint64 = 256 * 1024 * 1024

// DefaultUploadWorkers bounds the parallel chunk-upload worker pool used by
// WriteWALToChunkCtx / WriteShardedWALToChunkCtx. 1 means fully serial.
const DefaultUploadWorkers int = 16

// seqNumReservation is the size of each SeqNum high-water reservation window.
// reserveSeqNum hands out values lock-free via atomic.Add until the persisted
// high-water is crossed; the slow path advances by seqNumReservation and
// fsyncs VBState before any of the newly reserved values are handed out. A
// kill -9 between the bump and the next persist can lose up to one window;
// the loaded SeqNum on restart starts from the persisted high-water, so no
// value handed out before the crash is reused. 1<<20 keeps the slow path off
// the per-write critical path (one fsync per million writes).
const seqNumReservation uint64 = 1 << 20

type VBState struct {
	VolumeName string `json:"VolumeName"`
	VolumeSize uint64 `json:"VolumeSize"`

	BlockSize    uint32 `json:"BlockSize"`
	ObjBlockSize uint32 `json:"ObjBlockSize"`

	SeqNum    uint64 `json:"SeqNum"`
	ObjectNum uint64 `json:"ObjectNum"`
	WALNum    uint64 `json:"WALNum"`

	BlockToObjectWALNum uint64 `json:"BlockToObjectWALNum"`

	Version uint16 `json:"Version"`

	VolumeConfig VolumeConfig `json:"VolumeConfig"`

	ShardedWAL bool `json:"ShardedWAL,omitempty"`

	SnapshotID       string `json:"SnapshotID,omitempty"`
	SourceVolumeName string `json:"SourceVolumeName,omitempty"`

	// Encryption-at-rest fields (populated when EncryptionEnabled is true).
	// EncryptionEnabled is the authoritative persistent flag. VolumeUUID seeds
	// the per-volume nonce subspace and is generated via crypto/rand on first
	// SaveState of an encrypted volume. SeqNumHighWater is the durable upper
	// bound on the next SeqNum the volume may hand out post-restart — crash-safe
	// nonce uniqueness. StateSeqNum is a monotonic SaveState generation counter
	// used by the metadata HMAC and to break ties in LoadState.
	// KeyFingerprint surfaces master-key mismatch at Open with a clear error
	// instead of silent decrypt failure.
	EncryptionEnabled bool    `json:"EncryptionEnabled,omitempty"`
	VolumeUUID        [4]byte `json:"VolumeUUID,omitempty"`
	SeqNumHighWater   uint64  `json:"SeqNumHighWater,omitempty"`
	StateSeqNum       uint64  `json:"StateSeqNum,omitempty"`
	KeyFingerprint    string  `json:"KeyFingerprint,omitempty"`
}

// snapshotAncestor carries one level of the flattened read chain for a COW clone.
// Index 0 is the grandparent (parent's base), with deeper ancestors at higher indices.
type snapshotAncestor struct {
	blocks               *BlocksToObject
	sourceVolumeName     string
	sourceVolumeUUID     [4]byte
	sourceVolumeNameHash [32]byte
}

type VB struct {
	VolumeName string
	VolumeSize uint64

	BlockSize    uint32
	ObjBlockSize uint32

	FlushInterval time.Duration
	FlushSize     uint32

	// UploadWorkers bounds the parallel chunk-upload worker pool used when
	// draining WAL/ShardedWAL to backend chunks. 0 uses DefaultUploadWorkers;
	// 1 runs uploads serially. Negative values are clamped to 1 in New().
	UploadWorkers int

	// WALSyncInterval controls periodic fsync of WAL to disk (default 200ms)
	// Inspired by PostgreSQL's wal_writer_delay, BadgerDB's SyncWrites, MongoDB's journalCommitInterval
	WALSyncInterval time.Duration

	// WAL syncer control (background goroutine for periodic fsync)
	walSyncTicker *time.Ticker
	walSyncStop   chan struct{}
	walSyncDone   chan struct{}

	// ChunkUploadInterval controls how often the background goroutine flushes WAL
	// chunks and the live checkpoint to S3 (default 30s). <= 0 disables.
	ChunkUploadInterval time.Duration

	chunkUploadTicker *time.Ticker
	chunkUploadStop   chan struct{}
	chunkUploadDone   chan struct{}

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

	// MaxPendingBytes bounds outstanding buffered write bytes (Writes.Blocks +
	// PendingBackendWrites.Blocks). 0 uses DefaultMaxPendingBytes. WriteAtCtx
	// blocks once pendingBytes crosses this until a synchronous drain brings
	// it back under MaxPendingBytes/2 — see awaitBackpressure.
	MaxPendingBytes uint64

	// pendingBytes tracks outstanding buffered write bytes across
	// Writes.Blocks and PendingBackendWrites.Blocks. Incremented in
	// WriteAtCtx when writes land in Writes.Blocks, decremented in
	// createChunkFile when blocks are evicted from PendingBackendWrites
	// after a successful chunk upload. Atomic and read without the
	// Writes/PendingBackendWrites locks: an approximate watermark is fine,
	// exact linearizability with the slices is not required.
	pendingBytes atomic.Int64

	// drainInFlight ensures at most one goroutine actively drives
	// DrainToBackendCtx from inside awaitBackpressure at a time; concurrent
	// blocked writers poll pendingBytes instead of each launching a
	// redundant drain. It is only an optimization for the backpressure path;
	// drainMu below is the actual mutual-exclusion guarantee across every
	// drain trigger.
	drainInFlight atomic.Bool

	// drainMu serializes DrainToBackendCtx across ALL its triggers
	// (backpressure, ChunkUploadInterval ticker, size trigger, and the GC
	// sweep). Two concurrent drains each rotate to a different sequential WAL
	// segment and run WriteWALToChunk in parallel; because createChunkFile
	// installs BlockLookup entries by block number, an older segment's write
	// completing last would clobber a newer chunk's live pointer and (via
	// gcTrackBlock) drop that chunk's refcount to zero, letting GC delete a
	// still-referenced chunk. Holding this for the whole drain body keeps at
	// most one WriteWALToChunk in flight, so no two createChunkFile calls ever
	// contend for the same block. createChunkFile's own per-entry SeqNum guard
	// is the belt to this mutex's braces.
	drainMu sync.Mutex

	// chunkUploadTrigger lets WriteAtCtx ask the background chunk uploader
	// to drain now instead of waiting for the next ChunkUploadInterval tick,
	// once pendingBytes crosses FlushSize. Buffered 1: a pending trigger
	// coalesces repeated sends. nil-safe to send on only after
	// StartChunkUploader has run.
	chunkUploadTrigger chan struct{}

	// Periodically writes (Blocks) are flushed to the WAL log (default 5s, or when 64MB written, or OS flushes)
	WAL WAL

	// ShardedWAL splits the WAL into NumShards parallel files for reduced lock contention.
	// When UseShardedWAL is true, writes route through ShardedWAL instead of WAL.
	ShardedWAL *ShardedWAL

	// UseShardedWAL enables the sharded WAL for write operations.
	// When false, uses the legacy single-file WAL for backward compatibility.
	UseShardedWAL bool

	// BlockToObject WAL, stored on persistent storage for redundancy and periodic checkpointing
	BlockToObjectWAL WAL

	// In-memory cache of recently used blocks from read/write operations
	Cache Cache

	// UnifiedBlockStore provides O(1) block lookups with sharded locking
	// Replaces Writes, PendingBackendWrites, Cache, and BlocksToObject lookups
	// when UseBlockStore is true
	BlockStore *UnifiedBlockStore

	// UseBlockStore enables the unified block store for read/write operations
	// When false, uses legacy data structures for backward compatibility
	UseBlockStore bool

	Version uint16

	ChunkMagic [4]byte

	Backend types.Backend

	BaseDir string

	VolumeConfig VolumeConfig

	// Logger, if set, is used for this instance's log lines instead of
	// slog.Default(). A library must never mutate its caller's global
	// logger, so New copies this (or slog.Default() when nil) into log
	// rather than calling slog.SetDefault.
	Logger *slog.Logger

	// log is the instance logger every VB method logs through. Set once by
	// New and only ever rebuilt in place by SetDebug.
	log *slog.Logger

	// BaseBlockMap is the frozen block-to-object map from a parent snapshot.
	// Used for copy-on-write clones: if a block isn't in our own BlocksToObject,
	// fall back to this map. Nil for non-cloned volumes.
	BaseBlockMap *BlocksToObject

	// SourceVolumeName is the volume whose chunk files the BaseBlockMap references.
	// Reads that hit BaseBlockMap fetch from this volume's chunks, not our own.
	SourceVolumeName string

	// SnapshotID is set when this volume was created from a snapshot.
	SnapshotID string

	// ancestors holds the flattened read chain beyond the immediate base,
	// populated from the inherited-layers section of the flat snapshot checkpoint
	// at OpenFromSnapshot time. Index 0 = grandparent, higher = deeper.
	ancestors []snapshotAncestor

	// Encryption-at-rest. MasterKey is supplied by the caller (NBD plugin /
	// CLI) via masterkey.LoadShared; aead caches MasterKey.AEAD for the hot
	// path. EncryptionEnabled is the caller-supplied flag for this volume; it
	// must agree with VBState.EncryptionEnabled on LoadState or LoadState
	// refuses. volumeNameHash is SHA256(VolumeName) cached at New for AAD
	// construction. VolumeUUID is the per-volume nonce subspace, persisted in
	// VBState. SourceVolumeUUID / sourceVolumeNameHash carry the source
	// volume's identity when this is a snapshot clone, populated from
	// SnapshotState on OpenFromSnapshot and zero otherwise.
	MasterKey            *masterkey.Key
	EncryptionEnabled    bool
	aead                 cipher.AEAD
	volumeNameHash       [32]byte
	VolumeUUID           [4]byte
	SourceVolumeUUID     [4]byte
	sourceVolumeNameHash [32]byte

	// chunkMagicChecked memoises the per-(volume,objectID) result of the chunk
	// magic preflight in checkChunkMagic. The chunk-read fast path is hit once
	// per consecutive run; without this cache every coalesced run would issue
	// an extra 4-byte backend Read just to validate the header. Keyed by
	// "<volumeName>:<objectID>" so snapshot-clone reads against the source
	// volume's chunks don't collide with our own. Stores struct{} for
	// validated-OK; errors are not cached (transient backend failures must
	// re-try).
	chunkMagicChecked sync.Map

	// SeqNumHighWater is the in-memory mirror of VBState.SeqNumHighWater —
	// the largest SeqNum that may be handed out without persisting a new
	// VBState. reserveSeqNum hands out values lock-free via SeqNum.Add until
	// the high-water is crossed; the slow path takes seqNumHighWaterMu,
	// advances by seqNumReservation, and SaveStates before releasing.
	seqNumHighWater   atomic.Uint64
	seqNumHighWaterMu sync.Mutex

	// nextStateSeqNum is the monotonic SaveState generation counter mirrored
	// from VBState.StateSeqNum. Each SaveState increments it before marshal;
	// the value is bound into the VBState metadata HMAC and breaks ties when
	// LoadState picks between local and backend copies.
	nextStateSeqNum atomic.Uint64

	// saveStateMu serialises persistStateLocal so the StateSeqNum bump,
	// marshal, and atomic rename are observed in monotonic order on disk.
	// Without serialisation two concurrent SaveStates could mint StateSeqNum
	// N and N+1 then rename in either order, leaving the lower StateSeqNum
	// as the on-disk winner. Held only across the marshal+fsync path; not
	// shared with the read-path mutex so LookupBlockToObject and friends
	// stay uncontended.
	saveStateMu sync.Mutex

	// GCEnabled turns on chunk garbage collection: deleting superseded
	// chunk objects that no live block references. Default false — GC is
	// opt-in until the cross-process "another process snapshots this
	// volume mid-run" window (see ensureGCSnapshotSafe) has a durable
	// answer, not just the in-process latch CreateSnapshot sets below.
	GCEnabled bool

	// GCInterval controls how often the background chunk uploader goroutine
	// also runs a GC sweep (default DefaultGCInterval). <= 0 disables the
	// periodic sweep even when GCEnabled is true, but Close/DrainToBackend
	// still run one on the way out. Ignored entirely when GCEnabled is false.
	GCInterval time.Duration

	gcTicker *time.Ticker

	// gcRefcount counts, per chunk ObjectID, how many entries in
	// BlocksToObject.BlockLookup currently point at it. Protected by
	// BlocksToObject.mu — every mutation site already holds that lock to
	// touch BlockLookup, so reusing it keeps refcount and map updates
	// atomic with each other instead of needing a second lock ordering to
	// reason about. A zero count marks the chunk a GC candidate; the entry
	// is left in the map (not deleted) at zero so sweepChunks can find it,
	// and is removed only once the chunk is actually deleted from the
	// backend. Only maintained when GCEnabled — see gcTrackBlock.
	//
	// gcRefcount alone cannot see a chunk that became unreferenced and was
	// never swept before this process last closed: parseBlockCheckpoint
	// rebuilds gcRefcount purely from the current live map, and a
	// zero-reference chunk is by definition absent from that map — it
	// looks identical to a chunk that was never minted. reconcileChunksOnce
	// (gcReconciled below) closes that gap once per VB lifetime.
	gcRefcount map[uint64]uint64

	// gcReconciled marks whether reconcileChunksOnce has already run for
	// this VB instance. Set once, lazily, on the first sweep attempt.
	gcReconciled atomic.Bool

	// gcFloor and gcFloorReady cache the lowest chunk ObjectID chunk GC may
	// ever consider (see ensureGCFloor): everything below it may still be
	// referenced by the current numbered checkpoint
	// (checkpoints/blocks.%08d.bin), which is read as a fallback if the
	// live checkpoint is unreadable. Computed lazily and cached for the
	// life of the process — numbered checkpoints are only rewritten at
	// Close/RecoverLocalWALs, at which point the process is tearing down or
	// re-deriving state from scratch anyway.
	gcFloor      atomic.Uint64
	gcFloorReady atomic.Bool

	// gcSnapshotSafe and gcSnapshotChecked cache the outcome of
	// ensureGCSnapshotSafe's bucket-wide ancestry scan: whether any
	// existing snapshot already references this volume. Once a scan
	// completes (safe or not), the result is cached for the process
	// lifetime — see ensureGCSnapshotSafe's doc comment for why a positive
	// result is not re-validated. A scan that errors leaves gcSnapshotChecked
	// false so the next sweep attempt retries rather than caching a
	// transient failure as "unsafe forever".
	gcSnapshotSafe    atomic.Bool
	gcSnapshotChecked atomic.Bool

	// gcLatchedOff is set permanently, never cleared, the moment this VB
	// instance does anything that invalidates chunk GC's invariants, and is
	// checked by ensureGCSnapshotSafe (and so, transitively, sweepChunks).
	// Two callers set it:
	//   - CreateSnapshot: closes the in-process half of the snapshot-
	//     ancestry hazard. A snapshot created after ensureGCSnapshotSafe
	//     last scanned would otherwise be invisible to a cached "safe"
	//     result. The cross-process half (another process snapshotting
	//     this volume while this instance keeps GC running) is NOT covered
	//     by this latch and has no fix in this phase — it is the reason
	//     GCEnabled defaults to false.
	//   - Reset: Reset re-issues ObjectID 0, violating the "chunk IDs are
	//     never reused" invariant GC's refcount/watermark/floor reasoning
	//     depends on entirely.
	gcLatchedOff atomic.Bool
}

// CacheConfig holds configuration for the LRU cache.
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
	Objects map[uint64]Block
}

type BlockCache struct {
	Data []byte
}

type Block struct {
	SeqNum uint64 `json:"SeqNum"`
	Block  uint64 `json:"Block"`
	Offset uint64 `json:"Offset"`
	Len    uint64 `json:"Len"`
	Data   []byte `json:"Data"`
}

type BlockOptimised struct {
	SeqNum uint64 `json:"SeqNum"`
	Index  int    `json:"Index"`
}

type Blocks struct {
	Blocks []Block `json:"Blocks"`
	mu     sync.RWMutex
}

type BlocksToObject struct {
	mu    sync.RWMutex
	dirty atomic.Bool

	BlockLookup map[uint64]BlockLookup
}

type BlockLookup struct {
	StartBlock   uint64
	NumBlocks    uint16
	ObjectID     uint64
	ObjectOffset uint32
	// SeqNum is the chunk-write generation that produced this block's
	// ciphertext on the backend. Drives nonce + AAD reconstruction on the
	// decrypt path: the on-disk chunk carries no nonce, so the per-block
	// SeqNum here is the only source. Always populated from BlockEntry.SeqNum
	// in createChunkFile (zero for blocks written by pre-encryption code paths
	// — those volumes are unreadable post-cutover by design).
	SeqNum uint64
}

type ConsecutiveBlock struct {
	BlockPosition uint64
	StartBlock    uint64
	NumBlocks     uint16
	OffsetStart   uint64
	OffsetEnd     uint64
	ObjectID      uint64
	ObjectOffset  uint32
	// SeqNum is the chunk-write generation that sealed this block's
	// ciphertext, used on pre-coalesce per-block entries (NumBlocks=1)
	// populated from BlockLookup.SeqNum or BlockEntry.SeqNum.
	// After coalescing, SeqNums holds the per-block SeqNums for the full
	// run and SeqNum is left zero — openChunkRun iterates SeqNums for nonce
	// + AAD reconstruction. Unused on unencrypted reads.
	SeqNum  uint64
	SeqNums []uint64
}

type ConsecutiveBlocks []ConsecutiveBlock

type BlocksMap map[uint64]Block

type BlocksMapOptimised map[uint64]BlockOptimised

type WAL struct {
	DB       []*os.File
	WallNum  atomic.Uint64
	BaseDir  string
	WALMagic [4]byte

	// dirty tracks whether there are unflushed writes since last sync
	// Uses atomic for lock-free access from write path and sync goroutine
	dirty atomic.Bool

	mu sync.RWMutex
}

// WALShard represents a single shard of a sharded WAL.
// Each shard has its own file and mutex, so writes to different shards
// have zero lock contention.
type WALShard struct {
	DB      *os.File
	dirty   atomic.Bool
	mu      sync.RWMutex
	shardID int
}

// ShardedWAL splits the WAL into NumShards parallel files.
// Blocks are routed to shards via blockNum & ShardMask.
// During consolidation, all shards are merged into unified 4MB chunks.
type ShardedWAL struct {
	Shards   [NumShards]*WALShard
	WallNum  atomic.Uint64
	BaseDir  string
	WALMagic [4]byte
}

type VolumeConfig struct {
	VolumeMetadata VolumeMetadata      `json:"VolumeMetadata"`
	AMIMetadata    AMIMetadata         `json:"AMIMetadata"`
	Modification   *VolumeModification `json:"Modification,omitempty"`
}

// VolumeModification records the most recent ModifyVolume request against a
// volume. It mirrors the AWS EC2 VolumeModification shape so the spinifex API
// edge can convert with no field gymnastics. A single record is kept per
// volume; a subsequent ModifyVolume overwrites it.
type VolumeModification struct {
	VolumeID           string    `json:"VolumeID"`
	ModificationState  string    `json:"ModificationState"` // "modifying"|"optimizing"|"completed"|"failed"
	Progress           int64     `json:"Progress"`
	StatusMessage      string    `json:"StatusMessage,omitempty"`
	OriginalSize       int64     `json:"OriginalSize"`
	OriginalIops       int64     `json:"OriginalIops"`
	OriginalVolumeType string    `json:"OriginalVolumeType"`
	TargetSize         int64     `json:"TargetSize"`
	TargetIops         int64     `json:"TargetIops"`
	TargetVolumeType   string    `json:"TargetVolumeType"`
	StartTime          time.Time `json:"StartTime"`
	EndTime            time.Time `json:"EndTime,omitzero"`
}

// Meta-data.
type VolumeMetadata struct {
	VolumeID            string            `json:"VolumeID"`   // e.g. "vol-0abcd1234ef567890"
	VolumeName          string            `json:"VolumeName"` // Optional name for UI or tagging
	TenantID            string            `json:"TenantID"`   // For multi-tenant support
	SizeGiB             uint64            `json:"SizeGiB"`    // Volume size in GiB
	State               string            `json:"State"`      // "creating", "available", "in-use", "deleted"
	CreatedAt           time.Time         `json:"CreatedAt"`
	AttachedAt          time.Time         `json:"AttachedAt"`          // When volume was attached to instance
	AvailabilityZone    string            `json:"AvailabilityZone"`    // Optional: "us-west-1a"
	AttachedInstance    string            `json:"AttachedInstance"`    // Instance ID (if any)
	DeviceName          string            `json:"DeviceName"`          // e.g. "/dev/nbd1"
	VolumeType          string            `json:"VolumeType"`          // e.g. "gp3", "io1"
	IOPS                int               `json:"IOPS"`                // For provisioned volumes
	Tags                map[string]string `json:"Tags"`                // User-defined metadata
	SnapshotID          string            `json:"SnapshotID"`          // If created from a snapshot
	DeleteOnTermination bool              `json:"DeleteOnTermination"` // Whether to delete volume when instance terminates
}

type AMIMetadata struct {
	ImageID         string            `json:"ImageID"` // e.g. "ami-0fbce8adcf7e5166f"
	Name            string            `json:"Name"`    // e.g. "debian-12-cloud"
	Description     string            `json:"Description"`
	Architecture    string            `json:"Architecture"`    // "x86_64", "arm64"
	PlatformDetails string            `json:"PlatformDetails"` // "Linux/UNIX"
	CreationDate    time.Time         `json:"CreationDate"`
	RootDeviceType  string            `json:"RootDeviceType"`         // "ebs"
	Virtualization  string            `json:"Virtualization"`         // "hvm"
	ImageOwnerAlias string            `json:"ImageOwnerAlias"`        // e.g. "spinifex"
	VolumeSizeGiB   uint64            `json:"VolumeSizeGiB"`          // Size of the root image
	SnapshotID      string            `json:"SnapshotID"`             // Snapshot ID for zero-copy cloning
	BootMode        string            `json:"BootMode,omitempty"`     // "bios" | "uefi" | "uefi-preferred"; empty for legacy AMIs registered before this field existed
	Distro          string            `json:"Distro,omitempty"`       // e.g. "debian", "ubuntu", "rocky", "alpine"; empty for AMIs registered before this field existed
	DistroFamily    string            `json:"DistroFamily,omitempty"` // "debian" | "rhel" | "alpine"; drives cloud-init template branching. Empty defaults to debian-family rendering at launch time.
	Tags            map[string]string `json:"Tags"`                   // Metadata tags
}

// Error messages

var ErrZeroBlock = errors.New("zero block")
var ErrRequestTooLarge = errors.New("request too large")
var ErrRequestOutOfRange = errors.New("request out of range")
var ErrRequestBlockSize = errors.New("request must be a multiple of block size")
var ErrRequestBufferEmpty = errors.New("request requires a buffer > 0")

// ErrStateNotFound is returned by LoadState when both the local file and the
// backend object are genuinely absent (NoSuchKey/os.ErrNotExist). The volume
// has no persisted state — caller decides whether that is expected (newly
// created volume pre-SaveState) or a hard error (recovery of an existing
// volume).
var ErrStateNotFound = errors.New("viperblock: state not found")

// ErrStateBackendUnavailable is returned by LoadState when the backend Read
// failed with a non-not-found error (timeout, network, 5xx). Callers should
// retry with backoff; the state may become available shortly.
var ErrStateBackendUnavailable = errors.New("viperblock: state backend unavailable")

// ErrEncryptionMismatch is returned when the runtime master key and the
// persisted VBState disagree on whether the volume is encrypted, when the
// KeyFingerprint on disk does not match the loaded key, or when an encrypted
// configuration is combined with UseShardedWAL (refused for the duration of
// the single-file-WAL-only encryption scope).
var ErrEncryptionMismatch = errors.New("viperblock: encryption configuration mismatch")

// ErrIntegrity wraps every AEAD-open failure on the read paths (WAL replay,
// chunk read, snapshot-clone base read). A non-nil unwrap of ErrIntegrity
// means an attacker either tampered with on-disk ciphertext / tag, swapped
// one volume's chunk into another's prefix, or replayed an older authentic
// ciphertext at the same offset. Callers must fail-closed on any wrap.
var ErrIntegrity = errors.New("viperblock: integrity check failed")

// ErrPreEncryptionFormat is returned when an encrypted runtime
// (EncryptionEnabled=true) encounters an on-disk artifact that still carries
// the pre-encryption magic — VBCH for chunks, VBWL for the single-file WAL.
// Without this dedicated signal the chunk read path would try to AEAD-open
// plaintext bytes and surface a generic ErrIntegrity, leaving operators with
// no actionable migration cue. The supported migration path is a hard cutover:
// create a new encrypted volume and copy data across via guest-side tooling
// (dd, filesystem-level copy). Callers must refuse to open the volume.
var ErrPreEncryptionFormat = errors.New("viperblock: chunk has pre-encryption format (VBCH) but volume opened with EncryptionEnabled=true; create a new encrypted volume and copy data across via guest-side tooling")

// classifyStateLoad maps the local and backend LoadStateRequest errors into a
// sentinel suitable for callers. The local read is informational — its
// absence is normal on multi-node deployments and post-restart recovery. The
// backend error drives the classification: a not-found there means the
// volume has no persisted state (genuine "new volume"), anything else means
// the backend is transiently unreachable.
//
// Backends signal "object missing" by wrapping os.ErrNotExist (file backend
// returns os.PathError directly; s3 backend's wrapNotFound translates
// NoSuchKey/NoSuchBucket/etc.). Any other error is treated as transient.
func classifyStateLoad(localErr, backendErr error) error {
	if backendErr != nil && !errors.Is(backendErr, os.ErrNotExist) {
		return fmt.Errorf("%w: %w", ErrStateBackendUnavailable, backendErr)
	}
	localMissing := localErr == nil || errors.Is(localErr, os.ErrNotExist)
	backendMissing := backendErr == nil || errors.Is(backendErr, os.ErrNotExist)
	if localMissing && backendMissing {
		return ErrStateNotFound
	}
	return fmt.Errorf("%w: state present but BlockSize=0", ErrStateNotFound)
}

// getSystemMemory returns the total system memory in bytes.
func getSystemMemory() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys
}

// calculateCacheSize calculates the number of blocks that can fit in the cache
// based on the system memory and block size.
func calculateCacheSize(blockSize uint32, percent int) int {
	if percent <= 0 || percent > 100 {
		percent = 30 // default to 30%
	}

	systemMemory := getSystemMemory()
	cacheMemory := (systemMemory * utils.SafeIntToUint64(percent)) / 100

	return utils.SafeUint64ToInt(cacheMemory / uint64(blockSize))
}

// SetCacheSize sets the size of the LRU cache in number of blocks.
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
		return fmt.Errorf("failed to create new LRU cache: %w", err)
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

// SetCacheSystemMemory sets the cache size based on a percentage of system memory.
func (vb *VB) SetCacheSystemMemory(percent int) error {
	if percent <= 0 || percent > 100 {
		return fmt.Errorf("system memory percentage must be between 1 and 100")
	}

	size := calculateCacheSize(vb.BlockSize, percent)

	return vb.SetCacheSize(size, percent)
}

func New(config *VB, btype string, backendConfig any) (vb *VB, err error) {
	var backend types.Backend

	if config == nil {
		return nil, fmt.Errorf("config must not be nil")
	}

	// Volume name and size are set by the backend
	if config.VolumeName == "" || config.VolumeSize == 0 {
		return nil, fmt.Errorf("volume name and size must be set")
	}

	// Encryption invariants: flag and key must agree, and sharded WAL is
	// refused for encrypted volumes (plan §Scope discipline — only the
	// single-file WAL is in scope; refusing here makes the unsupported combo
	// a startup-time error instead of a silent fall-through to unencrypted
	// writes via the sharded path).
	if config.EncryptionEnabled && config.MasterKey == nil {
		return nil, fmt.Errorf("%w: EncryptionEnabled=true requires MasterKey", ErrEncryptionMismatch)
	}
	if !config.EncryptionEnabled && config.MasterKey != nil {
		return nil, fmt.Errorf("%w: MasterKey provided but EncryptionEnabled=false", ErrEncryptionMismatch)
	}
	if config.EncryptionEnabled && config.UseShardedWAL {
		return nil, fmt.Errorf("%w: EncryptionEnabled is incompatible with UseShardedWAL", ErrEncryptionMismatch)
	}

	// log is this instance's logger: config.Logger if the caller supplied
	// one, otherwise slog.Default() so an embedder's own logger is honored.
	// Never slog.SetDefault — a library must not mutate its caller's global.
	log := config.Logger
	if log == nil {
		log = slog.Default()
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
	backend.SetLogger(log)

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

	if config.MaxPendingBytes == 0 {
		config.MaxPendingBytes = DefaultMaxPendingBytes
	}

	// UploadWorkers: 0 means use default, negative is clamped to 1 (serial).
	if config.UploadWorkers == 0 {
		config.UploadWorkers = DefaultUploadWorkers
	} else if config.UploadWorkers < 0 {
		config.UploadWorkers = 1
	}

	// WALSyncInterval: 0 means use default, negative means disabled
	if config.WALSyncInterval == 0 {
		config.WALSyncInterval = DefaultWALSyncInterval
	}
	// ChunkUploadInterval: 0 means use default, negative means disabled
	if config.ChunkUploadInterval == 0 {
		config.ChunkUploadInterval = DefaultChunkUploadInterval
	}
	// GCInterval: 0 means use default, negative means disabled (periodic
	// sweep only; Close/DrainToBackend still run one on the way out).
	// Applied regardless of GCEnabled so a later flip has a sane value, but
	// StartChunkUploader itself gates all GC behavior on GCEnabled.
	if config.GCInterval == 0 {
		config.GCInterval = DefaultGCInterval
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

	// Magic selection: encrypted volumes use VBCE chunks and VBWE single-file
	// WAL records (AEAD-sealed, no CRC). Unencrypted volumes keep the legacy
	// VBCH / VBWL formats unchanged. The sharded WAL keeps VBWL because the
	// sharded path is refused at startup under encryption (see New
	// validation); pre-existing unencrypted shards remain readable.
	chunkMagic := [4]byte{'V', 'B', 'C', 'H'}
	walMagic := [4]byte{'V', 'B', 'W', 'L'}
	if config.EncryptionEnabled {
		chunkMagic = [4]byte{'V', 'B', 'C', 'E'}
		walMagic = [4]byte{'V', 'B', 'W', 'E'}
	}

	vb = &VB{
		VolumeName:          config.VolumeName,
		VolumeSize:          config.VolumeSize,
		BlockSize:           config.BlockSize,
		ObjBlockSize:        config.ObjBlockSize,
		FlushInterval:       config.FlushInterval,
		FlushSize:           config.FlushSize,
		MaxPendingBytes:     config.MaxPendingBytes,
		UploadWorkers:       config.UploadWorkers,
		WALSyncInterval:     config.WALSyncInterval,
		ChunkUploadInterval: config.ChunkUploadInterval,
		GCEnabled:           config.GCEnabled,
		GCInterval:          config.GCInterval,
		Writes:              Blocks{},
		WAL:                 WAL{BaseDir: config.BaseDir, WALMagic: walMagic},
		BlockToObjectWAL:    WAL{BaseDir: config.BaseDir, WALMagic: blockToObjectWALMagic},
		Cache: Cache{
			lru: lruCache,
			Config: CacheConfig{
				Size:                config.Cache.Config.Size,
				UseSystemMemory:     config.Cache.Config.UseSystemMemory,
				SystemMemoryPercent: config.Cache.Config.SystemMemoryPercent,
			},
		},
		Version: 1,

		ChunkMagic:     chunkMagic,
		BlocksToObject: BlocksToObject{},
		Backend:        backend,
		BaseDir:        config.BaseDir,
		VolumeConfig:   config.VolumeConfig,

		// Initialize UnifiedBlockStore for O(1) lookups (enabled by default)
		BlockStore:    NewUnifiedBlockStore(config.BlockSize),
		UseBlockStore: true,

		UseShardedWAL: false,
		ShardedWAL:    NewShardedWAL(config.BaseDir, [4]byte{'V', 'B', 'W', 'L'}),

		chunkUploadTrigger: make(chan struct{}, 1),

		MasterKey:         config.MasterKey,
		EncryptionEnabled: config.EncryptionEnabled,

		log: log,
	}

	if config.EncryptionEnabled {
		vb.aead = config.MasterKey.AEAD
		vb.volumeNameHash = computeVolumeNameHash(config.VolumeName)
	}

	vb.BlocksToObject.BlockLookup = make(map[uint64]BlockLookup)
	if vb.GCEnabled {
		vb.gcRefcount = make(map[uint64]uint64)
	}

	// New intentionally does not touch the process-wide slog default: a
	// library must never mutate its caller's global logger state. All log
	// lines go through vb.log (config.Logger, or slog.Default() when unset),
	// so embedders keep whatever logger they already installed. Standalone
	// entrypoints (nbdkit plugin bootstrap, viperblockd) opt into their own
	// process-wide default explicitly via telemetry.SetDefaultJSONLogger.

	// Create the base directory if it doesn't exist
	if err := os.MkdirAll(filepath.Join(vb.BaseDir, vb.GetVolume()), 0750); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	// Create the checkpoint directory if it doesn't exist
	if err := os.MkdirAll(filepath.Join(vb.BaseDir, vb.GetVolume(), "checkpoints"), 0750); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	// Start background WAL syncer for periodic fsync (if interval > 0)
	vb.StartWALSyncer()

	// Start background chunk uploader (if interval > 0)
	vb.StartChunkUploader()

	return vb, nil
}

// logger returns vb.log, falling back to slog.Default() for VB values
// assembled without going through New() (e.g. spinifex's snapshotRunningVolume
// path, which hand-builds a read-only VB sharing another instance's backend).
func (vb *VB) logger() *slog.Logger {
	if vb.log == nil {
		return slog.Default()
	}
	return vb.log
}

// SetDebug rebuilds this instance's logger (JSON to stdout, fanning out to
// an OTLP bridge if telemetry.Init already configured one) at Debug or Error
// level. It only ever touches vb.log and the backend's logger — never the
// process-wide slog default — so it is safe for an embedded caller to have
// this called on its behalf.
func (vb *VB) SetDebug(debug bool) {
	level := slog.LevelError
	if debug {
		level = slog.LevelDebug
	}
	vb.log = telemetry.NewJSONLogger("viperblock", level)
	if vb.Backend != nil {
		vb.Backend.SetLogger(vb.log)
	}
}

func (vb *VB) SetWALBaseDir(baseDir string) {
	vb.WAL.BaseDir = baseDir
}

func (vb *VB) SetBlockWALBaseDir(baseDir string) {
	vb.BlockToObjectWAL.BaseDir = baseDir
}

// StartWALSyncer starts a background goroutine that periodically fsyncs the WAL to disk.
// This implements the "group commit" pattern used by PostgreSQL (wal_writer_delay),
// BadgerDB (SyncWrites with ticker), and MongoDB (journalCommitInterval).
//
// The syncer only performs fsync when there are dirty (unflushed) writes,
// avoiding unnecessary disk I/O when the system is idle.
func (vb *VB) StartWALSyncer() {
	if vb.WALSyncInterval <= 0 {
		vb.logger().Debug("WAL syncer disabled (interval <= 0)")
		return
	}

	vb.walSyncStop = make(chan struct{})
	vb.walSyncDone = make(chan struct{})
	vb.walSyncTicker = time.NewTicker(vb.WALSyncInterval)

	go func() {
		defer close(vb.walSyncDone)
		defer vb.walSyncTicker.Stop()

		for {
			select {
			case <-vb.walSyncTicker.C:
				if vb.UseShardedWAL {
					vb.syncShardedWALIfDirty()
				} else {
					vb.syncWALIfDirty()
				}
			case <-vb.walSyncStop:
				// Final sync before shutdown
				if vb.UseShardedWAL {
					vb.syncShardedWALIfDirty()
				} else {
					vb.syncWALIfDirty()
				}
				return
			}
		}
	}()

	vb.logger().Debug("WAL syncer started", "interval", vb.WALSyncInterval)
}

// StopWALSyncer gracefully stops the background WAL sync goroutine.
// It signals the goroutine to stop and waits for it to complete its final sync.
func (vb *VB) StopWALSyncer() {
	if vb.walSyncStop == nil {
		return
	}

	close(vb.walSyncStop)
	<-vb.walSyncDone

	vb.walSyncStop = nil
	vb.walSyncDone = nil
	vb.walSyncTicker = nil

	vb.logger().Debug("WAL syncer stopped")
}

// StartChunkUploader starts a background goroutine that periodically calls
// DrainToBackend so snapshots have a reasonably current S3 view without
// blocking the guest fsync path.
func (vb *VB) StartChunkUploader() {
	if vb.ChunkUploadInterval <= 0 {
		vb.logger().Debug("chunk uploader disabled (interval <= 0)")
		return
	}

	vb.chunkUploadStop = make(chan struct{})
	vb.chunkUploadDone = make(chan struct{})
	vb.chunkUploadTicker = time.NewTicker(vb.ChunkUploadInterval)

	// Chunk GC runs on its own cadence, decoupled from ChunkUploadInterval:
	// a short upload interval (tuned for snapshot freshness) should not
	// force GC's one-time-per-lifetime bucket-wide ancestry scan (see
	// ensureGCSnapshotSafe) to be considered far more often than necessary.
	// gcTickerC stays nil (permanently non-firing in the select below) when
	// GC is off or GCInterval <= 0, so this goroutine's only extra cost in
	// the common, GC-disabled case is one always-false receive case.
	var gcTickerC <-chan time.Time
	if vb.GCEnabled && vb.GCInterval > 0 {
		vb.gcTicker = time.NewTicker(vb.GCInterval)
		gcTickerC = vb.gcTicker.C
	}

	go func() {
		defer close(vb.chunkUploadDone)
		defer vb.chunkUploadTicker.Stop()
		defer func() {
			if vb.gcTicker != nil {
				vb.gcTicker.Stop()
			}
		}()

		for {
			select {
			case <-vb.chunkUploadTicker.C:
				if err := vb.DrainToBackendCtx(context.Background()); err != nil {
					vb.logger().Warn("chunk uploader: DrainToBackend failed", "err", err)
				}
			case <-vb.chunkUploadTrigger:
				// Size-triggered drain: pendingBytes crossed FlushSize in
				// WriteAtCtx. Bounds steady-state memory to ~FlushSize
				// without waiting on the ChunkUploadInterval ticker.
				if err := vb.DrainToBackendCtx(context.Background()); err != nil {
					vb.logger().Warn("chunk uploader: size-triggered DrainToBackend failed", "err", err)
				}
			case <-gcTickerC:
				vb.runGCSweep(context.Background())
			case <-vb.chunkUploadStop:
				return
			}
		}
	}()

	vb.logger().Debug("chunk uploader started", "interval", vb.ChunkUploadInterval, "gcEnabled", vb.GCEnabled, "gcInterval", vb.GCInterval)
}

// StopChunkUploader stops the background chunk upload goroutine.
func (vb *VB) StopChunkUploader() {
	if vb.chunkUploadStop == nil {
		return
	}

	close(vb.chunkUploadStop)
	<-vb.chunkUploadDone

	vb.chunkUploadStop = nil
	vb.chunkUploadDone = nil
	vb.chunkUploadTicker = nil
	vb.gcTicker = nil

	vb.logger().Debug("chunk uploader stopped")
}

// syncWALIfDirty performs fsync on the active WAL file if there are pending writes.
// This is the core of the periodic sync mechanism - it checks the dirty flag
// and only syncs when necessary to avoid unnecessary I/O.
//
// Note: Only the last file in vb.WAL.DB is the active WAL being written to.
// Previous files are closed after WriteWALToChunk processes them.
func (vb *VB) syncWALIfDirty() {
	// Fast path: check dirty flag without lock
	if !vb.WAL.dirty.Load() {
		return
	}

	// Clear dirty flag before sync (writes during sync will re-set it)
	vb.WAL.dirty.Store(false)

	vb.WAL.mu.RLock()
	defer vb.WAL.mu.RUnlock()

	// Only sync the current active WAL (last in slice)
	// Previous WAL files are already closed after chunking
	if len(vb.WAL.DB) > 0 {
		activeWAL := vb.WAL.DB[len(vb.WAL.DB)-1]
		if activeWAL != nil {
			if err := activeWAL.Sync(); err != nil {
				vb.logger().Error("WAL sync failed", "error", err)
				// Re-mark as dirty so next tick retries
				vb.WAL.dirty.Store(true)
			}
		}
	}
}

// syncShardedWALIfDirty fsyncs only the shards that have been written to since the last sync.
func (vb *VB) syncShardedWALIfDirty() {
	sw := vb.ShardedWAL
	if sw == nil {
		return
	}

	for i := range NumShards {
		shard := sw.Shards[i]

		// Fast path: skip clean shards
		if !shard.dirty.Load() {
			continue
		}

		shard.dirty.Store(false)

		shard.mu.RLock()
		if shard.DB != nil {
			if err := shard.DB.Sync(); err != nil {
				vb.logger().Error("Sharded WAL sync failed", "shard", i, "error", err)
				shard.dirty.Store(true)
			}
		}
		shard.mu.RUnlock()
	}
}

// WAL functions.
func (vb *VB) OpenWAL(wal *WAL, filename string) (err error) {
	// Lock operations on the WAL
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return vb.openWALLocked(wal, filename)
}

// openWALLocked creates and opens a new WAL file. Caller must hold wal.mu.
func (vb *VB) openWALLocked(wal *WAL, filename string) (err error) {
	// Create the directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(filename), 0750); err != nil {
		return fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Create the file if it doesn't exist, make sure writes and committed immediately
	// Removed syscall.O_SYNC, TODO implement buffer, sync every 250ms / 1MB data
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}

	// Append the WAL header, format
	// Check our type
	var headers []byte

	switch wal.WALMagic {
	case vb.WAL.WALMagic:
		// Write the WAL header
		headers = vb.WALHeader()
	case vb.BlockToObjectWAL.WALMagic:
		// Write the BlockToObjectWAL header
		headers = vb.BlockToObjectWALHeader()
	default:
		return fmt.Errorf("invalid WAL magic")
	}

	_, err = file.Write(headers)

	if err != nil {
		return err
	}

	// Append the latest "hot" WAL file to the DB
	wal.DB = append(wal.DB, file)

	vb.logger().Debug("OpenWAL complete, new WAL", "file", *file)

	return err
}

// NewShardedWAL creates a ShardedWAL with initialized (but unopened) shards.
func NewShardedWAL(baseDir string, magic [4]byte) *ShardedWAL {
	sw := &ShardedWAL{
		BaseDir:  baseDir,
		WALMagic: magic,
	}
	for i := range NumShards {
		sw.Shards[i] = &WALShard{shardID: i}
	}
	return sw
}

// OpenShardedWAL opens all shard files for the current WAL generation.
// Each shard gets its own file with a standard WAL header.
func (vb *VB) OpenShardedWAL() error {
	sw := vb.ShardedWAL
	if sw == nil {
		return fmt.Errorf("ShardedWAL not initialized")
	}

	walNum := sw.WallNum.Load()
	header := vb.WALHeader()

	for i := range NumShards {
		shard := sw.Shards[i]
		shard.mu.Lock()

		filename := filepath.Join(sw.BaseDir,
			types.GetShardedWALPath(vb.GetVolume(), walNum, i))

		if err := os.MkdirAll(filepath.Dir(filename), 0750); err != nil {
			shard.mu.Unlock()
			return fmt.Errorf("failed to create shard directory %d: %w", i, err)
		}

		file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
		if err != nil {
			shard.mu.Unlock()
			// Close any shards we already opened
			for j := range i {
				sw.Shards[j].mu.Lock()
				if sw.Shards[j].DB != nil {
					if cerr := sw.Shards[j].DB.Close(); cerr != nil {
						vb.logger().Warn("failed to close shard during cleanup", "shard", j, "error", cerr)
					}
					sw.Shards[j].DB = nil
				}
				sw.Shards[j].mu.Unlock()
			}
			return fmt.Errorf("failed to open shard %d: %w", i, err)
		}

		if _, err := file.Write(header); err != nil {
			if cerr := file.Close(); cerr != nil {
				vb.logger().Warn("failed to close shard file during cleanup", "shard", i, "error", cerr)
			}
			shard.mu.Unlock()
			return fmt.Errorf("failed to write header for shard %d: %w", i, err)
		}

		shard.DB = file
		shard.mu.Unlock()
	}

	vb.logger().Debug("OpenShardedWAL complete", "walNum", walNum, "shards", NumShards)
	return nil
}

func (vb *VB) WriteAt(offset uint64, data []byte) error {
	return vb.WriteAtCtx(context.Background(), offset, data)
}

// PendingBytes returns the current outstanding buffered write bytes across
// Writes.Blocks and PendingBackendWrites.Blocks — the counter the WriteAtCtx
// backpressure gate watches.
func (vb *VB) PendingBytes() uint64 {
	return uint64(vb.pendingBytes.Load()) //nolint:gosec // G115: increments/decrements are matched byte counts, never negative
}

// maxPendingBytes returns the configured high-watermark, or the default if
// unset (covers VB values assembled without going through New).
func (vb *VB) maxPendingBytes() uint64 {
	if vb.MaxPendingBytes == 0 {
		return DefaultMaxPendingBytes
	}
	return vb.MaxPendingBytes
}

// signalSizeTrigger asks the background chunk uploader to drain now, once
// pendingBytes crosses FlushSize, instead of waiting for the next
// ChunkUploadInterval tick. Non-blocking: a trigger already pending
// coalesces, and the synchronous gate in awaitBackpressure is the hard bound
// if the uploader can't keep up.
func (vb *VB) signalSizeTrigger() {
	flushSize := vb.FlushSize
	if flushSize == 0 {
		flushSize = DefaultFlushSize
	}
	if vb.PendingBytes() < uint64(flushSize) {
		return
	}
	select {
	case vb.chunkUploadTrigger <- struct{}{}:
	default:
	}
}

// awaitBackpressure blocks the caller once pendingBytes has crossed
// MaxPendingBytes, driving DrainToBackendCtx synchronously until pendingBytes
// falls back under the low-watermark (MaxPendingBytes/2). This is the core
// backpressure mechanism: a guest write that outruns the backend's ingest
// rate self-throttles here instead of growing Writes.Blocks /
// PendingBackendWrites.Blocks without bound.
//
// Called after WriteAtCtx has already released vb.Writes.mu, so the Flush()
// invoked by DrainToBackendCtx (which takes that same lock) cannot deadlock
// against us. drainInFlight ensures only one blocked writer actually drives
// the drain at a time; the rest poll pendingBytes with a bounded backoff.
func (vb *VB) awaitBackpressure(ctx context.Context) error {
	high := vb.maxPendingBytes()
	if vb.PendingBytes() <= high {
		return nil
	}

	low := high / 2
	backoff := 10 * time.Millisecond
	const maxBackoff = 500 * time.Millisecond

	for vb.PendingBytes() > low {
		if err := ctx.Err(); err != nil {
			return err
		}

		if vb.drainInFlight.CompareAndSwap(false, true) {
			err := vb.DrainToBackendCtx(ctx)
			vb.drainInFlight.Store(false)
			if err != nil {
				vb.logger().Warn("write backpressure: drain failed, retrying", "err", err)
			}
			if vb.PendingBytes() <= low {
				break
			}
		}

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}

	return nil
}

// WriteAtCtx is WriteAt with a caller-supplied context that flows through any
// read-modify-write backend fetches for trace propagation.
func (vb *VB) WriteAtCtx(ctx context.Context, offset uint64, data []byte) error {
	// First check the block exists in our volume size
	if offset > vb.GetVolumeSize() {
		return ErrRequestTooLarge
	}

	// Check if the request is within range
	if offset+uint64(len(data)) > vb.GetVolumeSize() {
		return ErrRequestOutOfRange
	}

	blockSize := uint64(vb.BlockSize)
	dataLen := uint64(len(data))

	// Request buffer must be > 0
	if dataLen == 0 {
		return ErrRequestBufferEmpty
	}

	// Check blockLen a multiple of a blocksize
	// No longer required, WriteAt can handle different block sizes (default 4096)
	// Issue was with GRUB which requires 512 blocksize to write bootloader, ignorning the block size specified for the volume.
	//if blockLen%uint64(vb.BlockSize) != 0 {
	//	return ErrRequestBlockSize
	//}

	startBlock := offset / blockSize
	endOffset := offset + dataLen
	endBlock := (endOffset - 1) / blockSize

	// Reserve a contiguous SeqNum batch up-front. reserveSeqNum may call
	// SaveState (which takes BlocksToObject.mu), so it must run before we
	// acquire vb.Writes.mu below to keep the lock order consistent. We issue
	// start+1..start+n to preserve the legacy "atomic.Add(1) post-increment"
	// semantics (issued SeqNums are >= 1; SeqNum == 0 reads as uninitialised
	// in BlockStore).
	start, err := vb.reserveSeqNum(ctx, endBlock-startBlock+1)
	if err != nil {
		return err
	}
	nextSeqNum := start + 1

	var writes []Block

	for b := startBlock; b <= endBlock; b++ {
		blockStart := b * blockSize
		blockEnd := blockStart + blockSize

		// Slice the range of data to write into this block
		var writeStart uint64
		var writeEnd uint64

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
			existing, err := vb.ReadAtCtx(ctx, b*blockSize, blockSize)

			if err != nil && !errors.Is(err, ErrZeroBlock) {
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
			SeqNum: nextSeqNum,
			Block:  b,
			Len:    blockSize,
			Data:   blockData,
		})
		nextSeqNum++
	}

	// Thread-safe write into memory buffer
	vb.Writes.mu.Lock()
	vb.Writes.Blocks = append(vb.Writes.Blocks, writes...)
	vb.Writes.mu.Unlock()

	// Also update BlockStore if enabled (for O(1) read lookups)
	if vb.UseBlockStore && vb.BlockStore != nil {
		for _, block := range writes {
			vb.BlockStore.WriteWithSeqNum(block.Block, block.Data, block.SeqNum)
		}
	}

	vb.pendingBytes.Add(int64(len(writes)) * int64(blockSize)) //nolint:gosec // G115: blockSize is 4KB-class, no overflow risk
	vb.signalSizeTrigger()

	// Backpressure gate: block this guest write until buffered bytes drop
	// back under the low-watermark if MaxPendingBytes has been crossed. Runs
	// after releasing vb.Writes.mu above so the drain this drives (which
	// re-acquires that lock) cannot deadlock against us.
	if err := vb.awaitBackpressure(ctx); err != nil {
		return err
	}

	return nil
}

func (vb *VB) Write(block uint64, data []byte) (err error) {
	blockLen := uint64(len(data))

	// First check the block exists in our volume size
	if block*uint64(vb.BlockSize) > vb.GetVolumeSize() {
		return ErrRequestTooLarge
	}

	// Check if the request is within range
	if block*uint64(vb.BlockSize)+blockLen > vb.GetVolumeSize() {
		return ErrRequestOutOfRange
	}

	// Check blockLen a multiple of a blocksize
	if blockLen%uint64(vb.BlockSize) != 0 {
		return ErrRequestBlockSize
	}

	blockRequests := blockLen / uint64(vb.BlockSize)

	//vb.logger().Info("\tVBWRITE:", "blockRequests", blockRequests, "block", block, "blockLen", blockLen)

	// Reserve a contiguous SeqNum batch up-front. reserveSeqNum may call
	// SaveState (which takes BlocksToObject.mu), so it must run before we
	// acquire vb.Writes.mu below to keep the lock order consistent. We issue
	// start+1..start+n to preserve the legacy "atomic.Add(1) post-increment"
	// semantics (issued SeqNums are >= 1; SeqNum == 0 reads as uninitialised
	// in BlockStore).
	start, err := vb.reserveSeqNum(context.Background(), blockRequests)
	if err != nil {
		return err
	}
	seqNum := start + 1

	vb.Writes.mu.Lock()

	// Loop through each block request
	for i := range blockRequests {
		currentBlock := block + i

		start := i * uint64(vb.BlockSize)
		end := start + uint64(vb.BlockSize)

		blockCopy := make([]byte, vb.BlockSize)
		copy(blockCopy, data[start:end])

		//vb.logger().Info("\t\tBLOCKWRITE:", "currentBlock", currentBlock, "start", start, "end", end, "i", i)

		vb.Writes.Blocks = append(vb.Writes.Blocks, Block{
			SeqNum: seqNum,
			Block:  currentBlock,
			Data:   blockCopy,
		})

		// Also update BlockStore if enabled (for O(1) read lookups)
		if vb.UseBlockStore && vb.BlockStore != nil {
			vb.BlockStore.WriteWithSeqNum(currentBlock, blockCopy, seqNum)
		}

		//vb.logger().Info("WRITE:", "seqNum", seqNum, "BLOCK:", currentBlock, "start", start, "end", end)

		seqNum++
	}

	vb.Writes.mu.Unlock()

	return nil
}

// Flush the main memory (writes) to the WAL.
func (vb *VB) Flush() (err error) {
	start := time.Now()
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		telemetry.RecordWALOp(context.Background(), "flush", vb.VolumeName, outcome, time.Since(start))
	}()

	vb.Writes.mu.Lock()
	defer vb.Writes.mu.Unlock()
	if vb.UseShardedWAL {
		return vb.flushLockedSharded()
	}
	return vb.flushLocked()
}

// DrainToBackend flushes all in-memory writes to the WAL, uploads accumulated
// WAL chunks to S3, and saves the live checkpoint. Call this at snapshot-prepare
// time or on clean shutdown to ensure S3 state is fully current.
func (vb *VB) DrainToBackend() error {
	return vb.DrainToBackendCtx(context.Background())
}

// DrainToBackendCtx is DrainToBackend with a caller-supplied context threaded
// through the chunk-upload and checkpoint S3 writes.
func (vb *VB) DrainToBackendCtx(ctx context.Context) error {
	// Serialize every drain trigger. See drainMu's doc comment: overlapping
	// drains race in createChunkFile and can strand a live chunk at refcount
	// zero, which GC would then physically delete.
	vb.drainMu.Lock()
	defer vb.drainMu.Unlock()

	if err := vb.Flush(); err != nil {
		return fmt.Errorf("drain flush: %w", err)
	}
	var err error
	if vb.UseShardedWAL {
		err = vb.WriteShardedWALToChunkCtx(ctx, true)
	} else {
		err = vb.WriteWALToChunkCtx(ctx, true)
	}
	if err != nil {
		return fmt.Errorf("drain chunk upload: %w", err)
	}
	if err := vb.SaveLiveCheckpointCtx(ctx); err != nil {
		return fmt.Errorf("drain live checkpoint: %w", err)
	}
	return nil
}

// flushLocked flushes hot writes to WAL. Caller must hold vb.Writes.mu.Lock().
func (vb *VB) flushLocked() error {
	flushBlocks := make([]Block, len(vb.Writes.Blocks))
	copy(flushBlocks, vb.Writes.Blocks)

	// flushed maps block number -> latest SeqNum that landed in the WAL,
	// used to filter vb.Writes.Blocks and feed PendingBackendWrites. It
	// dedupes by block number, so its cardinality CANNOT be used to detect
	// partial flushes: when a hot block is rewritten N times in a window,
	// N successful WriteWAL calls collapse into one map entry. successCount
	// tracks records persisted, which is what "partial flush" actually means.
	flushed := make(map[uint64]uint64)
	successCount := 0

	for _, block := range flushBlocks {
		if err := vb.WriteWAL(block); err != nil {
			vb.logger().Error("ERROR FLUSHING:", "block", block.Block, "error", err)
			break
		}

		successCount++
		flushed[block.Block] = block.SeqNum

		// Mark block as Pending in BlockStore (Hot -> Pending transition)
		if vb.UseBlockStore && vb.BlockStore != nil {
			vb.BlockStore.MarkPending(block.Block)
		}
	}

	// Filter vb.Writes.Blocks to keep only blocks NOT successfully flushed
	if len(flushed) > 0 {
		remaining := make([]Block, 0)
		for _, b := range vb.Writes.Blocks {
			if _, ok := flushed[b.Block]; !ok {
				remaining = append(remaining, b)
			}
		}

		vb.Writes.Blocks = remaining
	}

	// Append only successfully flushed blocks to PendingBackendWrites
	vb.PendingBackendWrites.mu.Lock()
	for _, b := range flushBlocks {
		if _, ok := flushed[b.Block]; ok {
			vb.PendingBackendWrites.Blocks = append(vb.PendingBackendWrites.Blocks, b)
		}
	}
	vb.PendingBackendWrites.mu.Unlock()

	if successCount < len(flushBlocks) {
		return fmt.Errorf("partial flush: %d of %d records flushed", successCount, len(flushBlocks))
	}

	return nil
}

// flushLockedSharded flushes hot writes to the sharded WAL in parallel.
// Blocks are grouped by shard and written concurrently — one goroutine per shard.
// Caller must hold vb.Writes.mu.Lock().
func (vb *VB) flushLockedSharded() error {
	flushBlocks := make([]Block, len(vb.Writes.Blocks))
	copy(flushBlocks, vb.Writes.Blocks)

	if len(flushBlocks) == 0 {
		return nil
	}

	// Group blocks by shard
	var shardGroups [NumShards][]Block
	for _, block := range flushBlocks {
		idx := block.Block & ShardMask
		shardGroups[idx] = append(shardGroups[idx], block)
	}

	// Write to each shard in parallel
	type shardError struct {
		err          error
		flushed      map[uint64]uint64
		successCount int
	}
	results := make([]shardError, NumShards)
	var wg sync.WaitGroup

	for i := range NumShards {
		if len(shardGroups[i]) == 0 {
			results[i].flushed = make(map[uint64]uint64)
			continue
		}
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			flushed := make(map[uint64]uint64)
			successCount := 0
			for _, block := range shardGroups[shardID] {
				if err := vb.WriteShardedWAL(block); err != nil {
					vb.logger().Error("ERROR FLUSHING SHARD:", "shard", shardID, "block", block.Block, "error", err)
					results[shardID].err = err
					results[shardID].flushed = flushed
					results[shardID].successCount = successCount
					return
				}
				successCount++
				flushed[block.Block] = block.SeqNum

				if vb.UseBlockStore && vb.BlockStore != nil {
					vb.BlockStore.MarkPending(block.Block)
				}
			}
			results[shardID].flushed = flushed
			results[shardID].successCount = successCount
		}(i)
	}
	wg.Wait()

	// Merge flushed maps from all shards. allFlushed dedupes by block number
	// so its cardinality CANNOT be used to detect partial flushes — see
	// flushLocked. totalSuccess sums records persisted per shard.
	allFlushed := make(map[uint64]uint64)
	var firstErr error
	totalSuccess := 0
	for i := range NumShards {
		maps.Copy(allFlushed, results[i].flushed)
		totalSuccess += results[i].successCount
		if results[i].err != nil && firstErr == nil {
			firstErr = results[i].err
		}
	}

	// Filter vb.Writes.Blocks to keep only blocks NOT successfully flushed
	if len(allFlushed) > 0 {
		remaining := make([]Block, 0)
		for _, b := range vb.Writes.Blocks {
			if _, ok := allFlushed[b.Block]; !ok {
				remaining = append(remaining, b)
			}
		}
		vb.Writes.Blocks = remaining
	}

	// Append successfully flushed blocks to PendingBackendWrites
	vb.PendingBackendWrites.mu.Lock()
	for _, b := range flushBlocks {
		if _, ok := allFlushed[b.Block]; ok {
			vb.PendingBackendWrites.Blocks = append(vb.PendingBackendWrites.Blocks, b)
		}
	}
	vb.PendingBackendWrites.mu.Unlock()

	if firstErr != nil {
		return fmt.Errorf("partial sharded flush: %d of %d records flushed: %w", totalSuccess, len(flushBlocks), firstErr)
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
		//vb.logger().Info("FLUSH:", "block", block.Block, "seqnum", block.SeqNum)

		// Write the block to the WAL
		err = vb.WriteWAL(block)
		if err != nil {
			vb.logger().Error("ERROR FLUSHING:", "error", err)
			return err
		}
	}

	return nil
}

func (vb *VB) WriteWAL(block Block) (err error) {
	var record []byte

	if vb.EncryptionEnabled {
		// Encrypted layout (magic VBWE), per-record:
		//   [SeqNum(8) | BlockNum(8) | BlockLen(8) | ciphertext(BlockLen) | tag(16)]
		// CRC32 is dropped — the 16-byte GCM tag subsumes it (NIST SP 800-38D
		// §5: AEAD provides confidentiality and integrity in one primitive).
		// Nonce: (SeqNum, VolumeUUID, DomainWAL). AAD: (volumeNameHash,
		// BlockNum, SeqNum). Bound together they defeat cross-volume swap,
		// in-place rollback, and positional shuffle on WAL replay.
		const headerLen = 24
		record = make([]byte, headerLen, headerLen+len(block.Data)+16)
		binary.BigEndian.PutUint64(record[0:8], block.SeqNum)
		binary.BigEndian.PutUint64(record[8:16], block.Block)
		binary.BigEndian.PutUint64(record[16:24], block.Len)
		nonce := makeNonce(block.SeqNum, vb.VolumeUUID, DomainWAL)
		var aad [AADLen]byte
		initAAD(&aad, vb.volumeNameHash)
		updateAAD(&aad, block.Block, block.SeqNum)
		record = vb.aead.Seal(record, nonce[:], block.Data, aad[:])
	} else {
		// Legacy layout (magic VBWL), per-record:
		//   [SeqNum(8) | BlockNum(8) | BlockLen(8) | CRC32(4) | data(BlockLen)]
		recordSize := 28 + len(block.Data)
		record = make([]byte, recordSize)
		binary.BigEndian.PutUint64(record[0:8], block.SeqNum)
		binary.BigEndian.PutUint64(record[8:16], block.Block)
		binary.BigEndian.PutUint64(record[16:24], block.Len)
		checksum := crc32.ChecksumIEEE(record[0:24])
		checksum = crc32.Update(checksum, crc32.IEEETable, block.Data)
		binary.BigEndian.PutUint32(record[24:28], checksum)
		copy(record[28:], block.Data)
	}

	vb.WAL.mu.Lock()
	currentWAL := vb.WAL.DB[len(vb.WAL.DB)-1]
	// O_APPEND makes the file offset unreliable; Stat is the source of truth
	// for the on-disk boundary we may need to roll back to.
	preStat, statErr := currentWAL.Stat()
	if statErr != nil {
		vb.WAL.mu.Unlock()
		return fmt.Errorf("error obtaining WAL size before write: %w", statErr)
	}
	preSize := preStat.Size()
	n, err := currentWAL.Write(record)
	vb.WAL.dirty.Store(true)

	if err != nil || n != len(record) {
		// Torn write: roll back to the last record boundary so future appends
		// and replay stay aligned with the record framing.
		truncErr := currentWAL.Truncate(preSize)
		vb.WAL.mu.Unlock()

		if truncErr != nil {
			return fmt.Errorf("incomplete WAL write (wrote %d of %d bytes) and truncate to %d failed: %w", n, len(record), preSize, truncErr)
		}
		if err != nil {
			vb.logger().Error("WAL write failed, truncated to last boundary", "n", n, "expected", len(record), "preSize", preSize, "error", err)
			return fmt.Errorf("WAL write failed (truncated to %d): %w", preSize, err)
		}
		vb.logger().Error("WAL incomplete write, truncated to last boundary", "n", n, "expected", len(record), "preSize", preSize)
		return fmt.Errorf("incomplete write to WAL: wrote %d of %d bytes (truncated to %d)", n, len(record), preSize)
	}

	vb.WAL.mu.Unlock()
	return nil
}

// WriteShardedWAL writes a block to the appropriate shard based on block number.
// Only the target shard's mutex is acquired, so writes to different shards
// have zero lock contention.
func (vb *VB) WriteShardedWAL(block Block) error {
	sw := vb.ShardedWAL
	shardIdx := block.Block & ShardMask
	shard := sw.Shards[shardIdx]

	// Pre-allocate record buffer (28 byte header + data)
	recordSize := 28 + len(block.Data)
	record := make([]byte, recordSize)

	// Format: [seq_number, uint64][block_number, uint64][block_length, uint64][checksum, uint32][block_data, []byte]
	binary.BigEndian.PutUint64(record[0:8], block.SeqNum)
	binary.BigEndian.PutUint64(record[8:16], block.Block)
	binary.BigEndian.PutUint64(record[16:24], block.Len)

	checksum := crc32.ChecksumIEEE(record[0:24])
	checksum = crc32.Update(checksum, crc32.IEEETable, block.Data)
	binary.BigEndian.PutUint32(record[24:28], checksum)

	copy(record[28:], block.Data)

	shard.mu.Lock()
	n, err := shard.DB.Write(record)
	shard.dirty.Store(true)
	shard.mu.Unlock()

	if n != recordSize {
		vb.logger().Error("ERROR WRITING BLOCK TO SHARDED WAL: incomplete write", "shard", shardIdx, "n", n, "expected", recordSize)
		return fmt.Errorf("incomplete write to sharded WAL shard %d: wrote %d of %d bytes", shardIdx, n, recordSize)
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
		_, err := currentWAL.Read(headers)

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
		var n int
		n, err = currentWAL.Read(block.Data)

		if n != utils.SafeUint64ToInt(block.Len) {
			return fmt.Errorf("incomplete read: got %d bytes, expected %d", n, block.Len)
		}

		// Calculate a CRC32 checksum of the block data and headers
		checksum_validated := crc32.ChecksumIEEE(headers[:8])
		checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, headers[8:16])
		checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, headers[16:24])
		checksum_validated = crc32.Update(checksum_validated, crc32.IEEETable, block.Data[:n])

		if checksum_validated != checksum {
			err2 := errors.New("checksum mismatch for block " + strconv.FormatUint(block.Block, 10) + " offset: " + strconv.FormatUint(block.Offset, 10))
			vb.logger().Error("checksum mismatch", "error", err2)
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
	return err

	// vb.BlockToObjectWAL.mu.Lock()

	// // Get the current WAL file
	// currentWAL := vb.BlockToObjectWAL.DB[len(vb.BlockToObjectWAL.DB)-1]

	// //vb.logger().Info("Writing to Block WAL file", "filename", currentWAL.Name())

	// // Format for each block in the BlockWAL
	// // [start_block, uint64][num_blocks, uint16][object_id, uint64][object_offset, uint32][checksum, uint32]
	// // big endian

	// for _, block := range *blocks {
	// 	//vb.logger().Info("Writing block to BlockWAL", "block", block)

	// 	data := vb.writeBlockWalChunk(&block)

	// 	_, err := currentWAL.Write(data)

	// 	if err != nil {
	// 		vb.logger().Error("ERROR WRITING BLOCK TO BLOCK WAL:", "error", err)
	// 		vb.BlockToObjectWAL.mu.Unlock()
	// 		return err
	// 	}

	// }

	// vb.BlockToObjectWAL.mu.Unlock()

	// // Cycle to the next Block WAL file
	// // Create the Block WAL
	// nextBlockWalNum := vb.BlockToObjectWAL.WallNum.Add(1)
	// err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, nextBlockWalNum, vb.GetVolume())))
	// //	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s/wal/blocks/blocks.%08d.bin", vb.BlockToObjectWAL.BaseDir, vb.GetVolume(), nextBlockWalNum))
	// if err != nil {
	// 	vb.logger().Error("ERROR OPENING BLOCK WAL:", "error", err)
	// 	return err
	// }

	// return nil
}

// blockWalChunkSize is the on-disk size of a serialized BlockLookup entry in
// the block-to-object checkpoint. Format (big-endian):
//
//	[StartBlock(8) | NumBlocks(2) | ObjectID(8) | ObjectOffset(4) | SeqNum(8) | CRC32(4)]
//
// SeqNum drives nonce + AAD reconstruction on the decrypt path; the field
// is populated unconditionally so encrypted and unencrypted volumes share
// a single checkpoint format. Pre-encryption
// checkpoints written under the previous 26-byte layout are unreadable by
// post-cutover binaries — volumes must be recreated. CRC32 is retained on
// metadata because the checkpoint stays plaintext (not AEAD-sealed).
const blockWalChunkSize = 34

// blockToObjectWALMagic identifies a block-to-object checkpoint stream.
//
// Unlike the chunk and single-file-WAL magics, this one is NOT switched by
// EncryptionEnabled: the block map is metadata and stays plaintext on both
// encrypted and unencrypted volumes, so both write the same magic. That is
// what lets a checkpoint be decoded from its bytes alone, with no volume
// identity and no master key.
var blockToObjectWALMagic = [4]byte{'V', 'B', 'W', 'B'}

// blockCheckpointHeaderSize is the size of the header preceding the
// blockWalChunkSize-sized records: magic(4) + Version(2) + Timestamp(8).
const blockCheckpointHeaderSize = 14

func (vb *VB) writeBlockWalChunk(block *BlockLookup) (data []byte) {
	data = make([]byte, blockWalChunkSize)

	binary.BigEndian.PutUint64(data[0:8], block.StartBlock)
	binary.BigEndian.PutUint16(data[8:10], block.NumBlocks)
	binary.BigEndian.PutUint64(data[10:18], block.ObjectID)
	binary.BigEndian.PutUint32(data[18:22], block.ObjectOffset)
	binary.BigEndian.PutUint64(data[22:30], block.SeqNum)

	checksum := crc32.ChecksumIEEE(data[0:30])
	binary.BigEndian.PutUint32(data[30:34], checksum)

	return data
}

// decodeBlockWalChunk decodes one blockWalChunkSize-sized record and verifies
// its CRC32. data must be exactly blockWalChunkSize bytes; callers slice it.
// Receiver-free so the checkpoint format can be decoded without a VB — see
// ParseBlockCheckpointBytes.
func decodeBlockWalChunk(data []byte) (block BlockLookup, err error) {
	if len(data) != blockWalChunkSize {
		return block, fmt.Errorf("block record is %d bytes, want %d", len(data), blockWalChunkSize)
	}

	block.StartBlock = binary.BigEndian.Uint64(data[:8])
	block.NumBlocks = binary.BigEndian.Uint16(data[8:10])
	block.ObjectID = binary.BigEndian.Uint64(data[10:18])
	block.ObjectOffset = binary.BigEndian.Uint32(data[18:22])
	block.SeqNum = binary.BigEndian.Uint64(data[22:30])

	checksum := binary.BigEndian.Uint32(data[30:34])

	checksumValidated := crc32.ChecksumIEEE(data[:30])
	if checksumValidated != checksum {
		return block, fmt.Errorf("checksum mismatch: got %d, want %d", checksumValidated, checksum)
	}

	return block, nil
}

func (vb *VB) readBlockWalChunk(data []byte) (block BlockLookup, err error) {
	block, err = decodeBlockWalChunk(data)
	if err != nil {
		vb.logger().Error("Block record decode failed", "error", err)
		return block, err
	}
	return block, nil
}

// chunkJob is one unit of work for the parallel chunk uploader: a filled
// chunk buffer plus the blocks it contains, ready for createChunkFile.
type chunkJob struct {
	buf    []byte
	blocks []Block
}

// parallelUploader bounds concurrent createChunkFile calls to vb.UploadWorkers
// in-flight jobs. Combined with the existing guest-write MaxPendingBytes
// backpressure gate, this keeps memory bounded during a drain. The first
// worker error cancels the derived context so in-flight createChunkFile calls
// (and the Backend.WriteCtx they drive) unwind promptly instead of continuing
// to push chunks that will just be discarded on drain failure.
type parallelUploader struct {
	vb       *VB
	walNum   uint64
	workChan chan chunkJob
	wg       sync.WaitGroup
	cancel   context.CancelFunc
	errOnce  sync.Once
	firstErr error
	failed   atomic.Bool
}

// newParallelUploader starts vb.UploadWorkers goroutines (1 if UploadWorkers
// is unset/invalid) draining a bounded work channel into createChunkFile for
// the given walNum. Returns the uploader and a context derived from ctx that
// is canceled on the first worker error; callers do not need to use the
// returned context themselves (submit/wait handle it), but createChunkFile
// receives it so backend writes fail fast after a sibling chunk's error.
func (vb *VB) newParallelUploader(ctx context.Context, walNum uint64) (*parallelUploader, context.Context) {
	workers := vb.UploadWorkers
	if workers <= 0 {
		workers = DefaultUploadWorkers
	}

	jobCtx, cancel := context.WithCancel(ctx)

	u := &parallelUploader{
		vb:       vb,
		walNum:   walNum,
		workChan: make(chan chunkJob, workers),
		cancel:   cancel,
	}

	u.wg.Add(workers)
	for range workers {
		go func() {
			defer u.wg.Done()
			for job := range u.workChan {
				if err := vb.createChunkFile(jobCtx, u.walNum, &job.buf, &job.blocks); err != nil {
					u.errOnce.Do(func() {
						u.firstErr = err
						u.failed.Store(true)
						cancel()
					})
				}
			}
		}()
	}

	return u, jobCtx
}

// submit copies buf and blocks into fresh slices, then hands the copy to a
// worker. The copy is required: the caller (WriteWALToChunkCtx /
// WriteShardedWALToChunkCtx) resets/reuses those backing arrays for the next
// chunk immediately after calling submit. No-op once an earlier job in this
// drain pass has failed — the derived context is already canceled, so there
// is nothing to gain from queuing more chunk uploads.
func (u *parallelUploader) submit(buf []byte, blocks []Block) {
	if u.failed.Load() {
		return
	}

	bufCopy := make([]byte, len(buf))
	copy(bufCopy, buf)
	blocksCopy := make([]Block, len(blocks))
	copy(blocksCopy, blocks)

	u.workChan <- chunkJob{buf: bufCopy, blocks: blocksCopy}
}

// wait closes the work channel, waits for all in-flight uploads to finish,
// and returns the first error encountered across all workers (nil if every
// chunk uploaded successfully).
func (u *parallelUploader) wait() error {
	close(u.workChan)
	u.wg.Wait()
	u.cancel()
	return u.firstErr
}

// WriteShardedWALToChunk consolidates all shard files into unified 4MB chunks.
// It briefly locks all shards to rotate to the next generation, then reads
// the closed shard files in parallel, deduplicates, sorts, and creates chunks.
func (vb *VB) WriteShardedWALToChunk(force bool) error {
	return vb.WriteShardedWALToChunkCtx(context.Background(), force)
}

// WriteShardedWALToChunkCtx is WriteShardedWALToChunk with a caller-supplied
// context threaded through the chunk uploads.
func (vb *VB) WriteShardedWALToChunkCtx(ctx context.Context, force bool) error {
	sw := vb.ShardedWAL
	if sw == nil {
		return fmt.Errorf("ShardedWAL not initialized")
	}

	// If no shard files are open, this VB instance doesn't own the WAL.
	// Skip consolidation (matches legacy WriteWALToChunk empty-DB guard).
	hasOpenShards := false
	for i := range NumShards {
		sw.Shards[i].mu.RLock()
		open := sw.Shards[i].DB != nil
		sw.Shards[i].mu.RUnlock()
		if open {
			hasOpenShards = true
			break
		}
	}
	if !hasOpenShards {
		return nil
	}

	currentWALNum := sw.WallNum.Load()

	// Check total size across all shards
	if !force {
		var totalSize int64
		for i := range NumShards {
			shard := sw.Shards[i]
			shard.mu.RLock()
			if shard.DB != nil {
				if fstat, err := shard.DB.Stat(); err == nil {
					totalSize += fstat.Size()
				}
			}
			shard.mu.RUnlock()
		}
		if totalSize < int64(vb.ObjBlockSize) {
			vb.logger().InfoContext(ctx, "Sharded WAL total size less than chunk size, skipping", "totalSize", totalSize)
			return nil
		}
	}

	// Lock all shards, sync, close, and open next generation
	for i := range NumShards {
		sw.Shards[i].mu.Lock()
	}

	// Sync and close all current shard files
	for i := range NumShards {
		shard := sw.Shards[i]
		if shard.DB != nil {
			if err := shard.DB.Sync(); err != nil {
				vb.logger().Warn("failed to sync shard WAL", "shard", i, "error", err)
			}
			if err := shard.DB.Close(); err != nil {
				vb.logger().Warn("failed to close shard WAL", "shard", i, "error", err)
			}
			shard.DB = nil
		}
	}

	// Open next generation of shard files
	nextWalNum := sw.WallNum.Add(1)
	header := vb.WALHeader()

	for i := range NumShards {
		shard := sw.Shards[i]
		filename := filepath.Join(sw.BaseDir,
			types.GetShardedWALPath(vb.GetVolume(), nextWalNum, i))

		if err := os.MkdirAll(filepath.Dir(filename), 0750); err != nil {
			for j := range NumShards {
				sw.Shards[j].mu.Unlock()
			}
			return fmt.Errorf("failed to create next shard directory %d: %w", i, err)
		}

		file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
		if err != nil {
			// Unlock all shards before returning
			for j := range NumShards {
				sw.Shards[j].mu.Unlock()
			}
			return fmt.Errorf("failed to open next shard %d: %w", i, err)
		}
		if _, err := file.Write(header); err != nil {
			if cerr := file.Close(); cerr != nil {
				vb.logger().Warn("failed to close shard file during cleanup", "shard", i, "error", cerr)
			}
			for j := range NumShards {
				sw.Shards[j].mu.Unlock()
			}
			return fmt.Errorf("failed to write header for next shard %d: %w", i, err)
		}
		shard.DB = file
	}

	// Unlock all shards — new writes proceed to next generation
	for i := range NumShards {
		sw.Shards[i].mu.Unlock()
	}

	// Read closed shard files in parallel
	type shardResult struct {
		blocks []Block
		err    error
	}
	results := make([]shardResult, NumShards)
	var wg sync.WaitGroup
	headerSize := vb.WALHeaderSize()

	for i := range NumShards {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()

			filename := filepath.Join(sw.BaseDir,
				types.GetShardedWALPath(vb.GetVolume(), currentWALNum, shardID))

			file, err := os.OpenFile(filename, os.O_RDONLY, 0600)
			if err != nil {
				if os.IsNotExist(err) {
					return // Empty shard, no file
				}
				results[shardID].err = fmt.Errorf("failed to open shard %d for reading: %w", shardID, err)
				return
			}
			defer file.Close()

			// Skip WAL header
			if _, err := file.Seek(int64(headerSize), io.SeekStart); err != nil {
				results[shardID].err = fmt.Errorf("failed to seek past header in shard %d: %w", shardID, err)
				return
			}

			var blocks []Block
			recordSize := 28 + int(vb.BlockSize)
			for {
				data := make([]byte, recordSize)
				n, err := file.Read(data)
				if err != nil {
					if err == io.EOF {
						break
					}
					results[shardID].err = fmt.Errorf("error reading shard %d: %w", shardID, err)
					return
				}
				if n < recordSize {
					break // Incomplete record at EOF, discard
				}

				// Validate checksum
				checksum := binary.BigEndian.Uint32(data[24:28])
				computed := crc32.ChecksumIEEE(data[:24])
				computed = crc32.Update(computed, crc32.IEEETable, data[28:])
				if computed != checksum {
					vb.logger().Error("checksum mismatch in sharded WAL", "shard", shardID)
					results[shardID].err = fmt.Errorf("checksum mismatch in shard %d", shardID)
					return
				}

				blocks = append(blocks, Block{
					SeqNum: binary.BigEndian.Uint64(data[:8]),
					Block:  binary.BigEndian.Uint64(data[8:16]),
					Len:    binary.BigEndian.Uint64(data[16:24]),
					Data:   data[28:],
				})
			}
			results[shardID].blocks = blocks
		}(i)
	}
	wg.Wait()

	// Check for errors and merge all blocks
	var allBlocks []Block
	for i := range NumShards {
		if results[i].err != nil {
			return results[i].err
		}
		allBlocks = append(allBlocks, results[i].blocks...)
	}

	if len(allBlocks) == 0 {
		return nil
	}

	// Deduplicate: highest SeqNum wins
	blocksMap := make(BlocksMapOptimised, len(allBlocks))
	for index, block := range allBlocks {
		if existing, ok := blocksMap[block.Block]; !ok || existing.SeqNum < block.SeqNum {
			blocksMap[block.Block] = BlockOptimised{
				SeqNum: block.SeqNum,
				Index:  index,
			}
		}
	}

	// Sort by block number
	sortedBlocks := make([]*Block, 0, len(blocksMap))
	for _, block := range blocksMap {
		sortedBlocks = append(sortedBlocks, &allBlocks[block.Index])
	}
	sort.Slice(sortedBlocks, func(i, j int) bool { return sortedBlocks[i].Block < sortedBlocks[j].Block })

	// Create 4MB chunks, uploaded via a bounded parallel worker pool.
	chunkBuffer := make([]byte, 0, vb.ObjBlockSize)
	matchedBlocks := make([]Block, 0)

	up, _ := vb.newParallelUploader(ctx, currentWALNum)

	for _, block := range sortedBlocks {
		chunkBuffer = append(chunkBuffer, block.Data...)
		matchedBlocks = append(matchedBlocks, Block{
			SeqNum: block.SeqNum,
			Block:  block.Block,
		})

		if len(chunkBuffer) >= int(vb.ObjBlockSize) {
			up.submit(chunkBuffer, matchedBlocks)
			chunkBuffer = chunkBuffer[:0]
			matchedBlocks = make([]Block, 0)
		}
	}

	// Submit remaining data
	if len(chunkBuffer) > 0 {
		up.submit(chunkBuffer, matchedBlocks)
	}

	if err := up.wait(); err != nil {
		return fmt.Errorf("failed to create chunk file: %w", err)
	}

	return nil
}

func (vb *VB) WriteWALToChunk(force bool) error {
	return vb.WriteWALToChunkCtx(context.Background(), force)
}

// WriteWALToChunkCtx is WriteWALToChunk with a caller-supplied context
// threaded through the chunk uploads. Consolidates the WAL (or dispatches to
// the sharded WAL) into a durable chunk object on the backend.
func (vb *VB) WriteWALToChunkCtx(ctx context.Context, force bool) (err error) {
	start := time.Now()
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		telemetry.RecordWALOp(ctx, "consolidate", vb.VolumeName, outcome, time.Since(start))
	}()

	// Dispatch to sharded implementation when enabled
	if vb.UseShardedWAL {
		return vb.WriteShardedWALToChunkCtx(ctx, force)
	}

	// First, lock, and close the current WAL file
	vb.WAL.mu.Lock()
	if len(vb.WAL.DB) == 0 {
		vb.WAL.mu.Unlock()
		return nil
	}
	currentWALNum := vb.WAL.WallNum.Load()
	pendingWAL := vb.WAL.DB[len(vb.WAL.DB)-1]

	// Check if we should write the chunk based on size
	if !force {
		fstat, err := pendingWAL.Stat()
		if err != nil {
			vb.WAL.mu.Unlock()
			return fmt.Errorf("could not validate WAL size: %w", err)
		}
		if fstat.Size() < int64(vb.ObjBlockSize) {
			vb.WAL.mu.Unlock()
			vb.logger().InfoContext(ctx, "WAL is less than 4MB, skipping chunk write")
			return nil
		}
	}

	// Sync and close the pending WAL under the lock so no concurrent
	// WriteWAL() can write after sync but before close.
	if err := pendingWAL.Sync(); err != nil {
		vb.WAL.mu.Unlock()
		return fmt.Errorf("failed to sync WAL before chunking: %w", err)
	}
	if err := pendingWAL.Close(); err != nil {
		vb.logger().Warn("failed to close pending WAL", "error", err)
	}

	// Open the next WAL file while still holding the lock so there is no
	// window where syncWALIfDirty or WriteWAL can see a closed DB entry.
	nextWalNum := vb.WAL.WallNum.Add(1)
	err = vb.openWALLocked(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, nextWalNum, vb.GetVolume())))
	vb.WAL.mu.Unlock()
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, currentWALNum, vb.GetVolume()))
	//filename := fmt.Sprintf("%s/%s/wal/chunks/wal.%08d.bin", vb.WAL.BaseDir, vb.GetVolume(), currentWALNum)
	pendingWAL2, err := os.OpenFile(filename, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer pendingWAL2.Close()

	// Read and validate WAL header (magic, version, timestamp)
	headers := make([]byte, vb.WALHeaderSize())
	if _, err := pendingWAL2.Read(headers); err != nil {
		return fmt.Errorf("error reading WAL headers: %w", err)
	}

	if !bytes.Equal(headers[:4], vb.WAL.WALMagic[:]) {
		// Under encryption a VBWL header here means a pre-encryption WAL
		// file is being replayed by an encrypted runtime; surface the
		// dedicated migration error so callers can route the operator to
		// the migration tool instead of an opaque "magic mismatch".
		if vb.EncryptionEnabled && bytes.Equal(headers[:4], []byte{'V', 'B', 'W', 'L'}) {
			return fmt.Errorf("%w: WAL magic mismatch in %s (file=VBWL, runtime=VBWE)", ErrPreEncryptionFormat, filename)
		}
		return fmt.Errorf("WAL magic mismatch in %s: got %q, expected %q", filename, headers[:4], vb.WAL.WALMagic[:])
	}

	if binary.BigEndian.Uint16(headers[4:6]) != vb.Version {
		return fmt.Errorf("version mismatch")
	}

	// Read blocks from WAL, pre-allocate the estimated number of blocks that could be in the chunk
	blocks := make([]Block, 0, (vb.ObjBlockSize/vb.BlockSize)+1)

	if vb.EncryptionEnabled {
		// Encrypted WAL record layout (magic VBWE):
		//   [SeqNum(8) | BlockNum(8) | BlockLen(8) | ciphertext(BlockLen) | tag(16)]
		// = 40 + BlockSize bytes. Nonce (SeqNum, VolumeUUID, DomainWAL),
		// AAD (volumeNameHash, BlockNum, SeqNum). The 16-byte GCM tag
		// subsumes the dropped CRC32; aead.Open validates confidentiality
		// and integrity in one pass.
		recordSize := 40 + int(vb.BlockSize)
		record := make([]byte, recordSize)
		var aad [AADLen]byte
		initAAD(&aad, vb.volumeNameHash)
		for {
			if _, err := io.ReadFull(pendingWAL2, record); err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("error reading encrypted WAL data: %w", err)
			}

			seqNum := binary.BigEndian.Uint64(record[0:8])
			blockNum := binary.BigEndian.Uint64(record[8:16])
			blockLen := binary.BigEndian.Uint64(record[16:24])

			nonce := makeNonce(seqNum, vb.VolumeUUID, DomainWAL)
			updateAAD(&aad, blockNum, seqNum)
			plain, err := vb.aead.Open(nil, nonce[:], record[24:], aad[:])
			if err != nil {
				pos, _ := pendingWAL2.Seek(0, io.SeekCurrent)
				vb.logger().Error("WAL aead.Open failed", "filename", filename, "pos", pos, "block", blockNum, "seqNum", seqNum)
				return fmt.Errorf("%w: WAL record block %d seqNum %d in %s: %w", ErrIntegrity, blockNum, seqNum, filename, err)
			}

			blocks = append(blocks, Block{
				SeqNum: seqNum,
				Block:  blockNum,
				Len:    blockLen,
				Data:   plain,
			})
		}
	} else {
		for {
			data := make([]byte, 28+vb.BlockSize)
			_, err := pendingWAL2.Read(data)
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("error reading WAL data: %w", err)
			}

			// Validate checksum
			checksum := binary.BigEndian.Uint32(data[24:28])

			checksumValidated := crc32.ChecksumIEEE(data[:24])
			// Skip the checksum (24:28), just the data next
			checksumValidated = crc32.Update(checksumValidated, crc32.IEEETable, data[28:])

			if checksumValidated != checksum {
				pos, err := pendingWAL2.Seek(0, io.SeekCurrent)
				if err != nil {
					return fmt.Errorf("error seeking in WriteWALToChunk: %w", err)
				}

				vb.logger().Error("checksum mismatch in WriteWALToChunk", "filename", filename, "pos", pos, "checksum", checksum, "checksumValidated", checksumValidated)
				return fmt.Errorf("checksum mismatch in WriteWALToChunk")
			}

			blocks = append(blocks, Block{
				SeqNum: binary.BigEndian.Uint64(data[:8]),
				Block:  binary.BigEndian.Uint64(data[8:16]),
				Len:    binary.BigEndian.Uint64(data[16:24]),
				Data:   data[28:],
			})
		}
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

	up, ctx := vb.newParallelUploader(ctx, currentWALNum)

	for _, block := range sortedBlocks {
		chunkBuffer = append(chunkBuffer, block.Data...)
		matchedBlocks = append(matchedBlocks, Block{
			SeqNum: block.SeqNum,
			Block:  block.Block,
		})

		// If buffer is full (default 4MB), submit for upload
		if len(chunkBuffer) >= int(vb.ObjBlockSize) {
			up.submit(chunkBuffer, matchedBlocks)

			chunkBuffer = chunkBuffer[:0] // Reset buffer
			matchedBlocks = make([]Block, 0)
		}
	}

	// Submit any remaining data as the last chunk
	if len(chunkBuffer) > 0 {
		up.submit(chunkBuffer, matchedBlocks)
	}

	if err := up.wait(); err != nil {
		vb.logger().ErrorContext(ctx, "Failed to create chunk file", "error", err)
		return err
	}

	return nil
}
func (vb *VB) createChunkFile(ctx context.Context, currentWALNum uint64, chunkBuffer *[]byte, matchedBlocks *[]Block) (err error) {
	//runtime.LockOSThread()
	//defer runtime.UnlockOSThread()

	// Atomically allocate this chunk's ObjectID so concurrent consolidations
	// never share a chunk key. The old Load-at-caller/Add-at-end split let two
	// drains reuse an ID and overwrite a chunk (fatal AEAD mismatch on read).
	chunkIndex := vb.ObjectNum.Add(1) - 1

	//vb.logger().Info("Creating chunk file", "chunkIndex", chunkIndex, "currentWALNum", currentWALNum)

	headers := vb.ChunkHeader()

	// On encrypted volumes the chunk body is rebuilt as a sequence of sealed
	// blocks at stride BlockSize+16. Per-block nonce: (block.SeqNum,
	// VolumeUUID, DomainChunk). AAD: (volumeNameHash, block.Block,
	// block.SeqNum). The on-disk chunk carries no nonce; decrypt reconstructs
	// it from BlockLookup.SeqNum (set below) at read time.
	bodyBuffer := chunkBuffer
	if vb.EncryptionEnabled {
		bs := int(vb.BlockSize)
		sealed := make([]byte, 0, len(*matchedBlocks)*(bs+16))
		var aad [AADLen]byte
		initAAD(&aad, vb.volumeNameHash)
		for i, mb := range *matchedBlocks {
			start := i * bs
			end := start + bs
			if end > len(*chunkBuffer) {
				return fmt.Errorf("createChunkFile: matchedBlocks/chunkBuffer length mismatch (block %d of %d, buffer %d bytes)", i, len(*matchedBlocks), len(*chunkBuffer))
			}
			nonce := makeNonce(mb.SeqNum, vb.VolumeUUID, DomainChunk)
			updateAAD(&aad, mb.Block, mb.SeqNum)
			sealed = vb.aead.Seal(sealed, nonce[:], (*chunkBuffer)[start:end], aad[:])
		}
		bodyBuffer = &sealed
	}

	err = vb.Backend.WriteCtx(ctx, types.FileTypeChunk, chunkIndex, &headers, bodyBuffer)
	if err != nil {
		return err
	}

	// After upload completion, remove from PendingBackendWrites
	// Build a hash map of matched block numbers for O(1) lookup instead of O(n²)
	matchedBlockMap := make(map[uint64]struct{}, len(*matchedBlocks))
	for _, mb := range *matchedBlocks {
		matchedBlockMap[mb.Block] = struct{}{}
	}

	vb.PendingBackendWrites.mu.Lock()

	// Filter pending writes in-place using the hash map for O(n) complexity
	// We iterate through the slice once, keeping non-matched blocks at the front
	n := 0
	var freedBytes int64
	for _, block := range vb.PendingBackendWrites.Blocks {
		if _, matched := matchedBlockMap[block.Block]; !matched {
			// Block not in matched set, keep it in pending writes
			vb.PendingBackendWrites.Blocks[n] = block
			n++
		} else {
			// Block was successfully written to backend, update cache
			if vb.Cache.Config.Size > 0 {
				vb.Cache.lru.Add(block.Block, block.Data)
			}
			freedBytes += int64(len(block.Data))
		}
	}
	// Truncate the slice to remove processed blocks
	vb.PendingBackendWrites.Blocks = vb.PendingBackendWrites.Blocks[:n]

	vb.PendingBackendWrites.mu.Unlock()

	// This chunk's bytes have left both Writes.Blocks (filtered out at flush
	// time) and PendingBackendWrites.Blocks (just above) — release them from
	// the backpressure counter WriteAtCtx's awaitBackpressure gate watches.
	vb.pendingBytes.Add(-freedBytes)

	headerLen := len(headers)

	// Per-block on-disk stride: encrypted chunks add a 16-byte GCM tag after
	// each ciphertext block; unencrypted chunks stay at BlockSize.
	stride := int(vb.BlockSize)
	if vb.EncryptionEnabled {
		stride += 16
	}

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
			NumBlocks:    utils.SafeIntToUint16(numBlocks),
			ObjectID:     chunkIndex,
			ObjectOffset: utils.SafeIntToUint32(headerLen + (k * stride)),
			SeqNum:       block.SeqNum,
		}

		// TODO: Optimise for number of consecutive blocks to reduce the memory size
		oldBlock, hadOld := vb.BlocksToObject.BlockLookup[block.Block]

		// Reject a stale drain that would repoint this block backwards. Two
		// concurrent drains can process sequential WAL segments in parallel
		// (drainMu now prevents that, but a direct WriteWALToChunk caller or a
		// future scheduling change could reintroduce overlap); if the OLDER
		// segment's createChunkFile reached this write last, an unconditional
		// overwrite would install its stale chunk AND make gcTrackBlock
		// decrement the newer, live chunk's refcount -- potentially to zero,
		// letting the next sweep delete a still-referenced chunk. Only advance
		// on a strictly newer SeqNum, mirroring BlockStore.MarkPersisted. The
		// stale chunk this call already uploaded is simply left unreferenced,
		// so it becomes an ordinary GC candidate instead of corrupting the map.
		// gcTrackBlock is skipped on rejection so the live chunk
		// (oldBlock.ObjectID) keeps its reference and is never decremented.
		// The stale chunk we already uploaded (newBlock.ObjectID) is instead
		// recorded at refcount zero -- but only materialized if absent, so a
		// sibling block in this same batch that legitimately installs this
		// chunk still increments it to a positive count. That makes a wholly
		// orphaned stale chunk a prompt sweep candidate instead of leaking it
		// until the once-per-lifetime reconcile, while never fabricating a
		// reference that would pin a live chunk.
		if hadOld && oldBlock.SeqNum >= newBlock.SeqNum {
			if vb.GCEnabled {
				if _, tracked := vb.gcRefcount[newBlock.ObjectID]; !tracked {
					vb.gcRefcount[newBlock.ObjectID] = 0
				}
			}
			continue
		}

		vb.BlocksToObject.BlockLookup[block.Block] = newBlock
		vb.gcTrackBlock(oldBlock.ObjectID, hadOld, newBlock.ObjectID)

		// Update BlockStore: transition from Pending to Persisted. Pass the
		// sealed SeqNum so the location and seqNum stay bound as a unit; a
		// stale drain whose block was since re-written is rejected here.
		if vb.UseBlockStore && vb.BlockStore != nil {
			vb.BlockStore.MarkPersisted(block.Block, chunkIndex, newBlock.ObjectOffset, block.SeqNum)
		}
	}
	vb.BlocksToObject.dirty.Store(true)
	vb.BlocksToObject.mu.Unlock()

	// Do NOT checkpoint per chunk here. createChunkFile runs under the parallel
	// upload pool, so concurrent per-chunk SaveLiveCheckpointCtx calls can PUT
	// out of snapshot order: a worker holding an early, partial map can land its
	// write last and leave the live checkpoint pointing at a stale seqNum, which
	// an encrypted reattach then decrypts as garbage (bad superblock). dirty is
	// set above; the single serialized SaveLiveCheckpointCtx that every driver
	// runs after up.wait() (DrainToBackendCtx, RecoverLocalWALs, Close) writes
	// the complete coalesced map exactly once, after all chunk PUTs have landed.
	//
	// The same reasoning rules out sweeping GC candidates from here too: a
	// chunk this call just dereferenced (oldBlock above) is not safely
	// deletable until the coalesced live checkpoint that stops referencing
	// it has actually landed on the backend. GC only ever sweeps from the
	// single serialized point right after that checkpoint write succeeds.
	return nil
}

// gcTrackBlock updates the in-memory chunk refcount for one BlockLookup
// mutation. Call under BlocksToObject.mu (the same lock that guards
// BlockLookup itself — see gcRefcount's doc comment on VB). oldObjectID/
// hadOld describe the block's previous occupant, if any; newObjectID is the
// chunk it now belongs to. A no-op when GCEnabled is false so the refcount
// map is never allocated or touched for the common, GC-disabled case.
func (vb *VB) gcTrackBlock(oldObjectID uint64, hadOld bool, newObjectID uint64) {
	if !vb.GCEnabled {
		return
	}
	if hadOld && oldObjectID != newObjectID {
		if n := vb.gcRefcount[oldObjectID]; n > 0 {
			vb.gcRefcount[oldObjectID] = n - 1
		}
	}
	vb.gcRefcount[newObjectID]++
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
	if err := json.NewEncoder(file).Encode(vb.Writes.Blocks); err != nil {
		vb.Writes.mu.RUnlock()
		return fmt.Errorf("failed to encode blocks to JSON: %w", err)
	}

	defer vb.Writes.mu.RUnlock()

	return nil
}

func (vb *VB) SaveBlockState() (err error) {
	return vb.SaveBlockStateCtx(context.Background())
}

// SaveBlockStateCtx is SaveBlockState with a caller-supplied context threaded
// through the checkpoint upload.
func (vb *VB) SaveBlockStateCtx(ctx context.Context) (err error) {
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
			vb.logger().Error("ERROR WRITING BLOCK TO BLOCK WAL:", "error", err)
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
	if _, err = file.Write(checkpoint); err != nil {
		return fmt.Errorf("failed to write block checkpoint: %w", err)
	}

	headers := []byte{}

	// Next, upload the file to the backend
	err = vb.Backend.WriteCtx(ctx, types.FileTypeBlockCheckpoint, vb.BlockToObjectWAL.WallNum.Load(), &headers, &checkpoint)
	if err != nil {
		return err
	}

	// Increment the Block WAL sequence number
	vb.BlockToObjectWAL.WallNum.Add(1)

	return err
}

// walkBlockCheckpoint validates a serialized block checkpoint's header
// (magic + version) and calls fn once per decoded record, in on-disk order.
//
// Receiver-free and the single place the checkpoint wire format is parsed, so
// every caller — in-process load, and out-of-process readers that have only
// the bytes — decodes identically. Two copies of this loop drifting apart
// would make one caller silently wrong with no test noticing.
func walkBlockCheckpoint(raw []byte, version uint16, walMagic [4]byte, fn func(BlockLookup)) error {
	if len(raw) < blockCheckpointHeaderSize {
		return fmt.Errorf("checkpoint is %d bytes, shorter than the %d-byte header", len(raw), blockCheckpointHeaderSize)
	}

	if !bytes.Equal(raw[:4], walMagic[:]) {
		return fmt.Errorf("magic mismatch: got %q, want %q", raw[:4], walMagic[:])
	}

	if got := binary.BigEndian.Uint16(raw[4:6]); got != version {
		return fmt.Errorf("version mismatch: got %d, want %d", got, version)
	}

	// Reject a partial trailing record rather than reading up to it and
	// returning a short map: the records are fixed-width, so a remainder
	// means the bytes are truncated or not a checkpoint at all, and a
	// silently-short referenced set is worse than a hard error.
	dataSize := len(raw) - blockCheckpointHeaderSize
	if dataSize%blockWalChunkSize != 0 {
		return fmt.Errorf("checkpoint has %d trailing bytes (data section %d bytes is not a multiple of %d-byte records)",
			dataSize%blockWalChunkSize, dataSize, blockWalChunkSize)
	}

	for offset := blockCheckpointHeaderSize; offset+blockWalChunkSize <= len(raw); offset += blockWalChunkSize {
		block, err := decodeBlockWalChunk(raw[offset : offset+blockWalChunkSize])
		if err != nil {
			return fmt.Errorf("record at offset %d: %w", offset, err)
		}
		fn(block)
	}

	return nil
}

// ParseBlockCheckpointBytes decodes a block-to-object checkpoint into its
// block map, keyed by StartBlock, from the raw bytes alone.
//
// This is the read-only, no-side-effect way to inspect a volume's referenced
// chunk set. Constructing a VB to reach the same map is not equivalent: New()
// creates directories and starts the WAL syncer and chunk uploader, and the
// uploader writes to the backend — attaching a second writer to the volume
// being inspected. This function touches nothing.
//
// No master key is required: the block map is metadata and is written
// plaintext on encrypted volumes too (see blockToObjectWALMagic).
func ParseBlockCheckpointBytes(raw []byte, version uint16) (map[uint64]BlockLookup, error) {
	blocks := make(map[uint64]BlockLookup)
	if err := walkBlockCheckpoint(raw, version, blockToObjectWALMagic, func(block BlockLookup) {
		blocks[block.StartBlock] = block
	}); err != nil {
		return nil, err
	}
	return blocks, nil
}

// parseBlockCheckpoint deserialises a checkpoint binary into BlocksToObject.BlockLookup.
// Caller must hold BlocksToObject.mu (write).
func (vb *VB) parseBlockCheckpoint(checkpoint []byte) error {
	vb.BlocksToObject.BlockLookup = make(map[uint64]BlockLookup, 0)
	// Rebuild refcounts from scratch alongside the map: any zero-refcount
	// GC candidates tracked before this load (from a prior process
	// lifetime) are not derivable from the loaded map — a chunk with no
	// live references looks identical to one that was never referenced —
	// so they are lost here. That only makes GC miss reclaiming already-
	// known garbage across a restart; it never causes deleting something
	// still live, since every count below is derived from exactly the map
	// that just became this process's live view.
	if vb.GCEnabled {
		vb.gcRefcount = make(map[uint64]uint64)
	}

	vb.logger().Debug("Loaded checkpoint", "checkpoint", checkpoint)

	// Track the highest chunk ObjectID the map references so ObjectNum can be
	// reconciled to max+1 below, mirroring SeqNum -> maxSeqNum in RecoverLocalWALs.
	var maxObjectID uint64
	haveObject := false

	err := walkBlockCheckpoint(checkpoint, vb.Version, vb.BlockToObjectWAL.WALMagic, func(block BlockLookup) {
		vb.BlocksToObject.BlockLookup[block.StartBlock] = block
		if vb.GCEnabled {
			vb.gcRefcount[block.ObjectID]++
		}
		if !haveObject || block.ObjectID > maxObjectID {
			maxObjectID = block.ObjectID
			haveObject = true
		}
		if vb.UseBlockStore && vb.BlockStore != nil {
			vb.BlockStore.SetPersisted(block.StartBlock, block.ObjectID, block.ObjectOffset, block.SeqNum)
		}
	})
	if err != nil {
		vb.logger().Error("Error reading checkpoint", "error", err)
		return err
	}

	// Runtime drains persist chunks but not config.json ObjectNum, so a re-Open can
	// load a stale-low ObjectNum. Force it past every referenced chunk ID here so
	// createChunkFile cannot reuse a live ID and cause a durable AEAD tag failure.
	if haveObject {
		next := maxObjectID + 1
		for {
			current := vb.ObjectNum.Load()
			if next <= current {
				break
			}
			if vb.ObjectNum.CompareAndSwap(current, next) {
				break
			}
		}
	}

	return nil
}

// numberedCheckpointHighWater reads the current WallNum's numbered
// checkpoint (checkpoints/blocks.%08d.bin -- the only one ever read as a
// fallback, see LoadBlockStateCtx) and returns the highest chunk ObjectID
// it references. ok is false when no numbered checkpoint has been written
// yet for this volume (nothing to protect).
func (vb *VB) numberedCheckpointHighWater(ctx context.Context) (highWater uint64, ok bool, err error) {
	data, err := vb.Backend.ReadCtx(ctx, types.FileTypeBlockCheckpoint, vb.BlockToObjectWAL.WallNum.Load(), 0, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, false, nil
		}
		return 0, false, err
	}

	haveObject := false
	walkErr := walkBlockCheckpoint(data, vb.Version, vb.BlockToObjectWAL.WALMagic, func(block BlockLookup) {
		if !haveObject || block.ObjectID > highWater {
			highWater = block.ObjectID
			haveObject = true
		}
	})
	if walkErr != nil {
		return 0, false, walkErr
	}
	return highWater, haveObject, nil
}

// ensureGCFloor lazily computes and caches the GC floor: one past the
// highest chunk ObjectID referenced by the current numbered checkpoint, so
// chunk GC never deletes an object that checkpoint depends on (it is read
// as a fallback whenever the live checkpoint is unreadable -- see
// LoadLiveCheckpointCtx). Computed once per VB lifetime: numbered
// checkpoints are only rewritten at Close/RecoverLocalWALs, both of which
// end the process lifetime this cached value is valid for.
func (vb *VB) ensureGCFloor(ctx context.Context) uint64 {
	if vb.gcFloorReady.Load() {
		return vb.gcFloor.Load()
	}

	high, ok, err := vb.numberedCheckpointHighWater(ctx)
	if err != nil {
		vb.logger().Warn("chunk GC: failed to compute numbered-checkpoint floor, sweep skipped this round", "err", err)
		// Not cached: gcFloorReady stays false so the next sweep retries
		// instead of treating a transient read failure as "nothing to
		// protect".  ^uint64(0) (all bits set) makes every candidate fail
		// the "id >= floor" check this round.
		return ^uint64(0)
	}

	floor := uint64(0)
	if ok {
		floor = high + 1
	}
	vb.gcFloor.Store(floor)
	vb.gcFloorReady.Store(true)
	return floor
}

// snapPrefix is the top-level key prefix every CreateSnapshot writes a
// snapshot's checkpoint and config.json under (see CreateSnapshot's
// WriteTo(snapshotID, ...) calls) and the same prefix spinifex's
// DescribeSnapshots scans with a bucket-wide ListObjectsV2.
const snapPrefix = "snap-"

// ensureGCSnapshotSafe reports whether it is safe for chunk GC to run at
// all against this volume: no existing snapshot may reference it. This is
// the binary ancestry guard chunk GC relies on instead of computing a
// GCFloor from snapshot state -- see scanForOwnSnapshots's doc comment for
// why. The result is cached for the process lifetime once a scan completes,
// whether the outcome is "safe" or "unsafe": once a snapshot of this volume
// exists it always will (this phase does not lower the guard on snapshot
// deletion, matching the conservative default). A scan that errors is not
// cached, so the next sweep attempt retries rather than treating a
// transient failure as permanently unsafe -- or worse, permanently safe.
//
// This only covers snapshots that exist at scan time, plus any this same
// process creates afterward (see the gcLatchedOff CreateSnapshot sets
// below). A different process creating a snapshot of this volume after this
// scan last ran is not detected here -- that cross-process window is the
// reason GCEnabled defaults to false; it is not solved by this guard.
func (vb *VB) ensureGCSnapshotSafe(ctx context.Context) bool {
	if vb.gcLatchedOff.Load() {
		return false
	}
	if vb.gcSnapshotChecked.Load() {
		return vb.gcSnapshotSafe.Load()
	}

	safe, err := vb.scanForOwnSnapshots(ctx)
	if err != nil {
		vb.logger().Warn("chunk GC: snapshot-ancestry scan failed, sweep skipped this round", "err", err)
		return false
	}

	vb.gcSnapshotSafe.Store(safe)
	vb.gcSnapshotChecked.Store(true)
	if !safe {
		vb.logger().Warn("chunk GC: disabled, an existing snapshot references this volume", "volume", vb.VolumeName)
	}
	return safe
}

// scanForOwnSnapshots answers "does any existing snapshot reference this
// volume" the only way it can be answered without spinifex-side plumbing:
// snapshots are NOT stored under this volume's own key prefix. CreateSnapshot
// writes to {snapshotID}/, a top-level prefix sibling to {volumeName}/, keyed
// by an independently generated snapshot ID with no derivable relationship
// to the source volume name (confirmed against types.GetFilePath's layout
// and spinifex's own bucket-wide ListObjectsV2(Prefix:"snap-") in
// DescribeSnapshots). So this cannot be a bounded, per-volume prefix listing:
// it lists every top-level "snap-" prefix in the backend and reads each
// candidate's config.json to compare SourceVolumeName.
//
// Reading config.json needs no decryption key: the metadata envelope
// authenticates the JSON, it does not encrypt it (see StateBody's doc
// comment), so this works identically for encrypted and unencrypted
// volumes.
//
// Cost: O(total snapshots in the whole deployment), not O(this volume's own
// snapshots). Run at most once per VB lifetime (see ensureGCSnapshotSafe's
// caching) and only when GCEnabled, never on a request path -- but it is a
// real, deployment-wide-scaling cost that a volume-scoped snapshot index
// would avoid. Out of scope for this phase.
func (vb *VB) scanForOwnSnapshots(ctx context.Context) (safe bool, err error) {
	names, err := vb.Backend.ListPrefixesCtx(ctx, snapPrefix)
	if err != nil {
		return false, fmt.Errorf("list snapshot prefixes: %w", err)
	}

	for _, name := range names {
		configData, readErr := vb.Backend.ReadFromCtx(ctx, name, types.FileTypeConfig, 0, 0, 0)
		if readErr != nil {
			if errors.Is(readErr, os.ErrNotExist) {
				// Checkpoint written but config.json not yet landed (see
				// CreateSnapshot's write order) or already deleted. Either
				// way this is not a currently-valid, readable snapshot, and
				// nothing else can treat it as one either.
				continue
			}
			return false, fmt.Errorf("read %s/config.json: %w", name, readErr)
		}

		var probe struct {
			SourceVolumeName string `json:"SourceVolumeName"`
		}
		if unmarshalErr := json.Unmarshal(StateBody(configData), &probe); unmarshalErr != nil {
			// Malformed or foreign metadata this scan doesn't understand.
			// Fail closed: "cannot rule out", not "not mine".
			return false, fmt.Errorf("parse %s/config.json: %w", name, unmarshalErr)
		}

		if probe.SourceVolumeName == vb.VolumeName {
			return false, nil
		}
	}

	return true, nil
}

// chunkKeyPattern extracts the ObjectID from a chunk key of the form
// "{volumeName}/chunks/chunk.%08d.bin" (see types.GetFilePath), independent
// of volumeName's own content (volume names are operator-chosen and must
// not be assumed free of digits or path separators).
var chunkKeyPattern = regexp.MustCompile(`/chunks/chunk\.(\d+)\.bin$`)

// parseChunkObjectID extracts the ObjectID from a chunk object key, or
// returns ok=false for any key that isn't a chunk file (e.g. an unrelated
// key sharing a list prefix by coincidence).
func parseChunkObjectID(key string) (id uint64, ok bool) {
	m := chunkKeyPattern.FindStringSubmatch(key)
	if m == nil {
		return 0, false
	}
	id, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// reconcileChunksOnce lists this volume's own chunks/ prefix once per VB
// lifetime and adds any chunk object not already tracked in gcRefcount as a
// zero-refcount GC candidate. Without this, a chunk that became
// unreferenced and was never swept before this process last closed can
// never be reclaimed by a later VB instance: parseBlockCheckpoint's rebuild
// only sees the current live map, and a zero-reference chunk is by
// definition absent from it -- indistinguishable from a chunk that was
// never minted at all. This is the "bounded, opt-in enumeration reconcile"
// this design relies on to stay correct across a restart; unlike
// ensureGCSnapshotSafe's bucket-wide scan, it is scoped to this volume's
// own chunks/ prefix only, proportional to this volume's own chunk count.
//
// Assumes parseBlockCheckpoint (and so gcRefcount's initial population)
// runs at most once per VB lifetime, before the first sweep -- true today
// (only LoadLiveCheckpointCtx/LoadBlockStateCtx call it, both Open-time
// paths) but would need revisiting if that ever changes, since a later
// rebuild would wipe the zero-entries this adds without re-adding them
// (gcReconciled would already be true).
func (vb *VB) reconcileChunksOnce(ctx context.Context) {
	if vb.gcReconciled.Load() {
		return
	}

	keys, err := vb.Backend.ListObjectsCtx(ctx, vb.VolumeName+"/chunks/")
	if err != nil {
		vb.logger().Warn("chunk GC: chunk-prefix reconcile failed, will retry next sweep", "err", err)
		return
	}

	vb.BlocksToObject.mu.Lock()
	for _, key := range keys {
		id, ok := parseChunkObjectID(key)
		if !ok {
			continue
		}
		if _, tracked := vb.gcRefcount[id]; !tracked {
			vb.gcRefcount[id] = 0
		}
	}
	vb.BlocksToObject.mu.Unlock()

	vb.gcReconciled.Store(true)
}

// sweepChunks deletes superseded chunk objects: those below the GC
// watermark captured here, at or above the numbered-checkpoint floor (see
// ensureGCFloor), whose in-memory refcount has dropped to zero. Call only
// after a successful SaveLiveCheckpointCtx (runGCSweep and Close both
// arrange this) -- the durable live checkpoint has already stopped
// referencing anything this sweep is about to delete, so a crash between
// checkpoint save and sweep leaves garbage on disk (safe, just not yet
// reclaimed), never a dangling reference (unsafe).
func (vb *VB) sweepChunks(ctx context.Context) {
	if !vb.GCEnabled {
		return
	}
	if !vb.ensureGCSnapshotSafe(ctx) {
		return
	}

	vb.reconcileChunksOnce(ctx)

	floor := vb.ensureGCFloor(ctx)
	// Watermark is captured once, here, and never revisited for this sweep:
	// any chunk minted after this point is structurally excluded by the
	// "id < watermark" check below, without needing to hold
	// BlocksToObject.mu across the delete calls that follow.
	watermark := vb.ObjectNum.Load()

	vb.BlocksToObject.mu.Lock()
	var candidates []uint64
	for id, refs := range vb.gcRefcount {
		if refs == 0 && id >= floor && id < watermark {
			candidates = append(candidates, id)
		}
	}
	vb.BlocksToObject.mu.Unlock()

	swept := vb.deleteChunkObjects(ctx, candidates)

	if swept > 0 {
		vb.logger().Info("chunk GC: sweep complete", "swept", swept, "candidates", len(candidates), "floor", floor, "watermark", watermark)
	} else {
		vb.logger().Debug("chunk GC: sweep found nothing to reclaim", "floor", floor, "watermark", watermark)
	}
}

// deleteChunkObjects issues a DeleteObject call for each chunk ObjectID in
// ids and returns how many were actually reclaimed (deleted, or already
// gone). This is the sole call site chunk GC uses to remove backend
// objects, deliberately isolated from sweepChunks's candidate-selection
// logic: predastore has no batch DeleteObjects route today (POST
// ?delete= returns 405 -- see the s3 backend's Delete doc comment), so
// this issues one DeleteCtx per key, but a future batched backend call can
// replace this loop's body without sweepChunks or its safety predicate
// (floor/watermark/refcount) changing at all. Not a hot path: a sweep runs
// at most every few minutes over at most tens of candidate keys.
//
// A delete that fails with anything other than "already gone"
// (errors.Is(err, os.ErrNotExist)) is left both in gcRefcount and out of
// the swept count, so the next sweep retries it -- silent under-collection
// (a leaked chunk) is always preferred over losing track of a candidate.
func (vb *VB) deleteChunkObjects(ctx context.Context, ids []uint64) (swept int) {
	for _, id := range ids {
		if err := vb.Backend.DeleteCtx(ctx, types.FileTypeChunk, id); err != nil && !errors.Is(err, os.ErrNotExist) {
			vb.logger().Warn("chunk GC: delete failed, will retry next sweep", "objectID", id, "err", err)
			continue
		}
		vb.BlocksToObject.mu.Lock()
		delete(vb.gcRefcount, id)
		vb.BlocksToObject.mu.Unlock()
		swept++
	}
	return swept
}

// runGCSweep drains this VB to the backend -- persisting a live checkpoint
// that already excludes zero-refcount chunks -- and, only if that succeeds,
// sweeps the chunks it left excluded. Sweeping without a preceding
// successful drain would risk deleting a chunk an already-durable
// checkpoint still references; see sweepChunks's doc comment.
func (vb *VB) runGCSweep(ctx context.Context) {
	if err := vb.DrainToBackendCtx(ctx); err != nil {
		vb.logger().Warn("chunk GC: drain before sweep failed, sweep skipped", "err", err)
		return
	}
	vb.sweepChunks(ctx)
}

// Load the previous blockstate from disk.
func (vb *VB) LoadBlockState() (err error) {
	return vb.LoadBlockStateCtx(context.Background())
}

// LoadBlockStateCtx is LoadBlockState with a caller-supplied context threaded
// through the checkpoint fetch.
func (vb *VB) LoadBlockStateCtx(ctx context.Context) (err error) {
	var checkpoint []byte

	// Step 1. Validate the local persistent disk contains the state
	filename := fmt.Sprintf("%s/%s", vb.BaseDir, types.GetFilePath(types.FileTypeBlockCheckpoint, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))

	_, err = os.Stat(filename)
	if err != nil {
		vb.logger().InfoContext(ctx, "No state found in local file, using backend state", "error", err)

		// Open the latest checkpoint from the backend
		checkpoint, err = vb.Backend.ReadCtx(ctx, types.FileTypeBlockCheckpoint, vb.BlockToObjectWAL.WallNum.Load(), 0, 0)

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

	vb.BlocksToObject.mu.Lock()
	defer vb.BlocksToObject.mu.Unlock()
	return vb.parseBlockCheckpoint(checkpoint)
}

// SaveLiveCheckpoint writes the current block map to a fixed S3 key so a concurrent
// process can read a crash-consistent checkpoint without stopping nbdkit. Unlike
// SaveBlockState, this does not write a local file and does not increment WallNum —
// the key is always overwritten in place.
func (vb *VB) SaveLiveCheckpoint() error {
	return vb.SaveLiveCheckpointCtx(context.Background())
}

// SaveLiveCheckpointCtx is SaveLiveCheckpoint with a caller-supplied context.
func (vb *VB) SaveLiveCheckpointCtx(ctx context.Context) error {
	if !vb.BlocksToObject.dirty.Load() {
		return nil
	}

	// Serialize the map under the read lock, then release before doing I/O so
	// the lock is not held across network writes or retry sleeps.
	vb.BlocksToObject.mu.RLock()
	checkpoint := vb.BlockToObjectWALHeader()
	for _, block := range vb.BlocksToObject.BlockLookup {
		checkpoint = append(checkpoint, vb.writeBlockWalChunk(&block)...)
	}
	vb.BlocksToObject.mu.RUnlock()

	headers := []byte{}
	backoff := time.Second
	var err error
	for attempt := range 3 {
		err = vb.Backend.WriteCtx(ctx, types.FileTypeBlockCheckpointLive, 0, &headers, &checkpoint)
		if err == nil {
			vb.BlocksToObject.dirty.Store(false)
			return nil
		}
		if attempt < 2 {
			slog.WarnContext(ctx, "SaveLiveCheckpoint: write failed, retrying",
				"attempt", attempt+1, "backoff", backoff, "err", err)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
			backoff *= 2
		}
	}
	return fmt.Errorf("SaveLiveCheckpoint: failed after retries: %w", err)
}

// LoadLiveCheckpoint reads the live checkpoint written by SaveLiveCheckpoint. If no
// live checkpoint exists yet, it falls back to LoadBlockState (numbered checkpoint).
func (vb *VB) LoadLiveCheckpoint() error {
	return vb.LoadLiveCheckpointCtx(context.Background())
}

// LoadLiveCheckpointCtx is LoadLiveCheckpoint with a caller-supplied context.
func (vb *VB) LoadLiveCheckpointCtx(ctx context.Context) error {
	checkpoint, err := vb.Backend.ReadCtx(ctx, types.FileTypeBlockCheckpointLive, 0, 0, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return vb.LoadBlockStateCtx(ctx)
		}
		return fmt.Errorf("read live checkpoint: %w", err)
	}
	vb.BlocksToObject.mu.Lock()
	defer vb.BlocksToObject.mu.Unlock()
	return vb.parseBlockCheckpoint(checkpoint)
}

// readWALFileForRecovery opens a WAL file read-only and returns all valid blocks.
// Unlike WriteWALToChunk which discards everything on checksum mismatch, this is
// checksum-tolerant: on mismatch or unexpected EOF it stops reading but returns
// all valid blocks read before the corrupt entry.
func (vb *VB) readWALFileForRecovery(filename string) ([]Block, uint64, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open WAL file for recovery: %w", err)
	}
	defer f.Close()

	// Validate header (magic + version + blocksize + timestamp = 18 bytes)
	headerSize := vb.WALHeaderSize()
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, 0, fmt.Errorf("failed to read WAL header: %w", err)
	}

	if !bytes.Equal(header[:4], vb.WAL.WALMagic[:]) {
		if vb.EncryptionEnabled && bytes.Equal(header[:4], []byte{'V', 'B', 'W', 'L'}) {
			return nil, 0, fmt.Errorf("%w: WAL magic mismatch in %s (file=VBWL, runtime=VBWE)", ErrPreEncryptionFormat, filename)
		}
		return nil, 0, fmt.Errorf("WAL magic mismatch in %s: got %q, expected %q", filename, header[:4], vb.WAL.WALMagic[:])
	}
	if binary.BigEndian.Uint16(header[4:6]) != vb.Version {
		return nil, 0, fmt.Errorf("WAL version mismatch in %s", filename)
	}
	walBlockSize := binary.BigEndian.Uint32(header[6:10])
	if walBlockSize != vb.BlockSize {
		return nil, 0, fmt.Errorf("WAL blocksize mismatch in %s: WAL has %d, expected %d", filename, walBlockSize, vb.BlockSize)
	}

	var blocks []Block
	var maxSeqNum uint64

	// Encrypted WAL records are 40+BlockSize (24-byte header + ciphertext +
	// 16-byte tag); unencrypted records are 28+BlockSize (24-byte header +
	// CRC32 + plaintext). Both record sizes are fixed, so a tail tear shows
	// up as ErrUnexpectedEOF and stops replay at the last fully-written
	// record (same fail-tolerant posture as the legacy CRC path).
	if vb.EncryptionEnabled {
		recordSize := 40 + int(vb.BlockSize)
		record := make([]byte, recordSize)
		var aad [AADLen]byte
		initAAD(&aad, vb.volumeNameHash)
		for {
			if _, err := io.ReadFull(f, record); err != nil {
				// Tail tear: the last record was only partially written
				// before a crash. Same fail-tolerant posture as the
				// legacy CRC path — stop replay and return what we have.
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				// Any other I/O error means we can't be sure whether
				// the remainder is a tear or tamper; keep the WAL for
				// retry rather than dropping records on the floor.
				return nil, 0, fmt.Errorf("readWALFileForRecovery: %s: read record: %w", filename, err)
			}

			seqNum := binary.BigEndian.Uint64(record[0:8])
			blockNum := binary.BigEndian.Uint64(record[8:16])
			blockLen := binary.BigEndian.Uint64(record[16:24])

			nonce := makeNonce(seqNum, vb.VolumeUUID, DomainWAL)
			updateAAD(&aad, blockNum, seqNum)
			plain, err := vb.aead.Open(nil, nonce[:], record[24:], aad[:])
			if err != nil {
				// AEAD failure on a fully-read record is tamper, not
				// tear. Surface as ErrIntegrity so RecoverLocalWALs
				// keeps the file (no silent truncation of recovery).
				vb.logger().Error("WAL recovery: aead.Open failed", "file", filename, "block", blockNum, "seqNum", seqNum, "valid_records_so_far", len(blocks))
				return nil, 0, fmt.Errorf("readWALFileForRecovery: %w: WAL record block %d seqNum %d in %s: %w", ErrIntegrity, blockNum, seqNum, filename, err)
			}

			blocks = append(blocks, Block{
				SeqNum: seqNum,
				Block:  blockNum,
				Len:    blockLen,
				Data:   plain,
			})

			maxSeqNum = max(maxSeqNum, seqNum)
		}
		return blocks, maxSeqNum, nil
	}

	recordSize := 28 + int(vb.BlockSize)
	for {
		record := make([]byte, recordSize)
		if _, err := io.ReadFull(f, record); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				vb.logger().Warn("WAL recovery: I/O error reading record, stopping", "file", filename, "error", err, "valid_records_so_far", len(blocks))
			}
			break
		}

		// Validate CRC32 checksum
		checksum := binary.BigEndian.Uint32(record[24:28])
		checksumValidated := crc32.ChecksumIEEE(record[:24])
		checksumValidated = crc32.Update(checksumValidated, crc32.IEEETable, record[28:])

		if checksumValidated != checksum {
			vb.logger().Warn("WAL recovery: checksum mismatch, stopping read", "file", filename, "valid_records_so_far", len(blocks))
			break
		}

		seqNum := binary.BigEndian.Uint64(record[:8])

		blocks = append(blocks, Block{
			SeqNum: seqNum,
			Block:  binary.BigEndian.Uint64(record[8:16]),
			Len:    binary.BigEndian.Uint64(record[16:24]),
			Data:   append([]byte{}, record[28:]...),
		})

		maxSeqNum = max(maxSeqNum, seqNum)
	}

	return blocks, maxSeqNum, nil
}

// RecoverLocalWALs scans for orphaned WAL files left behind by a crash and replays
// valid blocks into S3 chunks via createChunkFile. This must be called between
// LoadBlockState() and OpenWAL() during boot to prevent data loss.
func (vb *VB) RecoverLocalWALs() (err error) {
	start := time.Now()
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		telemetry.RecordWALOp(context.Background(), "replay", vb.VolumeName, outcome, time.Since(start))
	}()

	walDir := filepath.Join(vb.BaseDir, vb.GetVolume(), "wal", "chunks")

	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	// Collect WAL filenames and sort ascending
	var walFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		walFiles = append(walFiles, entry.Name())
	}

	if len(walFiles) == 0 {
		return nil
	}

	sort.Strings(walFiles)

	vb.logger().Info("WAL recovery: found orphaned WAL files", "count", len(walFiles))

	// Read all valid blocks from all WAL files
	var allBlocks []Block
	var maxSeqNum uint64
	var successFiles []string // only delete files we successfully read

	for _, fname := range walFiles {
		fullPath := filepath.Join(walDir, fname)
		blocks, fileMaxSeq, err := vb.readWALFileForRecovery(fullPath)
		if err != nil {
			// An integrity failure on a recovered WAL is fail-closed: the
			// tampered or torn file may hold the only durable copy of writes
			// that never made it to a chunk, and silently skipping it would
			// surface as data loss. Abort so the caller can refuse to bring
			// the volume up; the file stays on disk for forensics + retry.
			if errors.Is(err, ErrIntegrity) {
				return fmt.Errorf("WAL recovery: integrity failure in %s: %w", fname, err)
			}
			vb.logger().Error("WAL recovery: failed to read WAL file, keeping for retry", "file", fname, "error", err)
			continue
		}
		successFiles = append(successFiles, fname)
		allBlocks = append(allBlocks, blocks...)
		maxSeqNum = max(maxSeqNum, fileMaxSeq)
	}

	if len(allBlocks) == 0 {
		vb.logger().Info("WAL recovery: no valid blocks found in orphaned WAL files, cleaning up")
		for _, fname := range successFiles {
			if err := os.Remove(filepath.Join(walDir, fname)); err != nil {
				vb.logger().Warn("failed to remove orphaned WAL file", "file", fname, "error", err)
			}
		}
		return nil
	}

	// Deduplicate: highest SeqNum wins per block number
	blocksMap := make(BlocksMapOptimised, len(allBlocks))
	for index, block := range allBlocks {
		if existing, ok := blocksMap[block.Block]; !ok || existing.SeqNum < block.SeqNum {
			blocksMap[block.Block] = BlockOptimised{
				SeqNum: block.SeqNum,
				Index:  index,
			}
		}
	}

	// Sort by block number and buffer into 4MB chunks
	sortedBlocks := make([]*Block, 0, len(blocksMap))
	for _, opt := range blocksMap {
		sortedBlocks = append(sortedBlocks, &allBlocks[opt.Index])
	}
	sort.Slice(sortedBlocks, func(i, j int) bool { return sortedBlocks[i].Block < sortedBlocks[j].Block })

	vb.logger().Info("WAL recovery: replaying blocks", "unique_blocks", len(sortedBlocks))

	// Reconcile ObjectNum past every chunk the loaded map already references
	// before replay allocates new chunk IDs. createChunkFile hands out IDs via
	// ObjectNum.Add, so a stale-low ObjectNum here would let a replayed chunk
	// reuse a live chunk ID and overwrite it, corrupting reads with a durable
	// AEAD tag failure. Mirrors the SeqNum -> maxSeqNum reconcile below.
	vb.BlocksToObject.mu.RLock()
	var maxObjectID uint64
	haveObject := false
	for _, lookup := range vb.BlocksToObject.BlockLookup {
		if !haveObject || lookup.ObjectID > maxObjectID {
			maxObjectID = lookup.ObjectID
			haveObject = true
		}
	}
	vb.BlocksToObject.mu.RUnlock()
	if haveObject {
		next := maxObjectID + 1
		for {
			current := vb.ObjectNum.Load()
			if next <= current {
				break
			}
			if vb.ObjectNum.CompareAndSwap(current, next) {
				break
			}
		}
	}

	chunkBuffer := make([]byte, 0, vb.ObjBlockSize)
	matchedBlocks := make([]Block, 0)

	for _, block := range sortedBlocks {
		chunkBuffer = append(chunkBuffer, block.Data...)
		matchedBlocks = append(matchedBlocks, Block{
			SeqNum: block.SeqNum,
			Block:  block.Block,
		})

		if len(chunkBuffer) >= int(vb.ObjBlockSize) {
			if err := vb.createChunkFile(context.Background(), 0, &chunkBuffer, &matchedBlocks); err != nil {
				return fmt.Errorf("WAL recovery: failed to create chunk file: %w", err)
			}
			chunkBuffer = chunkBuffer[:0]
			matchedBlocks = make([]Block, 0)
		}
	}

	if len(chunkBuffer) > 0 {
		if err := vb.createChunkFile(context.Background(), 0, &chunkBuffer, &matchedBlocks); err != nil {
			return fmt.Errorf("WAL recovery: failed to create chunk file: %w", err)
		}
	}

	// Sync BlockStore from BlocksToObject since createChunkFile's MarkPersisted
	// won't work during recovery (blocks aren't in Pending state in BlockStore)
	if vb.UseBlockStore && vb.BlockStore != nil {
		vb.BlocksToObject.mu.RLock()
		for _, block := range sortedBlocks {
			if lookup, ok := vb.BlocksToObject.BlockLookup[block.Block]; ok {
				vb.BlockStore.SetPersisted(block.Block, lookup.ObjectID, lookup.ObjectOffset, block.SeqNum)
			}
		}
		vb.BlocksToObject.mu.RUnlock()
	}

	// Advance SeqNum if recovered blocks have higher values
	for {
		current := vb.SeqNum.Load()
		if maxSeqNum <= current {
			break
		}
		if vb.SeqNum.CompareAndSwap(current, maxSeqNum) {
			break
		}
	}

	// Persist recovered state
	if err := vb.SaveState(); err != nil {
		return fmt.Errorf("WAL recovery: failed to save state: %w", err)
	}
	if err := vb.SaveBlockState(); err != nil {
		return fmt.Errorf("WAL recovery: failed to save block state: %w", err)
	}

	// Chunk uploads no longer refresh the live checkpoint per-chunk (that was
	// a parallel-upload hazard, see createChunkFile). Refresh it once here so
	// it reflects the just-recovered blocks: a subsequent Open always prefers
	// the live checkpoint over the numbered one just saved above, and the
	// recovered WAL files are removed below, so a stale live checkpoint here
	// would silently lose this recovery's blocks on a second crash.
	if cpErr := vb.SaveLiveCheckpoint(); cpErr != nil {
		vb.logger().Warn("WAL recovery: SaveLiveCheckpoint failed", "err", cpErr)
	}

	// Remove only successfully-read WAL files (keep failed ones for retry)
	for _, fname := range successFiles {
		if err := os.Remove(filepath.Join(walDir, fname)); err != nil {
			vb.logger().Warn("WAL recovery: failed to remove WAL file", "file", fname, "error", err)
		}
	}

	vb.logger().Info("WAL recovery: complete", "recovered_blocks", len(sortedBlocks))

	return nil
}

// checkChunkMagic preflights the first 4 bytes of a chunk file before the
// body decrypt path runs, so a pre-encryption volume opened under
// EncryptionEnabled=true fails with a dedicated, actionable
// ErrPreEncryptionFormat instead of a generic ErrIntegrity from aead.Open on
// every read. The result is memoised in vb.chunkMagicChecked: hot read paths
// (coalesced consecutive runs) hit one extra 4-byte backend Read per chunk
// per process lifetime, not per run.
//
// volumeName parameterises the cache key so snapshot-clone reads of the
// source volume's chunks (fetchBaseBlocksFromBackend, ReadFrom path) don't
// collide with the local volume's entries. read is a function so the caller
// can pass the right backend method — Backend.Read for self, Backend.ReadFrom
// for source-volume reads.
func (vb *VB) checkChunkMagic(volumeName string, objectID uint64, read func(offset, length uint32) ([]byte, error)) error {
	if !vb.EncryptionEnabled {
		return nil
	}
	key := fmt.Sprintf("%s:%d", volumeName, objectID)
	if _, ok := vb.chunkMagicChecked.Load(key); ok {
		return nil
	}
	header, err := read(0, 4)
	if err != nil {
		return fmt.Errorf("chunk magic preflight: read object %d: %w", objectID, err)
	}
	if len(header) < 4 {
		return fmt.Errorf("chunk magic preflight: short header on object %d (got %d bytes)", objectID, len(header))
	}
	preEncrypted := [4]byte{'V', 'B', 'C', 'H'}
	encrypted := [4]byte{'V', 'B', 'C', 'E'}
	if bytes.Equal(header[:4], preEncrypted[:]) {
		return fmt.Errorf("%w (volume=%q objectID=%d)", ErrPreEncryptionFormat, volumeName, objectID)
	}
	if !bytes.Equal(header[:4], encrypted[:]) {
		return fmt.Errorf("chunk magic preflight: object %d in volume %q has unknown magic %q", objectID, volumeName, header[:4])
	}
	vb.chunkMagicChecked.Store(key, struct{}{})
	return nil
}

// openChunkRun decrypts a coalesced run of N chunk blocks read from the
// backend at stride BlockSize+16. Each block's nonce is reconstructed from
// (cb.SeqNums[k], volumeUUID, DomainChunk) and AAD from (volumeNameHash,
// cb.StartBlock+k, cb.SeqNums[k]). On any AEAD-open failure we wrap
// ErrIntegrity and refuse to populate dst — fail-closed: a partial decrypt
// would let an attacker corrupt one block while leaving the rest readable.
// The dst slice must have length cb.NumBlocks*BlockSize; ciphertext must
// have length cb.NumBlocks*(BlockSize+16).
//
// volumeUUID + volumeNameHash are passed explicitly (not read off vb) so
// snapshot-clone reads can decrypt source-volume chunks under the source's
// identity, not the clone's.
func (vb *VB) openChunkRun(ciphertext []byte, cb ConsecutiveBlock, volumeUUID [4]byte, volumeNameHash [32]byte, dst []byte) error {
	bs := int(vb.BlockSize)
	sealedStride := bs + 16
	expected := int(cb.NumBlocks) * sealedStride
	if len(ciphertext) != expected {
		return fmt.Errorf("openChunkRun: short ciphertext for object %d offset %d run %d: got %d bytes, expected %d", cb.ObjectID, cb.ObjectOffset, cb.NumBlocks, len(ciphertext), expected)
	}
	if len(dst) != int(cb.NumBlocks)*bs {
		return fmt.Errorf("openChunkRun: dst length %d does not match run plaintext size %d", len(dst), int(cb.NumBlocks)*bs)
	}
	if len(cb.SeqNums) != int(cb.NumBlocks) {
		return fmt.Errorf("openChunkRun: SeqNums length %d does not match NumBlocks %d", len(cb.SeqNums), cb.NumBlocks)
	}
	var aad [AADLen]byte
	initAAD(&aad, volumeNameHash)
	for k := 0; k < int(cb.NumBlocks); k++ {
		seqNum := cb.SeqNums[k]
		blockNum := cb.StartBlock + uint64(k)
		nonce := makeNonce(seqNum, volumeUUID, DomainChunk)
		updateAAD(&aad, blockNum, seqNum)
		sealedStart := k * sealedStride
		sealedEnd := sealedStart + sealedStride
		dstStart := k * bs
		dstEnd := dstStart + bs
		if _, err := vb.aead.Open(dst[dstStart:dstStart:dstEnd], nonce[:], ciphertext[sealedStart:sealedEnd], aad[:]); err != nil {
			return fmt.Errorf("%w: chunk %d block %d (seqNum %d): %w", ErrIntegrity, cb.ObjectID, blockNum, seqNum, err)
		}
	}
	return nil
}

// LookupBlockToObject returns the persisted location of a block plus its
// chunk-write SeqNum. The SeqNum return drives nonce + AAD reconstruction on
// the decrypt path; on unencrypted volumes callers ignore it. Returns
// ErrZeroBlock when the block has never been written.
func (vb *VB) LookupBlockToObject(block uint64) (objectID uint64, objectOffset uint32, seqNum uint64, err error) {
	vb.logger().Debug("LookupBlockToObject", "block", block)

	vb.BlocksToObject.mu.RLock()

	blockLookup, ok := vb.BlocksToObject.BlockLookup[block]

	vb.BlocksToObject.mu.RUnlock()

	vb.logger().Debug("\tLOOKUP BLOCK TO OBJECT:", "block", block, "blockLookup", blockLookup)

	if ok {
		return blockLookup.ObjectID, blockLookup.ObjectOffset, blockLookup.SeqNum, nil
	} else {
		return 0, 0, 0, ErrZeroBlock
	}
}

// SaveState persists VBState to disk and to the backend.
//
// For encrypted volumes, the first call bootstraps the per-volume nonce
// subspace: if VolumeUUID is zero we mint it via crypto/rand and seed
// SeqNumHighWater = seqNumReservation. Subsequent calls preserve the existing
// UUID and advance StateSeqNum monotonically (the value is bound into the
// metadata HMAC). The local write is atomic — tmp file + fsync + rename +
// fsync parent dir — so a crash mid-persist cannot leave a torn config.json
// that would let reserveSeqNum re-hand-out values on restart.
func (vb *VB) SaveState() error {
	return vb.SaveStateCtx(context.Background())
}

// SaveStateCtx is SaveState with a caller-supplied context threaded through
// the backend state push.
func (vb *VB) SaveStateCtx(ctx context.Context) error {
	if vb.EncryptionEnabled {
		minted, err := vb.mintVolumeUUID()
		if err != nil {
			return fmt.Errorf("SaveState: %w", err)
		}
		if minted {
			vb.seqNumHighWater.Store(seqNumReservation)
		}
	}
	return vb.saveStateWithHighWater(ctx, vb.seqNumHighWater.Load())
}

// mintVolumeUUID seeds the per-volume nonce subspace via crypto/rand when it is
// still zero, returning true if it minted. VolumeUUID seeds every chunk/WAL
// AES-GCM nonce, so it must be non-zero before any block is sealed; minting it
// lazily and persisting a different value later makes read-back reconstruct a
// divergent nonce and fail tag verify. Callers persist the result durably in
// the same critical section. No-op (false) once VolumeUUID is set.
func (vb *VB) mintVolumeUUID() (bool, error) {
	var zero [4]byte
	if vb.VolumeUUID != zero {
		return false, nil
	}
	if _, err := rand.Read(vb.VolumeUUID[:]); err != nil {
		return false, fmt.Errorf("mint VolumeUUID: %w", err)
	}
	return true, nil
}

// EnsureVolumeUUID mints and durably persists the per-volume nonce subspace
// eagerly, before any block is sealed. Callers run it at Open while still
// single-threaded so a lazy mint cannot race lock-free VolumeUUID reads in the
// seal path (a torn read corrupts one block with a durable AEAD tag failure).
// Unlike SaveState, it never regresses SeqNumHighWater — clones inherit a high
// water mark, and reseeding it to seqNumReservation could re-issue SeqNums.
// No-op on unencrypted volumes or once the UUID is set.
func (vb *VB) EnsureVolumeUUID() error {
	if !vb.EncryptionEnabled {
		return nil
	}
	var zero [4]byte
	if vb.VolumeUUID != zero {
		return nil
	}
	vb.seqNumHighWaterMu.Lock()
	defer vb.seqNumHighWaterMu.Unlock()
	minted, err := vb.mintVolumeUUID()
	if err != nil {
		return fmt.Errorf("EnsureVolumeUUID: %w", err)
	}
	if !minted {
		return nil
	}
	// Keep a high water that already covers every issued SeqNum; advance in
	// whole reservation windows so it is never below the current SeqNum.
	hw := max(vb.seqNumHighWater.Load(), seqNumReservation)
	for hw < vb.SeqNum.Load() {
		hw += seqNumReservation
	}
	persisted, err := vb.persistStateLocal(hw)
	if err != nil {
		return fmt.Errorf("EnsureVolumeUUID: persistStateLocal: %w", err)
	}
	vb.seqNumHighWater.Store(hw)
	return vb.pushStateToBackend(context.Background(), persisted)
}

// saveStateWithHighWater persists VBState with an explicit SeqNumHighWater
// value rather than reading vb.seqNumHighWater. reserveSeqNum needs to durably
// commit a NEW high-water before publishing it via vb.seqNumHighWater.Store —
// publishing first and then persisting would race the lock-free fast path:
// concurrent writers observing the speculative in-memory value would issue
// SeqNums above the persisted high-water, and a crash before persist
// completion would let those values be re-issued on restart (catastrophic
// AES-GCM nonce reuse, NIST SP 800-38D §8.3).
func (vb *VB) saveStateWithHighWater(ctx context.Context, highWater uint64) error {
	persisted, err := vb.persistStateLocal(highWater)
	if err != nil {
		return err
	}
	return vb.pushStateToBackend(ctx, persisted)
}

// persistStateLocal marshals VBState, seals it for encrypted volumes, and
// fsyncs the local config.json. Returns the persisted bytes so callers can
// hand them to pushStateToBackend without re-marshaling. Crash-safety lives
// in this step: when it returns nil, the new state is durable on local disk.
func (vb *VB) persistStateLocal(highWater uint64) ([]byte, error) {
	vb.saveStateMu.Lock()
	defer vb.saveStateMu.Unlock()

	walNum := vb.WAL.WallNum.Load()
	if vb.UseShardedWAL && vb.ShardedWAL != nil {
		walNum = vb.ShardedWAL.WallNum.Load()
	}

	state := VBState{
		VolumeName:          vb.VolumeName,
		VolumeSize:          vb.VolumeSize,
		BlockSize:           vb.BlockSize,
		ObjBlockSize:        vb.ObjBlockSize,
		SeqNum:              vb.SeqNum.Load(),
		ObjectNum:           vb.ObjectNum.Load(),
		WALNum:              walNum,
		BlockToObjectWALNum: vb.BlockToObjectWAL.WallNum.Load(),
		Version:             vb.Version,
		VolumeConfig:        vb.VolumeConfig,
		ShardedWAL:          vb.UseShardedWAL,
		SnapshotID:          vb.SnapshotID,
		SourceVolumeName:    vb.SourceVolumeName,
		EncryptionEnabled:   vb.EncryptionEnabled,
		VolumeUUID:          vb.VolumeUUID,
		SeqNumHighWater:     highWater,
	}

	if vb.EncryptionEnabled {
		state.KeyFingerprint = vb.MasterKey.Fingerprint
	}

	state.StateSeqNum = vb.nextStateSeqNum.Add(1)

	jsonData, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}

	// Encrypted volumes append a 16-byte AES-GCM tag binding the JSON bytes to
	// (volumeNameHash, "vbstate", StateSeqNum). An attacker swapping volume
	// A's config.json into volume B's prefix is rejected at LoadStateRequest
	// because the AAD reconstruction uses vb.volumeNameHash (our own
	// identity), which differs from the seal-time hash for volume A.
	persisted := jsonData
	if vb.EncryptionEnabled {
		nonce := makeNonce(state.StateSeqNum, vb.VolumeUUID, DomainVBStateMeta)
		aad := makeMetaAAD(vb.volumeNameHash, "vbstate", state.StateSeqNum)
		persisted = sealMeta(vb.aead, jsonData, aad, nonce)
	}

	filename := fmt.Sprintf("%s/%s", vb.BaseDir, types.GetFilePath(types.FileTypeConfig, 0, vb.GetVolume()))
	if err := writeFileAtomic(filename, persisted, 0600); err != nil {
		return nil, fmt.Errorf("SaveState: atomic write %s: %w", filename, err)
	}

	return persisted, nil
}

// pushStateToBackend writes pre-marshaled VBState bytes to the backend (S3
// PUT for the S3 backend). Safe to call without seqNumHighWaterMu held —
// LoadState reconciles local vs backend by max(SeqNum), so a stale or
// in-flight backend write is non-fatal as long as the local fsync from
// persistStateLocal succeeded.
func (vb *VB) pushStateToBackend(ctx context.Context, persisted []byte) error {
	headers := []byte{}
	return vb.Backend.WriteCtx(ctx, types.FileTypeConfig, 0, &headers, &persisted)
}

// writeFileAtomic writes data to path atomically via tmp + fsync + rename +
// fsync(parent). On return without error, the file exists at path with the
// new contents fully durable on disk. A crash before return may leave a
// stale path.tmp behind (caller-tolerable: next SaveState rewrites it).
//
// The parent dir is created if absent so a remount on a node that never held
// the volume locally (LoadState pulls authoritative state from the backend,
// then persists it locally) does not fail opening the tmp file with ENOENT.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return fsyncDir(dir)
}

// fsyncDir fsyncs a directory entry so a preceding rename is durable. POSIX:
// without this, the rename may be lost on crash even if the file content was
// fsynced (the directory entry change is buffered separately).
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

// Load the block tracking state from disk.
func (vb *VB) LoadState() error {
	return vb.LoadStateCtx(context.Background())
}

// LoadStateCtx is LoadState with a caller-supplied context threaded through
// the backend state fetch.
func (vb *VB) LoadStateCtx(ctx context.Context) error {
	// Step 1. Query the state locally
	localPath := fmt.Sprintf("%s/%s", vb.BaseDir, types.GetFilePath(types.FileTypeConfig, 0, vb.GetVolume()))
	state, localErr := vb.LoadStateRequestCtx(ctx, localPath)
	if errors.Is(localErr, ErrIntegrity) || errors.Is(localErr, ErrEncryptionMismatch) {
		return fmt.Errorf("LoadState: local %s: %w", localPath, localErr)
	}
	if localErr != nil {
		vb.logger().InfoContext(ctx, "No state found in local file, using backend state", "error", localErr)
	}

	// Step 2. Query the state from the backend
	stateBackend, backendErr := vb.LoadStateRequestCtx(ctx, "")

	if errors.Is(backendErr, ErrIntegrity) || errors.Is(backendErr, ErrEncryptionMismatch) {
		return fmt.Errorf("LoadState: backend: %w", backendErr)
	}
	if backendErr != nil {
		vb.logger().WarnContext(ctx, "Failed to load state from backend", "error", backendErr)
	}

	if stateBackend.BlockSize == 0 && state.BlockSize == 0 {
		classified := classifyStateLoad(localErr, backendErr)
		if errors.Is(classified, ErrStateBackendUnavailable) {
			vb.logger().WarnContext(ctx, "LoadState: backend unavailable, retry recommended",
				"volume", vb.GetVolume(), "err", backendErr)
		} else {
			vb.logger().DebugContext(ctx, "LoadState: no state in local or backend",
				"volume", vb.GetVolume())
		}
		return classified
	}

	// Step 3. Compare the two states and select the authoritative copy.
	// StateSeqNum is the SaveState generation counter — bumped on every
	// persist whether or not data writes occurred — so it is the
	// strictly-monotonic tiebreak the rollback defense rests on (an attacker
	// who rolls the backend back to v1 loses to local v2 because v2's
	// StateSeqNum is strictly higher). When local failed to load (e.g. no
	// local file for a newly created volume), always prefer the backend
	// state regardless.
	if localErr != nil || stateBackend.StateSeqNum > state.StateSeqNum {
		state = stateBackend
	}

	// Step 4. Validate encryption invariants against the selected state
	// BEFORE mutating any vb.* field. A wrong-key open, a flag XOR mismatch,
	// or a key-fingerprint mismatch must leave the VB untouched so callers
	// that log the error and continue cannot operate on partially-loaded
	// state derived from the rejected blob.
	if vb.EncryptionEnabled != state.EncryptionEnabled {
		return fmt.Errorf("%w: runtime EncryptionEnabled=%v, persisted=%v (volume %s)",
			ErrEncryptionMismatch, vb.EncryptionEnabled, state.EncryptionEnabled, vb.VolumeName)
	}
	if vb.EncryptionEnabled {
		if vb.MasterKey == nil {
			return fmt.Errorf("%w: volume %s requires master key", ErrEncryptionMismatch, vb.VolumeName)
		}
		if state.KeyFingerprint != vb.MasterKey.Fingerprint {
			return fmt.Errorf("%w: volume %s sealed under key %s, supplied key is %s",
				ErrEncryptionMismatch, vb.VolumeName, state.KeyFingerprint, vb.MasterKey.Fingerprint)
		}
	}

	// Reconcile VolumeSize with VolumeConfig.SizeGiB (safety net for resize).
	// After ModifyVolume updates SizeGiB, VBState.VolumeSize may be stale.
	// The VolumeConfig's SizeGiB (set by ModifyVolume) takes precedence.
	configSizeBytes := state.VolumeConfig.VolumeMetadata.SizeGiB * 1024 * 1024 * 1024
	if configSizeBytes > 0 && configSizeBytes > state.VolumeSize {
		vb.logger().Info("LoadState: reconciling VolumeSize from VolumeConfig.SizeGiB",
			"oldSize", state.VolumeSize, "newSize", configSizeBytes,
			"sizeGiB", state.VolumeConfig.VolumeMetadata.SizeGiB)
		state.VolumeSize = configSizeBytes
	} else if configSizeBytes > 0 && configSizeBytes < state.VolumeSize {
		vb.logger().Warn("LoadState: VolumeConfig.SizeGiB < VBState.VolumeSize (shrink not supported, keeping current size)",
			"configSize", configSizeBytes, "stateSize", state.VolumeSize)
	}

	vb.VolumeName = state.VolumeName
	vb.VolumeSize = state.VolumeSize
	vb.BlockSize = state.BlockSize
	vb.ObjBlockSize = state.ObjBlockSize
	vb.SeqNum.Store(state.SeqNum)
	vb.ObjectNum.Store(state.ObjectNum)
	vb.WAL.WallNum.Store(state.WALNum)
	vb.BlockToObjectWAL.WallNum.Store(state.BlockToObjectWALNum)

	// Restore sharded WAL state
	if state.ShardedWAL {
		vb.UseShardedWAL = true
		if vb.ShardedWAL == nil {
			vb.ShardedWAL = NewShardedWAL(vb.BaseDir, vb.WAL.WALMagic)
		}
		vb.ShardedWAL.WallNum.Store(state.WALNum)
	}

	vb.Version = state.Version
	vb.VolumeConfig = state.VolumeConfig

	vb.nextStateSeqNum.Store(state.StateSeqNum)

	// If this volume was created from a snapshot, restore the base block map
	// BEFORE the encrypted SeqNum bootstrap below. OpenFromSnapshot sets
	// SnapshotID/SourceVolumeName, and the encrypted bumpSeqNumHighWater
	// durably persists VBState. Restoring the snapshot link after that persist
	// would write a config.json with an empty SnapshotID, so the next open
	// loads no base map and serves an all-zeros disk. OpenFromSnapshot verifies
	// the snapshot under the source identity, independent of this volume's
	// VolumeUUID, so it is safe to run first.
	if state.SnapshotID != "" {
		if err := vb.OpenFromSnapshot(state.SnapshotID); err != nil {
			vb.logger().Error("Failed to load snapshot base map", "snapshotID", state.SnapshotID, "error", err)
			return fmt.Errorf("failed to load snapshot %s: %w", state.SnapshotID, err)
		}
	}

	// Encryption SeqNum bootstrap. On a successful Open of an existing
	// encrypted volume, restart SeqNum at the persisted high-water and
	// reserve the next window durably before any data write can hand out a
	// value. This is what makes nonce uniqueness crash-safe: a kill -9
	// before the next SaveState loses up to one reservation window of
	// values, but every value handed out before the crash sits below the
	// high-water we just persisted, so none can be re-issued.
	if vb.EncryptionEnabled {
		vb.VolumeUUID = state.VolumeUUID
		vb.SeqNum.Store(state.SeqNumHighWater)
		vb.seqNumHighWater.Store(state.SeqNumHighWater)
		if err := vb.bumpSeqNumHighWater(ctx); err != nil {
			return fmt.Errorf("LoadState: reserve initial SeqNum window: %w", err)
		}
	}

	vb.logger().DebugContext(ctx, "Loaded state", "state", state)

	return nil
}

// reserveSeqNum hands out n consecutive sequence numbers for the data path.
// Common path is lock-free atomic.Add; the high-water is checked after the
// fact and only when crossed do we take seqNumHighWaterMu and SaveState to
// advance it. Refuses past MaxSeqNum (56-bit nonce slot).
//
// Lock order: the slow path takes seqNumHighWaterMu then calls
// persistStateLocal, which acquires saveStateMu. saveStateMu must not be
// held when calling reserveSeqNum.
//
// Unencrypted volumes fall through to plain atomic.Add — the high-water is a
// nonce-uniqueness mechanism and is irrelevant when no nonce exists.
func (vb *VB) reserveSeqNum(ctx context.Context, n uint64) (start uint64, err error) {
	end := vb.SeqNum.Add(n)
	start = end - n
	if !vb.EncryptionEnabled {
		return start, nil
	}
	if end > MaxSeqNum {
		return 0, fmt.Errorf("viperblock: SeqNum %d exceeds 56-bit nonce limit, volume %s must be recreated", end, vb.VolumeName)
	}
	if end <= vb.seqNumHighWater.Load() {
		return start, nil
	}
	vb.seqNumHighWaterMu.Lock()
	// Re-check under the mutex: another caller may have advanced past us.
	hw := vb.seqNumHighWater.Load()
	if end <= hw {
		vb.seqNumHighWaterMu.Unlock()
		return start, nil
	}
	// Mint the per-volume nonce subspace before the first durable persist or
	// seal. The first encrypted write always reaches here (seqNumHighWater
	// starts at zero), so minting under this mutex guarantees VolumeUUID is
	// non-zero and persisted by persistStateLocal below before any block is
	// sealed under it. Without this, freshly-seeded volumes (EFI varstore, raw
	// imports) seal chunk 0 under the zero UUID while a later SaveState mints a
	// random one, breaking read-back with a tag-verify failure.
	if _, err := vb.mintVolumeUUID(); err != nil {
		vb.seqNumHighWaterMu.Unlock()
		return 0, fmt.Errorf("reserveSeqNum: %w", err)
	}
	for end > hw {
		hw += seqNumReservation
	}
	// Persist locally BEFORE publishing the new hw to vb.seqNumHighWater.
	// While we hold the mutex, concurrent fast-path callers checking
	// vb.seqNumHighWater see the OLD value: below it they proceed safely
	// within the already-persisted range; above it they take the slow path
	// and block here. Either way, no SeqNum above the durable hw is issued.
	// If persistStateLocal fails, hw is never published — recovery is just
	// "retry on next reserve."
	persisted, err := vb.persistStateLocal(hw)
	if err != nil {
		vb.seqNumHighWaterMu.Unlock()
		return 0, fmt.Errorf("reserveSeqNum: persistStateLocal: %w", err)
	}
	vb.seqNumHighWater.Store(hw)
	vb.seqNumHighWaterMu.Unlock()

	// Backend push happens outside the mutex — the S3 PUT can be tens to
	// hundreds of ms and would otherwise gate every reservation-window
	// crossing. Crash-safety lives in the local fsync above; LoadState picks
	// max(local, backend) SeqNum so a lagged or failed backend write is
	// recoverable on next SaveState.
	if perr := vb.pushStateToBackend(ctx, persisted); perr != nil {
		vb.logger().WarnContext(ctx, "reserveSeqNum: backend state push failed; local fsync is durable",
			"volume", vb.VolumeName, "highWater", hw, "err", perr)
	}
	return start, nil
}

// bumpSeqNumHighWater advances the persisted SeqNumHighWater by
// seqNumReservation and durably SaveStates. Called from LoadState's startup
// path to claim a fresh reservation window before any data write can issue a
// SeqNum that overlaps the pre-crash range.
func (vb *VB) bumpSeqNumHighWater(ctx context.Context) error {
	vb.seqNumHighWaterMu.Lock()
	newHW := vb.seqNumHighWater.Load() + seqNumReservation
	persisted, err := vb.persistStateLocal(newHW)
	if err != nil {
		vb.seqNumHighWaterMu.Unlock()
		return err
	}
	vb.seqNumHighWater.Store(newHW)
	vb.seqNumHighWaterMu.Unlock()

	if perr := vb.pushStateToBackend(ctx, persisted); perr != nil {
		vb.logger().WarnContext(ctx, "bumpSeqNumHighWater: backend state push failed; local fsync is durable",
			"volume", vb.VolumeName, "highWater", newHW, "err", perr)
	}
	return nil
}

// Query the local state from file or the backend.
func (vb *VB) LoadStateRequest(filename string) (state VBState, err error) {
	return vb.LoadStateRequestCtx(context.Background(), filename)
}

// LoadStateRequestCtx is LoadStateRequest with a caller-supplied context
// threaded through the backend fetch.
func (vb *VB) LoadStateRequestCtx(ctx context.Context, filename string) (state VBState, err error) {
	var jsonData []byte

	// Read from file
	if filename != "" {
		jsonData, err = os.ReadFile(filename)
		if err != nil {
			return state, err
		}
	} else {
		jsonData, err = vb.Backend.ReadCtx(ctx, types.FileTypeConfig, 0, 0, 0)
		if err != nil {
			return state, err
		}
	}

	// Encrypted volumes wrap the JSON in a metaEnvelope whose authtag binds
	// the payload to (volumeNameHash, "vbstate", StateSeqNum). The nonce +
	// structured AAD reconstruction needs StateSeqNum + VolumeUUID, both of
	// which live in the payload — split the envelope, extract them via a
	// minimal peek struct over the verbatim payload, then verify before
	// unmarshalling the full state. The peek's VolumeUUID is the nonce
	// subspace identifier (binds the seal-time nonce); vb.volumeNameHash is
	// the trusted volume identity (caller-supplied at New, untouched by the
	// parsed JSON) — splicing in another volume's blob is rejected because
	// the AAD's volumeNameHash differs. KeyFingerprint is checked pre-verify
	// so a wrong-key open surfaces as ErrEncryptionMismatch (with both
	// fingerprints in the error) rather than the generic "tag verify failed".
	if vb.EncryptionEnabled {
		payload, tag, splitErr := splitEnvelope(jsonData)
		if splitErr != nil {
			return state, fmt.Errorf("%w: VBState envelope: %w", ErrIntegrity, splitErr)
		}
		var peek struct {
			VolumeUUID     [4]byte `json:"VolumeUUID"`
			StateSeqNum    uint64  `json:"StateSeqNum"`
			KeyFingerprint string  `json:"KeyFingerprint"`
		}
		if err := json.Unmarshal(payload, &peek); err != nil {
			return state, fmt.Errorf("%w: VBState peek parse: %w", ErrIntegrity, err)
		}
		if peek.KeyFingerprint != vb.MasterKey.Fingerprint {
			return state, fmt.Errorf("%w: volume %s sealed under key %s, supplied key is %s",
				ErrEncryptionMismatch, vb.VolumeName, peek.KeyFingerprint, vb.MasterKey.Fingerprint)
		}
		nonce := makeNonce(peek.StateSeqNum, peek.VolumeUUID, DomainVBStateMeta)
		aad := makeMetaAAD(vb.volumeNameHash, "vbstate", peek.StateSeqNum)
		if err := verifyMeta(vb.aead, payload, tag, aad, nonce); err != nil {
			return state, fmt.Errorf("%w: VBState tag verify: %w", ErrIntegrity, err)
		}
		jsonData = payload
	}

	// StateBody strips the envelope so a plain-mode runtime reading an
	// encrypted blob still decodes the inner payload (which carries
	// EncryptionEnabled=true) — LoadState's flag-mismatch check then surfaces
	// the misconfig as ErrEncryptionMismatch rather than silently decoding the
	// envelope wrapper into a zero-valued state. For the encrypted path
	// jsonData is already the verified payload, so StateBody is a no-op.
	err = json.NewDecoder(bytes.NewReader(StateBody(jsonData))).Decode(&state)

	return state, err
}

// Private function to read a block from the storage backend, use ReadAt for public access.
func (vb *VB) read(ctx context.Context, block uint64, blockLen uint64) (data []byte, err error) {
	// Use optimized BlockStore path if enabled
	if vb.UseBlockStore {
		return vb.readBlockStore(ctx, block, blockLen)
	}

	// Check blockLen a multiple of a blocksize
	if blockLen%uint64(vb.BlockSize) != 0 {
		return nil, ErrRequestBlockSize
	}

	var zeroBlockErr error
	data = make([]byte, blockLen)

	// Preprocess latest writes - pre-allocate map to avoid rehashing
	vb.Writes.mu.RLock()
	writesLen := len(vb.Writes.Blocks)
	writesCopy := make([]Block, writesLen)
	copy(writesCopy, vb.Writes.Blocks)
	vb.Writes.mu.RUnlock()

	latestWrites := make(BlocksMap, writesLen) // Pre-allocate with capacity
	for _, wr := range writesCopy {
		if prev, ok := latestWrites[wr.Block]; !ok || wr.SeqNum > prev.SeqNum {
			latestWrites[wr.Block] = wr
		}
	}

	// Preprocess pending writes (after WAL write to backend upload success/completion)
	vb.PendingBackendWrites.mu.RLock()
	pendingLen := len(vb.PendingBackendWrites.Blocks)
	pendingWritesCopy := make([]Block, pendingLen)
	copy(pendingWritesCopy, vb.PendingBackendWrites.Blocks)
	vb.PendingBackendWrites.mu.RUnlock()

	latestPendingWrites := make(BlocksMap, pendingLen) // Pre-allocate with capacity
	for _, wr := range pendingWritesCopy {
		latestPendingWrites[wr.Block] = wr
	}

	blockRequests := blockLen / uint64(vb.BlockSize)

	var consecutiveBlocks ConsecutiveBlocks
	var baseConsecutiveBlocks ConsecutiveBlocks
	ancestorConsBlocks := make([]ConsecutiveBlocks, len(vb.ancestors))

	for i := range blockRequests {
		currentBlock := block + i
		start := i * uint64(vb.BlockSize)
		end := start + uint64(vb.BlockSize)

		// If matched in our HOT writes, copy the data
		if wr, ok := latestWrites[currentBlock]; ok {
			//vb.logger().Info("[READ] HOT BLOCK:", "block", wr.Block, "seqnum", wr.SeqNum)

			copy(data[start:end], bytes.Clone(wr.Data))
			continue
		}

		// Next, check the pending backend writes buffer
		if lp, ok := latestPendingWrites[currentBlock]; ok {
			//vb.logger().Info("[READ] PENDING BLOCK:", "block", lp.Block, "seqnum", lp.SeqNum)

			copy(data[start:end], bytes.Clone(lp.Data))
			continue
		}

		// Next query the LRU cache if the data does not exist in the HOT write path, or pending write buffer.
		if vb.Cache.Config.Size > 0 {
			if cachedData, ok := vb.Cache.lru.Get(currentBlock); ok {
				//vb.logger().Info("[READ] LRU CACHE BLOCK:", "block", currentBlock)

				copy(data[start:end], cachedData)
				continue
			}
		}

		// Next, fetch which object and offset the block is within
		objectID, objectOffset, seqNum, err := vb.LookupBlockToObject(currentBlock)
		if err != nil {
			if !errors.Is(err, ErrZeroBlock) {
				return nil, fmt.Errorf("LookupBlockToObject block %d: %w", currentBlock, err)
			}
			// Block not in our own map -- check snapshot base map
			if vb.BaseBlockMap != nil {
				baseObjectID, baseObjectOffset, baseSeqNum, baseErr := vb.LookupBaseBlockToObject(currentBlock)
				if baseErr == nil {
					baseConsecutiveBlocks = append(baseConsecutiveBlocks, ConsecutiveBlock{
						BlockPosition: i,
						StartBlock:    currentBlock,
						NumBlocks:     1,
						OffsetStart:   start,
						OffsetEnd:     end,
						ObjectID:      baseObjectID,
						ObjectOffset:  baseObjectOffset,
						SeqNum:        baseSeqNum,
					})
					continue
				}
			}
			// Base map miss — check ancestor layers
			ancFound := false
			for ai := range vb.ancestors {
				vb.ancestors[ai].blocks.mu.RLock()
				lookup, ok := vb.ancestors[ai].blocks.BlockLookup[currentBlock]
				vb.ancestors[ai].blocks.mu.RUnlock()
				if ok {
					ancestorConsBlocks[ai] = append(ancestorConsBlocks[ai], ConsecutiveBlock{
						BlockPosition: i,
						StartBlock:    currentBlock,
						NumBlocks:     1,
						OffsetStart:   start,
						OffsetEnd:     end,
						ObjectID:      lookup.ObjectID,
						ObjectOffset:  lookup.ObjectOffset,
						SeqNum:        lookup.SeqNum,
					})
					ancFound = true
					break
				}
			}
			if ancFound {
				continue
			}
			zeroBlockErr = ErrZeroBlock
			vb.logger().Debug("[READ] ZERO BLOCK:", "block", currentBlock)

			copy(data[start:end], make([]byte, vb.BlockSize)) // zero
			continue
		}

		vb.logger().Debug("[READ] OBJECT ID:", "objectID", objectID, "objectOffset", objectOffset)

		consecutiveBlocks = append(consecutiveBlocks, ConsecutiveBlock{
			BlockPosition: i,
			StartBlock:    currentBlock,
			NumBlocks:     1,
			OffsetStart:   start,
			OffsetEnd:     end,
			ObjectID:      objectID,
			ObjectOffset:  objectOffset,
			SeqNum:        seqNum,
		})
	}

	// Loop through all consecutive blocks that are required to fetch from the backend
	var consecutiveBlocksToRead ConsecutiveBlocks

	// Store which consecutive blocks we have already read - pre-allocate
	consecutiveBlocksRead := make(map[uint64]bool, len(consecutiveBlocks))

	for i := 0; i < len(consecutiveBlocks); i++ {
		vb.logger().Debug("[READ] CONSECUTIVE BLOCK:", "startBlock", consecutiveBlocks[i].StartBlock, "numBlocks", consecutiveBlocks[i].NumBlocks, "offsetStart", consecutiveBlocks[i].OffsetStart, "offsetEnd", consecutiveBlocks[i].OffsetEnd, "objectID", consecutiveBlocks[i].ObjectID, "objectOffset", consecutiveBlocks[i].ObjectOffset)

		// Skip if this blocks belongs to a previous consecutive block
		if _, ok := consecutiveBlocksRead[consecutiveBlocks[i].StartBlock]; ok {
			vb.logger().Debug("[READ] SKIPPING CONSECUTIVE BLOCK READ:", "startBlock", consecutiveBlocks[i].StartBlock)
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

		// Carry per-block SeqNums for the run so the encrypted decrypt loop
		// below can reconstruct each block's nonce + AAD. Unused on
		// unencrypted reads.
		var seqNums []uint64
		if vb.EncryptionEnabled {
			seqNums = make([]uint64, numBlocks)
			for k := 0; k < numBlocks; k++ {
				seqNums[k] = consecutiveBlocks[i+k].SeqNum
			}
		}

		consecutiveBlocksToRead = append(consecutiveBlocksToRead, ConsecutiveBlock{
			BlockPosition: consecutiveBlocks[i].BlockPosition,
			StartBlock:    consecutiveBlocks[i].StartBlock,
			NumBlocks:     utils.SafeIntToUint16(numBlocks),
			OffsetStart:   consecutiveBlocks[i].OffsetStart,
			OffsetEnd:     consecutiveBlocks[i].OffsetEnd,
			ObjectID:      consecutiveBlocks[i].ObjectID,
			ObjectOffset:  consecutiveBlocks[i].ObjectOffset,
			SeqNums:       seqNums,
		})
	}

	// Per-block on-disk stride: encrypted chunks add a 16-byte GCM tag after
	// each ciphertext block; unencrypted chunks stay at BlockSize.
	stride := vb.BlockSize
	if vb.EncryptionEnabled {
		stride += 16
	}

	// Next, read our consecutive blocks from the backend
	for _, cb := range consecutiveBlocksToRead {
		vb.logger().Debug("[READ] READING CONSECUTIVE BLOCK:", "startBlock", cb.StartBlock, "numBlocks", cb.NumBlocks, "offsetStart", cb.OffsetStart, "offsetEnd", cb.OffsetEnd, "objectID", cb.ObjectID, "objectOffset", cb.ObjectOffset)

		consecutiveBlockOffset := uint32(cb.NumBlocks) * stride

		start := cb.BlockPosition * uint64(vb.BlockSize)
		end := start + uint64(cb.NumBlocks)*uint64(vb.BlockSize)

		objectID := cb.ObjectID
		if err := vb.checkChunkMagic(vb.VolumeName, objectID, func(off, length uint32) ([]byte, error) {
			return vb.Backend.ReadCtx(ctx, types.FileTypeChunk, objectID, off, length)
		}); err != nil {
			return nil, err
		}

		blockData, err := vb.Backend.ReadCtx(ctx, types.FileTypeChunk, cb.ObjectID, cb.ObjectOffset, consecutiveBlockOffset)
		if err != nil {
			return nil, err
		}

		vb.logger().DebugContext(ctx, "[READ] COPYING BLOCK DATA:", "start", start, "end", end)
		vb.logger().DebugContext(ctx, "[READ] DATA:", "data len", len(data))

		if vb.EncryptionEnabled {
			if err := vb.openChunkRun(blockData, cb, vb.VolumeUUID, vb.volumeNameHash, data[start:end]); err != nil {
				return nil, err
			}
		} else {
			copy(data[start:end], blockData)
		}

		// Update the cache with the read data
		if vb.Cache.Config.Size > 0 {
			for i := uint64(0); i < uint64(cb.NumBlocks); i++ {
				currentBlock := cb.StartBlock + i
				vb.Cache.lru.Add(currentBlock, bytes.Clone(data[start+i*uint64(vb.BlockSize):start+(i+1)*uint64(vb.BlockSize)]))
			}
		}
	}

	// Fetch blocks from the source volume's backend (snapshot fallback)
	if len(baseConsecutiveBlocks) > 0 {
		err := vb.fetchBaseBlocksFromBackend(ctx, vb.SourceVolumeName, vb.SourceVolumeUUID, vb.sourceVolumeNameHash, baseConsecutiveBlocks, data)
		if err != nil {
			return nil, err
		}
	}

	// Fetch blocks from ancestor snapshot layers
	for ai, anc := range vb.ancestors {
		if len(ancestorConsBlocks[ai]) == 0 {
			continue
		}
		if err := vb.fetchBaseBlocksFromBackend(ctx, anc.sourceVolumeName, anc.sourceVolumeUUID, anc.sourceVolumeNameHash, ancestorConsBlocks[ai], data); err != nil {
			return nil, err
		}
	}

	return data, zeroBlockErr
}

func (vb *VB) ReadAt(offset uint64, length uint64) ([]byte, error) {
	return vb.ReadAtCtx(context.Background(), offset, length)
}

// ReadAtCtx is ReadAt with a caller-supplied context that flows through
// backend chunk fetches for trace propagation.
func (vb *VB) ReadAtCtx(ctx context.Context, offset uint64, length uint64) ([]byte, error) {
	// First check the block exists in our volume size
	if offset > vb.GetVolumeSize() {
		return nil, ErrRequestTooLarge
	}

	if offset+length > vb.GetVolumeSize() {
		return nil, ErrRequestOutOfRange
	}

	blockSize := uint64(vb.BlockSize)

	// Calculate first and last block numbers
	firstBlock := offset / blockSize
	lastBlock := (offset + length - 1) / blockSize
	blockCount := lastBlock - firstBlock + 1

	// Read entire range of needed blocks
	fullData, err := vb.read(ctx, firstBlock, blockCount*blockSize)

	if err != nil && !errors.Is(err, ErrZeroBlock) {
		return nil, err
	}

	// Compute offset within the first block
	innerOffset := offset % blockSize
	return fullData[innerOffset : innerOffset+length], err
}

func (vb *VB) Close() error {
	vb.logger().Info("VB Close, flushing block state to disk")

	// Stop background goroutines before flushing
	vb.StopChunkUploader()
	vb.StopWALSyncer()

	if err := vb.Flush(); err != nil {
		vb.logger().Error("failed to flush during Close", "error", err)
	}

	var walErr error
	if vb.UseShardedWAL {
		walErr = vb.WriteShardedWALToChunk(true)
	} else {
		walErr = vb.WriteWALToChunk(true)
	}
	if walErr != nil {
		vb.logger().Error("Could not Write WAL to Chunk during Close, proceeding to save block state", "err", walErr)
	} else {
		// Chunk uploads no longer refresh the live checkpoint per-chunk (that
		// was a parallel-upload hazard, see createChunkFile). Refresh it once
		// here so a subsequent Open's LoadLiveCheckpoint (which is always
		// preferred over the numbered checkpoint saved below) reflects the
		// chunks just uploaded during this Close. Non-fatal: SaveBlockState
		// below still persists the same map to the numbered checkpoint.
		if cpErr := vb.SaveLiveCheckpoint(); cpErr != nil {
			vb.logger().Warn("Close: SaveLiveCheckpoint failed", "err", cpErr)
		} else {
			// One last sweep on the way out, now that the live checkpoint
			// durably excludes every zero-refcount chunk below. Run before
			// SaveBlockState/SaveState so ensureGCFloor still sees the
			// numbered checkpoint from before this Close, not the one
			// SaveBlockState is about to write under a bumped WallNum.
			vb.sweepChunks(context.Background())
		}
	}

	path := fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, vb.GetVolume())

	vb.logger().Debug("Saving Close state to", "path", path)

	var firstErr error

	// Upload the state to the backend
	if err := vb.SaveState(); err != nil {
		vb.logger().Error("Could not save state", "err", err)
		firstErr = err
	}

	// Always attempt to save the block checkpoint even if SaveState or WAL
	// flush failed — in-memory BlockLookup may reflect successfully flushed
	// writes from earlier Flush calls and must not be lost.
	if err := vb.SaveBlockState(); err != nil {
		vb.logger().Error("Could not save block state", "err", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return firstErr
	}

	// Remove local WAL and block state files, upload/sync in prior steps.
	err := vb.RemoveLocalFiles()
	if err != nil {
		vb.logger().Error("Failed to remove local files", "err", err)
	}

	if walErr != nil {
		return walErr
	}
	return nil
}

// Remove local WAL and block state files, connection must be closed first.
func (vb *VB) RemoveLocalFiles() (err error) {
	localPath := filepath.Join(vb.BaseDir, vb.GetVolume())

	vb.logger().Info("Removing local files", "path", localPath)

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

// ownsWAL returns true if this VB instance has open WAL files.
// A VB that never called OpenWAL/OpenShardedWAL (e.g. viperblockd's snapshot
// VB) does not own the WAL and must not flush or consolidate.
func (vb *VB) ownsWAL() bool {
	if vb.UseShardedWAL && vb.ShardedWAL != nil {
		for i := range NumShards {
			vb.ShardedWAL.Shards[i].mu.RLock()
			open := vb.ShardedWAL.Shards[i].DB != nil
			vb.ShardedWAL.Shards[i].mu.RUnlock()
			if open {
				return true
			}
		}
		return false
	}
	return len(vb.WAL.DB) > 0
}

func (vb *VB) Reset() error {
	// Reset re-issues ObjectID 0 below (vb.ObjectNum.Store(0)), which
	// violates the "chunk IDs are never reused" invariant chunk GC's
	// refcount/watermark/floor reasoning all depend on -- a reused ID could
	// alias a chunk GC already deleted under the pre-reset generation.
	// Latch GC off permanently for this instance rather than trying to
	// reconcile gcRefcount/gcFloor/gcSnapshotSafe across the reset; nothing
	// in this codebase calls Reset in production today, so this is
	// defense-in-depth for whatever eventually does.
	if vb.GCEnabled {
		vb.gcLatchedOff.Store(true)
	}

	vb.BlocksToObject.mu.Lock()
	vb.BlocksToObject.BlockLookup = make(map[uint64]BlockLookup, 0)
	vb.BlocksToObject.mu.Unlock()

	vb.Writes.mu.Lock()
	vb.Writes.Blocks = make([]Block, 0)
	vb.Writes.mu.Unlock()

	vb.PendingBackendWrites.mu.Lock()
	vb.PendingBackendWrites.Blocks = make([]Block, 0)
	vb.PendingBackendWrites.mu.Unlock()

	vb.pendingBytes.Store(0)

	if vb.Cache.lru != nil {
		vb.Cache.lru.Purge()
	}

	// Reset BlockStore if enabled
	if vb.BlockStore != nil {
		vb.BlockStore.Clear()
		vb.BlockStore.SetSeqNum(0)
	}

	// Reset SeqNum and ObjectNum
	vb.SeqNum.Store(0)
	vb.ObjectNum.Store(0)

	// Reset WAL
	vb.WAL.mu.Lock()
	vb.WAL.WallNum.Store(0)
	vb.WAL.mu.Unlock()

	// Reset ShardedWAL if enabled
	if vb.ShardedWAL != nil {
		for i := range NumShards {
			shard := vb.ShardedWAL.Shards[i]
			shard.mu.Lock()
			if shard.DB != nil {
				if err := shard.DB.Close(); err != nil {
					vb.logger().Warn("failed to close shard during reset", "shard", i, "error", err)
				}
				shard.DB = nil
			}
			shard.dirty.Store(false)
			shard.mu.Unlock()
		}
		vb.ShardedWAL.WallNum.Store(0)
	}

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
	binary.BigEndian.PutUint64(header[10:18], utils.SafeInt64ToUint64(time.Now().Unix()))
	return header
}

// WALHeaderSize returns the size of the WAL header in bytes.
func (vb *VB) WALHeaderSize() int {
	// Magic bytes (4) + Version (2) + BlockSize (4) + Timestamp (8)
	return len(vb.WAL.WALMagic) + binary.Size(vb.Version) + binary.Size(vb.BlockSize) + binary.Size(time.Now().Unix())
}

func (vb *VB) BlockToObjectWALHeader() []byte {
	header := make([]byte, vb.BlockToObjectWALHeaderSize())

	vb.logger().Debug("Writing BlockToObjectWALHeader", "header", header, "size", vb.BlockToObjectWALHeaderSize())
	copy(header[:len(vb.BlockToObjectWAL.WALMagic)], vb.BlockToObjectWAL.WALMagic[:])
	binary.BigEndian.PutUint16(header[4:6], vb.Version)
	binary.BigEndian.PutUint64(header[6:14], utils.SafeInt64ToUint64(time.Now().Unix()))
	return header
}

func (vb *VB) BlockToObjectWALHeaderSize() int {
	// Magic bytes (4) + Version (2) + Timestamp (8)
	return blockCheckpointHeaderSize
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

func GenerateVolumeID(voltype, name, bucket string, timestamp int64) string {
	// Combine the fields
	input := fmt.Sprintf("%-s-%s-%s-%d", voltype, name, bucket, timestamp)

	// Create SHA-256 hash
	hash := sha256.Sum256([]byte(input))

	// Convert first 17 characters of hex
	shortHash := hex.EncodeToString(hash[:])[:17]

	return fmt.Sprintf("%s-%s", voltype, shortHash)
}

// FindFreePort allocates a free TCP port from the OS.
func FindFreePort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

// EnableBlockStore enables the unified block store for O(1) lookups.
func (vb *VB) EnableBlockStore() {
	vb.UseBlockStore = true
	vb.logger().Info("BlockStore enabled")
}

// DisableBlockStore disables the unified block store and uses legacy data structures.
func (vb *VB) DisableBlockStore() {
	vb.UseBlockStore = false
	vb.logger().Info("BlockStore disabled")
}

// readBlockStore is the optimized read path using UnifiedBlockStore
// Provides O(1) lookups instead of O(n) map rebuilding per read.
func (vb *VB) readBlockStore(ctx context.Context, block uint64, blockLen uint64) (data []byte, err error) {
	// Check blockLen is a multiple of blocksize
	if blockLen%uint64(vb.BlockSize) != 0 {
		return nil, ErrRequestBlockSize
	}

	var zeroBlockErr error
	data = make([]byte, blockLen)
	blockRequests := blockLen / uint64(vb.BlockSize)

	var consecutiveBlocks ConsecutiveBlocks
	var baseConsecutiveBlocks ConsecutiveBlocks
	ancestorConsBlocks := make([]ConsecutiveBlocks, len(vb.ancestors))

	for i := range blockRequests {
		currentBlock := block + i
		start := i * uint64(vb.BlockSize)
		end := start + uint64(vb.BlockSize)

		// O(1) lookup using BlockStore. A single atomically-captured snapshot
		// (state + data + persisted location together) — NOT ReadSingle
		// followed by a separate ReadBlock call, which would leave a window
		// for a concurrent rewrite to change the entry between the two reads
		// and hand back a torn (stale-state, fresh-location) pair. See
		// ReadEntry's doc comment.
		entry, exists := vb.BlockStore.ReadEntry(currentBlock)
		state := entry.State
		if !exists {
			state = BlockStateEmpty
		}

		switch state {
		case BlockStateHot, BlockStatePending, BlockStateCached:
			// Data is available in memory: a cache hit.
			telemetry.RecordCacheLookup(ctx, true)
			copy(data[start:end], entry.Data)

		case BlockStatePersisted:
			// Need to fetch from backend: a cache miss.
			telemetry.RecordCacheLookup(ctx, false)

			consecutiveBlocks = append(consecutiveBlocks, ConsecutiveBlock{
				BlockPosition: i,
				StartBlock:    currentBlock,
				NumBlocks:     1,
				OffsetStart:   start,
				OffsetEnd:     end,
				ObjectID:      entry.ObjectID,
				ObjectOffset:  entry.ObjectOffset,
				SeqNum:        entry.SeqNum,
			})

		case BlockStateEmpty:
			// Block not in our own map -- check snapshot base map
			if vb.BaseBlockMap != nil {
				objectID, objectOffset, baseSeqNum, baseErr := vb.LookupBaseBlockToObject(currentBlock)
				if baseErr == nil {
					baseConsecutiveBlocks = append(baseConsecutiveBlocks, ConsecutiveBlock{
						BlockPosition: i,
						StartBlock:    currentBlock,
						NumBlocks:     1,
						OffsetStart:   start,
						OffsetEnd:     end,
						ObjectID:      objectID,
						ObjectOffset:  objectOffset,
						SeqNum:        baseSeqNum,
					})
					continue
				}
			}
			// Base map miss — check ancestor layers
			ancFound := false
			for ai := range vb.ancestors {
				vb.ancestors[ai].blocks.mu.RLock()
				lookup, ok := vb.ancestors[ai].blocks.BlockLookup[currentBlock]
				vb.ancestors[ai].blocks.mu.RUnlock()
				if ok {
					ancestorConsBlocks[ai] = append(ancestorConsBlocks[ai], ConsecutiveBlock{
						BlockPosition: i,
						StartBlock:    currentBlock,
						NumBlocks:     1,
						OffsetStart:   start,
						OffsetEnd:     end,
						ObjectID:      lookup.ObjectID,
						ObjectOffset:  lookup.ObjectOffset,
						SeqNum:        lookup.SeqNum,
					})
					ancFound = true
					break
				}
			}
			if ancFound {
				continue
			}
			// ReadEntry's !exists / default-state paths both correspond to
			// "no own-volume, base-map, or ancestor data for this block".
			zeroBlockErr = ErrZeroBlock
		}
	}

	// Fetch consecutive blocks from our own backend
	if len(consecutiveBlocks) > 0 {
		err = vb.fetchConsecutiveBlocksFromBackend(ctx, consecutiveBlocks, data)
		if err != nil {
			return nil, err
		}
	}

	// Fetch blocks from the source volume's backend (snapshot fallback)
	if len(baseConsecutiveBlocks) > 0 {
		err = vb.fetchBaseBlocksFromBackend(ctx, vb.SourceVolumeName, vb.SourceVolumeUUID, vb.sourceVolumeNameHash, baseConsecutiveBlocks, data)
		if err != nil {
			return nil, err
		}
	}

	// Fetch blocks from ancestor snapshot layers
	for ai, anc := range vb.ancestors {
		if len(ancestorConsBlocks[ai]) == 0 {
			continue
		}
		if err = vb.fetchBaseBlocksFromBackend(ctx, anc.sourceVolumeName, anc.sourceVolumeUUID, anc.sourceVolumeNameHash, ancestorConsBlocks[ai], data); err != nil {
			return nil, err
		}
	}

	return data, zeroBlockErr
}

// fetchConsecutiveBlocksFromBackend fetches blocks from backend storage
// Used by both legacy and BlockStore read paths.
func (vb *VB) fetchConsecutiveBlocksFromBackend(ctx context.Context, consecutiveBlocks ConsecutiveBlocks, data []byte) error {
	var consecutiveBlocksToRead ConsecutiveBlocks
	consecutiveBlocksRead := make(map[uint64]bool, len(consecutiveBlocks))

	for i := range consecutiveBlocks {
		if _, ok := consecutiveBlocksRead[consecutiveBlocks[i].StartBlock]; ok {
			continue
		}

		numBlocks := 1
		for j := i + 1; j < len(consecutiveBlocks); j++ {
			if (consecutiveBlocks[j].StartBlock == consecutiveBlocks[j-1].StartBlock+1) &&
				consecutiveBlocks[j].ObjectID == consecutiveBlocks[j-1].ObjectID {
				numBlocks++
				consecutiveBlocksRead[consecutiveBlocks[j].StartBlock] = true
			} else {
				break
			}
		}

		var seqNums []uint64
		if vb.EncryptionEnabled {
			seqNums = make([]uint64, numBlocks)
			for k := 0; k < numBlocks; k++ {
				seqNums[k] = consecutiveBlocks[i+k].SeqNum
			}
		}

		consecutiveBlocksToRead = append(consecutiveBlocksToRead, ConsecutiveBlock{
			BlockPosition: consecutiveBlocks[i].BlockPosition,
			StartBlock:    consecutiveBlocks[i].StartBlock,
			NumBlocks:     utils.SafeIntToUint16(numBlocks),
			OffsetStart:   consecutiveBlocks[i].OffsetStart,
			OffsetEnd:     consecutiveBlocks[i].OffsetEnd,
			ObjectID:      consecutiveBlocks[i].ObjectID,
			ObjectOffset:  consecutiveBlocks[i].ObjectOffset,
			SeqNums:       seqNums,
		})
	}

	stride := vb.BlockSize
	if vb.EncryptionEnabled {
		stride += 16
	}

	for _, cb := range consecutiveBlocksToRead {
		vb.logger().DebugContext(ctx, "[READ] READING CONSECUTIVE BLOCK:", "startBlock", cb.StartBlock, "numBlocks", cb.NumBlocks)

		consecutiveBlockOffset := uint32(cb.NumBlocks) * stride
		start := cb.BlockPosition * uint64(vb.BlockSize)
		end := start + uint64(cb.NumBlocks)*uint64(vb.BlockSize)

		objectID := cb.ObjectID
		if err := vb.checkChunkMagic(vb.VolumeName, objectID, func(off, length uint32) ([]byte, error) {
			return vb.Backend.ReadCtx(ctx, types.FileTypeChunk, objectID, off, length)
		}); err != nil {
			return err
		}

		blockData, err := vb.Backend.ReadCtx(ctx, types.FileTypeChunk, cb.ObjectID, cb.ObjectOffset, consecutiveBlockOffset)
		if err != nil {
			return err
		}

		if vb.EncryptionEnabled {
			if err := vb.openChunkRun(blockData, cb, vb.VolumeUUID, vb.volumeNameHash, data[start:end]); err != nil {
				return err
			}
		} else {
			copy(data[start:end], blockData)
		}

		// Cache blocks in BlockStore (post-decrypt plaintext)
		if vb.UseBlockStore {
			for i := uint64(0); i < uint64(cb.NumBlocks); i++ {
				currentBlock := cb.StartBlock + i
				blockStart := start + i*uint64(vb.BlockSize)
				blockEnd := blockStart + uint64(vb.BlockSize)
				vb.BlockStore.Cache(currentBlock, data[blockStart:blockEnd])
			}
		}

		// Also update legacy LRU cache if enabled (post-decrypt plaintext)
		if vb.Cache.Config.Size > 0 {
			for i := uint64(0); i < uint64(cb.NumBlocks); i++ {
				currentBlock := cb.StartBlock + i
				blockStart := start + i*uint64(vb.BlockSize)
				blockEnd := blockStart + uint64(vb.BlockSize)
				vb.Cache.lru.Add(currentBlock, bytes.Clone(data[blockStart:blockEnd]))
			}
		}
	}

	return nil
}

// fetchBaseBlocksFromBackend fetches blocks from the source volume's backend storage.
// Used by clone volumes to read blocks from the base or an ancestor snapshot layer.
// uuid and nameHash carry the encryption identity of the source volume; ignored on
// unencrypted volumes.
func (vb *VB) fetchBaseBlocksFromBackend(ctx context.Context, sourceVolume string, uuid [4]byte, nameHash [32]byte, consecutiveBlocks ConsecutiveBlocks, data []byte) error {
	if sourceVolume == "" {
		return fmt.Errorf("fetchBaseBlocksFromBackend: sourceVolume is empty")
	}
	var consecutiveBlocksToRead ConsecutiveBlocks
	consecutiveBlocksRead := make(map[uint64]bool, len(consecutiveBlocks))

	for i := range consecutiveBlocks {
		if _, ok := consecutiveBlocksRead[consecutiveBlocks[i].StartBlock]; ok {
			continue
		}

		numBlocks := 1
		for j := i + 1; j < len(consecutiveBlocks); j++ {
			if (consecutiveBlocks[j].StartBlock == consecutiveBlocks[j-1].StartBlock+1) &&
				consecutiveBlocks[j].ObjectID == consecutiveBlocks[j-1].ObjectID {
				numBlocks++
				consecutiveBlocksRead[consecutiveBlocks[j].StartBlock] = true
			} else {
				break
			}
		}

		var seqNums []uint64
		if vb.EncryptionEnabled {
			seqNums = make([]uint64, numBlocks)
			for k := 0; k < numBlocks; k++ {
				seqNums[k] = consecutiveBlocks[i+k].SeqNum
			}
		}

		consecutiveBlocksToRead = append(consecutiveBlocksToRead, ConsecutiveBlock{
			BlockPosition: consecutiveBlocks[i].BlockPosition,
			StartBlock:    consecutiveBlocks[i].StartBlock,
			NumBlocks:     utils.SafeIntToUint16(numBlocks),
			OffsetStart:   consecutiveBlocks[i].OffsetStart,
			OffsetEnd:     consecutiveBlocks[i].OffsetEnd,
			ObjectID:      consecutiveBlocks[i].ObjectID,
			ObjectOffset:  consecutiveBlocks[i].ObjectOffset,
			SeqNums:       seqNums,
		})
	}

	stride := vb.BlockSize
	if vb.EncryptionEnabled {
		stride += 16
	}

	for _, cb := range consecutiveBlocksToRead {
		vb.logger().DebugContext(ctx, "[READ] READING BASE BLOCK:", "startBlock", cb.StartBlock, "numBlocks", cb.NumBlocks, "sourceVolume", sourceVolume)

		consecutiveBlockOffset := uint32(cb.NumBlocks) * stride
		start := cb.BlockPosition * uint64(vb.BlockSize)
		end := start + uint64(cb.NumBlocks)*uint64(vb.BlockSize)

		objectID := cb.ObjectID
		if err := vb.checkChunkMagic(sourceVolume, objectID, func(off, length uint32) ([]byte, error) {
			return vb.Backend.ReadFromCtx(ctx, sourceVolume, types.FileTypeChunk, objectID, off, length)
		}); err != nil {
			return fmt.Errorf("ReadFrom source %s: %w", sourceVolume, err)
		}

		blockData, err := vb.Backend.ReadFromCtx(ctx, sourceVolume, types.FileTypeChunk, cb.ObjectID, cb.ObjectOffset, consecutiveBlockOffset)
		if err != nil {
			return fmt.Errorf("ReadFrom source %s object %d: %w", sourceVolume, cb.ObjectID, err)
		}

		expectedLen := int(consecutiveBlockOffset)
		if len(blockData) != expectedLen {
			return fmt.Errorf("ReadFrom source %s object %d: short read: got %d bytes, expected %d", sourceVolume, cb.ObjectID, len(blockData), expectedLen)
		}

		if vb.EncryptionEnabled {
			if err := vb.openChunkRun(blockData, cb, uuid, nameHash, data[start:end]); err != nil {
				return fmt.Errorf("base chunk decrypt source %s: %w", sourceVolume, err)
			}
		} else {
			copy(data[start:end], blockData)
		}

		// Cache blocks in BlockStore (post-decrypt plaintext)
		if vb.UseBlockStore {
			for i := uint64(0); i < uint64(cb.NumBlocks); i++ {
				currentBlock := cb.StartBlock + i
				blockStart := start + i*uint64(vb.BlockSize)
				blockEnd := blockStart + uint64(vb.BlockSize)
				vb.BlockStore.Cache(currentBlock, data[blockStart:blockEnd])
			}
		}

		if vb.Cache.Config.Size > 0 {
			for i := uint64(0); i < uint64(cb.NumBlocks); i++ {
				currentBlock := cb.StartBlock + i
				blockStart := start + i*uint64(vb.BlockSize)
				blockEnd := blockStart + uint64(vb.BlockSize)
				vb.Cache.lru.Add(currentBlock, bytes.Clone(data[blockStart:blockEnd]))
			}
		}
	}

	return nil
}

// WriteBlockStore writes a block using the unified block store.
func (vb *VB) WriteBlockStore(block uint64, data []byte) (err error) {
	blockLen := uint64(len(data))

	if block*uint64(vb.BlockSize) > vb.GetVolumeSize() {
		return ErrRequestTooLarge
	}

	if block*uint64(vb.BlockSize)+blockLen > vb.GetVolumeSize() {
		return ErrRequestOutOfRange
	}

	if blockLen%uint64(vb.BlockSize) != 0 {
		return ErrRequestBlockSize
	}

	blockRequests := blockLen / uint64(vb.BlockSize)

	for i := range blockRequests {
		currentBlock := block + i
		start := i * uint64(vb.BlockSize)
		end := start + uint64(vb.BlockSize)

		// Write to BlockStore (returns seqNum)
		seqNum := vb.BlockStore.Write(currentBlock, data[start:end])

		// Also update main SeqNum for compatibility
		for {
			current := vb.SeqNum.Load()
			if seqNum <= current {
				break
			}
			if vb.SeqNum.CompareAndSwap(current, seqNum) {
				break
			}
		}

		// Also write to legacy Writes buffer for WAL compatibility
		vb.Writes.mu.Lock()
		blockCopy := make([]byte, vb.BlockSize)
		copy(blockCopy, data[start:end])
		vb.Writes.Blocks = append(vb.Writes.Blocks, Block{
			SeqNum: seqNum,
			Block:  currentBlock,
			Data:   blockCopy,
		})
		vb.Writes.mu.Unlock()
	}

	return nil
}

// FlushBlockStore flushes hot blocks using the BlockStore path.
func (vb *VB) FlushBlockStore() error {
	// Get all hot blocks from BlockStore
	hotBlocks := vb.BlockStore.GetHotBlocks()

	if len(hotBlocks) == 0 {
		return nil
	}

	flushed := make(map[uint64]uint64) // block -> seqnum
	successCount := 0

	for _, block := range hotBlocks {
		if err := vb.WriteWAL(block); err != nil {
			vb.logger().Error("ERROR FLUSHING:", "block", block.Block, "error", err)
			break
		}

		successCount++
		flushed[block.Block] = block.SeqNum
		// Transition to Pending in BlockStore
		vb.BlockStore.MarkPending(block.Block)
	}

	// Also update legacy Writes buffer for compatibility
	if len(flushed) > 0 {
		vb.Writes.mu.Lock()
		remaining := make([]Block, 0)
		for _, b := range vb.Writes.Blocks {
			if _, ok := flushed[b.Block]; !ok {
				remaining = append(remaining, b)
			}
		}
		vb.Writes.Blocks = remaining
		vb.Writes.mu.Unlock()

		// Append to pending backend writes for legacy compatibility
		vb.PendingBackendWrites.mu.Lock()
		vb.PendingBackendWrites.Blocks = append(vb.PendingBackendWrites.Blocks, hotBlocks...)
		vb.PendingBackendWrites.mu.Unlock()
	}

	if successCount < len(hotBlocks) {
		return fmt.Errorf("partial flush: %d of %d records flushed", successCount, len(hotBlocks))
	}

	return nil
}

// SyncBlockStoreFromLegacy synchronizes BlockStore state from legacy data structures
// Used during migration or recovery.
func (vb *VB) SyncBlockStoreFromLegacy() {
	// Sync from Writes
	vb.Writes.mu.RLock()
	for _, block := range vb.Writes.Blocks {
		vb.BlockStore.WriteWithSeqNum(block.Block, block.Data, block.SeqNum)
	}
	vb.Writes.mu.RUnlock()

	// Sync from PendingBackendWrites
	vb.PendingBackendWrites.mu.RLock()
	for _, block := range vb.PendingBackendWrites.Blocks {
		vb.BlockStore.WriteWithSeqNum(block.Block, block.Data, block.SeqNum)
		vb.BlockStore.MarkPending(block.Block)
	}
	vb.PendingBackendWrites.mu.RUnlock()

	// Sync from BlocksToObject
	vb.BlocksToObject.mu.RLock()
	for blockNum, lookup := range vb.BlocksToObject.BlockLookup {
		vb.BlockStore.SetPersisted(blockNum, lookup.ObjectID, lookup.ObjectOffset, 0)
	}
	vb.BlocksToObject.mu.RUnlock()

	// Sync sequence number
	vb.BlockStore.SetSeqNum(vb.SeqNum.Load())

	vb.logger().Info("BlockStore synchronized from legacy data structures",
		"blocks", vb.BlockStore.Count())
}

// GetBlockStoreStats returns statistics from the BlockStore.
func (vb *VB) GetBlockStoreStats() (reads, writes, cacheHits, cacheMiss uint64) {
	if vb.BlockStore == nil {
		return 0, 0, 0, 0
	}
	return vb.BlockStore.GetStats()
}
