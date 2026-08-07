package viperblock

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/utils"
)

// SnapshotState holds metadata for a frozen snapshot stored on the backend.
//
// SourceVolumeUUID + SourceVolumeNameHash carry the source volume's
// encryption identity forward so a clone reading base chunks via
// fetchBaseBlocksFromBackend reconstructs the source's nonce + AAD, not the
// clone's. Both are hex strings (8 chars / 64 chars) rather than [N]byte to
// keep SnapshotState plaintext-readable for operator debugging — JSON would
// base64-encode fixed-size byte arrays. Empty on snapshots of unencrypted
// volumes (zero-valued, ignored on the read path).
//
// StateSeqNum is the source volume's VB.nextStateSeqNum value at the moment
// CreateSnapshot sealed this state — bound into the snapshot-meta nonce so
// back-to-back snapshots on the same volume use disjoint nonces (domain
// separation alone does not protect two seals in the same domain).
//
// HasFlatSection marks checkpoints that carry an inherited-blocks section after
// the own-blocks entries. When true, BlockCount is exact and the section starts
// at headerSize + BlockCount*blockWalChunkSize bytes into the checkpoint.
type SnapshotState struct {
	SnapshotID           string    `json:"SnapshotID"`
	SourceVolumeName     string    `json:"SourceVolumeName"`
	SeqNum               uint64    `json:"SeqNum"`
	ObjectNum            uint64    `json:"ObjectNum"`
	BlockSize            uint32    `json:"BlockSize"`
	ObjBlockSize         uint32    `json:"ObjBlockSize"`
	BlockCount           uint64    `json:"BlockCount"`
	CreatedAt            time.Time `json:"CreatedAt"`
	SourceVolumeUUID     string    `json:"SourceVolumeUUID,omitempty"`
	SourceVolumeNameHash string    `json:"SourceVolumeNameHash,omitempty"`
	StateSeqNum          uint64    `json:"StateSeqNum,omitempty"`
	HasFlatSection       bool      `json:"HasFlatSection,omitempty"`
}

// flatSectionMagic is the 4-byte header that identifies the inherited-blocks
// section appended after own-block entries in a flat snapshot checkpoint.
var flatSectionMagic = [4]byte{'F', 'L', 'A', 'T'}

// InheritedLayer carries the block map and source identity for one ancestor
// level decoded from a flat snapshot's inherited-blocks section.
type InheritedLayer struct {
	Blocks               *BlocksToObject
	SourceVolumeName     string
	SourceVolumeUUID     [4]byte
	SourceVolumeNameHash [32]byte
}

// drainForSnapshot runs the flush -> WriteWALToChunk -> SaveLiveCheckpoint
// sequence that gets the source volume fully persisted before its map is
// frozen into a snapshot. That sequence is a drain in all but name, so it
// holds drainMu for exactly the same reason DrainToBackendCtx does (see
// drainMu's doc comment): two overlapping drains rotate different WAL
// segments and can publish their live checkpoints out of order, leaving the
// older map on the backend. The volume then boots pointing blocks at
// superseded chunks and cold reads hand back stale bytes with no error
// anywhere -- the durability half of the stale-drain repoint hazard.
//
// Scoped to its own function rather than deferred inside CreateSnapshot so
// the lock is released before the (potentially slow) snapshot checkpoint
// serialization and upload, which no drain contends with.
func (vb *VB) drainForSnapshot() error {
	vb.drainMu.Lock()
	defer vb.drainMu.Unlock()

	if flushErr := vb.flushWrites(); flushErr != nil {
		return fmt.Errorf("snapshot flush failed: %w", flushErr)
	}

	// Persist WAL to chunk files on backend (potentially slow S3 upload,
	// no need to hold the write lock -- new writes go to the next WAL file)
	if err := vb.WriteWALToChunk(true); err != nil {
		return fmt.Errorf("snapshot WAL-to-chunk failed: %w", err)
	}

	// Chunk uploads no longer refresh the live checkpoint per-chunk (that
	// was a parallel-upload hazard, see createChunkFile). Refresh it once
	// here so the source volume's live checkpoint reflects the chunks just
	// uploaded, same as DrainToBackendCtx does after its own drain.
	// Non-fatal: a stale live checkpoint self-heals on the next drain.
	if err := vb.SaveLiveCheckpoint(); err != nil {
		vb.logger().Warn("CreateSnapshot: SaveLiveCheckpoint failed", "err", err)
	}

	return nil
}

// CreateSnapshot flushes all in-flight data and creates a frozen snapshot of
// the current block-to-object mapping. The snapshot is stored on the backend
// under {snapshotID}/. No data is copied -- the snapshot references the
// source volume's existing chunk files.
func (vb *VB) CreateSnapshot(snapshotID string) (*SnapshotState, error) {
	// Latch chunk GC off permanently the moment a snapshot is created,
	// regardless of outcome below. This closes a race ensureGCSnapshotSafe
	// can't: a snapshot created while a sweep is already in flight would
	// otherwise stay invisible to that sweep's cached result.
	if vb.GCEnabled && vb.gcLatchedOff.CompareAndSwap(false, true) {
		vb.logger().Warn("chunk GC: disabled permanently, CreateSnapshot called on this volume", "volume", vb.VolumeName, "snapshotID", snapshotID)
	}

	// The latch above only reaches a sweeper inside this process, and the
	// engine that sweeps an attached volume (the NBD plugin) is a different
	// one from the engine that snapshots it (the daemon). Publish the marker
	// so that sweeper sees this snapshot, and publish it BEFORE the map is
	// read below, which is what makes the sweeper's post-checkpoint read of it
	// sound (see writeSnapshotMarker).
	//
	// Fail the snapshot if the marker doesn't land: a snapshot no sweeper can
	// observe is one whose chunks may be reclaimed underneath it. A retryable
	// error here is cheaper than a corrupt snapshot.
	if err := vb.writeSnapshotMarker(context.Background(), snapshotID); err != nil {
		return nil, err
	}

	vb.logger().Info("CreateSnapshot: flushing data", "snapshotID", snapshotID, "volume", vb.VolumeName)

	// 1. Flush hot writes to WAL and persist to chunks.
	// Skip if this VB doesn't own the WAL files (e.g. viperblockd snapshot VB
	// where the NBD plugin process owns the WAL).
	if vb.ownsWAL() {
		if err := vb.drainForSnapshot(); err != nil {
			return nil, err
		}
	}

	// 2. Serialize the current block-to-object map as the snapshot checkpoint.
	// For COW clones (vb.BaseBlockMap != nil), also write a flat inherited-blocks
	// section so the snapshot is self-contained with no ancestry chain to walk.
	vb.BlocksToObject.mu.RLock()
	// BlockCount must be the physical block count, not the number of
	// coalesced map entries — sum each entry's NumBlocks rather than len().
	var blockCount uint64
	checkpoint := vb.BlockToObjectWALHeader()
	stride := vb.blockStride()
	vb.BlocksToObject.lookup.scan(func(block BlockLookup) bool {
		blockCount += uint64(block.NumBlocks)
		for _, single := range expandBlockLookup(block, stride) {
			checkpoint = append(checkpoint, vb.writeBlockWalChunk(&single)...)
		}
		return true
	})
	vb.BlocksToObject.mu.RUnlock()

	hasFlatSection := vb.BaseBlockMap != nil
	if hasFlatSection {
		flatBytes, err := vb.buildFlatSection()
		if err != nil {
			return nil, fmt.Errorf("build flat section: %w", err)
		}
		checkpoint = append(checkpoint, flatBytes...)
	}

	// Write checkpoint to {snapshotID}/checkpoints/blocks.00000001.bin
	emptyHeaders := []byte{}
	if err := vb.Backend.WriteTo(snapshotID, types.FileTypeBlockCheckpoint, 0, &emptyHeaders, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to write snapshot checkpoint: %w", err)
	}

	// 3. Build and save snapshot metadata.
	snap := &SnapshotState{
		SnapshotID:       snapshotID,
		SourceVolumeName: vb.VolumeName,
		SeqNum:           vb.SeqNum.Load(),
		ObjectNum:        vb.ObjectNum.Load(),
		BlockSize:        vb.BlockSize,
		ObjBlockSize:     vb.ObjBlockSize,
		BlockCount:       blockCount,
		CreatedAt:        time.Now(),
		HasFlatSection:   hasFlatSection,
	}

	if vb.EncryptionEnabled {
		snap.SourceVolumeNameHash = hex.EncodeToString(vb.volumeNameHash[:])
		snap.StateSeqNum = vb.nextStateSeqNum.Add(1)
		// Persist the bumped counter before sealing under it: a crash
		// between this Add and the next SaveState would otherwise reset
		// nextStateSeqNum to the previously persisted value on restart,
		// letting a subsequent snapshot re-issue snap.StateSeqNum and
		// reuse this nonce under the same (key, VolumeUUID, domain=0x03)
		// triple — catastrophic for AES-GCM. SaveState bumps once more
		// for its own VBState, so the durable high-water lands at
		// snap.StateSeqNum + 1 (or higher under concurrent saves).
		if err := vb.SaveState(); err != nil {
			return nil, fmt.Errorf("snapshot StateSeqNum persist: %w", err)
		}
		// Capture VolumeUUID AFTER SaveState: SaveState mints it on first
		// use for an encrypted volume, and the meta nonce below seals under
		// the minted value. Recording it earlier persists the pre-mint zero
		// UUID, so open-time nonce reconstruction diverges from the seal
		// nonce and every read fails tag verify.
		snap.SourceVolumeUUID = hex.EncodeToString(vb.VolumeUUID[:])
	}

	snapJSON, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshot state: %w", err)
	}

	// Encrypted snapshots append a 16-byte AES-GCM tag binding the JSON to
	// (volumeNameHash, "snap:"||snapshotID, StateSeqNum). The snapshotID
	// literal in the AAD prevents cross-snapshot splicing under the same key;
	// tampering with any persisted field (SourceVolumeUUID, BlockCount, etc.)
	// breaks verification because the JSON bytes are part of the covered AAD.
	persisted := snapJSON
	if vb.EncryptionEnabled {
		nonce := makeNonce(snap.StateSeqNum, vb.VolumeUUID, DomainSnapshotMeta)
		aad := makeMetaAAD(vb.volumeNameHash, "snap:"+snapshotID, snap.StateSeqNum)
		persisted = sealMeta(vb.aead, snapJSON, aad, nonce)
	}

	if err := vb.Backend.WriteTo(snapshotID, types.FileTypeConfig, 0, &emptyHeaders, &persisted); err != nil {
		return nil, fmt.Errorf("failed to write snapshot config: %w", err)
	}

	vb.logger().Info("CreateSnapshot: complete", "snapshotID", snapshotID, "sourceVolume", vb.VolumeName)
	return snap, nil
}

// SnapshotIdentity carries the source volume's encryption-identity fields
// recovered from SnapshotState so clone reads can reconstruct the source's
// nonce + AAD. Zero-valued on snapshots of unencrypted volumes; callers
// branch on vb.EncryptionEnabled rather than testing zero.
// InheritedLayers is non-nil for flat snapshots and carries the decoded
// inherited-blocks section (all ancestor layers, no further I/O needed).
type SnapshotIdentity struct {
	SourceVolumeName     string
	SourceVolumeUUID     [4]byte
	SourceVolumeNameHash [32]byte
	InheritedLayers      []InheritedLayer
}

// readSnapshotState reads a snapshot's config.json from the backend and, on
// encrypted volumes, authenticates its metaEnvelope before unmarshalling.
//
// Two-stage parse: split the envelope, peek the verbatim payload for
// SourceVolumeUUID + SourceVolumeNameHash + StateSeqNum (needed to reconstruct
// nonce + AAD), verify, then unmarshal the verified bytes into the full
// struct. The snapshotID parameter is the trusted external identity bound into
// the AAD — splicing in another snapshot's blob fails because the literal
// "snap:"||snapshotID differs from the seal-time value.
func (vb *VB) readSnapshotState(snapshotID string) (*SnapshotState, error) {
	configData, err := vb.Backend.ReadFrom(snapshotID, types.FileTypeConfig, 0, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot config %s: %w", snapshotID, err)
	}

	if vb.EncryptionEnabled {
		payload, tag, splitErr := splitEnvelope(configData)
		if splitErr != nil {
			return nil, fmt.Errorf("%w: snapshot %s envelope: %w", ErrIntegrity, snapshotID, splitErr)
		}
		var peek struct {
			SourceVolumeUUID     string `json:"SourceVolumeUUID"`
			SourceVolumeNameHash string `json:"SourceVolumeNameHash"`
			StateSeqNum          uint64 `json:"StateSeqNum"`
		}
		if err := json.Unmarshal(payload, &peek); err != nil {
			return nil, fmt.Errorf("%w: snapshot %s peek parse: %w", ErrIntegrity, snapshotID, err)
		}
		uuid, uuidErr := hex.DecodeString(peek.SourceVolumeUUID)
		if uuidErr != nil || len(uuid) != 4 {
			return nil, fmt.Errorf("%w: snapshot %s SourceVolumeUUID invalid", ErrIntegrity, snapshotID)
		}
		nameHash, nhErr := hex.DecodeString(peek.SourceVolumeNameHash)
		if nhErr != nil || len(nameHash) != 32 {
			return nil, fmt.Errorf("%w: snapshot %s SourceVolumeNameHash invalid", ErrIntegrity, snapshotID)
		}
		var nonceUUID [4]byte
		copy(nonceUUID[:], uuid)
		var aadNameHash [32]byte
		copy(aadNameHash[:], nameHash)
		nonce := makeNonce(peek.StateSeqNum, nonceUUID, DomainSnapshotMeta)
		aad := makeMetaAAD(aadNameHash, "snap:"+snapshotID, peek.StateSeqNum)
		if err := verifyMeta(vb.aead, payload, tag, aad, nonce); err != nil {
			return nil, fmt.Errorf("%w: snapshot %s tag verify: %w", ErrIntegrity, snapshotID, err)
		}
		configData = payload
	}

	var snap SnapshotState
	if err := json.Unmarshal(configData, &snap); err != nil {
		return nil, fmt.Errorf("failed to parse snapshot config %s: %w", snapshotID, err)
	}
	return &snap, nil
}

// LoadSnapshotBlockMap reads a snapshot's frozen block-to-object map from the
// backend and returns it along with the source volume's encryption identity.
// The returned BlocksToObject is intended to be used as a read-only base map
// for clone volumes.
func (vb *VB) LoadSnapshotBlockMap(snapshotID string) (*BlocksToObject, SnapshotIdentity, error) {
	var ident SnapshotIdentity

	// 1. Read and (for encrypted volumes) authenticate the snapshot config
	snapPtr, err := vb.readSnapshotState(snapshotID)
	if err != nil {
		return nil, ident, err
	}
	snap := *snapPtr

	ident.SourceVolumeName = snap.SourceVolumeName
	if snap.SourceVolumeUUID != "" {
		uuid, err := hex.DecodeString(snap.SourceVolumeUUID)
		if err != nil || len(uuid) != 4 {
			return nil, ident, fmt.Errorf("snapshot %s: invalid SourceVolumeUUID %q", snapshotID, snap.SourceVolumeUUID)
		}
		copy(ident.SourceVolumeUUID[:], uuid)
	}
	if snap.SourceVolumeNameHash != "" {
		nameHash, err := hex.DecodeString(snap.SourceVolumeNameHash)
		if err != nil || len(nameHash) != 32 {
			return nil, ident, fmt.Errorf("snapshot %s: invalid SourceVolumeNameHash", snapshotID)
		}
		copy(ident.SourceVolumeNameHash[:], nameHash)
	}

	// 2. Read the checkpoint
	checkpoint, err := vb.Backend.ReadFrom(snapshotID, types.FileTypeBlockCheckpoint, 0, 0, 0)
	if err != nil {
		return nil, ident, fmt.Errorf("failed to read snapshot checkpoint %s: %w", snapshotID, err)
	}

	// 3. Validate header
	headerSize := vb.BlockToObjectWALHeaderSize()
	if len(checkpoint) < headerSize {
		return nil, ident, fmt.Errorf("snapshot checkpoint too small: %d bytes", len(checkpoint))
	}

	headers := checkpoint[:headerSize]
	if !bytes.Equal(headers[:4], vb.BlockToObjectWAL.WALMagic[:]) {
		return nil, ident, fmt.Errorf("snapshot checkpoint magic mismatch")
	}
	if binary.BigEndian.Uint16(headers[4:6]) != vb.Version {
		return nil, ident, fmt.Errorf("snapshot checkpoint version mismatch")
	}

	// 4. Validate own-blocks section size. For flat snapshots BlockCount is exact;
	// for legacy snapshots the entire data section must be a multiple of entry size.
	dataSize := len(checkpoint) - headerSize
	ownBlocksSize := dataSize
	if snap.HasFlatSection {
		if snap.BlockCount > math.MaxInt/blockWalChunkSize {
			return nil, ident, fmt.Errorf("snapshot BlockCount %d overflows addressable size", snap.BlockCount)
		}
		ownBlocksSize = int(snap.BlockCount) * blockWalChunkSize
	}
	if ownBlocksSize%blockWalChunkSize != 0 {
		return nil, ident, fmt.Errorf("snapshot checkpoint has %d trailing bytes (own-blocks section %d bytes is not a multiple of %d-byte entries)", ownBlocksSize%blockWalChunkSize, ownBlocksSize, blockWalChunkSize)
	}

	// 5. Deserialize own block lookup entries
	baseMap := &BlocksToObject{}

	offset := headerSize
	end := headerSize + ownBlocksSize
	for offset+blockWalChunkSize <= end {
		block, err := vb.readBlockWalChunk(checkpoint[offset : offset+blockWalChunkSize])
		if err != nil {
			return nil, ident, fmt.Errorf("failed to read snapshot block entry at offset %d: %w", offset, err)
		}
		baseMap.lookup.set(block)
		offset += blockWalChunkSize
	}

	// 6. Validate block count against metadata
	if snap.BlockCount > 0 && utils.SafeIntToUint64(baseMap.lookup.len()) != snap.BlockCount {
		return nil, ident, fmt.Errorf("snapshot block count mismatch: metadata says %d, checkpoint has %d", snap.BlockCount, baseMap.lookup.len())
	}

	// 7. Parse the flat inherited-blocks section if present
	if snap.HasFlatSection {
		layers, err := parseFlatSection(checkpoint[end:])
		if err != nil {
			return nil, ident, fmt.Errorf("snapshot %s flat section: %w", snapshotID, err)
		}
		ident.InheritedLayers = layers
	}

	vb.logger().Debug("LoadSnapshotBlockMap: loaded",
		"snapshotID", snapshotID,
		"sourceVolume", snap.SourceVolumeName,
		"blocks", baseMap.lookup.len(),
		"inheritedLayers", len(ident.InheritedLayers))

	return baseMap, ident, nil
}

// CopySnapshotMeta duplicates the snapshot at srcSnapshotID under
// dstSnapshotID, producing a second, independently addressable snapshot over
// the same frozen block map. No block data is copied — both snapshots
// reference the source volume's existing chunk files.
//
// It must be called on a VB open over the snapshot's own source volume. The
// destination is sealed in that volume's nonce subspace (its VolumeUUID),
// because SourceVolumeUUID/SourceVolumeNameHash are carried across verbatim so
// a clone of the copy still decrypts source chunks under the source identity.
// Only a handle on that volume owns nextStateSeqNum, the counter that keeps
// the snapshot-meta nonce unique within the subspace, so copying from any
// other volume is refused rather than risking AES-GCM nonce reuse.
//
// The checkpoint object is copied byte-for-byte: it is plaintext block-map
// metadata with no nonce or AAD of its own, and the identity a reader needs to
// decrypt the chunks it points at comes from the (authenticated) config.
func (vb *VB) CopySnapshotMeta(srcSnapshotID, dstSnapshotID string) (*SnapshotState, error) {
	if srcSnapshotID == "" || dstSnapshotID == "" {
		return nil, fmt.Errorf("copy snapshot: source and destination snapshot IDs must both be set")
	}
	if srcSnapshotID == dstSnapshotID {
		return nil, fmt.Errorf("copy snapshot: destination %q must differ from the source", dstSnapshotID)
	}

	// Latch chunk GC off for the same reason CreateSnapshot does: the copy is
	// a new snapshot pinning this volume's chunks, and a sweep already in
	// flight cannot see it via its cached ancestry answer.
	if vb.GCEnabled && vb.gcLatchedOff.CompareAndSwap(false, true) {
		vb.logger().Warn("chunk GC: disabled permanently, CopySnapshotMeta called on this volume", "volume", vb.VolumeName, "snapshotID", dstSnapshotID)
	}

	// Refuse to clobber a committed destination. config.json is the commit
	// point (it is written last, below), so a lone checkpoint is a torn prior
	// attempt this call overwrites rather than a snapshot anything can read.
	if _, err := vb.Backend.ReadFrom(dstSnapshotID, types.FileTypeConfig, 0, 0, 0); err == nil {
		return nil, fmt.Errorf("copy snapshot: destination %s already exists", dstSnapshotID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("copy snapshot: probe destination %s: %w", dstSnapshotID, err)
	}

	src, err := vb.readSnapshotState(srcSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("copy snapshot: source %s: %w", srcSnapshotID, err)
	}

	// On an unencrypted volume nothing binds the blob to its key, so this is
	// the only available check that the object read really is srcSnapshotID's
	// state; it also catches an envelope parsed by a non-encrypted reader.
	if src.SnapshotID != srcSnapshotID {
		return nil, fmt.Errorf("copy snapshot: source %s config declares SnapshotID %q", srcSnapshotID, src.SnapshotID)
	}

	if vb.EncryptionEnabled {
		if err := vb.checkSnapshotOwnership(srcSnapshotID, src); err != nil {
			return nil, err
		}
	}

	// Read the source checkpoint before anything is minted or written, so a
	// missing or unreadable source fails with no durable side effects.
	checkpoint, err := vb.Backend.ReadFrom(srcSnapshotID, types.FileTypeBlockCheckpoint, 0, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("copy snapshot: read source checkpoint %s: %w", srcSnapshotID, err)
	}

	// Everything describing the frozen map (SeqNum, ObjectNum, block sizes,
	// BlockCount, HasFlatSection) and the source's encryption identity is
	// carried over unchanged — the copy freezes the same map, and BlockCount
	// is cross-checked against the copied checkpoint on read. Only SnapshotID,
	// CreatedAt and StateSeqNum are re-derived: the copy is a new object with
	// its own creation time, and it must never re-seal under the source's
	// StateSeqNum.
	dst := &SnapshotState{
		SnapshotID:           dstSnapshotID,
		SourceVolumeName:     src.SourceVolumeName,
		SeqNum:               src.SeqNum,
		ObjectNum:            src.ObjectNum,
		BlockSize:            src.BlockSize,
		ObjBlockSize:         src.ObjBlockSize,
		BlockCount:           src.BlockCount,
		CreatedAt:            time.Now(),
		SourceVolumeUUID:     src.SourceVolumeUUID,
		SourceVolumeNameHash: src.SourceVolumeNameHash,
		HasFlatSection:       src.HasFlatSection,
	}

	if vb.EncryptionEnabled {
		dst.StateSeqNum = vb.nextStateSeqNum.Add(1)
		// Persist the bumped counter before sealing under it, exactly as
		// CreateSnapshot does: a crash between this Add and the next SaveState
		// would reset nextStateSeqNum on restart and let a later seal re-issue
		// dst.StateSeqNum, reusing this nonce under the same (key, VolumeUUID,
		// domain=0x03) triple — catastrophic for AES-GCM.
		if err := vb.SaveState(); err != nil {
			return nil, fmt.Errorf("copy snapshot: StateSeqNum persist: %w", err)
		}
	}

	dstJSON, err := json.Marshal(dst)
	if err != nil {
		return nil, fmt.Errorf("copy snapshot: marshal destination state: %w", err)
	}

	// The AAD's "snap:"||dstSnapshotID literal is what stops the copy being
	// spliced back over the source (and vice versa) under the same key.
	persisted := dstJSON
	if vb.EncryptionEnabled {
		nonce := makeNonce(dst.StateSeqNum, vb.VolumeUUID, DomainSnapshotMeta)
		aad := makeMetaAAD(vb.volumeNameHash, "snap:"+dstSnapshotID, dst.StateSeqNum)
		persisted = sealMeta(vb.aead, dstJSON, aad, nonce)
	}

	// Checkpoint first, config last — same order as CreateSnapshot. A failure
	// between the two leaves an orphan checkpoint that no reader accepts as a
	// snapshot (scanForOwnSnapshots skips a prefix with no config.json).
	emptyHeaders := []byte{}
	if err := vb.Backend.WriteTo(dstSnapshotID, types.FileTypeBlockCheckpoint, 0, &emptyHeaders, &checkpoint); err != nil {
		return nil, fmt.Errorf("copy snapshot: write destination checkpoint %s: %w", dstSnapshotID, err)
	}
	if err := vb.Backend.WriteTo(dstSnapshotID, types.FileTypeConfig, 0, &emptyHeaders, &persisted); err != nil {
		return nil, fmt.Errorf("copy snapshot: write destination config %s: %w", dstSnapshotID, err)
	}

	vb.logger().Info("CopySnapshotMeta: complete",
		"srcSnapshotID", srcSnapshotID,
		"dstSnapshotID", dstSnapshotID,
		"sourceVolume", dst.SourceVolumeName,
		"blocks", dst.BlockCount)

	return dst, nil
}

// checkSnapshotOwnership verifies this VB is the volume the snapshot was taken
// of, by comparing the persisted source identity against our own. The copy
// seals in that volume's nonce subspace under its nextStateSeqNum counter, so
// sealing from any other volume risks reusing a nonce the owner has issued.
func (vb *VB) checkSnapshotOwnership(snapshotID string, snap *SnapshotState) error {
	uuid, err := hex.DecodeString(snap.SourceVolumeUUID)
	if err != nil || len(uuid) != 4 {
		return fmt.Errorf("copy snapshot: source %s has invalid SourceVolumeUUID %q", snapshotID, snap.SourceVolumeUUID)
	}
	nameHash, err := hex.DecodeString(snap.SourceVolumeNameHash)
	if err != nil || len(nameHash) != 32 {
		return fmt.Errorf("copy snapshot: source %s has invalid SourceVolumeNameHash", snapshotID)
	}
	if !bytes.Equal(uuid, vb.VolumeUUID[:]) || !bytes.Equal(nameHash, vb.volumeNameHash[:]) {
		return fmt.Errorf("copy snapshot: source %s belongs to volume %q, not %q — copy must run on the snapshot's own volume so StateSeqNum stays unique in its nonce subspace",
			snapshotID, snap.SourceVolumeName, vb.VolumeName)
	}
	return nil
}

// OpenFromSnapshot loads the base block map from a snapshot and configures
// this volume for copy-on-write reads. After calling this, reads that miss
// in the volume's own block map will fall through to the snapshot's frozen
// map and read chunk data from the source volume.
//
// For encrypted clones the source volume's VolumeUUID + volumeNameHash are
// plumbed onto vb so fetchBaseBlocksFromBackend can decrypt source chunks
// under the source's identity (its nonce/AAD), not the clone's own.
func (vb *VB) OpenFromSnapshot(snapshotID string) error {
	baseMap, ident, err := vb.LoadSnapshotBlockMap(snapshotID)
	if err != nil {
		return err
	}

	if ident.SourceVolumeName == "" {
		return fmt.Errorf("snapshot %s has empty SourceVolumeName", snapshotID)
	}

	vb.BaseBlockMap = baseMap
	vb.SourceVolumeName = ident.SourceVolumeName
	vb.SnapshotID = snapshotID
	vb.SourceVolumeUUID = ident.SourceVolumeUUID
	vb.sourceVolumeNameHash = ident.SourceVolumeNameHash
	vb.ancestors = nil

	if len(ident.InheritedLayers) > 0 {
		// Flat snapshot: all ancestor layers are embedded in the checkpoint.
		for _, layer := range ident.InheritedLayers {
			vb.ancestors = append(vb.ancestors, snapshotAncestor{
				blocks:               layer.Blocks,
				sourceVolumeName:     layer.SourceVolumeName,
				sourceVolumeUUID:     layer.SourceVolumeUUID,
				sourceVolumeNameHash: layer.SourceVolumeNameHash,
			})
		}
	}

	vb.logger().Debug("OpenFromSnapshot: clone ready",
		"volume", vb.VolumeName,
		"snapshotID", snapshotID,
		"sourceVolume", ident.SourceVolumeName,
		"baseBlocks", baseMap.lookup.len(),
		"ancestorLayers", len(vb.ancestors))

	return nil
}

// LookupBaseBlockToObject looks up a block in the base (snapshot) block map.
// Returns ErrZeroBlock if the block was never written in the snapshot either.
// The SeqNum return drives source-volume nonce + AAD reconstruction for
// encrypted clones; on unencrypted volumes callers ignore it.
func (vb *VB) LookupBaseBlockToObject(block uint64) (uint64, uint32, uint64, error) {
	if vb.BaseBlockMap == nil {
		return 0, 0, 0, ErrZeroBlock
	}

	vb.BaseBlockMap.mu.RLock()
	lookup, pos, ok := vb.BaseBlockMap.resolveBlockLookup(block)
	vb.BaseBlockMap.mu.RUnlock()

	if !ok {
		return 0, 0, 0, ErrZeroBlock
	}
	return lookup.ObjectID, lookup.offsetAt(pos, vb.blockStride()), lookup.seqNumAt(pos), nil
}

// buildFlatSection serialises all inherited ancestor blocks into a binary
// section appended after the own-block entries in a flat snapshot checkpoint.
//
// Format:
//
//	magic[4] version[1] numSources[1]
//	For each source:
//	  nameLen[2] name[N] uuid[4] nameHash[32] numBlocks[4]
//	  [BlockWalChunk × numBlocks]
func (vb *VB) buildFlatSection() ([]byte, error) {
	type sourceGroup struct {
		name     string
		uuid     [4]byte
		nameHash [32]byte
		blocks   []BlockLookup
	}

	// Build a seen-set from own blocks so inherited entries only include blocks
	// not already covered by the volume's own delta. Own entries may be
	// coalesced runs (NumBlocks > 1), so expand each entry's full range into
	// the seen-set rather than just its map key (StartBlock).
	seen := make(map[uint64]struct{}, vb.BlocksToObject.lookup.len())
	vb.BlocksToObject.mu.RLock()
	vb.BlocksToObject.lookup.scan(func(bl BlockLookup) bool {
		for i := uint16(0); i < bl.NumBlocks; i++ {
			seen[bl.StartBlock+uint64(i)] = struct{}{}
		}
		return true
	})
	vb.BlocksToObject.mu.RUnlock()

	// Collect layers in priority order: base map first, then deeper ancestors.
	sources := make([]sourceGroup, 0, 1+len(vb.ancestors))

	baseGroup := sourceGroup{name: vb.SourceVolumeName, uuid: vb.SourceVolumeUUID, nameHash: vb.sourceVolumeNameHash}
	vb.BaseBlockMap.mu.RLock()
	vb.BaseBlockMap.lookup.scan(func(v BlockLookup) bool {
		if _, exists := seen[v.StartBlock]; !exists {
			baseGroup.blocks = append(baseGroup.blocks, v)
			seen[v.StartBlock] = struct{}{}
		}
		return true
	})
	vb.BaseBlockMap.mu.RUnlock()
	sources = append(sources, baseGroup)

	for _, anc := range vb.ancestors {
		g := sourceGroup{name: anc.sourceVolumeName, uuid: anc.sourceVolumeUUID, nameHash: anc.sourceVolumeNameHash}
		anc.blocks.mu.RLock()
		anc.blocks.lookup.scan(func(v BlockLookup) bool {
			if _, exists := seen[v.StartBlock]; !exists {
				g.blocks = append(g.blocks, v)
				seen[v.StartBlock] = struct{}{}
			}
			return true
		})
		anc.blocks.mu.RUnlock()
		sources = append(sources, g)
	}

	if len(sources) > math.MaxUint8 {
		return nil, fmt.Errorf("too many ancestor sources (%d > %d)", len(sources), math.MaxUint8)
	}

	var buf []byte
	buf = append(buf, flatSectionMagic[:]...)
	buf = append(buf, 1)                   // version
	buf = append(buf, uint8(len(sources))) //nolint:gosec // G115: bounded above
	for _, src := range sources {
		nameBytes := []byte(src.name)
		if len(nameBytes) > math.MaxUint16 {
			return nil, fmt.Errorf("source name too long (%d bytes)", len(nameBytes))
		}
		if len(src.blocks) > math.MaxUint32 {
			return nil, fmt.Errorf("source block count %d exceeds uint32 limit", len(src.blocks))
		}
		nameLenBuf := make([]byte, 2)
		binary.BigEndian.PutUint16(nameLenBuf, uint16(len(nameBytes))) //nolint:gosec // G115: bounded above
		buf = append(buf, nameLenBuf...)
		buf = append(buf, nameBytes...)
		buf = append(buf, src.uuid[:]...)
		buf = append(buf, src.nameHash[:]...)
		numBlocksBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(numBlocksBuf, uint32(len(src.blocks))) //nolint:gosec // G115: bounded above
		buf = append(buf, numBlocksBuf...)
		for i := range src.blocks {
			buf = append(buf, vb.writeBlockWalChunk(&src.blocks[i])...)
		}
	}
	return buf, nil
}

// parseFlatSection decodes the inherited-blocks section produced by buildFlatSection.
func parseFlatSection(data []byte) ([]InheritedLayer, error) {
	if len(data) < 6 {
		return nil, fmt.Errorf("flat section too short (%d bytes)", len(data))
	}
	if [4]byte(data[0:4]) != flatSectionMagic {
		return nil, fmt.Errorf("flat section magic mismatch")
	}
	// data[4] = version (reserved, must be 1)
	numSources := int(data[5])
	offset := 6

	layers := make([]InheritedLayer, 0, numSources)
	for i := range numSources {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("flat section: source %d: truncated name length", i)
		}
		nameLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+nameLen > len(data) {
			return nil, fmt.Errorf("flat section: source %d: truncated name", i)
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen
		if offset+4+32+4 > len(data) {
			return nil, fmt.Errorf("flat section: source %d: truncated identity", i)
		}
		var uuid [4]byte
		copy(uuid[:], data[offset:offset+4])
		offset += 4
		var nameHash [32]byte
		copy(nameHash[:], data[offset:offset+32])
		offset += 32
		numBlocks := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		bm := &BlocksToObject{}
		// Reuse a zero-value VB just to call readBlockWalChunk (pure function on data).
		var tmp VB
		for j := range numBlocks {
			if offset+blockWalChunkSize > len(data) {
				return nil, fmt.Errorf("flat section: source %d: block %d: truncated", i, j)
			}
			block, err := tmp.readBlockWalChunk(data[offset : offset+blockWalChunkSize])
			if err != nil {
				return nil, fmt.Errorf("flat section: source %d: block %d: %w", i, j, err)
			}
			bm.lookup.set(block)
			offset += blockWalChunkSize
		}
		layers = append(layers, InheritedLayer{
			Blocks:               bm,
			SourceVolumeName:     name,
			SourceVolumeUUID:     uuid,
			SourceVolumeNameHash: nameHash,
		})
	}
	return layers, nil
}
