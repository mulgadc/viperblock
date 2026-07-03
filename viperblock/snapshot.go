package viperblock

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/mulgadc/viperblock/types"
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
}

// CreateSnapshot flushes all in-flight data and creates a frozen snapshot of
// the current block-to-object mapping. The snapshot is stored on the backend
// under {snapshotID}/. No data is copied -- the snapshot references the
// source volume's existing chunk files.
func (vb *VB) CreateSnapshot(snapshotID string) (*SnapshotState, error) {
	slog.Info("CreateSnapshot: flushing data", "snapshotID", snapshotID, "volume", vb.VolumeName)

	// 1. Flush hot writes to WAL and persist to chunks.
	// Skip if this VB doesn't own the WAL files (e.g. viperblockd snapshot VB
	// where the NBD plugin process owns the WAL).
	if vb.ownsWAL() {
		vb.Writes.mu.Lock()
		var flushErr error
		if vb.UseShardedWAL {
			flushErr = vb.flushLockedSharded()
		} else {
			flushErr = vb.flushLocked()
		}
		vb.Writes.mu.Unlock()
		if flushErr != nil {
			return nil, fmt.Errorf("snapshot flush failed: %w", flushErr)
		}

		// Persist WAL to chunk files on backend (potentially slow S3 upload,
		// no need to hold the write lock — new writes go to the next WAL file)
		if err := vb.WriteWALToChunk(true); err != nil {
			return nil, fmt.Errorf("snapshot WAL-to-chunk failed: %w", err)
		}
	}

	// 2. Serialize the current block-to-object map as the snapshot checkpoint
	vb.BlocksToObject.mu.RLock()
	blockCount := uint64(len(vb.BlocksToObject.BlockLookup))
	checkpoint := vb.BlockToObjectWALHeader()
	for _, block := range vb.BlocksToObject.BlockLookup {
		checkpoint = append(checkpoint, vb.writeBlockWalChunk(&block)...)
	}
	vb.BlocksToObject.mu.RUnlock()

	// Write checkpoint to {snapshotID}/checkpoints/blocks.00000001.bin
	emptyHeaders := []byte{}
	if err := vb.Backend.WriteTo(snapshotID, types.FileTypeBlockCheckpoint, 0, &emptyHeaders, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to write snapshot checkpoint: %w", err)
	}

	// 3. Build and save snapshot metadata
	snap := &SnapshotState{
		SnapshotID:       snapshotID,
		SourceVolumeName: vb.VolumeName,
		SeqNum:           vb.SeqNum.Load(),
		ObjectNum:        vb.ObjectNum.Load(),
		BlockSize:        vb.BlockSize,
		ObjBlockSize:     vb.ObjBlockSize,
		BlockCount:       blockCount,
		CreatedAt:        time.Now(),
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

	slog.Info("CreateSnapshot: complete", "snapshotID", snapshotID, "sourceVolume", vb.VolumeName)
	return snap, nil
}

// SnapshotIdentity carries the source volume's encryption-identity fields
// recovered from SnapshotState so clone reads can reconstruct the source's
// nonce + AAD. Zero-valued on snapshots of unencrypted volumes; callers
// branch on vb.EncryptionEnabled rather than testing zero.
type SnapshotIdentity struct {
	SourceVolumeName     string
	SourceVolumeUUID     [4]byte
	SourceVolumeNameHash [32]byte
}

// LoadSnapshotBlockMap reads a snapshot's frozen block-to-object map from the
// backend and returns it along with the source volume's encryption identity.
// The returned BlocksToObject is intended to be used as a read-only base map
// for clone volumes.
func (vb *VB) LoadSnapshotBlockMap(snapshotID string) (*BlocksToObject, SnapshotIdentity, error) {
	var ident SnapshotIdentity

	// 1. Read snapshot config
	configData, err := vb.Backend.ReadFrom(snapshotID, types.FileTypeConfig, 0, 0, 0)
	if err != nil {
		return nil, ident, fmt.Errorf("failed to read snapshot config %s: %w", snapshotID, err)
	}

	// Encrypted snapshots wrap the JSON in a metaEnvelope. Two-stage parse:
	// split the envelope, peek the verbatim payload for SourceVolumeUUID +
	// SourceVolumeNameHash + StateSeqNum (needed to reconstruct nonce + AAD),
	// verify, then unmarshal the verified bytes into the full struct. The
	// snapshotID parameter is the trusted external identity bound into the
	// AAD — splicing in another snapshot's blob fails because the literal
	// "snap:"||snapshotID differs from the seal-time value.
	if vb.EncryptionEnabled {
		payload, tag, splitErr := splitEnvelope(configData)
		if splitErr != nil {
			return nil, ident, fmt.Errorf("%w: snapshot %s envelope: %w", ErrIntegrity, snapshotID, splitErr)
		}
		var peek struct {
			SourceVolumeUUID     string `json:"SourceVolumeUUID"`
			SourceVolumeNameHash string `json:"SourceVolumeNameHash"`
			StateSeqNum          uint64 `json:"StateSeqNum"`
		}
		if err := json.Unmarshal(payload, &peek); err != nil {
			return nil, ident, fmt.Errorf("%w: snapshot %s peek parse: %w", ErrIntegrity, snapshotID, err)
		}
		uuid, uuidErr := hex.DecodeString(peek.SourceVolumeUUID)
		if uuidErr != nil || len(uuid) != 4 {
			return nil, ident, fmt.Errorf("%w: snapshot %s SourceVolumeUUID invalid", ErrIntegrity, snapshotID)
		}
		nameHash, nhErr := hex.DecodeString(peek.SourceVolumeNameHash)
		if nhErr != nil || len(nameHash) != 32 {
			return nil, ident, fmt.Errorf("%w: snapshot %s SourceVolumeNameHash invalid", ErrIntegrity, snapshotID)
		}
		var nonceUUID [4]byte
		copy(nonceUUID[:], uuid)
		var aadNameHash [32]byte
		copy(aadNameHash[:], nameHash)
		nonce := makeNonce(peek.StateSeqNum, nonceUUID, DomainSnapshotMeta)
		aad := makeMetaAAD(aadNameHash, "snap:"+snapshotID, peek.StateSeqNum)
		if err := verifyMeta(vb.aead, payload, tag, aad, nonce); err != nil {
			return nil, ident, fmt.Errorf("%w: snapshot %s tag verify: %w", ErrIntegrity, snapshotID, err)
		}
		configData = payload
	}

	var snap SnapshotState
	if err := json.Unmarshal(configData, &snap); err != nil {
		return nil, ident, fmt.Errorf("failed to parse snapshot config %s: %w", snapshotID, err)
	}

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

	// 4. Validate checkpoint size: must contain only complete entries after header.
	dataSize := len(checkpoint) - headerSize
	if dataSize%blockWalChunkSize != 0 {
		return nil, ident, fmt.Errorf("snapshot checkpoint has %d trailing bytes (data section %d bytes is not a multiple of %d-byte entries)", dataSize%blockWalChunkSize, dataSize, blockWalChunkSize)
	}

	// 5. Deserialize block lookup entries
	baseMap := &BlocksToObject{
		BlockLookup: make(map[uint64]BlockLookup),
	}

	offset := headerSize
	for offset+blockWalChunkSize <= len(checkpoint) {
		block, err := vb.readBlockWalChunk(checkpoint[offset : offset+blockWalChunkSize])
		if err != nil {
			return nil, ident, fmt.Errorf("failed to read snapshot block entry at offset %d: %w", offset, err)
		}
		baseMap.BlockLookup[block.StartBlock] = block
		offset += blockWalChunkSize
	}

	// 6. Validate block count against metadata if available (BlockCount > 0 means
	// the snapshot was created with the block count field present)
	if snap.BlockCount > 0 && uint64(len(baseMap.BlockLookup)) != snap.BlockCount {
		return nil, ident, fmt.Errorf("snapshot block count mismatch: metadata says %d, checkpoint has %d", snap.BlockCount, len(baseMap.BlockLookup))
	}

	slog.Debug("LoadSnapshotBlockMap: loaded",
		"snapshotID", snapshotID,
		"sourceVolume", snap.SourceVolumeName,
		"blocks", len(baseMap.BlockLookup))

	return baseMap, ident, nil
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

	slog.Debug("OpenFromSnapshot: clone ready",
		"volume", vb.VolumeName,
		"snapshotID", snapshotID,
		"sourceVolume", ident.SourceVolumeName,
		"baseBlocks", len(baseMap.BlockLookup))

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
	lookup, ok := vb.BaseBlockMap.BlockLookup[block]
	vb.BaseBlockMap.mu.RUnlock()

	if !ok {
		return 0, 0, 0, ErrZeroBlock
	}
	return lookup.ObjectID, lookup.ObjectOffset, lookup.SeqNum, nil
}
