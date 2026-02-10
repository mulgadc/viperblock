// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package viperblock

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/mulgadc/viperblock/types"
)

// SnapshotState holds metadata for a frozen snapshot stored on the backend.
type SnapshotState struct {
	SnapshotID       string    `json:"SnapshotID"`
	SourceVolumeName string    `json:"SourceVolumeName"`
	SeqNum           uint64    `json:"SeqNum"`
	ObjectNum        uint64    `json:"ObjectNum"`
	BlockSize        uint32    `json:"BlockSize"`
	ObjBlockSize     uint32    `json:"ObjBlockSize"`
	BlockCount       uint64    `json:"BlockCount"`
	CreatedAt        time.Time `json:"CreatedAt"`
}

// CreateSnapshot flushes all in-flight data and creates a frozen snapshot of
// the current block-to-object mapping. The snapshot is stored on the backend
// under {snapshotID}/. No data is copied -- the snapshot references the
// source volume's existing chunk files.
func (vb *VB) CreateSnapshot(snapshotID string) (*SnapshotState, error) {
	slog.Info("CreateSnapshot: flushing data", "snapshotID", snapshotID, "volume", vb.VolumeName)

	// Hold the write lock for the entire snapshot operation to prevent new
	// writes from arriving between flush and block map serialization.
	vb.Writes.mu.Lock()
	defer vb.Writes.mu.Unlock()

	// 1. Flush hot writes to WAL, then persist WAL to chunk files on backend
	if err := vb.flushLocked(); err != nil {
		return nil, fmt.Errorf("snapshot flush failed: %w", err)
	}
	if err := vb.WriteWALToChunk(true); err != nil {
		return nil, fmt.Errorf("snapshot WAL-to-chunk failed: %w", err)
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

	snapJSON, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshot state: %w", err)
	}

	if err := vb.Backend.WriteTo(snapshotID, types.FileTypeConfig, 0, &emptyHeaders, &snapJSON); err != nil {
		return nil, fmt.Errorf("failed to write snapshot config: %w", err)
	}

	slog.Info("CreateSnapshot: complete", "snapshotID", snapshotID, "sourceVolume", vb.VolumeName)
	return snap, nil
}

// LoadSnapshotBlockMap reads a snapshot's frozen block-to-object map from the
// backend and returns it along with the source volume name. The returned
// BlocksToObject is intended to be used as a read-only base map for clone
// volumes.
func (vb *VB) LoadSnapshotBlockMap(snapshotID string) (*BlocksToObject, string, error) {
	// 1. Read snapshot config
	configData, err := vb.Backend.ReadFrom(snapshotID, types.FileTypeConfig, 0, 0, 0)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read snapshot config %s: %w", snapshotID, err)
	}

	var snap SnapshotState
	if err := json.Unmarshal(configData, &snap); err != nil {
		return nil, "", fmt.Errorf("failed to parse snapshot config %s: %w", snapshotID, err)
	}

	// 2. Read the checkpoint
	checkpoint, err := vb.Backend.ReadFrom(snapshotID, types.FileTypeBlockCheckpoint, 0, 0, 0)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read snapshot checkpoint %s: %w", snapshotID, err)
	}

	// 3. Validate header
	headerSize := vb.BlockToObjectWALHeaderSize()
	if len(checkpoint) < headerSize {
		return nil, "", fmt.Errorf("snapshot checkpoint too small: %d bytes", len(checkpoint))
	}

	headers := checkpoint[:headerSize]
	if !bytes.Equal(headers[:4], vb.BlockToObjectWAL.WALMagic[:]) {
		return nil, "", fmt.Errorf("snapshot checkpoint magic mismatch")
	}
	if binary.BigEndian.Uint16(headers[4:6]) != vb.Version {
		return nil, "", fmt.Errorf("snapshot checkpoint version mismatch")
	}

	// 4. Validate checkpoint size: must contain only complete 26-byte entries after header
	dataSize := len(checkpoint) - headerSize
	if dataSize%26 != 0 {
		return nil, "", fmt.Errorf("snapshot checkpoint has %d trailing bytes (data section %d bytes is not a multiple of 26-byte entries)", dataSize%26, dataSize)
	}

	// 5. Deserialize block lookup entries
	baseMap := &BlocksToObject{
		BlockLookup: make(map[uint64]BlockLookup),
	}

	offset := headerSize
	for offset+26 <= len(checkpoint) {
		block, err := vb.readBlockWalChunk(checkpoint[offset : offset+26])
		if err != nil {
			return nil, "", fmt.Errorf("failed to read snapshot block entry at offset %d: %w", offset, err)
		}
		baseMap.BlockLookup[block.StartBlock] = block
		offset += 26
	}

	// 6. Validate block count against metadata if available (BlockCount > 0 means
	// the snapshot was created with the block count field present)
	if snap.BlockCount > 0 && uint64(len(baseMap.BlockLookup)) != snap.BlockCount {
		return nil, "", fmt.Errorf("snapshot block count mismatch: metadata says %d, checkpoint has %d", snap.BlockCount, len(baseMap.BlockLookup))
	}

	slog.Info("LoadSnapshotBlockMap: loaded",
		"snapshotID", snapshotID,
		"sourceVolume", snap.SourceVolumeName,
		"blocks", len(baseMap.BlockLookup))

	return baseMap, snap.SourceVolumeName, nil
}

// OpenFromSnapshot loads the base block map from a snapshot and configures
// this volume for copy-on-write reads. After calling this, reads that miss
// in the volume's own block map will fall through to the snapshot's frozen
// map and read chunk data from the source volume.
func (vb *VB) OpenFromSnapshot(snapshotID string) error {
	baseMap, sourceVolume, err := vb.LoadSnapshotBlockMap(snapshotID)
	if err != nil {
		return err
	}

	if sourceVolume == "" {
		return fmt.Errorf("snapshot %s has empty SourceVolumeName", snapshotID)
	}

	vb.BaseBlockMap = baseMap
	vb.SourceVolumeName = sourceVolume
	vb.SnapshotID = snapshotID

	slog.Info("OpenFromSnapshot: clone ready",
		"volume", vb.VolumeName,
		"snapshotID", snapshotID,
		"sourceVolume", sourceVolume,
		"baseBlocks", len(baseMap.BlockLookup))

	return nil
}

// LookupBaseBlockToObject looks up a block in the base (snapshot) block map.
// Returns ErrZeroBlock if the block was never written in the snapshot either.
func (vb *VB) LookupBaseBlockToObject(block uint64) (uint64, uint32, error) {
	if vb.BaseBlockMap == nil {
		return 0, 0, ErrZeroBlock
	}

	vb.BaseBlockMap.mu.RLock()
	lookup, ok := vb.BaseBlockMap.BlockLookup[block]
	vb.BaseBlockMap.mu.RUnlock()

	if !ok {
		return 0, 0, ErrZeroBlock
	}
	return lookup.ObjectID, lookup.ObjectOffset, nil
}
