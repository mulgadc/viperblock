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
	CreatedAt        time.Time `json:"CreatedAt"`
}

// CreateSnapshot flushes all in-flight data and creates a frozen snapshot of
// the current block-to-object mapping. The snapshot is stored on the backend
// under {snapshotID}/. No data is copied -- the snapshot references the
// source volume's existing chunk files.
func (vb *VB) CreateSnapshot(snapshotID string) (*SnapshotState, error) {
	slog.Info("CreateSnapshot: flushing data", "snapshotID", snapshotID, "volume", vb.VolumeName)

	// 1. Flush hot writes to WAL, then persist WAL to chunk files on backend
	if err := vb.Flush(); err != nil {
		return nil, fmt.Errorf("snapshot flush failed: %w", err)
	}
	if err := vb.WriteWALToChunk(true); err != nil {
		return nil, fmt.Errorf("snapshot WAL-to-chunk failed: %w", err)
	}

	// 2. Serialize the current block-to-object map as the snapshot checkpoint
	vb.BlocksToObject.mu.RLock()
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

	// 4. Deserialize block lookup entries
	baseMap := &BlocksToObject{
		BlockLookup: make(map[uint64]BlockLookup),
	}

	offset := headerSize
	for offset < len(checkpoint) {
		if offset+26 > len(checkpoint) {
			break
		}
		block, err := vb.readBlockWalChunk(checkpoint[offset : offset+26])
		if err != nil {
			return nil, "", fmt.Errorf("failed to read snapshot block entry: %w", err)
		}
		baseMap.BlockLookup[block.StartBlock] = block
		offset += 26
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
