package types

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// ErrNoSpace is returned by a Backend's Write/WriteCtx/WriteTo/WriteToCtx
// when the underlying storage is full, so callers can check for it uniformly
// via errors.Is regardless of backend. Lives here rather than in the
// viperblock package to avoid an import cycle (backends are imported by
// viperblock, so can't import it back).
var ErrNoSpace = errors.New("viperblock: backend out of space")

type Backend interface {
	Init() error
	InitCtx(ctx context.Context) error
	Open(fname string) error
	Read(fileType FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error)
	ReadCtx(ctx context.Context, fileType FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error)
	Write(fileType FileType, objectId uint64, headers *[]byte, data *[]byte) (err error)
	WriteCtx(ctx context.Context, fileType FileType, objectId uint64, headers *[]byte, data *[]byte) (err error)
	ReadFrom(volumeName string, fileType FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error)
	ReadFromCtx(ctx context.Context, volumeName string, fileType FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error)
	WriteTo(volumeName string, fileType FileType, objectId uint64, headers *[]byte, data *[]byte) (err error)
	WriteToCtx(ctx context.Context, volumeName string, fileType FileType, objectId uint64, headers *[]byte, data *[]byte) (err error)
	// Delete removes an object from this backend's own volume. There is
	// deliberately no cross-volume DeleteTo/DeleteFrom: deleters (chunk GC)
	// only ever remove objects they minted themselves.
	Delete(fileType FileType, objectId uint64) (err error)
	DeleteCtx(ctx context.Context, fileType FileType, objectId uint64) (err error)
	// ListPrefixes returns the top-level names under prefix, one level deep
	// (delimiter "/"), scoped to the whole backend rather than one volume.
	ListPrefixes(prefix string) (names []string, err error)
	ListPrefixesCtx(ctx context.Context, prefix string) (names []string, err error)
	// ListObjects returns every object's full key under prefix, recursively
	// (no delimiter) — unlike ListPrefixes, these are leaf keys, not grouped
	// path segments. Used to reconcile a volume's chunks/ directory against
	// its in-memory state.
	ListObjects(prefix string) (keys []string, err error)
	ListObjectsCtx(ctx context.Context, prefix string) (keys []string, err error)
	Sync()
	GetBackendType() string
	GetHost() string
	SetConfig(config any)
	// SetLogger installs the logger the backend uses for its own log lines.
	// Backends never call slog.SetDefault; the logger is scoped to the
	// instance so an embedding process's global logger is left untouched.
	SetLogger(logger *slog.Logger)
}

// FileType represents the type of file being written to S3.
type FileType int

const (
	FileTypeConfig FileType = iota
	FileTypeChunk
	FileTypeBlockCheckpoint
	FileTypeWALChunk
	FileTypeWALBlock
	FileTypeSSHAuthKey
	FileTypeWALChunkShard
	FileTypeBlockCheckpointLive
)

// getFilePath returns the appropriate S3 path based on file type and objectId.
func GetFilePath(fileType FileType, objectId uint64, volumeName string) string {
	switch fileType {
	case FileTypeConfig:
		return fmt.Sprintf("%s/config.json", volumeName)
	case FileTypeChunk:
		return fmt.Sprintf("%s/chunks/chunk.%08d.bin", volumeName, objectId)
	case FileTypeBlockCheckpoint:
		return fmt.Sprintf("%s/checkpoints/blocks.%08d.bin", volumeName, objectId)
	case FileTypeWALChunk:
		return fmt.Sprintf("%s/wal/chunks/wal.%08d.bin", volumeName, objectId)
	case FileTypeWALBlock:
		return fmt.Sprintf("%s/wal/blocks/blocks.%08d.bin", volumeName, objectId)
	case FileTypeWALChunkShard:
		// Shard ID is encoded in the low 8 bits of objectId, WAL number in the upper bits.
		// Use GetShardedWALPath for the explicit interface.
		shardID := objectId & 0xFF
		walNum := objectId >> 8
		return fmt.Sprintf("%s/wal/chunks/wal.%08d.shard_%02d.bin", volumeName, walNum, shardID)
	case FileTypeBlockCheckpointLive:
		return fmt.Sprintf("%s/checkpoints/blocks.live.bin", volumeName)
	default:
		return fmt.Sprintf("%s/unknown.%08d.bin", volumeName, objectId)
	}
}

// GetShardedWALPath returns the file path for a sharded WAL file.
// Files live in the same wal/chunks/ directory as legacy WAL files
// so recovery can discover them alongside legacy files.
func GetShardedWALPath(volumeName string, walNum uint64, shardID int) string {
	return fmt.Sprintf("%s/wal/chunks/wal.%08d.shard_%02d.bin", volumeName, walNum, shardID)
}

// Example directory layout
// /vol-id/config.json
// /vol-id/chunks/
// /vol-id/checkpoints/
// /vol-id/wal/chunks
// /vol-id/wal/block2object/

// config.json
// {
//  "BlockSize": 4096,
//  "ObjBlockSize": 4194304,
//  "SeqNum": 87,
//  "ObjectNum": 24,
//  "WALNum": 32
// }
