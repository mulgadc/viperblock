// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package types

import "fmt"

type Backend interface {
	Init() error
	Open(fname string) error
	Read(fileType FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error)
	Write(fileType FileType, objectId uint64, headers *[]byte, data *[]byte) (err error)
	Sync()
	GetBackendType() string
	GetHost() string
	SetConfig(config any)
}

// FileType represents the type of file being written to S3
type FileType int

const (
	FileTypeConfig FileType = iota
	FileTypeChunk
	FileTypeBlockCheckpoint
	FileTypeWALChunk
	FileTypeWALBlock
	FileTypeSSHAuthKey
)

// getFilePath returns the appropriate S3 path based on file type and objectId
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
	default:
		return fmt.Sprintf("%s/unknown.%08d.bin", volumeName, objectId)
	}
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
