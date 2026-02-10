// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package file

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/utils"
)

// 2. Define config structs
type FileConfig struct {
	VolumeName string
	VolumeSize uint64
	BaseDir    string
}

type FileBackend struct {
	config FileConfig
}

type Backend struct {
	FileBackend
}

// 3. Implement WithConfig for each backend
func New(config any) (backend *Backend) {

	return &Backend{FileBackend: FileBackend{config: config.(FileConfig)}}

}

func (backend *Backend) Init() error {

	slog.Info("Init for file backend", "volumeName", backend.config.VolumeName)

	// Check if the directory exists
	if _, err := os.Stat(backend.config.BaseDir); os.IsNotExist(err) {
		slog.Error("Directory does not exist", "error", err)
		return err
	}

	dirPath := fmt.Sprintf("%s/%s", backend.config.BaseDir, backend.config.VolumeName)

	// Create the directory structure
	err := os.MkdirAll(dirPath, 0755)

	if err != nil {
		slog.Error("Failed to create directory", "error", err)
		return err
	}

	// Create the directory structure

	dirs := []string{
		"chunks",
		"checkpoints",
		"wal",
		"wal/chunks",
		"wal/blocks",
	}

	for _, dir := range dirs {
		dirPath := fmt.Sprintf("%s/%s/%s", backend.config.BaseDir, backend.config.VolumeName, dir)
		// Create all parent dirs
		err := os.MkdirAll(dirPath, 0755)
		if err != nil {
			slog.Error("Failed to create directory", "error", err)
			return err
		}
	}

	return nil

}

func (backend *Backend) Open(fname string) (err error) {

	/*
		backend.filename, err = os.Open(fname)

		if err != nil {
			return
		}

		fmt.Println("Open specified file")
	*/
	return

}

func (backend *Backend) Read(fileType types.FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error) {

	// Open the specified file
	//filename := fmt.Sprintf("%s/%s/chunk.%08d.bin", backend.config.BaseDir, backend.config.VolumeName, objectId)
	filename := fmt.Sprintf("%s/%s", backend.config.BaseDir, types.GetFilePath(fileType, objectId, backend.config.VolumeName))

	f, err := os.OpenFile(filename, os.O_RDONLY, 0640)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	// If the length is undefined, read the entire file (e.g config file, or block2object state)
	if length == 0 {
		stat, err := os.Stat(filename)
		if err != nil {
			return nil, err
		}
		length = utils.SafeInt64ToUint32(stat.Size())
	}

	// Read the specified block for the length
	data = make([]byte, length)
	_, err = f.ReadAt(data, int64(offset))

	if err != nil {
		return nil, err
	}

	return data, nil

}

func (backend *Backend) Write(fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) (err error) {

	// TODO Improve
	//filename := fmt.Sprintf("%s/%s/chunk.%08d.bin", backend.config.BaseDir, backend.config.VolumeName, objectId)

	filename := fmt.Sprintf("%s/%s", backend.config.BaseDir, types.GetFilePath(fileType, objectId, backend.config.VolumeName))
	//fmt.Println("CREATING CHUNK FILE:", filename)

	file, err := os.Create(filename)

	if err != nil {
		slog.Error("Failed to create chunk file", "error", err)
		return err
	}

	//fmt.Println("Writing headers & block")
	_, err = file.Write(*headers)

	if err != nil {
		slog.Error("Failed to write headers", "error", err)
		return err
	}

	_, err = file.Write(*data)

	if err != nil {
		slog.Error("Failed to write data", "error", err)
		return err
	}

	err = file.Close()

	if err != nil {
		slog.Error("Failed to close file", "error", err)
		return err
	}

	return nil

}

func (backend *Backend) Sync() {

	//fmt.Println("Syncing block")

}

func (backend *Backend) GetBackendType() string {
	return "file"
}

func (backend *Backend) SetConfig(config any) {
	backend.config = config.(FileConfig)
}

func (backend *Backend) ReadFrom(volumeName string, fileType types.FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error) {
	filename := fmt.Sprintf("%s/%s", backend.config.BaseDir, types.GetFilePath(fileType, objectId, volumeName))

	f, err := os.OpenFile(filename, os.O_RDONLY, 0640)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if length == 0 {
		stat, err := os.Stat(filename)
		if err != nil {
			return nil, err
		}
		length = utils.SafeInt64ToUint32(stat.Size())
	}

	data = make([]byte, length)
	_, err = f.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (backend *Backend) WriteTo(volumeName string, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) (err error) {
	filename := fmt.Sprintf("%s/%s", backend.config.BaseDir, types.GetFilePath(fileType, objectId, volumeName))

	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return fmt.Errorf("failed to create directory for %s: %w", filename, err)
	}

	file, err := os.Create(filename)
	if err != nil {
		slog.Error("Failed to create file", "error", err)
		return err
	}

	if headers != nil && len(*headers) > 0 {
		if _, err = file.Write(*headers); err != nil {
			file.Close()
			return err
		}
	}

	if _, err = file.Write(*data); err != nil {
		file.Close()
		return err
	}

	return file.Close()
}

func (backend *Backend) GetHost() string {
	return ""
}
