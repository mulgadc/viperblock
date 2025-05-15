// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package file

import (
	"fmt"
	"log/slog"
	"os"
)

// 2. Define config structs
type FileConfig struct {
	BaseDir    string
	VolumeName string
	VolumeSize uint64
}

type FileBackend struct {
	config FileConfig
}

type Backend struct {
	FileBackend
}

// 3. Implement WithConfig for each backend
func New(config interface{}) (backend *Backend) {

	return &Backend{FileBackend: FileBackend{config: config.(FileConfig)}}

}

func (backend *Backend) Init() error {

	slog.Info("Init for file backend", "volumeName", backend.config.VolumeName)

	// Check if the directory exists
	if _, err := os.Stat(backend.config.BaseDir); os.IsNotExist(err) {
		slog.Error("Directory does not exist", "error", err)
		return err
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

func (backend *Backend) Read(objectId uint64, offset uint32, length uint32) (data []byte, err error) {

	// Open the specified file
	filename := fmt.Sprintf("%s/%s/chunk.%08d.bin", backend.config.BaseDir, backend.config.VolumeName, objectId)
	f, err := os.OpenFile(filename, os.O_RDONLY, 0640)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	// Read the block
	data = make([]byte, length)
	_, err = f.ReadAt(data, int64(offset))

	if err != nil {
		return nil, err
	}

	//fmt.Println("Reading block")

	return data, nil

}

func (backend *Backend) Write(objectId uint64, headers *[]byte, data *[]byte) (err error) {

	// TODO Improve
	filename := fmt.Sprintf("%s/%s/chunk.%08d.bin", backend.config.BaseDir, backend.config.VolumeName, objectId)

	//fmt.Println("CREATING CHUNK FILE:", filename)

	file, err := os.Create(filename)

	if err != nil {
		slog.Error("Failed to create chunk file", "error", err)
		return err
	}

	//fmt.Println("Writing headers & block")
	file.Write(*headers)
	file.Write(*data)

	file.Close()

	return nil

}

func (backend *Backend) Sync() {

	//fmt.Println("Syncing block")

}

func (backend *Backend) GetVolume() string {
	return backend.config.VolumeName
}

func (backend *Backend) GetVolumeSize() uint64 {
	return backend.config.VolumeSize
}

func (backend *Backend) GetBackendType() string {
	return "file"
}

func (backend *Backend) SetConfig(config interface{}) {
	backend.config = config.(FileConfig)
}

func (backend *Backend) GetHost() string {
	return ""
}
