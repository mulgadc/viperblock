// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package memory

import (
	"errors"

	"github.com/mulgadc/viperblock/types"
)

var ErrMemoryBackendStub = errors.New("memory backend is a stub and does not store data")

type Backend struct{}

func (backend *Backend) Init() error                { return nil }
func (backend *Backend) Open(fname string) error    { return nil }
func (backend *Backend) Sync()                      {}
func (backend *Backend) GetBackendType() string     { return "memory" }
func (backend *Backend) GetHost() string            { return "" }
func (backend *Backend) SetConfig(config any)       {}

func (backend *Backend) Read(fileType types.FileType, objectId uint64, offset uint32, length uint32) ([]byte, error) {
	return nil, ErrMemoryBackendStub
}

func (backend *Backend) Write(fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	return ErrMemoryBackendStub
}

func (backend *Backend) ReadFrom(volumeName string, fileType types.FileType, objectId uint64, offset uint32, length uint32) ([]byte, error) {
	return nil, ErrMemoryBackendStub
}

func (backend *Backend) WriteTo(volumeName string, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	return ErrMemoryBackendStub
}
