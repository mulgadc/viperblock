// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package types

type Backend interface {
	Init() error
	Open(fname string) error
	Read(objectId uint64, offset uint32, length uint32) (data []byte, err error)
	Write(objectId uint64, headers *[]byte, data *[]byte) (err error)
	Sync()
	GetVolumeSize() uint64
	GetVolume() string
	GetBackendType() string
	GetHost() string
	SetConfig(config interface{})
}
