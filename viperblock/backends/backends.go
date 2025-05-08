// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package backend

import (
	"fmt"
	"reflect"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/memory"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

// Configuration structs for each backend
type FileConfig struct {
	BaseDir    string
	VolumeName string
}

type S3Config struct {
	Host      string
	AccessKey string
	Secret    string
	Bucket    string
}

var backendTypes = map[string]reflect.Type{
	"file":   reflect.TypeOf(file.FileBackend{}),
	"memory": reflect.TypeOf(memory.Backend{}),
	"s3":     reflect.TypeOf(s3.Backend{}),
}

func New(btype string, config interface{}) (backend types.Backend, err error) {

	/*
		if backendTypes[btype] == nil {
			return nil, errors.New("invalid backend")
		}

		T := backendTypes[btype]

		backend = reflect.New(T).Interface().(types.Backend)
		backend.WithConfig(config)
	*/

	switch btype {
	case "file":
		if cfg, ok := config.(file.FileConfig); ok {
			fmt.Println("cfg", cfg)
			//return file.New(cfg), nil

			//return file.New(cfg), nil
		}
	case "s3":
		//if cfg, ok := config.(S3Config); ok {
		//return s3.NewBackend(cfg), nil
		//}
	}

	return backend, nil

}
