// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package backend

import (
	"reflect"

	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/memory"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

var backendTypes = map[string]reflect.Type{
	"file":   reflect.TypeOf(file.FileBackend{}),
	"memory": reflect.TypeOf(memory.Backend{}),
	"s3":     reflect.TypeOf(s3.Backend{}),
}
