//go:build nbdkit_keep

// Keeps libguestfs.org/nbdkit pinned in the root go.mod. The NBD plugin
// (nbd/viperblock.go) is built via `go build nbd/viperblock.go` from the
// viperblock root, which resolves through this module — but it lives behind
// nbd/go.mod, so `go mod tidy` can't see the import and would otherwise
// strip the require/replace on every Dependabot run.
//
// The nbdkit_keep tag is never set, so this file never compiles (no cgo /
// pkg-config nbdkit needed). `go mod tidy` walks all build tags since Go
// 1.17, so the import is still seen and the require is preserved.
package viperblock

import _ "libguestfs.org/nbdkit"
