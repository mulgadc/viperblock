package main

import (
	"C"
	"strconv"
	"unsafe"

	"libguestfs.org/nbdkit"
)
import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"syscall"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/telemetry"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"

	_ "github.com/mulgadc/viperblock/internal/fipsboot"
)

var pluginName = "viperblock"

// serviceName identifies this process to OTLP (resource service.name) and
// to the otelslog bridge, matching the metrics-apm.app.<name> data stream
// dashboards are built against.
const serviceName = "viperblockd"

// otelShutdown flushes the OTLP exporters on Unload, if telemetry.Init
// configured real ones. nil (a no-op) when OTLP export is not configured.
var otelShutdown func(context.Context) error

type ViperBlockPlugin struct {
	nbdkit.Plugin
}

type ViperBlockConnection struct {
	nbdkit.Connection
	vb *viperblock.VB
}

// activeVB holds the VB for the current open connection. CanMultiConn returns
// false so at most one connection exists at a time. Unload uses this as a
// fallback flush when nbdkit exits without calling the per-connection Close
// (observed when the NBD client is killed while nbdkit is already in SIGTERM
// shutdown mode).
var activeVB *viperblock.VB

// snapshotListener is the unix socket that spinifex connects to before calling
// CreateSnapshot. The handler calls DrainToBackend and acks "OK\n", ensuring S3
// state is current without requiring every guest fsync to hit S3.
var snapshotListener net.Listener
var snapshotListenerDone chan struct{}

var size uint64

var volume string
var bucket string
var region string
var access_key string
var secret_key string
var host string
var base_dir string
var cache_size int = 20
var max_pending_bytes uint64 = 0
var upload_workers int = 16
var use_shardwal bool = false
var encryption_key_file string

// gc_enabled mirrors viperblock.VB.GCEnabled. Defaults false so chunk GC
// stays off until the spawning process explicitly opts in.
var gc_enabled bool = false

var disk []byte

// loadedMasterKey is populated once in ConfigComplete from
// -encryption-key-file / ENCRYPTION_KEY_FILE so per-connection Open does not
// re-read the key file on every NBD client attach. nil iff encryption is
// disabled for this plugin instance.
var loadedMasterKey *masterkey.Key

// NOTE: For profiling viperblock/nbdkit, use Linux perf instead of Go pprof.
// Go's pprof does NOT work correctly when running as a CGO shared library.
//
// Usage:
//   perf record -g -p $(pgrep -f nbdkit) -- sleep 60
//   perf report

func (p *ViperBlockPlugin) Config(key string, value string) error {

	// Config options
	// size (bytes)
	// volume (string)
	// bucket (string)

	if key == "size" {
		var err error
		size, err = strconv.ParseUint(value, 0, 64)
		if err != nil {
			return err
		}
		return nil
	} else if key == "volume" {
		volume = value
		return nil
	} else if key == "bucket" {
		bucket = value
		return nil
	} else if key == "region" {
		region = value
		return nil
	} else if key == "access_key" {
		access_key = value
		return nil
	} else if key == "secret_key" {
		secret_key = value
	} else if key == "host" {
		host = value
		return nil
	} else if key == "base_dir" {
		base_dir = value
		return nil
	} else if key == "cache_size" {
		var err error
		cache_size, err = strconv.Atoi(value)
		if err != nil {
			return err
		}
		return nil
	} else if key == "max_pending_bytes" {
		var err error
		max_pending_bytes, err = strconv.ParseUint(value, 0, 64)
		if err != nil {
			return err
		}
		return nil
	} else if key == "upload_workers" {
		var err error
		upload_workers, err = strconv.Atoi(value)
		if err != nil {
			return err
		}
		return nil
	} else if key == "shardwal" {
		use_shardwal = value == "true" || value == "1"
		return nil
	} else if key == "gc_enabled" {
		gc_enabled = value == "true" || value == "1"
		return nil
	} else if key == "encryption_key_file" {
		encryption_key_file = value
		return nil
	} else {
		return nbdkit.PluginError{Errmsg: "unknown parameter"}
	}

	return nil
}

func (p *ViperBlockPlugin) ConfigComplete() error {
	if size == 0 {
		return nbdkit.PluginError{Errmsg: "size parameter is required"}
	} else if volume == "" {
		return nbdkit.PluginError{Errmsg: "volume parameter is required"}
	} else if bucket == "" {
		return nbdkit.PluginError{Errmsg: "bucket parameter is required"}
	} else if region == "" {
		return nbdkit.PluginError{Errmsg: "region parameter is required"}
	} else if access_key == "" {
		return nbdkit.PluginError{Errmsg: "access_key parameter is required"}
	} else if secret_key == "" {
		return nbdkit.PluginError{Errmsg: "secret_key parameter is required"}
	} else if base_dir == "" {
		return nbdkit.PluginError{Errmsg: "base_dir parameter is required"}
	} else if host == "" {
		return nbdkit.PluginError{Errmsg: "host parameter is required"}
	}

	// Wire OTLP export + the slog bridge before anything else logs, so
	// standalone (nbdkit) logs reach ES via OTLP, not only journald. Both
	// are no-ops without OTEL_EXPORTER_OTLP_* configured. This is the only
	// place viperblock's process-wide slog default is touched — never from
	// viperblock.New, which would hijack an embedding process's logger.
	shutdown, err := telemetry.Init(context.Background(), serviceName)
	if err != nil {
		slog.Error("otel telemetry init failed, continuing without OTLP export", "err", err)
	} else {
		otelShutdown = shutdown
	}
	level := slog.LevelInfo
	if os.Getenv("VIPERBLOCK_DEBUG") == "1" {
		level = slog.LevelDebug
	}
	telemetry.SetDefaultJSONLogger(serviceName, level)

	// Resolve and load the master key once at plugin startup so per-NBD-
	// connection Open avoids a disk read + permission check on every attach.
	mkey, err := viperblock.LoadMasterKeyFromFlagOrEnv(encryption_key_file)
	if err != nil {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not load encryption key: %v", err)}
	}
	loadedMasterKey = mkey
	if use_shardwal && loadedMasterKey != nil {
		return nbdkit.PluginError{Errmsg: "shardwal is incompatible with encryption: sharded WAL is not supported on encrypted volumes"}
	}
	return nil
}

func (p *ViperBlockPlugin) GetReady() error {
	return nil
}

func (p *ViperBlockPlugin) Open(readonly bool) (nbdkit.ConnectionInterface, error) {

	cfg := s3.S3Config{
		VolumeName: volume,
		VolumeSize: size,
		Bucket:     bucket,
		Region:     region,
		AccessKey:  access_key,
		SecretKey:  secret_key,
		Host:       host,
	}

	vbconfig := viperblock.VB{
		VolumeName: volume,
		VolumeSize: size,
		BaseDir:    base_dir,
		// Data-path engine. Recorded on the volume-open metric so a volume
		// simultaneously held by a control-plane import is visible.
		Role: "nbdkit",
		Cache: viperblock.Cache{
			Config: viperblock.CacheConfig{
				Size: cache_size,
			},
		},
		MaxPendingBytes:   max_pending_bytes,
		UploadWorkers:     upload_workers,
		MasterKey:         loadedMasterKey,
		EncryptionEnabled: loadedMasterKey != nil,
		// Set here, not post-construction like UseShardedWAL below: New
		// copies GCEnabled once at construction time.
		GCEnabled: gc_enabled,
	}

	slog.Info("Creating Viperblock backend with btype, config", cfg)
	vb, err := viperblock.New(&vbconfig, "s3", cfg)
	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not create Viperblock backend: %v", err)}
	}

	// Apply nbdkit config override for sharded WAL
	vb.UseShardedWAL = use_shardwal

	// Debug logging is configured once for the whole process in
	// ConfigComplete via telemetry.SetDefaultJSONLogger, not per-connection.

	// Set 20% LRU memory from host
	//vb.SetCacheSize(0, 20)

	// Generate a temp directory that ends in viperblock
	//voldata := "/tmp/viperblock"

	// Initialize the backend
	err = vb.Backend.Init()
	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: "Could not initialize backend"}
	}

	var walNum uint64

	// First, fetch the state from the remote backend
	err = vb.LoadState()

	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not load state: %v", err)}
	}

	// Mint + persist VolumeUUID now, single-threaded, before WAL recovery or any
	// served write seals a block. A lazy mint under concurrent boot load lets a
	// drain read a half-written UUID (or seal recovery chunks under the zero
	// UUID), corrupting a block with a durable AEAD tag failure.
	if err = vb.EnsureVolumeUUID(); err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not mint volume UUID: %v", err)}
	}

	// Load the LIVE checkpoint, not the numbered one. Runtime drains rewrite the
	// live checkpoint on every chunk upload but only bump the numbered checkpoint
	// on a clean Close — which a killed nbdkit VB (qemu reconnect) never reaches.
	// Loading the stale numbered map here would reconcile ObjectNum too low, so
	// recovery/drains would reuse a live chunk ID and overwrite it, leaving a
	// durable AEAD tag failure. LoadLiveCheckpoint falls back to the numbered
	// checkpoint when no live one exists yet (brand-new volume).
	err = vb.LoadLiveCheckpoint()

	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not load block state: %v", err)}
	}

	err = vb.RecoverLocalWALs()
	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not recover local WALs: %v", err)}
	}

	walNum = vb.WAL.WallNum.Add(1)
	slog.Info("Loaded WAL number", "walNum", walNum)

	// Open the chunk WAL (sharded or legacy)
	if vb.UseShardedWAL {
		vb.ShardedWAL = viperblock.NewShardedWAL(vb.WAL.BaseDir, vb.WAL.WALMagic)
		vb.ShardedWAL.WallNum.Store(vb.WAL.WallNum.Load())
		err = vb.OpenShardedWAL()
	} else {
		err = vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume())))
	}

	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not open WAL: %v", err)}
	}

	// Open the block to object WAL
	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume())))

	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not open Block WAL: %v", err)}
	}

	activeVB = vb
	snapshotListenerDone = startSnapshotListener(vb, filepath.Join(base_dir, volume, "snapshot.sock"))
	return &ViperBlockConnection{vb: vb}, nil
}

// startSnapshotListener opens a unix socket that spinifex connects to before
// calling CreateSnapshot. The handler calls DrainToBackend — flushing WAL
// chunks and the live checkpoint to S3 — then acks "OK\n". Returns a channel
// closed when the goroutine exits.
func startSnapshotListener(vb *viperblock.VB, sockPath string) chan struct{} {
	done := make(chan struct{})

	// Remove any stale socket file from a previous run
	_ = os.Remove(sockPath)

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		slog.Error("snapshot socket: listen failed", "path", sockPath, "err", err)
		close(done)
		return done
	}
	snapshotListener = ln

	go func() {
		defer close(done)
		defer os.Remove(sockPath)
		for {
			conn, err := ln.Accept()
			if err != nil {
				// Listener closed — normal shutdown path
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if err := vb.DrainToBackend(); err != nil {
					slog.Error("snapshot socket: DrainToBackend failed", "err", err)
					_, _ = c.Write([]byte("ERR\n"))
					return
				}
				_, _ = c.Write([]byte("OK\n"))
			}(conn)
		}
	}()

	slog.Info("snapshot socket listening", "path", sockPath)
	return done
}

// stopSnapshotListener closes the unix socket and waits for the goroutine to exit.
func stopSnapshotListener() {
	if snapshotListener == nil {
		return
	}
	snapshotListener.Close()
	if snapshotListenerDone != nil {
		<-snapshotListenerDone
	}
	snapshotListener = nil
	snapshotListenerDone = nil
}

// backendErrToPluginError wraps a viperblock write-path error as an
// nbdkit.PluginError, mapping viperblock.ErrNoSpace onto ENOSPC so the guest
// kernel reports "no space left on device" instead of a generic I/O error.
func backendErrToPluginError(msgPrefix string, err error) nbdkit.PluginError {
	perr := nbdkit.PluginError{Errmsg: fmt.Sprintf("%s: %v", msgPrefix, err)}
	if errors.Is(err, viperblock.ErrNoSpace) {
		perr.Errno = syscall.ENOSPC
	}
	return perr
}

func (c *ViperBlockConnection) GetSize() (uint64, error) {
	return c.vb.GetVolumeSize(), nil

}

// Clients are allowed to make multiple connections safely.
// TODO: confirm changes
func (c *ViperBlockConnection) CanMultiConn() (bool, error) {
	return false, nil

}

func (c *ViperBlockConnection) PRead(buf []byte, offset uint64, flags uint32) error {
	slog.Debug("PREAD:", "offset", offset, "len", len(buf))
	data, err := c.vb.ReadAt(offset, uint64(len(buf)))
	if err != nil && err != viperblock.ErrZeroBlock {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not read data: %v", err)}
	}

	copy(buf, data)
	return nil
}

// Note that CanWrite is required in golang plugins, otherwise PWrite
// will never be called.
func (c *ViperBlockConnection) CanWrite() (bool, error) {
	return true, nil
}

func (c *ViperBlockConnection) PWrite(buf []byte, offset uint64,
	flags uint32) error {

	//slog.Info("PWRITE:", "len", len(buf), "offset", offset)

	data := make([]byte, len(buf))

	copy(data, buf)
	err := c.vb.WriteAt(offset, data)

	if err != nil {
		return backendErrToPluginError("Could not write data", err)
	}

	return nil
}

func (c *ViperBlockConnection) CanZero() (bool, error) {
	return true, nil
}

func (c *ViperBlockConnection) Zero(count uint32, offset uint64, flags uint32) error {

	slog.Debug("ZERO:", "len", count, "offset", offset)

	if count == 0 {
		return nil
	}

	data := make([]byte, count)

	err := c.vb.WriteAt(offset, data)

	if err != nil {
		return backendErrToPluginError("Could not write zero data", err)
	}

	return nil
}

func (c *ViperBlockConnection) CanTrim() (bool, error) {
	return true, nil
}

func (c *ViperBlockConnection) Trim(count uint32, offset uint64, flags uint32) error {

	slog.Debug("TRIM:", "len", count, "offset", offset)

	return nil
}

// Note that CanFlush() is required in golang plugins that implement
// Flush(), otherwise Flush() will never be called.
func (c *ViperBlockConnection) CanFlush() (bool, error) {
	return true, nil
}

func (c *ViperBlockConnection) Flush(flags uint32) error {
	if err := c.vb.Flush(); err != nil {
		return backendErrToPluginError("Flush failed", err)
	}
	return nil
}

func (c *ViperBlockConnection) Close() {
	slog.Info("Close, flushing block state to disk")

	stopSnapshotListener()

	// DrainToBackend before Close so the live checkpoint is current. Close
	// will also flush WAL chunks, but does not save the live checkpoint.
	if err := c.vb.DrainToBackend(); err != nil {
		slog.Error("Close: DrainToBackend failed", "err", err)
	}

	if err := c.vb.Close(); err != nil {
		slog.Error("Could not close VB", "err", err)
	}
	activeVB = nil
}

// Unload is called by nbdkit immediately before the process exits. It flushes
// and saves block state if Close was not called on the active connection — this
// happens when the NBD client is killed abruptly while nbdkit is already in
// SIGTERM shutdown mode, which causes nbdkit to skip the per-connection close
// callback.
func (p *ViperBlockPlugin) Unload() {
	defer func() {
		if otelShutdown == nil {
			return
		}
		if err := otelShutdown(context.Background()); err != nil {
			slog.Error("Unload: otel shutdown failed", "err", err)
		}
	}()

	if activeVB == nil {
		return
	}
	slog.Info("Unload: Close was not called, flushing block state now")
	stopSnapshotListener()
	if err := activeVB.DrainToBackend(); err != nil {
		slog.Error("Unload: DrainToBackend failed", "err", err)
	}
	if err := activeVB.Close(); err != nil {
		slog.Error("Unload: could not close VB", "err", err)
	}
	activeVB = nil
}

//----------------------------------------------------------------------
//
// The boilerplate below this line is required by all golang plugins,
// as well as importing "C" and "unsafe" modules at the top of the
// file.

//export plugin_init
func plugin_init() unsafe.Pointer {
	// If your plugin needs to do any initialization, you can
	// either put it here or implement a Load() method.
	// ...

	// Then you must call the following function.
	return nbdkit.PluginInitialize(pluginName, &ViperBlockPlugin{})
}

// This is never(?) called, but must exist.
func main() {}
