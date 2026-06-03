package main

import (
	"C"
	"strconv"
	"unsafe"

	"libguestfs.org/nbdkit"
)
import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"

	_ "github.com/mulgadc/viperblock/internal/fipsboot"
)

type ebsSnapshotRequest struct {
	Volume     string `json:"Volume"`
	SnapshotID string `json:"SnapshotID"`
}

type ebsSnapshotResponse struct {
	SnapshotID string `json:"SnapshotID"`
	Success    bool   `json:"Success"`
	Error      string `json:"Error,omitempty"`
}

var pluginName = "viperblock"

type ViperBlockPlugin struct {
	nbdkit.Plugin
}

type ViperBlockConnection struct {
	nbdkit.Connection
	vb           *viperblock.VB
	snapListener net.Listener
}

var size uint64

var volume string
var bucket string
var region string
var access_key string
var secret_key string
var host string
var base_dir string
var cache_size int = 20
var use_shardwal bool = false
var encryption_key_file string
var snapshot_socket string

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
	} else if key == "shardwal" {
		use_shardwal = value == "true" || value == "1"
		return nil
	} else if key == "encryption_key_file" {
		encryption_key_file = value
		return nil
	} else if key == "snapshot_socket" {
		snapshot_socket = value
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
		Cache: viperblock.Cache{
			Config: viperblock.CacheConfig{
				Size: cache_size,
			},
		},
		MasterKey:         loadedMasterKey,
		EncryptionEnabled: loadedMasterKey != nil,
	}

	slog.Info("Creating Viperblock backend with btype, config", cfg)
	vb, err := viperblock.New(&vbconfig, "s3", cfg)
	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not create Viperblock backend: %v", err)}
	}

	// Apply nbdkit config override for sharded WAL
	vb.UseShardedWAL = use_shardwal

	//vb.SetDebug(true)

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

	err = vb.LoadBlockState()

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

	conn := &ViperBlockConnection{vb: vb}

	if snapshot_socket != "" {
		ln, err := serveSnapshotSocket(vb, snapshot_socket)
		if err != nil {
			slog.Error("NBD plugin: failed to start snapshot socket", "volume", volume, "socket", snapshot_socket, "err", err)
		} else {
			conn.snapListener = ln
		}
	}

	return conn, nil
}

// serveSnapshotSocket starts a Unix domain socket listener so viperblockd can
// forward snapshot requests to this plugin process (which owns the WAL).
// The listener is returned so Close() can shut it down cleanly.
func serveSnapshotSocket(vb *viperblock.VB, socketPath string) (net.Listener, error) {
	_ = os.Remove(socketPath) // clean up stale socket from a previous run
	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen snapshot socket: %w", err)
	}
	slog.Info("NBD plugin: snapshot socket ready", "volume", vb.VolumeName, "socket", socketPath)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener was closed
			}
			go handleSnapshotConn(vb, conn)
		}
	}()
	return ln, nil
}

func handleSnapshotConn(vb *viperblock.VB, conn net.Conn) {
	defer conn.Close()
	var req ebsSnapshotRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		resp, _ := json.Marshal(ebsSnapshotResponse{Error: fmt.Sprintf("bad request: %v", err)})
		_, _ = conn.Write(resp)
		return
	}
	slog.Info("NBD plugin: handling snapshot request", "volume", vb.VolumeName, "snapshotId", req.SnapshotID)
	_, snapErr := vb.CreateSnapshot(req.SnapshotID)
	var resp ebsSnapshotResponse
	if snapErr != nil {
		slog.Error("NBD plugin: CreateSnapshot failed", "volume", vb.VolumeName, "snapshotId", req.SnapshotID, "err", snapErr)
		resp = ebsSnapshotResponse{SnapshotID: req.SnapshotID, Error: snapErr.Error()}
	} else {
		slog.Info("NBD plugin: snapshot created", "volume", vb.VolumeName, "snapshotId", req.SnapshotID)
		resp = ebsSnapshotResponse{SnapshotID: req.SnapshotID, Success: true}
	}
	data, _ := json.Marshal(resp)
	_, _ = conn.Write(data)
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
	slog.Info("PREAD:", "offset", offset, "len", len(buf))
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
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not write data: %v", err)}
	}

	return nil
}

func (c *ViperBlockConnection) CanZero() (bool, error) {
	return true, nil
}

func (c *ViperBlockConnection) Zero(count uint32, offset uint64, flags uint32) error {

	slog.Info("ZERO:", "len", count, "offset", offset)

	if count == 0 {
		return nil
	}

	data := make([]byte, count)

	err := c.vb.WriteAt(offset, data)

	if err != nil {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not write zero data: %v", err)}
	}

	return nil
}

func (c *ViperBlockConnection) CanTrim() (bool, error) {
	return true, nil
}

func (c *ViperBlockConnection) Trim(count uint32, offset uint64, flags uint32) error {

	slog.Info("TRIM:", "len", count, "offset", offset)

	return nil
}

// Note that CanFlush() is required in golang plugins that implement
// Flush(), otherwise Flush() will never be called.
func (c *ViperBlockConnection) CanFlush() (bool, error) {
	return true, nil
}

func (c *ViperBlockConnection) Flush(flags uint32) error {

	if err := c.vb.Flush(); err != nil {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Flush failed: %v", err)}
	}

	var err error
	if c.vb.UseShardedWAL {
		err = c.vb.WriteShardedWALToChunk(false)
	} else {
		err = c.vb.WriteWALToChunk(false)
	}

	if err != nil {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not Write WAL to Chunk: %v", err)}
	}

	return nil

}

func (c *ViperBlockConnection) Close() {

	slog.Info("Close, flushing block state to disk")

	if c.snapListener != nil {
		if err := c.snapListener.Close(); err != nil {
			slog.Error("NBD plugin: failed to close snapshot socket", "err", err)
		}
		_ = os.Remove(snapshot_socket)
	}

	// Gracefully close VB and save state to disk
	err := c.vb.Close()

	if err != nil {
		slog.Error("Could not close VB", "err", err)
	}
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
