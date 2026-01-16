package main

import (
	"C"
	"strconv"
	"unsafe"

	"libguestfs.org/nbdkit"
)
import (
	"fmt"
	"log/slog"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

var pluginName = "viperblock"

type ViperBlockPlugin struct {
	nbdkit.Plugin
}

type ViperBlockConnection struct {
	nbdkit.Connection
	vb *viperblock.VB
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

var disk []byte

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
	return nil
}

func (p *ViperBlockPlugin) GetReady() error {
	// Allocate the RAM disk.
	//disk = make([]byte, size)

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
	}

	slog.Info("Creating Viperblock backend with btype, config", cfg)
	vb, err := viperblock.New(&vbconfig, "s3", cfg)
	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not create Viperblock backend: %v", err)}
	}

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

	walNum = vb.WAL.WallNum.Add(1)
	slog.Info("Loaded WAL number", "walNum", walNum)

	// Open the chunk WAL
	err = vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume())))

	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not open WAL: %v", err)}
	}

	// Open the block to object WAL
	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume())))

	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not open Block WAL: %v", err)}
	}

	return &ViperBlockConnection{vb: vb}, nil
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
	if err != nil && err != viperblock.ZeroBlock {
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

	data := make([]byte, count)

	err := c.vb.Write(offset/uint64(c.vb.BlockSize), data)

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

	c.vb.Flush()

	err := c.vb.WriteWALToChunk(false)

	if err != nil {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not Write WAL to Chunk: %v", err)}
	}

	return nil

}

func (c *ViperBlockConnection) Close() {

	slog.Info("Close, flushing block state to disk")

	// Gracefully close VB and save state to disk
	err := c.vb.Close()

	if err != nil {
		slog.Error("Could not close VB", "err", err)
	}

	return
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
