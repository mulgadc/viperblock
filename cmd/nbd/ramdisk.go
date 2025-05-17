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
	"os"

	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
)

var pluginName = "ramdisk"

type RAMDiskPlugin struct {
	nbdkit.Plugin
}

type RAMDiskConnection struct {
	nbdkit.Connection
	vb viperblock.VB
}

var size uint64
var size_set = false
var disk []byte

var vb viperblock.VB

func (p *RAMDiskPlugin) Config(key string, value string) error {
	if key == "size" {
		var err error
		size, err = strconv.ParseUint(value, 0, 64)
		if err != nil {
			return err
		}
		size_set = true
		return nil
	} else {
		return nbdkit.PluginError{Errmsg: "unknown parameter"}
	}
}

func (p *RAMDiskPlugin) ConfigComplete() error {
	if !size_set {
		return nbdkit.PluginError{Errmsg: "size parameter is required"}
	}
	return nil
}

func (p *RAMDiskPlugin) GetReady() error {
	// Allocate the RAM disk.
	//disk = make([]byte, size)

	return nil
}

func (p *RAMDiskPlugin) Open(readonly bool) (nbdkit.ConnectionInterface, error) {

	cfg := s3.S3Config{
		VolumeName: "test",
		VolumeSize: size,
		Bucket:     "predastore",
		Region:     "ap-southeast-2",
		AccessKey:  "AKIAIOSFODNN7EXAMPLE",
		SecretKey:  "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Host:       "https://127.0.0.1:8443",
	}

	slog.Info("Creating Viperblock backend with btype, config", cfg)
	vb = *viperblock.New("s3", cfg)

	// Set 20% LRU memory from host
	vb.SetCacheSize(0, 20)

	// Generate a temp directory that ends in viperblock
	voldata := "/tmp/viperblock"

	// For testing, purge, and create

	os.RemoveAll(voldata)

	os.MkdirAll(voldata, 0755)

	vb.SetWALBaseDir(voldata)

	walNum := vb.WAL.WallNum.Add(1)

	err := vb.OpenWAL(fmt.Sprintf("%s/%s/wal.%08d.bin", voldata, vb.Backend.GetVolume(), walNum))

	if err != nil {
		return &RAMDiskConnection{}, nbdkit.PluginError{Errmsg: "Could not open WAL"}
	}

	// Initialize the backend
	err = vb.Backend.Init()
	if err != nil {
		return &RAMDiskConnection{}, nbdkit.PluginError{Errmsg: "Could not initialize backend"}
	}

	return &RAMDiskConnection{vb: vb}, nil
}

func (c *RAMDiskConnection) GetSize() (uint64, error) {
	return size, nil

}

// Clients are allowed to make multiple connections safely.
// TODO: confirm changes
func (c *RAMDiskConnection) CanMultiConn() (bool, error) {
	return false, nil

}

func (c *RAMDiskConnection) PRead(buf []byte, offset uint64, flags uint32) error {
	slog.Info("PREAD:", "offset", offset, "len", len(buf))
	data, err := c.vb.ReadAt(offset, uint64(len(buf)))
	if err != nil && err != viperblock.ZeroBlock {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not read data: %v", err)}
	}
	copy(buf, data)
	return nil
}

func (c *RAMDiskConnection) PRead2(buf []byte, offset uint64,
	flags uint32) error {

	data, err := c.vb.ReadAt(offset/uint64(c.vb.BlockSize), uint64(len(buf)))
	if err != nil && err != viperblock.ZeroBlock {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not read data: %v", err)}
	}

	copy(buf, data)
	return nil
}

// Note that CanWrite is required in golang plugins, otherwise PWrite
// will never be called.
func (c *RAMDiskConnection) CanWrite() (bool, error) {
	return true, nil
}

func (c *RAMDiskConnection) PWrite(buf []byte, offset uint64,
	flags uint32) error {

	slog.Info("PWRITE:", "len", len(buf), "offset", offset)

	data := make([]byte, len(buf))

	copy(data, buf)
	err := c.vb.Write(offset/uint64(c.vb.BlockSize), data)

	if err != nil {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not write data: %v", err)}
	}

	return nil
}

func (c *RAMDiskConnection) CanZero() (bool, error) {
	return true, nil
}

func (c *RAMDiskConnection) Zero(count uint32, offset uint64, flags uint32) error {

	slog.Info("ZERO:", "len", count, "offset", offset)

	data := make([]byte, count)

	err := c.vb.Write(offset/uint64(c.vb.BlockSize), data)

	if err != nil {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not write zero data: %v", err)}
	}

	return nil
}

func (c *RAMDiskConnection) CanTrim() (bool, error) {
	return true, nil
}

func (c *RAMDiskConnection) Trim(count uint32, offset uint64, flags uint32) error {

	slog.Info("TRIM:", "len", count, "offset", offset)

	return nil
}

// Note that CanFlush() is required in golang plugins that implement
// Flush(), otherwise Flush() will never be called.
func (c *RAMDiskConnection) CanFlush() (bool, error) {
	return true, nil
}

// This is only an example, but if this was a real plugin, because
// these disks are transient and deleted when the client exits, it
// would make no sense to implement a Flush() callback.
func (c *RAMDiskConnection) Flush(flags uint32) error {

	c.vb.SaveState("/tmp/viperblock/state.json")
	c.vb.SaveHotState("/tmp/viperblock/hotstate.json")
	c.vb.SaveBlockState("/tmp/viperblock/blockstate.json")

	c.vb.Flush()

	err := c.vb.WriteWALToChunk()

	if err != nil {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not Write WAL to Chunk: %v", err)}
	}

	return nil

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
	return nbdkit.PluginInitialize(pluginName, &RAMDiskPlugin{})
}

// This is never(?) called, but must exist.
func main() {}
