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

	// Check
	vb.SetCacheSize(1024, 0)

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
	/*
		buf, err := c.vb.Read(offset/uint64(c.vb.BlockSize), uint64(len(buf)))

		if err != nil && err != viperblock.ZeroBlock {
			return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not read data: %v", err)}
		}

	*/

	/*
		// Get the len of buf, divide by 4096 to get the number of chunks
		bufLen := len(buf) / 4096

		// Read each chunk and append to the buf for the next 4096 block, incrementing the offset each time
		for i := 0; i < bufLen; i++ {
			data, err := c.vb.Read(offset + uint64(i)*4096)
			if err != nil {
				return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not read data: %v", err)}
			}

			// Only copy the current 4096 chunk
			copy(buf[i*4096:i*4096+4096], data)
		}

		//slog.Info("PREAD:", "offset", offset, "len", len(buf), "data", data)

		//copy(buf, data)

		//copy(buf, disk[offset:int(offset)+len(buf)])
	*/

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

	/*
		slog.Info("FLUSHING")

		err = c.vb.Flush()

		if err != nil {
			return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not flush data: %v", err)}
		}
	*/

	/*
		//, "data", buf)

		// Split buffer write into 4096 chunks and increment the offset
		for i := 0; i < len(buf); i += 4096 {
			slog.Error("PWRITE:", "offset", offset+uint64(i), "len", 4096, "data", buf[i:i+4096])
			err := c.vb.Write((offset/4096)+uint64(i), buf[i:i+4096])
			if err != nil {
				return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not write data: %v", err)}
			}

		}

		//	vb.Write(offset/4096, buf)

		slog.Error("FLUSHING")

		err := c.vb.Flush()

		if err != nil {
			return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not flush data: %v", err)}
		}

		//copy(disk[offset:int(offset)+len(buf)], buf)
	*/
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

	//c.vb.SaveState("/tmp/viperblock/state.json")
	//c.vb.SaveHotState("/tmp/viperblock/hotstate.json")
	//c.vb.SaveBlockState("/tmp/viperblock/blockstate.json")

	slog.Error("Flush called")

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
