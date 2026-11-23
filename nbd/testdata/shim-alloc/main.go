package main

import "C"
import (
	"strings"
	"syscall"
	"unsafe"

	"libguestfs.org/nbdkit"
)

type probePlugin struct{ nbdkit.Plugin }
type probeConnection struct{ nbdkit.Connection }

var message = strings.Repeat("recorded shim allocation during local test. ", 25)

func (*probePlugin) Open(bool) (nbdkit.ConnectionInterface, error) {
	return &probeConnection{}, nil
}

func (*probeConnection) GetSize() (uint64, error) { return 8192, nil }

func (*probeConnection) PRead(buf []byte, offset uint64, flags uint32) error {
	if offset == 0 {
		return nbdkit.PluginError{Errmsg: message, Errno: syscall.EIO}
	}
	nbdkit.Debug(message)
	clear(buf)
	return nil
}

//export plugin_init
func plugin_init() unsafe.Pointer { return nbdkit.PluginInitialize("shimprobe", &probePlugin{}) }

func main() {}
