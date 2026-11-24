package main

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func BenchmarkShimCallbackError(b *testing.B) {
	plugin := os.Getenv("SHIM_PROBE_PLUGIN")
	if plugin == "" {
		b.Skip("set SHIM_PROBE_PLUGIN to the built shimprobe plugin")
	}
	server := os.Getenv("NBDKIT")
	if server == "" {
		server = "/usr/bin/nbdkit"
	}
	socket := filepath.Join(b.TempDir(), "worker.sock")
	cmd := exec.Command(server, "-f", "-t", "4", "-U", socket, plugin)
	if err := cmd.Start(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	})

	var conn net.Conn
	var err error
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		conn, err = net.DialTimeout("unix", socket, time.Second)
		if err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		b.Fatal(err)
	}

	// Fixed newstyle, unnamed export, and simple replies.
	var hello [18]byte
	if _, err := io.ReadFull(conn, hello[:]); err != nil || string(hello[:16]) != "NBDMAGICIHAVEOPT" {
		b.Fatalf("unexpected negotiation %x: %v", hello, err)
	}
	if _, err := conn.Write([]byte{0, 0, 0, 3}); err != nil {
		b.Fatal(err)
	}
	var option [16]byte
	copy(option[:8], "IHAVEOPT")
	binary.BigEndian.PutUint32(option[8:12], 1)
	if _, err := conn.Write(option[:]); err != nil {
		b.Fatal(err)
	}
	var export [10]byte
	if _, err := io.ReadFull(conn, export[:]); err != nil {
		b.Fatal(err)
	}
	if binary.BigEndian.Uint64(export[:8]) != 8192 {
		b.Fatalf("unexpected export %x", export)
	}

	var request [28]byte
	binary.BigEndian.PutUint32(request[:4], 0x25609513)
	binary.BigEndian.PutUint32(request[24:], 4096)
	var reply [16]byte
	b.ReportAllocs()
	for b.Loop() {
		if _, err := conn.Write(request[:]); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(conn, reply[:]); err != nil {
			b.Fatal(err)
		}
		if binary.BigEndian.Uint32(reply[:4]) != 0x67446698 ||
			binary.BigEndian.Uint32(reply[4:8]) != 5 {
			b.Fatalf("expected EIO, got %x", reply)
		}
	}
}
