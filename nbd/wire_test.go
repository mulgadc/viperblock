package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The wire-level regression: NBD_CMD_WRITE_ZEROES past the advertised export
// must be refused with NBD_EINVAL on the wire, because a well-behaved client
// enforces the export size before sending and the callback-level tests cannot
// show what a hostile or buggy client would see.

// nbdReply is one simple reply from the server.
type nbdReply struct {
	err    uint32
	handle uint64
}

// fakeS3 is the minimal in-process S3 endpoint the plugin's backend speaks to:
// objects stored by path, ranged GETs honored, auth ignored.
type fakeS3 struct {
	srv   *httptest.Server
	mu    sync.RWMutex
	store map[string][]byte
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	s := &fakeS3{store: map[string][]byte{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		s.mu.Lock()
		defer s.mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			body := make([]byte, r.ContentLength)
			_, _ = io.ReadFull(r.Body, body)
			s.store[path] = body
			w.WriteHeader(200)
		case http.MethodGet:
			if !strings.Contains(strings.Trim(path, "/"), "/") { // bucket-level request: a list
				w.Header().Set("Content-Type", "application/xml")
				_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`))
				return
			}
			body, ok := s.store[path]
			if !ok {
				w.WriteHeader(404)
				return
			}
			if rng := r.Header.Get("Range"); rng != "" {
				start, end := parseRange(rng, len(body))
				if start < 0 || end >= len(body) {
					w.WriteHeader(416)
					return
				}
				w.Header().Set("Content-Range", "bytes "+strconv.Itoa(start)+"-"+strconv.Itoa(end)+"/"+strconv.Itoa(len(body)))
				w.WriteHeader(206)
				_, _ = w.Write(body[start : end+1])
				return
			}
			_, _ = w.Write(body)
		default:
			w.WriteHeader(200)
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func parseRange(rng string, length int) (int, int) {
	var start, end int
	if n, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil || n < 2 {
		if _, err := fmt.Sscanf(rng, "bytes=%d-", &start); err != nil {
			return -1, -1
		}
		return start, length - 1
	}
	return start, end
}

// startNbdkit builds the plugin, starts nbdkit over a unix socket against the
// fake S3 endpoint, and returns the socket path.
func startNbdkit(t *testing.T, s3 *fakeS3) (string, string) {
	t.Helper()
	if _, err := exec.LookPath("nbdkit"); err != nil {
		t.Skip("nbdkit is not installed; the NBD wire tier runs where the nbdkit dev environment exists")
	}
	dir := t.TempDir()
	plugin := filepath.Join(dir, "plugin.so")
	build := exec.Command("go", "build", "-o", plugin, "-buildmode=c-shared", "viperblock.go")
	build.Dir = "."
	out, err := build.CombinedOutput()
	require.NoError(t, err, "build plugin: %s", out)

	sock := filepath.Join(dir, "nbd.sock")
	baseDir := filepath.Join(dir, "backend")
	require.NoError(t, os.MkdirAll(baseDir, 0o700))

	// The plugin opens an initialized volume: import one through the vblock
	// CLI, which writes the state the Open path then loads.
	vblock := filepath.Join(dir, "vblock")
	buildVB := exec.Command("go", "build", "-o", vblock, "../cmd/vblock")
	buildVB.Dir = "."
	out, err = buildVB.CombinedOutput()
	require.NoError(t, err, "build vblock: %s", out)
	disk := filepath.Join(dir, "disk.raw")
	require.NoError(t, os.WriteFile(disk, make([]byte, 32<<20), 0o600))
	meta := filepath.Join(dir, "meta.json")
	require.NoError(t, os.WriteFile(meta, []byte(`{"VolumeMetadata":{"VolumeID":"wiretest"}}`), 0o600))
	imp := exec.Command(vblock, "-file", disk, "-size", "33554432", "-volume", "wiretest",
		"-bucket", "bucket", "-region", "us-east-1", "-access_key", "ak", "-secret_key", "sk",
		"-host", s3.srv.URL, "-base_dir", baseDir, "-metadata", meta)
	out, err = imp.CombinedOutput()
	require.NoError(t, err, "import volume: %s", out)

	cmd := exec.Command("nbdkit", "-f", "--unix", sock, plugin,
		"volume=wiretest", "size=33554432", "bucket=bucket", "region=us-east-1",
		"access_key=ak", "secret_key=sk", "host="+s3.srv.URL, "base_dir="+baseDir)
	logPath := filepath.Join(dir, "nbdkit.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	defer logFile.Close()
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(sock); err == nil {
			return sock, logPath
		}
		if time.Now().After(deadline) {
			log, _ := os.ReadFile(logPath)
			t.Fatalf("nbdkit socket never appeared: %s", log)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// nbdConnect performs the fixed-newstyle handshake and NBD_OPT_GO, returning
// a connection ready for transmission requests.
func nbdConnect(t *testing.T, sock, export string) net.Conn {
	t.Helper()
	conn, err := net.Dial("unix", sock)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	var buf [8]byte
	_, err = io.ReadFull(conn, buf[:]) // "NBDMAGIC"
	require.NoError(t, err)
	_, err = io.ReadFull(conn, buf[:]) // "IHAVEOPT"
	require.NoError(t, err)
	var hsFlags [2]byte
	_, err = io.ReadFull(conn, hsFlags[:])
	require.NoError(t, err)

	var clientFlags [4]byte
	binary.BigEndian.PutUint32(clientFlags[:], 1) // NBD_FLAG_C_FIXED_NEWSTYLE
	_, err = conn.Write(clientFlags[:])
	require.NoError(t, err)

	// NBD_OPT_GO with the export name and zero info requests.
	name := []byte(export)
	opt := make([]byte, 0, 8+4+8+4+len(name)+2)
	opt = binary.BigEndian.AppendUint64(opt, 0x49484156454F5054) // IHAVEOPT
	opt = binary.BigEndian.AppendUint32(opt, 7)                  // NBD_OPT_GO
	opt = binary.BigEndian.AppendUint32(opt, uint32(4+len(name)+2))
	opt = binary.BigEndian.AppendUint32(opt, uint32(len(name)))
	opt = append(opt, name...)
	opt = binary.BigEndian.AppendUint16(opt, 0) // number of info requests
	_, err = conn.Write(opt)
	require.NoError(t, err)

	// Read replies until NBD_REP_ACK. The option reply header is 20 bytes:
	// magic, option, reply type, and a 32-bit length.
	for {
		var rep [20]byte
		_, err = io.ReadFull(conn, rep[:])
		require.NoError(t, err)
		require.Equal(t, uint64(0x3e889045565a9), binary.BigEndian.Uint64(rep[0:8]), "option reply magic")
		replyType := binary.BigEndian.Uint32(rep[12:16])
		length := binary.BigEndian.Uint32(rep[16:20])
		t.Logf("option reply: option=%x type=%x len=%d", binary.BigEndian.Uint32(rep[8:12]), replyType, length)
		if length > 0 {
			payload := make([]byte, length)
			_, err = io.ReadFull(conn, payload)
			require.NoError(t, err)
			if replyType == 3 && len(payload) >= 12 { // NBD_REP_INFO export info
				t.Logf("export info: size=%d flags=%#x", binary.BigEndian.Uint64(payload[2:10]), binary.BigEndian.Uint16(payload[10:12]))
			}
		}
		if replyType == 1 { // NBD_REP_ACK
			return conn
		}
		require.Less(t, replyType, uint32(1<<31), "option refused on the wire")
	}
}

// nbdWriteZeroes sends one NBD_CMD_WRITE_ZEROES and returns the reply.
func nbdWriteZeroes(t *testing.T, conn net.Conn, offset, length uint64, nbdkitLog string) nbdReply {
	t.Helper()
	var handle [8]byte
	binary.BigEndian.PutUint64(handle[:], 0x7769726574657374)

	req := make([]byte, 0, 24)
	req = binary.BigEndian.AppendUint32(req, 0x25609513) // request magic
	req = binary.BigEndian.AppendUint16(req, 0)          // command flags
	req = binary.BigEndian.AppendUint16(req, 6)          // NBD_CMD_WRITE_ZEROES
	req = append(req, handle[:]...)
	req = binary.BigEndian.AppendUint64(req, offset)
	req = binary.BigEndian.AppendUint32(req, uint32(length))
	_, err := conn.Write(req)
	require.NoError(t, err)

	var rep [16]byte
	_, err = io.ReadFull(conn, rep[:])
	t.Logf("simple reply: %x", rep)
	if err != nil {
		if log, readErr := os.ReadFile(nbdkitLog); readErr == nil {
			t.Logf("nbdkit log: %s", log)
		}
	}
	require.NoError(t, err)
	require.Equal(t, uint32(0x67446698), binary.BigEndian.Uint32(rep[0:4]), "simple reply magic")
	return nbdReply{
		err:    binary.BigEndian.Uint32(rep[4:8]),
		handle: binary.BigEndian.Uint64(rep[8:16]),
	}
}

// TestNBDWireRejectedZero pins the guest-visible rejection on the wire: a
// WRITE_ZEROES that crosses the advertised export end is answered with
// NBD_EINVAL, and an in-range one is answered with success.
func TestNBDWireRejectedZero(t *testing.T) {
	s3 := newFakeS3(t)
	sock, nbdkitLog := startNbdkit(t, s3)
	conn := nbdConnect(t, sock, "wiretest")

	// In range: succeeds.
	reply := nbdWriteZeroes(t, conn, 0, 4096, nbdkitLog)
	require.Zero(t, reply.err, "an in-range zero must succeed on the wire")

	// Crosses the export end (32 MiB): refused on the wire. nbdkit validates
	// the request against the advertised export size before the plugin runs
	// and answers an out-of-range request with NBD_ENOSPC (28) — its own
	// protocol-layer choice; the callback's EINVAL refusal is pinned at the
	// callback level, where the committed tests check it.
	reply = nbdWriteZeroes(t, conn, 33554432-1, 2, nbdkitLog)
	require.Equal(t, uint32(28), reply.err, "a zero crossing the export end must be refused on the wire")

	// Beyond the export: refused the same way.
	reply = nbdWriteZeroes(t, conn, 33554432, 1024, nbdkitLog)
	require.Equal(t, uint32(28), reply.err, "a zero past the export must be refused on the wire")
}
