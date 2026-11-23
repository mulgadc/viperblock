package viperblock

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/require"
)

// TestIOProfileServe exposes the existing loopback Predastore test cluster.
func TestIOProfileServe(t *testing.T) {
	root := os.Getenv("VB_IO_PROFILE_ROOT")
	if root == "" {
		t.Skip("standalone fixture")
	}
	key := testKey(t, 0x26)
	require.NoError(t, os.WriteFile(filepath.Join(root, "key"), bytes.Repeat([]byte{0x26}, 32), 0600))

	newVolume := func(name string) *VB {
		const size = 128 << 20
		config := &VB{
			VolumeName: name, VolumeSize: size, BaseDir: t.TempDir(),
			LeaseEpoch: 1, MasterKey: key, EncryptionEnabled: true,
			WALSyncInterval: -1, ChunkUploadInterval: -1,
		}
		vb, err := New(config, "s3", s3.S3Config{
			VolumeName: name, VolumeSize: size, Region: "ap-southeast-2",
			Bucket: sharedBucket, AccessKey: AccessKey, SecretKey: SecretKey,
			Host: "https://" + sharedServerHost, HTTPClient: testHTTPClient,
		})
		require.NoError(t, err)
		t.Cleanup(vb.Detach)
		require.NoError(t, vb.Backend.Init())
		require.NoError(t, vb.InitMapDeltaCtx(t.Context(), true))
		require.NoError(t, vb.SaveState())
		openMapWALs(t, vb)
		return vb
	}

	parent := newVolume("io-profile-source")
	image := make([]byte, 4<<20)
	for i := range image {
		image[i] = byte(i + i/4096)
	}
	require.NoError(t, parent.WriteAt(0, image))
	_, err := parent.CreateSnapshot("io-profile-snapshot")
	require.NoError(t, err)
	require.NoError(t, parent.Close())

	child := newVolume("io-profile-clone")
	require.NoError(t, child.OpenFromSnapshot("io-profile-snapshot"))
	require.NoError(t, child.SaveState())
	require.NoError(t, child.Close())
	require.NoError(t, os.WriteFile(filepath.Join(root, "image"), image, 0600))

	upstream, err := url.Parse("https://" + sharedServerHost)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.Transport = testHTTPClient.Transport
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	record := os.Getenv("VB_IO_PROFILE_RECORD") == "1"
	server := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rangeHeader := r.Header.Get("Range")
			if !record || r.Method != http.MethodGet || rangeHeader == "" {
				proxy.ServeHTTP(w, r)
				return
			}
			start := time.Now()
			log.Printf("profile-start %s %s", r.URL.Path, rangeHeader)
			proxy.ServeHTTP(w, r)
			log.Printf("profile-range %s %s %d", r.URL.Path, rangeHeader, time.Since(start).Nanoseconds())
		}),
	}
	t.Cleanup(func() { _ = server.Close() })
	go func() {
		if err := server.ServeTLS(listener, "../config/server.pem", "../config/server.key"); err != nil && err != http.ErrServerClosed {
			log.Printf("profile server failed: %v", err)
		}
	}()
	ctx, done := signal.NotifyContext(t.Context(), syscall.SIGTERM)
	defer done()
	require.NoError(t, os.WriteFile(filepath.Join(root, "endpoint"), []byte(fmt.Sprint(listener.Addr())), 0600))
	<-ctx.Done()
}

// TestIOProfileBarrierPhases keeps the engine and socket timing samples separate.
func TestIOProfileBarrierPhases(t *testing.T) {
	root := os.Getenv("VB_IO_PROFILE_WAL")
	if root == "" {
		t.Skip("requires scratch WAL on the measured filesystem")
	}
	const name, size = "io-profile-barrier", 128 << 20
	vb, err := New(&VB{
		VolumeName: name, VolumeSize: size, BaseDir: root,
		LeaseEpoch: 1, MasterKey: testKey(t, 0x26), EncryptionEnabled: true,
		WALSyncInterval: -1, ChunkUploadInterval: -1,
	}, "s3", s3.S3Config{
		VolumeName: name, VolumeSize: size, Region: "ap-southeast-2",
		Bucket: sharedBucket, AccessKey: AccessKey, SecretKey: SecretKey,
		Host: "https://" + sharedServerHost, HTTPClient: testHTTPClient,
	})
	require.NoError(t, err)
	t.Cleanup(vb.Detach)
	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.InitMapDeltaCtx(t.Context(), true))
	require.NoError(t, vb.SaveState())
	openMapWALs(t, vb)

	original := recordWALOp
	t.Cleanup(func() { recordWALOp = original })
	var lock sync.Mutex
	recordWALOp = func(ctx context.Context, phase, volume, outcome string, elapsed time.Duration) {
		original(ctx, phase, volume, outcome, elapsed)
		if volume == name {
			lock.Lock()
			t.Logf("profile-wal %s %s %d", phase, outcome, elapsed.Nanoseconds())
			lock.Unlock()
		}
	}
	data := bytes.Repeat([]byte{0x5b}, 4096)
	for batch := range 6 {
		for block := range 16 {
			offset := uint64((batch*16 + block) * len(data))
			require.NoError(t, vb.WriteAt(offset, data))
			start := time.Now()
			require.NoError(t, vb.flushWrites())
			t.Logf("profile-wal append success %d", time.Since(start).Nanoseconds())
			start = time.Now()
			require.NoError(t, vb.syncWAL())
			t.Logf("profile-wal sync success %d", time.Since(start).Nanoseconds())
		}
		start := time.Now()
		require.NoError(t, vb.DrainToBackend())
		t.Logf("profile-wal full-drain success %d", time.Since(start).Nanoseconds())
	}
	require.NoError(t, vb.Close())
}
