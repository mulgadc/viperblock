package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"libguestfs.org/nbdkit"
)

// resetPluginConfigState clears package-level nbdkit config vars so each
// test starts from ConfigComplete's zero state instead of leaking values
// set by a previous test.
func resetPluginConfigState() {
	size = 0
	volume = ""
	bucket = ""
	region = ""
	access_key = ""
	secret_key = ""
	host = ""
	base_dir = ""
	wal_base_dir = ""
	encryption_key_file = ""
	loadedMasterKey = nil
	lease_epoch = 0
	qualified_chunks = false
	launchChunkIDs.Store(0)
}

// TestConfig_WalBaseDirRoundTrips pins that the "wal_base_dir" nbdkit param
// sets the package var Open() reads into viperblock.VB.WALBaseDir, and that
// leaving it unset does not trip ConfigComplete's required-param checks.
func TestConfig_WalBaseDirRoundTrips(t *testing.T) {
	resetPluginConfigState()
	defer resetPluginConfigState()

	p := &ViperBlockPlugin{}
	err := p.Config("wal_base_dir", "/mnt/wal-device")
	assert.NoError(t, err)
	assert.Equal(t, "/mnt/wal-device", wal_base_dir)

	size = 1
	volume = "vol-1"
	bucket = "bucket-1"
	region = "region-1"
	access_key = "argv-access-key"
	secret_key = "argv-secret-key"
	base_dir = "/data"
	host = "localhost:9000"

	err = p.ConfigComplete()
	assert.NoError(t, err, "wal_base_dir must stay optional")
}

// TestCredentialFromArgOrEnv_ArgvTakesPriority pins that a non-empty argv
// value wins over the environment, matching LoadMasterKeyFromFlagOrEnv's
// flag-then-env precedence.
func TestCredentialFromArgOrEnv_ArgvTakesPriority(t *testing.T) {
	t.Setenv("VB_TEST_CRED", "from-env")

	got := credentialFromArgOrEnv("from-argv", "VB_TEST_CRED")
	assert.Equal(t, "from-argv", got)
}

// TestCredentialFromArgOrEnv_FallsBackToEnv pins that an empty argv value
// falls back to the named environment variable.
func TestCredentialFromArgOrEnv_FallsBackToEnv(t *testing.T) {
	t.Setenv("VB_TEST_CRED", "from-env")

	got := credentialFromArgOrEnv("", "VB_TEST_CRED")
	assert.Equal(t, "from-env", got)
}

// TestCredentialFromArgOrEnv_EmptyWhenNeitherSet pins that an empty argv
// value with no environment variable set resolves to empty.
func TestCredentialFromArgOrEnv_EmptyWhenNeitherSet(t *testing.T) {
	got := credentialFromArgOrEnv("", "VB_TEST_CRED_UNSET")
	assert.Equal(t, "", got)
}

// TestConfigComplete_AcceptsEnvSuppliedCredentials pins that ConfigComplete
// succeeds when credentials arrive via VB_ACCESS_KEY/VB_SECRET_KEY instead
// of argv, and that the resolved package vars carry the env values.
func TestConfigComplete_AcceptsEnvSuppliedCredentials(t *testing.T) {
	resetPluginConfigState()
	defer resetPluginConfigState()

	t.Setenv("VB_ACCESS_KEY", "env-access-key")
	t.Setenv("VB_SECRET_KEY", "env-secret-key")

	size = 1
	volume = "vol-1"
	bucket = "bucket-1"
	region = "region-1"
	base_dir = "/data"
	host = "localhost:9000"

	p := &ViperBlockPlugin{}
	err := p.ConfigComplete()
	assert.NoError(t, err)
	assert.Equal(t, "env-access-key", access_key)
	assert.Equal(t, "env-secret-key", secret_key)
}

// TestConfigComplete_StillAcceptsArgvSuppliedCredentials pins the transition
// path: an unpinned spinifex still passing credentials via argv (Config
// callbacks) must keep working with no environment variables set.
func TestConfigComplete_StillAcceptsArgvSuppliedCredentials(t *testing.T) {
	resetPluginConfigState()
	defer resetPluginConfigState()

	size = 1
	volume = "vol-1"
	bucket = "bucket-1"
	region = "region-1"
	access_key = "argv-access-key"
	secret_key = "argv-secret-key"
	base_dir = "/data"
	host = "localhost:9000"

	p := &ViperBlockPlugin{}
	err := p.ConfigComplete()
	assert.NoError(t, err)
	assert.Equal(t, "argv-access-key", access_key)
	assert.Equal(t, "argv-secret-key", secret_key)
}

// TestConfigComplete_RejectsMissingCredentialsFromBothSources pins that
// ConfigComplete still errors when credentials are absent from both argv
// and the environment.
func TestConfigComplete_RejectsMissingCredentialsFromBothSources(t *testing.T) {
	resetPluginConfigState()
	defer resetPluginConfigState()

	size = 1
	volume = "vol-1"
	bucket = "bucket-1"
	region = "region-1"

	p := &ViperBlockPlugin{}
	err := p.ConfigComplete()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "access_key parameter is required")
}

// TestBackendErrToPluginErrorMapsErrNoSpaceToENOSPC pins that a
// viperblock.ErrNoSpace write error reaches nbdkit as ENOSPC, not a generic
// I/O error.
func TestBackendErrToPluginErrorMapsErrNoSpaceToENOSPC(t *testing.T) {
	err := fmt.Errorf("drain chunk upload: %w", viperblock.ErrNoSpace)

	perr := backendErrToPluginError("Could not write data", err)

	assert.Equal(t, syscall.ENOSPC, perr.Errno,
		"an ErrNoSpace-wrapped error must set PluginError.Errno to syscall.ENOSPC")
	assert.Contains(t, perr.Errmsg, "Could not write data")
	assert.Contains(t, perr.Errmsg, err.Error())
}

// TestBackendErrToPluginErrorLeavesOtherErrorsAlone guards against
// over-matching: an unrelated write failure must not be given an ENOSPC
// errno it did not earn.
func TestBackendErrToPluginErrorLeavesOtherErrorsAlone(t *testing.T) {
	err := fmt.Errorf("connection reset by peer")

	perr := backendErrToPluginError("Could not write data", err)

	assert.Equal(t, syscall.Errno(0), perr.Errno,
		"an unrelated error must not set Errno (nbdkit defaults to EIO when Errno is 0)")
	assert.Contains(t, perr.Errmsg, "Could not write data")
}

// TestPluginErrorErrnoPropagatesAsError pins that the returned value
// satisfies error via nbdkit.PluginError, since implError type-asserts on it.
func TestPluginErrorErrnoPropagatesAsError(t *testing.T) {
	var err error = backendErrToPluginError("boom", fmt.Errorf("%w", viperblock.ErrNoSpace))

	perr, ok := err.(nbdkit.PluginError)
	if assert.True(t, ok, "backendErrToPluginError must return a value assignable to error via nbdkit.PluginError") {
		assert.Equal(t, syscall.ENOSPC, perr.Errno)
	}
}

// nbdkit closes and reopens stdio between GetReady and AfterFork, which fails
// if another thread allocates a descriptor meanwhile. With OTLP configured the
// plugin must therefore start no goroutines before AfterFork.
func TestConfigComplete_StartsNoBackgroundWorkBeforeAfterFork(t *testing.T) {
	resetPluginConfigState()
	defer resetPluginConfigState()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Cleanup(func() {
		if otelShutdown != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_ = otelShutdown(ctx)
			otelShutdown = nil
		}
	})

	size = 1
	volume = "vol-1"
	bucket = "bucket-1"
	region = "region-1"
	access_key = "argv-access-key"
	secret_key = "argv-secret-key"
	base_dir = t.TempDir()
	host = "localhost:9000"

	p := &ViperBlockPlugin{}
	before := runtime.NumGoroutine()
	assert.NoError(t, p.ConfigComplete())
	assert.NoError(t, p.GetReady())
	assert.Equal(t, before, runtime.NumGoroutine(),
		"ConfigComplete/GetReady must not start goroutines: nbdkit resets stdio after them")
	assert.Nil(t, otelShutdown, "OTLP export must not start before AfterFork")

	assert.NoError(t, p.AfterFork())
	assert.NotNil(t, otelShutdown, "AfterFork must start OTLP export when an endpoint is configured")
	assert.Greater(t, runtime.NumGoroutine(), before, "OTLP exporters run in background goroutines")
}

func TestConfig_LeaseEpochParsesAndRejectsGarbage(t *testing.T) {
	resetPluginConfigState()
	defer resetPluginConfigState()

	p := &ViperBlockPlugin{}
	assert.NoError(t, p.Config("lease_epoch", "18446744073709551615"))
	assert.Equal(t, uint64(18446744073709551615), lease_epoch)
	assert.Error(t, p.Config("lease_epoch", "-1"))
	assert.Error(t, p.Config("lease_epoch", "seven"))
}

// TestPWriteDoesNotRetainRequestBuffer models nbdkit reusing the input
// as soon as the callback returns.
func TestPWriteDoesNotRetainRequestBuffer(t *testing.T) {
	t.Run("cleartext", func(t *testing.T) {
		checkPWriteBufferReuse(t, nil)
	})
	t.Run("encrypted", func(t *testing.T) {
		raw := bytes.Repeat([]byte{0x67}, masterkey.MasterKeySize)
		aead, err := masterkey.NewAEAD(raw)
		require.NoError(t, err)
		checkPWriteBufferReuse(t, &masterkey.Key{AEAD: aead, Fingerprint: masterkey.Fingerprint(raw)})
	})
}

func checkPWriteBufferReuse(t *testing.T, key *masterkey.Key) {
	t.Helper()
	root := t.TempDir()
	vb := newWriteBufferVolume(t, root, 1, key)
	t.Cleanup(func() { detachWriteBufferVolume(vb) })
	openWriteBufferWAL(t, vb)
	conn := &ViperBlockConnection{vb: vb}

	want := make([]byte, 1<<20)
	requestBuffer := make([]byte, len(want))
	for _, request := range []struct {
		offset int
		length int
		value  byte
	}{
		{0, 1 << 20, 0x45},
		{4096, 4096, 0x72},
		{8192 - 256, 512, 0x98},
	} {
		buf := requestBuffer[:request.length]
		for i := range buf {
			buf[i] = request.value
		}
		expected := want[request.offset : request.offset+request.length]
		copy(expected, buf)
		require.NoError(t, conn.PWrite(buf, uint64(request.offset), 0))
		require.Equal(t, expected, buf, "PWrite modified the input")
		clear(buf)
	}

	got, err := vb.ReadAt(0, uint64(len(want)))
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.NoError(t, conn.Flush(0))
	// Detach without draining so the next engine must use the local WAL.
	detachWriteBufferVolume(vb)
	vb = nil

	reopened := reopenWriteBufferVolume(t, root, key)
	_, _, _, err = reopened.LookupBlockToObject(0)
	require.ErrorIs(t, err, viperblock.ErrZeroBlock, "the checkpoint must not contain the new blocks")
	require.NoError(t, reopened.RecoverLocalWALs())
	got, err = reopened.ReadAt(0, uint64(len(want)))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestPWriteDoesNotRetainRequestBufferOnError forces the second block's
// read-modify-write to fail after the first full block has been buffered.
func TestPWriteDoesNotRetainRequestBufferOnError(t *testing.T) {
	const blockSize = 4096
	root := t.TempDir()
	vb := newWriteBufferVolume(t, root, 1, nil)
	t.Cleanup(func() { detachWriteBufferVolume(vb) })
	openWriteBufferWAL(t, vb)
	require.NoError(t, vb.WriteAt(blockSize, bytes.Repeat([]byte{0x31}, blockSize)))
	require.NoError(t, vb.DrainToBackend())
	object, _, _, err := vb.LookupBlockToObject(1)
	require.NoError(t, err)
	chunkPath := types.GetFilePath(types.FileTypeChunk, object, vb.GetVolume())
	require.NoError(t, os.Remove(filepath.Join(root, "backend", chunkPath)))
	detachWriteBufferVolume(vb)
	vb = nil

	reopened := reopenWriteBufferVolume(t, root, nil)
	conn := &ViperBlockConnection{vb: reopened}
	input := bytes.Repeat([]byte{0x64}, blockSize+512)
	want := bytes.Clone(input)
	err = conn.PWrite(input, 0, 0)
	require.ErrorContains(t, err, "failed to read block 1 for RMW")
	require.Equal(t, want, input)
	clear(input)
	got, err := reopened.ReadAt(0, blockSize)
	require.NoError(t, err)
	require.Equal(t, want[:blockSize], got)
}

func reopenWriteBufferVolume(t *testing.T, root string, key *masterkey.Key) *viperblock.VB {
	t.Helper()
	vb := newWriteBufferVolume(t, root, 2, key)
	t.Cleanup(vb.Detach)
	require.NoError(t, vb.LoadState())
	require.NoError(t, vb.LoadLiveCheckpoint())
	require.NoError(t, vb.ClaimMapCtx(t.Context()))
	return vb
}

func newWriteBufferVolume(t testing.TB, root string, epoch uint64, key *masterkey.Key) *viperblock.VB {
	t.Helper()
	backendDir := filepath.Join(root, "backend")
	require.NoError(t, os.MkdirAll(backendDir, 0700))
	config := viperblock.VB{
		VolumeName: "borrowed-buf", VolumeSize: 128 << 20,
		BaseDir: filepath.Join(root, "local"), LeaseEpoch: epoch,
		WALSyncInterval: -1, ChunkUploadInterval: -1,
		MasterKey: key, EncryptionEnabled: key != nil,
	}
	vb, err := viperblock.New(&config, "file", file.FileConfig{
		VolumeName: config.VolumeName, VolumeSize: config.VolumeSize, BaseDir: backendDir,
	})
	require.NoError(t, err)
	if err := vb.Backend.Init(); err != nil {
		vb.Detach()
		t.Fatal(err)
	}
	return vb
}

func openWriteBufferWAL(t testing.TB, vb *viperblock.VB) {
	t.Helper()
	require.NoError(t, vb.InitMapDeltaCtx(context.Background(), true))
	require.NoError(t, vb.SaveState())
	walPath := filepath.Join(vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Add(1), vb.GetVolume()))
	require.NoError(t, vb.OpenWAL(&vb.WAL, walPath))
}

func detachWriteBufferVolume(vb *viperblock.VB) {
	if vb == nil {
		return
	}
	vb.Detach()
	for _, wal := range vb.WAL.DB {
		_ = wal.Close()
	}
}

func BenchmarkPWrite(b *testing.B) {
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(logger)

	for _, size := range []int{4 << 10, 1 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			root := filepath.Join(b.TempDir(), "volume")
			newVolume := func() *viperblock.VB {
				vb := newWriteBufferVolume(b, root, 1, nil)
				openWriteBufferWAL(b, vb)
				return vb
			}
			conn := &ViperBlockConnection{vb: newVolume()}
			defer func() { detachWriteBufferVolume(conn.vb) }()
			buf := bytes.Repeat([]byte{0x56}, size)
			// Keep both the dirty set and temporary disk use bounded.
			const window = 16 << 20
			offset := 0
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				if offset == window {
					b.StopTimer()
					detachWriteBufferVolume(conn.vb)
					conn.vb = nil
					require.NoError(b, os.RemoveAll(root))
					conn.vb = newVolume()
					offset = 0
					b.StartTimer()
				}
				if err := conn.PWrite(buf, uint64(offset), 0); err != nil {
					b.Fatal(err)
				}
				offset += size
			}
		})
	}
}

func TestPReadDestinationContentsAndOwnership(t *testing.T) {
	const bs = 4096
	root := t.TempDir()
	vb := newWriteBufferVolume(t, root, 1, nil)
	defer detachWriteBufferVolume(vb)
	openWriteBufferWAL(t, vb)
	hot := &ViperBlockConnection{vb: vb}
	want := make([]byte, 3*bs)
	copy(want, bytes.Repeat([]byte{0xb6}, bs))
	copy(want[2*bs:], bytes.Repeat([]byte{0x3e}, bs))
	require.NoError(t, vb.WriteAt(0, want[:bs]))
	require.NoError(t, vb.WriteAt(2*bs, want[2*bs:]))

	check := func(conn *ViperBlockConnection) {
		t.Helper()
		for _, r := range []struct{ offset, length int }{{0, len(want)}, {bs, bs}, {bs - 512, bs}, {2 * bs, bs}} {
			dst := bytes.Repeat([]byte{0xf9}, r.length)
			require.NoError(t, conn.PRead(dst, uint64(r.offset), 0))
			require.Equal(t, want[r.offset:r.offset+r.length], dst)
			clear(dst)
		}
	}
	check(hot)
	require.NoError(t, vb.DrainToBackend())
	detachWriteBufferVolume(vb)

	reopened := reopenWriteBufferVolume(t, root, nil)
	require.NoError(t, reopened.SetCacheSize(32, 0))
	mapped := &ViperBlockConnection{vb: reopened}
	check(mapped)
	check(mapped)
	require.Error(t, mapped.PRead(make([]byte, bs), reopened.GetVolumeSize()-512, 0))
	require.Error(t, mapped.PRead(make([]byte, bs), reopened.GetVolumeSize()+1, 0))
	patch := bytes.Repeat([]byte{0x7e}, 512)
	require.NoError(t, mapped.PWrite(patch, 512, 0))
	require.NoError(t, mapped.PWrite(patch, bs+512, 0))
	copy(want[512:], patch)
	copy(want[bs+512:], patch)
	check(mapped)
	_, err := reopened.ReadAt(3*bs+1, 0)
	require.ErrorIs(t, err, viperblock.ErrZeroBlock)
}

type callbackReadFault struct {
	types.Backend
	corruptTag bool
}

func (f callbackReadFault) ReadCtx(ctx context.Context, typ types.FileType, id uint64, off, n uint32) ([]byte, error) {
	got, err := f.Backend.ReadCtx(ctx, typ, id, off, n)
	if err != nil || typ != types.FileTypeChunk || off == 0 || len(got) < 2 {
		return got, err
	}
	if f.corruptTag {
		got[len(got)-1] ^= 0x80
		return got, nil
	}
	return got[:len(got)-1], nil
}

func TestPReadRejectsFaultyMappedData(t *testing.T) {
	for _, mode := range []string{"short", "bad-tag"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			var key *masterkey.Key
			if mode == "bad-tag" {
				var err error
				key, err = masterkey.New(bytes.Repeat([]byte{0x26}, masterkey.MasterKeySize))
				require.NoError(t, err)
			}
			vb := newWriteBufferVolume(t, root, 1, key)
			defer detachWriteBufferVolume(vb)
			openWriteBufferWAL(t, vb)
			require.NoError(t, vb.WriteAt(0, bytes.Repeat([]byte{0xac}, 4096)))
			require.NoError(t, vb.DrainToBackend())
			detachWriteBufferVolume(vb)

			reopened := reopenWriteBufferVolume(t, root, key)
			reopened.Backend = callbackReadFault{Backend: reopened.Backend, corruptTag: mode == "bad-tag"}
			conn := &ViperBlockConnection{vb: reopened}
			err := conn.PRead(make([]byte, 4096), 0, 0)
			if mode == "bad-tag" {
				require.ErrorContains(t, err, "integrity")
			} else {
				require.ErrorContains(t, err, "short")
			}
		})
	}
}

func TestPReadDestinationWithConcurrentOverwrite(t *testing.T) {
	vb := newWriteBufferVolume(t, t.TempDir(), 1, nil)
	defer detachWriteBufferVolume(vb)
	openWriteBufferWAL(t, vb)
	conn := &ViperBlockConnection{vb: vb}
	a := bytes.Repeat([]byte{0x24}, 4096)
	b := bytes.Repeat([]byte{0xa5}, 4096)
	require.NoError(t, conn.PWrite(a, 0, 0))
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Go(func() {
		<-start
		for range 128 {
			for _, src := range [][]byte{b, a} {
				if err := conn.PWrite(src, 0, 0); err != nil {
					t.Error(err)
					return
				}
			}
		}
	})
	group.Go(func() {
		<-start
		dst := make([]byte, 4096)
		for range 128 {
			if err := conn.PRead(dst, 0, 0); err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(dst, a) && !bytes.Equal(dst, b) {
				t.Error("read returned neither full block")
				return
			}
			clear(dst)
		}
	})
	close(start)
	group.Wait()
}

func TestPReadDestinationCoversCloneAndHoles(t *testing.T) {
	const bs = 4096
	root := t.TempDir()
	source := newWriteBufferVolume(t, root, 1, nil)
	defer detachWriteBufferVolume(source)
	openWriteBufferWAL(t, source)
	inherited := bytes.Repeat([]byte{0x97}, bs)
	own := bytes.Repeat([]byte{0x64}, bs)
	require.NoError(t, source.WriteAt(0, inherited))
	_, err := source.CreateSnapshot("read-destination-snapshot")
	require.NoError(t, err)

	clone, err := viperblock.New(&viperblock.VB{
		VolumeName: "read-destination-clone", VolumeSize: 128 << 20,
		BaseDir: filepath.Join(root, "local"), LeaseEpoch: 1,
		WALSyncInterval: -1, ChunkUploadInterval: -1,
	}, "file", file.FileConfig{VolumeName: "read-destination-clone", VolumeSize: 128 << 20, BaseDir: filepath.Join(root, "backend")})
	require.NoError(t, err)
	defer detachWriteBufferVolume(clone)
	require.NoError(t, clone.Backend.Init())
	require.NoError(t, clone.OpenFromSnapshot("read-destination-snapshot"))
	require.NoError(t, clone.WriteAt(2*bs, own))
	dst := bytes.Repeat([]byte{0x2c}, 3*bs)
	conn := &ViperBlockConnection{vb: clone}
	require.NoError(t, conn.PRead(dst, 0, 0))
	want := make([]byte, 3*bs)
	copy(want, inherited)
	copy(want[2*bs:], own)
	require.Equal(t, want, dst)
}

type callbackReadQueueProbe struct {
	types.Backend
	firstOffset      uint32
	entered, release chan struct{}
	bodyBytes        atomic.Uint64
}

func (p *callbackReadQueueProbe) ReadCtx(ctx context.Context, typ types.FileType, id uint64, off, n uint32) ([]byte, error) {
	if typ == types.FileTypeChunk {
		if off == p.firstOffset {
			close(p.entered)
			<-p.release
		}
		p.bodyBytes.Add(uint64(n))
	}
	return p.Backend.ReadCtx(ctx, typ, id, off, n)
}

func TestPReadDestinationPreservesReadAheadQueueAccounting(t *testing.T) {
	const bs = 4096
	root := t.TempDir()
	writer := newWriteBufferVolume(t, root, 1, nil)
	defer detachWriteBufferVolume(writer)
	openWriteBufferWAL(t, writer)
	image := bytes.Repeat([]byte{0x13}, 16*bs)
	copy(image[bs:2*bs], bytes.Repeat([]byte{0x83}, bs))
	require.NoError(t, writer.WriteAt(0, image))
	require.NoError(t, writer.DrainToBackend())
	_, offset, _, err := writer.LookupBlockToObject(0)
	require.NoError(t, err)
	detachWriteBufferVolume(writer)

	vb := reopenWriteBufferVolume(t, root, nil)
	require.NoError(t, vb.SetCacheSize(16, 0))
	probe := &callbackReadQueueProbe{Backend: vb.Backend, firstOffset: offset,
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	vb.Backend = probe
	release := sync.OnceFunc(func() { close(probe.release) })
	defer release()
	conn := &ViperBlockConnection{vb: vb}
	firstResult := make(chan error, 1)
	first, second := make([]byte, bs), make([]byte, bs)
	go func() { firstResult <- conn.PRead(first, 0, 0) }()
	select {
	case <-probe.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first read never entered the backend")
	}
	secondResult := make(chan error, 1)
	go func() { secondResult <- conn.PRead(second, bs, 0) }()
	select {
	case err := <-secondResult:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("second read did not complete while the first was held")
	}
	release()
	require.NoError(t, <-firstResult)
	require.Equal(t, image[:bs], first)
	require.Equal(t, image[bs:2*bs], second)
	require.EqualValues(t, 2*bs, probe.bodyBytes.Load())
}

func BenchmarkPRead(b *testing.B) {
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(logger)

	for _, size := range []int{4 << 10, 1 << 20, 64 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			vb := newWriteBufferVolume(b, b.TempDir(), 1, nil)
			b.Cleanup(func() { detachWriteBufferVolume(vb) })
			openWriteBufferWAL(b, vb)
			conn := &ViperBlockConnection{vb: vb}
			seed := bytes.Repeat([]byte{0x56}, size)
			require.NoError(b, vb.WriteAt(0, seed))
			dst := make([]byte, size)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if err := conn.PRead(dst, 0, 0); err != nil {
					b.Fatal(err)
				}
			}
			require.Equal(b, seed, dst)
		})
	}
}

type heldChunkWrite struct {
	types.Backend
	entered chan struct{}
	release chan struct{}
}

var _ types.Backend = (*heldChunkWrite)(nil)

func (b *heldChunkWrite) WriteChunkCtx(ctx context.Context, ref types.ChunkRef, headers, data *[]byte) error {
	close(b.entered)
	select {
	case <-b.release:
		return b.Backend.WriteChunkCtx(ctx, ref, headers, data)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *heldChunkWrite) WriteCtx(ctx context.Context, kind types.FileType, id uint64, headers, data *[]byte) error {
	if kind == types.FileTypeChunk {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.Backend.WriteCtx(ctx, kind, id, headers, data)
}

// Verifies: Q-417
func TestColdNBDReadSurvivesPriorEpochChunkWrite(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
			var key *masterkey.Key
			if encrypted {
				var err error
				key, err = masterkey.New(bytes.Repeat([]byte{0x26}, masterkey.MasterKeySize))
				require.NoError(t, err)
			}

			backendDir := t.TempDir()
			const volumeName = "epoch-chunk"
			const volumeSize = 128 << 20
			newEngine := func(epoch uint64) *viperblock.VB {
				t.Helper()
				config := viperblock.VB{
					VolumeName: volumeName, VolumeSize: volumeSize,
					BaseDir: t.TempDir(), LeaseEpoch: epoch, MasterKey: key,
					EncryptionEnabled: encrypted, WALSyncInterval: -1, ChunkUploadInterval: -1,
					QualifiedChunks: epoch > 1,
				}
				engine, err := viperblock.New(&config, "file", file.FileConfig{
					VolumeName: volumeName, VolumeSize: volumeSize, BaseDir: backendDir,
				})
				require.NoError(t, err)
				t.Cleanup(func() { detachWriteBufferVolume(engine) })
				require.NoError(t, engine.Backend.Init())
				return engine
			}
			openWriter := func(engine *viperblock.VB) *ViperBlockConnection {
				t.Helper()
				path := filepath.Join(engine.WAL.BaseDir, types.GetFilePath(
					types.FileTypeWALChunk, engine.WAL.WallNum.Add(1), volumeName))
				require.NoError(t, engine.OpenWAL(&engine.WAL, path))
				return &ViperBlockConnection{vb: engine}
			}
			claim := func(engine *viperblock.VB) {
				t.Helper()
				require.NoError(t, engine.LoadState())
				require.NoError(t, engine.LoadLiveCheckpoint())
				require.NoError(t, engine.ClaimMapCtx(t.Context()))
			}

			initial := newEngine(1)
			require.NoError(t, initial.InitMapDeltaCtx(t.Context(), true))
			require.NoError(t, initial.SaveState())

			old := newEngine(10)
			claim(old)
			oldConn := openWriter(old)
			require.NoError(t, oldConn.PWrite(bytes.Repeat([]byte{0x19}, 4096), 0, 0))
			require.NoError(t, oldConn.Flush(0))

			blocker := &heldChunkWrite{
				Backend: old.Backend, entered: make(chan struct{}), release: make(chan struct{}),
			}
			old.Backend = blocker
			release := sync.OnceFunc(func() { close(blocker.release) })
			defer release()
			done := make(chan error, 1)
			go func() { done <- old.DrainToBackendCtx(t.Context()) }()
			select {
			case <-blocker.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("older epoch did not submit its chunk")
			}

			current := newEngine(20)
			claim(current)
			conn := openWriter(current)
			want := bytes.Repeat([]byte{0x42}, 4096)
			require.NoError(t, conn.PWrite(want, 0, 0))
			require.NoError(t, conn.Flush(0))
			require.NoError(t, current.DrainToBackendCtx(t.Context()))

			release()
			select {
			case err := <-done:
				if err != nil {
					require.ErrorIs(t, err, viperblock.ErrMapFenced)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("older epoch chunk did not complete")
			}

			cold := newEngine(30)
			claim(cold)
			buf := make([]byte, len(want))
			require.NoError(t, (&ViperBlockConnection{vb: cold}).PRead(buf, 0, 0))
			require.True(t, bytes.Equal(want, buf), "cold read did not recover the successor's acknowledged generation")
		})
	}
}

type heldCompactionWrite struct {
	types.Backend
	entered  chan string
	release  chan struct{}
	finished chan error
}

var _ types.Backend = (*heldCompactionWrite)(nil)

func (b *heldCompactionWrite) WriteKeyCtx(ctx context.Context, key string, data []byte) error {
	if !strings.HasPrefix(key, "map/base.") {
		return b.Backend.WriteKeyCtx(ctx, key, data)
	}
	b.entered <- key
	<-b.release
	err := b.Backend.WriteKeyCtx(context.WithoutCancel(ctx), key, data)
	b.finished <- err
	return err
}

func TestOpenRetryAfterReplayWithHeldCompaction(t *testing.T) {
	resetPluginConfigState()
	t.Cleanup(resetPluginConfigState)
	resetCounters := func() {
		mapClaimed.Store(false)
		mapHeadN.Store(0)
		mapObjN.Store(0)
	}
	resetCounters()
	t.Cleanup(resetCounters)
	previousSharding := use_shardwal
	t.Cleanup(func() { use_shardwal = previousSharding })
	use_shardwal = false

	root := t.TempDir()
	seed := newWriteBufferVolume(t, root, 5, nil)
	openWriteBufferWAL(t, seed)
	// Fill the delta chain so the next replay commit starts compaction.
	seedData := bytes.Repeat([]byte{0x31}, int(seed.BlockSize))
	for range 512 {
		require.NoError(t, seed.Write(0, seedData))
		require.NoError(t, seed.DrainToBackend())
	}
	replayed := bytes.Repeat([]byte{0x72}, int(seed.BlockSize))
	require.NoError(t, seed.Write(1, replayed))
	require.NoError(t, seed.Flush())
	replayedWAL := filepath.Join(seed.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, seed.WAL.WallNum.Load(), seed.GetVolume()))
	require.FileExists(t, replayedWAL)
	detachWriteBufferVolume(seed)

	volume, base_dir, size, lease_epoch = seed.GetVolume(), seed.BaseDir, seed.VolumeSize, 6
	fileConfig := file.FileConfig{
		VolumeName: volume, VolumeSize: size, BaseDir: filepath.Join(root, "backend"),
	}
	held := &heldCompactionWrite{
		entered: make(chan string, 1), release: make(chan struct{}), finished: make(chan error, 1),
	}
	release := sync.OnceFunc(func() { close(held.release) })
	t.Cleanup(release)
	newVolume := func(cfg *viperblock.VB, _ s3.S3Config) (*viperblock.VB, error) {
		return viperblock.New(cfg, "file", fileConfig)
	}
	newFailingVolume := func(cfg *viperblock.VB, backend s3.S3Config) (*viperblock.VB, error) {
		vb, err := newVolume(cfg, backend)
		if err == nil {
			held.Backend = vb.Backend
			vb.Backend = held
		}
		return vb, err
	}

	var failedChunkWAL *os.File
	var heldKey string
	failBlockWAL := func(vb *viperblock.VB, wal *viperblock.WAL, path string) error {
		select {
		case heldKey = <-held.entered:
		case <-time.After(5 * time.Second):
			return fmt.Errorf("replay did not start background compaction")
		}
		failedChunkWAL = vb.WAL.DB[0]
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
		defer os.Remove(path)
		return vb.OpenWAL(wal, path)
	}

	type openResult struct {
		connection nbdkit.ConnectionInterface
		err        error
	}
	plugin := &ViperBlockPlugin{}
	openWhileHeld := func(factory func(*viperblock.VB, s3.S3Config) (*viperblock.VB, error),
		openWAL func(*viperblock.VB, *viperblock.WAL, string) error,
	) openResult {
		t.Helper()
		result := make(chan openResult, 1)
		go func() {
			conn, err := plugin.open(false, factory, openWAL)
			result <- openResult{conn, err}
		}()
		select {
		case opened := <-result:
			return opened
		case <-time.After(10 * time.Second):
			release()
			<-result
			t.Fatal("Open waited for held compaction")
			return openResult{}
		}
	}

	failed := openWhileHeld(newFailingVolume, failBlockWAL)
	require.ErrorContains(t, failed.err, "Could not open Block WAL: open WAL file")
	require.NotEmpty(t, heldKey)
	require.NotNil(t, failedChunkWAL)
	_, err := failedChunkWAL.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	require.NoFileExists(t, replayedWAL)
	failedHead, failedObject := mapHeadN.Load(), mapObjN.Load()
	var heldEpoch, heldObject uint64
	_, err = fmt.Sscanf(heldKey, "map/base.%d.%d", &heldEpoch, &heldObject)
	require.NoError(t, err)
	require.Equal(t, lease_epoch, heldEpoch)
	require.Positive(t, failedHead)
	require.Greater(t, failedObject, heldObject)

	reopened := openWhileHeld(newVolume, (*viperblock.VB).OpenWAL)
	require.NoError(t, reopened.err)
	retry := reopened.connection.(*ViperBlockConnection)
	closeRetry := sync.OnceFunc(retry.Close)
	t.Cleanup(closeRetry)
	head, object := retry.vb.MapCounters()
	require.GreaterOrEqual(t, head, failedHead)
	require.GreaterOrEqual(t, object, failedObject)

	newData := bytes.Repeat([]byte{0xa8}, int(seed.BlockSize))
	blockSize := uint64(seed.BlockSize)
	require.NoError(t, retry.PWrite(newData, 2*blockSize, 0))
	require.NoError(t, retry.vb.DrainToBackend())
	head, object = retry.vb.MapCounters()
	require.Greater(t, head, failedHead)
	require.Greater(t, object, failedObject)
	release()
	select {
	case err := <-held.finished:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the held compaction PUT did not finish")
	}
	closeRetry()

	reader, err := viperblock.New(&viperblock.VB{
		BaseDir: t.TempDir(), VolumeName: volume, VolumeSize: size, LeaseEpoch: 7,
		WALSyncInterval: -1, ChunkUploadInterval: -1, GCInterval: -1,
	}, "file", fileConfig)
	require.NoError(t, err)
	t.Cleanup(reader.Detach)
	require.NoError(t, reader.Backend.Init())
	require.NoError(t, reader.LoadStateAndLiveCheckpointCtx(t.Context()))
	cold := &ViperBlockConnection{vb: reader, readonly: true}
	got := make([]byte, 2*blockSize)
	require.NoError(t, cold.PRead(got, blockSize, 0))
	require.Equal(t, append(replayed, newData...), got)
}

// pendingProbeBackend records the most buffered write bytes the engine holds
// at any chunk upload, which is when a drain is moving them out.
type pendingProbeBackend struct {
	types.Backend

	vb   *viperblock.VB
	peak atomic.Uint64
}

var _ types.Backend = (*pendingProbeBackend)(nil)

func (p *pendingProbeBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectID uint64, headers *[]byte, data *[]byte) error {
	if fileType == types.FileTypeChunk {
		pending := p.vb.PendingBytes()
		for {
			peak := p.peak.Load()
			if pending <= peak || p.peak.CompareAndSwap(peak, pending) {
				break
			}
		}
	}
	return p.Backend.WriteCtx(ctx, fileType, objectID, headers, data)
}

func TestZeroBoundsPendingBytesPerChunk(t *testing.T) {
	const (
		maxPending = 8 << 20
		chunkBytes = 4 << 20
		written    = 72 << 20
		offset     = 4096 + 512
		count      = 64 << 20
	)
	vb := newWriteBufferVolume(t, t.TempDir(), 1, nil)
	t.Cleanup(func() { detachWriteBufferVolume(vb) })
	openWriteBufferWAL(t, vb)
	vb.MaxPendingBytes = maxPending
	probe := &pendingProbeBackend{Backend: vb.Backend, vb: vb}
	vb.Backend = probe

	pattern := make([]byte, written)
	for i := range pattern {
		pattern[i] = byte(i%251) + 1
	}
	require.NoError(t, vb.WriteAt(0, pattern))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.DrainToBackend())
	probe.peak.Store(0)

	conn := &ViperBlockConnection{vb: vb}
	require.NoError(t, conn.Zero(count, offset, 0))
	require.LessOrEqual(t, probe.peak.Load(), uint64(maxPending+chunkBytes+int(vb.BlockSize)),
		"a zero request must be buffered a chunk at a time, under backpressure")

	got := make([]byte, written)
	require.NoError(t, conn.PRead(got, 0, 0))
	require.Equal(t, pattern[:offset], got[:offset], "bytes before the range changed")
	require.Equal(t, make([]byte, count), got[offset:offset+count], "the range is not zero")
	require.Equal(t, pattern[offset+count:], got[offset+count:], "bytes after the range changed")
}

func TestZeroOutOfRangeWritesNothing(t *testing.T) {
	vb := newWriteBufferVolume(t, t.TempDir(), 1, nil)
	t.Cleanup(func() { detachWriteBufferVolume(vb) })
	openWriteBufferWAL(t, vb)
	block := bytes.Repeat([]byte{0x5a}, int(vb.BlockSize))
	require.NoError(t, vb.WriteAt(0, block))
	conn := &ViperBlockConnection{vb: vb}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := conn.Zero(math.MaxUint32, 0, 0)
	runtime.ReadMemStats(&after)
	require.ErrorContains(t, err, viperblock.ErrRequestOutOfRange.Error())
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(16<<20),
		"an out-of-range request must be refused before it allocates its length")

	got := make([]byte, len(block))
	require.NoError(t, conn.PRead(got, 0, 0))
	require.Equal(t, block, got)
}

func BenchmarkZero(b *testing.B) {
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(logger)

	for _, size := range []int{4 << 10, 1 << 20, 4 << 20, 64 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			root := filepath.Join(b.TempDir(), "volume")
			newVolume := func() *viperblock.VB {
				vb := newWriteBufferVolume(b, root, 1, nil)
				openWriteBufferWAL(b, vb)
				return vb
			}
			conn := &ViperBlockConnection{vb: newVolume()}
			defer func() { detachWriteBufferVolume(conn.vb) }()
			// Keep both the dirty set and temporary disk use bounded.
			window := max(16<<20, size)
			offset := 0
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				if offset+size > window {
					b.StopTimer()
					detachWriteBufferVolume(conn.vb)
					conn.vb = nil
					require.NoError(b, os.RemoveAll(root))
					conn.vb = newVolume()
					offset = 0
					b.StartTimer()
				}
				if err := conn.Zero(uint32(size), uint64(offset), 0); err != nil { //nolint:gosec // G115: size is at most 64 MiB
					b.Fatal(err)
				}
				offset += size
			}
		})
	}
}
