package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
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
