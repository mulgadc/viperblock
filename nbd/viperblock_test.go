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
