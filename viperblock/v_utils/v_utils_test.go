package v_utils

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/viperblock/internal/predastoretest"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportDiskImage(t *testing.T) {
	s3Config := &s3.S3Config{}
	vbConfig := &viperblock.VB{
		VolumeConfig: viperblock.VolumeConfig{},
	}

	filename := "test.img"

	err := ImportDiskImage(s3Config, vbConfig, filename, nil)

	assert.ErrorContains(t, err, "failed to open disk file")

	// Correct file, different error
	filename = "../../tests/unit-test-disk-image.raw"

	err = ImportDiskImage(s3Config, vbConfig, filename, nil)

	assert.ErrorContains(t, err, "failed to connect to Viperblock store")

	// Test defaults are set
	assert.NotEmpty(t, vbConfig.VolumeConfig.VolumeMetadata.VolumeID)
	assert.NotEmpty(t, vbConfig.VolumeConfig.VolumeMetadata.VolumeName)
	assert.NotEmpty(t, vbConfig.VolumeConfig.VolumeMetadata.State)
	assert.NotZero(t, vbConfig.VolumeConfig.VolumeMetadata.CreatedAt)

	// Next, create a simulated AMI and check defaults
	vbConfig = &viperblock.VB{
		VolumeConfig: viperblock.VolumeConfig{
			AMIMetadata: viperblock.AMIMetadata{
				Name: "Test AMI",
			},
		},
	}

	err = ImportDiskImage(s3Config, vbConfig, filename, nil)

	assert.ErrorContains(t, err, "failed to connect to Viperblock store")

	// Test AMI defaults are set
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.ImageID)
	assert.NotZero(t, vbConfig.VolumeConfig.AMIMetadata.CreationDate)
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.Architecture)
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.PlatformDetails)
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.RootDeviceType)
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.Virtualization)
	assert.NotEmpty(t, vbConfig.VolumeConfig.AMIMetadata.ImageOwnerAlias)
}

// TestProgressReporterThrottlesToPercent drives the reporter over a simulated
// multi-GiB stream in 4 KiB steps and asserts the callback fires at most 101
// times (one per percent plus the final 100%), not proportional to block count.
// It also checks the contract: current is monotonic and ends exactly at total.
func TestProgressReporterThrottlesToPercent(t *testing.T) {
	const blockSize = 4096
	const total uint64 = 4 * 1024 * 1024 * 1024 // 4 GiB

	var calls int
	var last uint64
	monotonic := true
	reporter := newProgressReporter(func(current, reportedTotal uint64) {
		calls++
		if current < last {
			monotonic = false
		}
		last = current
		assert.Equal(t, total, reportedTotal)
	}, total)

	for current := uint64(blockSize); current <= total; current += blockSize {
		reporter.report(current)
	}
	reporter.finish()

	assert.True(t, monotonic, "current must never decrease")
	assert.LessOrEqual(t, calls, 101, "callback bounded to one per percent plus final")
	assert.Equal(t, total, last, "final report ends exactly at total")
	assert.Less(t, uint64(calls), total/blockSize, "renders must not scale with block count")
}

// TestProgressReporterNilTolerated verifies a nil ProgressFunc is a no-op and
// that a zero total never divides by zero or emits a callback.
func TestProgressReporterNilTolerated(t *testing.T) {
	reporter := newProgressReporter(nil, 1024)
	assert.NotPanics(t, func() {
		reporter.report(512)
		reporter.finish()
	})

	var called bool
	zero := newProgressReporter(func(current, total uint64) { called = true }, 0)
	assert.NotPanics(t, func() {
		zero.report(0)
		zero.finish()
	})
	assert.False(t, called, "zero-total import reports nothing")
}

// Default test credentials for the local predastore test server started in
// TestMain (matches the ones used across the parent viperblock package).
const (
	testAccessKey = "AKIAIOSFODNN7EXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

// sharedServerHost / sharedBucket describe the predastore test server started
// once in TestMain and shared by every test in this package.
var (
	sharedServerHost string
	sharedBucket     string
)

// sharedHTTPClient skips TLS verification for the self-signed test server cert.
var sharedHTTPClient = &http.Client{
	Timeout: 60 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // only for test
	},
}

// TestMain starts one in-process predastore cluster for every test in this
// package that needs a real S3 round trip (the AMI-state tests below): a
// failed or partial ImportDiskImage is only observable by reading back
// whatever actually landed in the backend, which a fake or stubbed client
// cannot stand in for.
func TestMain(m *testing.M) {
	os.Exit(func() int {
		dir, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get working directory: %v\n", err)
			return 1
		}
		repoRoot := filepath.Join(dir, "..", "..")

		// predastore's internal s3db + QUIC clients verify TLS strictly against
		// the OS trust store. Point SystemCertPool at our self-signed test cert
		// so verify succeeds without installing the cert system-wide.
		certPath := filepath.Join(repoRoot, "config", "server.pem")
		if err := os.Setenv("SSL_CERT_FILE", certPath); err != nil {
			fmt.Fprintf(os.Stderr, "failed to set SSL_CERT_FILE: %v\n", err)
			return 1
		}

		runRoot, err := os.MkdirTemp("", "viperblock-vutils-predastore-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
			return 1
		}
		defer os.RemoveAll(runRoot)

		dataDir := filepath.Join(runRoot, "predastore")
		if err := os.MkdirAll(dataDir, 0o750); err != nil {
			fmt.Fprintf(os.Stderr, "failed to create predastore data dir: %v\n", err)
			return 1
		}

		srv, err := predastoretest.Start(predastoretest.Options{
			DataDir:    dataDir,
			CertPath:   certPath,
			KeyPath:    filepath.Join(repoRoot, "config", "server.key"),
			BucketName: "predastore",
			AccessKey:  testAccessKey,
			SecretKey:  testSecretKey,
			AccountID:  "123456789012",
			Region:     "ap-southeast-2",
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to start predastore test cluster: %v\n", err)
			return 1
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil {
				fmt.Fprintf(os.Stderr, "predastore shutdown error: %v\n", shutdownErr)
			}
		}()

		sharedServerHost = srv.Endpoint
		sharedBucket = srv.Bucket

		return m.Run()
	}())
}

// testMasterKey builds a random AES-GCM master key for encrypted-volume tests.
func testMasterKey(t *testing.T) *masterkey.Key {
	t.Helper()

	var raw [masterkey.MasterKeySize]byte
	_, err := rand.Read(raw[:])
	require.NoError(t, err)

	aead, err := masterkey.NewAEAD(raw[:])
	require.NoError(t, err)

	return &masterkey.Key{AEAD: aead, Fingerprint: masterkey.Fingerprint(raw[:])}
}

// loadPersistedAMIState opens a fresh VB against the same volume/backend
// ImportDiskImage just wrote to, rooted at a brand-new local dir so LoadState
// has no local copy to fall back on and must read what is actually durable in
// the backend — the same view a separate process (e.g. spx describe-images)
// would get.
func loadPersistedAMIState(t *testing.T, volumeName string, volumeSize uint64, s3Config *s3.S3Config, key *masterkey.Key) string {
	t.Helper()

	readerConfig := &viperblock.VB{
		VolumeName:        volumeName,
		VolumeSize:        volumeSize,
		BaseDir:           t.TempDir(),
		WALSyncInterval:   -1,
		EncryptionEnabled: key != nil,
		MasterKey:         key,
		Cache:             viperblock.Cache{Config: viperblock.CacheConfig{Size: 0}},
	}

	vb, err := viperblock.New(readerConfig, "s3", *s3Config)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.LoadState())

	return vb.VolumeConfig.AMIMetadata.State
}

// makeRawImage writes a size-byte raw disk image filled with a non-zero
// repeating byte (so ImportDiskImage's zero-block skip never triggers) to a
// fresh file under dir and returns its path.
func makeRawImage(t *testing.T, dir string, size uint64) string {
	t.Helper()

	path := filepath.Join(dir, "disk.raw")
	img := bytes.Repeat([]byte{0xAB}, int(size))
	require.NoError(t, os.WriteFile(path, img, 0o600))
	return path
}

// TestImportDiskImage_FailedImportLeavesPendingState is the regression guard
// for mulga-sn062: a failed admin AMI import used to leave the AMI registered
// and, absent a state field, indistinguishable from a complete one.
//
// The volume is deliberately sized to one block while the source image is
// three, so WriteAt fails with ErrRequestOutOfRange on the second block —
// after the encrypted pre-loop SaveState has already registered "pending",
// but before any chunk data or Close ever runs. That is the live path: AMI
// imports are encrypted by default, and this is the earliest point at which
// the AMI becomes visible in the backend at all.
func TestImportDiskImage_FailedImportLeavesPendingState(t *testing.T) {
	tmpDir := t.TempDir()
	volumeName := fmt.Sprintf("vol-pending-%d", time.Now().UnixNano())
	blockSize := uint64(viperblock.DefaultBlockSize)

	imagePath := makeRawImage(t, tmpDir, 3*blockSize)
	key := testMasterKey(t)

	vbConfig := &viperblock.VB{
		VolumeName:        volumeName,
		VolumeSize:        blockSize, // one block: the second write overflows it
		BaseDir:           filepath.Join(tmpDir, "viperblock"),
		WALSyncInterval:   -1,
		EncryptionEnabled: true,
		MasterKey:         key,
		Cache:             viperblock.Cache{Config: viperblock.CacheConfig{Size: 0}},
		VolumeConfig: viperblock.VolumeConfig{
			AMIMetadata: viperblock.AMIMetadata{Name: "pending-test-ami"},
		},
	}

	s3Config := &s3.S3Config{
		VolumeName: volumeName,
		VolumeSize: blockSize,
		Region:     "ap-southeast-2",
		Bucket:     sharedBucket,
		AccessKey:  testAccessKey,
		SecretKey:  testSecretKey,
		Host:       fmt.Sprintf("https://%s", sharedServerHost),
		HTTPClient: sharedHTTPClient,
	}

	err := ImportDiskImage(s3Config, vbConfig, imagePath, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to write block")

	state := loadPersistedAMIState(t, volumeName, blockSize, s3Config, key)
	assert.Equal(t, "pending", state)
}

// TestImportDiskImage_CleanImportLeavesAvailableState is the companion pin: a
// clean import must end with State: "available" durably persisted, not just
// held in memory. This exercises the unencrypted path deliberately — nothing
// registers the AMI before Close's own state save runs, so the "available"
// flip and its follow-up SaveState after Close returns nil is the only thing
// that ever persists a non-pending state for this path.
func TestImportDiskImage_CleanImportLeavesAvailableState(t *testing.T) {
	tmpDir := t.TempDir()
	volumeName := fmt.Sprintf("vol-available-%d", time.Now().UnixNano())
	blockSize := uint64(viperblock.DefaultBlockSize)
	volumeSize := 2 * blockSize

	imagePath := makeRawImage(t, tmpDir, volumeSize)

	vbConfig := &viperblock.VB{
		VolumeName:      volumeName,
		VolumeSize:      volumeSize,
		BaseDir:         filepath.Join(tmpDir, "viperblock"),
		WALSyncInterval: -1,
		Cache:           viperblock.Cache{Config: viperblock.CacheConfig{Size: 0}},
		VolumeConfig: viperblock.VolumeConfig{
			AMIMetadata: viperblock.AMIMetadata{Name: "available-test-ami"},
		},
	}

	s3Config := &s3.S3Config{
		VolumeName: volumeName,
		VolumeSize: volumeSize,
		Region:     "ap-southeast-2",
		Bucket:     sharedBucket,
		AccessKey:  testAccessKey,
		SecretKey:  testSecretKey,
		Host:       fmt.Sprintf("https://%s", sharedServerHost),
		HTTPClient: sharedHTTPClient,
	}

	err := ImportDiskImage(s3Config, vbConfig, imagePath, nil)
	require.NoError(t, err)

	state := loadPersistedAMIState(t, volumeName, volumeSize, s3Config, nil)
	assert.Equal(t, "available", state)
}
