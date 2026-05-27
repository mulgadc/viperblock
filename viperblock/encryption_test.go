// Copyright 2026 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

// Stage 1 unit tests for the encryption-at-rest plumbing — minimal coverage
// for the new code paths (New validation, writeFileAtomic, SaveState first-
// open bootstrap, LoadState fingerprint + high-water advance, reserveSeqNum
// slow path, bumpSeqNumHighWater). Stage 5 lands the comprehensive matrix.

package viperblock

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testKey builds a deterministic *masterkey.Key from a single seed byte so
// tests can assert against KeyFingerprint mismatch by varying the seed.
func testKey(t *testing.T, seed byte) *masterkey.Key {
	t.Helper()
	var raw [masterkey.MasterKeySize]byte
	for i := range raw {
		raw[i] = seed
	}
	aead, err := masterkey.NewAEAD(raw[:])
	require.NoError(t, err)
	return &masterkey.Key{
		AEAD:        aead,
		Fingerprint: masterkey.Fingerprint(raw[:]),
	}
}

// newFileBackedVB stands up a viperblock instance against a file backend in a
// temp dir — no predastore server required, so these tests run cheaply.
func newFileBackedVB(t *testing.T, name string, key *masterkey.Key) *VB {
	t.Helper()
	dir := t.TempDir()
	cfg := file.FileConfig{BaseDir: dir, VolumeName: name}
	vb, err := New(&VB{
		VolumeName:        name,
		VolumeSize:        4 * 1024 * 1024,
		BaseDir:           dir,
		MasterKey:         key,
		EncryptionEnabled: key != nil,
	}, "file", cfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	return vb
}

func TestNew_EncryptionInvariants(t *testing.T) {
	dir := t.TempDir()
	cfg := file.FileConfig{BaseDir: dir, VolumeName: "vol-1"}

	t.Run("flag without key", func(t *testing.T) {
		_, err := New(&VB{
			VolumeName:        "vol-1",
			VolumeSize:        4096,
			BaseDir:           dir,
			EncryptionEnabled: true,
		}, "file", cfg)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrEncryptionMismatch)
	})

	t.Run("key without flag", func(t *testing.T) {
		_, err := New(&VB{
			VolumeName: "vol-1",
			VolumeSize: 4096,
			BaseDir:    dir,
			MasterKey:  testKey(t, 0x42),
		}, "file", cfg)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrEncryptionMismatch)
	})

	t.Run("encryption with sharded WAL", func(t *testing.T) {
		_, err := New(&VB{
			VolumeName:        "vol-1",
			VolumeSize:        4096,
			BaseDir:           dir,
			MasterKey:         testKey(t, 0x42),
			EncryptionEnabled: true,
			UseShardedWAL:     true,
		}, "file", cfg)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrEncryptionMismatch)
	})

	t.Run("happy path", func(t *testing.T) {
		vb, err := New(&VB{
			VolumeName:        "vol-1",
			VolumeSize:        4096,
			BaseDir:           dir,
			MasterKey:         testKey(t, 0x42),
			EncryptionEnabled: true,
		}, "file", cfg)
		require.NoError(t, err)
		require.NotNil(t, vb.aead)
		assert.Equal(t, computeVolumeNameHash("vol-1"), vb.volumeNameHash)
	})
}

func TestWriteFileAtomic_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "atomic.bin")
	payload := []byte("hello, durable world")

	require.NoError(t, writeFileAtomic(path, payload, 0600))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, payload, got)

	// Overwrite — atomic rename must replace cleanly without leaving the tmp.
	updated := []byte("second generation")
	require.NoError(t, writeFileAtomic(path, updated, 0600))
	got, err = os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, updated, got)
	_, err = os.Stat(path + ".tmp")
	assert.True(t, errors.Is(err, os.ErrNotExist), "tmp file should not remain after rename")
}

func TestSaveState_BootstrapsVolumeUUIDAndHighWater(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-bootstrap", key)
	vb.BlockSize = DefaultBlockSize

	var zero [4]byte
	require.Equal(t, zero, vb.VolumeUUID, "fresh VB starts with zero UUID")
	require.NoError(t, vb.SaveState())

	assert.NotEqual(t, zero, vb.VolumeUUID, "SaveState mints UUID on first encrypted persist")
	assert.Equal(t, seqNumReservation, vb.seqNumHighWater.Load(), "first SaveState seeds high-water to one reservation")

	// Round-trip via a second VB and LoadState — fingerprint must match,
	// high-water must advance, and SeqNum must restart at the prior high-water.
	vb2 := newFileBackedVB(t, "vol-bootstrap", key)
	vb2.BaseDir = vb.BaseDir
	vb2.BlockSize = DefaultBlockSize
	require.NoError(t, vb2.LoadState())
	assert.Equal(t, vb.VolumeUUID, vb2.VolumeUUID, "VolumeUUID survives LoadState")
	assert.Equal(t, seqNumReservation, vb2.SeqNum.Load(), "SeqNum restarts at the persisted high-water")
	assert.Equal(t, 2*seqNumReservation, vb2.seqNumHighWater.Load(), "LoadState advances high-water by one reservation")
}

func TestLoadState_KeyFingerprintMismatch(t *testing.T) {
	keyA := testKey(t, 0x01)
	keyB := testKey(t, 0x02)
	require.NotEqual(t, keyA.Fingerprint, keyB.Fingerprint)

	vb := newFileBackedVB(t, "vol-fp", keyA)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	// Reopen with the wrong key. The mismatch must surface as a clear error,
	// not silent decrypt failure later.
	vb2 := newFileBackedVB(t, "vol-fp", keyB)
	vb2.BaseDir = vb.BaseDir
	err := vb2.LoadState()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEncryptionMismatch)
}

func TestLoadState_RuntimeEncryptionFlagMustMatchPersisted(t *testing.T) {
	key := testKey(t, 0x99)
	vb := newFileBackedVB(t, "vol-enc", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	// Reopen claiming no encryption — must refuse.
	plain := newFileBackedVB(t, "vol-enc", nil)
	plain.BaseDir = vb.BaseDir
	err := plain.LoadState()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEncryptionMismatch)
}

func TestReserveSeqNum_FastAndSlowPath(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-reserve", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	hwBefore := vb.seqNumHighWater.Load()

	// Fast path: a single reservation well below the high-water must not
	// bump it (no extra SaveState).
	start, err := vb.reserveSeqNum(10)
	require.NoError(t, err)
	assert.Equal(t, hwBefore, vb.seqNumHighWater.Load(), "fast path must not advance high-water")
	assert.Equal(t, start+10, vb.SeqNum.Load(), "reservation advances SeqNum by n")

	// Slow path: jump SeqNum past the current window and reserve one. The
	// helper must bump the high-water and persist.
	vb.SeqNum.Store(hwBefore + 5)
	_, err = vb.reserveSeqNum(1)
	require.NoError(t, err)
	assert.Greater(t, vb.seqNumHighWater.Load(), hwBefore, "slow path must advance high-water")
}

func TestReserveSeqNum_UnencryptedFallthrough(t *testing.T) {
	dir := t.TempDir()
	cfg := file.FileConfig{BaseDir: dir, VolumeName: "vol-plain"}
	vb, err := New(&VB{
		VolumeName: "vol-plain",
		VolumeSize: 4096,
		BaseDir:    dir,
	}, "file", cfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())

	// Plain atomic.Add — no high-water consultation.
	start, err := vb.reserveSeqNum(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), start)
	assert.Equal(t, uint64(5), vb.SeqNum.Load())
}

func TestReserveSeqNum_RefusesPastMaxSeqNum(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-overflow", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	// Set SeqNum to one below the limit so a 2-wide reservation would cross it.
	vb.SeqNum.Store(MaxSeqNum - 1)
	_, err := vb.reserveSeqNum(2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds 56-bit nonce limit")
}

func TestReserveSeqNum_ConcurrentCallersSerializeHighWaterBumps(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-concurrent", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	// Drive reservations past the initial window from multiple goroutines —
	// the slow path is mutex-protected, so the final high-water must be a
	// multiple of seqNumReservation and bounded by SeqNum.
	const (
		goroutines = 8
		perWorker  = 16
	)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range perWorker {
				_, err := vb.reserveSeqNum(1)
				assert.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	hw := vb.seqNumHighWater.Load()
	assert.GreaterOrEqual(t, hw, vb.SeqNum.Load(), "high-water must cover all handed-out SeqNums")
	assert.Zero(t, hw%seqNumReservation, "high-water advances in multiples of seqNumReservation")
}

func TestSaveState_AtomicRenameLeavesNoTmp(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-atomic", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())
	require.NoError(t, vb.SaveState())

	// The config.json must exist, the .tmp must not.
	configDir := filepath.Join(vb.BaseDir, vb.GetVolume())
	entries, err := os.ReadDir(configDir)
	require.NoError(t, err)
	var sawConfig bool
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		assert.False(t, strings.HasSuffix(e.Name(), ".tmp"), "no .tmp should remain: %s", e.Name())
		if e.Name() == "config.json" {
			sawConfig = true
		}
	}
	assert.True(t, sawConfig, "config.json must exist after SaveState")
}
