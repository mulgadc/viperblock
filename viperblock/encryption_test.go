// Unit tests for the encryption-at-rest plumbing — coverage for New
// validation, writeFileAtomic, SaveState first-open bootstrap, LoadState
// fingerprint + high-water advance, reserveSeqNum slow path, and
// bumpSeqNumHighWater.

package viperblock

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
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

	// The tmp name is randomized per call (os.CreateTemp), so glob for any
	// leftover sibling rather than a fixed "path.tmp" suffix.
	leftovers, err := filepath.Glob(path + ".tmp-*")
	require.NoError(t, err)
	assert.Empty(t, leftovers, "no tmp file should remain after rename")
}

// TestWriteFileAtomic_ConcurrentWritersNeverTear is the direct A/B test for
// mulga-w1iu8 defect 1: many goroutines writing the SAME path concurrently
// must never produce a torn file. The old fixed "path.tmp" name let one
// writer's O_TRUNC clobber another's in-flight write and one writer's rename
// delete another's tmp, publishing a mix of two payloads (or, for an
// encrypted VBState, a blob that fails AEAD authentication). Each writer's
// payload here has a distinct length so a torn merge is detectable as a
// byte-for-byte mismatch against every single payload, not just a length
// check.
func TestWriteFileAtomic_ConcurrentWritersNeverTear(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	const writers = 16
	payloads := make([][]byte, writers)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte{byte('A' + i)}, 4096+i*37)
	}

	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := range writers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = writeFileAtomic(path, payloads[idx], 0600)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "writer %d", i)
	}

	got, err := os.ReadFile(path)
	require.NoError(t, err)

	matched := false
	for _, p := range payloads {
		if bytes.Equal(got, p) {
			matched = true
			break
		}
	}
	assert.True(t, matched,
		"final file (%d bytes) must exactly match exactly one writer's payload -- a mismatch means the rename published a torn mix of two writers", len(got))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".tmp-", "no leftover tmp file should remain: %s", e.Name())
	}
}

// A remount on a node that never held the volume locally has no per-volume
// dir; LoadState pulls backend state then persists it, so writeFileAtomic must
// create the missing parent rather than fail opening the tmp file with ENOENT.
func TestWriteFileAtomic_CreatesMissingParentDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vol-remount", "config.json")
	payload := []byte("state from backend")

	require.NoError(t, writeFileAtomic(path, payload, 0600))
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
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
	start, err := vb.reserveSeqNum(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, hwBefore, vb.seqNumHighWater.Load(), "fast path must not advance high-water")
	assert.Equal(t, start+10, vb.SeqNum.Load(), "reservation advances SeqNum by n")

	// Slow path: jump SeqNum past the current window and reserve one. The
	// helper must bump the high-water and persist.
	vb.SeqNum.Store(hwBefore + 5)
	_, err = vb.reserveSeqNum(context.Background(), 1)
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
	start, err := vb.reserveSeqNum(context.Background(), 5)
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
	_, err := vb.reserveSeqNum(context.Background(), 2)
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
				_, err := vb.reserveSeqNum(context.Background(), 1)
				assert.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	hw := vb.seqNumHighWater.Load()
	assert.GreaterOrEqual(t, hw, vb.SeqNum.Load(), "high-water must cover all handed-out SeqNums")
	assert.Zero(t, hw%seqNumReservation, "high-water advances in multiples of seqNumReservation")
}

// A failed SaveState in the high-water slow path must not publish the
// speculative high-water to vb.seqNumHighWater. If it did, concurrent
// fast-path callers would issue SeqNums above the persisted high-water; a
// crash + restart would restore vb.SeqNum from the lower on-disk value and
// re-issue the same SeqNums, causing AES-GCM nonce reuse under the same
// (key, VolumeUUID, domain) triple (NIST SP 800-38D §8.3 — catastrophic).
func TestReserveSeqNum_FailedSaveStateDoesNotPublishHighWater(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-faulty", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	hwBefore := vb.seqNumHighWater.Load()

	// Wedge writeFileAtomic by replacing the per-volume dir with a plain
	// file: writeFileAtomic's os.MkdirAll(dir, ...) fails with ENOTDIR
	// before it ever reaches os.CreateTemp (whose name is now randomized per
	// call, so it can no longer be pre-occupied by path). This is a
	// filesystem type mismatch, not a permission check, so it wedges
	// regardless of the test's uid. Removing the per-volume dir no longer
	// wedges since writeFileAtomic recreates it.
	configDir := filepath.Join(vb.BaseDir, vb.GetVolume())
	require.NoError(t, os.RemoveAll(configDir))
	require.NoError(t, os.WriteFile(configDir, []byte("blocking"), 0600))

	vb.SeqNum.Store(hwBefore + 5)
	_, err := vb.reserveSeqNum(context.Background(), 1)
	require.Error(t, err, "reserveSeqNum must propagate SaveState failure")

	assert.Equal(t, hwBefore, vb.seqNumHighWater.Load(),
		"failed SaveState must not publish the speculative high-water")

	// And after the failure is cleared, a successor reservation must persist
	// and publish the same target hw — no skipped window, no double-count.
	require.NoError(t, os.RemoveAll(configDir))
	_, err = vb.reserveSeqNum(context.Background(), 1)
	require.NoError(t, err)
	assert.Greater(t, vb.seqNumHighWater.Load(), hwBefore,
		"successor reservation persists and publishes new high-water")
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
		// The tmp name is now "config.json.tmp-<random>", so a trailing
		// HasSuffix(".tmp") check would silently stop catching leftovers —
		// match on the ".tmp-" infix the randomized suffix always follows.
		assert.NotContains(t, e.Name(), ".tmp-", "no .tmp-* should remain: %s", e.Name())
		if e.Name() == "config.json" {
			sawConfig = true
		}
	}
	assert.True(t, sawConfig, "config.json must exist after SaveState")
}
