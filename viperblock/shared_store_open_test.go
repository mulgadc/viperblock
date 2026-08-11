// What keeps two openers of one volume from colliding when they are on
// different hosts. AcquireVolumeLock is an flock under the opener's own
// BaseDir, so it excludes two openers on one host and nothing at all across
// hosts sharing a predastore. These tests use one shared store with two
// private local dirs, which is that shape exactly, and pin the mechanism that
// does cover it: every open claims a fresh SeqNum window through the shared
// state before it can issue anything.
package viperblock

import (
	"context"
	"testing"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

const sharedStoreVolumeSize = 4 * 1024 * 1024

// openOnSharedStore builds a VB whose local state lives in its own directory
// while its backend is the store both openers share.
func openOnSharedStore(t *testing.T, store, localDir, volume string, key *masterkey.Key) *VB {
	t.Helper()
	vb, err := New(&VB{
		VolumeName:        volume,
		VolumeSize:        sharedStoreVolumeSize,
		BaseDir:           localDir,
		MasterKey:         key,
		EncryptionEnabled: true,
	}, "file", file.FileConfig{BaseDir: store, VolumeName: volume})
	require.NoError(t, err)
	t.Cleanup(func() {
		vb.StopChunkUploader()
		vb.StopWALSyncer()
	})
	require.NoError(t, vb.Backend.Init())
	return vb
}

// seedSharedVolume creates the volume in the shared store, the way a
// CreateVolume would, and leaves nothing open behind it.
func seedSharedVolume(t *testing.T, store, localDir, volume string, key *masterkey.Key) {
	t.Helper()
	vb := openOnSharedStore(t, store, localDir, volume, key)
	require.NoError(t, vb.SaveState())
}

// TestSharedStore_TwoOpenersGetDisjointSeqNumWindows is the invariant the
// write-on-open exists for. A SeqNum is the nonce input, so two openers
// issuing the same one under the same master key would be a repeated
// (key, nonce) pair — the failure AEAD has no recovery from.
//
// LoadState starts SeqNum at the persisted high-water and advances that
// high-water by a full reservation before returning, so each opener leaves
// with a subspace no other opener can reach. Removing the write to make a
// read cheaper would remove this.
func TestSharedStore_TwoOpenersGetDisjointSeqNumWindows(t *testing.T) {
	store := t.TempDir()
	key := testKey(t, 0x51)
	const volume = "vol-sharedstore01"

	seedSharedVolume(t, store, t.TempDir(), volume, key)

	first := openOnSharedStore(t, store, t.TempDir(), volume, key)
	require.NoError(t, first.LoadState())
	second := openOnSharedStore(t, store, t.TempDir(), volume, key)
	require.NoError(t, second.LoadState())

	const reserve = 8
	firstStart, err := first.reserveSeqNum(context.Background(), reserve)
	require.NoError(t, err)
	secondStart, err := second.reserveSeqNum(context.Background(), reserve)
	require.NoError(t, err)

	overlap := firstStart < secondStart+reserve && secondStart < firstStart+reserve
	require.False(t, overlap,
		"two openers on one store issued overlapping SeqNums (%d-%d and %d-%d): same key, same nonce",
		firstStart, firstStart+reserve-1, secondStart, secondStart+reserve-1)
}

// TestSharedStore_SecondOpenerDoesNotRegressTheHighWater covers the other
// half. The high-water is what stops a crashed process's un-persisted range
// from being reissued, and pushStateToBackend is an unconditional PUT with no
// precondition on what it overwrites, so a later opener writing a lower
// ceiling is the shape to watch for.
func TestSharedStore_SecondOpenerDoesNotRegressTheHighWater(t *testing.T) {
	store := t.TempDir()
	key := testKey(t, 0x52)
	const volume = "vol-sharedstore02"

	seedSharedVolume(t, store, t.TempDir(), volume, key)

	// Both open before either issues anything, which is the ordering that
	// would expose a lost update if the ceiling were not claimed on open.
	first := openOnSharedStore(t, store, t.TempDir(), volume, key)
	require.NoError(t, first.LoadState())
	second := openOnSharedStore(t, store, t.TempDir(), volume, key)
	require.NoError(t, second.LoadState())

	// The first opener runs ahead, reserving well past one window.
	_, err := first.reserveSeqNum(context.Background(), seqNumReservation*3)
	require.NoError(t, err)
	ahead := first.seqNumHighWater.Load()

	// The second opener now persists its own, lower ceiling.
	_, err = second.reserveSeqNum(context.Background(), 1)
	require.NoError(t, err)

	reopened := openOnSharedStore(t, store, t.TempDir(), volume, key)
	require.NoError(t, reopened.LoadState())
	require.GreaterOrEqual(t, reopened.seqNumHighWater.Load(), ahead,
		"the durable high-water went backwards, so a reopen reissues SeqNums already sealed under")
}
