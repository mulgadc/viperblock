// CopySnapshotMeta tests. A copy must be readable end to end on an encrypted
// volume (clone reads decrypt source chunks through the copied checkpoint),
// must seal under a freshly minted StateSeqNum that is durable before the
// seal, and must refuse any input that would silently produce an unreadable
// or nonce-unsafe destination.

package viperblock

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedSnapshot writes blockCount random blocks to env.source, persists them to
// chunks, and takes snapshotID. Returns the plaintext for later comparison.
func seedSnapshot(t *testing.T, env *snapshotEnv, snapshotID string, blockCount uint64) ([]byte, *SnapshotState) {
	t.Helper()
	plaintext := make([]byte, uint64(env.source.BlockSize)*blockCount)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)

	require.NoError(t, env.source.Write(0, plaintext))
	require.NoError(t, env.source.Flush())
	require.NoError(t, env.source.WriteWALToChunk(true))

	snap, err := env.source.CreateSnapshot(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, snap)
	return plaintext, snap
}

// TestCopySnapshotMeta_EncryptedRoundTrip is the decisive gate: a clone opened
// from the COPY must decrypt the source volume's chunks and hand back the same
// plaintext as a clone of the original. This is what the spinifex CopySnapshot
// path was missing — the control plane wrote its own document under the new ID
// and nothing landed on the backend, so the clone blew up at open.
func TestCopySnapshotMeta_EncryptedRoundTrip(t *testing.T) {
	env := newSnapshotEnv(t, "src-copy-rt", testKey(t, 0x42))
	blockCount := uint64(4)
	plaintext, src := seedSnapshot(t, env, "snap-copy-rt-src", blockCount)

	dst, err := env.source.CopySnapshotMeta("snap-copy-rt-src", "snap-copy-rt-dst")
	require.NoError(t, err)
	require.NotNil(t, dst)

	assert.Equal(t, "snap-copy-rt-dst", dst.SnapshotID)
	assert.Equal(t, src.SourceVolumeName, dst.SourceVolumeName)
	assert.Equal(t, src.SourceVolumeUUID, dst.SourceVolumeUUID,
		"copy must carry the SOURCE volume identity, not a re-derived one, or clone chunk reads decrypt under the wrong nonce")
	assert.Equal(t, src.SourceVolumeNameHash, dst.SourceVolumeNameHash)
	assert.Equal(t, src.BlockCount, dst.BlockCount)

	// Metadata must authenticate under the DESTINATION id — the AAD binds
	// "snap:"||dstSnapshotID, so a config sealed for the source would fail here.
	_, ident, err := env.source.LoadSnapshotBlockMap("snap-copy-rt-dst")
	require.NoError(t, err, "copied snapshot metadata must verify under the destination id")
	assert.Equal(t, env.source.VolumeUUID, ident.SourceVolumeUUID)

	// End to end: a clone of the copy reads real plaintext, not zeros.
	clone := env.openEncryptedClone(t, "clone-of-copy", "snap-copy-rt-dst")
	require.NotEqual(t, env.source.VolumeUUID, clone.VolumeUUID,
		"clone must have its own VolumeUUID — otherwise this test is meaningless")
	for i := range blockCount {
		got, err := clone.ReadAt(i*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err, "clone-of-copy read of block %d failed", i)
		want := plaintext[i*uint64(env.source.BlockSize) : (i+1)*uint64(env.source.BlockSize)]
		assert.True(t, bytes.Equal(want, got), "block %d plaintext mismatch through the copied snapshot", i)
	}
}

// TestCopySnapshotMeta_MintsFreshStateSeqNum is the nonce-reuse guard. The
// destination JSON differs from the source's (different SnapshotID) but is
// sealed under the same key and the same VolumeUUID, so reusing the source's
// StateSeqNum would put two different plaintexts under one AES-GCM nonce.
func TestCopySnapshotMeta_MintsFreshStateSeqNum(t *testing.T) {
	env := newSnapshotEnv(t, "src-copy-seq", testKey(t, 0x42))
	_, src := seedSnapshot(t, env, "snap-copy-seq-src", 1)

	dst, err := env.source.CopySnapshotMeta("snap-copy-seq-src", "snap-copy-seq-dst")
	require.NoError(t, err)

	require.NotZero(t, src.StateSeqNum)
	assert.NotEqual(t, src.StateSeqNum, dst.StateSeqNum,
		"copy must mint a fresh StateSeqNum: the same nonce over different plaintext under one key is catastrophic for AES-GCM")
	assert.Greater(t, dst.StateSeqNum, src.StateSeqNum,
		"StateSeqNum must advance monotonically, never be reissued")

	// A second copy must advance again rather than reuse the first copy's value.
	second, err := env.source.CopySnapshotMeta("snap-copy-seq-src", "snap-copy-seq-dst2")
	require.NoError(t, err)
	assert.Greater(t, second.StateSeqNum, dst.StateSeqNum)
}

// TestCopySnapshotMeta_PersistsStateSeqNumBeforeSealing gates the crash-safety
// half of the nonce guard: the bumped counter must be durable on disk before
// the seal, or a restart resets it and a later seal re-issues the same value.
func TestCopySnapshotMeta_PersistsStateSeqNumBeforeSealing(t *testing.T) {
	env := newSnapshotEnv(t, "src-copy-durable", testKey(t, 0x42))
	seedSnapshot(t, env, "snap-copy-durable-src", 1)

	dst, err := env.source.CopySnapshotMeta("snap-copy-durable-src", "snap-copy-durable-dst")
	require.NoError(t, err)

	configPath := filepath.Join(env.dir, types.GetFilePath(types.FileTypeConfig, 0, "src-copy-durable"))
	persisted, err := env.source.LoadStateRequest(configPath)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, persisted.StateSeqNum, dst.StateSeqNum,
		"the copy's StateSeqNum must already be covered by the durable VBState, or a crash lets it be re-issued and the nonce reused")
}

// TestCopySnapshotMeta_RejectsForeignVolume gates the ownership check. The
// metadata tag does NOT catch this: the reader reconstructs nonce + AAD from
// the payload's own SourceVolumeUUID, so any volume sharing the master key
// verifies the source config fine. Only the explicit check stops a copy being
// sealed from a volume whose nextStateSeqNum counter does not govern the
// source's nonce subspace.
func TestCopySnapshotMeta_RejectsForeignVolume(t *testing.T) {
	env := newSnapshotEnv(t, "src-copy-foreign", testKey(t, 0x42))
	seedSnapshot(t, env, "snap-copy-foreign-src", 1)

	other := openEncryptedVBInDir(t, env.dir, "other-copy-foreign", env.key)
	require.NotEqual(t, env.source.VolumeUUID, other.VolumeUUID)

	// Sanity: the foreign volume CAN authenticate the source config, which is
	// exactly why the tag alone is not a sufficient guard here.
	_, _, err := other.LoadSnapshotBlockMap("snap-copy-foreign-src")
	require.NoError(t, err, "same master key verifies the blob — the ownership check is the only thing standing between this and nonce reuse")

	_, err = other.CopySnapshotMeta("snap-copy-foreign-src", "snap-copy-foreign-dst")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "copy must run on the snapshot's own volume")

	_, readErr := other.Backend.ReadFrom("snap-copy-foreign-dst", types.FileTypeBlockCheckpoint, 0, 0, 0)
	assert.ErrorIs(t, readErr, os.ErrNotExist, "a refused copy must not leave a partial destination behind")
}

// TestCopySnapshotMeta_RejectsExistingDestination gates against silently
// replacing a snapshot that volumes may already be cloned from.
func TestCopySnapshotMeta_RejectsExistingDestination(t *testing.T) {
	env := newSnapshotEnv(t, "src-copy-exists", testKey(t, 0x42))
	seedSnapshot(t, env, "snap-copy-exists-a", 1)
	seedSnapshot(t, env, "snap-copy-exists-b", 1)

	_, err := env.source.CopySnapshotMeta("snap-copy-exists-a", "snap-copy-exists-b")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "destination snap-copy-exists-b already exists")

	// The pre-existing destination is untouched and still readable.
	_, _, err = env.source.LoadSnapshotBlockMap("snap-copy-exists-b")
	require.NoError(t, err)
}

// TestCopySnapshotMeta_RejectsMissingSource gates the "describes fine, blows
// up on attach" failure mode: a copy of a source that is not on the backend
// must fail loudly rather than mint an empty destination.
func TestCopySnapshotMeta_RejectsMissingSource(t *testing.T) {
	env := newSnapshotEnv(t, "src-copy-missing", testKey(t, 0x42))

	_, err := env.source.CopySnapshotMeta("snap-copy-missing-src", "snap-copy-missing-dst")
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)

	_, readErr := env.source.Backend.ReadFrom("snap-copy-missing-dst", types.FileTypeConfig, 0, 0, 0)
	assert.ErrorIs(t, readErr, os.ErrNotExist, "a failed copy must not leave a destination config behind")
}

// TestCopySnapshotMeta_RejectsIDMismatch gates the only sanity check available
// on an unencrypted volume, where nothing binds the config bytes to the key:
// the blob read from srcSnapshotID/ must declare that same SnapshotID.
func TestCopySnapshotMeta_RejectsIDMismatch(t *testing.T) {
	env := newSnapshotEnv(t, "src-copy-mismatch", nil)
	seedSnapshot(t, env, "snap-copy-mismatch-src", 1)

	tampered, err := json.Marshal(&SnapshotState{SnapshotID: "snap-somewhere-else", SourceVolumeName: "src-copy-mismatch"})
	require.NoError(t, err)
	headers := []byte{}
	require.NoError(t, env.source.Backend.WriteTo("snap-copy-mismatch-src", types.FileTypeConfig, 0, &headers, &tampered))

	_, err = env.source.CopySnapshotMeta("snap-copy-mismatch-src", "snap-copy-mismatch-dst")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "declares SnapshotID")
}

// TestCopySnapshotMeta_RejectsSameID guards the degenerate case that would
// otherwise read and rewrite one prefix under a new StateSeqNum.
func TestCopySnapshotMeta_RejectsSameID(t *testing.T) {
	env := newSnapshotEnv(t, "src-copy-same", testKey(t, 0x42))
	seedSnapshot(t, env, "snap-copy-same-src", 1)

	_, err := env.source.CopySnapshotMeta("snap-copy-same-src", "snap-copy-same-src")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must differ from the source")

	_, err = env.source.CopySnapshotMeta("", "snap-copy-same-dst")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must both be set")
}

// TestCopySnapshotMeta_CheckpointCopiedVerbatim pins the decision that the
// checkpoint needs no re-encryption: it is plaintext block-map metadata with
// no nonce or AAD of its own, and the chunk identity a reader needs comes from
// the authenticated config, so the bytes are copied unchanged.
func TestCopySnapshotMeta_CheckpointCopiedVerbatim(t *testing.T) {
	env := newSnapshotEnv(t, "src-copy-ckpt", testKey(t, 0x42))
	seedSnapshot(t, env, "snap-copy-ckpt-src", 3)

	_, err := env.source.CopySnapshotMeta("snap-copy-ckpt-src", "snap-copy-ckpt-dst")
	require.NoError(t, err)

	srcCkpt, err := env.source.Backend.ReadFrom("snap-copy-ckpt-src", types.FileTypeBlockCheckpoint, 0, 0, 0)
	require.NoError(t, err)
	dstCkpt, err := env.source.Backend.ReadFrom("snap-copy-ckpt-dst", types.FileTypeBlockCheckpoint, 0, 0, 0)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(srcCkpt, dstCkpt), "checkpoint must be copied byte-for-byte")

	// Both prefixes resolve to the same frozen map.
	srcMap, _, err := env.source.LoadSnapshotBlockMap("snap-copy-ckpt-src")
	require.NoError(t, err)
	dstMap, _, err := env.source.LoadSnapshotBlockMap("snap-copy-ckpt-dst")
	require.NoError(t, err)
	assert.Equal(t, srcMap.lookup.len(), dstMap.lookup.len())
}

// TestCopySnapshotMeta_Unencrypted covers the plaintext path end to end so the
// encrypted-only branches are not the sole exercised code.
func TestCopySnapshotMeta_Unencrypted(t *testing.T) {
	env := newSnapshotEnv(t, "src-copy-plain", nil)
	blockCount := uint64(2)
	plaintext, src := seedSnapshot(t, env, "snap-copy-plain-src", blockCount)

	dst, err := env.source.CopySnapshotMeta("snap-copy-plain-src", "snap-copy-plain-dst")
	require.NoError(t, err)
	assert.Zero(t, src.StateSeqNum, "unencrypted snapshots allocate no StateSeqNum")
	assert.Zero(t, dst.StateSeqNum)

	clone := env.openEncryptedClone(t, "clone-of-plain-copy", "snap-copy-plain-dst")
	for i := range blockCount {
		got, err := clone.ReadAt(i*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err)
		want := plaintext[i*uint64(env.source.BlockSize) : (i+1)*uint64(env.source.BlockSize)]
		assert.True(t, bytes.Equal(want, got), "block %d mismatch through the copied snapshot", i)
	}
}
