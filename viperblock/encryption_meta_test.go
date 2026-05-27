// Copyright 2026 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

// Stage 5 metadata HMAC tests. Closes the cross-volume metadata pivot
// critical: a backend writer cannot substitute volume A's tagged
// VBState/SnapshotState into volume B's prefix, cannot bit-flip the
// JSON, and cannot rollback to an older authentic blob without losing
// the LoadState SeqNum tie-break to the local fsync'd copy.

package viperblock

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVBStateMeta_BitFlipFailsLoad — a one-byte flip in the persisted
// config.json after the HMAC seal must surface as ErrIntegrity at
// LoadStateRequest. This is the literal byte-tamper check: the JSON
// ships plaintext, the trailing 16-byte tag binds the bytes to the
// AAD via sealMeta, so any modification fails openMeta.
func TestVBStateMeta_BitFlipFailsLoad(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-meta-bitflip", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	configPath := filepath.Join(vb.BaseDir, types.GetFilePath(types.FileTypeConfig, 0, vb.GetVolume()))
	raw, err := os.ReadFile(configPath)
	require.NoError(t, err)
	require.Greater(t, len(raw), 20)
	raw[5] ^= 0x01 // flip a JSON byte
	require.NoError(t, os.WriteFile(configPath, raw, 0600))

	_, err = vb.LoadStateRequest(configPath)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIntegrity)
}

// TestVBStateMeta_TagFlipFailsLoad — flipping a byte in the trailing
// 16-byte tag must also fail. Distinct from the body flip: tag bytes
// are not parsed as JSON, so a regression that verifies the tag length
// but not its contents would pass the body-flip test and fail here.
func TestVBStateMeta_TagFlipFailsLoad(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-meta-tag", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	configPath := filepath.Join(vb.BaseDir, types.GetFilePath(types.FileTypeConfig, 0, vb.GetVolume()))
	raw, err := os.ReadFile(configPath)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 0x01 // flip last byte of tag
	require.NoError(t, os.WriteFile(configPath, raw, 0600))

	_, err = vb.LoadStateRequest(configPath)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIntegrity)
}

// TestVBStateMeta_CrossVolumeSwapFailsLoad — splicing volume A's tagged
// config.json into volume B's prefix must fail because B's runtime
// volumeNameHash differs from the seal-time hash bound into A's AAD.
// This is the live test of the cross-volume metadata pivot critical
// called out in viperblock-cmmc-level1-remediation.md (closed here by
// design, in case its archived "Complete" status was misleading).
func TestVBStateMeta_CrossVolumeSwapFailsLoad(t *testing.T) {
	key := testKey(t, 0x42)
	dir := t.TempDir()

	// Two VBs in the same dir, different volume names so they have
	// different volumeNameHashes.
	mkVB := func(name string) *VB {
		cfg := file.FileConfig{BaseDir: dir, VolumeName: name}
		vb, err := New(&VB{
			VolumeName:        name,
			VolumeSize:        4 * 1024 * 1024,
			BaseDir:           dir,
			MasterKey:         key,
			EncryptionEnabled: true,
		}, "file", cfg)
		require.NoError(t, err)
		require.NoError(t, vb.Backend.Init())
		vb.BlockSize = DefaultBlockSize
		require.NoError(t, vb.SaveState())
		return vb
	}
	volA := mkVB("vol-A-meta")
	volB := mkVB("vol-B-meta")

	configA := filepath.Join(dir, types.GetFilePath(types.FileTypeConfig, 0, volA.GetVolume()))
	configB := filepath.Join(dir, types.GetFilePath(types.FileTypeConfig, 0, volB.GetVolume()))
	rawA, err := os.ReadFile(configA)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configB, rawA, 0600))

	_, err = volB.LoadStateRequest(configB)
	require.Error(t, err, "cross-volume swap must fail HMAC verify")
	assert.ErrorIs(t, err, ErrIntegrity)
}

// TestVBStateMeta_RollbackLosesTieToFsync — write VBState v1, then v2,
// then substitute the v1 backend copy. LoadState's existing tie-break
// (highest SeqNum wins between local fsync and backend) must select
// the local v2 copy; HMAC verify of v2 succeeds. This gates the
// composition: HMAC alone does not stop replay of an older authentic
// blob — the SeqNum comparison does. An attacker who rewinds the
// backend copy is foiled because the local fsync'd copy holds a higher
// StateSeqNum and wins the tie-break.
func TestVBStateMeta_RollbackLosesTieToFsync(t *testing.T) {
	key := testKey(t, 0x42)
	dir := t.TempDir()
	cfg := file.FileConfig{BaseDir: dir, VolumeName: "vol-rollback"}
	vb, err := New(&VB{
		VolumeName:        "vol-rollback",
		VolumeSize:        4 * 1024 * 1024,
		BaseDir:           dir,
		MasterKey:         key,
		EncryptionEnabled: true,
	}, "file", cfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	vb.BlockSize = DefaultBlockSize

	// v1: first SaveState. Snapshot the backend blob.
	require.NoError(t, vb.SaveState())
	// Force a write to bump SeqNum on disk so v1 vs v2 differ by more
	// than just StateSeqNum.
	vb.SeqNum.Store(100)
	require.NoError(t, vb.SaveState())
	v1Backend, err := vb.Backend.Read(types.FileTypeConfig, 0, 0, 0)
	require.NoError(t, err)

	// v2: bump SeqNum and persist again.
	vb.SeqNum.Store(200)
	require.NoError(t, vb.SaveState())

	// Roll the BACKEND copy back to v1, leave the local fsync'd copy at
	// v2. LoadState's tie-break by SeqNum (or StateSeqNum) must prefer
	// local v2; HMAC verify of v2 succeeds.
	emptyHeaders := []byte{}
	require.NoError(t, vb.Backend.Write(types.FileTypeConfig, 0, &emptyHeaders, &v1Backend))

	// Reopen — must succeed and surface the v2 SeqNum, proving the
	// backend rollback lost the tie-break.
	vb2 := newFileBackedVB(t, "vol-rollback", key)
	vb2.BaseDir = vb.BaseDir
	require.NoError(t, vb2.LoadState())
	assert.GreaterOrEqual(t, vb2.SeqNum.Load(), uint64(200),
		"LoadState must select the higher-SeqNum local copy over the rolled-back backend")
}

// TestVBStateMeta_FirstOpenNoPriorState — a fresh encrypted volume with
// no persisted VBState must bootstrap on the first SaveState (mint
// VolumeUUID, seal a fresh blob with StateSeqNum=1). Subsequent
// LoadState must verify successfully. This is the "blank slate"
// case the metadata-HMAC chicken-and-egg discussion in the plan
// resolves: no prior tag, no verify; the helper falls through to the
// mint path.
func TestVBStateMeta_FirstOpenNoPriorState(t *testing.T) {
	key := testKey(t, 0x42)
	dir := t.TempDir()
	cfg := file.FileConfig{BaseDir: dir, VolumeName: "vol-fresh-meta"}
	vb, err := New(&VB{
		VolumeName:        "vol-fresh-meta",
		VolumeSize:        4 * 1024 * 1024,
		BaseDir:           dir,
		MasterKey:         key,
		EncryptionEnabled: true,
	}, "file", cfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	vb.BlockSize = DefaultBlockSize

	// Pre-condition: no VBState on disk yet.
	configPath := filepath.Join(dir, types.GetFilePath(types.FileTypeConfig, 0, vb.GetVolume()))
	_, statErr := os.Stat(configPath)
	require.True(t, os.IsNotExist(statErr), "fresh volume must not have a persisted VBState")

	require.NoError(t, vb.SaveState(), "first SaveState bootstraps VolumeUUID and seals fresh blob")
	var zero [4]byte
	assert.NotEqual(t, zero, vb.VolumeUUID, "first SaveState must mint VolumeUUID")

	// Reopen — LoadState must verify the freshly-sealed blob.
	vb2 := newFileBackedVB(t, "vol-fresh-meta", key)
	vb2.BaseDir = vb.BaseDir
	require.NoError(t, vb2.LoadState())
	assert.Equal(t, vb.VolumeUUID, vb2.VolumeUUID)
}

// TestSnapshotMeta_BitFlipFailsLoad — same primitive as VBState but
// scoped to SnapshotState. A bit-flip in the persisted snapshot config
// (post-seal) must fail at LoadSnapshotBlockMap.
func TestSnapshotMeta_BitFlipFailsLoad(t *testing.T) {
	env := newSnapshotEnv(t, "vol-snapmeta-bitflip", testKey(t, 0x42))
	data := make([]byte, env.source.BlockSize)
	_, err := rand.Read(data)
	require.NoError(t, err)
	require.NoError(t, env.source.Write(0, data))
	require.NoError(t, env.source.Flush())
	require.NoError(t, env.source.WriteWALToChunk(true))

	snapshotID := "snap-bitflip"
	_, err = env.source.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	configPath := filepath.Join(env.dir, types.GetFilePath(types.FileTypeConfig, 0, snapshotID))
	raw, err := os.ReadFile(configPath)
	require.NoError(t, err)
	raw[10] ^= 0x01
	require.NoError(t, os.WriteFile(configPath, raw, 0600))

	_, _, err = env.source.LoadSnapshotBlockMap(snapshotID)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIntegrity)
}

// TestBlockStoreReadEncrypted — the BlockStore-enabled read path
// (readBlockStore in viperblock.go) is a parallel implementation to the
// legacy `read`. Each must independently decrypt chunks under the
// volume's identity. The smoke covered by TestEncryptedRoundTrip_*
// only checks one round-trip; this is the explicit assertion that the
// BlockStore path's openChunkRun call wraps integrity failures the same
// way as the legacy path.
func TestBlockStoreReadEncrypted(t *testing.T) {
	env := newEncryptedTamperEnv(t, "vol-blockstore-decrypt", testKey(t, 0x42))

	plaintext := make([]byte, env.blockSize)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)

	env.vb.UseBlockStore = true
	env.writeAndChunk(t, 0, plaintext)

	// Sanity: BlockStore path returns plaintext.
	got, err := env.vb.ReadAt(0, uint64(env.blockSize))
	require.NoError(t, err)
	require.Equal(t, plaintext, got, "BlockStore decrypt baseline failed")

	// Now tamper a byte on the chunk and verify the BlockStore path
	// also fails closed.
	env.vb.BlockStore = NewUnifiedBlockStore(env.vb.BlockSize)
	env.vb.BlocksToObject.mu.Lock()
	env.vb.BlocksToObject.BlockLookup[0] = BlockLookup{
		StartBlock:   0,
		NumBlocks:    1,
		ObjectID:     0,
		ObjectOffset: uint32(env.blockOffset(0)),
		SeqNum:       1,
	}
	env.vb.BlocksToObject.mu.Unlock()
	env.vb.BlockStore.SetPersisted(0, 0, uint32(env.blockOffset(0)), 1)

	raw, err := os.ReadFile(env.chunkPath(env.vb.VolumeName, 0))
	require.NoError(t, err)
	raw[env.blockOffset(0)+8] ^= 0x01
	require.NoError(t, os.WriteFile(env.chunkPath(env.vb.VolumeName, 0), raw, 0600))

	_, err = env.vb.ReadAt(0, uint64(env.blockSize))
	require.Error(t, err, "BlockStore path must surface integrity failure")
	assert.ErrorIs(t, err, ErrIntegrity)
}

// TestVBStateMeta_TruncatedBlobFails — a backend-side truncation of
// the VBState blob below the 16-byte tag boundary must surface a
// clear error rather than panic on the slice arithmetic in
// LoadStateRequest. Defensive: an external operator who half-uploads
// a backend object should see a refused open, not a runtime panic.
func TestVBStateMeta_TruncatedBlobFails(t *testing.T) {
	key := testKey(t, 0x42)
	dir := t.TempDir()
	cfg := file.FileConfig{BaseDir: dir, VolumeName: "vol-trunc"}
	vb, err := New(&VB{
		VolumeName:        "vol-trunc",
		VolumeSize:        4 * 1024 * 1024,
		BaseDir:           dir,
		MasterKey:         key,
		EncryptionEnabled: true,
	}, "file", cfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	configPath := filepath.Join(dir, types.GetFilePath(types.FileTypeConfig, 0, vb.GetVolume()))
	// Truncate to fewer bytes than the tag length — must fail with a
	// clear "shorter than tag" message, not a slice-bounds panic.
	require.NoError(t, os.WriteFile(configPath, []byte{0x01, 0x02, 0x03}, 0600))

	_, err = vb.LoadStateRequest(configPath)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIntegrity)
	// Make sure the error message is informative.
	assert.Contains(t, err.Error(), "shorter")
}
