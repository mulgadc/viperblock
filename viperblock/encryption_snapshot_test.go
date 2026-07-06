// Snapshot/clone encryption tests. Snapshot clones inherit the
// source volume's encryption identity (SourceVolumeUUID,
// SourceVolumeNameHash) so base-chunk reads decrypt under the source's
// nonce + AAD, not the clone's. Any drift between source and clone
// identity surfaces as ErrIntegrity; any tampering of the SnapshotState
// blob surfaces as a metadata HMAC failure.

package viperblock

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snapshotEnv pairs a source VB with a shared file-backend BaseDir so
// clones can reach the source's chunks via Backend.ReadFrom. Both VBs
// place data under {baseDir}/{volumeName}/... so a clone opened with
// SourceVolumeName=source.VolumeName will resolve the source's chunks
// without any S3 indirection.
type snapshotEnv struct {
	source *VB
	dir    string
	key    *masterkey.Key
}

func newSnapshotEnv(t *testing.T, sourceName string, key *masterkey.Key) *snapshotEnv {
	t.Helper()
	dir := t.TempDir()
	source := openEncryptedVBInDir(t, dir, sourceName, key)
	return &snapshotEnv{source: source, dir: dir, key: key}
}

// openEncryptedVBInDir constructs an encrypted VB with file backend rooted
// at dir, opens WAL files, and seeds VBState. Shared between source and
// clone construction so the BaseDir + backend BaseDir + volume layout
// stays consistent.
func openEncryptedVBInDir(t *testing.T, dir, volumeName string, key *masterkey.Key) *VB {
	t.Helper()
	cfg := file.FileConfig{BaseDir: dir, VolumeName: volumeName}
	vb, err := New(&VB{
		VolumeName:        volumeName,
		VolumeSize:        64 * 1024 * 1024,
		BaseDir:           dir,
		MasterKey:         key,
		EncryptionEnabled: key != nil,
		WALSyncInterval:   -1,
	}, "file", cfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	vb.BlockSize = DefaultBlockSize
	vb.ObjBlockSize = 16 * DefaultBlockSize
	require.NoError(t, vb.SaveState())
	require.NoError(t, vb.OpenWAL(&vb.WAL,
		fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL,
		fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))
	return vb
}

// openEncryptedClone constructs an encrypted clone VB rooted at the same
// dir as its source, loads the snapshot's base map, and returns the clone
// ready for reads/writes.
func (env *snapshotEnv) openEncryptedClone(t *testing.T, cloneName, snapshotID string) *VB {
	t.Helper()
	clone := openEncryptedVBInDir(t, env.dir, cloneName, env.key)
	require.NoError(t, clone.OpenFromSnapshot(snapshotID))
	return clone
}

// TestSnapshotEncrypted_PersistsSourceIdentity gates that CreateSnapshot
// writes the source's VolumeUUID and volumeNameHash into SnapshotState in
// hex form, and that LoadSnapshotBlockMap recovers both verbatim. Without
// this, an encrypted clone has no way to reconstruct the source's nonce
// + AAD and every base-chunk read fails.
func TestSnapshotEncrypted_PersistsSourceIdentity(t *testing.T) {
	env := newSnapshotEnv(t, "src-persist", testKey(t, 0x42))

	data := make([]byte, env.source.BlockSize*4)
	_, err := rand.Read(data)
	require.NoError(t, err)
	require.NoError(t, env.source.Write(0, data))
	require.NoError(t, env.source.Flush())
	require.NoError(t, env.source.WriteWALToChunk(true))

	snapshotID := "snap-persist"
	snap, err := env.source.CreateSnapshot(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, snap)

	assert.Equal(t, hex.EncodeToString(env.source.VolumeUUID[:]), snap.SourceVolumeUUID,
		"snapshot must persist source VolumeUUID in hex")
	assert.Equal(t, hex.EncodeToString(env.source.volumeNameHash[:]), snap.SourceVolumeNameHash,
		"snapshot must persist source volumeNameHash in hex")
	assert.NotZero(t, snap.StateSeqNum, "snapshot must allocate a StateSeqNum so its metadata HMAC is unique")

	_, ident, err := env.source.LoadSnapshotBlockMap(snapshotID)
	require.NoError(t, err)
	assert.Equal(t, env.source.VolumeUUID, ident.SourceVolumeUUID,
		"LoadSnapshotBlockMap must hex-decode SourceVolumeUUID back to [4]byte")
	assert.Equal(t, env.source.volumeNameHash, ident.SourceVolumeNameHash)
}

// TestSnapshotEncrypted_SnapshotBeforeSaveStateMintsConsistentUUID gates the
// AMI-import ordering. ImportDiskImage writes blocks and snapshots WITHOUT a
// prior SaveState, so VolumeUUID is still zero entering CreateSnapshot, where
// SaveState mints it. CreateSnapshot must record the SAME (minted) UUID it
// seals the metadata nonce under — recording the pre-mint zero while sealing
// under the minted value makes LoadSnapshotBlockMap reconstruct the wrong
// nonce and fail tag verify. This is the exact "snapshot ... tag verify:
// message authentication failed" seen on every encrypted AMI launch.
func TestSnapshotEncrypted_SnapshotBeforeSaveStateMintsConsistentUUID(t *testing.T) {
	dir := t.TempDir()
	key := testKey(t, 0x42)
	cfg := file.FileConfig{BaseDir: dir, VolumeName: "src-import-order"}
	vb, err := New(&VB{
		VolumeName:        "src-import-order",
		VolumeSize:        64 * 1024 * 1024,
		BaseDir:           dir,
		MasterKey:         key,
		EncryptionEnabled: true,
		WALSyncInterval:   -1,
	}, "file", cfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	vb.BlockSize = DefaultBlockSize
	vb.ObjBlockSize = 16 * DefaultBlockSize
	// Deliberately NO SaveState here — mirrors ImportDiskImage, which leaves
	// VolumeUUID unminted entering CreateSnapshot.
	require.NoError(t, vb.OpenWAL(&vb.WAL,
		fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL,
		fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	var zero [4]byte
	require.Equal(t, zero, vb.VolumeUUID, "precondition: VolumeUUID must be unminted entering the snapshot")

	data := make([]byte, vb.BlockSize*4)
	_, err = rand.Read(data)
	require.NoError(t, err)
	require.NoError(t, vb.Write(0, data))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	snapshotID := "snap-import-order"
	snap, err := vb.CreateSnapshot(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, snap)

	require.NotEqual(t, zero, vb.VolumeUUID, "CreateSnapshot must mint VolumeUUID")
	assert.Equal(t, hex.EncodeToString(vb.VolumeUUID[:]), snap.SourceVolumeUUID,
		"snapshot must record the minted UUID it sealed the nonce under, not the pre-mint zero")

	// Decisive gate: the metadata envelope must verify. Pre-fix this returns
	// ErrIntegrity — sealed under the minted UUID, recorded (and reopened) as zero.
	_, ident, err := vb.LoadSnapshotBlockMap(snapshotID)
	require.NoError(t, err, "snapshot metadata must verify when VolumeUUID is minted during CreateSnapshot")
	assert.Equal(t, vb.VolumeUUID, ident.SourceVolumeUUID)
}

// TestSnapshotEncrypted_ReopenPreservesSnapshotLink gates the LoadState
// ordering for encrypted snapshot clones. The encrypted SeqNum bootstrap
// (bumpSeqNumHighWater) durably rewrites config.json during LoadState; it must
// run AFTER OpenFromSnapshot restores SnapshotID, or the rewritten config
// records an empty SnapshotID. The next open then loads no base map and serves
// an all-zeros disk — the exact "guest drops to UEFI shell, no FS0:" failure on
// every encrypted AMI launch. In-memory SnapshotID is restored either way
// (OpenFromSnapshot runs at the end), so this asserts the PERSISTED config and
// a fresh-open base read, which is what the runtime nbdkit plugin process sees.
func TestSnapshotEncrypted_ReopenPreservesSnapshotLink(t *testing.T) {
	env := newSnapshotEnv(t, "src-reopen", testKey(t, 0x42))

	blockCount := uint64(4)
	plaintext := make([]byte, uint64(env.source.BlockSize)*blockCount)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)
	require.NoError(t, env.source.Write(0, plaintext))
	require.NoError(t, env.source.Flush())
	require.NoError(t, env.source.WriteWALToChunk(true))

	snapshotID := "snap-reopen"
	_, err = env.source.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	// Open an encrypted clone and persist its state — config.json now records
	// SnapshotID, exactly as RunInstances' cloneAMIToVolume does.
	clone := env.openEncryptedClone(t, "clone-reopen", snapshotID)
	require.NoError(t, clone.SaveState())
	require.Equal(t, snapshotID, clone.SnapshotID)

	// Reopen the clone. Pre-fix, LoadState's bumpSeqNumHighWater persists
	// config.json before OpenFromSnapshot restores the link, clobbering
	// SnapshotID on disk.
	reopened := newCloneReopen(t, env.dir, "clone-reopen", env.key)
	require.NoError(t, reopened.LoadState())

	// Decisive gate 1: the persisted config must still carry the snapshot link.
	configPath := filepath.Join(env.dir, types.GetFilePath(types.FileTypeConfig, 0, "clone-reopen"))
	persistedState, err := reopened.LoadStateRequest(configPath)
	require.NoError(t, err)
	require.Equal(t, snapshotID, persistedState.SnapshotID,
		"LoadState must not persist an empty SnapshotID — that bricks base-map reads on the next open")

	// Decisive gate 2: a brand-new open (mirrors the nbdkit plugin process)
	// must load the base map from disk and decrypt source blocks, not zeros.
	fresh := newCloneReopen(t, env.dir, "clone-reopen", env.key)
	require.NoError(t, fresh.LoadState())
	require.Equal(t, snapshotID, fresh.SnapshotID, "fresh open must recover SnapshotID from disk")
	got, err := fresh.ReadAt(0, uint64(fresh.BlockSize))
	require.NoError(t, err, "fresh open of an encrypted clone must read base blocks, not an all-zeros disk")
	assert.True(t, bytes.Equal(plaintext[:fresh.BlockSize], got), "base block 0 must decrypt to source plaintext")
}

// newCloneReopen constructs a fresh encrypted VB over an existing on-disk
// volume in dir WITHOUT a pre-write SaveState, so LoadState reads the persisted
// config exactly as a cold reopen (or the out-of-process nbdkit plugin) would.
func newCloneReopen(t *testing.T, dir, volumeName string, key *masterkey.Key) *VB {
	t.Helper()
	cfg := file.FileConfig{BaseDir: dir, VolumeName: volumeName}
	vb, err := New(&VB{
		VolumeName:        volumeName,
		VolumeSize:        64 * 1024 * 1024,
		BaseDir:           dir,
		MasterKey:         key,
		EncryptionEnabled: key != nil,
		WALSyncInterval:   -1,
	}, "file", cfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	vb.BlockSize = DefaultBlockSize
	vb.ObjBlockSize = 16 * DefaultBlockSize
	return vb
}

// TestSnapshotEncrypted_CloneReadsBaseChunks — the clone opens the
// snapshot and reads blocks 0-3 through the base-chunk path. Each read
// must decrypt the source's chunks under the source's identity (the
// clone's own VolumeUUID would produce a nonce mismatch and ErrIntegrity).
// This is the end-to-end gate that SourceVolumeUUID / sourceVolumeNameHash
// are plumbed correctly through OpenFromSnapshot →
// fetchBaseBlocksFromBackend → openChunkRun.
func TestSnapshotEncrypted_CloneReadsBaseChunks(t *testing.T) {
	env := newSnapshotEnv(t, "src-clone-read", testKey(t, 0x42))

	blockCount := uint64(4)
	plaintext := make([]byte, uint64(env.source.BlockSize)*blockCount)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)

	require.NoError(t, env.source.Write(0, plaintext))
	require.NoError(t, env.source.Flush())
	require.NoError(t, env.source.WriteWALToChunk(true))

	snapshotID := "snap-clone-read"
	_, err = env.source.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	clone := env.openEncryptedClone(t, "clone-base", snapshotID)
	require.NotEqual(t, env.source.VolumeUUID, clone.VolumeUUID,
		"clone must have its own VolumeUUID — otherwise this test is meaningless")
	require.Equal(t, env.source.VolumeUUID, clone.SourceVolumeUUID,
		"clone must carry the source VolumeUUID for base-chunk decrypt")

	for i := range blockCount {
		got, err := clone.ReadAt(i*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err, "clone read of block %d failed", i)
		want := plaintext[i*uint64(env.source.BlockSize) : (i+1)*uint64(env.source.BlockSize)]
		assert.True(t, bytes.Equal(want, got), "block %d plaintext mismatch", i)
	}
}

// TestSnapshotEncrypted_CrossVolumeSubstitutionFailsHMAC — splicing a
// snapshot from a different source into this snapshot's prefix (or
// rewriting the hex SourceVolumeUUID field) must fail the metadata tag.
// The AAD binds volumeNameHash, "snap:"||snapshotID, and StateSeqNum;
// the JSON bytes are covered by the tag (sealMeta appends them to the
// AAD), so any modification to the JSON content fails before any base
// chunk is read. This is the cross-volume metadata pivot closer for
// snapshots — the symmetric counterpart of the VBState test.
func TestSnapshotEncrypted_CrossVolumeSubstitutionFailsHMAC(t *testing.T) {
	env := newSnapshotEnv(t, "src-substitute", testKey(t, 0x42))

	data := make([]byte, env.source.BlockSize)
	_, err := rand.Read(data)
	require.NoError(t, err)
	require.NoError(t, env.source.Write(0, data))
	require.NoError(t, env.source.Flush())
	require.NoError(t, env.source.WriteWALToChunk(true))

	snapshotID := "snap-substitute"
	_, err = env.source.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	// Tamper the SourceVolumeUUID in the JSON — this is what an attacker
	// with backend write access would do to pivot a clone onto a
	// different volume's chunks.
	configPath := filepath.Join(env.dir, types.GetFilePath(types.FileTypeConfig, 0, snapshotID))
	raw, err := os.ReadFile(configPath)
	require.NoError(t, err)
	// Split the envelope so we can rewrite the SourceVolumeUUID field in the
	// payload, then re-wrap with the ORIGINAL tag — verify must fail because
	// the payload bytes (bound into the AAD via sealMeta) have changed.
	payload, tag, err := splitEnvelope(raw)
	require.NoError(t, err)
	var snap SnapshotState
	require.NoError(t, json.Unmarshal(payload, &snap))
	require.NotEmpty(t, snap.SourceVolumeUUID)
	// Flip the first hex char.
	tampered := []byte(snap.SourceVolumeUUID)
	if tampered[0] == '0' {
		tampered[0] = '1'
	} else {
		tampered[0] = '0'
	}
	snap.SourceVolumeUUID = string(tampered)
	tamperedJSON, err := json.Marshal(snap)
	require.NoError(t, err)
	newBlob := fmt.Appendf(nil, `{"v":1,"payload":%s,"authtag":"%s"}`,
		tamperedJSON, base64.StdEncoding.EncodeToString(tag))
	require.NoError(t, os.WriteFile(configPath, newBlob, 0600))

	_, _, err = env.source.LoadSnapshotBlockMap(snapshotID)
	require.Error(t, err, "tampered SnapshotState must fail HMAC verify")
	assert.ErrorIs(t, err, ErrIntegrity)
}

// TestSnapshotEncrypted_CloneWriteUsesCloneIdentity — after the clone
// writes to a block that wasn't in the snapshot, that block must be
// sealed under the clone's own VolumeUUID + volumeNameHash (not the
// source's). Read back via the clone returns the plaintext.
//
// Reading a block that IS in the snapshot still goes via the source
// identity. Branching on (BlocksToObject hit vs BaseBlockMap hit) is
// the production-side switch this test gates.
func TestSnapshotEncrypted_CloneWriteUsesCloneIdentity(t *testing.T) {
	env := newSnapshotEnv(t, "src-clone-write", testKey(t, 0x42))

	srcData := make([]byte, env.source.BlockSize)
	_, err := rand.Read(srcData)
	require.NoError(t, err)
	require.NoError(t, env.source.Write(0, srcData))
	require.NoError(t, env.source.Flush())
	require.NoError(t, env.source.WriteWALToChunk(true))

	snapshotID := "snap-clone-write"
	_, err = env.source.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	clone := env.openEncryptedClone(t, "clone-write", snapshotID)

	// Clone writes a new block (not in the snapshot) — must seal under
	// clone identity.
	cloneData := make([]byte, clone.BlockSize)
	_, err = rand.Read(cloneData)
	require.NoError(t, err)
	require.NoError(t, clone.Write(10, cloneData))
	require.NoError(t, clone.Flush())
	require.NoError(t, clone.WriteWALToChunk(true))

	// Read block 10 via clone — its own backend chunk, sealed under
	// clone identity.
	got, err := clone.ReadAt(10*uint64(clone.BlockSize), uint64(clone.BlockSize))
	require.NoError(t, err)
	assert.True(t, bytes.Equal(cloneData, got),
		"clone read of its own block must round-trip plaintext")

	// And block 0 still reads from the snapshot under source identity.
	got, err = clone.ReadAt(0, uint64(clone.BlockSize))
	require.NoError(t, err)
	assert.True(t, bytes.Equal(srcData, got),
		"clone read of source-snapshot block must still decrypt under source identity")
}

// TestEncryptedChunk_LiveCheckpointSeqNumConsistency gates the ordering fix in
// createChunkFile (mulga-be2ev): after a chunk is sealed, SaveLiveCheckpoint is
// called immediately so the recorded BlockLookup.SeqNum matches the seal-time
// SeqNum. A stale SeqNum causes the nonce reconstructed on ReadAt to diverge
// from the seal nonce, making AES-GCM tag verification fail with ErrIntegrity.
//
// This mirrors the crash/remount scenario: a VB drains to a chunk, then a
// "remounted" VB loads the live checkpoint and reads the same blocks back via
// the backend. If the checkpoint SeqNum is stale (pre-fix: 30s async timer;
// runtime: leaked goroutine overwriting with clone-point data), ReadAt fails.
func TestEncryptedChunk_LiveCheckpointSeqNumConsistency(t *testing.T) {
	dir := t.TempDir()
	key := testKey(t, 0x77)

	// Writer: seal blocks into a chunk. Our fix has createChunkFile call
	// SaveLiveCheckpoint immediately after updating BlockLookup.
	writer := openEncryptedVBInDir(t, dir, "seqnum-consistency", key)
	defer writer.StopChunkUploader()

	data := make([]byte, writer.BlockSize*4)
	_, err := rand.Read(data)
	require.NoError(t, err)
	require.NoError(t, writer.Write(0, data))
	require.NoError(t, writer.Flush())
	// WriteWALToChunk → createChunkFile → SaveLiveCheckpoint (ordering fix).
	require.NoError(t, writer.WriteWALToChunk(true))

	// Remount: fresh VB over the same backend loading only from the live
	// checkpoint — no in-memory write buffer. This is what happens on remount
	// after a crash or after a leaked goroutine overwrites the checkpoint.
	remounted := newCloneReopen(t, dir, "seqnum-consistency", key)
	require.NoError(t, remounted.LoadState())
	require.NoError(t, remounted.LoadLiveCheckpoint())

	// Every BlockLookup entry in the live checkpoint must carry the exact
	// SeqNum that was used to seal the corresponding chunk ciphertext.
	writer.BlocksToObject.mu.RLock()
	defer writer.BlocksToObject.mu.RUnlock()
	for blockNum, want := range writer.BlocksToObject.BlockLookup {
		got, ok := remounted.BlocksToObject.BlockLookup[blockNum]
		require.True(t, ok, "block %d missing from live checkpoint after createChunkFile", blockNum)
		assert.Equal(t, want.SeqNum, got.SeqNum,
			"block %d: live checkpoint SeqNum must match seal-time SeqNum (AEAD nonce component)", blockNum)
	}

	// ReadAt on the remounted VB must decrypt using the checkpoint SeqNum.
	// Pre-fix, a stale SeqNum here returns ErrIntegrity (cipher: message
	// authentication failed) — the exact error logged by nbdkit on env19.
	got, err := remounted.ReadAt(0, uint64(remounted.BlockSize))
	require.NoError(t, err, "ReadAt on remounted encrypted VB must not fail with AEAD auth error")
	assert.True(t, bytes.Equal(data[:remounted.BlockSize], got),
		"remounted read must decrypt to original plaintext")
}
