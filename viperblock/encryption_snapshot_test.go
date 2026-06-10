// Copyright 2026 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

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
