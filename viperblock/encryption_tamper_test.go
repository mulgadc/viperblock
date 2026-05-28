// Copyright 2026 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

// Stage 5 tamper and on-disk integrity tests. Every test here exercises an
// adversary action that the AEAD construction is supposed to detect:
// - on-disk plaintext leakage (confidentiality — SC.L1-3.13.1)
// - ciphertext / tag bit-flip (integrity — SI.L1-3.14.2)
// - cross-volume chunk splice (AAD volumeNameHash binding)
// - in-place rollback (AAD seqNum binding)
// - WAL replay tamper (same primitive, WAL domain)
// - pre-encryption WAL magic under encrypted runtime (migration gate)

package viperblock

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// encryptedTamperEnv is the shared fixture for chunk/WAL tamper tests: a
// file-backed encrypted VB with WAL files already open and a known
// (BlockSize, ObjBlockSize) so on-disk offsets are predictable. ObjBlockSize
// is intentionally small (16 blocks) so most tests fit in a single chunk
// and the cross-chunk plumbing isn't on the critical path.
type encryptedTamperEnv struct {
	vb        *VB
	dir       string
	key       *masterkey.Key
	blockSize int
	stride    int // BlockSize + 16 (AEAD tag)
	chunkHdr  int // ChunkHeaderSize()
}

func newEncryptedTamperEnv(t *testing.T, volume string, key *masterkey.Key) *encryptedTamperEnv {
	t.Helper()
	dir := t.TempDir()
	cfg := file.FileConfig{BaseDir: dir, VolumeName: volume}
	vb, err := New(&VB{
		VolumeName:        volume,
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

	return &encryptedTamperEnv{
		vb:        vb,
		dir:       dir,
		key:       key,
		blockSize: int(vb.BlockSize),
		stride:    int(vb.BlockSize) + 16,
		chunkHdr:  vb.ChunkHeaderSize(),
	}
}

// writeAndChunk writes data starting at the given block, flushes to WAL,
// and consolidates the WAL into a chunk file. After it returns the block
// lookup points to the new chunk; reads exercise the backend decrypt path.
func (env *encryptedTamperEnv) writeAndChunk(t *testing.T, block uint64, data []byte) {
	t.Helper()
	require.NoError(t, env.vb.Write(block, data))
	require.NoError(t, env.vb.Flush())
	require.NoError(t, env.vb.WriteWALToChunk(true))
}

// chunkPath returns the on-disk path of the file backend's chunk file. The
// file backend writes to {BaseDir}/{volume}/chunks/chunk.NNNNNNNN.bin —
// see types.GetFilePath(FileTypeChunk, ...).
func (env *encryptedTamperEnv) chunkPath(volume string, chunkID uint64) string {
	return filepath.Join(env.dir, types.GetFilePath(types.FileTypeChunk, chunkID, volume))
}

// blockOffset returns the on-disk byte offset of the i-th block's
// ciphertext within an encrypted chunk file (header + i*stride).
func (env *encryptedTamperEnv) blockOffset(i int) int {
	return env.chunkHdr + i*env.stride
}

// patternBlock fills a block-sized buffer with a recognizable byte
// pattern so the on-disk plaintext-leakage test can check whether the
// raw bytes contain the plaintext.
func patternBlock(blockSize int, marker byte) []byte {
	b := make([]byte, blockSize)
	for i := range b {
		b[i] = marker
	}
	return b
}

// TestEncryptedChunk_OnDiskNotPlaintext writes a recognizable plaintext
// pattern, consolidates to a chunk file, and asserts (a) the chunk header
// carries the VBCE magic, (b) the plaintext pattern does not appear
// anywhere in the on-disk bytes. The block body and the 16-byte tag are
// the only payload after the header, so a leak here means seal-on-write
// regressed.
func TestEncryptedChunk_OnDiskNotPlaintext(t *testing.T) {
	env := newEncryptedTamperEnv(t, "vol-onsite-plain", testKey(t, 0x42))

	data := patternBlock(env.blockSize, 0xAB) // a 4KiB run of 0xAB
	env.writeAndChunk(t, 0, data)

	raw, err := os.ReadFile(env.chunkPath(env.vb.VolumeName, 0))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(raw), env.chunkHdr+env.stride, "chunk must contain header + one sealed block")
	assert.Equal(t, "VBCE", string(raw[:4]), "encrypted chunks must carry VBCE magic")

	// The whole-block 0xAB pattern as a contiguous run must not appear in
	// the on-disk bytes — AEAD ciphertext is indistinguishable from random
	// to an attacker without the key. We allow short coincidental 0xAB
	// runs (a 16-byte run has ~2^-128 expected occurrences in random
	// bytes) but a full BlockSize run is statistically impossible.
	assert.False(t, bytes.Contains(raw, data), "encrypted chunk must not contain block-sized plaintext run")
}

// TestEncryptedChunk_TamperByteFailsRead — flipping a single byte inside
// a sealed block's ciphertext body must surface as ErrIntegrity on read.
// Sliced thin: the AEAD already catches this; we gate that the wrapper
// path (openChunkRun in viperblock.go) wraps it in ErrIntegrity and
// returns fail-closed rather than handing back partial plaintext.
func TestEncryptedChunk_TamperByteFailsRead(t *testing.T) {
	env := newEncryptedTamperEnv(t, "vol-byte-tamper", testKey(t, 0x42))

	plaintext := make([]byte, env.blockSize)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)
	env.writeAndChunk(t, 0, plaintext)

	// Read once via the decrypt path to confirm the baseline works — if
	// this fails, the tamper test is meaningless because we'd be reading
	// from the in-memory cache instead of the backend.
	got, err := env.vb.ReadAt(0, uint64(env.blockSize))
	require.NoError(t, err)
	require.True(t, bytes.Equal(plaintext, got), "baseline decrypt failed; tamper test would be meaningless")

	// Clear caches so the next read must hit the backend.
	env.vb.BlockStore = NewUnifiedBlockStore(env.vb.BlockSize)
	env.vb.BlocksToObject.mu.Lock()
	env.vb.BlocksToObject.BlockLookup[0] = BlockLookup{
		StartBlock:   0,
		NumBlocks:    1,
		ObjectID:     0,
		ObjectOffset: uint32(env.blockOffset(0)),
		SeqNum:       1, // first write under a fresh VB allocates SeqNum 1
	}
	env.vb.BlocksToObject.mu.Unlock()
	env.vb.UseBlockStore = false

	chunkFile := env.chunkPath(env.vb.VolumeName, 0)
	raw, err := os.ReadFile(chunkFile)
	require.NoError(t, err)
	// Flip a byte in the middle of the ciphertext body.
	raw[env.blockOffset(0)+10] ^= 0x01
	require.NoError(t, os.WriteFile(chunkFile, raw, 0600))

	_, err = env.vb.ReadAt(0, uint64(env.blockSize))
	require.Error(t, err, "byte-tampered chunk must fail integrity check")
	assert.ErrorIs(t, err, ErrIntegrity)
}

// TestEncryptedChunk_TagTamperFailsRead — flipping a byte inside the
// trailing 16-byte tag, not the ciphertext body, must also fail. Same
// primitive; this exercises the tag-region detection specifically so a
// regression that excludes the tag from the verify step shows up here.
func TestEncryptedChunk_TagTamperFailsRead(t *testing.T) {
	env := newEncryptedTamperEnv(t, "vol-tag-tamper", testKey(t, 0x42))

	plaintext := make([]byte, env.blockSize)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)
	env.writeAndChunk(t, 0, plaintext)

	env.vb.BlockStore = NewUnifiedBlockStore(env.vb.BlockSize)
	env.vb.UseBlockStore = false
	env.vb.BlocksToObject.mu.Lock()
	env.vb.BlocksToObject.BlockLookup[0] = BlockLookup{
		StartBlock:   0,
		NumBlocks:    1,
		ObjectID:     0,
		ObjectOffset: uint32(env.blockOffset(0)),
		SeqNum:       1,
	}
	env.vb.BlocksToObject.mu.Unlock()

	chunkFile := env.chunkPath(env.vb.VolumeName, 0)
	raw, err := os.ReadFile(chunkFile)
	require.NoError(t, err)
	// Last byte of the first block's tag.
	raw[env.blockOffset(0)+env.stride-1] ^= 0x01
	require.NoError(t, os.WriteFile(chunkFile, raw, 0600))

	_, err = env.vb.ReadAt(0, uint64(env.blockSize))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIntegrity)
}

// TestEncryptedChunk_CrossVolumeSpliceFailsRead — sealing volume A's
// chunk file, then overwriting volume B's chunk file with A's bytes at
// the same offset, must fail integrity on B because the AAD's
// volumeNameHash bound at seal time was A's, but B reconstructs the AAD
// using B's volumeNameHash. This is SI.L1-3.14.2's positive case: a
// backend writer with access to A's chunks cannot splice them into B's
// prefix and have B read them as plaintext.
func TestEncryptedChunk_CrossVolumeSpliceFailsRead(t *testing.T) {
	key := testKey(t, 0x42) // same key — only the volume identity differs
	envA := newEncryptedTamperEnv(t, "vol-A", key)
	envB := newEncryptedTamperEnv(t, "vol-B", key)

	// Both volumes must share an on-disk root so swapping the chunk file
	// path is meaningful — but each test env uses its own t.TempDir(),
	// so we copy A's chunk bytes into B's chunk file directly.
	plaintext := make([]byte, envA.blockSize)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)

	envA.writeAndChunk(t, 0, plaintext)
	// Make B write *something* so its chunk-0 file exists with the right
	// header. We overwrite the body in-place below.
	envB.writeAndChunk(t, 0, plaintext)

	aBytes, err := os.ReadFile(envA.chunkPath(envA.vb.VolumeName, 0))
	require.NoError(t, err)

	// Splice block 0's sealed bytes from A into B's chunk file. Header
	// stays B's (same magic, same version, same blocksize for an
	// encrypted chunk), only the [ciphertext|tag] region is swapped.
	bBytes, err := os.ReadFile(envB.chunkPath(envB.vb.VolumeName, 0))
	require.NoError(t, err)
	copy(bBytes[envB.blockOffset(0):envB.blockOffset(0)+envB.stride],
		aBytes[envA.blockOffset(0):envA.blockOffset(0)+envA.stride])
	require.NoError(t, os.WriteFile(envB.chunkPath(envB.vb.VolumeName, 0), bBytes, 0600))

	// Force B's read to hit the backend rather than its in-memory cache.
	envB.vb.BlockStore = NewUnifiedBlockStore(envB.vb.BlockSize)
	envB.vb.UseBlockStore = false
	envB.vb.BlocksToObject.mu.Lock()
	envB.vb.BlocksToObject.BlockLookup[0] = BlockLookup{
		StartBlock:   0,
		NumBlocks:    1,
		ObjectID:     0,
		ObjectOffset: uint32(envB.blockOffset(0)),
		SeqNum:       1,
	}
	envB.vb.BlocksToObject.mu.Unlock()

	_, err = envB.vb.ReadAt(0, uint64(envB.blockSize))
	require.Error(t, err, "cross-volume chunk splice must fail integrity")
	assert.ErrorIs(t, err, ErrIntegrity)
}

// TestEncryptedChunk_InPlaceReplayFailsRead — block 5 written at SeqNum X
// then again at SeqNum Y lives in two different chunks (the second write
// allocates a new SeqNum and a new chunk). Splicing the SeqNum-X
// ciphertext+tag into the SeqNum-Y chunk at the same per-block offset
// must fail integrity: the BlockLookup says SeqNum Y, so the read
// reconstructs the AAD with Y, but the sealed bytes were bound to X.
// This is the only way to detect within-volume rollback when the
// attacker has both an old ciphertext and write access to the backend.
func TestEncryptedChunk_InPlaceReplayFailsRead(t *testing.T) {
	env := newEncryptedTamperEnv(t, "vol-replay", testKey(t, 0x42))

	older := make([]byte, env.blockSize)
	newer := make([]byte, env.blockSize)
	_, err := rand.Read(older)
	require.NoError(t, err)
	_, err = rand.Read(newer)
	require.NoError(t, err)

	// First write+chunk: block 5 at SeqNum N. Lookup -> chunk 0.
	env.writeAndChunk(t, 5, older)

	// Second write+chunk: block 5 at SeqNum N+something. Lookup -> chunk 1.
	env.writeAndChunk(t, 5, newer)

	env.vb.BlocksToObject.mu.RLock()
	cur := env.vb.BlocksToObject.BlockLookup[5]
	env.vb.BlocksToObject.mu.RUnlock()
	require.Equal(t, uint64(1), cur.ObjectID, "second write must land in chunk 1")

	// Splice the older sealed bytes from chunk 0 into chunk 1 at the
	// same per-block offset. cur.ObjectOffset is where the SeqNum-N+x
	// ciphertext currently lives in chunk 1; we overwrite that region
	// with the SeqNum-N bytes from chunk 0.
	chunk0, err := os.ReadFile(env.chunkPath(env.vb.VolumeName, 0))
	require.NoError(t, err)
	chunk1, err := os.ReadFile(env.chunkPath(env.vb.VolumeName, 1))
	require.NoError(t, err)
	// Both chunks have block 5 at the same per-block offset (single
	// block per chunk in this scenario, header + 0*stride).
	off0 := env.blockOffset(0)
	off1 := int(cur.ObjectOffset)
	copy(chunk1[off1:off1+env.stride], chunk0[off0:off0+env.stride])
	require.NoError(t, os.WriteFile(env.chunkPath(env.vb.VolumeName, 1), chunk1, 0600))

	// Force a backend re-read for block 5.
	env.vb.BlockStore = NewUnifiedBlockStore(env.vb.BlockSize)
	env.vb.UseBlockStore = false

	_, err = env.vb.ReadAt(5*uint64(env.blockSize), uint64(env.blockSize))
	require.Error(t, err, "in-place rollback splice must fail integrity")
	assert.ErrorIs(t, err, ErrIntegrity)
}

// TestEncryptedWAL_TamperFailsReplay — a WAL record with a flipped body
// byte must fail at WriteWALToChunk replay (AEAD on the WAL domain). The
// 16-byte GCM tag subsumes the dropped CRC32; the legacy path returned
// "checksum mismatch" — encrypted path returns ErrIntegrity.
func TestEncryptedWAL_TamperFailsReplay(t *testing.T) {
	env := newEncryptedTamperEnv(t, "vol-wal-tamper", testKey(t, 0x42))

	plaintext := make([]byte, env.blockSize)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)
	require.NoError(t, env.vb.Write(0, plaintext))
	require.NoError(t, env.vb.Flush())
	// Don't WriteWALToChunk yet — we want to tamper the WAL bytes first.

	walPath := filepath.Join(env.vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, env.vb.WAL.WallNum.Load(), env.vb.GetVolume()))
	// Sync so the WAL file on disk has the flushed record. The WAL
	// syncer is disabled (WALSyncInterval=-1) so no background work
	// will rewrite our tamper.
	env.vb.WAL.mu.Lock()
	require.NoError(t, env.vb.WAL.DB[len(env.vb.WAL.DB)-1].Sync())
	env.vb.WAL.mu.Unlock()

	raw, err := os.ReadFile(walPath)
	require.NoError(t, err)
	headerSize := env.vb.WALHeaderSize()
	// Encrypted WAL record: [SeqNum(8)|BlockNum(8)|BlockLen(8)|ct(BlockSize)|tag(16)]
	// Flip a byte inside the ciphertext.
	require.Greater(t, len(raw), headerSize+24+10)
	raw[headerSize+24+10] ^= 0x01
	require.NoError(t, os.WriteFile(walPath, raw, 0600))

	err = env.vb.WriteWALToChunk(true)
	require.Error(t, err, "WAL replay must fail integrity on body tamper")
	assert.ErrorIs(t, err, ErrIntegrity)
}

// TestEncryptedWAL_PreEncryptionMagicRejected — an encrypted runtime
// opening a WAL file with the legacy VBWL magic must refuse before any
// decrypt is attempted. The current production code path checks the
// magic at the top of WriteWALToChunk's per-record loop (line ~1807) and
// returns "magic mismatch". This is the migration gate: if a pre-cutover
// WAL bleeds into a post-cutover daemon, it fails loud rather than
// trying to AEAD-decrypt non-ciphertext and returning ErrIntegrity 4 MiB
// into the read.
func TestEncryptedWAL_PreEncryptionMagicRejected(t *testing.T) {
	env := newEncryptedTamperEnv(t, "vol-wal-vbwl", testKey(t, 0x42))

	// Write something so the WAL file exists, then rewrite its first 4
	// bytes to the legacy VBWL magic in place.
	plaintext := make([]byte, env.blockSize)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)
	require.NoError(t, env.vb.Write(0, plaintext))
	require.NoError(t, env.vb.Flush())

	walPath := filepath.Join(env.vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, env.vb.WAL.WallNum.Load(), env.vb.GetVolume()))
	env.vb.WAL.mu.Lock()
	require.NoError(t, env.vb.WAL.DB[len(env.vb.WAL.DB)-1].Sync())
	env.vb.WAL.mu.Unlock()

	raw, err := os.ReadFile(walPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(raw), 4)
	copy(raw[:4], []byte{'V', 'B', 'W', 'L'})
	require.NoError(t, os.WriteFile(walPath, raw, 0600))

	err = env.vb.WriteWALToChunk(true)
	require.Error(t, err, "VBWL magic must be rejected under encryption")
	// Under encryption, the magic mismatch must surface as
	// ErrPreEncryptionFormat so callers (and operators) get an
	// actionable migration signal rather than an opaque "magic
	// mismatch" or a deep ErrIntegrity from a downstream AEAD open.
	assert.ErrorIs(t, err, ErrPreEncryptionFormat)
	assert.Contains(t, err.Error(), "magic")
}

// TestEncryptedChunk_PreEncryptionMagicRejected — an encrypted runtime
// opening a chunk file that still carries the legacy VBCH magic must
// surface ErrPreEncryptionFormat before any AEAD-open is attempted on
// the body. Without this preflight, the chunk read path would feed
// pre-encryption plaintext bytes to aead.Open and return a generic
// ErrIntegrity, leaving operators with no actionable migration cue.
func TestEncryptedChunk_PreEncryptionMagicRejected(t *testing.T) {
	env := newEncryptedTamperEnv(t, "vol-chunk-vbch", testKey(t, 0x42))

	plaintext := make([]byte, env.blockSize)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)
	env.writeAndChunk(t, 0, plaintext)

	// Bypass caches so the next Read hits the backend (and therefore
	// the magic preflight).
	env.vb.BlockStore = NewUnifiedBlockStore(env.vb.BlockSize)
	env.vb.UseBlockStore = false
	env.vb.BlocksToObject.mu.Lock()
	env.vb.BlocksToObject.BlockLookup[0] = BlockLookup{
		StartBlock:   0,
		NumBlocks:    1,
		ObjectID:     0,
		ObjectOffset: uint32(env.blockOffset(0)),
		SeqNum:       1,
	}
	env.vb.BlocksToObject.mu.Unlock()

	// Rewrite the chunk file's first 4 bytes to the legacy VBCH magic
	// in place. The body stays valid VBCE ciphertext, but the
	// preflight must reject before any decrypt is attempted.
	chunkFile := env.chunkPath(env.vb.VolumeName, 0)
	raw, err := os.ReadFile(chunkFile)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(raw), 4)
	copy(raw[:4], []byte{'V', 'B', 'C', 'H'})
	require.NoError(t, os.WriteFile(chunkFile, raw, 0600))

	// Clear the magic-preflight memo so the preflight re-reads.
	env.vb.chunkMagicChecked.Delete(fmt.Sprintf("%s:%d", env.vb.VolumeName, uint64(0)))

	_, err = env.vb.ReadAt(0, uint64(env.blockSize))
	require.Error(t, err, "VBCH chunk magic must be rejected under encryption")
	assert.ErrorIs(t, err, ErrPreEncryptionFormat,
		"chunk magic mismatch must surface as ErrPreEncryptionFormat, not a generic ErrIntegrity")
	// Should NOT be ErrIntegrity — the whole point of the preflight is
	// to avoid the AEAD-open path on pre-encryption data.
	assert.False(t, errors.Is(err, ErrIntegrity),
		"preflight must reject before AEAD open; got integrity error instead")
}

// TestEncrypted_PropertyRoundTrip — randomised write/read round-trip
// across varying block alignments, run lengths, and starting offsets.
// Catches stride / offset / AAD-binding regressions that fixed-pattern
// tests would miss (e.g. a missing seqNum slot in BlockLookup
// serialisation would only surface on the second or third chunk).
func TestEncrypted_PropertyRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping property round-trip in short mode")
	}
	env := newEncryptedTamperEnv(t, "vol-property", testKey(t, 0x42))

	const trials = 32
	bs := uint64(env.blockSize)
	// Volume has 64 MiB / 4 KiB = 16384 blocks; we cap at 64 blocks per
	// run and 1024 starting blocks to keep the per-trial cost low.
	for trial := range trials {
		startBlock := randUint64(t, 1024)
		runBlocks := randUint64(t, 64) + 1
		buf := make([]byte, runBlocks*bs)
		_, err := rand.Read(buf)
		require.NoError(t, err)

		require.NoError(t, env.vb.Write(startBlock, buf))
		require.NoError(t, env.vb.Flush())
		require.NoError(t, env.vb.WriteWALToChunk(true))

		got, err := env.vb.ReadAt(startBlock*bs, runBlocks*bs)
		require.NoError(t, err, "trial %d: read failed", trial)
		require.True(t, bytes.Equal(buf, got),
			"trial %d: plaintext mismatch (startBlock=%d runBlocks=%d)", trial, startBlock, runBlocks)
	}
}

// randUint64 returns a uniformly random uint64 in [0, upper) using
// crypto/rand. Test-only — fail the test on any RNG error rather than
// silently returning 0 (which would skew the property distribution).
func randUint64(t *testing.T, upper uint64) uint64 {
	t.Helper()
	n, err := rand.Int(rand.Reader, big.NewInt(int64(upper)))
	require.NoError(t, err)
	return n.Uint64()
}

// TestEncryptedWAL_RecoveryAEADTamperSurfacesErrIntegrity — flipping a
// single byte in an encrypted WAL record on the recovery path must
// surface as ErrIntegrity from readWALFileForRecovery and the WAL file
// must remain on disk so RecoverLocalWALs does NOT silently truncate
// recovery. The legacy CRC path tolerates bit-rot and breaks the read
// loop; AEAD failure is tamper, not rot, so we fail closed.
func TestEncryptedWAL_RecoveryAEADTamperSurfacesErrIntegrity(t *testing.T) {
	env := newEncryptedTamperEnv(t, "vol-wal-recovery-tamper", testKey(t, 0x42))

	// Write two records: tampering the SECOND one must not allow the
	// caller to silently drop it (or any later records) on the floor.
	plain0 := make([]byte, env.blockSize)
	plain1 := make([]byte, env.blockSize)
	_, err := rand.Read(plain0)
	require.NoError(t, err)
	_, err = rand.Read(plain1)
	require.NoError(t, err)

	require.NoError(t, env.vb.Write(0, plain0))
	require.NoError(t, env.vb.Write(1, plain1))
	require.NoError(t, env.vb.Flush())

	walPath := filepath.Join(env.vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, env.vb.WAL.WallNum.Load(), env.vb.GetVolume()))
	env.vb.WAL.mu.Lock()
	require.NoError(t, env.vb.WAL.DB[len(env.vb.WAL.DB)-1].Sync())
	env.vb.WAL.mu.Unlock()

	raw, err := os.ReadFile(walPath)
	require.NoError(t, err)

	headerSize := env.vb.WALHeaderSize()
	recordSize := 40 + env.blockSize // 24-byte hdr + ct + 16-byte tag
	// Flip a byte in the SECOND record's ciphertext body.
	secondRecordStart := headerSize + recordSize
	tamperOff := secondRecordStart + 24 + 10
	require.Greater(t, len(raw), tamperOff)
	raw[tamperOff] ^= 0x01
	require.NoError(t, os.WriteFile(walPath, raw, 0600))

	// 1) Direct call: readWALFileForRecovery must return ErrIntegrity.
	blocks, _, err := env.vb.readWALFileForRecovery(walPath)
	require.Error(t, err, "AEAD tamper must NOT be swallowed as a bit-rot tear")
	assert.ErrorIs(t, err, ErrIntegrity)
	assert.Nil(t, blocks, "no blocks must be returned alongside an integrity error")

	// 2) RecoverLocalWALs must NOT delete the tampered WAL file.
	_, statErr := os.Stat(walPath)
	require.NoError(t, statErr, "WAL must still exist before RecoverLocalWALs")

	require.NoError(t, env.vb.RecoverLocalWALs(),
		"RecoverLocalWALs keeps tampered files for retry; it must not propagate the per-file error")

	_, statErr = os.Stat(walPath)
	assert.NoError(t, statErr, "tampered WAL must NOT be deleted by RecoverLocalWALs — silent truncation of recovery is the bug being fixed")
}
