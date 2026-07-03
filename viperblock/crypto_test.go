// Crypto-helper tests for the encryption-at-rest plumbing. Layout invariants
// (nonce byte positions, 56-bit SeqNum truncation, AAD shape, domain
// separation) live alongside AEAD round-trip, ciphertext + tag tamper
// detection, sealMeta / openMeta round-trip and tamper, ciphertext-level
// domain separation, and a 10^6-triple nonce uniqueness sweep.

package viperblock

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// freshAEAD builds an AES-256-GCM AEAD from a random 32-byte key. Caller-
// controlled key generation so tests can mint independent keys for
// cross-key separation checks.
func freshAEAD(t *testing.T) cipher.AEAD {
	t.Helper()
	var raw [masterkey.MasterKeySize]byte
	_, err := rand.Read(raw[:])
	require.NoError(t, err)
	aead, err := masterkey.NewAEAD(raw[:])
	require.NoError(t, err)
	return aead
}

func TestMakeNonce_LayoutAndDomainSeparation(t *testing.T) {
	uuid := [4]byte{0xde, 0xad, 0xbe, 0xef}
	seq := uint64(0x010203040506) // fits in 56 bits

	for _, tc := range []struct {
		name   string
		domain byte
	}{
		{"chunk", DomainChunk},
		{"wal", DomainWAL},
		{"vbstate", DomainVBStateMeta},
		{"snapshot", DomainSnapshotMeta},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := makeNonce(seq, uuid, tc.domain)
			// Bytes [0:7]: BE(seq) low-56.
			require.Equal(t, byte(0x00), n[0])
			require.Equal(t, byte(0x01), n[1])
			require.Equal(t, byte(0x02), n[2])
			require.Equal(t, byte(0x03), n[3])
			require.Equal(t, byte(0x04), n[4])
			require.Equal(t, byte(0x05), n[5])
			require.Equal(t, byte(0x06), n[6])
			// Bytes [7:11]: VolumeUUID.
			require.Equal(t, uuid[:], n[7:11])
			// Byte [11]: domain.
			require.Equal(t, tc.domain, n[11])
		})
	}

	// Same (seq, uuid) under different domains must yield different nonces —
	// this is the whole point of the domain byte (no WAL/chunk collision).
	chunkNonce := makeNonce(seq, uuid, DomainChunk)
	walNonce := makeNonce(seq, uuid, DomainWAL)
	assert.NotEqual(t, chunkNonce, walNonce)
}

func TestMakeNonce_SeqNumTopByteDropped(t *testing.T) {
	// SeqNum top byte must not leak into the nonce — the 56-bit truncation
	// is intentional (NIST SP 800-38D §8 deterministic IV). Compare a
	// 56-bit-bounded value against the same value with garbage in the top
	// byte; the two nonces must match.
	uuid := [4]byte{1, 2, 3, 4}
	low := uint64(0xAABBCCDDEEFF12)
	high := low | (uint64(0xFF) << 56)
	assert.Equal(t, makeNonce(low, uuid, DomainChunk), makeNonce(high, uuid, DomainChunk))
}

func TestMakeAAD_Layout(t *testing.T) {
	hash := sha256.Sum256([]byte("vol-1"))
	aad := makeAAD(hash, 12345, 67890)
	require.Len(t, aad, 48)
	assert.Equal(t, hash[:], aad[0:32])
	assert.Equal(t, uint64(12345), binary.BigEndian.Uint64(aad[32:40]))
	assert.Equal(t, uint64(67890), binary.BigEndian.Uint64(aad[40:48]))
}

func TestMakeAAD_TamperDetectionPrecondition(t *testing.T) {
	// makeAAD itself is pure; aead.Open on the decrypt path catches tampering.
	// What we verify here is that any single-field bit-flip yields a
	// different AAD byte string, which is the precondition for AEAD tamper
	// detection to fire.
	hash := sha256.Sum256([]byte("vol-1"))
	base := makeAAD(hash, 100, 200)

	// Bit-flip the volume hash.
	tampered := hash
	tampered[0] ^= 0x01
	assert.NotEqual(t, base, makeAAD(tampered, 100, 200))

	// Different blockNum.
	assert.NotEqual(t, base, makeAAD(hash, 101, 200))

	// Different seqNum.
	assert.NotEqual(t, base, makeAAD(hash, 100, 201))
}

func TestMakeMetaAAD_DomainSeparation(t *testing.T) {
	hash := sha256.Sum256([]byte("vol-1"))
	vbstate := makeMetaAAD(hash, "vbstate", 42)
	snap := makeMetaAAD(hash, "snap:snap-abc", 42)
	assert.NotEqual(t, vbstate, snap)
	// StateSeqNum is appended; bumping it changes the AAD.
	assert.NotEqual(t, vbstate, makeMetaAAD(hash, "vbstate", 43))
}

func TestComputeVolumeNameHash_MatchesSHA256(t *testing.T) {
	want := sha256.Sum256([]byte("vol-42"))
	assert.Equal(t, want, computeVolumeNameHash("vol-42"))
}

// TestAEAD_RoundTrip exercises the full data-domain primitive: seal with
// (nonce, aad, plaintext) and recover the plaintext via Open. The pure-
// helper tests above only assert layout; this gates that an actual
// aead.Seal / aead.Open round-trip through makeNonce + makeAAD is
// symmetric. Foundation for every tamper test below.
func TestAEAD_RoundTrip(t *testing.T) {
	aead := freshAEAD(t)
	hash := sha256.Sum256([]byte("vol-rt"))
	uuid := [4]byte{0xaa, 0xbb, 0xcc, 0xdd}
	plaintext := []byte("the only winning move is not to play")

	nonce := makeNonce(42, uuid, DomainChunk)
	aad := makeAAD(hash, 7, 42)
	sealed := aead.Seal(nil, nonce[:], plaintext, aad)
	require.Len(t, sealed, len(plaintext)+aead.Overhead())

	opened, err := aead.Open(nil, nonce[:], sealed, aad)
	require.NoError(t, err)
	assert.Equal(t, plaintext, opened)
}

// TestAEAD_CiphertextTamperFails — flipping a single bit in the body or
// the trailing 16-byte tag must surface as an aead.Open error. This is the
// SI.L1-3.14.2 backbone: chunk-store / WAL integrity rests on these two
// detections firing on every byte modification at rest.
func TestAEAD_CiphertextTamperFails(t *testing.T) {
	aead := freshAEAD(t)
	hash := sha256.Sum256([]byte("vol-tamper"))
	uuid := [4]byte{1, 2, 3, 4}
	plaintext := make([]byte, 512)
	_, err := rand.Read(plaintext)
	require.NoError(t, err)

	nonce := makeNonce(100, uuid, DomainChunk)
	aad := makeAAD(hash, 0, 100)
	sealed := aead.Seal(nil, nonce[:], plaintext, aad)

	t.Run("body bit flip", func(t *testing.T) {
		tampered := append([]byte(nil), sealed...)
		tampered[5] ^= 0x01
		_, err := aead.Open(nil, nonce[:], tampered, aad)
		assert.Error(t, err, "body tamper must fail Open")
	})

	t.Run("tag bit flip", func(t *testing.T) {
		tampered := append([]byte(nil), sealed...)
		tampered[len(tampered)-1] ^= 0x01
		_, err := aead.Open(nil, nonce[:], tampered, aad)
		assert.Error(t, err, "tag tamper must fail Open")
	})
}

// TestAEAD_AADTamperFails — Open must reject when any AAD field
// (volumeNameHash, blockNum, seqNum) differs from the seal-time value.
// This is what defeats cross-volume swap (volume hash differs), positional
// shuffle (blockNum differs), and in-place rollback (seqNum differs).
// makeAAD layout is asserted by TestMakeAAD_TamperDetectionPrecondition;
// here we wire that into a real Seal/Open pair to gate the cryptographic
// detection.
func TestAEAD_AADTamperFails(t *testing.T) {
	aead := freshAEAD(t)
	hash := sha256.Sum256([]byte("vol-aad"))
	uuid := [4]byte{0x10, 0x20, 0x30, 0x40}
	plaintext := []byte("authenticated data binding test")
	nonce := makeNonce(500, uuid, DomainChunk)
	aad := makeAAD(hash, 99, 500)
	sealed := aead.Seal(nil, nonce[:], plaintext, aad)

	t.Run("wrong volumeNameHash", func(t *testing.T) {
		other := sha256.Sum256([]byte("vol-aad-OTHER"))
		_, err := aead.Open(nil, nonce[:], sealed, makeAAD(other, 99, 500))
		assert.Error(t, err)
	})
	t.Run("wrong blockNum", func(t *testing.T) {
		_, err := aead.Open(nil, nonce[:], sealed, makeAAD(hash, 100, 500))
		assert.Error(t, err)
	})
	t.Run("wrong seqNum", func(t *testing.T) {
		_, err := aead.Open(nil, nonce[:], sealed, makeAAD(hash, 99, 501))
		assert.Error(t, err)
	})
}

// TestAEAD_DomainSeparationAtCiphertext — sealing identical plaintext under
// (same key, same seqNum, same uuid) but different domains yields distinct
// ciphertexts AND a tag from one domain does not verify under another. This
// is the cryptographic teeth of the domain byte: without it, consolidating
// a WAL record (DomainWAL) into a chunk block (DomainChunk) would reuse
// the same nonce — catastrophic for AES-GCM.
func TestAEAD_DomainSeparationAtCiphertext(t *testing.T) {
	aead := freshAEAD(t)
	hash := sha256.Sum256([]byte("vol-domain"))
	uuid := [4]byte{0xfe, 0xed, 0xfa, 0xce}
	plaintext := []byte("same plaintext, different domain")
	aad := makeAAD(hash, 1, 1)

	chunkNonce := makeNonce(1, uuid, DomainChunk)
	walNonce := makeNonce(1, uuid, DomainWAL)
	require.NotEqual(t, chunkNonce, walNonce)

	chunkSealed := aead.Seal(nil, chunkNonce[:], plaintext, aad)
	walSealed := aead.Seal(nil, walNonce[:], plaintext, aad)
	assert.False(t, bytes.Equal(chunkSealed, walSealed),
		"identical plaintext under different domain nonces must produce different ciphertexts")

	// Cross-domain Open must fail — a chunk ciphertext presented as a WAL
	// ciphertext (or vice versa) is rejected because the nonce differs.
	_, err := aead.Open(nil, walNonce[:], chunkSealed, aad)
	assert.Error(t, err, "chunk ciphertext must not verify under WAL nonce")
	_, err = aead.Open(nil, chunkNonce[:], walSealed, aad)
	assert.Error(t, err, "WAL ciphertext must not verify under chunk nonce")
}

// openMetaTest mirrors the production read path (splitEnvelope + verifyMeta):
// parse the envelope, verify the tag over the verbatim payload, return the
// payload. Keeps the tamper assertions below readable as a single call.
func openMetaTest(aead cipher.AEAD, blob, aad []byte, nonce [12]byte) ([]byte, error) {
	payload, tag, err := splitEnvelope(blob)
	if err != nil {
		return nil, err
	}
	if err := verifyMeta(aead, payload, tag, aad, nonce); err != nil {
		return nil, err
	}
	return payload, nil
}

// TestSealMeta_RoundTrip exercises the metadata HMAC primitive: seal a JSON
// blob with structured AAD + nonce into the valid-JSON envelope, recover the
// payload via splitEnvelope + verifyMeta. The payload ships in the clear
// (operators can `jq .payload config.json`) — the tag binds it to
// (volumeNameHash, domainTag, stateSeqNum) so cross-volume substitution and
// bit-flip are detectable.
func TestSealMeta_RoundTrip(t *testing.T) {
	aead := freshAEAD(t)
	hash := sha256.Sum256([]byte("vol-meta"))
	jsonBytes := []byte(`{"VolumeName":"vol-meta","StateSeqNum":7}`)
	nonce := makeNonce(7, [4]byte{1, 2, 3, 4}, DomainVBStateMeta)
	aad := makeMetaAAD(hash, "vbstate", 7)

	blob := sealMeta(aead, jsonBytes, aad, nonce)
	assert.True(t, json.Valid(blob), "sealed envelope must be valid JSON")

	payload, _, err := splitEnvelope(blob)
	require.NoError(t, err)
	assert.Equal(t, jsonBytes, payload, "payload recovered verbatim from the envelope")

	recovered, err := openMetaTest(aead, blob, aad, nonce)
	require.NoError(t, err)
	assert.Equal(t, jsonBytes, recovered)

	// StateBody strips the wrapper without a key (the unverified consumer path).
	assert.Equal(t, jsonBytes, StateBody(blob))
}

// TestSealMeta_TamperFails — bit-flip in the payload, the tag, or the
// structured AAD inputs (domainTag / stateSeqNum / volumeNameHash) all surface
// as errors on the read path. This is the cross-volume metadata pivot closer:
// an attacker swapping volume A's envelope into volume B's prefix is rejected
// because the AAD's volumeNameHash differs.
func TestSealMeta_TamperFails(t *testing.T) {
	aead := freshAEAD(t)
	hash := sha256.Sum256([]byte("vol-meta-tamper"))
	jsonBytes := []byte(`{"VolumeName":"vol-meta-tamper","StateSeqNum":99}`)
	nonce := makeNonce(99, [4]byte{9, 9, 9, 9}, DomainVBStateMeta)
	aad := makeMetaAAD(hash, "vbstate", 99)
	blob := sealMeta(aead, jsonBytes, aad, nonce)

	// payloadOffset points inside the verbatim payload (after the
	// `{"v":1,"payload":` prefix) so the flip exercises tag verification on
	// the payload, not envelope JSON well-formedness.
	payloadOffset := len(`{"v":1,"payload":`) + 5

	t.Run("payload byte flip", func(t *testing.T) {
		tampered := append([]byte(nil), blob...)
		tampered[payloadOffset] ^= 0x01
		_, err := openMetaTest(aead, tampered, aad, nonce)
		assert.Error(t, err)
	})

	t.Run("tag byte flip", func(t *testing.T) {
		tampered := append([]byte(nil), blob...)
		// Flip a base64 char of the authtag (before the closing `"}`).
		tampered[len(tampered)-3] ^= 0x01
		_, err := openMetaTest(aead, tampered, aad, nonce)
		assert.Error(t, err)
	})

	t.Run("wrong volumeNameHash", func(t *testing.T) {
		other := sha256.Sum256([]byte("vol-meta-OTHER"))
		_, err := openMetaTest(aead, blob, makeMetaAAD(other, "vbstate", 99), nonce)
		assert.Error(t, err, "cross-volume AAD must fail verify")
	})

	t.Run("wrong domain tag", func(t *testing.T) {
		_, err := openMetaTest(aead, blob, makeMetaAAD(hash, "snap:foo", 99), nonce)
		assert.Error(t, err, "vbstate blob must not verify under snapshot AAD")
	})

	t.Run("wrong stateSeqNum", func(t *testing.T) {
		_, err := openMetaTest(aead, blob, makeMetaAAD(hash, "vbstate", 100), nonce)
		assert.Error(t, err)
	})

	t.Run("not an envelope", func(t *testing.T) {
		_, err := openMetaTest(aead, []byte(`{"VolumeName":"x"}`), aad, nonce)
		assert.Error(t, err, "plain JSON without payload/authtag must fail split")
	})
}

// TestMakeNonce_UniquenessSweep stresses the (seqNum, volumeUUID, domain)
// nonce space over 10^6 random triples and asserts there is no collision.
// 12 bytes of nonce gives a 2^96 space, so a million-shot collision is
// astronomically unlikely if our construction actually uses all the bits
// — a collision here means makeNonce dropped a field. Catches regressions
// like "VolumeUUID slot mis-sliced" or "domain byte XORed instead of
// assigned" that would silently shrink the effective nonce space.
//
// At 10^6 triples this runs in ~200ms; gated by testing.Short so the
// short-mode preflight stays under a minute.
func TestMakeNonce_UniquenessSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million-triple nonce sweep in short mode")
	}
	const N = 1_000_000
	seen := make(map[[12]byte]struct{}, N)
	var buf [8]byte
	for i := range N {
		_, err := rand.Read(buf[:])
		require.NoError(t, err)
		seqNum := binary.BigEndian.Uint64(buf[:]) & MaxSeqNum
		var uuid [4]byte
		_, err = rand.Read(uuid[:])
		require.NoError(t, err)
		_, err = rand.Read(buf[:1])
		require.NoError(t, err)
		domain := buf[0]
		n := makeNonce(seqNum, uuid, domain)
		if _, dup := seen[n]; dup {
			t.Fatalf("nonce collision at iteration %d: %x (seqNum=%d uuid=%x domain=%#x)", i, n, seqNum, uuid, domain)
		}
		seen[n] = struct{}{}
	}
	assert.Len(t, seen, N)
}
