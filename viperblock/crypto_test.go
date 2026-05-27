// Copyright 2026 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

// Stage 1 smoke tests for the encryption-at-rest plumbing. The full Stage 5
// test inventory (round-trip, AAD/ciphertext tamper, domain separation,
// nonce uniqueness sweep, etc.) lands alongside the Stage 2-4 production
// code; these tests exist to keep the global coverage gate satisfied while
// the per-stage scope discipline is preserved.

package viperblock

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	// makeAAD itself is pure; Stage 3's aead.Open is what catches tampering.
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
