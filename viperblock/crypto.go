// Copyright 2026 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

// Package viperblock — crypto helpers for AES-256-GCM at-rest encryption.
//
// Nonce layout (12 bytes, big-endian throughout, NIST SP 800-38D §8.2.1
// deterministic IV):
//
//	[0:7]   BE(SeqNum)   56 bits of monotonic counter (top byte of uint64 dropped)
//	[7:11]  VolumeUUID   4 random bytes per volume, persisted in VBState
//	[11]    domain       0x00=chunk | 0x01=WAL | 0x02=vbstate-meta | 0x03=snapshot-meta
//
// AAD layout (48 bytes for data domains): SHA256(VolumeName) || BE(blockNum)
// || BE(seqNum). Metadata domains substitute the blockNum slot with a literal
// domain tag ("vbstate", "snap:"||snapshotID) and use VBState.StateSeqNum.

package viperblock

import (
	"crypto/sha256"
	"encoding/binary"
)

// Nonce domain bytes — disjoint nonce spaces under the shared master key so a
// WAL record sealed at (SeqNum=N, VolumeUUID=V) does not collide with the
// chunk block built from it.
const (
	DomainChunk        byte = 0x00
	DomainWAL          byte = 0x01
	DomainVBStateMeta  byte = 0x02
	DomainSnapshotMeta byte = 0x03
)

// MaxSeqNum is the largest sequence number representable in the 56-bit nonce
// slot. Writes past this point are refused; at 1M writes/sec sustained per
// volume that is ~2,283 years.
const MaxSeqNum uint64 = (1 << 56) - 1

// makeNonce builds a 12-byte GCM nonce from a sequence number, the per-volume
// UUID, and a domain byte. SeqNum is truncated to 56 bits (top byte of the
// uint64 is dropped); callers must enforce seqNum <= MaxSeqNum.
func makeNonce(seqNum uint64, volumeUUID [4]byte, domain byte) [12]byte {
	var n [12]byte
	// Low 56 bits of seqNum, big-endian, into n[0:7]. Mask each shift result
	// to satisfy gosec G115 — Go truncates byte(uint64) safely but the
	// linter cannot prove it.
	n[0] = byte((seqNum >> 48) & 0xff)
	n[1] = byte((seqNum >> 40) & 0xff)
	n[2] = byte((seqNum >> 32) & 0xff)
	n[3] = byte((seqNum >> 24) & 0xff)
	n[4] = byte((seqNum >> 16) & 0xff)
	n[5] = byte((seqNum >> 8) & 0xff)
	n[6] = byte(seqNum & 0xff)
	copy(n[7:11], volumeUUID[:])
	n[11] = domain
	return n
}

// makeAAD builds the 48-byte AAD bound into every data-domain seal/open:
// volumeNameHash || BE(blockNum) || BE(seqNum). Defeats cross-volume swap
// (hash differs), in-place rollback (seqnum differs from index-trusted
// value), and positional shuffle (blockNum differs).
func makeAAD(volumeNameHash [32]byte, blockNum, seqNum uint64) []byte {
	aad := make([]byte, 32+8+8)
	copy(aad[0:32], volumeNameHash[:])
	binary.BigEndian.PutUint64(aad[32:40], blockNum)
	binary.BigEndian.PutUint64(aad[40:48], seqNum)
	return aad
}

// makeMetaAAD builds the AAD for VBState and SnapshotState integrity tags:
// volumeNameHash || domainTag || BE(stateSeqNum). The literal domainTag
// ("vbstate" or "snap:"||snapshotID) prevents a snapshot blob tagged for
// volume A from being accepted as volume A's VBState blob and vice versa.
func makeMetaAAD(volumeNameHash [32]byte, domainTag string, stateSeqNum uint64) []byte {
	aad := make([]byte, 0, 32+len(domainTag)+8)
	aad = append(aad, volumeNameHash[:]...)
	aad = append(aad, domainTag...)
	var sn [8]byte
	binary.BigEndian.PutUint64(sn[:], stateSeqNum)
	return append(aad, sn[:]...)
}

// computeVolumeNameHash returns SHA256(volumeName), cached on the VB at Open
// to avoid recomputing for every seal/open call.
func computeVolumeNameHash(name string) [32]byte {
	return sha256.Sum256([]byte(name))
}
