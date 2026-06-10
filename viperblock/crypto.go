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
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
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

// AADLen is the fixed size of the data-domain AAD: 32-byte volumeNameHash +
// 8-byte blockNum + 8-byte seqNum.
const AADLen = 32 + 8 + 8

// makeAAD builds the 48-byte AAD bound into every data-domain seal/open:
// volumeNameHash || BE(blockNum) || BE(seqNum). Defeats cross-volume swap
// (hash differs), in-place rollback (seqnum differs from index-trusted
// value), and positional shuffle (blockNum differs).
//
// For per-block hot loops, prefer initAAD + updateAAD against a stack-
// allocated [AADLen]byte to avoid an allocation per block.
func makeAAD(volumeNameHash [32]byte, blockNum, seqNum uint64) []byte {
	aad := make([]byte, AADLen)
	copy(aad[0:32], volumeNameHash[:])
	binary.BigEndian.PutUint64(aad[32:40], blockNum)
	binary.BigEndian.PutUint64(aad[40:48], seqNum)
	return aad
}

// initAAD populates the fixed volumeNameHash prefix of an AAD buffer once.
// Per-block hot loops pair this with updateAAD to mutate only the block-
// varying suffix without reallocating or copying the 32-byte hash each call.
func initAAD(aad *[AADLen]byte, volumeNameHash [32]byte) {
	copy(aad[0:32], volumeNameHash[:])
}

// updateAAD overwrites the blockNum + seqNum suffix of an AAD buffer whose
// volumeNameHash prefix was previously set by initAAD.
func updateAAD(aad *[AADLen]byte, blockNum, seqNum uint64) {
	binary.BigEndian.PutUint64(aad[32:40], blockNum)
	binary.BigEndian.PutUint64(aad[40:48], seqNum)
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

// envelopeVersion is the schema tag written into the metadata envelope so a
// future format change is self-describing on read.
const envelopeVersion = 1

// metaEnvelope is the on-disk wrapper for authenticated VBState/SnapshotState
// metadata: {"v":1,"payload":<json>,"authtag":"<base64 tag>"}. The whole file
// stays valid JSON — `jq .payload config.json` works, and non-crypto consumers
// (e.g. the AWS gateway enumerating AMIs) parse it with a plain json.Unmarshal
// after StateBody strips the wrapper. Payload is the verbatim metadata JSON;
// authtag is the base64 AES-GCM tag binding the payload to the structured AAD.
type metaEnvelope struct {
	Version int             `json:"v"`
	Payload json.RawMessage `json:"payload"`
	AuthTag string          `json:"authtag"`
}

// sealMeta authenticates VBState/SnapshotState JSON with AES-GCM-as-MAC and
// wraps it in the valid-JSON metaEnvelope. The JSON body ships in the clear
// (authenticated, not encrypted); the tag covers aad || jsonBytes and binds
// the bytes to the structured metadata AAD (volumeNameHash || domain ||
// stateSeqNum) and to the nonce.
//
// The envelope is assembled by concatenation so the payload is spliced in
// verbatim — splitEnvelope recovers those exact bytes via json.RawMessage with
// no re-serialization, which is what keeps verification robust against JSON
// canonicalization drift (e.g. time.Time marshal round-trips are not byte-
// identical, so a re-marshal-to-verify scheme would corrupt the tag input).
//
// Caller is responsible for (key, nonce) uniqueness: callers must allocate a
// fresh stateSeqNum (via VB.nextStateSeqNum.Add(1)) for every seal, since
// domain separation only keeps metadata nonces disjoint from data-path nonces
// — it does not protect back-to-back metadata seals on the same volume.
func sealMeta(aead cipher.AEAD, jsonBytes, aad []byte, nonce [12]byte) []byte {
	fullAAD := make([]byte, 0, len(aad)+len(jsonBytes))
	fullAAD = append(fullAAD, aad...)
	fullAAD = append(fullAAD, jsonBytes...)
	tag := aead.Seal(nil, nonce[:], nil, fullAAD)

	prefix := fmt.Sprintf(`{"v":%d,"payload":`, envelopeVersion)
	suffix := `,"authtag":"` + base64.StdEncoding.EncodeToString(tag) + `"}`
	out := make([]byte, 0, len(prefix)+len(jsonBytes)+len(suffix))
	out = append(out, prefix...)
	out = append(out, jsonBytes...)
	out = append(out, suffix...)
	return out
}

// splitEnvelope parses a metaEnvelope and returns the verbatim payload JSON
// bytes and the decoded AES-GCM tag. It performs NO cryptographic
// verification — callers that need authentication pass the result to
// verifyMeta. Returns an error if the blob is not a well-formed envelope.
func splitEnvelope(blob []byte) (payload []byte, tag []byte, err error) {
	var env metaEnvelope
	if err := json.Unmarshal(blob, &env); err != nil {
		return nil, nil, fmt.Errorf("metadata envelope parse: %w", err)
	}
	if len(env.Payload) == 0 || env.AuthTag == "" {
		return nil, nil, fmt.Errorf("metadata envelope missing payload or authtag")
	}
	tag, err = base64.StdEncoding.DecodeString(env.AuthTag)
	if err != nil {
		return nil, nil, fmt.Errorf("metadata envelope authtag decode: %w", err)
	}
	return env.Payload, tag, nil
}

// verifyMeta checks the AES-GCM tag over aad || payload (payload + tag come
// from splitEnvelope). Any tampering with the payload, the tag, the structured
// AAD, or the nonce inputs surfaces as a non-nil error; callers must wrap with
// ErrIntegrity and fail-closed.
func verifyMeta(aead cipher.AEAD, payload, tag, aad []byte, nonce [12]byte) error {
	if len(tag) != aead.Overhead() {
		return fmt.Errorf("metadata tag %d bytes, want %d", len(tag), aead.Overhead())
	}
	fullAAD := make([]byte, 0, len(aad)+len(payload))
	fullAAD = append(fullAAD, aad...)
	fullAAD = append(fullAAD, payload...)
	if _, err := aead.Open(nil, nonce[:], tag, fullAAD); err != nil {
		return err
	}
	return nil
}

// StateBody returns the inner VBState/SnapshotState JSON from a config.json
// blob, transparently unwrapping the encrypted metadata envelope when present.
// It does NOT verify the authentication tag and needs no master key — it is
// for read-only metadata consumers (e.g. the AWS gateway enumerating AMIs and
// volumes) that read fields but do not authenticate them. Callers that must
// authenticate the bytes use VB.LoadStateRequest / VB.LoadSnapshotBlockMap.
//
// Plain (unencrypted) config.json is returned unchanged, so a single call site
// transparently handles both encrypted and legacy-cleartext volumes.
func StateBody(raw []byte) []byte {
	var probe struct {
		Payload json.RawMessage `json:"payload"`
		AuthTag string          `json:"authtag"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return raw
	}
	if len(probe.Payload) > 0 && probe.AuthTag != "" {
		return probe.Payload
	}
	return raw
}
