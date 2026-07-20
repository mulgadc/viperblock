package viperblock

import (
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildTestCheckpoint serialises entries into the on-disk checkpoint format
// using the production writer, so these tests exercise the real encoder rather
// than a hand-rolled copy of it.
func buildTestCheckpoint(t *testing.T, version uint16, magic [4]byte, entries []BlockLookup) []byte {
	t.Helper()

	header := make([]byte, blockCheckpointHeaderSize)
	copy(header[:4], magic[:])
	binary.BigEndian.PutUint16(header[4:6], version)
	binary.BigEndian.PutUint64(header[6:14], 0)

	raw := make([]byte, 0, blockCheckpointHeaderSize+len(entries)*blockWalChunkSize)
	raw = append(raw, header...)

	vb := &VB{}
	for i := range entries {
		raw = append(raw, vb.writeBlockWalChunk(&entries[i])...)
	}
	return raw
}

// TestParseBlockCheckpointBytesRoundtrip checks the exported parser decodes
// every field the encoder wrote, keyed by StartBlock.
func TestParseBlockCheckpointBytesRoundtrip(t *testing.T) {
	entries := []BlockLookup{
		{StartBlock: 0, NumBlocks: 1, ObjectID: 0, ObjectOffset: 0, SeqNum: 1},
		{StartBlock: 1, NumBlocks: 1, ObjectID: 0, ObjectOffset: 4096, SeqNum: 2},
		{StartBlock: 9000, NumBlocks: 1, ObjectID: 7, ObjectOffset: 8192, SeqNum: 3},
	}

	raw := buildTestCheckpoint(t, 1, blockToObjectWALMagic, entries)

	got, err := ParseBlockCheckpointBytes(raw, 1)
	require.NoError(t, err)
	require.Len(t, got, len(entries))

	for _, want := range entries {
		have, ok := got[want.StartBlock]
		require.True(t, ok, "block %d missing", want.StartBlock)
		assert.Equal(t, want, have, "block %d roundtrip mismatch", want.StartBlock)
	}
}

// TestParseBlockCheckpointBytesEmpty checks a header-only checkpoint parses to
// an empty map rather than an error: a volume that has never drained a chunk
// legitimately has no referenced blocks, and that is zero, not a failure.
func TestParseBlockCheckpointBytesEmpty(t *testing.T) {
	raw := buildTestCheckpoint(t, 1, blockToObjectWALMagic, nil)

	got, err := ParseBlockCheckpointBytes(raw, 1)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestParseBlockCheckpointBytesRejects covers every malformed input that must
// produce a hard error. A reader of this map decides which chunks are garbage,
// so a silently-short or silently-wrong parse is worse than no answer at all.
func TestParseBlockCheckpointBytesRejects(t *testing.T) {
	valid := buildTestCheckpoint(t, 1, blockToObjectWALMagic, []BlockLookup{
		{StartBlock: 0, NumBlocks: 1, ObjectID: 0, ObjectOffset: 0, SeqNum: 1},
		{StartBlock: 1, NumBlocks: 1, ObjectID: 1, ObjectOffset: 0, SeqNum: 2},
	})

	t.Run("empty input", func(t *testing.T) {
		_, err := ParseBlockCheckpointBytes(nil, 1)
		require.ErrorContains(t, err, "shorter than")
	})

	t.Run("truncated header", func(t *testing.T) {
		_, err := ParseBlockCheckpointBytes(valid[:blockCheckpointHeaderSize-1], 1)
		require.ErrorContains(t, err, "shorter than")
	})

	t.Run("magic mismatch", func(t *testing.T) {
		bad := buildTestCheckpoint(t, 1, [4]byte{'N', 'O', 'P', 'E'}, nil)
		_, err := ParseBlockCheckpointBytes(bad, 1)
		require.ErrorContains(t, err, "magic mismatch")
	})

	t.Run("version mismatch", func(t *testing.T) {
		_, err := ParseBlockCheckpointBytes(valid, 2)
		require.ErrorContains(t, err, "version mismatch")
	})

	// A truncated read is the realistic failure for a network reader. The
	// fixed-width loop must reject it up front: slicing a partial record
	// either panics (when the backing array ends at the data) or, when the
	// slice has spare capacity, silently decodes bytes past the end as a
	// phantom record. Both are worse than an error.
	t.Run("trailing partial record", func(t *testing.T) {
		truncated := valid[:len(valid)-5]
		_, err := ParseBlockCheckpointBytes(truncated, 1)
		require.ErrorContains(t, err, "trailing bytes")
	})

	t.Run("corrupt record crc", func(t *testing.T) {
		corrupt := make([]byte, len(valid))
		copy(corrupt, valid)
		// Flip a byte inside the second record's CRC-covered region.
		corrupt[blockCheckpointHeaderSize+blockWalChunkSize] ^= 0xFF
		_, err := ParseBlockCheckpointBytes(corrupt, 1)
		require.ErrorContains(t, err, "checksum mismatch")
	})
}

// TestParseBlockCheckpointBytesMatchesVBLoad pins the exported parser to the
// in-process loader: both must decode a real drained volume's live checkpoint
// to the same map. This is the guard against the two paths drifting apart.
func TestParseBlockCheckpointBytesMatchesVBLoad(t *testing.T) {
	runWithBackends(t, "parse_checkpoint_matches_vb_load", func(t *testing.T, vb *VB) {
		for i := range uint64(4) {
			data := make([]byte, DefaultBlockSize)
			_, err := rand.Read(data)
			require.NoError(t, err)
			require.NoError(t, vb.Write(i, data))
		}
		require.NoError(t, vb.Flush())
		if vb.UseShardedWAL {
			require.NoError(t, vb.WriteShardedWALToChunk(true))
		} else {
			require.NoError(t, vb.WriteWALToChunk(true))
		}
		require.NoError(t, vb.SaveLiveCheckpoint())

		// Read the same bytes the VB persisted, straight off the backend.
		raw, err := vb.Backend.Read(types.FileTypeBlockCheckpointLive, 0, 0, 0)
		require.NoError(t, err)

		got, err := ParseBlockCheckpointBytes(raw, vb.Version)
		require.NoError(t, err)

		vb.BlocksToObject.mu.RLock()
		defer vb.BlocksToObject.mu.RUnlock()

		// vb's map may be coalesced into extents while the parsed map stays
		// flat, so compare by physical block coverage rather than map shape.
		stride := vb.blockStride()
		wantTotal := 0
		for _, entry := range vb.BlocksToObject.BlockLookup {
			wantTotal += int(entry.NumBlocks)
			for i := range int(entry.NumBlocks) {
				blockNum := entry.StartBlock + uint64(i)
				have, ok := got[blockNum]
				require.True(t, ok, "block %d missing from parsed map", blockNum)
				assert.Equal(t, entry.ObjectID, have.ObjectID, "block %d ObjectID mismatch", blockNum)
				assert.Equal(t, entry.offsetAt(i, stride), have.ObjectOffset, "block %d ObjectOffset mismatch", blockNum)
				assert.Equal(t, entry.seqNumAt(i), have.SeqNum, "block %d SeqNum mismatch", blockNum)
			}
		}
		require.Len(t, got, wantTotal, "parsed map block count should match vb's physical block count")
	})
}
