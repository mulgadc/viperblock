package viperblock

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// consecutiveRun builds the candidate shape createChunkFile classifies: blocks
// [start, start+len(seqNums)) carrying the given sequence numbers.
func consecutiveRun(start uint64, seqNums ...uint64) []Block {
	run := make([]Block, len(seqNums))
	for i, seq := range seqNums {
		run[i] = Block{Block: start + uint64(i), SeqNum: seq}
	}
	return run
}

// TestClassifyRun pins the verdict for each shape a run can present against the
// extents covering it. The comparison is strictly greater and strictly per
// block: a coalesced extent's own SeqNum belongs to its first block only.
func TestClassifyRun(t *testing.T) {
	// [10, 14) at seqs 100..103, then a gap, then [20, 22) at seqs 50, 51.
	mapped := []BlockLookup{
		{StartBlock: 10, NumBlocks: 4, SeqNums: []uint64{100, 101, 102, 103}},
		{StartBlock: 20, NumBlocks: 2, SeqNums: []uint64{50, 51}},
	}

	cases := []struct {
		name     string
		overlaps []BlockLookup
		run      []Block
		want     []bool
	}{
		{
			name:     "nothing mapped",
			overlaps: nil,
			run:      consecutiveRun(0, 1, 2, 3),
			want:     []bool{true, true, true},
		},
		{
			name:     "every candidate newer",
			overlaps: mapped[:1],
			run:      consecutiveRun(10, 200, 201, 202, 203),
			want:     []bool{true, true, true, true},
		},
		{
			name:     "every candidate older",
			overlaps: mapped[:1],
			run:      consecutiveRun(10, 1, 2, 3, 4),
			want:     []bool{false, false, false, false},
		},
		{
			name:     "equal is not newer",
			overlaps: mapped[:1],
			run:      consecutiveRun(10, 100, 101, 102, 103),
			want:     []bool{false, false, false, false},
		},
		{
			// The reason the verdict cannot be taken once for a whole run: the
			// extent's own SeqNum is 100, but its blocks carry 100..103.
			name:     "mixed within one coalesced extent",
			overlaps: mapped[:1],
			run:      consecutiveRun(10, 101, 101, 104, 1),
			want:     []bool{true, false, true, false},
		},
		{
			name:     "starts before the first extent",
			overlaps: mapped[:1],
			run:      consecutiveRun(8, 1, 1, 1, 1),
			want:     []bool{true, true, false, false},
		},
		{
			name:     "runs off the end of the last extent",
			overlaps: mapped[:1],
			run:      consecutiveRun(12, 1, 1, 1, 1),
			want:     []bool{false, false, true, true},
		},
		{
			name:     "crosses the gap between two extents",
			overlaps: mapped,
			run:      consecutiveRun(13, 1, 1, 1, 1, 1, 1, 1, 1, 1),
			want:     []bool{false, true, true, true, true, true, true, false, false},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, classifyRun(tc.overlaps, tc.run, nil))
		})
	}
}

// TestClassifyRunMatchesPerBlockLookup is the differential guard on the ordered
// merge: against a randomly fragmented index it must give the identical verdict
// to supersedesLocked, which reaches it with one tree descent per block.
func TestClassifyRunMatchesPerBlockLookup(t *testing.T) {
	rng := rand.New(rand.NewSource(20260904))
	vb := &VB{}

	// A fragmented map: extents of random length with random gaps between
	// them, and per-block sequence numbers that rise and fall within each.
	for block := uint64(0); block < 4096; {
		block += uint64(rng.Intn(4)) // gap, sometimes zero
		n := 1 + rng.Intn(12)
		seqNums := make([]uint64, n)
		for i := range seqNums {
			seqNums[i] = uint64(rng.Intn(2000))
		}
		vb.BlocksToObject.lookup.set(BlockLookup{
			StartBlock: block,
			NumBlocks:  uint16(n),
			SeqNums:    seqNums,
		})
		block += uint64(n)
	}
	require.NotZero(t, vb.BlocksToObject.lookup.len())

	for range 500 {
		start := uint64(rng.Intn(4096))
		run := make([]Block, 1+rng.Intn(64))
		for i := range run {
			run[i] = Block{Block: start + uint64(i), SeqNum: uint64(rng.Intn(2000))}
		}

		overlaps := vb.BlocksToObject.lookup.collectOverlaps(nil, run[0].Block, run[len(run)-1].Block+1)
		got := classifyRun(overlaps, run, nil)

		want := make([]bool, len(run))
		for i, candidate := range run {
			want[i] = vb.supersedesLocked(candidate)
		}

		require.Equal(t, want, got, "run starting at block %d", start)
	}
}
