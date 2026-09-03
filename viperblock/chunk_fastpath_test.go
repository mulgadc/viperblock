package viperblock

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// drainChunk buffers blocks [base, base+count) filled with fill, then drains
// them to a chunk, returning how many runs each classification path took.
func drainChunk(t *testing.T, vb *VB, base uint64, count int, fill byte) (fast, classified uint64) {
	t.Helper()

	blockSize := uint64(vb.BlockSize)
	data := bytes.Repeat([]byte{fill}, int(blockSize))
	for i := range count {
		require.NoError(t, vb.WriteAt((base+uint64(i))*blockSize, data))
	}
	require.NoError(t, vb.Flush())

	beforeFast := vb.chunkRunsFastPath.Load()
	beforeClassified := vb.chunkRunsClassified.Load()
	require.NoError(t, vb.WriteWALToChunk(true))

	return vb.chunkRunsFastPath.Load() - beforeFast,
		vb.chunkRunsClassified.Load() - beforeClassified
}

// assertBlocks reads count blocks back from base and fails unless every byte is
// fill, so a run installed by either path is checked against the same standard.
func assertBlocks(t *testing.T, vb *VB, base uint64, count int, fill byte) {
	t.Helper()

	blockSize := uint64(vb.BlockSize)
	want := bytes.Repeat([]byte{fill}, int(blockSize))
	for i := range count {
		got, err := vb.ReadAt((base+uint64(i))*blockSize, uint64(blockSize))
		require.NoError(t, err, "block %d", base+uint64(i))
		assert.Equal(t, want, got, "block %d", base+uint64(i))
	}
}

// TestChunkDrainTakesTheFastPathOnlyWhenNothingOverlaps pins which branch each
// shape of drain takes. A run the block map does not cover is all-new by
// construction, so one range query settles it; a run it does cover must fall
// back to the per-block classifier, because a coalesced extent's SeqNum belongs
// to its first block and a replayed run straddles opposite verdicts.
func TestChunkDrainTakesTheFastPathOnlyWhenNothingOverlaps(t *testing.T) {
	vb, _ := newEnospcTestVB(t)

	// Fresh volume: nothing is mapped, so the whole run is accepted at once.
	fast, classified := drainChunk(t, vb, 0, 64, 0xA1)
	assert.Equal(t, uint64(1), fast, "a run against an empty block map must take the fast path")
	assert.Zero(t, classified, "no run should have been classified block by block")
	assertBlocks(t, vb, 0, 64, 0xA1)

	// A disjoint region is still all-new, even though the map is not empty.
	fast, classified = drainChunk(t, vb, 512, 64, 0xB2)
	assert.Equal(t, uint64(1), fast, "a run into a region the map does not cover must take the fast path")
	assert.Zero(t, classified)
	assertBlocks(t, vb, 512, 64, 0xB2)

	// Rewriting mapped blocks must not: the verdict is per block there.
	fast, classified = drainChunk(t, vb, 0, 64, 0xC3)
	assert.Zero(t, fast, "a run overlapping the map must not be accepted whole")
	assert.Equal(t, uint64(1), classified)
	assertBlocks(t, vb, 0, 64, 0xC3)
	assertBlocks(t, vb, 512, 64, 0xB2)

	// Partial overlap is the case the range query must not wave through: only
	// the tail of this run is new.
	fast, classified = drainChunk(t, vb, 32, 64, 0xD4)
	assert.Zero(t, fast, "a run overlapping the map at one end only must not be accepted whole")
	assert.Equal(t, uint64(1), classified)
	assertBlocks(t, vb, 0, 32, 0xC3)
	assertBlocks(t, vb, 32, 64, 0xD4)

	// The map must survive a reopen: the fast path writes the same extents.
	require.NoError(t, vb.SaveState())
	require.NoError(t, vb.Close())
}

// TestChunkDrainFastPathSurvivesAColdReopen pins that extents the fast path
// installed are checkpointed and reloaded like any other, so the shortcut
// cannot leave the map correct only in memory.
func TestChunkDrainFastPathSurvivesAColdReopen(t *testing.T) {
	vb, _ := newEnospcTestVB(t)

	fast, _ := drainChunk(t, vb, 0, 128, 0x7E)
	require.Equal(t, uint64(1), fast)

	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
	require.NoError(t, vb.SaveState())

	vb.BlocksToObject.mu.Lock()
	vb.BlocksToObject.lookup.clear()
	vb.BlocksToObject.mu.Unlock()

	require.NoError(t, vb.LoadBlockStateCtx(context.Background()))
	assertBlocks(t, vb, 0, 128, 0x7E)
}
