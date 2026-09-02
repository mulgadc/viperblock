package viperblock

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests pin the staleness decision createChunkFile makes when the block
// it is about to repoint is already covered by a COALESCED extent.
//
// An extent's own SeqNum belongs to its first block; every other block in the
// run carries its own value in SeqNums. Comparing a new chunk's SeqNum against
// the extent's therefore compares against the wrong block, and gets it wrong in
// both directions: newer data dropped where the extent happens to start newer,
// superseded data installed where it happens to start older.
//
// Each case is read back after a cold reopen, because that is where it bites.
// The verdict is what goes into the checkpoint, so a fresh process — a restart
// after a failed seal, which is the only path that replays a WAL over an
// existing map — serves the wrong bytes with nothing to indicate it.

// chunkOf builds the chunk body and matched-block list for a run of
// consecutive blocks starting at start, each filled with fill and carrying the
// SeqNum at the same index.
func chunkOf(vb *VB, start uint64, fill byte, seqNums []uint64) ([]byte, []Block) {
	bs := int(vb.BlockSize)
	buf := bytes.Repeat([]byte{fill}, bs*len(seqNums))
	blocks := make([]Block, len(seqNums))
	for i, seq := range seqNums {
		blocks[i] = Block{Block: start + uint64(i), SeqNum: seq, Len: uint64(bs)}
	}
	return buf, blocks
}

// checkpointAndReopen publishes the map and boots a fresh VB over the same
// backend, so what follows reads only what was made durable.
func checkpointAndReopen(t *testing.T, vb *VB, root, volumeName string) *VB {
	t.Helper()
	require.NoError(t, vb.SaveState())
	require.NoError(t, vb.SaveLiveCheckpoint())
	return reopenGCTestVB(t, root, volumeName, true)
}

// requireBlockFill fails unless the whole block reads back as fill.
func requireBlockFill(t *testing.T, vb *VB, block uint64, fill byte, msg string) {
	t.Helper()
	bs := uint64(vb.BlockSize)
	got, err := vb.ReadAt(block*bs, bs)
	require.NoError(t, err)
	require.Equalf(t, bytes.Repeat([]byte{fill}, int(bs)), got,
		"%s (block %d first byte got=%#x want=%#x)", msg, block, got[0], fill)
}

// TestCoalescedExtent_NewerWriteToAMiddleBlockIsNotDropped is the data-loss
// direction. Block 2 is superseded by a strictly newer chunk, but the extent
// covering it starts at block 0 whose SeqNum is higher, so the run reads as
// stale and the newer bytes never land.
func TestCoalescedExtent_NewerWriteToAMiddleBlockIsNotDropped(t *testing.T) {
	root := t.TempDir()
	const vol = "vol-coalesced-newer-middle"
	vb := newGCTestVB(t, root, vol, true)
	ctx := context.Background()

	// One chunk covering blocks 0-3 as a single coalesced extent. Block 0 was
	// rewritten last, so the extent's SeqNum (100) is far above block 2's (11).
	buf, blocks := chunkOf(vb, 0, 0x11, []uint64{100, 10, 11, 12})
	require.NoError(t, vb.createChunkFile(ctx, 0, &buf, &blocks))

	// A later write to block 2 alone: SeqNum 50 supersedes block 2's 11.
	newer, newerBlocks := chunkOf(vb, 2, 0x22, []uint64{50})
	require.NoError(t, vb.createChunkFile(ctx, 0, &newer, &newerBlocks))

	reopened := checkpointAndReopen(t, vb, root, vol)
	requireBlockFill(t, reopened, 2, 0x22,
		"block 2's newer chunk was rejected against block 0's SeqNum, so an acknowledged write was dropped")
}

// TestCoalescedExtent_OlderWriteToAMiddleBlockIsRejected is the same mistake in
// the other direction: an older chunk accepted because the extent it overwrites
// starts at a lower SeqNum than the block it actually replaces.
func TestCoalescedExtent_OlderWriteToAMiddleBlockIsRejected(t *testing.T) {
	root := t.TempDir()
	const vol = "vol-coalesced-older-middle"
	vb := newGCTestVB(t, root, vol, true)
	ctx := context.Background()

	// Block 0 is the oldest in the run; block 2 is the newest.
	buf, blocks := chunkOf(vb, 0, 0x33, []uint64{10, 40, 90, 41})
	require.NoError(t, vb.createChunkFile(ctx, 0, &buf, &blocks))

	// A stale drain for block 2 at SeqNum 50: older than block 2's 90, newer
	// than the extent's 10.
	stale, staleBlocks := chunkOf(vb, 2, 0x44, []uint64{50})
	require.NoError(t, vb.createChunkFile(ctx, 0, &stale, &staleBlocks))

	reopened := checkpointAndReopen(t, vb, root, vol)
	requireBlockFill(t, reopened, 2, 0x33,
		"a stale chunk was accepted against block 0's SeqNum, so block 2 reverted to superseded bytes")
}

// TestCoalescedExtent_MixedRunKeepsOnlyItsNewerBlocks covers the granularity
// half. A replayed run spans blocks that are newer and blocks that are stale,
// and one verdict for the whole run either drops the newer blocks or installs
// the stale ones. A WAL replayed over an existing map produces exactly this.
func TestCoalescedExtent_MixedRunKeepsOnlyItsNewerBlocks(t *testing.T) {
	root := t.TempDir()
	const vol = "vol-coalesced-mixed-run"
	vb := newGCTestVB(t, root, vol, true)
	ctx := context.Background()

	// Blocks 0-3, with block 0 much newer than the rest.
	buf, blocks := chunkOf(vb, 0, 0x55, []uint64{900, 10, 11, 12})
	require.NoError(t, vb.createChunkFile(ctx, 0, &buf, &blocks))

	// A replay covering the same four blocks. Only blocks 1-3 are newer;
	// block 0's 800 is stale against the 900 already mapped.
	replay, replayBlocks := chunkOf(vb, 0, 0x66, []uint64{800, 810, 811, 812})
	require.NoError(t, vb.createChunkFile(ctx, 0, &replay, &replayBlocks))

	reopened := checkpointAndReopen(t, vb, root, vol)
	requireBlockFill(t, reopened, 0, 0x55, "block 0 took the replay's older bytes")
	for block := uint64(1); block <= 3; block++ {
		requireBlockFill(t, reopened, block, 0x66,
			"the run was rejected wholesale on block 0's verdict, so a superseded block survived")
	}
}
