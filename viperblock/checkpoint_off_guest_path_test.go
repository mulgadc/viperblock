package viperblock

import (
	"context"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingVB opens a file-backed VB with the background uploader stopped and a
// counting backend installed, so a test can attribute every backend write to
// the call that made it.
func countingVB(t *testing.T, name string) (*VB, *countingBackend) {
	t.Helper()
	vb := openEncryptedVBInDir(t, t.TempDir(), name, nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	cb := &countingBackend{Backend: vb.Backend, counts: map[types.FileType]int{}}
	vb.Backend = cb
	return vb, cb
}

// TestDrainChunksWritesNoCheckpoint is the point of the split. The guest stall
// path needs pendingBytes to fall, and rebuilding the whole block map does not
// lower it, so a chunk drain must not touch the live checkpoint at all.
func TestDrainChunksWritesNoCheckpoint(t *testing.T) {
	vb, cb := countingVB(t, "drain-chunks-only")

	data := make([]byte, vb.BlockSize)
	for i := range 16 {
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), data))
	}

	require.NoError(t, vb.DrainChunksCtx(context.Background()))

	assert.Positive(t, cb.count(types.FileTypeChunk),
		"a chunk drain must still upload the blocks it is draining")
	assert.Equal(t, 0, cb.count(types.FileTypeBlockCheckpointLive),
		"a chunk drain must not write the live checkpoint")
}

// TestDrainChunksLowersPendingBytes gates that the cheap half is the half that
// actually releases a blocked writer. If this stopped being true, the guest
// would spin in awaitBackpressure instead of stalling on the checkpoint.
func TestDrainChunksLowersPendingBytes(t *testing.T) {
	vb, _ := countingVB(t, "drain-chunks-pending")

	data := make([]byte, vb.BlockSize)
	for i := range 32 {
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), data))
	}
	before := vb.PendingBytes()
	require.Positive(t, before, "the writes must have registered as pending")

	require.NoError(t, vb.DrainChunksCtx(context.Background()))

	assert.Zero(t, vb.PendingBytes(),
		"a chunk drain must release every byte it made durable, from %d", before)
}

// TestDrainChunksDefersWALReclaim is the durability half. Reclaim is gated on
// the checkpoint, so a drain that skips the checkpoint must leave the WAL
// generation on disk: it is the only record mapping those blocks until a
// checkpoint names them.
func TestDrainChunksDefersWALReclaim(t *testing.T) {
	vb, _ := countingVB(t, "drain-chunks-reclaim")

	data := make([]byte, vb.BlockSize)
	for i := range 8 {
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), data))
	}

	require.NoError(t, vb.DrainChunksCtx(context.Background()))

	vb.pendingWALMu.Lock()
	held := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	assert.Equal(t, 1, held, "a chunk drain must hold its generation, not reclaim it")

	files, _ := walDirBytes(t, vb)
	assert.GreaterOrEqual(t, files, 2,
		"the consolidated generation must stay on disk alongside the open one")

	// The full drain checkpoints, which is what makes the held generation
	// redundant and releases it.
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	vb.pendingWALMu.Lock()
	remaining := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	assert.Zero(t, remaining, "the full drain must reclaim what the chunk drain held")
}

// TestFullDrainStillCheckpoints guards the callers that were deliberately left
// on the full drain -- the background uploader, Close, snapshots and the nbd
// entrypoints. Their semantics must be unchanged by the split.
func TestFullDrainStillCheckpoints(t *testing.T) {
	vb, cb := countingVB(t, "full-drain-checkpoint")

	data := make([]byte, vb.BlockSize)
	for i := range 8 {
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), data))
	}

	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	assert.Positive(t, cb.count(types.FileTypeChunk), "the full drain must upload chunks")
	assert.Positive(t, cb.count(types.FileTypeBlockCheckpointLive),
		"the full drain must still write the live checkpoint")
}

// TestReadsSurviveChunkOnlyDrain is the correctness gate on deferring the
// checkpoint: blocks made durable by a chunk drain must still read back
// through the WAL the drain deliberately did not reclaim.
func TestReadsSurviveChunkOnlyDrain(t *testing.T) {
	vb, _ := countingVB(t, "drain-chunks-readback")

	blocks := 24
	written := make([][]byte, blocks)
	for i := range blocks {
		buf := make([]byte, vb.BlockSize)
		for j := range buf {
			buf[j] = byte(i + 1)
		}
		written[i] = buf
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), buf))
	}

	require.NoError(t, vb.DrainChunksCtx(context.Background()))

	for i := range blocks {
		got, err := vb.ReadAt(uint64(i)*uint64(vb.BlockSize), uint64(vb.BlockSize))
		require.NoError(t, err, "block %d must be readable after a chunk-only drain", i)
		assert.Equal(t, written[i], got, "block %d must be byte-identical", i)
	}
}
