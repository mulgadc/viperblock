package viperblock

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gateBackend wraps a file backend so a test can hold a live-checkpoint or
// chunk write at a barrier, or lose a checkpoint write's ack after it lands.
type gateBackend struct {
	*file.Backend

	parkCheckpoint atomic.Bool
	parkChunk      atomic.Bool
	loseAck        atomic.Bool

	entered chan struct{}
	release chan struct{}
}

var _ types.Backend = (*gateBackend)(nil)

func newGateBackend(t *testing.T, vb *VB) *gateBackend {
	t.Helper()
	fb, ok := vb.Backend.(*file.Backend)
	require.True(t, ok)
	g := &gateBackend{Backend: fb, entered: make(chan struct{}), release: make(chan struct{})}
	vb.Backend = g
	vb.checkpointRetryBackoff = time.Microsecond
	return g
}

func (g *gateBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	switch fileType {
	case types.FileTypeBlockCheckpointLive:
		if g.parkCheckpoint.CompareAndSwap(true, false) {
			// Parked before the write: the durable checkpoint is still the old one.
			close(g.entered)
			<-g.release
		}
		if g.loseAck.Load() {
			if err := g.Backend.WriteCtx(ctx, fileType, objectId, headers, data); err != nil {
				return err
			}
			return errors.New("injected: live checkpoint ack lost after the write landed")
		}
	case types.FileTypeChunk:
		if g.parkChunk.CompareAndSwap(true, false) {
			// Parked after the write: the chunk is listable but not installed.
			if err := g.Backend.WriteCtx(ctx, fileType, objectId, headers, data); err != nil {
				return err
			}
			close(g.entered)
			<-g.release
			return nil
		}
	default:
	}
	return g.Backend.WriteCtx(ctx, fileType, objectId, headers, data)
}

// liveCheckpointChunks returns the durable live checkpoint's bytes and the
// chunk IDs it names.
func liveCheckpointChunks(t *testing.T, vb *VB) ([]byte, map[uint64]struct{}) {
	t.Helper()
	data, err := vb.Backend.ReadCtx(context.Background(), types.FileTypeBlockCheckpointLive, 0, 0, 0)
	require.NoError(t, err)
	ids := make(map[uint64]struct{})
	require.NoError(t, walkBlockCheckpoint(data, vb.Version, vb.BlockToObjectWAL.WALMagic, func(b BlockLookup) {
		ids[b.ObjectID] = struct{}{}
	}))
	return data, ids
}

func readChunk(t *testing.T, vb *VB, id uint64) []byte {
	t.Helper()
	data, err := vb.Backend.ReadCtx(context.Background(), types.FileTypeChunk, id, 0, 0)
	require.NoErrorf(t, err, "chunk %d named by the durable checkpoint must be readable", id)
	return data
}

func refcountOf(vb *VB, id uint64) (uint64, bool) {
	vb.BlocksToObject.mu.RLock()
	defer vb.BlocksToObject.mu.RUnlock()
	n, ok := vb.gcRefcount[id]
	return n, ok
}

// A drain landing between runGCSweep's checkpoint and its sweep supersedes a
// chunk the durable checkpoint names, and parks writing the checkpoint that
// would stop naming it. The sweep must leave the durable checkpoint whole.
// Verifies: ADR-0003:S7.
func TestS7_ChunkGCKeepsAChunkTheDurableCheckpointNames(t *testing.T) {
	vb := newGCTestVB(t, t.TempDir(), "vol-gc-durable", true)
	gate := newGateBackend(t, vb)
	ctx := context.Background()

	writeAndChunk(t, vb, 0, randomBlockData(1))
	chunkA := vb.ObjectNum.Load() - 1
	require.NoError(t, vb.DrainToBackendCtx(ctx))

	durableBefore, named := liveCheckpointChunks(t, vb)
	require.Contains(t, named, chunkA, "the durable checkpoint must name chunk A for this test to mean anything")
	chunkBytes := make(map[uint64][]byte, len(named))
	for id := range named {
		chunkBytes[id] = readChunk(t, vb, id)
	}

	overwrite := randomBlockData(1)
	require.NoError(t, vb.Write(0, overwrite))
	gate.parkCheckpoint.Store(true)
	drainErr := make(chan error, 1)
	go func() { drainErr <- vb.DrainToBackendCtx(ctx) }()
	<-gate.entered

	refs, tracked := refcountOf(vb, chunkA)
	require.True(t, tracked)
	require.Zero(t, refs, "chunk A must be superseded in the live map while the durable checkpoint still names it")

	vb.sweepChunks(ctx)

	durableAfter, _ := liveCheckpointChunks(t, vb)
	require.Equal(t, durableBefore, durableAfter, "nothing may have rewritten the durable checkpoint while its write is parked")
	for id, want := range chunkBytes {
		assert.Equalf(t, want, readChunk(t, vb, id),
			"ADR-0003:S7 violated: chunk %d is named by the durable live checkpoint and must read back unchanged after a sweep; select candidates against the checkpoint, not live refcounts", id)
	}

	close(gate.release)
	require.NoError(t, <-drainErr)
	vb.sweepChunks(ctx)
	assertChunkGone(t, vb.Backend, vb.VolumeName, chunkA)
	assertClosure(t, vb)
	got, err := vb.ReadAt(0, uint64(vb.BlockSize))
	require.NoError(t, err)
	assert.Equal(t, overwrite, got)
}

// A checkpoint serialised before a chunk was superseded is still being
// written. The sweep must not delete a chunk it is about to make durable.
// Verifies: ADR-0003:S7.
func TestS7_ChunkGCKeepsAChunkAnInFlightCheckpointNames(t *testing.T) {
	vb := newGCTestVB(t, t.TempDir(), "vol-gc-inflight", true)
	gate := newGateBackend(t, vb)
	ctx := context.Background()

	writeAndChunk(t, vb, 5, randomBlockData(1))
	require.NoError(t, vb.SaveLiveCheckpointCtx(ctx))

	writeAndChunk(t, vb, 0, randomBlockData(1))
	chunkA := vb.ObjectNum.Load() - 1

	gate.parkCheckpoint.Store(true)
	saveErr := make(chan error, 1)
	go func() { saveErr <- vb.SaveLiveCheckpointCtx(ctx) }()
	<-gate.entered

	overwrite := randomBlockData(1)
	writeAndChunk(t, vb, 0, overwrite)
	refs, _ := refcountOf(vb, chunkA)
	require.Zero(t, refs)

	vb.sweepChunks(ctx)
	assertChunkPresent(t, vb.Backend, vb.VolumeName, chunkA)

	close(gate.release)
	require.NoError(t, <-saveErr)
	_, named := liveCheckpointChunks(t, vb)
	require.Contains(t, named, chunkA, "the parked checkpoint landed naming chunk A")
	vb.sweepChunks(ctx)
	assertChunkPresent(t, vb.Backend, vb.VolumeName, chunkA)

	require.NoError(t, vb.SaveLiveCheckpointCtx(ctx))
	vb.sweepChunks(ctx)
	assertChunkGone(t, vb.Backend, vb.VolumeName, chunkA)
	assertClosure(t, vb)
	got, err := vb.ReadAt(0, uint64(vb.BlockSize))
	require.NoError(t, err)
	assert.Equal(t, overwrite, got)
}

// A checkpoint write can land and still report failure. Until a later one
// succeeds, the chunks it named must be treated as durable.
// Verifies: ADR-0003:S7.
func TestS7_ChunkGCKeepsAChunkAFailedCheckpointMayHaveWritten(t *testing.T) {
	vb := newGCTestVB(t, t.TempDir(), "vol-gc-ack-lost", true)
	gate := newGateBackend(t, vb)
	ctx := context.Background()

	writeAndChunk(t, vb, 5, randomBlockData(1))
	require.NoError(t, vb.SaveLiveCheckpointCtx(ctx))

	writeAndChunk(t, vb, 0, randomBlockData(1))
	chunkA := vb.ObjectNum.Load() - 1
	gate.loseAck.Store(true)
	require.Error(t, vb.SaveLiveCheckpointCtx(ctx))
	gate.loseAck.Store(false)
	_, named := liveCheckpointChunks(t, vb)
	require.Contains(t, named, chunkA, "the failed write must have landed for this test to mean anything")

	writeAndChunk(t, vb, 0, randomBlockData(1))
	vb.sweepChunks(ctx)
	assertChunkPresent(t, vb.Backend, vb.VolumeName, chunkA)

	require.NoError(t, vb.SaveLiveCheckpointCtx(ctx))
	vb.sweepChunks(ctx)
	assertChunkGone(t, vb.Backend, vb.VolumeName, chunkA)
	assertClosure(t, vb)
}

// The first sweep lists the chunk prefix to find orphans. A chunk uploaded but
// not yet installed is listable and untracked, and is not an orphan.
// Verifies: ADR-0003:S7.
func TestS7_ChunkGCReconcileSkipsAChunkStillBeingInstalled(t *testing.T) {
	vb := newGCTestVB(t, t.TempDir(), "vol-gc-uploading", true)
	gate := newGateBackend(t, vb)
	ctx := context.Background()

	writeAndChunk(t, vb, 5, randomBlockData(1))
	require.NoError(t, vb.SaveLiveCheckpointCtx(ctx))
	require.False(t, vb.gcReconciled.Load(), "the sweep below must be the one that reconciles")

	data := randomBlockData(1)
	require.NoError(t, vb.Write(0, data))
	require.NoError(t, vb.Flush())
	chunkC := vb.ObjectNum.Load()
	gate.parkChunk.Store(true)
	uploadErr := make(chan error, 1)
	go func() { uploadErr <- vb.WriteWALToChunk(true) }()
	<-gate.entered

	vb.sweepChunks(ctx)
	require.True(t, vb.gcReconciled.Load())
	_, tracked := refcountOf(vb, chunkC)
	assert.False(t, tracked, "an upload awaiting install must not be tracked as an orphan")
	assertChunkPresent(t, vb.Backend, vb.VolumeName, chunkC)

	close(gate.release)
	require.NoError(t, <-uploadErr)
	require.NoError(t, vb.SaveLiveCheckpointCtx(ctx))
	vb.sweepChunks(ctx)
	assertChunkPresent(t, vb.Backend, vb.VolumeName, chunkC)
	assertClosure(t, vb)
	got, err := vb.ReadAt(0, uint64(vb.BlockSize))
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// A process that has not written a live checkpoint cannot know which chunks
// the durable one names, so it deletes nothing until runGCSweep writes one.
func TestChunkGC_SweepWaitsForAKnownDurableCheckpoint(t *testing.T) {
	vb := newGCTestVB(t, t.TempDir(), "vol-gc-unknown", true)
	ctx := context.Background()

	writeAndChunk(t, vb, 0, randomBlockData(1))
	chunkA := vb.ObjectNum.Load() - 1
	writeAndChunk(t, vb, 0, randomBlockData(1))
	refs, _ := refcountOf(vb, chunkA)
	require.Zero(t, refs)

	vb.sweepChunks(ctx)
	assertChunkPresent(t, vb.Backend, vb.VolumeName, chunkA)

	vb.runGCSweep(ctx)
	assertChunkGone(t, vb.Backend, vb.VolumeName, chunkA)
	assertClosure(t, vb)
}
