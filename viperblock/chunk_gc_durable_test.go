package viperblock

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"path/filepath"

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
	delayWrite     atomic.Bool

	// delayed commits a checkpoint PUT that failed on the client but is
	// still in flight, so the test can land it after a newer PUT.
	delayed func() error

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
		if g.delayWrite.Load() {
			if g.delayed == nil {
				d := append([]byte(nil), (*data)...)
				var h []byte
				if headers != nil {
					h = append([]byte(nil), (*headers)...)
				}
				g.delayed = func() error {
					return g.Backend.WriteCtx(context.Background(), fileType, objectId, &h, &d)
				}
			}
			return errors.New("injected: live checkpoint PUT timed out while still in flight")
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

// A checkpoint write can land and still report failure. The chunks it named
// stay protected even after a later write succeeds.
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
	assertChunkPresent(t, vb.Backend, vb.VolumeName, chunkA)
	assertClosure(t, vb)
}

// A checkpoint PUT that timed out on the client can still land at the store
// after a newer PUT succeeds, making the older map durable again. No chunk it
// names may be swept. Verifies: ADR-0003:S7.
func TestS7_ChunkGCKeepsAChunkALateCheckpointWriteRestores(t *testing.T) {
	vb := newGCTestVB(t, t.TempDir(), "vol-gc-late-put", true)
	gate := newGateBackend(t, vb)
	ctx := context.Background()

	writeAndChunk(t, vb, 5, randomBlockData(1))
	require.NoError(t, vb.SaveLiveCheckpointCtx(ctx))

	dataA := randomBlockData(1)
	writeAndChunk(t, vb, 0, dataA)
	chunkA := vb.ObjectNum.Load() - 1
	gate.delayWrite.Store(true)
	require.Error(t, vb.SaveLiveCheckpointCtx(ctx))
	gate.delayWrite.Store(false)
	require.NotNil(t, gate.delayed)

	writeAndChunk(t, vb, 0, randomBlockData(1))
	require.NoError(t, vb.SaveLiveCheckpointCtx(ctx))
	_, named := liveCheckpointChunks(t, vb)
	require.NotContains(t, named, chunkA, "the newer checkpoint must have superseded chunk A")
	vb.sweepChunks(ctx)

	require.NoError(t, gate.delayed(), "the timed-out PUT lands last")
	_, named = liveCheckpointChunks(t, vb)
	require.Contains(t, named, chunkA)
	for id := range named {
		readChunk(t, vb, id)
	}
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

// A process that has neither loaded nor written a live checkpoint cannot know
// which chunks the durable one names, so it deletes nothing until it does.
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

// GC starts when the VB is constructed, before the opener loads the map. A
// sweep in that window must neither delete a chunk nor rewrite the durable
// checkpoint with the still-empty map.
// Verifies: ADR-0003:S7.
func TestS7_ChunkGCLeavesAVolumeWhoseMapIsNotLoaded(t *testing.T) {
	root := t.TempDir()
	const vol = "vol-gc-unloaded"
	ctx := context.Background()

	writer := newGCTestVB(t, root, vol, true)
	data := randomBlockData(1)
	writeAndChunk(t, writer, 0, data)
	chunkA := writer.ObjectNum.Load() - 1
	require.NoError(t, writer.DrainToBackendCtx(ctx))
	require.NoError(t, writer.SaveState())
	durableBefore, named := liveCheckpointChunks(t, writer)
	require.Contains(t, named, chunkA)

	opener, err := New(&VB{
		VolumeName:      vol,
		VolumeSize:      volumeSize,
		BaseDir:         filepath.Join(root, "viperblock"),
		WALSyncInterval: -1,
		GCEnabled:       true,
		GCInterval:      -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}, FileBackend, file.FileConfig{VolumeName: vol, VolumeSize: volumeSize, BaseDir: root})
	require.NoError(t, err)
	require.NoError(t, opener.Backend.Init())
	t.Cleanup(func() { opener.StopChunkUploader(); opener.StopWALSyncer() })

	opener.runGCSweep(ctx)

	durableAfter, _ := liveCheckpointChunks(t, opener)
	assert.Equal(t, durableBefore, durableAfter,
		"ADR-0003:S7 violated: a sweep before the map is loaded rewrote the durable live checkpoint; never force a checkpoint from GC")
	assertChunkPresent(t, opener.Backend, vol, chunkA)
}

// After a restart an uploaded-but-never-installed chunk can sit above the
// restored counter. Reconcile must move the counter past it, and an ID that
// is mid-upload is never a candidate even if already tracked at zero.
// Verifies: ADR-0003:S7.
func TestS7_ChunkGCNeverDeletesAChunkBeingUploaded(t *testing.T) {
	vb := newGCTestVB(t, t.TempDir(), "vol-gc-reuse", true)
	gate := newGateBackend(t, vb)
	ctx := context.Background()

	writeAndChunk(t, vb, 5, randomBlockData(1))
	require.NoError(t, vb.SaveLiveCheckpointCtx(ctx))

	orphan := vb.ObjectNum.Load() + 2
	headers := vb.ChunkHeader()
	body := randomBlockData(1)
	require.NoError(t, gate.Backend.WriteCtx(ctx, types.FileTypeChunk, orphan, &headers, &body))
	vb.sweepChunks(ctx)
	require.Greater(t, vb.ObjectNum.Load(), orphan, "reconcile must advance the counter past a listed orphan")
	assertChunkGone(t, vb.Backend, vb.VolumeName, orphan)

	// Stands in for an ID tracked at zero and then minted again.
	reused := vb.ObjectNum.Load()
	vb.BlocksToObject.mu.Lock()
	vb.gcRefcount[reused] = 0
	vb.BlocksToObject.mu.Unlock()

	data := randomBlockData(1)
	require.NoError(t, vb.Write(0, data))
	require.NoError(t, vb.Flush())
	gate.parkChunk.Store(true)
	uploadErr := make(chan error, 1)
	go func() { uploadErr <- vb.WriteWALToChunk(true) }()
	<-gate.entered

	vb.sweepChunks(ctx)
	assertChunkPresent(t, vb.Backend, vb.VolumeName, reused)

	close(gate.release)
	require.NoError(t, <-uploadErr)
	require.NoError(t, vb.SaveLiveCheckpointCtx(ctx))
	vb.sweepChunks(ctx)
	assertClosure(t, vb)
	got, err := vb.ReadAt(0, uint64(vb.BlockSize))
	require.NoError(t, err)
	assert.Equal(t, data, got)
}
