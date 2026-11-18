package viperblock

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// checkpointBarrierHold bounds how long the first checkpoint writer is held
// inside its PUT when no second writer reaches the backend. A second writer
// that is not serialised arrives within microseconds, so this only ever
// lengthens the passing run.
const checkpointBarrierHold = 2 * time.Second

// hookCheckpointBackend runs fn before delegating the first live-checkpoint
// write: the window between the map being serialised and the PUT landing.
type hookCheckpointBackend struct {
	types.Backend

	once sync.Once
	fn   func()
}

func (h *hookCheckpointBackend) WriteCtx(ctx context.Context, ft types.FileType, id uint64, headers *[]byte, data *[]byte) error {
	if ft == types.FileTypeBlockCheckpointLive {
		h.once.Do(h.fn)
	}
	return h.Backend.WriteCtx(ctx, ft, id, headers, data)
}

// cancelCheckpointBackend fails every live-checkpoint write and cancels the
// caller's context as it does, so the cancellation lands while the caller is
// inside its retry backoff rather than before the first attempt.
type cancelCheckpointBackend struct {
	types.Backend

	cancel context.CancelFunc
}

func (c *cancelCheckpointBackend) WriteCtx(ctx context.Context, ft types.FileType, id uint64, headers *[]byte, data *[]byte) error {
	if ft == types.FileTypeBlockCheckpointLive {
		c.cancel()
		return errors.New("injected checkpoint failure")
	}
	return c.Backend.WriteCtx(ctx, ft, id, headers, data)
}

// barrierCheckpointBackend holds the first live-checkpoint write inside its
// PUT and records whether a second one reaches the backend while it is there.
type barrierCheckpointBackend struct {
	types.Backend

	entered   chan struct{}
	release   chan struct{}
	closeOnce sync.Once

	mu       sync.Mutex
	seen     int
	inFlight bool
	overlap  bool
}

func (b *barrierCheckpointBackend) WriteCtx(ctx context.Context, ft types.FileType, id uint64, headers *[]byte, data *[]byte) error {
	if ft != types.FileTypeBlockCheckpointLive {
		return b.Backend.WriteCtx(ctx, ft, id, headers, data)
	}

	b.mu.Lock()
	b.seen++
	first := b.seen == 1
	if first {
		b.inFlight = true
	} else if b.inFlight {
		b.overlap = true
		b.closeOnce.Do(func() { close(b.release) })
	}
	b.mu.Unlock()

	if first {
		close(b.entered)
		select {
		case <-b.release:
		case <-time.After(checkpointBarrierHold):
		}
		b.mu.Lock()
		b.inFlight = false
		b.mu.Unlock()
	}
	return b.Backend.WriteCtx(ctx, ft, id, headers, data)
}

func (b *barrierCheckpointBackend) overlapped() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.overlap
}

// checkpointReader opens a second view of the volume that has no in-memory
// map and no WAL of its own, so every block it returns came from the durable
// live checkpoint and the chunks that checkpoint names.
func checkpointReader(t *testing.T, vb *VB) *VB {
	t.Helper()
	reader := &VB{
		VolumeName:   vb.VolumeName,
		VolumeSize:   vb.VolumeSize,
		BlockSize:    vb.BlockSize,
		ObjBlockSize: vb.ObjBlockSize,
		Version:      vb.Version,
		BaseDir:      vb.BaseDir,
		Backend:      vb.Backend,
		BlockToObjectWAL: WAL{
			WALMagic: vb.BlockToObjectWAL.WALMagic,
		},
		BlocksToObject: BlocksToObject{},
	}
	require.NoError(t, reader.LoadLiveCheckpoint())
	return reader
}

// consolidateBlock writes one block and rotates the WAL generation holding
// it, which is what marks the block map dirty.
func consolidateBlock(t *testing.T, vb *VB, block uint64, fill byte) []byte {
	t.Helper()
	buf := make([]byte, vb.BlockSize)
	for i := range buf {
		buf[i] = fill
	}
	require.NoError(t, vb.WriteAt(block*uint64(vb.BlockSize), buf))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))
	return buf
}

// TestCheckpointKeepsAMappingInstalledAfterTheMapWasSerialized gates where
// the dirty flag is cleared. The map is serialised under its read lock and
// the PUT happens after that lock is dropped, so a mapping installed in
// between is absent from the uploaded checkpoint. Clearing the flag after the
// write wipes that mapping's only signal that it still needs persisting, and
// reclaim then deletes the WAL generation holding it.
func TestCheckpointKeepsAMappingInstalledAfterTheMapWasSerialized(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "checkpoint-late-install", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	consolidateBlock(t, vb, 0, 0x11)

	var late []byte
	vb.Backend = &hookCheckpointBackend{Backend: vb.Backend, fn: func() {
		late = consolidateBlock(t, vb, 1, 0x22)
	}}

	require.NoError(t, vb.SaveLiveCheckpointCtx(context.Background()))

	assert.True(t, vb.BlocksToObject.dirty.Load(),
		"a mapping installed after the map was serialised is absent from this checkpoint, so the map must still read dirty")

	// It really is absent: the durable checkpoint cannot name a block that
	// did not exist when the map was serialised, and an unmapped block reads
	// as a hole rather than as the data the guest was told was written.
	_, err := checkpointReader(t, vb).ReadAt(uint64(vb.BlockSize), uint64(vb.BlockSize))
	require.ErrorIs(t, err, ErrZeroBlock,
		"the late block must be missing from the first checkpoint for this test to mean anything")

	// Because the map still reads dirty, the next caller writes rather than
	// returning early, and the late mapping becomes durable.
	require.NoError(t, vb.SaveLiveCheckpointCtx(context.Background()))
	got, err := checkpointReader(t, vb).ReadAt(uint64(vb.BlockSize), uint64(vb.BlockSize))
	require.NoError(t, err)
	assert.Equal(t, late, got, "the next checkpoint must carry the mapping the first one missed")
}

// TestCheckpointCancelledInBackoffLeavesTheMapDirty covers the exit a
// statement at the end of the function would miss. A context cancelled inside
// the retry backoff returns early, and the map it did not persist must not be
// left reading clean: the next drain would skip its checkpoint and reclaim the
// generation holding the only record of those blocks.
func TestCheckpointCancelledInBackoffLeavesTheMapDirty(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "checkpoint-cancelled", nil)
	vb.StopChunkUploader()
	// Long enough that the cancelled context is the only ready case in the
	// backoff select, so the early return is the exit under test.
	vb.checkpointRetryBackoff = time.Minute
	t.Cleanup(func() { vb.StopWALSyncer() })

	want := consolidateBlock(t, vb, 0, 0x33)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	durable := vb.Backend
	vb.Backend = &cancelCheckpointBackend{Backend: durable, cancel: cancel}

	require.ErrorIs(t, vb.SaveLiveCheckpointCtx(ctx), context.Canceled)
	require.True(t, vb.BlocksToObject.dirty.Load(),
		"a checkpoint cancelled before it persisted the map must leave the map dirty")

	// The next drain therefore writes a checkpoint naming the block before it
	// reclaims the generation that holds it.
	vb.Backend = durable
	require.NoError(t, vb.DrainToBackend())

	vb.pendingWALMu.Lock()
	held := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	require.Zero(t, held, "the drain must have reclaimed its generations")

	got, err := checkpointReader(t, vb).ReadAt(0, uint64(vb.BlockSize))
	require.NoError(t, err)
	assert.Equal(t, want, got,
		"the block must survive the reclaim, which means the drain's checkpoint named it")
}

// TestConcurrentCheckpointWritersDoNotOverlap gates the serialisation. Every
// writer PUTs the same key, so two of them in flight at once can land in
// either order and leave the durable checkpoint holding a map older than one
// already written, with the dirty flag saying it is current.
func TestConcurrentCheckpointWritersDoNotOverlap(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "checkpoint-concurrent", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	consolidateBlock(t, vb, 0, 0x44)

	bb := &barrierCheckpointBackend{
		Backend: vb.Backend,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	vb.Backend = bb

	firstDone := make(chan error, 1)
	go func() { firstDone <- vb.SaveLiveCheckpointCtx(context.Background()) }()
	<-bb.entered

	// The second writer carries a newer map than the one the first writer is
	// holding inside its PUT.
	newer := consolidateBlock(t, vb, 1, 0x55)

	secondDone := make(chan error, 1)
	go func() { secondDone <- vb.SaveLiveCheckpointCtx(context.Background()) }()

	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)

	assert.False(t, bb.overlapped(),
		"a second checkpoint writer reached the backend while the first was still in its PUT, so the two can land out of order")

	got, err := checkpointReader(t, vb).ReadAt(uint64(vb.BlockSize), uint64(vb.BlockSize))
	require.NoError(t, err)
	assert.Equal(t, newer, got,
		"the durable checkpoint must end up with the newer map, not the older snapshot that was in flight first")
}

// TestDrainSkippedAsCleanDoesNotReclaimAnUnnamedGeneration is the end-to-end
// consequence, driven through the real drain. A generation consolidated while
// a checkpoint is in flight is named by no durable checkpoint; if the flag it
// set is wiped, the next drain returns early as clean and reclaims it anyway,
// and an acknowledged write reads back as zeros with no error anywhere.
func TestDrainSkippedAsCleanDoesNotReclaimAnUnnamedGeneration(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "drain-clean-reclaim", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	var late []byte
	var lateGen uint64
	vb.Backend = &hookCheckpointBackend{Backend: vb.Backend, fn: func() {
		late = consolidateBlock(t, vb, 1, 0x66)
		vb.pendingWALMu.Lock()
		require.Len(t, vb.consolidatedWALs, 1,
			"the generation consolidated inside the checkpoint window must be the only one pending")
		lateGen = vb.consolidatedWALs[0]
		vb.pendingWALMu.Unlock()
	}}

	consolidateBlock(t, vb, 0, 0x77)
	require.NoError(t, vb.DrainToBackend())

	// The first drain only ever had the batch it took, so the generation
	// consolidated inside its checkpoint window is still on disk.
	for _, path := range vb.walGenerationFiles(lateGen) {
		_, err := os.Stat(path)
		require.NoError(t, err, "the first drain must not reclaim a generation its checkpoint does not name: %s", filepath.Base(path))
	}

	require.NoError(t, vb.DrainToBackend())

	vb.pendingWALMu.Lock()
	held := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	require.Zero(t, held, "the second drain must have released the held generation")

	got, err := checkpointReader(t, vb).ReadAt(uint64(vb.BlockSize), uint64(vb.BlockSize))
	require.NoError(t, err)
	assert.Equal(t, late, got,
		"the block whose generation was reclaimed must be readable from the durable checkpoint")
}
