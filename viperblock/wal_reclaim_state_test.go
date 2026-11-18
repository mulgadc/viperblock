package viperblock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stateFailBackend fails state pushes once reclaim is reachable, so the drain
// can be driven past its checkpoint and into the state write.
type stateFailBackend struct {
	types.Backend

	fail bool
}

func (s *stateFailBackend) WriteCtx(ctx context.Context, ft types.FileType, id uint64, h *[]byte, d *[]byte) error {
	if s.fail && ft == types.FileTypeConfig {
		return errors.New("injected state write failure")
	}
	return s.Backend.WriteCtx(ctx, ft, id, h, d)
}

// reopenVB replays the production boot order over an already-closed volume:
// state, then the live checkpoint, then whatever local WAL is left.
func reopenVB(t *testing.T, dir, vol string, like *VB) *VB {
	t.Helper()
	reopened, err := New(&VB{
		VolumeName:      vol,
		VolumeSize:      like.VolumeSize,
		BaseDir:         dir,
		WALSyncInterval: -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}, FileBackend, file.FileConfig{BaseDir: dir, VolumeName: vol})
	require.NoError(t, err)
	require.NoError(t, reopened.Backend.Init())
	reopened.StopChunkUploader()
	t.Cleanup(func() { reopened.StopWALSyncer() })
	require.NoError(t, reopened.LoadState())
	require.NoError(t, reopened.LoadLiveCheckpoint())
	require.NoError(t, reopened.RecoverLocalWALs())
	require.NoError(t, reopened.OpenWAL(&reopened.WAL,
		fmt.Sprintf("%s/%s", reopened.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, reopened.WAL.WallNum.Load(), reopened.GetVolume()))))
	require.NoError(t, reopened.OpenWAL(&reopened.BlockToObjectWAL,
		fmt.Sprintf("%s/%s", reopened.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, reopened.BlockToObjectWAL.WallNum.Load(), reopened.GetVolume()))))
	return reopened
}

// persistedState reads the volume's local config.json, which is the only
// source of the sequence counter for a reader that does not raise it from the
// live checkpoint.
func persistedState(t *testing.T, vb *VB) VBState {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(vb.BaseDir, vb.GetVolume(), "config.json"))
	require.NoError(t, err)
	var st VBState
	require.NoError(t, json.Unmarshal(raw, &st))
	return st
}

// TestReclaimPersistsTheCounterARollbackWouldRead is the property, not the
// mechanism. A reclaimed generation was the only record of the sequence
// numbers its writes used, and a binary that recovers the counter from
// config.json alone reissues them. classifyRun then discards the re-numbered
// writes as superseded, with no error anywhere.
func TestReclaimPersistsTheCounterARollbackWouldRead(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "reclaim-rollback", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	fill := func(b byte) []byte {
		buf := make([]byte, vb.BlockSize)
		for i := range buf {
			buf[i] = b
		}
		return buf
	}

	for i := range 8 {
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), fill(0xC1)))
	}
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	vb.pendingWALMu.Lock()
	held := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	require.Zero(t, held, "the drain must have reclaimed its generation")

	persistedSeq := persistedState(t, vb).SeqNum
	vb.StopWALSyncer()

	// A binary whose only source of the counter is config.json: the live map
	// comes from the checkpoint, the counter does not.
	rolled := reopenVB(t, dir, "reclaim-rollback", vb)
	rolled.SeqNum.Store(persistedSeq)

	want := fill(0xD2)
	require.NoError(t, rolled.WriteAt(0, want))
	require.NoError(t, rolled.DrainToBackendCtx(context.Background()))
	rolled.StopWALSyncer()

	reader := reopenVB(t, dir, "reclaim-rollback", vb)
	got, err := reader.ReadAt(0, uint64(vb.BlockSize))
	require.NoError(t, err)
	assert.Equal(t, want, got,
		"the overwrite must survive a reader that recovered the counter from state alone")
}

// TestReclaimPersistsCountersAtLeastTheLiveValues is the mechanism behind the
// test above: config.json has to name a counter no lower than the live one
// before the files that recorded it are unlinked.
func TestReclaimPersistsCountersAtLeastTheLiveValues(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "reclaim-counters", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	data := make([]byte, vb.BlockSize)
	for i := range 64 {
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), data))
	}
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	st := persistedState(t, vb)
	assert.GreaterOrEqual(t, st.SeqNum, vb.SeqNum.Load(),
		"config.json must name a sequence number the reclaimed generations cannot exceed")
}

// TestReclaimHeldWhenTheStateWriteFails gates the ordering. The counter is one
// of the durable things that replaces the local WAL, so a failure to persist
// it has to leave the files alone, exactly as a failed checkpoint does.
func TestReclaimHeldWhenTheStateWriteFails(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "reclaim-state-fail", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	sb := &stateFailBackend{Backend: vb.Backend}
	vb.Backend = sb

	data := make([]byte, vb.BlockSize)
	require.NoError(t, vb.WriteAt(0, data))

	sb.fail = true
	require.Error(t, vb.DrainToBackendCtx(context.Background()),
		"a state write that fails before reclaim must fail the drain")

	vb.pendingWALMu.Lock()
	held := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	require.Equal(t, 1, held, "the generation must be held until the counter is durable")

	filesBefore, _ := walDirBytes(t, vb)
	require.GreaterOrEqual(t, filesBefore, 2,
		"the held generation must stay on disk alongside the open one")

	sb.fail = false
	require.NoError(t, vb.WriteAt(uint64(vb.BlockSize), data))
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	vb.pendingWALMu.Lock()
	remaining := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	assert.Zero(t, remaining, "a durable counter must release every held generation")
}

// TestSnapshotDrainHoldsReclaimWhenTheStateWriteFails is the same ordering on
// the snapshot path, where both the checkpoint and the state write are
// non-fatal and the drain returns nil either way.
func TestSnapshotDrainHoldsReclaimWhenTheStateWriteFails(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "snap-state-fail", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	sb := &stateFailBackend{Backend: vb.Backend}
	vb.Backend = sb

	data := make([]byte, vb.BlockSize)
	require.NoError(t, vb.WriteAt(0, data))

	sb.fail = true
	require.NoError(t, vb.drainForSnapshot(), "the snapshot drain reports its own failures as warnings")

	vb.pendingWALMu.Lock()
	held := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	assert.Equal(t, 1, held, "the generation must be held until the counter is durable")

	files, _ := walDirBytes(t, vb)
	assert.GreaterOrEqual(t, files, 2, "the held generation must stay on disk")
}

// TestIdleDrainsWriteNoState keeps the idle path free. A forced drain rotates
// a generation even with nothing to consolidate, so the batch is rarely empty
// and the guard that matters is the counter not having moved. Without it an
// idle volume writes config.json on every tick, which is the amplification the
// checkpoint's own idle gate exists to prevent.
func TestIdleDrainsWriteNoState(t *testing.T) {
	dir := t.TempDir()
	vb, cb := openFastVBWithCounting(t, dir, "reclaim-no-state", 10*time.Second)

	data := make([]byte, vb.BlockSize)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
	after := cb.count(types.FileTypeConfig)
	require.Positive(t, after, "the drain that consolidated a block must have persisted the counter")

	for round := range 3 {
		require.NoError(t, vb.DrainToBackendCtx(context.Background()))
		assert.Equal(t, after, cb.count(types.FileTypeConfig),
			"idle drain %d wrote state with the counter unmoved", round)
	}
}

// TestEncryptedReclaimWritesNoState keeps encrypted volumes out of the drain's
// state write. They restart at the persisted high-water and reserve each
// window durably before publishing it, and SaveStateCtx samples that
// high-water outside the lock that persists it, so a drain-time write could
// persist a value older than a reservation that landed in between.
func TestEncryptedReclaimWritesNoState(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "reclaim-encrypted", testKey(t, 0x51))
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	cb := &countingBackend{Backend: vb.Backend, counts: make(map[types.FileType]int)}
	vb.Backend = cb

	data := make([]byte, vb.BlockSize)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	vb.pendingWALMu.Lock()
	held := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	require.Zero(t, held, "the drain must have reclaimed, so it reached the state write")

	assert.Zero(t, cb.count(types.FileTypeConfig),
		"an encrypted drain must not persist state: reserveSeqNum owns that counter")
}
