package viperblock

import (
	"context"
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

// walDirBytes returns the file count and total size of a volume's local WAL
// directory, the space that grew 1:1 with guest writes before reclaim existed.
func walDirBytes(t *testing.T, vb *VB) (int, int64) {
	t.Helper()
	baseDir := vb.WAL.BaseDir
	if vb.UseShardedWAL && vb.ShardedWAL != nil {
		baseDir = vb.ShardedWAL.BaseDir
	}
	dir := filepath.Join(baseDir, vb.GetVolume(), "wal", "chunks")

	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return 0, 0
	}
	require.NoError(t, err)

	var total int64
	for _, e := range entries {
		info, err := e.Info()
		require.NoError(t, err)
		total += info.Size()
	}
	return len(entries), total
}

// TestWALReclaimBoundsLocalGrowth is the regression gate. A consolidated WAL
// generation whose blocks are named by a durable checkpoint is redundant, but
// nothing deleted it: 128 MiB of guest writes left 128.9 MiB of WAL on disk
// across 9 files. That filled the WAL device, which collapsed the
// maxPendingBytes window onto its floor and multiplied drain frequency.
func TestWALReclaimBoundsLocalGrowth(t *testing.T) {
	runWithBackends(t, "wal_reclaim_growth", func(t *testing.T, vb *VB) {
		data := make([]byte, vb.BlockSize)
		block := 0

		// One generation per round: DrainToBackendCtx consolidates with
		// force=true, so every round rotates whatever it wrote.
		const rounds = 6
		const blocksPerRound = 64

		var firstFiles int
		var firstBytes int64

		for round := range rounds {
			for range blocksPerRound {
				require.NoError(t, vb.WriteAt(uint64(block)*uint64(vb.BlockSize), data))
				block++
			}
			require.NoError(t, vb.DrainToBackendCtx(context.Background()))

			files, bytes := walDirBytes(t, vb)
			if round == 0 {
				firstFiles, firstBytes = files, bytes
				continue
			}
			// Only the current open generation may remain, so the directory
			// must not grow with the number of rounds.
			assert.LessOrEqual(t, files, firstFiles,
				"round %d: WAL file count grew, consolidated generations are not being reclaimed", round)
			assert.LessOrEqual(t, bytes, firstBytes,
				"round %d: WAL bytes grew, consolidated generations are not being reclaimed", round)
		}
	})
}

// TestWALReclaimSkipsStrandedGeneration gates the ordering that makes reclaim
// safe. A generation whose chunk upload failed holds the only copy of its
// blocks, so it must stay on disk for retryPendingWALChunks even though a
// later drain succeeds and reclaims its own generation.
func TestWALReclaimSkipsStrandedGeneration(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "reclaim-stranded", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	eb := &errBackend{Backend: vb.Backend, failType: types.FileTypeChunk, failN: 1}
	vb.Backend = eb

	data := make([]byte, vb.BlockSize)
	require.NoError(t, vb.WriteAt(0, data))

	// The chunk upload fails, so the generation is stranded, not consolidated.
	require.Error(t, vb.DrainToBackendCtx(context.Background()),
		"a failed chunk upload must fail the drain")

	vb.pendingWALMu.Lock()
	strandedCount := len(vb.pendingWALChunks)
	consolidatedCount := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()

	assert.Equal(t, 1, strandedCount, "the failed generation must be queued for retry")
	assert.Equal(t, 0, consolidatedCount, "a stranded generation must never be marked consolidated")

	files, _ := walDirBytes(t, vb)
	assert.GreaterOrEqual(t, files, 2, "the stranded generation must stay on disk alongside the open one")
}

// TestWALReclaimDeferredUntilCheckpointLands gates the other half of the
// ordering. Until the checkpoint naming a generation's blocks is durable the
// local WAL is the only record that maps them, so a failed checkpoint must
// leave the file alone and the next successful one must release it.
func TestWALReclaimDeferredUntilCheckpointLands(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "reclaim-deferred", nil)
	vb.StopChunkUploader()
	vb.checkpointRetryBackoff = time.Microsecond
	t.Cleanup(func() { vb.StopWALSyncer() })

	// SaveLiveCheckpointCtx retries three times internally, so exhaust them.
	eb := &errBackend{Backend: vb.Backend, failType: types.FileTypeBlockCheckpointLive, failN: 3}
	vb.Backend = eb

	data := make([]byte, vb.BlockSize)
	require.NoError(t, vb.WriteAt(0, data))

	require.Error(t, vb.DrainToBackendCtx(context.Background()),
		"a checkpoint that exhausts its retries must fail the drain")

	vb.pendingWALMu.Lock()
	held := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	assert.Equal(t, 1, held, "the consolidated generation must be held until a checkpoint lands")

	filesBefore, _ := walDirBytes(t, vb)
	assert.GreaterOrEqual(t, filesBefore, 2,
		"the consolidated generation must stay on disk while the checkpoint is stale")

	// The injected failures are spent; this drain checkpoints cleanly.
	require.NoError(t, vb.WriteAt(uint64(vb.BlockSize), data))
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	vb.pendingWALMu.Lock()
	remaining := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	assert.Equal(t, 0, remaining, "a durable checkpoint must release every held generation")

	filesAfter, _ := walDirBytes(t, vb)
	assert.Less(t, filesAfter, filesBefore,
		"the held generations must be reclaimed once the checkpoint is durable")
}

// TestReclaimedWALStillReadsFromCheckpoint is the durability half: reclaim is
// only correct if the checkpoint that released a generation can rebuild its
// block map without it. A reader with no WAL of its own must see the data.
func TestReclaimedWALStillReadsFromCheckpoint(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "reclaim-durable", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	blocks := 32
	written := make([][]byte, blocks)
	for i := range blocks {
		buf := make([]byte, vb.BlockSize)
		for j := range buf {
			buf[j] = byte(i + 1)
		}
		written[i] = buf
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), buf))
	}

	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	vb.pendingWALMu.Lock()
	remaining := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	require.Equal(t, 0, remaining, "the drain must have reclaimed its generation")

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

	for i := range blocks {
		got, err := reader.ReadAt(uint64(i)*uint64(vb.BlockSize), uint64(vb.BlockSize))
		require.NoError(t, err, "block %d must still be readable after its WAL was reclaimed", i)
		assert.Equal(t, written[i], got, "block %d must be byte-identical after reclaim", i)
	}
}

// TestUnlinkBeforeCheckpointLosesAcknowledgedWrites is the reason reclaim
// waits for a checkpoint instead of unlinking as soon as the upload succeeds.
// The chunk upload makes the block DATA durable; it does not make the block
// MAP durable. Between the two the local WAL is the only record that says
// which chunk holds the block, so unlinking there loses an acknowledged write
// with no error anywhere.
func TestUnlinkBeforeCheckpointLosesAcknowledgedWrites(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "unlink-too-early", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	want := make([]byte, vb.BlockSize)
	for i := range want {
		want[i] = 0xAB
	}
	require.NoError(t, vb.WriteAt(0, want))

	// Upload the chunk and nothing else. This is the point the "just rm it
	// once it is in S3" shortcut would delete the file.
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	walDir := filepath.Join(vb.WAL.BaseDir, vb.GetVolume(), "wal", "chunks")
	entries, err := os.ReadDir(walDir)
	require.NoError(t, err)
	removed := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		// Keep the open generation, delete every consolidated one, exactly
		// what an unlink-on-upload-success implementation would do.
		// GetFilePath returns a volume-relative path, so compare basenames:
		// against the whole path this never matched and the open generation
		// was deleted too, which is not the shortcut being reproduced.
		open := filepath.Base(types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))
		if e.Name() == open {
			continue
		}
		require.NoError(t, os.Remove(filepath.Join(walDir, e.Name())))
		removed++
	}
	require.Positive(t, removed, "the consolidated generation must exist to be deleted")

	// A restarted process: backend state only, no in-memory map, and no WAL
	// left to replay because the shortcut deleted it.
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

	// ErrZeroBlock means "never written". The chunk holding this block is
	// still in the backend, intact; nothing points at it any more. nbd treats
	// ErrZeroBlock as a hole rather than an error (nbd/viperblock.go:537), so
	// the guest is handed zeros for a write it was told had succeeded.
	_, err = reader.ReadAt(0, uint64(vb.BlockSize))
	require.ErrorIs(t, err, ErrZeroBlock,
		"unlinking on upload success must lose the write: if this passes, the chunk upload alone made the map durable and the checkpoint ordering could be dropped")

	// The same volume, with the ordering honoured, keeps the data.
	require.NoError(t, vb.SaveLiveCheckpoint())
	after := &VB{
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
	require.NoError(t, after.LoadLiveCheckpoint())
	recovered, err := after.ReadAt(0, uint64(vb.BlockSize))
	require.NoError(t, err)
	assert.Equal(t, want, recovered,
		"once the checkpoint names the block, the chunk is reachable without the WAL")
}

// reopenAfterCrash opens a second VB over the same directory and backend the
// way a restarted process does: persisted state, then the live checkpoint,
// then whatever local WAL survived. No Close ran, so the state file holds the
// counters from the last SaveState rather than the volume's true high-water.
func reopenAfterCrash(t *testing.T, vb *VB) *VB {
	t.Helper()
	cfg := file.FileConfig{BaseDir: vb.BaseDir, VolumeName: vb.VolumeName}
	reopened, err := New(&VB{
		VolumeName:      vb.VolumeName,
		VolumeSize:      vb.VolumeSize,
		BaseDir:         vb.BaseDir,
		WALSyncInterval: -1,
	}, FileBackend, cfg)
	require.NoError(t, err)
	require.NoError(t, reopened.Backend.Init())
	require.NoError(t, reopened.LoadState())
	require.NoError(t, reopened.LoadLiveCheckpoint())
	require.NoError(t, reopened.RecoverLocalWALs())
	require.NoError(t, reopened.OpenWAL(&reopened.WAL,
		fmt.Sprintf("%s/%s", reopened.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, reopened.WAL.WallNum.Load(), reopened.GetVolume()))))
	require.NoError(t, reopened.OpenWAL(&reopened.BlockToObjectWAL,
		fmt.Sprintf("%s/%s", reopened.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, reopened.BlockToObjectWAL.WallNum.Load(), reopened.GetVolume()))))
	reopened.StopChunkUploader()
	t.Cleanup(func() { reopened.StopWALSyncer() })
	return reopened
}

// TestReclaimKeepsSeqNumMonotonicOnAnUnencryptedVolume covers what reclaim
// takes away. RecoverLocalWALs used to raise SeqNum past a stale persisted
// state by replaying every file on disk; reclaim deletes those files, and only
// encrypted volumes persist a high-water reservation to stand in for them.
//
// Without the checkpoint reconciliation, the reopened volume hands out
// sequence numbers it has already used, and classifyRun then discards the new
// write as superseded by the old one. The guest is told the write succeeded.
func TestReclaimKeepsSeqNumMonotonicOnAnUnencryptedVolume(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "seqnum-after-reclaim", nil)
	require.False(t, vb.EncryptionEnabled, "this test is about the path with no persisted high-water")
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	first := make([]byte, vb.BlockSize)
	for i := range first {
		first[i] = 0x11
	}
	for i := range 48 {
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), first))
	}
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	count, _ := walDirBytes(t, vb)
	require.LessOrEqual(t, count, 1, "the drain must have reclaimed its generations, leaving only the open one")

	reopened := reopenAfterCrash(t, vb)
	require.GreaterOrEqual(t, reopened.SeqNum.Load(), vb.SeqNum.Load(),
		"a reopen must not hand out a sequence number the reclaimed generations already used")

	second := make([]byte, vb.BlockSize)
	for i := range second {
		second[i] = 0x22
	}
	require.NoError(t, reopened.WriteAt(0, second))
	require.NoError(t, reopened.DrainToBackendCtx(context.Background()))

	final := reopenAfterCrash(t, reopened)
	got, err := final.ReadAt(0, uint64(final.BlockSize))
	require.NoError(t, err)
	assert.Equal(t, second, got,
		"the overwrite after the reopen must win: a reused sequence number makes classifyRun treat it as superseded and silently keep the old block")
}

// TestReclaimRetriesAFailedUnlink covers the other way lifetime growth comes
// back. A generation dropped from the list on a transient unlink failure is
// never attempted again, so its bytes are held until detach even though every
// drain afterwards succeeds.
func TestReclaimRetriesAFailedUnlink(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "reclaim-unlink-retry", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	buf := make([]byte, vb.BlockSize)
	for i := range buf {
		buf[i] = 0x33
	}
	for i := range 32 {
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), buf))
	}
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.pendingWALMu.Lock()
	pending := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	require.Positive(t, pending, "a consolidated generation must be waiting to be reclaimed")

	// Deny the unlink by taking write permission off the directory holding it.
	walDir := filepath.Join(vb.WAL.BaseDir, vb.GetVolume(), "wal", "chunks")
	info, err := os.Stat(walDir)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(walDir, 0o500))
	restored := false
	t.Cleanup(func() {
		if !restored {
			assert.NoError(t, os.Chmod(walDir, info.Mode()))
		}
	})

	before, _ := walDirBytes(t, vb)
	vb.reclaimConsolidatedWALs(vb.takeConsolidatedWALs())
	after, _ := walDirBytes(t, vb)
	require.Equal(t, before, after, "the unlink must actually have failed for this test to mean anything")

	vb.pendingWALMu.Lock()
	heldForRetry := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	require.Equal(t, pending, heldForRetry,
		"a generation whose unlink failed must stay on the list: dropping it holds those bytes for the life of the volume")

	require.NoError(t, os.Chmod(walDir, info.Mode()))
	restored = true

	vb.reclaimConsolidatedWALs(vb.takeConsolidatedWALs())
	vb.pendingWALMu.Lock()
	remaining := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	assert.Equal(t, 0, remaining, "the retry must reclaim the generation once the unlink can succeed")
}

// TestRecoveryWithASurvivingWALReadsByteIdentical covers the crash window
// reclaim leaves open: the checkpoint is durable and the unlink has not
// happened yet, so the next open sees both. RecoverLocalWALs replays the
// generation whose blocks that checkpoint already names.
//
// The overwrite matters. Replaying a generation that carries an older version
// of a block must not reinstate it over the newer one the checkpoint maps,
// which is the way a harmless replay turns into silent corruption.
func TestRecoveryWithASurvivingWALReadsByteIdentical(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "reclaim-crash-window", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	blocks := 16
	old := make([][]byte, blocks)
	for i := range blocks {
		buf := make([]byte, vb.BlockSize)
		for j := range buf {
			buf[j] = byte(i + 1)
		}
		old[i] = buf
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), buf))
	}
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	// Overwrite the first half, so the generation holding the old bytes is
	// still on disk when the newer one is consolidated over it.
	want := make([][]byte, blocks)
	copy(want, old)
	for i := range blocks / 2 {
		buf := make([]byte, vb.BlockSize)
		for j := range buf {
			buf[j] = byte(0xF0 + i)
		}
		want[i] = buf
		require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), buf))
	}
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))
	require.NoError(t, vb.SaveLiveCheckpoint())

	// The crash lands here: the checkpoint is durable and reclaim has not run,
	// so every consolidated generation is still on disk.
	count, _ := walDirBytes(t, vb)
	require.Positive(t, count, "the generations must still be present for this to be the crash window")

	reopened := reopenAfterCrash(t, vb)
	for i := range blocks {
		got, err := reopened.ReadAt(uint64(i)*uint64(vb.BlockSize), uint64(vb.BlockSize))
		require.NoError(t, err, "block %d must be readable with both a checkpoint and an un-reclaimed WAL present", i)
		assert.Equal(t, want[i], got,
			"block %d must read as its latest version: replaying a surviving generation must not reinstate the bytes the checkpoint already superseded", i)
	}
}

// TestReclaimLeavesAGenerationConsolidatedAfterTheBatchWasTaken drives the
// real drain and consolidates a second generation inside the window between
// the drain taking its batch and writing the checkpoint that covers it.
//
// That generation is named by no durable checkpoint, so reclaiming it loses
// the only record mapping its blocks, which is the same loss as unlinking on
// upload success. Calling reclaim directly would not gate the drain's
// ordering: it would stay green with the batch taken after the checkpoint.
func TestReclaimLeavesAGenerationConsolidatedAfterTheBatchWasTaken(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "reclaim-batch-boundary", nil)
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })

	write := func(block uint64, fill byte) {
		buf := make([]byte, vb.BlockSize)
		for i := range buf {
			buf[i] = fill
		}
		require.NoError(t, vb.WriteAt(block*uint64(vb.BlockSize), buf))
	}

	write(0, 0x11)
	require.NoError(t, vb.Flush())

	var late []uint64
	original := afterTakeConsolidatedWALs
	afterTakeConsolidatedWALs = func() {
		afterTakeConsolidatedWALs = original
		write(1, 0x22)
		require.NoError(t, vb.Flush())
		require.NoError(t, vb.WriteWALToChunk(true))
		late = vb.takeConsolidatedWALs()
		vb.returnConsolidatedWALs(late)
	}
	t.Cleanup(func() { afterTakeConsolidatedWALs = original })

	require.NoError(t, vb.DrainToBackend())

	require.Len(t, late, 1, "a generation must have been consolidated inside the window")
	for _, path := range vb.walGenerationFiles(late[0]) {
		_, err := os.Stat(path)
		assert.NoError(t, err,
			"a generation consolidated after the batch was taken is named by no durable checkpoint, so unlinking it loses the only record of its blocks")
	}

	vb.pendingWALMu.Lock()
	held := append([]uint64(nil), vb.consolidatedWALs...)
	vb.pendingWALMu.Unlock()
	assert.Equal(t, late, held, "it must still be waiting for the next checkpoint")
}

// TestReclaimAndChunkGCCannotLoseABlockTogether composes the two deletes.
// Chunk GC can delete a chunk the durable live checkpoint still names, since
// its candidate set comes from the live map. Reclaim takes the local WAL away,
// so the question is whether the generation that supersedes a swept chunk can
// be gone at the same time. It cannot: a chunk reaches zero references only
// because a later write superseded it, that write marks the map dirty under
// the same lock, and reclaim waits for a checkpoint covering it, which is a
// checkpoint that no longer names the swept chunk.
func TestReclaimAndChunkGCCannotLoseABlockTogether(t *testing.T) {
	root := t.TempDir()
	const vol = "vol-reclaim-gc"
	ctx := context.Background()

	backendConfig := file.FileConfig{BaseDir: root, VolumeName: vol}
	vb, err := New(&VB{
		VolumeName:      vol,
		VolumeSize:      64 * 1024 * 1024,
		BaseDir:         root,
		WALSyncInterval: -1,
		GCEnabled:       true,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}, FileBackend, backendConfig)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	vb.StopChunkUploader()
	t.Cleanup(func() { vb.StopWALSyncer() })
	vb.BlockSize = DefaultBlockSize
	vb.ObjBlockSize = 16 * DefaultBlockSize
	require.NoError(t, vb.SaveState())
	require.NoError(t, vb.OpenWAL(&vb.WAL,
		fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL,
		fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	const blocks = 8
	overwrite := func(fill byte) [][]byte {
		out := make([][]byte, blocks)
		for i := range blocks {
			buf := make([]byte, vb.BlockSize)
			for j := range buf {
				buf[j] = fill
			}
			out[i] = buf
			require.NoError(t, vb.WriteAt(uint64(i)*uint64(vb.BlockSize), buf))
		}
		return out
	}

	overwrite(0xA1)
	require.NoError(t, vb.DrainToBackendCtx(ctx))

	// The overwrite is consolidated but not checkpointed, so the durable
	// checkpoint still names the chunk it superseded and that chunk now has
	// no references in the live map.
	want := overwrite(0xB2)
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))

	vb.pendingWALMu.Lock()
	held := len(vb.consolidatedWALs)
	vb.pendingWALMu.Unlock()
	require.Equal(t, 1, held, "the superseding generation must still be held: no checkpoint names it yet")

	before, err := vb.Backend.ListObjectsCtx(ctx, vol+"/chunks/")
	require.NoError(t, err)
	vb.sweepChunks(ctx)
	after, err := vb.Backend.ListObjectsCtx(ctx, vol+"/chunks/")
	require.NoError(t, err)
	require.Less(t, len(after), len(before),
		"the sweep must have deleted the superseded chunk for this test to mean anything")

	// A restarted process: the durable checkpoint names a chunk the sweep
	// deleted, and the only thing that corrects it is the generation reclaim
	// has not released.
	reopened, err := New(&VB{
		VolumeName:      vol,
		VolumeSize:      vb.VolumeSize,
		BaseDir:         root,
		WALSyncInterval: -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}, FileBackend, backendConfig)
	require.NoError(t, err)
	require.NoError(t, reopened.Backend.Init())
	t.Cleanup(func() { reopened.StopChunkUploader(); reopened.StopWALSyncer() })
	require.NoError(t, reopened.LoadState())
	require.NoError(t, reopened.LoadLiveCheckpoint())
	require.NoError(t, reopened.RecoverLocalWALs())
	require.NoError(t, reopened.OpenWAL(&reopened.WAL,
		fmt.Sprintf("%s/%s", reopened.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, reopened.WAL.WallNum.Load(), reopened.GetVolume()))))
	require.NoError(t, reopened.OpenWAL(&reopened.BlockToObjectWAL,
		fmt.Sprintf("%s/%s", reopened.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, reopened.BlockToObjectWAL.WallNum.Load(), reopened.GetVolume()))))

	for i := range blocks {
		got, err := reopened.ReadAt(uint64(i)*uint64(vb.BlockSize), uint64(vb.BlockSize))
		require.NoError(t, err, "block %d must be readable: the chunk GC swept is superseded, not live", i)
		assert.Equal(t, want[i], got, "block %d must read as the overwrite, not as the swept chunk the checkpoint names", i)
	}
}
