package viperblock

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
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
