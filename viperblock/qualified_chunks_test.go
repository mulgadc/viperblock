package viperblock

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/require"
)

func claimQualified(t testing.TB, mv *mapVolume, epoch uint64) *VB {
	t.Helper()
	vb, _ := mv.engine(t, epoch)
	vb.QualifiedChunks = true
	vb.LaunchChunkIDs = &atomic.Uint64{}
	require.NoError(t, vb.LoadState())
	require.NoError(t, vb.LoadLiveCheckpoint())
	require.NoError(t, vb.ClaimMapCtx(context.Background()))
	require.NoError(t, vb.RecoverLocalWALs())
	openMapWALs(t, vb)
	return vb
}

func TestQualifiedChunkSnapshotCopyAndDescendant(t *testing.T) {
	ctx := context.Background()
	mv := newMapVolume(t, volumeSize, nil)
	legacy, _ := mv.create(t, 5)
	legacyBytes := randomBlockData(1)
	writeDrain(t, legacy, 0, legacyBytes)
	legacy.Detach()

	source := claimQualified(t, mv, 6)
	qualifiedBytes := randomBlockData(1)
	writeDrain(t, source, 1, qualifiedBytes)
	_, err := source.CreateSnapshot("source-snapshot")
	require.NoError(t, err)
	_, err = source.CopySnapshotMeta("source-snapshot", "source-copy")
	require.NoError(t, err)

	clone := createCloneVB(t, source, "source-copy")
	require.Equal(t, legacyBytes, readBlocks(t, clone, 0, 1))
	require.Equal(t, qualifiedBytes, readBlocks(t, clone, 1, 1))
	cloneBytes := randomBlockData(1)
	require.NoError(t, clone.Write(0, cloneBytes))
	require.NoError(t, clone.DrainToBackendCtx(ctx))
	_, err = clone.CreateSnapshot("clone-snapshot")
	require.NoError(t, err)

	descendant := createCloneVB(t, clone, "clone-snapshot")
	require.Equal(t, cloneBytes, readBlocks(t, descendant, 0, 1))
	require.Equal(t, qualifiedBytes, readBlocks(t, descendant, 1, 1))
}

func TestQualifiedChunkGCKeepsSameIDAtNewerEpoch(t *testing.T) {
	ctx := context.Background()
	mv := newMapVolume(t, volumeSize, nil)
	seed, _ := mv.create(t, 5)
	seed.Detach()

	old := claimQualified(t, mv, 6)
	writeDrain(t, old, 0, randomBlockData(1))
	oldRef, _, _, err := old.lookupChunk(0)
	require.NoError(t, err)
	old.Detach()

	newer := claimQualified(t, mv, 7)
	expected := randomBlockData(1)
	writeDrain(t, newer, 0, expected)
	currentRef, _, _, err := newer.lookupChunk(0)
	require.NoError(t, err)
	require.Equal(t, oldRef.ID, currentRef.ID)
	require.NotEqual(t, oldRef.Epoch, currentRef.Epoch)
	newer.Detach()

	reopened := claimQualified(t, mv, 8)
	reopened.runGCSweep(ctx)
	_, err = reopened.Backend.ReadChunkFromCtx(ctx, mv.name, oldRef, 0, 1)
	require.NoError(t, err)
	mv.clock.advance(DefaultMapDeleteDeferral)
	reopened.runGCSweep(ctx)
	_, err = reopened.Backend.ReadChunkFromCtx(ctx, mv.name, oldRef, 0, 1)
	require.ErrorIs(t, err, os.ErrNotExist, "retired pair should be gone")
	require.Equal(t, expected, readBlocks(t, reopened, 0, 1))
}

// ambiguousChunkWrite lets the client's failed recovery return before
// the backend finishes its PUT, as can happen after a transport timeout.
type ambiguousChunkWrite struct {
	types.Backend

	late func() error
	ref  types.ChunkRef
}

func (b *ambiguousChunkWrite) WriteChunkCtx(ctx context.Context, ref types.ChunkRef, headers, data *[]byte) error {
	h := append([]byte(nil), (*headers)...)
	d := append([]byte(nil), (*data)...)
	b.ref = ref
	b.late = func() error {
		return b.Backend.WriteChunkCtx(context.WithoutCancel(ctx), ref, &h, &d)
	}
	return context.DeadlineExceeded
}

func TestQualifiedChunkSameLaunchAllocatesAfterFailedRecovery(t *testing.T) {
	ctx := context.Background()
	mv := newMapVolume(t, volumeSize, nil)
	seed, _ := mv.create(t, 5)
	acked := randomBlockData(1)
	require.NoError(t, seed.Write(0, acked))
	require.NoError(t, seed.Flush())
	seed.Detach()

	cursor := &atomic.Uint64{}
	first, _ := mv.engine(t, 6)
	first.QualifiedChunks = true
	first.LaunchChunkIDs = cursor
	require.NoError(t, first.LoadState())
	require.NoError(t, first.LoadLiveCheckpoint())
	require.NoError(t, first.ClaimMapCtx(ctx))
	backend := &ambiguousChunkWrite{Backend: first.Backend}
	first.Backend = backend
	require.ErrorIs(t, first.RecoverLocalWALs(), context.DeadlineExceeded)
	require.NotNil(t, backend.late)
	acknowledgedWAL := filepath.Join(seed.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, seed.WAL.WallNum.Load(), mv.name))
	require.FileExists(t, acknowledgedWAL)
	first.Detach()

	second, _ := mv.engine(t, 6)
	second.LaunchChunkIDs = cursor
	second.ResumeEpoch = true
	second.ResumeHeadN, second.ResumeObjN = first.MapCounters()
	require.NoError(t, second.LoadState())
	require.NoError(t, second.LoadLiveCheckpoint())
	require.NoError(t, second.ClaimMapCtx(ctx))
	require.NoError(t, second.RecoverLocalWALs())
	require.Equal(t, acked, readBlocks(t, second, 0, 1))
	require.NoFileExists(t, acknowledgedWAL)
	openMapWALs(t, second)
	expected := randomBlockData(1)
	require.NoError(t, second.Write(0, expected))
	require.NoError(t, second.Flush())
	publishedWAL := second.WAL.DB[0].Name()
	require.FileExists(t, publishedWAL)
	require.NoError(t, second.DrainToBackendCtx(ctx))
	require.NoFileExists(t, publishedWAL)
	current, _, _, err := second.lookupChunk(0)
	require.NoError(t, err)
	require.Equal(t, backend.ref.Epoch, current.Epoch)
	require.Greater(t, current.ID, backend.ref.ID)
	require.NoError(t, backend.late())
	second.Detach()

	cold, _ := mv.engine(t, 7)
	require.NoError(t, cold.LoadState())
	require.NoError(t, cold.LoadLiveCheckpoint())
	require.Equal(t, expected, readBlocks(t, cold, 0, 1))
}

func TestQualifiedActivationRefusesMapWithPendingLegacyOverwrite(t *testing.T) {
	ctx := t.Context()
	mv := newMapVolume(t, volumeSize, nil)
	dormant, _ := mv.create(t, 5)
	acknowledged := randomBlockData(1)
	writeDrain(t, dormant, 0, acknowledged)
	ref, _, _, err := dormant.lookupChunk(0)
	require.NoError(t, err)
	require.Zero(t, ref.Epoch)
	dormant.Detach()

	activator, _ := mv.engine(t, 6)
	activator.QualifiedChunks = true
	require.NoError(t, activator.LoadState())
	require.NoError(t, activator.LoadLiveCheckpoint())

	before := mv.mapKeys(t, mapHeadPrefix)
	if err := activator.ClaimMapCtx(ctx); err != nil {
		require.Equal(t, before, mv.mapKeys(t, mapHeadPrefix), "activation must publish no head")
		require.Equal(t, acknowledged, readBlocks(t, activator, 0, 1))
		return
	}

	// Complete a legacy PUT that was submitted against this numeric key before
	// the activation claim. The current head still names that old key.
	headers := dormant.ChunkHeader()
	late := randomBlockData(1)
	require.NoError(t, dormant.Backend.WriteCtx(ctx, types.FileTypeChunk, ref.ID, &headers, &late))
	cold, _ := mv.engine(t, 7)
	require.NoError(t, cold.LoadState())
	require.NoError(t, cold.LoadLiveCheckpoint())
	require.True(t, bytes.Equal(acknowledged, readBlocks(t, cold, 0, 1)),
		"an activated map exposed a delayed legacy PUT at its retained numeric reference")
}
