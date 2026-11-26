package viperblock

import (
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

func TestRetainedMixedQualifiedSnapshotCopyAndDescendant(t *testing.T) {
	ctx := context.Background()
	mv := newMapVolume(t, volumeSize, nil)
	seed, _ := mv.create(t, 5)
	legacyBytes := randomBlockData(1)
	writeDrain(t, seed, 0, legacyBytes)
	seed.Detach()

	source, _ := mv.engine(t, 6)
	require.NoError(t, source.LoadState())
	require.NoError(t, source.LoadLiveCheckpoint())
	// Reproduce a mixed head written by the preceding candidate.
	source.mapChain.qualified.Store(true)
	source.LaunchChunkIDs = &atomic.Uint64{}
	require.NoError(t, source.ClaimMapCtx(ctx))
	require.NoError(t, source.RecoverLocalWALs())
	openMapWALs(t, source)
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

func TestQualifiedActivationKeepsPopulatedMapDormant(t *testing.T) {
	ctx := t.Context()
	mv := newMapVolume(t, volumeSize, nil)
	dormant, _ := mv.create(t, 5)
	acknowledged := randomBlockData(1)
	writeDrain(t, dormant, 0, acknowledged)
	dormant.Detach()

	activator, _ := mv.engine(t, 6)
	activator.QualifiedChunks = true
	require.NoError(t, activator.LoadState())
	require.NoError(t, activator.LoadLiveCheckpoint())

	require.NoError(t, activator.ClaimMapCtx(ctx))
	cold, _ := mv.engine(t, 7)
	require.NoError(t, cold.LoadState())
	require.NoError(t, cold.LoadLiveCheckpoint())
	require.Equal(t, acknowledged, readBlocks(t, cold, 0, 1))
	require.False(t, cold.mapChain.qualified.Load(), "a populated map must remain dormant")
}

func TestQualifiedActivationIsolatesEmptyMapFromLegacyPut(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	dormant, _ := mv.create(t, 5)
	dormant.Detach()

	writer := claimQualified(t, mv, 6)
	expected := randomBlockData(1)
	writeDrain(t, writer, 0, expected)
	ref, _, _, err := writer.lookupChunk(0)
	require.NoError(t, err)
	require.EqualValues(t, 6, ref.Epoch)
	writer.Detach()

	headers := dormant.ChunkHeader()
	late := randomBlockData(1)
	require.NoError(t, dormant.Backend.WriteCtx(t.Context(), types.FileTypeChunk, ref.ID, &headers, &late))
	cold, _ := mv.engine(t, 7)
	require.NoError(t, cold.LoadState())
	require.NoError(t, cold.LoadLiveCheckpoint())
	require.Equal(t, expected, readBlocks(t, cold, 0, 1))
}

func TestQualifiedActivationKeepsEmptyCloneDormant(t *testing.T) {
	for _, populated := range []bool{false, true} {
		name := "empty parent"
		if populated {
			name = "populated parent"
		}
		t.Run(name, func(t *testing.T) {
			mv := newMapVolume(t, volumeSize, nil)
			parent, _ := mv.create(t, 5)
			expected := make([]byte, parent.BlockSize)
			if populated {
				expected = randomBlockData(1)
				writeDrain(t, parent, 0, expected)
			}
			_, err := parent.CreateSnapshot("parent")
			require.NoError(t, err)

			cloneVolume := *mv
			cloneVolume.name = "vol-clone"
			seed, _ := cloneVolume.create(t, 5)
			require.NoError(t, seed.OpenFromSnapshot("parent"))
			require.NoError(t, seed.SaveState())
			seed.Detach()

			clone, _ := cloneVolume.engine(t, 6)
			clone.QualifiedChunks = true
			require.NoError(t, clone.LoadState())
			require.NoError(t, clone.LoadLiveCheckpoint())
			require.NoError(t, clone.ClaimMapCtx(t.Context()))

			cold, _ := cloneVolume.engine(t, 7)
			require.NoError(t, cold.LoadState())
			require.NoError(t, cold.LoadLiveCheckpoint())
			require.Equal(t, "parent", cold.SnapshotID)
			require.False(t, cold.mapChain.qualified.Load())
			data := make([]byte, cold.BlockSize)
			err = cold.ReadAtInto(0, data)
			if populated {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrZeroBlock)
			}
			require.Equal(t, expected, data)
		})
	}
}
