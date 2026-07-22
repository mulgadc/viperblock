package viperblock

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises chunk GC against the file backend only, validating
// the refcount/watermark/ancestry-guard logic in isolation. It says nothing
// about whether a DeleteObject against predastore reclaims physical bytes.

// newGCTestVB creates a fresh, file-backend-only VB rooted at root, with the
// periodic GC/WAL-sync tickers disabled so every test drives sweeps
// explicitly via runGCSweep/sweepChunks instead of racing a background
// goroutine. root is shared by the VB tree and the backend tree so ancestry
// tests can point multiple VBs (source + clones) at the same root.
func newGCTestVB(t *testing.T, root, volumeName string, gcEnabled bool) *VB {
	t.Helper()

	backendConfig := file.FileConfig{
		VolumeName: volumeName,
		VolumeSize: volumeSize,
		BaseDir:    root,
	}

	vbconfig := VB{
		VolumeName:      volumeName,
		VolumeSize:      volumeSize,
		BaseDir:         filepath.Join(root, "viperblock"),
		WALSyncInterval: -1,
		GCEnabled:       gcEnabled,
		GCInterval:      -1,
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}

	vb, err := New(&vbconfig, FileBackend, backendConfig)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, vb.RemoveLocalFiles())
	})

	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	return vb
}

// reopenGCTestVB simulates a fresh process opening an existing volume: a
// brand-new VB struct that loads persisted state from the backend rather
// than a New() VB's zero state. Mirrors the LoadState -> LoadLiveCheckpoint
// -> OpenWAL boot ordering.
func reopenGCTestVB(t *testing.T, root, volumeName string, gcEnabled bool) *VB {
	t.Helper()

	backendConfig := file.FileConfig{
		VolumeName: volumeName,
		VolumeSize: volumeSize,
		BaseDir:    root,
	}

	vbconfig := VB{
		VolumeName:      volumeName,
		VolumeSize:      volumeSize,
		BaseDir:         filepath.Join(root, "viperblock"),
		WALSyncInterval: -1,
		GCEnabled:       gcEnabled,
		GCInterval:      -1,
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}

	vb, err := New(&vbconfig, FileBackend, backendConfig)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, vb.RemoveLocalFiles())
	})

	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.LoadState())
	require.NoError(t, vb.LoadLiveCheckpoint())
	// Must run between LoadLiveCheckpoint and OpenWAL: a "crashed" instance
	// may have left an empty, freshly rotated local WAL segment at the same
	// WallNum this reopen is about to (re-)create, and OpenWAL appends
	// rather than truncates, so RecoverLocalWALs must clear it first.
	require.NoError(t, vb.RecoverLocalWALs())

	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	return vb
}

// cloneGCTestVB opens a COW clone of snapshotID sharing source's
// file-backend root, like snapshot_test.go's createCloneVB but with an
// explicit clone volume name and GC enablement, so ancestry tests can chain
// clones of clones and control GC per level.
func cloneGCTestVB(t *testing.T, source *VB, snapshotID, cloneVolumeName string, gcEnabled bool) *VB {
	t.Helper()

	cloneBackendConfig := file.FileConfig{
		VolumeName: cloneVolumeName,
		VolumeSize: source.VolumeSize,
		BaseDir:    filepath.Dir(source.BaseDir),
	}

	vbconfig := VB{
		VolumeName:      cloneVolumeName,
		VolumeSize:      source.VolumeSize,
		BaseDir:         source.BaseDir,
		WALSyncInterval: -1,
		GCEnabled:       gcEnabled,
		GCInterval:      -1,
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}

	clone, err := New(&vbconfig, FileBackend, cloneBackendConfig)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, clone.RemoveLocalFiles())
	})

	require.NoError(t, clone.Backend.Init())
	require.NoError(t, clone.OpenWAL(&clone.WAL, fmt.Sprintf("%s/%s", clone.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, clone.WAL.WallNum.Load(), clone.GetVolume()))))
	require.NoError(t, clone.OpenWAL(&clone.BlockToObjectWAL, fmt.Sprintf("%s/%s", clone.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, clone.BlockToObjectWAL.WallNum.Load(), clone.GetVolume()))))
	require.NoError(t, clone.OpenFromSnapshot(snapshotID))

	return clone
}

// writeAndChunk writes data starting at startBlock, flushes it to the WAL,
// and forces exactly one chunk consolidation. Callers keep data well under
// ObjBlockSize (4MB) so each call mints exactly one new chunk, making chunk
// IDs predictable via vb.ObjectNum.Load()-1 immediately afterward.
func writeAndChunk(t *testing.T, vb *VB, startBlock uint64, data []byte) {
	t.Helper()
	require.NoError(t, vb.Write(startBlock, data))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.WriteWALToChunk(true))
}

// randomBlockData returns n blocks worth of random data.
func randomBlockData(n uint64) []byte {
	data := make([]byte, n*uint64(DefaultBlockSize))
	if _, err := rand.Read(data); err != nil {
		panic(err)
	}
	return data
}

// assertClosure asserts the CLOSURE invariant for vb: every ObjectID
// referenced anywhere in vb's own block map, its frozen BaseBlockMap, and
// every ancestor layer resolves to a readable chunk. A failure means a
// dangling reference -- data loss.
func assertClosure(t *testing.T, vb *VB) {
	t.Helper()

	vb.BlocksToObject.mu.RLock()
	ownLookup := make(map[uint64]BlockLookup, len(vb.BlocksToObject.BlockLookup))
	maps.Copy(ownLookup, vb.BlocksToObject.BlockLookup)
	vb.BlocksToObject.mu.RUnlock()
	assertMapClosure(t, vb.Backend, vb.VolumeName, ownLookup)

	if vb.BaseBlockMap != nil {
		vb.BaseBlockMap.mu.RLock()
		baseLookup := make(map[uint64]BlockLookup, len(vb.BaseBlockMap.BlockLookup))
		maps.Copy(baseLookup, vb.BaseBlockMap.BlockLookup)
		vb.BaseBlockMap.mu.RUnlock()
		assertMapClosure(t, vb.Backend, vb.SourceVolumeName, baseLookup)
	}

	for _, anc := range vb.ancestors {
		if anc.blocks == nil {
			continue
		}
		anc.blocks.mu.RLock()
		ancLookup := make(map[uint64]BlockLookup, len(anc.blocks.BlockLookup))
		maps.Copy(ancLookup, anc.blocks.BlockLookup)
		anc.blocks.mu.RUnlock()
		assertMapClosure(t, vb.Backend, anc.sourceVolumeName, ancLookup)
	}
}

// assertMapClosure asserts every ObjectID named in blockLookup resolves to a
// readable chunk under volumeName's own prefix on backend.
func assertMapClosure(t *testing.T, backend types.Backend, volumeName string, blockLookup map[uint64]BlockLookup) {
	t.Helper()

	seen := make(map[uint64]bool)
	for _, lookup := range blockLookup {
		if seen[lookup.ObjectID] {
			continue
		}
		seen[lookup.ObjectID] = true

		_, err := backend.ReadFrom(volumeName, types.FileTypeChunk, lookup.ObjectID, 0, 4)
		assert.NoErrorf(t, err, "CLOSURE violated: %s chunk %d is referenced by a live map but unreadable: %v", volumeName, lookup.ObjectID, err)
	}
}

// assertChunkGone asserts a chunk object no longer exists.
func assertChunkGone(t *testing.T, backend types.Backend, volumeName string, objectID uint64) {
	t.Helper()
	_, err := backend.ReadFrom(volumeName, types.FileTypeChunk, objectID, 0, 4)
	require.Error(t, err, "expected chunk %s/%d to have been reclaimed", volumeName, objectID)
	assert.True(t, os.IsNotExist(err), "expected ErrNotExist for reclaimed chunk %s/%d, got %v", volumeName, objectID, err)
}

// assertChunkPresent asserts a chunk object still exists.
func assertChunkPresent(t *testing.T, backend types.Backend, volumeName string, objectID uint64) {
	t.Helper()
	_, err := backend.ReadFrom(volumeName, types.FileTypeChunk, objectID, 0, 4)
	assert.NoErrorf(t, err, "expected chunk %s/%d to still be present, got %v", volumeName, objectID, err)
}

// Test 1: Wholly-unreferenced chunk is reclaimed.
func TestChunkGC_WhollyUnreferencedChunkReclaimed(t *testing.T) {
	root := t.TempDir()
	vb := newGCTestVB(t, root, "vol-reclaim", true)
	ctx := context.Background()

	original := randomBlockData(4)
	writeAndChunk(t, vb, 0, original)
	firstChunkID := vb.ObjectNum.Load() - 1

	overwrite := randomBlockData(4)
	writeAndChunk(t, vb, 0, overwrite)

	vb.runGCSweep(ctx)
	assertClosure(t, vb)

	assertChunkGone(t, vb.Backend, vb.VolumeName, firstChunkID)

	readBack, err := vb.ReadAt(0, uint64(len(overwrite)))
	require.NoError(t, err)
	assert.Equal(t, overwrite, readBack)
}

// Test 2: Partially-referenced chunk survives. Guards against refcounting
// per chunk instead of per block.
func TestChunkGC_PartiallyReferencedChunkSurvives(t *testing.T) {
	root := t.TempDir()
	vb := newGCTestVB(t, root, "vol-partial", true)
	ctx := context.Background()

	original := randomBlockData(4)
	writeAndChunk(t, vb, 0, original)
	chunkAID := vb.ObjectNum.Load() - 1

	overwriteBlock := randomBlockData(1)
	writeAndChunk(t, vb, 1, overwriteBlock) // overwrite only block 1 of the 4

	vb.runGCSweep(ctx)
	assertClosure(t, vb)

	assertChunkPresent(t, vb.Backend, vb.VolumeName, chunkAID)

	readBack, err := vb.ReadAt(0, uint64(len(original)))
	require.NoError(t, err)
	expected := append([]byte{}, original...)
	copy(expected[DefaultBlockSize:2*DefaultBlockSize], overwriteBlock)
	assert.Equal(t, expected, readBack)
}

// Test 3: Refcount rebuild across restart. A chunk that becomes wholly
// unreferenced but is never swept before the process exits must still be
// reclaimable by the next process's first sweep, via reconcileChunksOnce --
// parseBlockCheckpoint's rebuild only sees the loaded live map, which by
// definition omits a zero-reference chunk.
func TestChunkGC_RefcountRebuildAcrossRestart(t *testing.T) {
	root := t.TempDir()
	volumeName := "vol-restart"
	ctx := context.Background()

	vb := newGCTestVB(t, root, volumeName, true)

	original := randomBlockData(4)
	writeAndChunk(t, vb, 0, original)
	firstChunkID := vb.ObjectNum.Load() - 1

	overwrite := randomBlockData(4)
	writeAndChunk(t, vb, 0, overwrite)

	// Persist the live checkpoint but never call Close() or SaveBlockState(),
	// simulating a crash before graceful shutdown and before any numbered
	// checkpoint exists for this volume.
	require.NoError(t, vb.DrainToBackendCtx(ctx))
	require.NoError(t, vb.SaveState())

	// Wipe local WAL state so RecoverLocalWALs on reopen has nothing to
	// replay -- otherwise WAL replay alone would reclaim firstChunkID and
	// the test would pass regardless of whether reconcileChunksOnce works.
	require.NoError(t, vb.RemoveLocalFiles())

	reopened := reopenGCTestVB(t, root, volumeName, true)
	reopened.runGCSweep(ctx)
	assertClosure(t, reopened)

	assertChunkGone(t, reopened.Backend, volumeName, firstChunkID)

	readBack, err := reopened.ReadAt(0, uint64(len(overwrite)))
	require.NoError(t, err)
	assert.Equal(t, overwrite, readBack)
}

// Test 4: Reads survive a full GC cycle. Interleaves writes/overwrites
// across a small region with sweeps, asserting every live block matches a
// shadow buffer and CLOSURE holds after every sweep.
func TestChunkGC_ReadsSurviveFullGCCycle(t *testing.T) {
	root := t.TempDir()
	vb := newGCTestVB(t, root, "vol-property", true)
	ctx := context.Background()

	const regionBlocks = 8
	shadow := make([]byte, regionBlocks*uint64(DefaultBlockSize))
	written := false

	for i := range 24 {
		startBlock := uint64(i%regionBlocks) % (regionBlocks - 2)
		lenBlocks := uint64(i%3) + 1
		data := randomBlockData(lenBlocks)

		writeAndChunk(t, vb, startBlock, data)
		copy(shadow[startBlock*uint64(DefaultBlockSize):], data)
		written = true

		if i%4 == 3 {
			vb.runGCSweep(ctx)
			assertClosure(t, vb)
		}
	}
	require.True(t, written)

	vb.runGCSweep(ctx)
	assertClosure(t, vb)

	readBack, err := vb.ReadAt(0, uint64(len(shadow)))
	require.NoError(t, err)
	assert.Equal(t, shadow, readBack)
}

// Test 5: Snapshot pins superseded chunks. Runs on a freshly reopened VB
// (not the one that called CreateSnapshot) so this exercises
// scanForOwnSnapshots's ancestry scan, not just the same-instance
// gcLatchedOff shortcut.
func TestChunkGC_SnapshotPinsSupersededChunks(t *testing.T) {
	root := t.TempDir()
	volumeName := "vol-snap-pin"
	ctx := context.Background()

	vb := newGCTestVB(t, root, volumeName, true)

	original := randomBlockData(4)
	writeAndChunk(t, vb, 0, original)
	frozenChunkID := vb.ObjectNum.Load() - 1

	snapshotID := "snap-" + volumeName
	_, err := vb.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	overwrite := randomBlockData(4)
	writeAndChunk(t, vb, 0, overwrite)

	require.NoError(t, vb.DrainToBackendCtx(ctx))
	require.NoError(t, vb.Close())

	reopened := reopenGCTestVB(t, root, volumeName, true)
	reopened.runGCSweep(ctx)
	assertClosure(t, reopened)

	// The frozen chunk must survive: it's still what the snapshot references.
	assertChunkPresent(t, reopened.Backend, volumeName, frozenChunkID)

	clone := createCloneVB(t, reopened, snapshotID)
	for i := range uint64(4) {
		readData, err := clone.ReadAt(i*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err)
		expected := original[i*uint64(DefaultBlockSize) : (i+1)*uint64(DefaultBlockSize)]
		assert.Equal(t, expected, readData, "block %d mismatch reading through snapshot", i)
	}

	readBack, err := reopened.ReadAt(0, uint64(len(overwrite)))
	require.NoError(t, err)
	assert.Equal(t, overwrite, readBack)
}

// Test 6: Clone never GCs its parent. Own-prefix-only Delete plus the
// ancestry guard must combine so a clone's sweep cannot touch any key under
// its parent's prefix.
func TestChunkGC_CloneNeverGCsParent(t *testing.T) {
	root := t.TempDir()
	parentVolume := "vol-parent-6"
	ctx := context.Background()

	parent := newGCTestVB(t, root, parentVolume, false)
	baseData := randomBlockData(4)
	writeAndChunk(t, parent, 0, baseData)
	parentChunkID := parent.ObjectNum.Load() - 1

	snapshotID := "snap-" + parentVolume
	_, err := parent.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	parentKeysBefore, err := parent.Backend.ListObjects(parentVolume + "/chunks/")
	require.NoError(t, err)

	clone := cloneGCTestVB(t, parent, snapshotID, "vol-clone-6", true)

	cloneData := randomBlockData(2)
	writeAndChunk(t, clone, 6, cloneData) // block range clone-only, not in base
	cloneOverwrite := randomBlockData(2)
	writeAndChunk(t, clone, 6, cloneOverwrite)

	clone.runGCSweep(ctx)
	assertClosure(t, clone)

	parentKeysAfter, err := parent.Backend.ListObjects(parentVolume + "/chunks/")
	require.NoError(t, err)
	assert.ElementsMatch(t, parentKeysBefore, parentKeysAfter, "clone's sweep must not touch any key under the parent's prefix")

	assertChunkPresent(t, parent.Backend, parentVolume, parentChunkID)

	// Clone still reads its base blocks correctly.
	for i := range uint64(4) {
		readData, err := clone.ReadAt(i*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err)
		expected := baseData[i*uint64(DefaultBlockSize) : (i+1)*uint64(DefaultBlockSize)]
		assert.Equal(t, expected, readData, "base block %d mismatch", i)
	}

	// And its own overwritten data.
	readBack, err := clone.ReadAt(6*uint64(clone.BlockSize), uint64(len(cloneOverwrite)))
	require.NoError(t, err)
	assert.Equal(t, cloneOverwrite, readBack)
}

// Test 7: Sibling clone isolation. Sweeping one clone of a snapshot must
// never affect a sibling clone of the same snapshot.
func TestChunkGC_SiblingCloneIsolation(t *testing.T) {
	root := t.TempDir()
	parentVolume := "vol-parent-7"
	ctx := context.Background()

	parent := newGCTestVB(t, root, parentVolume, false)
	baseData := randomBlockData(4)
	writeAndChunk(t, parent, 0, baseData)

	snapshotID := "snap-" + parentVolume
	_, err := parent.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	cloneA := cloneGCTestVB(t, parent, snapshotID, "vol-clone-7a", true)
	cloneB := cloneGCTestVB(t, parent, snapshotID, "vol-clone-7b", true)

	// A supersedes one of its own chunks.
	dataA1 := randomBlockData(2)
	writeAndChunk(t, cloneA, 6, dataA1)
	chunkA0 := cloneA.ObjectNum.Load() - 1
	dataA2 := randomBlockData(2)
	writeAndChunk(t, cloneA, 6, dataA2)

	cloneA.runGCSweep(ctx)
	assertClosure(t, cloneA)
	assertChunkGone(t, cloneA.Backend, "vol-clone-7a", chunkA0)

	// B, untouched by A's sweep, still reads its base blocks correctly.
	for i := range uint64(4) {
		readData, err := cloneB.ReadAt(i*uint64(cloneB.BlockSize), uint64(cloneB.BlockSize))
		require.NoError(t, err)
		expected := baseData[i*uint64(DefaultBlockSize) : (i+1)*uint64(DefaultBlockSize)]
		assert.Equal(t, expected, readData, "sibling clone base block %d mismatch after unrelated sweep", i)
	}
	assertClosure(t, cloneB)
}

// Test 8: Multi-level chain (clone of a clone). Every level must still read
// correctly after each level runs its own GC. Uses the same-instance
// CreateSnapshot latch (rather than a reopen per level, already covered in
// depth by Test 5) to keep the chain manageable.
func TestChunkGC_MultiLevelChain(t *testing.T) {
	root := t.TempDir()
	parentVolume := "vol-parent-8"
	ctx := context.Background()

	blockSize := uint64(DefaultBlockSize)

	parent := newGCTestVB(t, root, parentVolume, true)
	baseData := randomBlockData(4)
	writeAndChunk(t, parent, 0, baseData) // blocks 0-3
	parentChunkID := parent.ObjectNum.Load() - 1

	snap1 := "snap-" + parentVolume
	_, err := parent.CreateSnapshot(snap1) // latches parent off
	require.NoError(t, err)

	// Overwrite the same blocks parent's own snapshot depends on. This makes
	// parentChunkID unreferenced in parent's own live map, but snap1 (and any
	// clone made from it) still needs it -- exactly the window
	// ensureGCSnapshotSafe exists to guard. A sweep here must be a no-op:
	// gcLatchedOff was set by CreateSnapshot above, so GC never runs.
	parentOverwrite := randomBlockData(4)
	writeAndChunk(t, parent, 0, parentOverwrite) // overwrite blocks 0-3
	parent.runGCSweep(ctx)
	assertClosure(t, parent)
	assertChunkPresent(t, parent.Backend, parentVolume, parentChunkID)

	cloneA := cloneGCTestVB(t, parent, snap1, "vol-clone-8a", true)

	dataA0 := randomBlockData(4)
	writeAndChunk(t, cloneA, 4, dataA0) // blocks 4-7, chunk a0
	chunkA0 := cloneA.ObjectNum.Load() - 1
	dataA1 := randomBlockData(4)
	writeAndChunk(t, cloneA, 4, dataA1) // overwrite -> chunk a1, a0 now garbage

	cloneA.runGCSweep(ctx) // no snapshot of A yet -- a0 is reclaimable
	assertClosure(t, cloneA)
	assertChunkGone(t, cloneA.Backend, "vol-clone-8a", chunkA0)

	snap2 := "snap-vol-clone-8a"
	_, err = cloneA.CreateSnapshot(snap2) // latches A off from here on
	require.NoError(t, err)
	chunkA1 := cloneA.ObjectNum.Load() - 1

	// Same window one level down: overwriting these blocks makes chunkA1
	// unreferenced in A's live map, but snap2 (and cloneB below) still
	// needs it. Sweep must be a no-op -- CreateSnapshot latched A off too.
	dataA2 := randomBlockData(4)
	writeAndChunk(t, cloneA, 4, dataA2) // overwrite blocks 4-7 again
	cloneA.runGCSweep(ctx)
	assertClosure(t, cloneA)
	assertChunkPresent(t, cloneA.Backend, "vol-clone-8a", chunkA1)

	cloneB := cloneGCTestVB(t, cloneA, snap2, "vol-clone-8b", true)

	dataB0 := randomBlockData(4)
	writeAndChunk(t, cloneB, 8, dataB0) // blocks 8-11, chunk b0
	chunkB0 := cloneB.ObjectNum.Load() - 1
	dataB1 := randomBlockData(4)
	writeAndChunk(t, cloneB, 8, dataB1) // overwrite -> chunk b1, b0 now garbage

	cloneB.runGCSweep(ctx) // no snapshot of B -- b0 is reclaimable
	assertClosure(t, cloneB)
	assertChunkGone(t, cloneB.Backend, "vol-clone-8b", chunkB0)

	// Every level still reads correctly through the full 2-hop chain.
	for i := range uint64(4) {
		readData, err := cloneB.ReadAt(i*blockSize, blockSize)
		require.NoError(t, err)
		assert.Equal(t, baseData[i*blockSize:(i+1)*blockSize], readData, "grandparent block %d mismatch", i)
	}
	for i := range uint64(4) {
		readData, err := cloneB.ReadAt((4+i)*blockSize, blockSize)
		require.NoError(t, err)
		assert.Equal(t, dataA1[i*blockSize:(i+1)*blockSize], readData, "parent block %d mismatch", i+4)
	}
	readData, err := cloneB.ReadAt(8*blockSize, uint64(len(dataB1)))
	require.NoError(t, err)
	assert.Equal(t, dataB1, readData, "own block mismatch")

	// Chunks still needed by a live ancestor chain must never be reclaimed.
	assertChunkPresent(t, parent.Backend, parentVolume, parentChunkID)
	assertChunkPresent(t, cloneA.Backend, "vol-clone-8a", chunkA1)

	assertClosure(t, cloneB)
}

// Test 9: Chunk minted after mark is not swept. There's no seam to inject a
// real concurrent write mid-sweep, so this manufactures the boundary
// directly: a gcRefcount entry for an ObjectID at or above the current
// ObjectNum, standing in for "minted after the mark." A missing/wrong
// watermark clamp would attempt to delete it.
func TestChunkGC_WatermarkExcludesChunksMintedAfterMark(t *testing.T) {
	root := t.TempDir()
	vb := newGCTestVB(t, root, "vol-watermark", true)
	ctx := context.Background()

	original := randomBlockData(1)
	writeAndChunk(t, vb, 0, original)
	require.NoError(t, vb.DrainToBackendCtx(ctx))

	futureID := vb.ObjectNum.Load() + 5 // never minted; stands in for "minted after mark"
	vb.BlocksToObject.mu.Lock()
	vb.gcRefcount[futureID] = 0
	vb.BlocksToObject.mu.Unlock()

	vb.sweepChunks(ctx)
	assertClosure(t, vb)

	vb.BlocksToObject.mu.Lock()
	_, stillTracked := vb.gcRefcount[futureID]
	vb.BlocksToObject.mu.Unlock()
	assert.True(t, stillTracked, "watermark clamp must leave a not-yet-minted id untouched, not attempt to delete or drop-track it")
}

// failingCheckpointBackend wraps a real file backend and can be told to fail
// every live-checkpoint write, to fault-inject the "no delete before
// checkpoint durability" ordering guarantee (Test 10).
type failingCheckpointBackend struct {
	*file.Backend

	failLiveCheckpoint atomic.Bool
}

var _ types.Backend = (*failingCheckpointBackend)(nil)

func (b *failingCheckpointBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	if fileType == types.FileTypeBlockCheckpointLive && b.failLiveCheckpoint.Load() {
		return errors.New("injected: live checkpoint write failure")
	}
	return b.Backend.WriteCtx(ctx, fileType, objectId, headers, data)
}

// Test 10: No delete before checkpoint durability. Fault-injects a
// SaveLiveCheckpointCtx failure and asserts zero deletes were issued.
func TestChunkGC_NoDeleteBeforeCheckpointDurability(t *testing.T) {
	root := t.TempDir()
	vb := newGCTestVB(t, root, "vol-fault", true)
	ctx := context.Background()

	fb, ok := vb.Backend.(*file.Backend)
	require.True(t, ok)
	wrapped := &failingCheckpointBackend{Backend: fb}
	vb.Backend = wrapped

	original := randomBlockData(4)
	writeAndChunk(t, vb, 0, original)
	garbageChunkID := vb.ObjectNum.Load() - 1
	overwrite := randomBlockData(4)
	writeAndChunk(t, vb, 0, overwrite)

	wrapped.failLiveCheckpoint.Store(true)
	vb.runGCSweep(ctx) // DrainToBackendCtx must fail before sweepChunks runs

	assertChunkPresent(t, vb.Backend, vb.VolumeName, garbageChunkID)
	vb.BlocksToObject.mu.Lock()
	refs, tracked := vb.gcRefcount[garbageChunkID]
	vb.BlocksToObject.mu.Unlock()
	require.True(t, tracked)
	assert.Zero(t, refs)

	// Clearing the fault lets the same chunk be reclaimed normally,
	// confirming the gate only blocks deletes, not GC's ability to recover.
	wrapped.failLiveCheckpoint.Store(false)
	vb.runGCSweep(ctx)
	assertClosure(t, vb)
	assertChunkGone(t, vb.Backend, vb.VolumeName, garbageChunkID)
}

// Test 11: Crash-consistency. Simulates a crash between a chunk's durable
// upload and the checkpoint save that would have made it durable: the new
// chunk lands via WriteWALToChunk, but SaveLiveCheckpointCtx never runs. On
// recovery the durable checkpoint still points at the old chunk, so the new,
// never-checkpointed chunk is legitimately orphaned and reclaimable.
func TestChunkGC_CrashConsistency(t *testing.T) {
	root := t.TempDir()
	volumeName := "vol-crash"
	ctx := context.Background()

	vb := newGCTestVB(t, root, volumeName, true)

	original := randomBlockData(1)
	writeAndChunk(t, vb, 0, original)
	survivingChunkID := vb.ObjectNum.Load() - 1

	require.NoError(t, vb.DrainToBackendCtx(ctx)) // durable checkpoint: block0 -> survivingChunkID
	require.NoError(t, vb.SaveState())

	// "Crash": a new chunk lands durably, but the checkpoint that would
	// supersede survivingChunkID never gets saved.
	uncommitted := randomBlockData(1)
	writeAndChunk(t, vb, 0, uncommitted)
	require.NoError(t, vb.SaveState())

	// Wipe local disk so recovery only sees backend-durable state --
	// otherwise WAL replay would restore the "uncommitted" write, which is a
	// different (legitimate) recovery path than the one under test here.
	require.NoError(t, vb.RemoveLocalFiles())

	recovered := reopenGCTestVB(t, root, volumeName, true)
	recovered.runGCSweep(ctx)
	assertClosure(t, recovered)

	assertChunkPresent(t, recovered.Backend, volumeName, survivingChunkID)

	readBack, err := recovered.ReadAt(0, uint64(len(original)))
	require.NoError(t, err)
	assert.Equal(t, original, readBack)
}

// countChunkObjects returns the number of chunk objects currently on the
// backend under volumeName's own chunks/ prefix -- the acceptance-level
// signal for "did GC actually bound backend growth", independent of
// gcRefcount's in-memory bookkeeping.
func countChunkObjects(t *testing.T, backend types.Backend, volumeName string) int {
	t.Helper()
	keys, err := backend.ListObjects(volumeName + "/chunks/")
	require.NoError(t, err)
	return len(keys)
}

// Test 12 (acceptance headline): rewriting the same logical blocks N times
// yields a BOUNDED chunk object count, not one that grows with N. Every pass
// fully supersedes the previous pass's single chunk, so an unbounded/leaking
// implementation would show live chunk count == pass count; a correct one
// holds flat at 1 after each sweep.
func TestChunkGC_BoundedGrowthUnderSustainedOverwrite(t *testing.T) {
	root := t.TempDir()
	vb := newGCTestVB(t, root, "vol-bounded", true)
	ctx := context.Background()

	const passes = 10
	counts := make([]int, 0, passes)

	for i := range passes {
		writeAndChunk(t, vb, 0, randomBlockData(4))
		vb.runGCSweep(ctx)
		assertClosure(t, vb)

		count := countChunkObjects(t, vb.Backend, vb.VolumeName)
		counts = append(counts, count)
		assert.Equalf(t, 1, count, "pass %d: expected exactly 1 live chunk after sweep, got %d (counts so far: %v)", i, count, counts)
	}

	// The final count must not scale with passes: a naive no-op GC would
	// show counts[passes-1] == passes.
	require.Less(t, counts[len(counts)-1], passes,
		"chunk object count grew with pass count (got %v) -- GC did not bound backend growth", counts)

	// Data correctness survives the whole sustained-overwrite + GC cycle.
	final, err := vb.ReadAt(0, 4*uint64(DefaultBlockSize))
	require.NoError(t, err)
	require.Len(t, final, 4*int(DefaultBlockSize))
}

// Test 13: a chunk still referenced by ANY live block-map entry is never a
// GC candidate, even alongside many wholly-unreferenced siblings in the
// same sweep.
func TestChunkGC_LiveReferencedChunkNeverDeletedAmongGarbage(t *testing.T) {
	root := t.TempDir()
	vb := newGCTestVB(t, root, "vol-live-safety", true)
	ctx := context.Background()

	// Block range [0,4) stays live for the whole test -- never overwritten.
	pinned := randomBlockData(4)
	writeAndChunk(t, vb, 0, pinned)
	pinnedChunkID := vb.ObjectNum.Load() - 1

	// Block range [4,8) gets rewritten repeatedly, minting and orphaning a
	// chunk each time, interleaved with the pinned chunk's ID range so a
	// floor/watermark-only bug (not true per-chunk refcounting) would show.
	var garbageChunkIDs []uint64
	for range 5 {
		writeAndChunk(t, vb, 4, randomBlockData(4))
		garbageChunkIDs = append(garbageChunkIDs, vb.ObjectNum.Load()-1)
	}

	vb.runGCSweep(ctx)
	assertClosure(t, vb)

	// The pinned chunk must survive every sweep, loudly.
	assertChunkPresent(t, vb.Backend, vb.VolumeName, pinnedChunkID)

	// Every garbage chunk except the last (still live) must be gone.
	for _, id := range garbageChunkIDs[:len(garbageChunkIDs)-1] {
		assertChunkGone(t, vb.Backend, vb.VolumeName, id)
	}
	assertChunkPresent(t, vb.Backend, vb.VolumeName, garbageChunkIDs[len(garbageChunkIDs)-1])

	// Both live ranges still read back correctly.
	readPinned, err := vb.ReadAt(0, uint64(len(pinned)))
	require.NoError(t, err)
	assert.Equal(t, pinned, readPinned)
}

// Test 14 (over-collection blocker): a stale drain that installs a
// block-map entry backwards must never clobber a newer chunk's pointer or
// drive its refcount to zero. Drives the boundary deterministically by
// calling createChunkFile directly with the NEWER write (higher SeqNum)
// first and the STALE write (lower SeqNum) for the SAME block second,
// standing in for the newer of two racing drains winning the map-write
// ordering.
func TestChunkGC_StaleDrainNeverClobbersNewerChunk(t *testing.T) {
	root := t.TempDir()
	vb := newGCTestVB(t, root, "vol-stale-clobber", true)
	ctx := context.Background()
	bs := int(vb.BlockSize)

	// Bypass WriteAtCtx (the only path that populates BlockStore) so reads
	// go through the BlocksToObject map, where the monotonic guard lives.
	vb.UseBlockStore = false

	// Newer write (higher SeqNum) lands FIRST.
	newerData := make([]byte, bs)
	_, err := rand.Read(newerData)
	require.NoError(t, err)
	newerBuf := newerData
	newerMatched := []Block{{Block: 0, SeqNum: 20, Len: uint64(bs)}}
	require.NoError(t, vb.createChunkFile(ctx, 0, &newerBuf, &newerMatched))
	newerChunkID := vb.ObjectNum.Load() - 1

	// Stale write (lower SeqNum) for the SAME block lands SECOND.
	staleData := make([]byte, bs)
	_, err = rand.Read(staleData)
	require.NoError(t, err)
	staleBuf := staleData
	staleMatched := []Block{{Block: 0, SeqNum: 10, Len: uint64(bs)}}
	require.NoError(t, vb.createChunkFile(ctx, 0, &staleBuf, &staleMatched))
	staleChunkID := vb.ObjectNum.Load() - 1
	require.NotEqual(t, newerChunkID, staleChunkID)

	// The block must still point at the NEWER chunk, and refcounts must be
	// exactly: live chunk 1, orphaned stale chunk tracked at 0.
	vb.BlocksToObject.mu.RLock()
	lookup, mapOK := vb.BlocksToObject.BlockLookup[0]
	newerRefs := vb.gcRefcount[newerChunkID]
	staleRefs, staleTracked := vb.gcRefcount[staleChunkID]
	vb.BlocksToObject.mu.RUnlock()

	require.True(t, mapOK, "block 0 lost its map entry entirely")
	assert.Equalf(t, newerChunkID, lookup.ObjectID, "stale drain clobbered the live block pointer (map points at %d, want newer chunk %d)", lookup.ObjectID, newerChunkID)
	assert.Equal(t, uint64(20), lookup.SeqNum, "block pointer must carry the newer SeqNum")
	assert.Equalf(t, uint64(1), newerRefs, "live chunk %d refcount must stay 1; a stale drain must never decrement it", newerChunkID)
	assert.True(t, staleTracked, "orphaned stale chunk should be tracked for prompt reclaim, not leaked")
	assert.Zero(t, staleRefs, "orphaned stale chunk must be at refcount 0")

	// A sweep reclaims the stale/orphan chunk and NEVER the live one.
	vb.sweepChunks(ctx)
	assertClosure(t, vb)
	assertChunkPresent(t, vb.Backend, vb.VolumeName, newerChunkID)
	assertChunkGone(t, vb.Backend, vb.VolumeName, staleChunkID)

	// The read must return the NEWER data.
	got, err := vb.ReadAt(0, uint64(bs))
	require.NoError(t, err)
	assert.Equal(t, newerData, got, "read returned stale data -- the live chunk pointer was corrupted")
}

// Test 15 (concurrency, race-mode): drives real backpressure-triggered
// drains from several concurrent writers while a sweeper goroutine hammers
// runGCSweep, exercising the multi-trigger drain concurrency drainMu must
// serialize. After the storm settles, closure must hold and every block
// must read back its last-written value. Run under -race.
func TestChunkGC_ConcurrentDrainAndSweepPreserveLiveChunks(t *testing.T) {
	root := t.TempDir()
	vb := newGCTestVB(t, root, "vol-concurrent-drain", true)
	// Force backpressure so each concurrent WriteAtCtx drives its own
	// synchronous DrainToBackendCtx, overlapping the sweeper's drains.
	vb.MaxPendingBytes = 16 * uint64(vb.BlockSize)

	ctx := context.Background()
	bs := uint64(vb.BlockSize)

	const writers = 4
	const blocksPerWriter = 4
	const rounds = 30

	// Each writer owns an exclusive block range so its final value is known
	// unambiguously, while its repeated rewrites still churn chunks.
	lastWritten := make([][]byte, writers)
	writeErrs := make([]error, writers)

	var writersWG sync.WaitGroup
	var sweeperWG sync.WaitGroup
	stop := make(chan struct{})

	sweeperWG.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				vb.runGCSweep(ctx)
				time.Sleep(time.Millisecond)
			}
		}
	})

	for w := range writers {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			base := uint64(w*blocksPerWriter) * bs
			var last []byte
			for range rounds {
				data := make([]byte, blocksPerWriter*int(bs))
				if _, err := rand.Read(data); err != nil {
					writeErrs[w] = err
					return
				}
				if err := vb.WriteAtCtx(ctx, base, data); err != nil {
					writeErrs[w] = err
					return
				}
				last = data
			}
			lastWritten[w] = last
		}(w)
	}

	writersWG.Wait()
	close(stop)
	sweeperWG.Wait()

	for w, err := range writeErrs {
		require.NoErrorf(t, err, "writer %d failed", w)
	}

	// Final quiescent drain + sweep, then verify nothing live was lost.
	require.NoError(t, vb.DrainToBackendCtx(ctx))
	vb.sweepChunks(ctx)
	assertClosure(t, vb)

	for w := range writers {
		base := uint64(w*blocksPerWriter) * bs
		got, err := vb.ReadAt(base, uint64(blocksPerWriter)*bs)
		require.NoErrorf(t, err, "reading writer %d range", w)
		assert.Equalf(t, lastWritten[w], got, "writer %d: block range did not read back its last write -- a live chunk was superseded incorrectly or deleted", w)
	}
}

// openSnapshotOnlyVB models the VB spinifex's daemon builds to snapshot a
// volume another process is serving: same backend prefix, its own local WAL
// directory, and no WAL opened, so ownsWAL() is false and CreateSnapshot
// freezes the map it loaded rather than draining one it doesn't own.
func openSnapshotOnlyVB(t *testing.T, backendRoot, localRoot, volumeName string) *VB {
	t.Helper()

	backendConfig := file.FileConfig{
		VolumeName: volumeName,
		VolumeSize: volumeSize,
		BaseDir:    backendRoot,
	}

	vbconfig := VB{
		VolumeName:      volumeName,
		VolumeSize:      volumeSize,
		BaseDir:         filepath.Join(localRoot, "viperblock"),
		WALSyncInterval: -1,
		GCInterval:      -1,
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}

	vb, err := New(&vbconfig, FileBackend, backendConfig)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, vb.RemoveLocalFiles())
	})

	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.LoadState())
	require.NoError(t, vb.LoadLiveCheckpoint())

	return vb
}

// Test 16: a snapshot taken by another process, after the sweeping engine has
// already cached its ancestry answer, must still stop that engine sweeping.
// This is the case gcLatchedOff (in-process) and the cached scan both miss,
// and it is spinifex's primary snapshot path: nbdkit serves and sweeps the
// attached volume while the daemon snapshots it for CreateImage.
func TestChunkGC_CrossProcessSnapshotLatchesSweeperOff(t *testing.T) {
	backendRoot := t.TempDir()
	volumeName := "vol-xproc-snap"
	ctx := context.Background()

	server := newGCTestVB(t, backendRoot, volumeName, true)

	original := randomBlockData(4)
	writeAndChunk(t, server, 0, original)
	frozenChunkID := server.ObjectNum.Load() - 1

	// Sweep once with no snapshot in existence: this is what caches the
	// "safe" ancestry answer and the marker baseline for the VB's lifetime.
	server.runGCSweep(ctx)
	require.False(t, server.gcLatchedOff.Load(), "a volume with no snapshots must not latch off")
	assertChunkPresent(t, server.Backend, volumeName, frozenChunkID)

	// Another process snapshots the same volume. The serving engine above is
	// never told. Persist state first, since the second VB opens the volume
	// off the backend the way the daemon does.
	require.NoError(t, server.SaveState())

	snapshotID := "snap-" + volumeName
	snapper := openSnapshotOnlyVB(t, backendRoot, t.TempDir(), volumeName)
	_, err := snapper.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	// The cached ancestry answer is now stale and still says "safe" -- it was
	// computed before the snapshot existed and is never recomputed. Asserted
	// explicitly so it is clear the marker check below is the only thing
	// standing between this sweep and deleting the snapshot's chunks.
	require.True(t, server.ensureGCSnapshotSafe(ctx), "precondition: the cached ancestry answer must still be the pre-snapshot one")

	// The serving engine keeps writing, superseding every chunk the snapshot
	// froze, then sweeps on that stale answer.
	overwrite := randomBlockData(4)
	writeAndChunk(t, server, 0, overwrite)
	server.runGCSweep(ctx)
	assertClosure(t, server)

	assert.True(t, server.gcLatchedOff.Load(), "sweeper must latch off once the marker moves")
	assertChunkPresent(t, server.Backend, volumeName, frozenChunkID)

	// The snapshot restores byte-exact: the whole point of the pin.
	clone := cloneGCTestVB(t, server, snapshotID, "vol-xproc-clone", false)
	for i := range uint64(4) {
		readData, err := clone.ReadAt(i*uint64(clone.BlockSize), uint64(clone.BlockSize))
		require.NoError(t, err)
		expected := original[i*uint64(DefaultBlockSize) : (i+1)*uint64(DefaultBlockSize)]
		assert.Equal(t, expected, readData, "block %d mismatch reading through the cross-process snapshot", i)
	}

	// And the serving volume still reads its own latest data.
	readBack, err := server.ReadAt(0, uint64(len(overwrite)))
	require.NoError(t, err)
	assert.Equal(t, overwrite, readBack)
}

// failingMarkerBackend wraps a real file backend and can be told to fail the
// snapshot marker's write or read independently, for Tests 17 and 18.
type failingMarkerBackend struct {
	*file.Backend

	failMarkerWrite atomic.Bool
	failMarkerRead  atomic.Bool
}

var _ types.Backend = (*failingMarkerBackend)(nil)

func (b *failingMarkerBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	if fileType == types.FileTypeSnapshotMarker && b.failMarkerWrite.Load() {
		return errors.New("injected: snapshot marker write failure")
	}
	return b.Backend.WriteCtx(ctx, fileType, objectId, headers, data)
}

func (b *failingMarkerBackend) ReadCtx(ctx context.Context, fileType types.FileType, objectId uint64, offset uint32, length uint32) ([]byte, error) {
	if fileType == types.FileTypeSnapshotMarker && b.failMarkerRead.Load() {
		return nil, errors.New("injected: snapshot marker read failure")
	}
	return b.Backend.ReadCtx(ctx, fileType, objectId, offset, length)
}

// Test 17: a snapshot whose marker doesn't land must fail. Creating it anyway
// would leave a snapshot no sweeping engine can observe, whose chunks can be
// reclaimed underneath it.
func TestChunkGC_SnapshotMarkerWriteFailureFailsSnapshot(t *testing.T) {
	root := t.TempDir()
	volumeName := "vol-marker-write-fault"
	vb := newGCTestVB(t, root, volumeName, true)

	fb, ok := vb.Backend.(*file.Backend)
	require.True(t, ok)
	wrapped := &failingMarkerBackend{Backend: fb}
	vb.Backend = wrapped

	writeAndChunk(t, vb, 0, randomBlockData(4))

	wrapped.failMarkerWrite.Store(true)
	snapshotID := "snap-" + volumeName
	_, err := vb.CreateSnapshot(snapshotID)
	require.Error(t, err, "CreateSnapshot must fail when the snapshot marker cannot be published")

	// Nothing a reader would accept as a snapshot may be left behind.
	_, cfgErr := vb.Backend.ReadFrom(snapshotID, types.FileTypeConfig, 0, 0, 0)
	require.Error(t, cfgErr)
	assert.True(t, os.IsNotExist(cfgErr), "expected no snapshot config after a failed marker write, got %v", cfgErr)
}

// Test 18: a marker the sweeper cannot read is not evidence of "no snapshot".
// The sweep must skip and retry, not delete.
func TestChunkGC_SnapshotMarkerReadFailureSkipsSweep(t *testing.T) {
	root := t.TempDir()
	volumeName := "vol-marker-read-fault"
	ctx := context.Background()

	vb := newGCTestVB(t, root, volumeName, true)

	fb, ok := vb.Backend.(*file.Backend)
	require.True(t, ok)
	wrapped := &failingMarkerBackend{Backend: fb}
	vb.Backend = wrapped

	writeAndChunk(t, vb, 0, randomBlockData(4))
	garbageChunkID := vb.ObjectNum.Load() - 1
	writeAndChunk(t, vb, 0, randomBlockData(4))

	wrapped.failMarkerRead.Store(true)
	vb.runGCSweep(ctx)
	assertClosure(t, vb)
	assertChunkPresent(t, vb.Backend, volumeName, garbageChunkID)
	assert.False(t, vb.gcLatchedOff.Load(), "a transient marker read failure must not latch GC off permanently")

	// Clearing the fault lets the same chunk be reclaimed, proving the failure
	// only deferred the sweep.
	wrapped.failMarkerRead.Store(false)
	vb.runGCSweep(ctx)
	assertClosure(t, vb)
	assertChunkGone(t, vb.Backend, volumeName, garbageChunkID)
}

// Test 19: a marker that never moves must not latch GC off. Guards the
// opposite failure to Test 16 -- a marker check that fails closed on every
// sweep would silently disable reclaim for every volume.
func TestChunkGC_UnchangedMarkerStillSweeps(t *testing.T) {
	root := t.TempDir()
	volumeName := "vol-marker-steady"
	ctx := context.Background()

	vb := newGCTestVB(t, root, volumeName, true)

	for pass := range 3 {
		writeAndChunk(t, vb, 0, randomBlockData(4))
		garbageChunkID := vb.ObjectNum.Load() - 1
		writeAndChunk(t, vb, 0, randomBlockData(4))

		vb.runGCSweep(ctx)
		assertClosure(t, vb)
		assertChunkGone(t, vb.Backend, volumeName, garbageChunkID)
		assert.Falsef(t, vb.gcLatchedOff.Load(), "pass %d: an unmoved marker must not latch GC off", pass)
	}
}
