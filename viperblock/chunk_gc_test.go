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

// This file exercises chunk GC exclusively against the file backend, per
// the design doc's own testing scope ("file backend, no cluster"). That is
// sufficient to validate the refcount/watermark/ancestry-guard LOGIC in
// isolation, but it says nothing about whether a DeleteObject against
// predastore actually reclaims physical bytes -- predastore's own
// tombstone/compaction behavior is a separate, live-verified question.
// Nothing in this file should be read as evidence about predastore.

// newGCTestVB creates a fresh, file-backend-only VB rooted at root, with the
// periodic GC/WAL-sync tickers disabled (GCInterval: -1, WALSyncInterval:
// -1) so every test drives sweeps explicitly via runGCSweep/sweepChunks
// instead of racing a background goroutine. root is shared by both the VB
// tree ("{root}/viperblock") and the backend tree ("{root}/{volumeName}"),
// matching setupTestVB's layout, so ancestry tests can point multiple VBs
// (source + clones) at the same root and have them resolve each other's
// chunks.
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
// brand-new VB struct (empty gcRefcount, gcLatchedOff false, gcReconciled
// false) that loads persisted state from the backend rather than a New()
// VB's zero state. Mirrors the LoadState -> LoadLiveCheckpoint -> OpenWAL
// ordering RecoverLocalWALs's doc comment describes for boot.
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
	// Must run between LoadLiveCheckpoint and OpenWAL (see RecoverLocalWALs's
	// doc comment): a "crashed" instance in these tests never called Close,
	// so it may have left an empty, freshly rotated local WAL segment at the
	// same WallNum this reopen is about to (re-)create. OpenWAL always
	// appends rather than truncates, so opening straight over a leftover
	// header-only segment would double-header it; RecoverLocalWALs replays
	// (here, zero) records and removes the file first.
	require.NoError(t, vb.RecoverLocalWALs())

	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	return vb
}

// cloneGCTestVB opens a COW clone of snapshotID sharing source's file-backend
// root, the same shape as snapshot_test.go's createCloneVB, but with an
// explicit clone volume name and GC enablement so ancestry tests can chain
// clones of clones (createCloneVB's fixed "clone-{source}-{snapshotID}"
// naming would collide across chain levels) and control GC per level.
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
// referenced anywhere in vb's own block map, its frozen BaseBlockMap (COW
// parent), and every flattened ancestor layer resolves to a readable chunk
// file under the volume name that layer's map is keyed against. A CLOSURE
// failure means a dangling reference -- data loss -- and this must hold
// after every sweep in every test in this file, regardless of what else the
// test is checking.
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
// unreferenced but is never swept before the process exits (simulating a
// crash before a graceful Close) must still be reclaimable by the next
// process's first sweep. This is the scenario that requires
// reconcileChunksOnce: parseBlockCheckpoint's rebuild sees only the loaded
// live map, and a zero-reference chunk is by definition absent from it, so
// without a chunks/-prefix reconcile the new process's gcRefcount would
// never even learn firstChunkID exists.
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

	// Persist the live checkpoint (now referencing only the overwrite chunk)
	// and config.json, but deliberately never call Close() or
	// SaveBlockState() -- simulates a crash before the graceful shutdown
	// sweep, and before any numbered checkpoint is ever written for this
	// volume (SaveBlockState is the only thing that writes one). No
	// numbered checkpoint means ensureGCFloor finds nothing to protect
	// (floor 0), so this restart is not entangled with the separate
	// numbered-checkpoint-floor behavior exercised elsewhere.
	require.NoError(t, vb.DrainToBackendCtx(ctx))
	require.NoError(t, vb.SaveState())

	// Simulate reattaching on a different host (or losing local disk): wipe
	// the local WAL tree so reopenGCTestVB's RecoverLocalWALs has nothing to
	// replay. Without this, both consolidated WAL segments from this test's
	// two writeAndChunk calls are still sitting on local disk (viperblock
	// only ever deletes a consolidated WAL file via RecoverLocalWALs replay
	// or a full Close) and get replayed into a brand-new chunk on reopen --
	// which would reclaim firstChunkID on its own, via WAL replay, and this
	// test would pass whether or not reconcileChunksOnce works at all. This
	// isolates the test to what reconcileChunksOnce is actually for: a
	// chunk that only the backend-durable checkpoint plus a chunks/-prefix
	// listing can account for.
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

// Test 5: Snapshot pins superseded chunks. The sweep runs on a freshly
// reopened VB instance (not the one that called CreateSnapshot) so the
// assertion exercises scanForOwnSnapshots's bucket-wide ancestry scan, not
// just the simpler CreateSnapshot-sets-gcLatchedOff shortcut a same-instance
// test would trivially satisfy without proving the scan works at all.
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

	// Same window one level down: overwrite the blocks A's own snapshot
	// depends on. chunkA1 becomes unreferenced in A's live map but snap2
	// (and cloneB, cloned from it below) still needs it. Sweep must be a
	// no-op -- CreateSnapshot above latched A off too.
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

// Test 9: Chunk minted after mark is not swept. sweepChunks has no reachable
// seam to inject a real concurrent write mid-sweep without adding test-only
// instrumentation to production code, so this test manufactures the exact
// boundary condition instead: a gcRefcount entry for an ObjectID at or above
// the current ObjectNum counter, standing in for "a chunk minted after the
// mark." If the watermark clamp is missing or wrong, sweepChunks will
// attempt to delete it (and, since no such chunk file exists, silently
// forget it was ever tracked -- see the DeleteCtx/os.ErrNotExist handling in
// sweepChunks). If the clamp holds, the entry is left untouched.
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
// upload and the checkpoint save that would have made it (and its
// supersession of the prior chunk) durable: the new chunk lands on the
// backend via WriteWALToChunk, but SaveLiveCheckpointCtx never runs. On
// recovery, the durable checkpoint is unchanged (still points at the old
// chunk), so the new, never-checkpointed chunk is legitimately orphaned --
// correctly reclaimable, not a dangling reference -- while the old chunk
// the durable checkpoint still names is never touched.
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

	// "Crash": the chunk upload completes and durably lands...
	uncommitted := randomBlockData(1)
	writeAndChunk(t, vb, 0, uncommitted)
	require.NoError(t, vb.SaveState()) // metadata (ObjectNum/WALNum) advances...
	// ...but the checkpoint that would supersede survivingChunkID never
	// gets saved: no DrainToBackendCtx/SaveLiveCheckpoint call follows.

	// Wipe local disk so recovery can only see backend-durable state --
	// otherwise RecoverLocalWALs would find the still-on-disk, already-
	// consolidated WAL segments (viperblock never deletes a segment except
	// via WAL replay or a full Close) and replay the "uncommitted" write
	// right back in, which is a different, legitimate recovery path (real
	// same-host crash recovery) but would defeat this test's specific
	// point: an orphaned, never-checkpointed chunk is safe to reclaim.
	require.NoError(t, vb.RemoveLocalFiles())

	recovered := reopenGCTestVB(t, root, volumeName, true)
	recovered.runGCSweep(ctx)
	assertClosure(t, recovered)

	// The durable checkpoint's own reference must never be touched.
	assertChunkPresent(t, recovered.Backend, volumeName, survivingChunkID)

	// Recovery reflects the last durable checkpoint, not the crashed write:
	// local WAL replay was deliberately taken out of the picture above, so
	// only chunk GC's own safety is being exercised here.
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
// yields a BOUNDED chunk object count, not one that grows with N. Every
// pass fully supersedes the previous pass's single chunk (randomBlockData(4) is
// far under ObjBlockSize, so writeAndChunk mints exactly one chunk per
// call -- see its doc comment), so an unbounded/leaking implementation would
// show live chunk count == pass count; a correct one holds flat at 1 after
// each sweep. This is the plan doc's pre-registered prediction
// ("slope_per_footprint drops from ~4.603 to ~0") reproduced as a unit
// assertion instead of a live sweep.
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
		// Every pass fully supersedes the prior pass's one chunk, so after
		// its sweep exactly one chunk (this pass's own) must remain live --
		// never a growing tail of superseded ones.
		assert.Equalf(t, 1, count, "pass %d: expected exactly 1 live chunk after sweep, got %d (counts so far: %v)", i, count, counts)
	}

	// Belt-and-braces on the acceptance claim itself: the final count must
	// not scale with passes. A naive no-op GC would show counts[passes-1] ==
	// passes (10); bounded GC holds it at 1 regardless of N.
	require.Less(t, counts[len(counts)-1], passes,
		"chunk object count grew with pass count (got %v) -- GC did not bound backend growth", counts)

	// Data correctness survives the whole sustained-overwrite + GC cycle.
	final, err := vb.ReadAt(0, 4*uint64(DefaultBlockSize))
	require.NoError(t, err)
	require.Len(t, final, 4*int(DefaultBlockSize))
}

// Test 13: negative/safety belt-and-braces -- a chunk still referenced by
// ANY live block-map entry is never a GC candidate, even sitting alongside
// many wholly-unreferenced siblings in the same sweep. Loudly asserts the
// refcounted chunk is untouched across a sweep that reclaims everything
// else.
func TestChunkGC_LiveReferencedChunkNeverDeletedAmongGarbage(t *testing.T) {
	root := t.TempDir()
	vb := newGCTestVB(t, root, "vol-live-safety", true)
	ctx := context.Background()

	// Block range [0,4) stays live for the whole test -- never overwritten.
	pinned := randomBlockData(4)
	writeAndChunk(t, vb, 0, pinned)
	pinnedChunkID := vb.ObjectNum.Load() - 1

	// Block range [4,8) gets rewritten repeatedly, minting and orphaning a
	// chunk each time -- garbage the sweep should reclaim, interleaved with
	// the pinned chunk's ID range so a floor/watermark-only bug (rather than
	// true per-chunk refcounting) would be exposed.
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

// Test 14 (over-collection blocker): a stale drain that installs a block-map
// entry backwards must never clobber a newer chunk's pointer, and must never
// drive that live chunk's refcount to zero. Two concurrent drains rotate to
// different sequential WAL segments and run createChunkFile in parallel; if
// the OLDER segment's map write lands last, an unconditional overwrite would
// repoint the block at the stale chunk and (via gcTrackBlock) drop the newer,
// still-referenced chunk to refcount zero -- which the next sweep would then
// physically delete. That is the exact silent-data-loss catastrophe this
// feature must never cause.
//
// This drives the boundary deterministically by calling createChunkFile
// directly with the NEWER write (higher SeqNum) first and the STALE write
// (lower SeqNum) for the SAME block second, standing in for the newer of two
// racing drains winning the map-write ordering. WITHOUT the monotonic guard
// in createChunkFile the final assertions fail loudly: the live chunk is
// deleted and the read returns stale data. WITH it, the live chunk survives,
// the orphaned stale chunk is reclaimed, and the read is correct.
func TestChunkGC_StaleDrainNeverClobbersNewerChunk(t *testing.T) {
	root := t.TempDir()
	vb := newGCTestVB(t, root, "vol-stale-clobber", true)
	ctx := context.Background()
	bs := int(vb.BlockSize)

	// This test drives createChunkFile directly to force the exact
	// out-of-order boundary, bypassing WriteAtCtx -- which is also the only
	// path that populates the BlockStore. Route reads through the
	// BlocksToObject map (where the monotonic guard under test lives) so the
	// read reflects the map pointer, not an unpopulated BlockStore.
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

	// The read must return the NEWER data -- proof the live chunk was neither
	// repointed away nor deleted.
	got, err := vb.ReadAt(0, uint64(bs))
	require.NoError(t, err)
	assert.Equal(t, newerData, got, "read returned stale data -- the live chunk pointer was corrupted")
}

// Test 15 (concurrency, race-mode): drive real awaitBackpressure-triggered
// drains from several concurrent writers WHILE a sweeper goroutine hammers
// runGCSweep (which itself drains). This is the multi-trigger drain
// concurrency drainMu must serialize. The invariant proven: after the storm
// settles, closure holds (no live chunk was ever deleted -- a deleted live
// chunk would make a referenced ObjectID unreadable) and every block reads
// back its last-written value (proof each block's pointer resolves to its
// highest-SeqNum chunk, not a stale or reclaimed one). Run under -race to
// catch data races on BlockLookup / gcRefcount as well.
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
	// unambiguously; supersession within its own repeated rewrites still
	// churns chunks, and the drains still overlap across writers + sweeper.
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
