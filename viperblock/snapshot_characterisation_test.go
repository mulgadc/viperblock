package viperblock

// Characterisation tests for snapshot/clone correctness: CreateSnapshot,
// the base-block clone read path, snapshotAncestor, and
// buildFlatSection/parseFlatSection, exercised through their real code
// paths rather than inspection.
//
// One test (TestCreateSnapshot_NonOwningPath_MissesWritesDurableAfterLoad)
// documents a BUG rather than proving correct behaviour; it is marked
// explicitly. Everything else here passes.

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- (a)/(b): does CreateSnapshot flush before freezing on both sides of
// ownsWAL(), and is SnapshotState.ObjectNum the correct high-water? ---

// TestCreateSnapshot_OwningPath_FlushesPendingWritesBeforeFreeze pins that on
// the owning path, CreateSnapshot drains pending writes before freezing the
// block map, so an unflushed write is still visible through the snapshot.
func TestCreateSnapshot_OwningPath_FlushesPendingWritesBeforeFreeze(t *testing.T) {
	runWithBackends(t, "owning_path_flush_before_freeze", func(t *testing.T, vb *VB) {
		data := make([]byte, DefaultBlockSize*2)
		rand.Read(data)
		// Deliberately do not call Flush()/WriteWALToChunk() ourselves --
		// CreateSnapshot must do it.
		require.NoError(t, vb.Write(0, data))

		snapshotID := fmt.Sprintf("snap-%s", vb.VolumeName)
		snap, err := vb.CreateSnapshot(snapshotID)
		require.NoError(t, err)
		require.NotNil(t, snap)

		clone := createCloneVB(t, vb, snapshotID)
		readData, err := clone.ReadAt(0, uint64(len(data)))
		require.NoError(t, err, "CreateSnapshot must flush pending writes before freezing the map on the owning path")
		assert.Equal(t, data, readData)
	})
}

// TestCreateSnapshot_NonOwningPath_MissesWritesDurableAfterLoad documents a
// BUG: on the non-owning path (a VB that never opened its own WAL, e.g.
// spinifex's snapshotRunningVolume flow), CreateSnapshot skips its
// flush/drain step and freezes whatever map it already loaded, so writes the
// real owner drains afterward are silently missing.
//
// EXPECTED-CORRECT: a snapshot should observe every write durable at the
// moment CreateSnapshot was invoked, regardless of which VB instance
// performed it.
func TestCreateSnapshot_NonOwningPath_MissesWritesDurableAfterLoad(t *testing.T) {
	backend := BackendTest{
		Name:          "file_nonowning",
		BackendType:   FileBackend,
		CacheConfig:   CacheConfig{Size: 0},
		UseShardedWAL: true,
	}
	owner, _, shutdown, err := setupTestVB(t, TestVB{name: "nonowning_stale"}, backend)
	require.NoError(t, err)
	defer shutdown(owner.GetVolume())

	// Write and durably drain block 0 -- this is what a "load" in the
	// snapshotRunningVolume flow observes.
	blockAData := make([]byte, DefaultBlockSize)
	rand.Read(blockAData)
	require.NoError(t, owner.Write(0, blockAData))
	require.NoError(t, owner.Flush())
	require.NoError(t, owner.WriteShardedWALToChunk(true))
	require.NoError(t, owner.SaveLiveCheckpoint())

	// Construct a non-owning reader VB the way spinifex's
	// snapshotRunningVolume does: shares the backend, never opens WAL files,
	// loads the checkpoint once.
	reader := &VB{
		VolumeName:   owner.VolumeName,
		VolumeSize:   owner.VolumeSize,
		BlockSize:    owner.BlockSize,
		ObjBlockSize: owner.ObjBlockSize,
		Version:      owner.Version,
		BaseDir:      owner.BaseDir,
		Backend:      owner.Backend,
		BlockToObjectWAL: WAL{
			WALMagic: owner.BlockToObjectWAL.WALMagic,
		},
		BlocksToObject: BlocksToObject{
			BlockLookup: make(map[uint64]BlockLookup),
		},
	}
	require.NoError(t, reader.LoadLiveCheckpoint())
	require.False(t, reader.ownsWAL(), "reader must not own the WAL -- this is the path under test")

	objectNumAtLoad := reader.ObjectNum.Load()

	// The real owning process keeps serving writes: block 1 is drained to a
	// new chunk between the reader's load and the snapshot call below.
	blockBData := make([]byte, DefaultBlockSize)
	rand.Read(blockBData)
	require.NoError(t, owner.Write(1, blockBData))
	require.NoError(t, owner.Flush())
	require.NoError(t, owner.WriteShardedWALToChunk(true))
	require.NoError(t, owner.SaveLiveCheckpoint())
	require.Greater(t, owner.ObjectNum.Load(), objectNumAtLoad,
		"the owning process must have minted a new durable chunk for block 1")

	snapshotID := "snap-nonowning-stale"
	snap, err := reader.CreateSnapshot(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, snap)

	// BUG: ObjectNum is stale-low, reflecting the reader's load-time
	// counter rather than the true high-water at snapshot time.
	assert.Less(t, snap.ObjectNum, owner.ObjectNum.Load(),
		"BUG: non-owning CreateSnapshot's ObjectNum lags the true high-water at snapshot time")

	clone := createCloneVB(t, owner, snapshotID)

	// Block 0 (durable before the reader's load) is correctly visible.
	got, err := clone.ReadAt(0, uint64(DefaultBlockSize))
	require.NoError(t, err)
	assert.Equal(t, blockAData, got, "pre-load durable write must be visible")

	// BUG: block 1 was durable before CreateSnapshot ran, but the reader's
	// frozen map never saw it, so the clone reads it as never-written.
	got, err = clone.ReadAt(uint64(DefaultBlockSize), uint64(DefaultBlockSize))
	assert.ErrorIs(t, err, ErrZeroBlock,
		"BUG: a write durable before CreateSnapshot was called is silently missing from the snapshot on the non-owning path")
	assert.Equal(t, make([]byte, DefaultBlockSize), got)
}

// TestCreateSnapshot_OwningPath_ObjectNumAtLeastReferencedChunks pins that
// SnapshotState.ObjectNum is a safe high-water: every chunk the frozen map
// references has ObjectID strictly less than ObjectNum.
func TestCreateSnapshot_OwningPath_ObjectNumAtLeastReferencedChunks(t *testing.T) {
	runWithBackends(t, "owning_objectnum_highwater", func(t *testing.T, vb *VB) {
		blocksPerChunk := uint64(vb.ObjBlockSize / vb.BlockSize)
		data := make([]byte, blocksPerChunk*uint64(vb.BlockSize)*2)
		rand.Read(data)
		require.NoError(t, vb.Write(0, data))
		require.NoError(t, vb.Flush())
		require.NoError(t, vb.WriteWALToChunk(true))

		snapshotID := fmt.Sprintf("snap-%s", vb.VolumeName)
		snap, err := vb.CreateSnapshot(snapshotID)
		require.NoError(t, err)

		baseMap, _, err := vb.LoadSnapshotBlockMap(snapshotID)
		require.NoError(t, err)
		require.NotEmpty(t, baseMap.BlockLookup)

		var maxReferenced uint64
		for _, bl := range baseMap.BlockLookup {
			if bl.ObjectID > maxReferenced {
				maxReferenced = bl.ObjectID
			}
		}
		assert.GreaterOrEqual(t, snap.ObjectNum, maxReferenced+1,
			"SnapshotState.ObjectNum must exceed every chunk ObjectID the frozen map references")
	})
}

// TestCreateSnapshot_PinsSupersededChunk pins that after a snapshot is
// taken, if the source volume overwrites the same blocks (making the
// original chunk unreferenced by the source's own live map), the snapshot
// and any clone descended from it must still resolve that chunk.
func TestCreateSnapshot_PinsSupersededChunk(t *testing.T) {
	runWithBackends(t, "snapshot_pins_superseded", func(t *testing.T, vb *VB) {
		original := make([]byte, DefaultBlockSize*4)
		rand.Read(original)
		require.NoError(t, vb.Write(0, original))
		require.NoError(t, vb.Flush())
		require.NoError(t, vb.WriteWALToChunk(true))

		snapshotID := fmt.Sprintf("snap-%s", vb.VolumeName)
		_, err := vb.CreateSnapshot(snapshotID)
		require.NoError(t, err)

		clone := createCloneVB(t, vb, snapshotID)

		// The source volume now supersedes every block the snapshot froze.
		overwrite := make([]byte, DefaultBlockSize*4)
		rand.Read(overwrite)
		require.NoError(t, vb.Write(0, overwrite))
		require.NoError(t, vb.Flush())
		require.NoError(t, vb.WriteWALToChunk(true))

		// vb's own live read returns the new data...
		got, err := vb.ReadAt(0, uint64(len(overwrite)))
		require.NoError(t, err)
		assert.Equal(t, overwrite, got)

		// ...but the snapshot's clone must still resolve the ORIGINAL chunk.
		got, err = clone.ReadAt(0, uint64(len(original)))
		require.NoError(t, err)
		assert.Equal(t, original, got, "clone must still resolve the chunk vb's own live map has since superseded")
	})
}

// TestClone_WritesNeverTouchParentPrefix pins that every backend key a clone
// creates lands under the clone's own volume prefix, never the parent's.
// File backend only, so the on-disk key set can be enumerated directly.
func TestClone_WritesNeverTouchParentPrefix(t *testing.T) {
	backend := BackendTest{
		Name:          "file_prefix_isolation",
		BackendType:   FileBackend,
		CacheConfig:   CacheConfig{Size: 0},
		UseShardedWAL: true,
	}
	parent, _, shutdown, err := setupTestVB(t, TestVB{name: "prefix_isolation_parent"}, backend)
	require.NoError(t, err)
	defer shutdown(parent.GetVolume())

	data := make([]byte, DefaultBlockSize*2)
	rand.Read(data)
	require.NoError(t, parent.Write(0, data))
	require.NoError(t, parent.Flush())
	require.NoError(t, parent.WriteShardedWALToChunk(true))

	snapshotID := "snap-prefix-isolation"
	_, err = parent.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	storageRoot := filepath.Dir(parent.BaseDir)
	parentKeysBefore := listChunkKeys(t, storageRoot, parent.VolumeName)
	require.NotEmpty(t, parentKeysBefore)

	clone := createCloneVB(t, parent, snapshotID)

	// Write a full chunk on the clone, sized to stay within the test
	// volume's 8 MiB bound alongside the block-10 starting offset.
	blocksPerChunk := uint64(clone.ObjBlockSize / clone.BlockSize)
	cloneData := make([]byte, blocksPerChunk*uint64(clone.BlockSize))
	rand.Read(cloneData)
	require.NoError(t, clone.Write(10, cloneData))
	require.NoError(t, clone.Flush())
	// createCloneVB always opens a clone on the legacy (non-sharded) WAL,
	// regardless of the parent's mode.
	require.NoError(t, clone.WriteWALToChunk(true))

	cloneKeys := listChunkKeys(t, storageRoot, clone.VolumeName)
	assert.NotEmpty(t, cloneKeys, "clone must have minted its own chunks")

	parentKeysAfter := listChunkKeys(t, storageRoot, parent.VolumeName)
	assert.ElementsMatch(t, parentKeysBefore, parentKeysAfter,
		"a clone's writes must never create/modify/delete keys under the parent volume's own prefix")

	// Confirm isolation didn't break correctness.
	got, err := clone.ReadAt(10*uint64(clone.BlockSize), uint64(len(cloneData)))
	require.NoError(t, err)
	assert.Equal(t, cloneData, got)

	got, err = clone.ReadAt(0, uint64(len(data)))
	require.NoError(t, err)
	assert.Equal(t, data, got, "clone must still read inherited parent blocks after its own writes")
}

// TestSiblingClonesAreIsolated pins that two clones of the same snapshot
// don't observe each other's writes or share a chunk key namespace, and a
// third fresh clone still sees the original, unperturbed data.
func TestSiblingClonesAreIsolated(t *testing.T) {
	backend := BackendTest{
		Name:          "file_sibling_isolation",
		BackendType:   FileBackend,
		CacheConfig:   CacheConfig{Size: 0},
		UseShardedWAL: true,
	}
	parent, _, shutdown, err := setupTestVB(t, TestVB{name: "sibling_parent"}, backend)
	require.NoError(t, err)
	defer shutdown(parent.GetVolume())

	base := make([]byte, DefaultBlockSize)
	rand.Read(base)
	require.NoError(t, parent.Write(0, base))
	require.NoError(t, parent.Flush())
	require.NoError(t, parent.WriteShardedWALToChunk(true))

	snapshotID := "snap-siblings"
	_, err = parent.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	// createCloneVB names a clone deterministically from (source,
	// snapshotID), so two siblings off the same snapshot would collide on
	// one backend prefix. Use createNamedCloneVB with distinct suffixes to
	// give each its own namespace.
	cloneX := createNamedCloneVB(t, parent, snapshotID, "x")
	cloneY := createNamedCloneVB(t, parent, snapshotID, "y")

	xData := make([]byte, DefaultBlockSize)
	rand.Read(xData)
	require.NoError(t, cloneX.Write(0, xData))
	require.NoError(t, cloneX.Flush())
	// Clones always run legacy (non-sharded) WAL regardless of the parent's mode.
	require.NoError(t, cloneX.WriteWALToChunk(true))

	yData := make([]byte, DefaultBlockSize)
	rand.Read(yData)
	require.NoError(t, cloneY.Write(0, yData))
	require.NoError(t, cloneY.Flush())
	require.NoError(t, cloneY.WriteWALToChunk(true))

	gotX, err := cloneX.ReadAt(0, uint64(DefaultBlockSize))
	require.NoError(t, err)
	assert.Equal(t, xData, gotX, "cloneX must read back its own overwrite, not cloneY's")

	gotY, err := cloneY.ReadAt(0, uint64(DefaultBlockSize))
	require.NoError(t, err)
	assert.Equal(t, yData, gotY, "cloneY must read back its own overwrite, not cloneX's")

	// A third, fresh clone off the same snapshot must see the original data.
	freshSibling := createCloneVB(t, parent, snapshotID)
	gotBase, err := freshSibling.ReadAt(0, uint64(DefaultBlockSize))
	require.NoError(t, err)
	assert.Equal(t, base, gotBase, "shared ancestor snapshot must be untouched by sibling writes")

	// Chunk file names restart at 0 within every volume directory, so bare
	// names always "collide" between volumes; what matters is that the two
	// clones resolve to disjoint on-disk paths, so qualify each key with
	// its owning volume name before comparing.
	storageRoot := filepath.Dir(parent.BaseDir)
	xKeys := listChunkKeys(t, storageRoot, cloneX.VolumeName)
	yKeys := listChunkKeys(t, storageRoot, cloneY.VolumeName)
	require.NotEmpty(t, xKeys, "cloneX must have minted its own chunks (a non-empty key set is required for the exclusion check below to be meaningful)")
	require.NotEmpty(t, yKeys, "cloneY must have minted its own chunks (a non-empty key set is required for the exclusion check below to be meaningful)")

	qualify := func(volumeName string, keys []string) []string {
		out := make([]string, 0, len(keys))
		for _, k := range keys {
			out = append(out, filepath.Join(volumeName, k))
		}
		return out
	}
	xPaths := qualify(cloneX.VolumeName, xKeys)
	yPaths := qualify(cloneY.VolumeName, yKeys)
	for _, p := range xPaths {
		assert.NotContains(t, yPaths, p, "sibling clones must never share a chunk storage path")
	}
}

// listChunkKeys returns the sorted set of chunk file names under
// {storageRoot}/{volumeName}/chunks/. A missing directory is a valid
// "no chunks yet" state, not an error.
func listChunkKeys(t *testing.T, storageRoot, volumeName string) []string {
	t.Helper()
	dir := filepath.Join(storageRoot, volumeName, "chunks")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

// createNamedCloneVB is createCloneVB with an extra suffix folded into the
// clone's volume name, so two clones from the same snapshot don't collide
// on one backend prefix.
func createNamedCloneVB(t *testing.T, source *VB, snapshotID, suffix string) *VB {
	t.Helper()

	cloneName := fmt.Sprintf("clone-%s-%s-%s", source.VolumeName, snapshotID, suffix)

	var cloneBackendConfig any
	btype := source.Backend.GetBackendType()

	switch btype {
	case "file":
		cloneBackendConfig = file.FileConfig{
			VolumeName: cloneName,
			VolumeSize: source.VolumeSize,
			BaseDir:    filepath.Dir(source.BaseDir),
		}
	case "s3":
		cloneBackendConfig = s3.S3Config{
			VolumeName: cloneName,
			VolumeSize: source.VolumeSize,
			Region:     "ap-southeast-2",
			Bucket:     "predastore",
			AccessKey:  AccessKey,
			SecretKey:  SecretKey,
			Host:       source.Backend.GetHost(),
			HTTPClient: testHTTPClient,
		}
	}

	vbconfig := VB{
		VolumeName:      cloneName,
		VolumeSize:      source.VolumeSize,
		BaseDir:         source.BaseDir,
		WALSyncInterval: -1,
		Cache: Cache{
			Config: CacheConfig{Size: 0},
		},
	}

	clone, err := New(&vbconfig, btype, cloneBackendConfig)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, clone.RemoveLocalFiles())
	})

	err = clone.Backend.Init()
	require.NoError(t, err)

	if clone.UseShardedWAL {
		err = clone.OpenShardedWAL()
	} else {
		err = clone.OpenWAL(&clone.WAL, fmt.Sprintf("%s/%s", clone.WAL.BaseDir, fmt.Sprintf("%s/wal/chunks/wal.%08d.bin", cloneName, 0)))
	}
	require.NoError(t, err)

	err = clone.OpenWAL(&clone.BlockToObjectWAL, fmt.Sprintf("%s/%s", clone.BlockToObjectWAL.BaseDir, fmt.Sprintf("%s/wal/blocks/blocks.%08d.bin", cloneName, 0)))
	require.NoError(t, err)

	err = clone.OpenFromSnapshot(snapshotID)
	require.NoError(t, err)

	return clone
}

// TestClone_FrozenAtSnapshotTime_ParentContinuesWriting pins that a clone is
// a point-in-time COW view: writes the parent makes after CreateSnapshot
// ran must never become visible through the clone.
func TestClone_FrozenAtSnapshotTime_ParentContinuesWriting(t *testing.T) {
	runWithBackends(t, "clone_frozen_parent_continues", func(t *testing.T, parent *VB) {
		frozen := make([]byte, DefaultBlockSize)
		rand.Read(frozen)
		require.NoError(t, parent.Write(0, frozen))
		require.NoError(t, parent.Flush())
		require.NoError(t, parent.WriteWALToChunk(true))

		snapshotID := fmt.Sprintf("snap-%s", parent.VolumeName)
		_, err := parent.CreateSnapshot(snapshotID)
		require.NoError(t, err)

		clone := createCloneVB(t, parent, snapshotID)

		// Parent keeps writing: a fresh block, and an overwrite of the
		// block the snapshot already froze.
		afterSnapshot := make([]byte, DefaultBlockSize)
		rand.Read(afterSnapshot)
		require.NoError(t, parent.Write(50, afterSnapshot))
		overwrite := make([]byte, DefaultBlockSize)
		rand.Read(overwrite)
		require.NoError(t, parent.Write(0, overwrite))
		require.NoError(t, parent.Flush())
		require.NoError(t, parent.WriteWALToChunk(true))

		got, err := clone.ReadAt(0, uint64(DefaultBlockSize))
		require.NoError(t, err)
		assert.Equal(t, frozen, got, "clone must still read the frozen pre-snapshot data, not the parent's later overwrite")

		_, err = clone.ReadAt(50*uint64(DefaultBlockSize), uint64(DefaultBlockSize))
		assert.ErrorIs(t, err, ErrZeroBlock, "clone must not see a block the parent only wrote after the snapshot")
	})
}

// TestClone_ReadsAfterParentClosed pins that the clone read path reads
// durable backend state, not the parent VB instance's live memory, so it
// keeps working after the parent stops.
func TestClone_ReadsAfterParentClosed(t *testing.T) {
	runWithBackends(t, "clone_reads_after_parent_closed", func(t *testing.T, parent *VB) {
		data := make([]byte, DefaultBlockSize*3)
		rand.Read(data)
		require.NoError(t, parent.Write(0, data))
		require.NoError(t, parent.Flush())
		require.NoError(t, parent.WriteWALToChunk(true))

		snapshotID := fmt.Sprintf("snap-%s", parent.VolumeName)
		_, err := parent.CreateSnapshot(snapshotID)
		require.NoError(t, err)

		clone := createCloneVB(t, parent, snapshotID)

		// "Close" the parent: stop its background goroutines the way a
		// terminating instance would.
		parent.StopChunkUploader()
		parent.StopWALSyncer()

		got, err := clone.ReadAt(0, uint64(len(data)))
		require.NoError(t, err, "clone reads must not depend on the parent VB instance remaining open")
		assert.Equal(t, data, got)
	})
}

// TestMultiLevelChain_PinsAncestorChunksAcrossOverwrites pins that in a
// 3-generation chain (base -> cloneA -> cloneB) where both ancestors keep
// writing after being snapshotted/cloned from, the grandchild resolves
// every block through the correct frozen ancestor layer, immune to either
// ancestor's later mutations.
func TestMultiLevelChain_PinsAncestorChunksAcrossOverwrites(t *testing.T) {
	runWithBackends(t, "multilevel_chain_pins_ancestors", func(t *testing.T, base *VB) {
		blockSize := uint64(base.BlockSize)

		baseData := make([]byte, blockSize)
		rand.Read(baseData)
		require.NoError(t, base.Write(0, baseData))
		require.NoError(t, base.Flush())
		require.NoError(t, base.WriteWALToChunk(true))

		baseSnapID := "s0"
		_, err := base.CreateSnapshot(baseSnapID)
		require.NoError(t, err)

		cloneA := createCloneVB(t, base, baseSnapID)
		cloneAData := make([]byte, blockSize)
		rand.Read(cloneAData)
		require.NoError(t, cloneA.Write(1, cloneAData))
		require.NoError(t, cloneA.Flush())
		require.NoError(t, cloneA.WriteWALToChunk(true))

		cloneASnapID := "s1"
		_, err = cloneA.CreateSnapshot(cloneASnapID)
		require.NoError(t, err)

		cloneB := createCloneVB(t, cloneA, cloneASnapID)

		// Both ancestors keep mutating their own live data.
		baseOverwrite := make([]byte, blockSize)
		rand.Read(baseOverwrite)
		require.NoError(t, base.Write(0, baseOverwrite))
		require.NoError(t, base.Flush())
		require.NoError(t, base.WriteWALToChunk(true))

		cloneAOverwrite := make([]byte, blockSize)
		rand.Read(cloneAOverwrite)
		require.NoError(t, cloneA.Write(1, cloneAOverwrite))
		require.NoError(t, cloneA.Flush())
		require.NoError(t, cloneA.WriteWALToChunk(true))

		// cloneB must resolve block 0 through the grandparent's frozen layer.
		got, err := cloneB.ReadAt(0, blockSize)
		require.NoError(t, err)
		assert.Equal(t, baseData, got, "grandparent's post-snapshot overwrite must not be visible through the chain")

		// ...and block 1 through the parent's frozen layer.
		got, err = cloneB.ReadAt(blockSize, blockSize)
		require.NoError(t, err)
		assert.Equal(t, cloneAData, got, "parent's post-snapshot overwrite must not be visible through the chain")
	})
}
