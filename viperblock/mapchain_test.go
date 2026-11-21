package viperblock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errInjected = errors.New("injected map write failure")

// mapFaultBackend wraps the file backend to fail or hide map objects.
type mapFaultBackend struct {
	*file.Backend

	failHead       atomic.Bool
	failDelta      atomic.Bool
	hideNewestHead atomic.Bool
	failChunks     atomic.Bool
	keyWrites      atomic.Int64
	keyBytes       atomic.Int64

	// onMapList, when set, runs once inside the next listing of map/.
	onMapList atomic.Pointer[func()]
	// baseGate, when set, holds every base write until it is closed.
	baseGate atomic.Pointer[chan struct{}]
}

func (b *mapFaultBackend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	if b.failChunks.Load() && fileType == types.FileTypeChunk {
		return errInjected
	}
	return b.Backend.WriteCtx(ctx, fileType, objectId, headers, data)
}

var _ types.Backend = (*mapFaultBackend)(nil)

func (b *mapFaultBackend) WriteKeyCtx(ctx context.Context, key string, data []byte) error {
	if b.failHead.Load() && strings.HasPrefix(key, mapHeadPrefix) {
		return errInjected
	}
	if b.failDelta.Load() && strings.HasPrefix(key, mapDeltaPrefix) {
		return errInjected
	}
	if g := b.baseGate.Load(); g != nil && strings.HasPrefix(key, mapBasePrefix) {
		<-*g
	}
	b.keyWrites.Add(1)
	b.keyBytes.Add(int64(len(data)))
	return b.Backend.WriteKeyCtx(ctx, key, data)
}

func (b *mapFaultBackend) ListObjectsCtx(ctx context.Context, prefix string) ([]string, error) {
	if strings.HasSuffix(prefix, "/map/") {
		if fn := b.onMapList.Swap(nil); fn != nil {
			(*fn)()
		}
	}
	keys, err := b.Backend.ListObjectsCtx(ctx, prefix)
	if err != nil || !b.hideNewestHead.Load() {
		return keys, err
	}
	var heads []string
	for _, k := range keys {
		if strings.Contains(k, "/"+mapHeadPrefix) {
			heads = append(heads, k)
		}
	}
	if len(heads) == 0 {
		return keys, nil
	}
	slices.Sort(heads)
	newest := heads[len(heads)-1]
	return slices.DeleteFunc(keys, func(k string) bool { return k == newest }), nil
}

type mapTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *mapTestClock) get() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mapTestClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type mapVolume struct {
	root, name string
	size       uint64
	key        *masterkey.Key
	clock      *mapTestClock
}

func newMapVolume(t testing.TB, size uint64, key *masterkey.Key) *mapVolume {
	t.Helper()
	return &mapVolume{
		root: t.TempDir(), name: "vol-map", size: size, key: key,
		clock: &mapTestClock{now: time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)},
	}
}

// engine constructs a VB for the volume under epoch, without loading it.
func (mv *mapVolume) engine(t testing.TB, epoch uint64) (*VB, *mapFaultBackend) {
	t.Helper()
	vb, err := New(&VB{
		VolumeName:          mv.name,
		VolumeSize:          mv.size,
		BaseDir:             filepath.Join(mv.root, "viperblock"),
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		GCEnabled:           true,
		GCInterval:          -1,
		MasterKey:           mv.key,
		EncryptionEnabled:   mv.key != nil,
		Cache:               Cache{Config: CacheConfig{Size: 0}},
		LeaseEpoch:          epoch,
	}, FileBackend, file.FileConfig{VolumeName: mv.name, VolumeSize: mv.size, BaseDir: mv.root})
	require.NoError(t, err)
	t.Cleanup(vb.Detach)
	fb, ok := vb.Backend.(*file.Backend)
	require.True(t, ok)
	wrapped := &mapFaultBackend{Backend: fb}
	vb.Backend = wrapped
	vb.checkpointRetryBackoff = time.Microsecond
	vb.mapClock = mv.clock.get
	require.NoError(t, vb.Backend.Init())
	return vb, wrapped
}

func openWALs(t testing.TB, vb *VB) {
	t.Helper()
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))
}

// create makes the volume format 2 under a creation epoch, VBState last.
func (mv *mapVolume) create(t testing.TB, epoch uint64) (*VB, *mapFaultBackend) {
	t.Helper()
	vb, fb := mv.engine(t, epoch)
	require.NoError(t, vb.InitMapDeltaCtx(context.Background(), true))
	require.NoError(t, vb.SaveState())
	openWALs(t, vb)
	return vb, fb
}

// open loads, claims and recovers the volume the way the plugin does.
func (mv *mapVolume) open(t *testing.T, epoch uint64) (*VB, *mapFaultBackend, error) {
	t.Helper()
	vb, fb := mv.engine(t, epoch)
	require.NoError(t, vb.LoadState())
	if err := vb.LoadLiveCheckpoint(); err != nil {
		return vb, fb, err
	}
	if err := vb.ClaimMapCtx(context.Background()); err != nil {
		return vb, fb, err
	}
	if err := vb.RecoverLocalWALs(); err != nil {
		return vb, fb, err
	}
	openWALs(t, vb)
	return vb, fb, nil
}

func writeDrain(t *testing.T, vb *VB, block uint64, data []byte) {
	t.Helper()
	require.NoError(t, vb.Write(block, data))
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
}

func readBlocks(t *testing.T, vb *VB, block uint64, n int) []byte {
	t.Helper()
	got, err := vb.ReadAt(block*uint64(vb.BlockSize), uint64(n)*uint64(vb.BlockSize))
	require.NoError(t, err)
	return got
}

func (mv *mapVolume) mapKeys(t *testing.T, prefix string) []string {
	t.Helper()
	var out []string
	dir := filepath.Join(mv.root, mv.name, "map")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	for _, e := range entries {
		if strings.HasPrefix("map/"+e.Name(), prefix) {
			out = append(out, "map/"+e.Name())
		}
	}
	slices.Sort(out)
	return out
}

func TestMapRunsCodec_RoundTripsEverySeq(t *testing.T) {
	runs := []BlockLookup{
		{StartBlock: 10, NumBlocks: 4, ObjectID: 7, ObjectOffset: 96, SeqNum: 100, SeqNums: []uint64{100, 101, 102, 103}},
		{StartBlock: 2, NumBlocks: 3, ObjectID: 3, ObjectOffset: 0, SeqNum: 50, SeqNums: []uint64{50, 49, 400}},
		{StartBlock: 1 << 40, NumBlocks: 1, ObjectID: 1 << 33, ObjectOffset: 4096, SeqNum: 1},
		{StartBlock: 11, NumBlocks: 2, ObjectID: 8, ObjectOffset: 8, SeqNum: 900, SeqNums: []uint64{900, 900}},
	}
	got, err := decodeMapRuns(encodeMapRuns(runs))
	require.NoError(t, err)
	require.Len(t, got, len(runs))
	for i, r := range runs {
		assert.Equal(t, r.StartBlock, got[i].StartBlock)
		assert.Equal(t, r.NumBlocks, got[i].NumBlocks)
		assert.Equal(t, r.ObjectID, got[i].ObjectID)
		assert.Equal(t, r.ObjectOffset, got[i].ObjectOffset)
		for b := range int(r.NumBlocks) {
			assert.Equal(t, r.seqNumAt(b), got[i].seqNumAt(b), "run %d block %d", i, b)
		}
	}
}

func TestMapRunsCodec_SequentialRunsAreSmall(t *testing.T) {
	runs := make([]BlockLookup, 1000)
	var seq uint64 = 1
	for i := range runs {
		seqs := make([]uint64, 1024)
		for j := range seqs {
			seqs[j] = seq
			seq++
		}
		runs[i] = BlockLookup{StartBlock: uint64(i) * 1024, NumBlocks: 1024, ObjectID: uint64(i), ObjectOffset: 64, SeqNum: seqs[0], SeqNums: seqs}
	}
	perRun := float64(len(encodeMapRuns(runs))) / float64(len(runs))
	assert.Less(t, perRun, 16.0, "a sequential run must average under 16 B")
}

func TestMapObject_RejectsCorruptionAndWrongVolume(t *testing.T) {
	obj := frameMapObject(mapKindDelta, "vol-a", 5, 9, []byte("body"))
	body, err := unframeMapObject(obj, mapKindDelta, "vol-a", 5, 9)
	require.NoError(t, err)
	assert.Equal(t, []byte("body"), body)

	_, err = unframeMapObject(obj, mapKindDelta, "vol-b", 5, 9)
	assert.ErrorIs(t, err, ErrMapObjectCorrupt)
	_, err = unframeMapObject(obj, mapKindDelta, "vol-a", 6, 9)
	assert.ErrorIs(t, err, ErrMapObjectCorrupt)
	bad := slices.Clone(obj)
	bad[len(bad)/2] ^= 0xFF
	_, err = unframeMapObject(bad, mapKindDelta, "vol-a", 5, 9)
	assert.ErrorIs(t, err, ErrMapObjectCorrupt)
}

func TestMapFenceStub_EveryV1ParserRejectsIt(t *testing.T) {
	stub := MapFenceStub()
	_, err := ParseBlockCheckpointBytes(stub, 1)
	assert.Error(t, err)
	vb := newGCTestVB(t, t.TempDir(), "vol-v1", false)
	vb.BlocksToObject.mu.Lock()
	err = vb.parseBlockCheckpoint(stub)
	vb.BlocksToObject.mu.Unlock()
	assert.Error(t, err, "a v1 load must fail on the stub, never open an empty map")
}

func TestMapDelta_ReopenReadsEveryCommittedWrite(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, _ := mv.create(t, 5)
	a, b := randomBlockData(3), randomBlockData(2)
	writeDrain(t, vb, 0, a)
	writeDrain(t, vb, 1, b)
	require.NoError(t, vb.CloseCtx(context.Background()))

	want := append(slices.Clone(a[:DefaultBlockSize]), b...)
	re, _, err := mv.open(t, 6)
	require.NoError(t, err)
	assert.Equal(t, want, readBlocks(t, re, 0, 3))
	assert.Contains(t, mv.mapKeys(t, mapHeadPrefix), mapObjectKey(mapHeadPrefix, 6, 0), "the launch claims before serving IO")
	assert.Empty(t, mv.mapKeys(t, "map/blocks"), "no v1 checkpoint is written")
	live, err := os.ReadFile(filepath.Join(mv.root, types.GetFilePath(types.FileTypeBlockCheckpointLive, 0, mv.name)))
	require.NoError(t, err)
	assert.True(t, IsMapFenceStub(live), "the live key keeps the stub")
}

func TestMapDelta_OpenFailsWhenEpochIsNotNewer(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, _ := mv.create(t, 5)
	writeDrain(t, vb, 0, randomBlockData(1))
	vb.Detach()

	re, _ := mv.engine(t, 5)
	require.NoError(t, re.LoadState())
	err := re.LoadLiveCheckpoint()
	require.ErrorIs(t, err, ErrMapEpochNotNewer)
	re.BlocksToObject.mu.RLock()
	assert.Zero(t, re.BlocksToObject.lookup.len(), "a refused open touches nothing")
	re.BlocksToObject.mu.RUnlock()
}

func TestMapDelta_MissingOrCorruptDeltaFailsTheOpen(t *testing.T) {
	for _, damage := range []string{"missing", "corrupt"} {
		t.Run(damage, func(t *testing.T) {
			mv := newMapVolume(t, volumeSize, nil)
			vb, _ := mv.create(t, 5)
			writeDrain(t, vb, 0, randomBlockData(1))
			writeDrain(t, vb, 1, randomBlockData(1))
			vb.Detach()

			deltas := mv.mapKeys(t, mapDeltaPrefix)
			require.Len(t, deltas, 2)
			path := filepath.Join(mv.root, mv.name, filepath.FromSlash(deltas[0]))
			if damage == "missing" {
				require.NoError(t, os.Remove(path))
			} else {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				data[len(data)/2] ^= 0xFF
				require.NoError(t, os.WriteFile(path, data, 0o600))
			}
			_, _, err := mv.open(t, 6)
			require.ErrorIs(t, err, ErrMapObjectCorrupt, "never fall back to an older head")
		})
	}
}

func TestMapDelta_FailedHeadCommitsNothingAndMergesIntoTheNext(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, fb := mv.create(t, 5)
	first, second := randomBlockData(1), randomBlockData(1)

	require.NoError(t, vb.Write(0, first))
	fb.failHead.Store(true)
	require.Error(t, vb.DrainToBackendCtx(context.Background()))
	fb.failHead.Store(false)
	orphans := mv.mapKeys(t, mapDeltaPrefix)
	require.Len(t, orphans, 1, "the delta landed, but no head names it")

	writeDrain(t, vb, 1, second)
	vb.Detach()

	re, _, err := mv.open(t, 6)
	require.NoError(t, err)
	assert.Equal(t, append(slices.Clone(first), second...), readBlocks(t, re, 0, 2))
	re.checkpointMu.Lock()
	named := re.mapChain.head.Deltas
	re.checkpointMu.Unlock()
	for _, d := range named {
		assert.NotEqual(t, orphans[0], d.key(mapDeltaPrefix), "a new attempt never reuses a name")
	}
}

func TestMapDelta_ForwardProbeFindsAHeadTheListingMissed(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, _ := mv.create(t, 5)
	writeDrain(t, vb, 0, randomBlockData(1))
	latest := randomBlockData(1)
	writeDrain(t, vb, 0, latest)
	vb.Detach()

	re, fb := mv.engine(t, 6)
	fb.hideNewestHead.Store(true)
	require.NoError(t, re.LoadState())
	require.NoError(t, re.LoadLiveCheckpoint())
	assert.Equal(t, latest, readBlocks(t, re, 0, 1))
}

func TestMapDelta_StaleOnlyDrainWritesNoDelta(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, fb := mv.create(t, 5)
	writeDrain(t, vb, 0, randomBlockData(1))
	before := fb.keyWrites.Load()
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
	assert.Equal(t, before, fb.keyWrites.Load(), "an unchanged map writes nothing")
	assert.False(t, vb.BlocksToObject.dirty.Load())
}

// The bytes a drain persists depend on the runs it changed, not on how much
// of the volume is already mapped.
func TestMapDelta_DeltaSizeIndependentOfVolumeSize(t *testing.T) {
	sizes := map[string]uint64{"4GiB": 4 << 30, "400GiB": 400 << 30}
	got := map[string]int64{}
	for name, size := range sizes {
		mv := newMapVolume(t, size, nil)
		vb, fb := mv.create(t, 5)
		if name == "400GiB" {
			// Map 2,000 scattered runs first.
			for i := range 2000 {
				require.NoError(t, vb.Write(uint64(i)*997, randomBlockData(1)))
			}
			require.NoError(t, vb.DrainToBackendCtx(context.Background()))
		}
		before := fb.keyBytes.Load()
		writeDrain(t, vb, 5, randomBlockData(16))
		got[name] = fb.keyBytes.Load() - before
	}
	// Only the head grows with the number of deltas above the base.
	assert.InDelta(t, got["4GiB"], got["400GiB"], 64, "delta plus head: %v", got)
}

func TestMapDelta_EncryptedMixedSeqRunReopens(t *testing.T) {
	mv := newMapVolume(t, 64<<20, testKey(t, 7))
	vb, _ := mv.create(t, 5)
	ctx := context.Background()
	require.NoError(t, vb.Write(10, randomBlockData(1)))
	b11 := randomBlockData(1)
	require.NoError(t, vb.Write(11, b11))
	b10 := randomBlockData(1)
	require.NoError(t, vb.Write(10, b10))
	require.NoError(t, vb.DrainToBackendCtx(ctx))
	over := randomBlockData(3)
	writeDrain(t, vb, 9, over)
	b12 := randomBlockData(1)
	writeDrain(t, vb, 12, b12)
	require.NoError(t, vb.CloseCtx(ctx))

	re, _, err := mv.open(t, 6)
	require.NoError(t, err)
	want := slices.Concat(over, b12)
	assert.Equal(t, want, readBlocks(t, re, 9, 4))
}

func TestMapDelta_BaseCoversItsDeltaAndPrunesOneGenerationBack(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, _ := mv.create(t, 5)
	bases := make(chan struct{}, 4)
	deletes := make(chan struct{}, 4)
	vb.mapChain.afterBase = func() { bases <- struct{}{} }
	vb.mapChain.afterDeletes = func() { deletes <- struct{}{} }
	ctx := context.Background()

	// Two base generations, each due by age.
	var last []byte
	for range 2 {
		writeDrain(t, vb, 0, randomBlockData(1))
		mv.clock.advance(mapBaseMaxAge)
		last = randomBlockData(1)
		writeDrain(t, vb, 1, last)
		<-bases
	}
	vb.checkpointMu.Lock()
	head := vb.mapChain.head.clone()
	queued := len(vb.mapChain.deletes)
	vb.checkpointMu.Unlock()
	assert.Empty(t, head.Deltas, "the base covers the delta it was taken at")
	assert.Positive(t, queued, "the first base's predecessors are queued")
	assert.Len(t, mv.mapKeys(t, mapBasePrefix), 3, "nothing is deleted before the deferral")

	mv.clock.advance(DefaultMapDeleteDeferral)
	untilTrue(t, deletes, func() bool { return len(mv.mapKeys(t, mapBasePrefix)) == 2 },
		func() { writeDrain(t, vb, 2, randomBlockData(1)) })
	assert.Len(t, mv.mapKeys(t, mapBasePrefix), 2, "the base before the previous one goes; one generation back stays")

	vb.Detach()
	re, _, err := mv.open(t, 6)
	require.NoError(t, err)
	assert.Equal(t, last, readBlocks(t, re, 1, 1))
	_ = ctx
}

func TestMapDelta_StaleWriterFencesItselfBeforeDeleting(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	old, _ := mv.create(t, 5)
	bases := make(chan struct{}, 4)
	deletes := make(chan struct{}, 4)
	old.mapChain.afterBase = func() { bases <- struct{}{} }
	old.mapChain.afterDeletes = func() { deletes <- struct{}{} }
	for range 2 {
		writeDrain(t, old, 0, randomBlockData(1))
		mv.clock.advance(mapBaseMaxAge)
		writeDrain(t, old, 1, randomBlockData(1))
		<-bases
	}
	waitDeletesIdle(old, deletes)
	before := mv.mapKeys(t, "map/")

	// A newer launch takes over while the old writer is still running.
	_, _, err := mv.open(t, 6)
	require.NoError(t, err)

	mv.clock.advance(DefaultMapDeleteDeferral)
	untilTrue(t, deletes, old.mapChain.fenced.Load, func() {
		old.checkpointMu.Lock()
		old.maybeProcessMapDeletes()
		old.checkpointMu.Unlock()
	})
	assert.True(t, old.mapChain.fenced.Load())
	assert.Subset(t, mv.mapKeys(t, "map/"), before, "a fenced writer deletes nothing")
	assert.ErrorIs(t, old.Write(3, randomBlockData(1)), ErrMapFenced)
}

func TestMapDelta_RecoveryKeepsWALWhenTheCommitFails(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, _ := mv.create(t, 5)
	writeDrain(t, vb, 0, randomBlockData(1))
	acked := randomBlockData(1)
	require.NoError(t, vb.Write(0, acked))
	require.NoError(t, vb.Flush())
	require.NoError(t, vb.SaveState())
	vb.Detach()
	before := walFilesUnder(t, mv.root, mv.name)
	require.NotEmpty(t, before)

	re, fb := mv.engine(t, 6)
	require.NoError(t, re.LoadState())
	require.NoError(t, re.LoadLiveCheckpoint())
	require.NoError(t, re.ClaimMapCtx(context.Background()))
	fb.failHead.Store(true)
	require.Error(t, re.RecoverLocalWALs())
	assert.Subset(t, walFilesUnder(t, mv.root, mv.name), before, "replayed WAL is kept")
	re.Detach()

	healthy, _, err := mv.open(t, 7)
	require.NoError(t, err)
	assert.Equal(t, acked, readBlocks(t, healthy, 0, 1))
}

func TestMapDelta_CloseKeepsLocalFilesWhenTheCommitFails(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, fb := mv.create(t, 5)
	acked := randomBlockData(2)
	require.NoError(t, vb.Write(0, acked))
	require.NoError(t, vb.Flush())
	fb.failHead.Store(true)
	require.Error(t, vb.CloseCtx(context.Background()))
	require.NotEmpty(t, walFilesUnder(t, mv.root, mv.name))

	re, _, err := mv.open(t, 6)
	require.NoError(t, err)
	assert.Equal(t, acked, readBlocks(t, re, 0, 2))
}

func TestMapDelta_ChunkDeletesWaitOutTheDeferral(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, _ := mv.create(t, 5)
	ctx := context.Background()
	writeDrain(t, vb, 0, randomBlockData(1))
	garbage := vb.ObjectNum.Load() - 1
	writeDrain(t, vb, 0, randomBlockData(1))

	vb.runGCSweep(ctx)
	assertChunkPresent(t, vb.Backend, mv.name, garbage)
	mv.clock.advance(DefaultMapDeleteDeferral - time.Second)
	vb.runGCSweep(ctx)
	assertChunkPresent(t, vb.Backend, mv.name, garbage)
	mv.clock.advance(time.Second)
	vb.runGCSweep(ctx)
	assertChunkGone(t, vb.Backend, mv.name, garbage)
}

func TestMapDelta_StubIsTheAuthorityWithoutVBState(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, _ := mv.create(t, 5)
	data := randomBlockData(1)
	writeDrain(t, vb, 0, data)
	vb.Detach()

	re, _ := mv.engine(t, 6)
	require.NoError(t, re.LoadState())
	re.MapFormat = 0
	re.BlocksToObject.trackChanges = false
	require.NoError(t, re.LoadLiveCheckpoint())
	assert.True(t, re.IsMapDelta())
	assert.Equal(t, data, readBlocks(t, re, 0, 1))
}

func TestMapDelta_ClaimFailsWhenAFenceStubWasReplaced(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, _ := mv.create(t, 5)
	writeDrain(t, vb, 0, randomBlockData(1))
	vb.Detach()
	live := filepath.Join(mv.root, types.GetFilePath(types.FileTypeBlockCheckpointLive, 0, mv.name))
	require.NoError(t, os.WriteFile(live, []byte("a late v1 checkpoint"), 0o600))

	_, _, err := mv.open(t, 6)
	require.ErrorIs(t, err, ErrMapFenceStubAbsent)
}

func walFilesUnder(t *testing.T, root, volumeName string) []string {
	t.Helper()
	var out []string
	_ = filepath.Walk(filepath.Join(root, "viperblock", volumeName, "wal"), func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && info.Size() > 0 {
			out = append(out, path)
		}
		return nil
	})
	slices.Sort(out)
	return out
}

// drainSignals discards signals from earlier background passes, waiting for
// any pass still running to finish first.
func drainSignals(ch chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// untilTrue runs trigger and waits for a background pass to signal, until
// cond holds. A pass already running when trigger fires skips it, so the
// trigger repeats; the deadline only bounds a hang.
func untilTrue(t *testing.T, signals chan struct{}, cond func() bool, trigger func()) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for !cond() {
		trigger()
		select {
		case <-signals:
		case <-deadline:
			t.Fatal("background pass never satisfied the condition")
		}
	}
}

func TestMapDelta_EmptyCommitNeedsNoEpochAndAChangeDoes(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, _ := mv.create(t, 5)
	writeDrain(t, vb, 0, randomBlockData(1))
	vb.Detach()

	reader, _ := mv.engine(t, 0)
	require.NoError(t, reader.LoadState())
	require.NoError(t, reader.LoadLiveCheckpoint())
	require.NoError(t, reader.RecoverLocalWALs(), "recovery with nothing to replay commits nothing")
	openWALs(t, reader)
	require.NoError(t, reader.DrainToBackendCtx(context.Background()), "an idle drain on a reader succeeds")
	require.NoError(t, reader.Write(1, randomBlockData(1)))
	require.ErrorIs(t, reader.DrainToBackendCtx(context.Background()), ErrMapNoLeaseEpoch)
}

// The same nbdkit process reconnecting resumes the heads it wrote under its
// own epoch; any other launch with that epoch is still refused.
func TestMapDelta_SameProcessReconnectResumesItsEpoch(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, _ := mv.create(t, 5)
	writeDrain(t, vb, 0, randomBlockData(1))
	vb.Detach()

	first, _, err := mv.open(t, 6)
	require.NoError(t, err)
	a := randomBlockData(1)
	writeDrain(t, first, 1, a)
	require.NoError(t, first.CloseCtx(context.Background()))

	refused, _ := mv.engine(t, 6)
	require.NoError(t, refused.LoadState())
	require.ErrorIs(t, refused.LoadLiveCheckpoint(), ErrMapEpochNotNewer, "another launch may never reuse an epoch")

	again, _ := mv.engine(t, 6)
	again.ResumeEpoch = true
	require.NoError(t, again.LoadState())
	require.NoError(t, again.LoadLiveCheckpoint())
	require.NoError(t, again.ClaimMapCtx(context.Background()))
	require.NoError(t, again.RecoverLocalWALs())
	openWALs(t, again)
	b := randomBlockData(1)
	writeDrain(t, again, 2, b)
	require.NoError(t, again.CloseCtx(context.Background()))

	next, _, err := mv.open(t, 7)
	require.NoError(t, err)
	assert.Equal(t, append(slices.Clone(a), b...), readBlocks(t, next, 1, 2))
}

// A seeded create publishes no head until the seed's map is a durable base,
// and a failed commit leaves no VBState to report the volume created.
func TestMapDelta_SeededCreatePublishesOnlyTheCompleteMap(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("commit-fails=%t", fail), func(t *testing.T) {
			mv := newMapVolume(t, volumeSize, nil)
			vb, fb := mv.engine(t, 5)
			require.NoError(t, vb.InitMapDeltaCtx(context.Background(), false))
			openWALs(t, vb)
			seed := randomBlockData(4)
			require.NoError(t, vb.Write(0, seed))
			assert.Empty(t, mv.mapKeys(t, mapHeadPrefix), "no head before the seed is durable")
			fb.failHead.Store(fail)
			err := vb.CloseCtx(context.Background())
			state := filepath.Join(mv.root, types.GetFilePath(types.FileTypeConfig, 0, mv.name))
			if fail {
				require.Error(t, err)
				assert.Empty(t, mv.mapKeys(t, mapHeadPrefix))
				_, serr := os.Stat(state)
				assert.True(t, os.IsNotExist(serr), "no VBState after a failed seed commit")
				return
			}
			require.NoError(t, err)
			_, serr := os.Stat(state)
			require.NoError(t, serr)
			re, _, oerr := mv.open(t, 6)
			require.NoError(t, oerr)
			assert.Equal(t, seed, readBlocks(t, re, 0, 4))
			re.checkpointMu.Lock()
			assert.Empty(t, re.mapChain.head.Deltas, "the seed is the base")
			re.checkpointMu.Unlock()
		})
	}
}

func TestMapDelta_ClaimAfterItsDeadlineWritesNothing(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, _ := mv.create(t, 5)
	writeDrain(t, vb, 0, randomBlockData(1))
	vb.Detach()
	before := mv.mapKeys(t, mapHeadPrefix)

	re, _ := mv.engine(t, 6)
	require.NoError(t, re.LoadState())
	require.NoError(t, re.LoadLiveCheckpoint())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, re.ClaimMapCtx(ctx))
	assert.Equal(t, before, mv.mapKeys(t, mapHeadPrefix))
}

func TestMapDelta_ClaimMovesChunkIDsPastAStaleWriter(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	old, _ := mv.create(t, 5)
	writeDrain(t, old, 0, randomBlockData(1))
	staleNext := old.ObjectNum.Load()

	re, _ := mv.engine(t, 6)
	require.NoError(t, re.LoadState())
	require.NoError(t, re.LoadLiveCheckpoint())
	require.NoError(t, re.ClaimMapCtx(context.Background()))
	assert.GreaterOrEqual(t, re.ObjectNum.Load(), staleNext+mapClaimObjectGap)
}

// A failed chunk upload at Close, like a failed commit, leaves no VBState to
// report a seeded creation complete.
func TestMapDelta_SeededCreateFailedChunkUploadWritesNoState(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, fb := mv.engine(t, 5)
	require.NoError(t, vb.InitMapDeltaCtx(context.Background(), false))
	openWALs(t, vb)
	require.NoError(t, vb.Write(0, randomBlockData(2)))
	fb.failChunks.Store(true)
	require.Error(t, vb.CloseCtx(context.Background()))
	_, err := os.Stat(filepath.Join(mv.root, types.GetFilePath(types.FileTypeConfig, 0, mv.name)))
	assert.True(t, os.IsNotExist(err))
	assert.Empty(t, mv.mapKeys(t, mapHeadPrefix))
}

// A delta committed while a prune pass lists map/ is outside the pass's
// bound, so it survives the deferred delete and the next open reads it.
func TestMapDelta_PruneNeverDeletesADeltaCommittedDuringItsListing(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, fb := mv.create(t, 5)
	bases := make(chan struct{}, 4)
	deletes := make(chan struct{}, 8)
	vb.mapChain.afterBase = func() { bases <- struct{}{} }
	vb.mapChain.afterDeletes = func() { deletes <- struct{}{} }

	writeDrain(t, vb, 0, randomBlockData(1))
	mv.clock.advance(mapBaseMaxAge)
	during := randomBlockData(1)
	hook := func() { writeDrain(t, vb, 7, during) }
	fb.onMapList.Store(&hook)
	writeDrain(t, vb, 1, randomBlockData(1))
	<-bases

	mv.clock.advance(DefaultMapDeleteDeferral)
	untilTrue(t, deletes, func() bool {
		vb.checkpointMu.Lock()
		defer vb.checkpointMu.Unlock()
		return len(vb.mapChain.deletes) == 0
	}, func() { writeDrain(t, vb, 2, randomBlockData(1)) })
	vb.Detach()

	re, _, err := mv.open(t, 6)
	require.NoError(t, err, "every delta the newest head names still exists")
	assert.Equal(t, during, readBlocks(t, re, 7, 1))
}

// Close waits for a background base, so no head is written after it returns.
func TestMapDelta_CloseWaitsForABaseInFlight(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, fb := mv.create(t, 5)
	var finished atomic.Bool
	vb.mapChain.afterBase = func() { finished.Store(true) }
	gate := make(chan struct{})
	fb.baseGate.Store(&gate)

	writeDrain(t, vb, 0, randomBlockData(1))
	mv.clock.advance(mapBaseMaxAge)
	writeDrain(t, vb, 1, randomBlockData(1))
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	vb.mapChain.beforeWait = release
	require.NoError(t, vb.CloseCtx(context.Background()))
	assert.True(t, finished.Load(), "the base finished before Close returned")
}

// waitDeletesIdle waits for a delete pass still running to finish, then
// discards its signal. A pass clears its busy flag before signalling.
func waitDeletesIdle(vb *VB, ch chan struct{}) {
	for vb.mapChain.deleteBusy.Load() {
		<-ch
	}
	drainSignals(ch)
}
