package viperblock

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createV1 makes a format-1 volume the way a build without format 2 did:
// no lease epoch, VBState without a map format.
func (mv *mapVolume) createV1(t *testing.T) *VB {
	t.Helper()
	vb, _ := mv.engine(t, 0)
	require.NoError(t, vb.SaveState())
	openWALs(t, vb)
	return vb
}

// convSignals makes the engine signal each finished conversion base.
func convSignals(vb *VB) chan struct{} {
	ch := make(chan struct{}, 16)
	vb.checkpointMu.Lock()
	vb.mapChain.conv.afterBase = func() { ch <- struct{}{} }
	vb.checkpointMu.Unlock()
	return ch
}

func convPhase(vb *VB) mapConvPhase {
	vb.checkpointMu.Lock()
	defer vb.checkpointMu.Unlock()
	return vb.mapChain.conv.phase
}

// convertReady opens a format-1 volume under epoch and drains until its
// conversion base is durable, so the next ordinary drain flips.
func (mv *mapVolume) convertReady(t *testing.T, epoch uint64) (*VB, *mapFaultBackend) {
	t.Helper()
	vb, fb, err := mv.open(t, epoch)
	require.NoError(t, err)
	require.False(t, vb.IsMapDelta())
	signals := convSignals(vb)
	writeDrain(t, vb, 900, randomBlockData(1))
	<-signals
	require.Equal(t, convReady, convPhase(vb), "conversion base did not become durable")
	return vb, fb
}

func persistedFormat(t *testing.T, mv *mapVolume) uint8 {
	t.Helper()
	vb, _ := mv.engine(t, 0)
	// Read the backend's copy, which is what another host opens.
	if err := os.Remove(filepath.Join(vb.BaseDir, types.GetFilePath(types.FileTypeConfig, 0, mv.name))); err != nil {
		require.ErrorIs(t, err, os.ErrNotExist)
	}
	require.NoError(t, vb.LoadState())
	return vb.persistedMapFormat()
}

func liveIsStub(t *testing.T, mv *mapVolume) bool {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(mv.root, types.GetFilePath(types.FileTypeBlockCheckpointLive, 0, mv.name)))
	require.NoError(t, err)
	return IsMapFenceStub(data)
}

type convShape struct {
	name   string
	key    bool
	writes func(t *testing.T, vb *VB) map[uint64][]byte
}

func convShapes() []convShape {
	return []convShape{
		{"empty", false, func(*testing.T, *VB) map[uint64][]byte { return map[uint64][]byte{} }},
		{"small", false, func(t *testing.T, vb *VB) map[uint64][]byte {
			d := randomBlockData(4)
			writeDrain(t, vb, 0, d)
			return map[uint64][]byte{0: d}
		}},
		{"fragmented", false, func(t *testing.T, vb *VB) map[uint64][]byte {
			out := map[uint64][]byte{}
			for _, b := range []uint64{3, 70, 11, 500, 12, 201, 4, 999} {
				d := randomBlockData(1)
				require.NoError(t, vb.Write(b, d))
				out[b] = d
			}
			require.NoError(t, vb.DrainToBackendCtx(context.Background()))
			return out
		}},
		{"encrypted", true, func(t *testing.T, vb *VB) map[uint64][]byte {
			out := map[uint64][]byte{}
			for _, b := range []uint64{8, 9, 10, 40} {
				d := randomBlockData(1)
				writeDrain(t, vb, b, d)
				out[b] = d
			}
			return out
		}},
	}
}

func assertBlockMap(t *testing.T, vb *VB, want map[uint64][]byte) {
	t.Helper()
	for b, d := range want {
		assert.Equal(t, d, readBlocks(t, vb, b, len(d)/int(vb.BlockSize)), "block %d", b)
	}
}

func TestMapConvert_OpenRunsAndReopensEveryShape(t *testing.T) {
	for _, shape := range convShapes() {
		t.Run(shape.name, func(t *testing.T) {
			mv := newMapVolume(t, 64<<20, nil)
			if shape.key {
				mv.key = testKey(t, 9)
			}
			v1 := mv.createV1(t)
			want := shape.writes(t, v1)
			require.NoError(t, v1.CloseCtx(context.Background()))

			vb, _ := mv.convertReady(t, 7)
			late := randomBlockData(2)
			writeDrain(t, vb, 300, late)
			want[300] = late
			assert.True(t, vb.IsMapDelta(), "the drain after the base flips")
			after := randomBlockData(1)
			writeDrain(t, vb, 310, after)
			want[310] = after
			require.NoError(t, vb.CloseCtx(context.Background()))

			assert.Equal(t, MapFormatDelta, persistedFormat(t, mv))
			assert.True(t, liveIsStub(t, mv))
			re, _, err := mv.open(t, 8)
			require.NoError(t, err)
			assert.True(t, re.IsMapDelta())
			assertBlockMap(t, re, want)
		})
	}
}

func TestMapConvert_AFailureBeforeVBStateLeavesFormatOne(t *testing.T) {
	for _, step := range []string{"delta", "head", "state"} {
		t.Run(step, func(t *testing.T) {
			mv := newMapVolume(t, 64<<20, nil)
			v1 := mv.createV1(t)
			first := randomBlockData(1)
			writeDrain(t, v1, 0, first)
			require.NoError(t, v1.CloseCtx(context.Background()))

			vb, _ := mv.convertReady(t, 7)
			vb.checkpointMu.Lock()
			vb.mapChain.conv.stopBeforeFlip = func(s string) error {
				if s == step {
					return errConvStep
				}
				return nil
			}
			vb.checkpointMu.Unlock()
			late := randomBlockData(1)
			require.NoError(t, vb.Write(5, late))
			require.ErrorIs(t, vb.DrainToBackendCtx(context.Background()), errConvStep)
			assert.False(t, vb.IsMapDelta())
			vb.Detach()

			assert.NotEqual(t, MapFormatDelta, persistedFormat(t, mv))
			re, _, err := mv.open(t, 8)
			require.NoError(t, err)
			assert.False(t, re.IsMapDelta())
			assertBlockMap(t, re, map[uint64][]byte{0: first, 5: late})
		})
		t.Run(step+"/retried", func(t *testing.T) {
			mv := newMapVolume(t, 64<<20, nil)
			v1 := mv.createV1(t)
			require.NoError(t, v1.CloseCtx(context.Background()))

			vb, _ := mv.convertReady(t, 7)
			vb.checkpointMu.Lock()
			vb.mapChain.conv.stopBeforeFlip = func(s string) error {
				if s == step {
					return errConvStep
				}
				return nil
			}
			vb.checkpointMu.Unlock()
			late := randomBlockData(1)
			require.NoError(t, vb.Write(5, late))
			require.ErrorIs(t, vb.DrainToBackendCtx(context.Background()), errConvStep)
			vb.checkpointMu.Lock()
			vb.mapChain.conv.stopBeforeFlip = nil
			vb.checkpointMu.Unlock()
			require.NoError(t, vb.DrainToBackendCtx(context.Background()))
			require.True(t, vb.IsMapDelta())
			require.NoError(t, vb.CloseCtx(context.Background()))

			re, _, err := mv.open(t, 8)
			require.NoError(t, err)
			assertBlockMap(t, re, map[uint64][]byte{5: late})
		})
	}
}

func TestMapConvert_UnacknowledgedVBStateKeepsWALUntilItLands(t *testing.T) {
	// Encrypted, so no counter save after the drain masks the flip's result.
	mv := newMapVolume(t, 64<<20, testKey(t, 11))
	v1 := mv.createV1(t)
	first := randomBlockData(1)
	writeDrain(t, v1, 0, first)
	require.NoError(t, v1.CloseCtx(context.Background()))

	vb, fb := mv.convertReady(t, 7)
	fb.failConfig.Store(true)
	late := randomBlockData(1)
	require.NoError(t, vb.Write(6, late))
	require.Error(t, vb.DrainToBackendCtx(context.Background()))
	assert.True(t, vb.IsMapDelta(), "an unacknowledged flip never returns to format 1")
	assert.NotEmpty(t, walFilesUnder(t, mv.root, mv.name), "the drain keeps its WAL")
	more := randomBlockData(1)
	require.NoError(t, vb.Write(7, more))
	require.Error(t, vb.DrainToBackendCtx(context.Background()))

	// A crash now: the next opener reads format 1 and recovers the WAL.
	vb.Detach()
	re, _, err := mv.open(t, 8)
	require.NoError(t, err)
	assertBlockMap(t, re, map[uint64][]byte{0: first, 6: late, 7: more})
}

func TestMapConvert_StateRetriedUntilAcknowledged(t *testing.T) {
	mv := newMapVolume(t, 64<<20, nil)
	v1 := mv.createV1(t)
	require.NoError(t, v1.CloseCtx(context.Background()))

	vb, fb := mv.convertReady(t, 7)
	fb.failConfig.Store(true)
	writeData := randomBlockData(1)
	require.NoError(t, vb.Write(6, writeData))
	require.Error(t, vb.DrainToBackendCtx(context.Background()))
	fb.failConfig.Store(false)
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
	assert.Equal(t, MapFormatDelta, persistedFormat(t, mv))
	assert.True(t, liveIsStub(t, mv))
}

func TestMapConvert_V1LoaderFindingTheStubRewritesVBState(t *testing.T) {
	mv := newMapVolume(t, 64<<20, nil)
	v1 := mv.createV1(t)
	require.NoError(t, v1.CloseCtx(context.Background()))
	cfg := filepath.Join(mv.root, types.GetFilePath(types.FileTypeConfig, 0, mv.name))
	oldState, err := os.ReadFile(cfg)
	require.NoError(t, err)

	vb, _ := mv.convertReady(t, 7)
	d := randomBlockData(1)
	writeDrain(t, vb, 2, d)
	require.True(t, vb.IsMapDelta())
	require.NoError(t, vb.CloseCtx(context.Background()))
	// A late VBState PUT from before the flip restores format 1.
	require.NoError(t, os.WriteFile(cfg, oldState, 0o600))
	require.NotEqual(t, MapFormatDelta, persistedFormat(t, mv))

	re, _, err := mv.open(t, 8)
	require.NoError(t, err)
	assert.True(t, re.IsMapDelta())
	assertBlockMap(t, re, map[uint64][]byte{2: d})
	assert.Equal(t, MapFormatDelta, persistedFormat(t, mv), "the claim rewrote VBState")
}

func TestMapConvert_LateV1CheckpointAfterTheFlipIsIgnored(t *testing.T) {
	mv := newMapVolume(t, 64<<20, nil)
	v1 := mv.createV1(t)
	old := randomBlockData(1)
	writeDrain(t, v1, 0, old)
	require.NoError(t, v1.CloseCtx(context.Background()))
	live := filepath.Join(mv.root, types.GetFilePath(types.FileTypeBlockCheckpointLive, 0, mv.name))
	staleV1, err := os.ReadFile(live)
	require.NoError(t, err)

	vb, _ := mv.convertReady(t, 7)
	newer := randomBlockData(1)
	writeDrain(t, vb, 0, newer)
	require.True(t, vb.IsMapDelta())
	require.NoError(t, vb.CloseCtx(context.Background()))
	require.NoError(t, os.WriteFile(live, staleV1, 0o600))

	re, _, err := mv.open(t, 8)
	require.NoError(t, err)
	assert.True(t, re.IsMapDelta())
	assert.Equal(t, newer, readBlocks(t, re, 0, 1))
}

func TestMapConvert_CloseNeverFlips(t *testing.T) {
	mv := newMapVolume(t, 64<<20, nil)
	v1 := mv.createV1(t)
	require.NoError(t, v1.CloseCtx(context.Background()))

	vb, _ := mv.convertReady(t, 7)
	d := randomBlockData(1)
	require.NoError(t, vb.Write(3, d))
	vb.StopMapBackground()
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))
	require.NoError(t, vb.CloseCtx(context.Background()))
	assert.False(t, vb.IsMapDelta())
	assert.NotEqual(t, MapFormatDelta, persistedFormat(t, mv))
	assert.False(t, liveIsStub(t, mv))

	re, _, err := mv.open(t, 8)
	require.NoError(t, err)
	assertBlockMap(t, re, map[uint64][]byte{3: d})
}

func TestMapConvert_CloseDoesNotWaitForAStalledBase(t *testing.T) {
	mv := newMapVolume(t, 64<<20, nil)
	v1 := mv.createV1(t)
	require.NoError(t, v1.CloseCtx(context.Background()))

	vb, fb, err := mv.open(t, 7)
	require.NoError(t, err)
	gate := make(chan struct{})
	fb.baseGate.Store(&gate)
	signals := convSignals(vb)
	d := randomBlockData(1)
	writeDrain(t, vb, 1, d)
	require.Equal(t, convBase, convPhase(vb))

	require.NoError(t, vb.CloseCtx(context.Background()), "close returns with the base still stalled")
	close(gate)
	<-signals
	vb.waitMapBackground()
	assert.NotEqual(t, MapFormatDelta, persistedFormat(t, mv))

	re, _, err := mv.open(t, 8)
	require.NoError(t, err)
	assertBlockMap(t, re, map[uint64][]byte{1: d})
}
