package viperblock

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnedWriteReplaysDespitePublicBufferMutation(t *testing.T) {
	volume := newMapVolume(t, 128<<20, testKey(t, 0x31))
	vb, backend := volume.create(t, 1)
	blockSize := int(vb.BlockSize)
	expected := bytes.Repeat([]byte{0x61}, blockSize)
	src := bytes.Clone(expected)
	require.NoError(t, vb.WriteAt(0, src))
	clear(src)

	store := vb.BlockStore
	snapshot, found := store.ReadBlock(0)
	require.True(t, found)
	clear(snapshot.Data)
	single, _, err := store.ReadSingle(0)
	require.NoError(t, err)
	clear(single)
	entry, found := store.ReadEntry(0)
	require.True(t, found)
	clear(entry.Data)
	runs, foundRun := make([]BlockEntry, 1), make([]bool, 1)
	store.ReadEntryRun(0, 1, runs, foundRun)
	require.True(t, foundRun[0])
	clear(runs[0].Data)
	hot := store.GetHotBlocks()
	require.Len(t, hot, 1)
	clear(hot[0].Data)

	require.NoError(t, vb.Flush())
	pending := store.GetPendingBlocks()
	require.Len(t, pending, 1)
	clear(pending[0].Data)
	dst := make([]byte, blockSize)
	require.NoError(t, vb.ReadAtInto(0, dst))
	require.Equal(t, expected, dst)

	change := bytes.Repeat([]byte{0xa5}, 512)
	require.NoError(t, vb.WriteAt(512, change))
	copy(expected[512:], change)
	require.NoError(t, vb.ReadAtInto(0, dst))
	require.Equal(t, expected, dst)
	require.NoError(t, vb.WriteAt(uint64(blockSize)+512, change))
	require.NoError(t, vb.Flush())
	second := make([]byte, blockSize)
	copy(second[512:], change)
	expected = append(expected, second...)

	protected := make(map[string]int64)
	for _, path := range walFilesUnder(t, volume.root, volume.name) {
		info, err := os.Stat(path)
		require.NoError(t, err)
		if info.Size() > int64(vb.WALHeaderSize()) {
			protected[path] = info.Size()
		}
	}
	require.NotEmpty(t, protected)
	backend.failHead.Store(true)
	require.Error(t, vb.DrainToBackendCtx(context.Background()))
	for path, size := range protected {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, size, info.Size())
	}
	vb.Detach()

	healthy, _, err := volume.open(t, 2)
	require.NoError(t, err)
	require.Equal(t, expected, readBlocks(t, healthy, 0, 2))
	for path := range protected {
		info, err := os.Stat(path)
		if err == nil {
			// Reopen may create an empty generation at the same name.
			require.LessOrEqual(t, info.Size(), int64(vb.WALHeaderSize()))
		} else {
			require.ErrorIs(t, err, os.ErrNotExist)
		}
	}
	healthy.Detach()

	fresh, _ := volume.engine(t, 3)
	require.NoError(t, fresh.LoadState())
	require.NoError(t, fresh.LoadLiveCheckpoint())
	require.Equal(t, expected, readBlocks(t, fresh, 0, 2))
}
