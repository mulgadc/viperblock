package viperblock

import (
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupLooseTestVB creates a VB with the sharded WAL, a configurable sync
// interval and loose-sync mode, for the WALLooseSync tests. With a positive
// interval the periodic syncer New() starts is left running; the caller's
// cleanup stops it before removing the local files.
func setupLooseTestVB(t *testing.T, interval time.Duration, loose bool) (*VB, func()) {
	t.Helper()

	tmpDir := t.TempDir()
	testVol := fmt.Sprintf("loose_test_%d", os.Getpid())

	vbconfig := VB{
		VolumeName:      testVol,
		VolumeSize:      volumeSize,
		BaseDir:         fmt.Sprintf("%s/viperblock", tmpDir),
		WALSyncInterval: interval,
		WALLooseSync:    loose,
		Cache:           Cache{Config: CacheConfig{Size: 0}},
	}

	vb, err := New(&vbconfig, "file", file.FileConfig{
		VolumeName: testVol,
		VolumeSize: volumeSize,
		BaseDir:    tmpDir,
	})
	require.NoError(t, err)

	err = vb.Backend.Init()
	require.NoError(t, err)

	vb.UseShardedWAL = true
	vb.ShardedWAL = NewShardedWAL(vb.WAL.BaseDir, vb.WAL.WALMagic)
	err = vb.OpenShardedWAL()
	require.NoError(t, err)

	// Open BlockToObjectWAL (needed for full Flush path)
	err = vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume())))
	require.NoError(t, err)

	cleanup := func() {
		vb.StopWALSyncer()
		vb.RemoveLocalFiles()
	}

	return vb, cleanup
}

// A loose flush must return without syncing: the shard stays dirty for the
// periodic syncer, which covers it within its interval. Visibility is
// unchanged -- the records are in the file the moment flushWrites lands them.
func TestLooseFlushDefersToSyncer(t *testing.T) {
	vb, cleanup := setupLooseTestVB(t, 200*time.Millisecond, true)
	defer cleanup()

	blockSize := uint64(vb.BlockSize)
	data := make([]byte, blockSize)
	rand.Read(data)
	require.NoError(t, vb.WriteAt(0, data))

	require.NoError(t, vb.Flush())

	shard := vb.ShardedWAL.Shards[0]
	assert.True(t, shard.dirty.Load(),
		"a loose flush must leave the shard for the syncer, not sync inline")

	// The syncer covers the flush within its interval. This pass also runs
	// the parallel sync path the syncer shares with inline barriers.
	require.Eventually(t, func() bool { return !shard.dirty.Load() },
		2*time.Second, 5*time.Millisecond,
		"the periodic syncer must cover a loose flush")

	got, err := vb.ReadAt(0, blockSize)
	require.NoError(t, err)
	assert.Equal(t, data, got, "data must read back through the WAL unchanged")
}

// With the syncer disabled there is nothing to defer to: the barrier must
// sync inline, or it would report durability it never waited for.
func TestLooseFlushSyncsInlineWhenSyncerDisabled(t *testing.T) {
	vb, cleanup := setupLooseTestVB(t, -1, true)
	defer cleanup()

	blockSize := uint64(vb.BlockSize)
	data := make([]byte, blockSize)
	rand.Read(data)
	require.NoError(t, vb.WriteAt(0, data))

	require.NoError(t, vb.Flush())

	assert.False(t, vb.ShardedWAL.Shards[0].dirty.Load(),
		"with the syncer disabled, a loose flush must sync inline")
}

// Close stops the syncer before it flushes, so from the moment a stop is
// requested a loose flush must go back to syncing inline: nothing would cover
// its records otherwise.
func TestLooseFlushFallsBackWhenSyncerStops(t *testing.T) {
	vb, cleanup := setupLooseTestVB(t, 50*time.Millisecond, true)
	defer cleanup()

	blockSize := uint64(vb.BlockSize)
	data := make([]byte, blockSize)
	rand.Read(data)
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.Flush())

	// The deferred flush is covered by the syncer's next pass.
	shard := vb.ShardedWAL.Shards[0]
	require.Eventually(t, func() bool { return !shard.dirty.Load() },
		2*time.Second, 5*time.Millisecond)

	// Block 16 also routes to shard 0.
	vb.StopWALSyncer()
	require.NoError(t, vb.WriteAt(16*blockSize, data))
	require.NoError(t, vb.Flush())

	assert.False(t, shard.dirty.Load(),
		"after the syncer stops, a loose flush must sync inline")
}
