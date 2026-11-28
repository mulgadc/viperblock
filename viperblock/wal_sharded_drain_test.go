package viperblock

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shardedDrainVolume writes to a format-2 volume with a sharded WAL and
// returns it with the name and bytes of one shard that holds records.
func shardedDrainVolume(t *testing.T) (*VB, *scanChunkWrites, string, []byte) {
	t.Helper()
	volume := newMapVolume(t, 128<<20, nil)
	vb, _ := volume.engine(t, 1)
	require.NoError(t, vb.InitMapDeltaCtx(t.Context(), true))
	require.NoError(t, vb.SaveState())
	vb.UseShardedWAL = true
	vb.ShardedWAL = NewShardedWAL(vb.WAL.BaseDir, vb.WAL.WALMagic)
	require.NoError(t, vb.OpenShardedWAL())
	t.Cleanup(func() {
		vb.Detach()
		for _, shard := range vb.ShardedWAL.Shards {
			_ = shard.DB.Close()
		}
	})
	writes := &scanChunkWrites{Backend: vb.Backend}
	vb.Backend = writes
	require.NoError(t, vb.WriteAt(0, bytes.Repeat([]byte{0x47}, 8192)))
	require.NoError(t, vb.Flush())
	for _, shard := range vb.ShardedWAL.Shards {
		info, err := shard.DB.Stat()
		require.NoError(t, err)
		if info.Size() > int64(vb.WALHeaderSize()) {
			name := shard.DB.Name()
			data, err := os.ReadFile(name)
			require.NoError(t, err)
			return vb, writes, name, data
		}
	}
	t.Fatal("no shard holds a record")
	return nil, nil, "", nil
}

// The automatic drain must refuse a shard it cannot fully read and keep it,
// because the shard may hold the only copy of an acknowledged write.
func TestShardedDrainRetainsDamagedShard(t *testing.T) {
	damages := map[string]func(t *testing.T, name string, data []byte) []byte{
		"tornRecord": func(t *testing.T, name string, data []byte) []byte {
			damaged := data[:len(data)-1]
			require.NoError(t, os.Truncate(name, int64(len(damaged))))
			return damaged
		},
		"magic": func(t *testing.T, name string, data []byte) []byte {
			damaged := bytes.Clone(data)
			copy(damaged[:4], "XXXX")
			require.NoError(t, os.WriteFile(name, damaged, 0o600))
			return damaged
		},
	}
	for name, damage := range damages {
		t.Run(name, func(t *testing.T) {
			vb, writes, shardName, data := shardedDrainVolume(t)
			damaged := damage(t, shardName, data)

			for attempt := range 2 {
				assert.Error(t, vb.DrainToBackendCtx(t.Context()), "attempt %d", attempt)
				checkWALRetained(t, shardName, damaged)
			}
			assert.Zero(t, writes.count.Load(), "a refused generation must not upload chunks")
		})
	}
}
