package viperblock

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shardedDrainVolume writes to a format-2 volume with a sharded WAL and
// returns it with the name and bytes of one shard that holds records, and
// the name of one shard that holds only its header.
func shardedDrainVolume(t *testing.T) (*VB, *scanChunkWrites, string, []byte, string) {
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
	var name, empty string
	var data []byte
	for _, shard := range vb.ShardedWAL.Shards {
		info, err := shard.DB.Stat()
		require.NoError(t, err)
		switch {
		case info.Size() > int64(vb.WALHeaderSize()) && name == "":
			name = shard.DB.Name()
			data, err = os.ReadFile(name)
			require.NoError(t, err)
		case info.Size() == int64(vb.WALHeaderSize()) && empty == "":
			empty = shard.DB.Name()
		}
	}
	require.NotEmpty(t, name, "no shard holds a record")
	require.NotEmpty(t, empty, "no shard holds only its header")
	return vb, writes, name, data, empty
}

// The automatic drain must refuse a shard it cannot fully read and keep it,
// because the shard may hold the only copy of an acknowledged write.
func TestShardedDrainRetainsDamagedShard(t *testing.T) {
	truncate := func(t *testing.T, name string, data []byte, size int) []byte {
		t.Helper()
		require.NoError(t, os.Truncate(name, int64(size)))
		return data[:size]
	}
	cases := map[string]struct {
		damage    func(t *testing.T, vb *VB, name string, data []byte, empty string) []byte
		integrity bool
	}{
		"tornRecord": {
			damage: func(t *testing.T, _ *VB, name string, data []byte, _ string) []byte {
				return truncate(t, name, data, len(data)-1)
			},
			integrity: true,
		},
		"shortHeader": {
			damage: func(t *testing.T, vb *VB, name string, data []byte, _ string) []byte {
				return truncate(t, name, data, vb.WALHeaderSize()-1)
			},
			integrity: true,
		},
		"magic": {
			damage: func(t *testing.T, _ *VB, name string, data []byte, _ string) []byte {
				damaged := bytes.Clone(data)
				copy(damaged[:4], "XXXX")
				require.NoError(t, os.WriteFile(name, damaged, 0o600))
				return damaged
			},
		},
		"missingShard": {
			damage: func(t *testing.T, _ *VB, _ string, data []byte, empty string) []byte {
				require.NoError(t, os.Remove(empty))
				return data
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			vb, writes, shardName, data, empty := shardedDrainVolume(t)
			damaged := tc.damage(t, vb, shardName, data, empty)

			for attempt := range 2 {
				err := vb.DrainToBackendCtx(t.Context())
				assert.Error(t, err, "attempt %d", attempt)
				if tc.integrity {
					assert.ErrorIs(t, err, ErrIntegrity, "attempt %d", attempt)
				}
				checkWALRetained(t, shardName, damaged)
			}
			assert.Zero(t, writes.count.Load(), "a refused generation must not upload chunks")
		})
	}
}
