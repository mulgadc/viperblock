package viperblock

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShardedWAL_TornRecordDropsPostTearRecords reproduces the defect this
// plan closes. A record torn on disk (the state a short write leaves behind)
// makes both readers stop at the tear, so every record appended after it --
// which Flush reported durable -- is silently dropped by consolidation and by
// recovery. The single-file WAL truncates back to the last record boundary on
// a short write; the sharded WAL does not.
func TestShardedWAL_TornRecordDropsPostTearRecords(t *testing.T) {
	vb, cleanup := setupShardedTestVB(t)
	defer cleanup()

	data := make([]byte, 4096)
	rand.Read(data)

	// A good record through the real append path, flushed.
	require.NoError(t, vb.WriteShardedWAL(Block{SeqNum: 1, Block: 0, Len: 4096, Data: data}))
	require.NoError(t, vb.Flush())

	// Simulate the on-disk state after a torn append: a partial record at the
	// end of the shard's file. 18 bytes is a header with a truncated length
	// field, which is what a crash mid-write leaves.
	walNum := vb.ShardedWAL.WallNum.Load()
	shardPath := filepath.Join(vb.ShardedWAL.BaseDir,
		types.GetShardedWALPath(vb.GetVolume(), walNum, 0))
	f, err := os.OpenFile(shardPath, os.O_APPEND|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	partial := make([]byte, 18)
	_, err = f.Write(partial)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Post-tear records through the real append path, in the SAME shard as
	// the tear (blocks 16 and 17 route to shard 0 via block & 0xF). Flush
	// reports them durable: the writes landed and the shard synced cleanly.
	require.NoError(t, vb.WriteShardedWAL(Block{SeqNum: 2, Block: 16, Len: 4096, Data: data}))
	require.NoError(t, vb.WriteShardedWAL(Block{SeqNum: 3, Block: 17, Len: 4096, Data: data}))
	require.NoError(t, vb.Flush())

	// Consolidate. The reader hits the tear inside shard 0.
	require.NoError(t, vb.WriteShardedWALToChunk(true),
		"consolidation must tolerate a torn record and keep the records after it")

	// Block 0's record precedes the tear and must survive.
	_, _, _, err = vb.LookupBlockToObject(0)
	require.NoError(t, err, "the record before the tear must survive consolidation")

	// Blocks 16 and 17 were reported durable by Flush after the tear. The map
	// must name them. Today it does not: they are dropped, silently.
	for _, b := range []uint64{16, 17} {
		_, _, _, err := vb.LookupBlockToObject(b)
		assert.NoError(t, err,
			"block %d was reported durable by Flush but is missing from the map after consolidation", b)
	}
}
