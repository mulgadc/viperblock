package viperblock

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardedLRUCapacityIsExact(t *testing.T) {
	for _, tc := range []struct{ size, shards int }{
		{1, 1}, {511, 1}, {512, 2}, {DefaultCacheBlocks, 64}, {DefaultCacheBlocks + 1, 64},
	} {
		s, err := newShardedLRU(tc.size)
		require.NoError(t, err)
		assert.Len(t, s.shards, tc.shards, "size %d", tc.size)

		for b := uint64(0); b < uint64(tc.size)*8; b++ {
			s.Add(b, cachedBlock{seqNum: b})
		}
		total := 0
		for _, c := range s.shards {
			total += c.Len()
		}
		assert.Equal(t, tc.size, total, "a full cache of size %d holds exactly that many blocks", tc.size)
	}
}

func TestShardedLRUEvictsWithinShard(t *testing.T) {
	s, err := newShardedLRU(1024)
	require.NoError(t, err)
	require.Len(t, s.shards, 4)

	// Collect 257 keys per shard from a bounded search, fill every shard, then
	// overfill shard 0 by one entry.
	byShard := make([][]uint64, 4)
	for b := range uint64(1 << 16) {
		if i := s.shardIndex(b); len(byShard[i]) < 257 {
			byShard[i] = append(byShard[i], b)
		}
	}
	for i := range byShard {
		require.Len(t, byShard[i], 257, "shard %d is reachable by enough keys", i)
		for _, b := range byShard[i][:256] {
			s.Add(b, cachedBlock{seqNum: b})
		}
	}
	s.Add(byShard[0][256], cachedBlock{seqNum: byShard[0][256]})

	_, ok := s.Get(byShard[0][0])
	assert.False(t, ok, "the least recent entry of the overfilled shard is evicted")
	for i := 1; i < 4; i++ {
		for _, b := range byShard[i][:256] {
			_, ok := s.Get(b)
			require.True(t, ok, "shard %d keeps block %d", i, b)
		}
	}
}

func TestShardedLRUSpreadsStrides(t *testing.T) {
	s, err := newShardedLRU(DefaultCacheBlocks)
	require.NoError(t, err)
	n := len(s.shards)
	for _, stride := range []uint64{1, 64, 256, 1024} {
		counts := make([]int, n)
		for i := uint64(0); i < uint64(n)*64; i++ {
			counts[s.shardIndex(i*stride)]++
		}
		for shard, c := range counts {
			assert.Positive(t, c, "stride %d reaches shard %d", stride, shard)
			assert.LessOrEqual(t, c, 2*64, "stride %d overloads shard %d", stride, shard)
		}
	}
}
