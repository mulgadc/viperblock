package viperblock

import (
	"math/bits"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	maxCacheShards     = 64
	minBlocksPerShard  = 256
	cacheShardHashMult = 0x9E3779B97F4A7C15
)

// shardedLRU splits the block cache so concurrent readers of different blocks
// do not contend on one lock. Recency is kept per shard, so eviction is
// approximately LRU across the cache; the shard sizes sum to the request.
type shardedLRU struct {
	shards []*lru.Cache[uint64, cachedBlock]
	shift  uint
}

func newShardedLRU(size int) (*shardedLRU, error) {
	n := 1
	for n*2 <= maxCacheShards && n*2*minBlocksPerShard <= size {
		n *= 2
	}
	s := &shardedLRU{shards: make([]*lru.Cache[uint64, cachedBlock], n), shift: uint(64 - bits.TrailingZeros(uint(n)))}
	for i := range s.shards {
		per := size / n
		if i < size%n {
			per++
		}
		c, err := lru.New[uint64, cachedBlock](per)
		if err != nil {
			return nil, err
		}
		s.shards[i] = c
	}
	return s, nil
}

func (s *shardedLRU) shardIndex(block uint64) int {
	if len(s.shards) == 1 {
		return 0
	}
	return int((block * cacheShardHashMult) >> s.shift) //nolint:gosec // G115: shift >= 58, so the index is below 64
}

func (s *shardedLRU) shard(block uint64) *lru.Cache[uint64, cachedBlock] {
	return s.shards[s.shardIndex(block)]
}

func (s *shardedLRU) Get(block uint64) (cachedBlock, bool) { return s.shard(block).Get(block) }

func (s *shardedLRU) Add(block uint64, cb cachedBlock) bool { return s.shard(block).Add(block, cb) }

func (s *shardedLRU) Purge() {
	for _, c := range s.shards {
		c.Purge()
	}
}

func (s *shardedLRU) Len() int {
	n := 0
	for _, c := range s.shards {
		n += c.Len()
	}
	return n
}
