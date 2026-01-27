// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package viperblock

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

func TestBlockStore_NewUnifiedBlockStore(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)
	if bs == nil {
		t.Fatal("NewUnifiedBlockStore returned nil")
	}

	if bs.blockSize != 4096 {
		t.Errorf("expected blockSize 4096, got %d", bs.blockSize)
	}

	// Verify all shards are initialized
	for i := 0; i < NumShards; i++ {
		if bs.shards[i] == nil {
			t.Errorf("shard %d is nil", i)
		}
		if bs.shards[i].entries == nil {
			t.Errorf("shard %d entries map is nil", i)
		}
	}
}

func TestBlockStore_ReadWrite(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	// Write a block
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}

	seqNum := bs.Write(100, data)
	if seqNum != 1 {
		t.Errorf("expected seqNum 1, got %d", seqNum)
	}

	// Read it back
	readData, state, err := bs.ReadSingle(100)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if state != BlockStateHot {
		t.Errorf("expected state Hot, got %s", state.String())
	}
	if !bytes.Equal(readData, data) {
		t.Error("read data doesn't match written data")
	}
}

func TestBlockStore_ReadNonExistent(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	_, state, err := bs.ReadSingle(999)
	if err != ErrZeroBlock {
		t.Errorf("expected ErrZeroBlock, got %v", err)
	}
	if state != BlockStateEmpty {
		t.Errorf("expected state Empty, got %s", state.String())
	}
}

func TestBlockStore_StateTransitions(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	data := make([]byte, 4096)
	data[0] = 0xAB

	// Write block -> Hot state
	bs.Write(50, data)
	entry, ok := bs.ReadBlock(50)
	if !ok {
		t.Fatal("block not found after write")
	}
	if entry.State != BlockStateHot {
		t.Errorf("expected Hot, got %s", entry.State.String())
	}

	// Hot -> Pending
	if !bs.MarkPending(50) {
		t.Error("MarkPending failed")
	}
	entry, _ = bs.ReadBlock(50)
	if entry.State != BlockStatePending {
		t.Errorf("expected Pending, got %s", entry.State.String())
	}

	// Pending -> Persisted
	if !bs.MarkPersisted(50, 1, 1024) {
		t.Error("MarkPersisted failed")
	}
	entry, _ = bs.ReadBlock(50)
	if entry.State != BlockStatePersisted {
		t.Errorf("expected Persisted, got %s", entry.State.String())
	}
	if entry.ObjectID != 1 {
		t.Errorf("expected ObjectID 1, got %d", entry.ObjectID)
	}
	if entry.ObjectOffset != 1024 {
		t.Errorf("expected ObjectOffset 1024, got %d", entry.ObjectOffset)
	}
	if entry.Data != nil {
		t.Error("Data should be nil for Persisted state")
	}

	// Persisted -> Cached
	cacheData := make([]byte, 4096)
	cacheData[0] = 0xCD
	if !bs.Cache(50, cacheData) {
		t.Error("Cache failed")
	}
	entry, _ = bs.ReadBlock(50)
	if entry.State != BlockStateCached {
		t.Errorf("expected Cached, got %s", entry.State.String())
	}
	if !bytes.Equal(entry.Data, cacheData) {
		t.Error("cached data mismatch")
	}

	// Cached -> Persisted (eviction)
	if !bs.EvictCache(50) {
		t.Error("EvictCache failed")
	}
	entry, _ = bs.ReadBlock(50)
	if entry.State != BlockStatePersisted {
		t.Errorf("expected Persisted after eviction, got %s", entry.State.String())
	}
}

func TestBlockStore_OverwriteWithHigherSeqNum(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	data1 := make([]byte, 4096)
	data1[0] = 0x11
	bs.Write(100, data1)

	data2 := make([]byte, 4096)
	data2[0] = 0x22
	bs.Write(100, data2)

	// Should have newer data
	readData, _, _ := bs.ReadSingle(100)
	if readData[0] != 0x22 {
		t.Errorf("expected 0x22, got 0x%02x", readData[0])
	}
}

func TestBlockStore_ConcurrentAccess(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	const numGoroutines = 100
	const numOpsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < numOpsPerGoroutine; i++ {
				blockNum := uint64(goroutineID*numOpsPerGoroutine + i)

				// Write
				data := make([]byte, 4096)
				data[0] = byte(goroutineID)
				data[1] = byte(i)
				bs.Write(blockNum, data)

				// Read
				readData, state, _ := bs.ReadSingle(blockNum)
				if state == BlockStateHot && readData[0] != byte(goroutineID) {
					t.Errorf("data corruption detected")
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify count
	count := bs.Count()
	expectedCount := numGoroutines * numOpsPerGoroutine
	if count != expectedCount {
		t.Errorf("expected %d blocks, got %d", expectedCount, count)
	}
}

func TestBlockStore_ShardDistribution(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	// Write 1600 blocks (100 per shard if evenly distributed)
	const numBlocks = 1600
	data := make([]byte, 4096)

	for i := uint64(0); i < numBlocks; i++ {
		bs.Write(i, data)
	}

	// Count blocks per shard
	shardCounts := make([]int, NumShards)
	for i := 0; i < NumShards; i++ {
		shard := bs.shards[i]
		shard.mu.RLock()
		shardCounts[i] = len(shard.entries)
		shard.mu.RUnlock()
	}

	// Each shard should have exactly numBlocks/NumShards blocks
	expected := numBlocks / NumShards
	for i, count := range shardCounts {
		if count != expected {
			t.Errorf("shard %d has %d blocks, expected %d", i, count, expected)
		}
	}
}

func TestBlockStore_GetBlocksByState(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	data := make([]byte, 4096)

	// Create 10 Hot blocks
	for i := uint64(0); i < 10; i++ {
		bs.Write(i, data)
	}

	// Move 5 to Pending
	for i := uint64(0); i < 5; i++ {
		bs.MarkPending(i)
	}

	hotBlocks := bs.GetBlocksByState(BlockStateHot)
	if len(hotBlocks) != 5 {
		t.Errorf("expected 5 hot blocks, got %d", len(hotBlocks))
	}

	pendingBlocks := bs.GetBlocksByState(BlockStatePending)
	if len(pendingBlocks) != 5 {
		t.Errorf("expected 5 pending blocks, got %d", len(pendingBlocks))
	}
}

func TestBlockStore_GetHotBlocks(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	// Write some blocks
	for i := uint64(0); i < 10; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i)
		bs.Write(i, data)
	}

	// Mark some as pending
	for i := uint64(0); i < 5; i++ {
		bs.MarkPending(i)
	}

	hotBlocks := bs.GetHotBlocks()
	if len(hotBlocks) != 5 {
		t.Errorf("expected 5 hot blocks, got %d", len(hotBlocks))
	}

	// Verify data integrity
	for _, block := range hotBlocks {
		if block.Data[0] != byte(block.Block) {
			t.Errorf("data mismatch for block %d", block.Block)
		}
	}
}

func TestBlockStore_SetPersisted(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	// Set persisted directly (simulating checkpoint load)
	bs.SetPersisted(100, 5, 2048, 42)

	entry, ok := bs.ReadBlock(100)
	if !ok {
		t.Fatal("block not found")
	}
	if entry.State != BlockStatePersisted {
		t.Errorf("expected Persisted, got %s", entry.State.String())
	}
	if entry.ObjectID != 5 {
		t.Errorf("expected ObjectID 5, got %d", entry.ObjectID)
	}
	if entry.ObjectOffset != 2048 {
		t.Errorf("expected ObjectOffset 2048, got %d", entry.ObjectOffset)
	}
	if entry.SeqNum != 42 {
		t.Errorf("expected SeqNum 42, got %d", entry.SeqNum)
	}
}

func TestBlockStore_GetPersistedInfo(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	// Non-existent block
	_, _, ok := bs.GetPersistedInfo(999)
	if ok {
		t.Error("expected ok=false for non-existent block")
	}

	// Set a persisted block
	bs.SetPersisted(100, 7, 4096, 1)

	objectID, offset, ok := bs.GetPersistedInfo(100)
	if !ok {
		t.Error("expected ok=true for persisted block")
	}
	if objectID != 7 || offset != 4096 {
		t.Errorf("expected (7, 4096), got (%d, %d)", objectID, offset)
	}
}

func TestBlockStore_ClearAndDelete(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	data := make([]byte, 4096)
	for i := uint64(0); i < 100; i++ {
		bs.Write(i, data)
	}

	// Delete specific block
	if !bs.Delete(50) {
		t.Error("Delete returned false for existing block")
	}
	if bs.Delete(50) {
		t.Error("Delete returned true for already-deleted block")
	}

	if bs.Count() != 99 {
		t.Errorf("expected 99 blocks after delete, got %d", bs.Count())
	}

	// Clear all
	bs.Clear()
	if bs.Count() != 0 {
		t.Errorf("expected 0 blocks after clear, got %d", bs.Count())
	}
}

func TestBlockStore_CountByState(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	data := make([]byte, 4096)

	// Create mixed states
	for i := uint64(0); i < 30; i++ {
		bs.Write(i, data)
	}

	// 10 stay Hot, 10 go Pending, 10 go Persisted
	for i := uint64(10); i < 20; i++ {
		bs.MarkPending(i)
	}
	for i := uint64(20); i < 30; i++ {
		bs.MarkPending(i)
		bs.MarkPersisted(i, 0, 0)
	}

	counts := bs.CountByState()
	if counts[BlockStateHot] != 10 {
		t.Errorf("expected 10 Hot, got %d", counts[BlockStateHot])
	}
	if counts[BlockStatePending] != 10 {
		t.Errorf("expected 10 Pending, got %d", counts[BlockStatePending])
	}
	if counts[BlockStatePersisted] != 10 {
		t.Errorf("expected 10 Persisted, got %d", counts[BlockStatePersisted])
	}
}

func TestBlockStore_WriteWithSeqNum(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	data := make([]byte, 4096)
	data[0] = 0xAA

	// Write with specific seq num
	bs.WriteWithSeqNum(100, data, 50)

	entry, ok := bs.ReadBlock(100)
	if !ok {
		t.Fatal("block not found")
	}
	if entry.SeqNum != 50 {
		t.Errorf("expected SeqNum 50, got %d", entry.SeqNum)
	}

	// Write with lower seq num should be ignored
	data2 := make([]byte, 4096)
	data2[0] = 0xBB
	bs.WriteWithSeqNum(100, data2, 25)

	entry, _ = bs.ReadBlock(100)
	if entry.Data[0] != 0xAA {
		t.Error("lower seq num write should have been ignored")
	}

	// Write with higher seq num should succeed
	data3 := make([]byte, 4096)
	data3[0] = 0xCC
	bs.WriteWithSeqNum(100, data3, 100)

	entry, _ = bs.ReadBlock(100)
	if entry.Data[0] != 0xCC {
		t.Error("higher seq num write should have succeeded")
	}
}

func TestBlockStore_SeqNumIncrement(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	data := make([]byte, 4096)

	seq1 := bs.Write(0, data)
	seq2 := bs.Write(1, data)
	seq3 := bs.Write(2, data)

	if seq1 != 1 || seq2 != 2 || seq3 != 3 {
		t.Errorf("expected seq nums 1,2,3, got %d,%d,%d", seq1, seq2, seq3)
	}

	if bs.GetSeqNum() != 3 {
		t.Errorf("expected GetSeqNum()=3, got %d", bs.GetSeqNum())
	}
}

func TestBlockStore_SetSeqNum(t *testing.T) {
	bs := NewUnifiedBlockStore(4096)

	bs.SetSeqNum(1000)
	if bs.GetSeqNum() != 1000 {
		t.Errorf("expected GetSeqNum()=1000, got %d", bs.GetSeqNum())
	}

	data := make([]byte, 4096)
	seq := bs.Write(0, data)
	if seq != 1001 {
		t.Errorf("expected seq 1001, got %d", seq)
	}
}

func TestBlockState_String(t *testing.T) {
	tests := []struct {
		state    BlockState
		expected string
	}{
		{BlockStateEmpty, "Empty"},
		{BlockStateHot, "Hot"},
		{BlockStatePending, "Pending"},
		{BlockStatePersisted, "Persisted"},
		{BlockStateCached, "Cached"},
		{BlockState(99), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("BlockState(%d).String() = %s, expected %s", tt.state, got, tt.expected)
		}
	}
}

// Benchmarks

func BenchmarkBlockStore_SingleRead(b *testing.B) {
	bs := NewUnifiedBlockStore(4096)
	data := make([]byte, 4096)

	// Pre-populate with blocks
	for i := uint64(0); i < 10000; i++ {
		bs.Write(i, data)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		blockNum := uint64(i % 10000)
		bs.ReadSingle(blockNum)
	}
}

func BenchmarkBlockStore_SingleWrite(b *testing.B) {
	bs := NewUnifiedBlockStore(4096)
	data := make([]byte, 4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		bs.Write(uint64(i), data)
	}
}

func BenchmarkBlockStore_ConcurrentReadWrite(b *testing.B) {
	bs := NewUnifiedBlockStore(4096)
	data := make([]byte, 4096)

	// Pre-populate
	for i := uint64(0); i < 10000; i++ {
		bs.Write(i, data)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		localData := make([]byte, 4096)
		i := 0
		for pb.Next() {
			blockNum := uint64(i % 10000)
			if i%2 == 0 {
				bs.ReadSingle(blockNum)
			} else {
				bs.Write(blockNum, localData)
			}
			i++
		}
	})
}

func BenchmarkBlockStore_ConcurrentRead(b *testing.B) {
	bs := NewUnifiedBlockStore(4096)
	data := make([]byte, 4096)

	// Pre-populate
	for i := uint64(0); i < 10000; i++ {
		bs.Write(i, data)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			blockNum := uint64(i % 10000)
			bs.ReadSingle(blockNum)
			i++
		}
	})
}

func BenchmarkBlockStore_RandomReadWrite(b *testing.B) {
	bs := NewUnifiedBlockStore(4096)
	data := make([]byte, 4096)

	// Pre-populate
	for i := uint64(0); i < 10000; i++ {
		bs.Write(i, data)
	}

	b.ResetTimer()
	b.ReportAllocs()

	var counter atomic.Uint64

	b.RunParallel(func(pb *testing.PB) {
		localData := make([]byte, 4096)
		rng := rand.New(rand.NewSource(int64(counter.Add(1))))

		for pb.Next() {
			blockNum := uint64(rng.Intn(10000))
			if rng.Intn(10) < 3 { // 30% writes
				bs.Write(blockNum, localData)
			} else {
				bs.ReadSingle(blockNum)
			}
		}
	})
}

func BenchmarkBlockStore_StateTransition(b *testing.B) {
	bs := NewUnifiedBlockStore(4096)
	data := make([]byte, 4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		blockNum := uint64(i)
		bs.Write(blockNum, data)
		bs.MarkPending(blockNum)
		bs.MarkPersisted(blockNum, uint64(i), uint32(i*4096))
		bs.Cache(blockNum, data)
		bs.EvictCache(blockNum)
	}
}

// Arena benchmarks

func BenchmarkArena_Alloc(b *testing.B) {
	arena := NewArena(4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		arena.Alloc()
	}
}

func BenchmarkArena_AllocCopy(b *testing.B) {
	arena := NewArena(4096)
	data := make([]byte, 4096)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		arena.AllocCopy(data)
	}
}

func BenchmarkMakeSlice(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = make([]byte, 4096)
	}
}

// Comparison benchmark: Old map rebuild vs new O(1) lookup
func BenchmarkOldVsNew_MapRebuild(b *testing.B) {
	// Simulate the old approach: rebuilding map on each read
	blocks := make([]Block, 1000)
	for i := range blocks {
		blocks[i] = Block{
			SeqNum: uint64(i),
			Block:  uint64(i),
			Data:   make([]byte, 4096),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Rebuild map (this is what the old code did on every read)
		latestWrites := make(map[uint64]Block, len(blocks))
		for _, wr := range blocks {
			if prev, ok := latestWrites[wr.Block]; !ok || wr.SeqNum > prev.SeqNum {
				latestWrites[wr.Block] = wr
			}
		}
		// Lookup
		_ = latestWrites[500]
	}
}

func BenchmarkOldVsNew_UnifiedStore(b *testing.B) {
	// The new approach: O(1) lookup
	bs := NewUnifiedBlockStore(4096)
	data := make([]byte, 4096)
	for i := uint64(0); i < 1000; i++ {
		bs.Write(i, data)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		bs.ReadSingle(500)
	}
}
