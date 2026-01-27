// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package viperblock

import (
	"sync"
	"sync/atomic"
)

// Arena is a bump-pointer memory allocator for write data.
// It reduces GC pressure by allocating blocks from large contiguous regions.
// When an arena slab is full, a new one is allocated. Old slabs are freed
// when all their blocks are released.
type Arena struct {
	slabSize  uint32
	blockSize uint32

	mu     sync.Mutex
	slabs  []*ArenaSlab
	active *ArenaSlab

	// Stats
	totalAllocs atomic.Uint64
	totalBytes  atomic.Uint64
}

// ArenaSlab is a contiguous memory region for block allocations
type ArenaSlab struct {
	data   []byte
	offset uint32
	refs   atomic.Int32 // Reference count for blocks in this slab
}

// DefaultSlabSize is 4MB - matches ObjBlockSize for efficiency
const DefaultSlabSize = 4 * 1024 * 1024

// NewArena creates a new arena allocator
func NewArena(blockSize uint32) *Arena {
	return NewArenaWithSlabSize(blockSize, DefaultSlabSize)
}

// NewArenaWithSlabSize creates an arena with a custom slab size
func NewArenaWithSlabSize(blockSize, slabSize uint32) *Arena {
	a := &Arena{
		slabSize:  slabSize,
		blockSize: blockSize,
		slabs:     make([]*ArenaSlab, 0, 16),
	}
	a.active = a.newSlab()
	return a
}

// newSlab allocates a new memory slab
func (a *Arena) newSlab() *ArenaSlab {
	slab := &ArenaSlab{
		data:   make([]byte, a.slabSize),
		offset: 0,
	}
	slab.refs.Store(1) // Start with ref count of 1
	a.slabs = append(a.slabs, slab)
	return slab
}

// Alloc allocates a block-sized chunk from the arena
// Returns the allocated slice. The slice is valid until Release is called.
func (a *Arena) Alloc() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if we need a new slab
	if a.active.offset+a.blockSize > a.slabSize {
		a.active = a.newSlab()
	}

	start := a.active.offset
	a.active.offset += a.blockSize
	a.active.refs.Add(1)

	a.totalAllocs.Add(1)
	a.totalBytes.Add(uint64(a.blockSize))

	return a.active.data[start : start+a.blockSize]
}

// AllocCopy allocates a block and copies data into it
func (a *Arena) AllocCopy(data []byte) []byte {
	buf := a.Alloc()
	copy(buf, data)
	return buf
}

// AllocN allocates n consecutive blocks from the arena
func (a *Arena) AllocN(n int) []byte {
	size := uint32(n) * a.blockSize

	a.mu.Lock()
	defer a.mu.Unlock()

	// If requested size exceeds slab size, allocate dedicated slab
	if size > a.slabSize {
		slab := &ArenaSlab{
			data:   make([]byte, size),
			offset: size,
		}
		slab.refs.Store(2) // 1 for existence + 1 for this allocation
		a.slabs = append(a.slabs, slab)
		a.totalAllocs.Add(1)
		a.totalBytes.Add(uint64(size))
		return slab.data
	}

	// Check if we need a new slab
	if a.active.offset+size > a.slabSize {
		a.active = a.newSlab()
	}

	start := a.active.offset
	a.active.offset += size
	a.active.refs.Add(1)

	a.totalAllocs.Add(1)
	a.totalBytes.Add(uint64(size))

	return a.active.data[start : start+size]
}

// Release decrements the reference count for a slab
// When ref count reaches zero, the slab can be reused
// Note: The caller must track which slab a buffer belongs to
func (a *Arena) Release(slab *ArenaSlab) {
	if slab == nil {
		return
	}
	slab.refs.Add(-1)
}

// Reset clears all slabs and starts fresh
// Only safe to call when no allocations are in use
func (a *Arena) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Clear all slabs
	a.slabs = a.slabs[:0]
	a.active = a.newSlab()
}

// Compact removes empty slabs (ref count <= 1)
// The active slab is never removed
func (a *Arena) Compact() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	removed := 0
	newSlabs := make([]*ArenaSlab, 0, len(a.slabs))

	for _, slab := range a.slabs {
		// Keep slab if it's active or has allocations (refs > 1)
		if slab == a.active || slab.refs.Load() > 1 {
			newSlabs = append(newSlabs, slab)
		} else {
			removed++
		}
	}

	a.slabs = newSlabs
	return removed
}

// Stats returns allocation statistics
func (a *Arena) Stats() (numSlabs int, totalAllocs, totalBytes uint64) {
	a.mu.Lock()
	numSlabs = len(a.slabs)
	a.mu.Unlock()

	return numSlabs, a.totalAllocs.Load(), a.totalBytes.Load()
}

// SlabCount returns the current number of slabs
func (a *Arena) SlabCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.slabs)
}

// ActiveSlabUsage returns how much of the active slab is used
func (a *Arena) ActiveSlabUsage() (used, total uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active == nil {
		return 0, a.slabSize
	}
	return a.active.offset, a.slabSize
}

// PooledArena wraps Arena with a sync.Pool for temporary allocations
// that are frequently allocated and released
type PooledArena struct {
	arena     *Arena
	blockSize uint32
	pool      sync.Pool
}

// NewPooledArena creates an arena with pooling for frequently reused blocks
func NewPooledArena(blockSize uint32) *PooledArena {
	pa := &PooledArena{
		arena:     NewArena(blockSize),
		blockSize: blockSize,
	}
	pa.pool.New = func() interface{} {
		return make([]byte, blockSize)
	}
	return pa
}

// Get returns a block from the pool
func (pa *PooledArena) Get() []byte {
	return pa.pool.Get().([]byte)
}

// Put returns a block to the pool
func (pa *PooledArena) Put(b []byte) {
	// Clear the buffer before returning to pool
	clear(b)
	pa.pool.Put(b)
}

// AllocPermanent allocates from the arena (not pooled)
func (pa *PooledArena) AllocPermanent() []byte {
	return pa.arena.Alloc()
}

// AllocCopyPermanent allocates and copies to arena (not pooled)
func (pa *PooledArena) AllocCopyPermanent(data []byte) []byte {
	return pa.arena.AllocCopy(data)
}
