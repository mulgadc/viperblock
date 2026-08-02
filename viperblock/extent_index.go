package viperblock

import "github.com/tidwall/btree"

// extent is what the ordered index needs from a stored value: the half-open
// block range [start, end) it covers. Both block indexes key on the start
// block, and the index relies on stored ranges being disjoint.
type extent interface {
	start() uint64
	end() uint64
}

// extentIndex is an ordered index over disjoint block extents, keyed by start
// block. It answers the only two queries the storage layer asks -- "which
// extent covers block X" and "which extents overlap [s, e)" -- by seeking to a
// position rather than examining every entry, so cost tracks the number of
// extents actually touched rather than the number stored.
//
// A hash map cannot answer either query without a full scan, because a block
// in the interior of a coalesced run has no key of its own. That is what made
// both queries O(N) and, at a million extents, made a single 4 KiB lookup take
// tens of milliseconds.
//
// Not safe for concurrent use: callers hold the lock guarding their index.
type extentIndex[V extent] struct {
	t btree.Map[uint64, V]
}

// find returns the extent covering block, if one does. The only candidate is
// the extent with the greatest start key <= block, so a single descent
// answers it.
func (ix *extentIndex[V]) find(block uint64) (V, bool) {
	var (
		found V
		ok    bool
	)
	ix.t.Descend(block, func(_ uint64, v V) bool {
		if block < v.end() {
			found, ok = v, true
		}
		return false
	})
	return found, ok
}

// overlaps calls fn for each extent intersecting [start, end), in ascending
// order, stopping early if fn returns false.
//
// fn must not mutate the index: the walk holds a cursor into the tree. Callers
// that fracture collect first, then mutate.
func (ix *extentIndex[V]) overlaps(start, end uint64, fn func(V) bool) {
	if start >= end {
		return
	}

	// The extent immediately below start may still reach into the range, so
	// begin the ascent from there rather than from start itself.
	from := start
	ix.t.Descend(start, func(k uint64, _ V) bool {
		from = k
		return false
	})

	ix.t.Ascend(from, func(k uint64, v V) bool {
		if k >= end {
			return false
		}
		if v.end() <= start {
			return true // the predecessor stops short of the range
		}
		return fn(v)
	})
}

// collectOverlaps returns the extents intersecting [start, end), appended to
// dst. Separate from overlaps because fracturing mutates the index, which
// cannot be done during a walk; dst lets the caller reuse a scratch buffer.
func (ix *extentIndex[V]) collectOverlaps(dst []V, start, end uint64) []V {
	ix.overlaps(start, end, func(v V) bool {
		dst = append(dst, v)
		return true
	})
	return dst
}

// set inserts or replaces the extent starting at v.start().
func (ix *extentIndex[V]) set(v V) {
	ix.t.Set(v.start(), v)
}

// remove deletes the extent starting exactly at block.
func (ix *extentIndex[V]) remove(block uint64) {
	ix.t.Delete(block)
}

// len returns the number of extents held.
func (ix *extentIndex[V]) len() int {
	return ix.t.Len()
}

// scan visits every extent in ascending start order, stopping early if fn
// returns false.
func (ix *extentIndex[V]) scan(fn func(V) bool) {
	ix.t.Scan(func(_ uint64, v V) bool { return fn(v) })
}

// clear drops every extent.
func (ix *extentIndex[V]) clear() {
	ix.t.Clear()
}
