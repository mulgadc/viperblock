package viperblock

// lookupMap materialises the block index as the flat map tests used to read
// directly. Only for assertions -- production code walks the index in order.
func lookupMap(b *BlocksToObject) map[uint64]BlockLookup {
	out := make(map[uint64]BlockLookup, b.lookup.len())
	b.lookup.scan(func(bl BlockLookup) bool {
		out[bl.StartBlock] = bl
		return true
	})
	return out
}
