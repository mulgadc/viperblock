package viperblock

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pendingBlockSet returns the block numbers still buffered in Writes.Blocks,
// read under the same lock the write and flush paths take.
func pendingBlockSet(vb *VB) map[uint64]struct{} {
	vb.Writes.mu.Lock()
	defer vb.Writes.mu.Unlock()

	pending := make(map[uint64]struct{}, len(vb.Writes.Blocks))
	for _, blk := range vb.Writes.Blocks {
		pending[blk.Block] = struct{}{}
	}
	return pending
}

// TestFlushCoversWritesAckedBeforeIt pins the NBD flush contract against the
// parallel thread model: nbdkit dispatches Flush concurrently with in-flight
// PWrites, so a flush must cover every write that already returned to the
// guest, whatever else is in flight beside it.
func TestFlushCoversWritesAckedBeforeIt(t *testing.T) {
	vb, _ := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	const writers = 8
	const perWriter = 64

	// Disjoint blocks, each written exactly once, so a block leaving
	// Writes.Blocks can only mean it was flushed and never that a later
	// write to it happened to be flushed instead.
	var ackedMu sync.Mutex
	acked := make(map[uint64]struct{}, writers*perWriter)

	var writersWG sync.WaitGroup
	for w := range writers {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			for i := range perWriter {
				block := uint64(w*perWriter + i)
				data := make([]byte, blockSize)
				data[0] = byte(w)
				if err := vb.WriteAt(block*blockSize, data); err != nil {
					t.Errorf("write of block %d failed: %v", block, err)
					return
				}

				ackedMu.Lock()
				acked[block] = struct{}{}
				ackedMu.Unlock()
			}
		}(w)
	}

	// Flush alongside the writers, and after each flush assert every write
	// acked before it started has left the pending buffer.
	var flusherWG sync.WaitGroup
	flusherWG.Add(1)
	done := make(chan struct{})
	go func() {
		defer flusherWG.Done()
		for {
			select {
			case <-done:
				return
			default:
			}

			ackedMu.Lock()
			snapshot := make([]uint64, 0, len(acked))
			for block := range acked {
				snapshot = append(snapshot, block)
			}
			ackedMu.Unlock()

			if err := vb.Flush(); err != nil {
				t.Errorf("flush failed: %v", err)
				return
			}

			pending := pendingBlockSet(vb)
			for _, block := range snapshot {
				if _, ok := pending[block]; ok {
					t.Errorf("block %d was acked before this flush but is still buffered after it", block)
					return
				}
			}
		}
	}()

	writersWG.Wait()
	close(done)
	flusherWG.Wait()

	require.NoError(t, vb.Flush())
	assert.Empty(t, pendingBlockSet(vb), "a final flush must leave nothing buffered")

	// Every block reached the WAL file exactly once: header plus one record
	// per block, at the unencrypted record stride.
	walPath := fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))
	fi, err := os.Stat(walPath)
	require.NoError(t, err)
	wantSize := int64(vb.WALHeaderSize()) + int64(writers*perWriter)*int64(28+blockSize)
	assert.Equal(t, wantSize, fi.Size(), "WAL must hold exactly one record per acked write")
}
