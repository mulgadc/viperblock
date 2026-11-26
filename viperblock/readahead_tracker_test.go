package viperblock

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTracker() *readAheadTracker {
	t := &readAheadTracker{}
	t.reset()
	return t
}

// read begins and ends one request that made a backend read.
func (t *readAheadTracker) read(block, n uint64) bool {
	widen, ticket := t.begin(block, n, readAheadBlocks, true)
	t.end(ticket)
	return widen
}

func TestReadAheadTracker_AStreamSurvivesUnrelatedReadsBetween(t *testing.T) {
	tr := newTracker()
	assert.False(t, tr.read(100, 1), "a first read is not a stream")
	assert.True(t, tr.read(101, 1), "the latest position widens, as with one position")
	assert.True(t, tr.read(102, 1))
	for i := range uint64(100) {
		assert.False(t, tr.read(1000+10*i, 1))
	}
	assert.True(t, tr.read(103, 1), "unrelated reads ended the stream at 100")
}

func TestReadAheadTracker_ALandingOnAnOlderReadIsNotWidened(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	tr.read(900, 1)
	assert.False(t, tr.read(101, 1), "a read where an older single read ended was widened")
	assert.True(t, tr.read(102, 1), "the latest position widens")
}

// stream establishes a stream of three requests from block.
func (t *readAheadTracker) stream(block uint64) {
	for i := range uint64(3) {
		t.read(block+i, 1)
	}
}

func TestReadAheadTracker_AStreamIsDroppedAfterFourNewerStreams(t *testing.T) {
	tr := newTracker()
	tr.stream(100)
	for i := range uint64(readAheadStreams - 1) {
		tr.stream(1000 + 100*i)
	}
	assert.True(t, tr.read(103, 1), "three newer streams dropped the stream")
	for i := range uint64(readAheadStreams) {
		tr.stream(2000 + 100*i)
	}
	_, ticket := tr.begin(104, 1, readAheadBlocks, true)
	assert.False(t, ticket.continued, "the stream outlived four newer streams")
	tr.end(ticket)
}

func TestReadAheadTracker_ASingleReadIsDroppedWhenEveryCandidateIsNewer(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	for i := range uint64(readAheadCandidates - 1) {
		tr.read(1000+10*i, 1)
	}
	_, ticket := tr.begin(101, 1, readAheadBlocks, true)
	assert.True(t, ticket.continued, "fewer newer reads than candidates dropped the position")
	tr.end(ticket)
	tr.read(500, 1)
	for i := range uint64(readAheadCandidates) {
		tr.read(2000+10*i, 1)
	}
	_, ticket = tr.begin(501, 1, readAheadBlocks, true)
	assert.False(t, ticket.continued, "a single read's position outlived a full set of newer reads")
	tr.end(ticket)
}

func TestReadAheadTracker_ACacheHitOnlyAdvancesAPosition(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	tr.read(101, 1)
	for i := range uint64(100) {
		widen, ticket := tr.begin(1000+10*i, 1, readAheadBlocks, false)
		assert.False(t, widen)
		assert.Negative(t, ticket.tier, "a cache hit took a position")
		tr.end(ticket)
	}
	_, ticket := tr.begin(102, 1, readAheadBlocks, false)
	assert.True(t, ticket.continued, "a cache hit did not advance the position")
	tr.end(ticket)
	assert.True(t, tr.read(103, 1), "cache hits pushed the position out")
}

func TestReadAheadTracker_ACacheHitBetweenLeavesTheLatestPosition(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	_, hit := tr.begin(900, 1, readAheadBlocks, false)
	tr.end(hit)
	assert.True(t, tr.read(101, 1), "an unrecorded cache hit stopped 100 being the latest position")
}

func TestReadAheadTracker_StreamsAreReplacedByRecencyEvenWhileServed(t *testing.T) {
	tr := newTracker()
	tr.stream(100)
	_, held := tr.begin(103, 1, readAheadBlocks, true)
	for i := range uint64(readAheadStreams) {
		tr.stream(1000 + 100*i)
	}
	tr.end(held)
	_, ticket := tr.begin(104, 1, readAheadBlocks, true)
	assert.False(t, ticket.continued, "a served stream outlived four newer streams")
	tr.end(ticket)
	assert.Zero(t, tr.streaming)
}

// startBeside starts a stream at block with gap unrelated backend reads before
// each of its requests, and reports whether its fourth request widens.
func startBeside(gap int) bool {
	tr := newTracker()
	other := uint64(10000)
	widened := false
	for i := range uint64(4) {
		for range gap {
			tr.read(other, 1)
			other += 10
		}
		widened = tr.read(100+i, 1)
	}
	return widened
}

func TestReadAheadTracker_AStreamStartsBesideFewerUnrelatedReadsThanCandidates(t *testing.T) {
	assert.True(t, startBeside(readAheadCandidates-1), "unrelated reads between requests kept the stream from starting")
	assert.False(t, startBeside(readAheadCandidates), "the stream started with a full set of unrelated reads between requests")
}

func TestReadAheadTracker_NoWideningWhileAStreamRequestIsServed(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)

	// The request that set the stream's position is still being served.
	_, first := tr.begin(101, 1, readAheadBlocks, true)
	widen, second := tr.begin(102, 1, readAheadBlocks, true)
	assert.False(t, widen, "widened while the stream's previous request was served")
	tr.end(second)
	tr.end(first)
	assert.True(t, tr.read(103, 1), "end did not release the stream")

	// Another stream's continuing request is being served.
	tr.read(5000, 1)
	_, other := tr.begin(5001, 1, readAheadBlocks, true)
	assert.False(t, tr.read(104, 1), "widened beside another stream's request")
	tr.end(other)
	assert.True(t, tr.read(105, 1))
	assert.Zero(t, tr.streaming)
}

func TestReadAheadTracker_ARequestEndingAtTheSameBlockHoldsWidening(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	tr.read(101, 1)
	tr.read(102, 1)
	// A held read of 100 to 102 ends where the stream does.
	_, held := tr.begin(100, 3, readAheadBlocks, true)
	assert.False(t, tr.read(103, 1), "widened while a read ending at 103 was served")
	tr.end(held)
	assert.True(t, tr.read(104, 1))
}

func TestReadAheadTracker_AReusedSlotIsNotReleasedByAnOldTicket(t *testing.T) {
	tr := newTracker()
	tickets := make([]readAheadTicket, 0, readAheadCandidates)
	for i := range uint64(readAheadCandidates) {
		_, ticket := tr.begin(1000+10*i, 1, readAheadBlocks, true)
		tickets = append(tickets, ticket)
	}
	// Every candidate is served, so the next read reuses the oldest.
	_, reuse := tr.begin(7000, 1, readAheadBlocks, true)
	require.Equal(t, tickets[0].slot, reuse.slot)
	slot := &tr.slots(reuse.tier)[reuse.slot]
	tr.end(tickets[0])
	assert.Equal(t, 1, slot.serving, "an old ticket released a reused slot")
	tr.end(reuse)
	assert.Zero(t, slot.serving)
	for _, ticket := range tickets[1:] {
		tr.end(ticket)
	}
}

// failNextChunk fails the next chunk read once armed.
type failNextChunk struct {
	types.Backend

	armed atomic.Bool
}

var _ types.Backend = (*failNextChunk)(nil)

func (f *failNextChunk) ReadCtx(ctx context.Context, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	if fileType == types.FileTypeChunk && f.armed.CompareAndSwap(true, false) {
		return nil, errors.New("injected chunk read failure")
	}
	return f.Backend.ReadCtx(ctx, fileType, objectID, offset, length)
}

func TestReadahead_AFailedStreamReadReleasesTheStream(t *testing.T) {
	const volumeName = "vol-readahead-failed"
	root, written := seedSequentialVolume(t, volumeName, nil)
	reader, counter := coldReaderVB(t, root, volumeName, nil)
	failing := &failNextChunk{Backend: reader.Backend}
	reader.Backend = failing
	bs := uint64(DefaultBlockSize)

	_, err := reader.ReadAtCtx(context.Background(), 100*bs, bs)
	require.NoError(t, err)
	failing.armed.Store(true)
	_, err = reader.ReadAtCtx(context.Background(), 101*bs, bs)
	require.Error(t, err)

	before := counter.bytes.Load()
	data, err := reader.ReadAtCtx(context.Background(), 102*bs, bs)
	require.NoError(t, err)
	require.Equal(t, written[102*bs:103*bs], data)
	assert.Greater(t, counter.bytes.Load()-before, int64(bs), "the failed read left the stream held")
}

// holdInherited holds the first inherited payload read once armed, and counts
// the own volume's chunk payload bytes.
type holdInherited struct {
	types.Backend

	armed    atomic.Bool
	entered  chan struct{}
	release  chan struct{}
	ownBytes atomic.Int64
}

var _ types.Backend = (*holdInherited)(nil)

func (h *holdInherited) ReadFromCtx(ctx context.Context, volume string, typ types.FileType, object uint64, offset, length uint32) ([]byte, error) {
	if typ == types.FileTypeChunk && offset != 0 && h.armed.CompareAndSwap(true, false) {
		close(h.entered)
		<-h.release
	}
	return h.Backend.ReadFromCtx(ctx, volume, typ, object, offset, length)
}

func (h *holdInherited) ReadCtx(ctx context.Context, typ types.FileType, object uint64, offset, length uint32) ([]byte, error) {
	if typ == types.FileTypeChunk && offset != 0 {
		h.ownBytes.Add(int64(length))
	}
	return h.Backend.ReadCtx(ctx, typ, object, offset, length)
}

// TestReadahead_NoWideningWhileAnInheritedPredecessorIsServed reads own
// block 1, holds the read of inherited block 2 that continues it, and reads
// own block 3, which must not widen while block 2 is served.
func TestReadahead_NoWideningWhileAnInheritedPredecessorIsServed(t *testing.T) {
	for _, blockstore := range []bool{true, false} {
		t.Run(map[bool]string{true: "blockstore", false: "legacy"}[blockstore], func(t *testing.T) {
			_, child, original := createInheritedCacheFixture(t)
			bs := uint64(child.BlockSize)
			own := bytes.Repeat([]byte{0x6c}, int(33*bs))
			require.NoError(t, child.WriteAt(bs, own[:bs]))
			require.NoError(t, child.WriteAt(3*bs, own[bs:]))
			require.NoError(t, child.DrainToBackend())
			child.UseBlockStore = blockstore
			require.NoError(t, child.SetCacheSize(64, 0))
			hold := &holdInherited{Backend: child.Backend, entered: make(chan struct{}), release: make(chan struct{})}
			child.Backend = hold
			release := sync.OnceFunc(func() { close(hold.release) })
			defer release()

			dst := make([]byte, bs)
			require.NoError(t, child.ReadAtInto(bs, dst))
			hold.armed.Store(true)
			first := make(chan error, 1)
			held := make([]byte, bs)
			go func() { first <- child.ReadAtInto(2*bs, held) }()
			select {
			case <-hold.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the inherited read never reached the backend")
			}
			before := hold.ownBytes.Load()
			require.NoError(t, child.ReadAtInto(3*bs, dst))
			require.Equal(t, own[bs:2*bs], dst)
			assert.LessOrEqual(t, hold.ownBytes.Load()-before, int64(2*bs), "block 3 widened while block 2 was served")
			release()
			require.NoError(t, <-first)
			require.Equal(t, original, held)
		})
	}
}

// TestReadahead_AnOffTrackInheritedReadHoldsWidening streams own blocks 0 and 1
// and inherited block 2, holds a cold re-read of block 2, which starts off the
// stream and ends where it does, and reads own block 3, which must wait.
func TestReadahead_AnOffTrackInheritedReadHoldsWidening(t *testing.T) {
	for _, blockstore := range []bool{true, false} {
		t.Run(map[bool]string{true: "blockstore", false: "legacy"}[blockstore], func(t *testing.T) {
			_, child, original := createInheritedCacheFixture(t)
			bs := uint64(child.BlockSize)
			own := bytes.Repeat([]byte{0x6c}, int(34*bs))
			require.NoError(t, child.WriteAt(0, own[:2*bs]))
			require.NoError(t, child.WriteAt(3*bs, own[2*bs:]))
			require.NoError(t, child.DrainToBackend())
			child.UseBlockStore = blockstore
			require.NoError(t, child.SetCacheSize(64, 0))
			hold := &holdInherited{Backend: child.Backend, entered: make(chan struct{}), release: make(chan struct{})}
			child.Backend = hold
			release := sync.OnceFunc(func() { close(hold.release) })
			defer release()

			dst := make([]byte, bs)
			for block := range uint64(3) {
				require.NoError(t, child.ReadAtInto(block*bs, dst))
			}
			require.NoError(t, child.SetCacheSize(64, 0))
			hold.armed.Store(true)
			first := make(chan error, 1)
			held := make([]byte, bs)
			go func() { first <- child.ReadAtInto(2*bs, held) }()
			select {
			case <-hold.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the inherited read never reached the backend")
			}
			before := hold.ownBytes.Load()
			require.NoError(t, child.ReadAtInto(3*bs, dst))
			require.Equal(t, own[2*bs:3*bs], dst)
			assert.LessOrEqual(t, hold.ownBytes.Load()-before, int64(2*bs), "block 3 widened while a read ending there was served")
			release()
			require.NoError(t, <-first)
			require.Equal(t, original, held)
		})
	}
}
