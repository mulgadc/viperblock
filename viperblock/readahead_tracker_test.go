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

func TestReadAheadTracker_AStreamSurvivesAnUnrelatedReadBetween(t *testing.T) {
	tr := newTracker()
	assert.False(t, tr.read(100, 1), "a first read is not a stream")
	assert.True(t, tr.read(101, 1), "the latest position widens, as with one position")
	assert.False(t, tr.read(900, 1))
	assert.True(t, tr.read(102, 1), "an unrelated read ended the stream at 100")
}

func TestReadAheadTracker_ALandingOnAnOlderReadIsNotWidened(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	tr.read(900, 1)
	assert.False(t, tr.read(101, 1), "a read where an older single read ended was widened")
	assert.True(t, tr.read(102, 1), "once continued, the position is a stream")
}

func TestReadAheadTracker_APositionIsDroppedAfterFourNewerReads(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	tr.read(101, 1)
	for i := range uint64(readAheadStreams - 1) {
		tr.read(1000+10*i, 1)
	}
	assert.True(t, tr.read(102, 1), "three newer reads dropped the stream")
	for i := range uint64(readAheadStreams) {
		tr.read(2000+10*i, 1)
	}
	_, ticket := tr.begin(103, 1, readAheadBlocks, true)
	assert.False(t, ticket.continued, "the stream outlived four newer reads")
	tr.end(ticket)
}

func TestReadAheadTracker_ACacheHitOnlyAdvancesAPosition(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	tr.read(101, 1)
	for i := range uint64(100) {
		widen, ticket := tr.begin(1000+10*i, 1, readAheadBlocks, false)
		assert.False(t, widen)
		assert.Negative(t, ticket.slot, "a cache hit took a position")
		tr.end(ticket)
	}
	_, ticket := tr.begin(102, 1, readAheadBlocks, false)
	assert.True(t, ticket.continued, "a cache hit did not advance the stream")
	tr.end(ticket)
	assert.True(t, tr.read(103, 1), "cache hits pushed the stream out")
}

func TestReadAheadTracker_ACacheHitBetweenLeavesTheLatestPosition(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	_, hit := tr.begin(900, 1, readAheadBlocks, false)
	tr.end(hit)
	assert.True(t, tr.read(101, 1), "an unrecorded cache hit stopped 100 being the latest position")
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

func TestReadAheadTracker_AReusedSlotIsNotReleasedByAnOldTicket(t *testing.T) {
	tr := newTracker()
	tickets := make([]readAheadTicket, 0, readAheadStreams)
	for i := range uint64(readAheadStreams) {
		_, ticket := tr.begin(1000+10*i, 1, readAheadBlocks, true)
		tickets = append(tickets, ticket)
	}
	// Every slot is served, so the next read reuses the oldest.
	_, reuse := tr.begin(7000, 1, readAheadBlocks, true)
	require.Equal(t, tickets[0].slot, reuse.slot)
	tr.end(tickets[0])
	assert.Equal(t, 1, tr.streams[reuse.slot].serving, "an old ticket released a reused slot")
	tr.end(reuse)
	assert.Zero(t, tr.streams[reuse.slot].serving)
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

// TestReadahead_AnInheritedCacheHitRecordsNothing re-reads a cached inherited
// block that continues no position, and checks the tracker is unchanged.
func TestReadahead_AnInheritedCacheHitRecordsNothing(t *testing.T) {
	_, child, _ := createInheritedCacheFixture(t)
	bs := uint64(child.BlockSize)
	require.NoError(t, child.SetCacheSize(64, 0))
	dst := make([]byte, bs)
	require.NoError(t, child.ReadAtInto(0, dst))
	require.NoError(t, child.ReadAtInto(2*bs, dst))

	before := child.readAhead.snapshot()
	require.NoError(t, child.ReadAtInto(0, dst))
	assert.Equal(t, before, child.readAhead.snapshot(), "a cached inherited read changed the tracker")
}

type trackerState struct {
	streams [readAheadStreams]readAheadStream
	tick    uint64
}

func (t *readAheadTracker) snapshot() trackerState {
	t.mu.Lock()
	defer t.mu.Unlock()
	return trackerState{t.streams, t.tick}
}
