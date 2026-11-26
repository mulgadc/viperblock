package viperblock

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTracker() *readAheadTracker {
	t := &readAheadTracker{}
	t.reset()
	return t
}

// read begins and ends one request, as a request served at once does.
func (t *readAheadTracker) read(block, n uint64) bool {
	widen, ticket := t.begin(block, n, readAheadBlocks)
	t.end(ticket)
	return widen
}

func TestReadAheadTracker_AStreamSurvivesAnUnrelatedReadBetween(t *testing.T) {
	tr := newTracker()
	assert.False(t, tr.read(100, 1), "a first read is not a stream")
	assert.True(t, tr.read(101, 1))
	assert.False(t, tr.read(900, 1))
	assert.True(t, tr.read(102, 1), "the stream at 100 ended after one unrelated read")
}

func TestReadAheadTracker_ALandingOnAnOlderReadIsNotAStream(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	tr.read(900, 1)
	assert.False(t, tr.read(101, 1), "a read where an older single read ended was widened")
	assert.True(t, tr.read(102, 1), "once continued, the position is a stream")
}

func TestReadAheadTracker_AStreamOutlivesManyUnrelatedReads(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	tr.read(101, 1)
	tr.read(102, 1)
	for i := range uint64(100) {
		tr.read(1000+10*i, 1)
	}
	assert.True(t, tr.read(103, 1), "unrelated reads evicted an established stream")
}

func TestReadAheadTracker_AnUnrelatedPositionIsDroppedAfterFourNewerReads(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)
	tr.read(101, 1)
	tr.read(500, 1)
	for i := range uint64(readAheadStreams - 2) {
		tr.read(1000+10*i, 1)
	}
	assert.False(t, tr.read(501, 1), "an older single read was taken as a stream")
	tr.read(600, 1)
	for i := range uint64(readAheadStreams) {
		tr.read(2000+10*i, 1)
	}
	_, ticket := tr.begin(601, 1, readAheadBlocks)
	assert.False(t, ticket.continued, "a single read's position outlived four newer ones")
	tr.end(ticket)
}

func TestReadAheadTracker_NoWideningWhileAStreamRequestIsServed(t *testing.T) {
	tr := newTracker()
	tr.read(100, 1)

	// The request that set the stream's position is still being served.
	_, first := tr.begin(101, 1, readAheadBlocks)
	widen, second := tr.begin(102, 1, readAheadBlocks)
	assert.False(t, widen, "widened while the stream's previous request was served")
	tr.end(second)
	tr.end(first)
	assert.True(t, tr.read(103, 1), "end did not release the stream")

	// Another stream's continuing request is being served.
	tr.read(5000, 1)
	_, other := tr.begin(5001, 1, readAheadBlocks)
	assert.False(t, tr.read(104, 1), "widened beside another stream's request")
	tr.end(other)
	assert.True(t, tr.read(105, 1))
	assert.Zero(t, tr.streaming)
}

func TestReadAheadTracker_AReusedSlotIsNotReleasedByAnOldTicket(t *testing.T) {
	tr := newTracker()
	_, held := tr.begin(100, 1, readAheadBlocks)
	for i := range uint64(readAheadStreams) {
		tr.read(1000+10*i, 1)
	}
	_, reuse := tr.begin(7000, 1, readAheadBlocks)
	slot := reuse.slot
	tr.end(held)
	assert.Equal(t, 1, tr.streams[slot].serving, "an old ticket released a reused slot")
	tr.end(reuse)
	assert.Zero(t, tr.streams[slot].serving)
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
