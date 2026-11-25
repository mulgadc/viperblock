package viperblock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fan-out a default block-store read may reach: per request, and across
// every request one volume is serving at once. Legacy reads stay serial.
const (
	fragRequestFetchBound = 8
	fragVolumeFetchBound  = 16
)

const (
	fragVolumeBytes = 16 << 20
	// fragBlocks are written evens then odds, one chunk each, so every block
	// in [0, fragBlocks) is a run of its own and neighbours alternate chunks.
	fragBlocks = 64
	// fragRequestBlocks is one request's span: 16 runs, more than one request
	// may fetch at once.
	fragRequestBlocks = 16
	// gateFallback ends a hold whose target is never reached, so a serial
	// reader fails the overlap assertion instead of hanging.
	gateFallback = 3 * time.Second
)

var errInjectedRun = errors.New("injected run failure")

type fragRequestKey struct{}

// fetchGate wraps a backend and holds each chunk payload read until target
// reads are in flight together, so overlap is shown by arrival, not timing.
// Header reads (offset 0, 4 bytes) pass through and are recorded per chunk.
type fetchGate struct {
	types.Backend

	mu          sync.Mutex
	target      int
	release     chan struct{}
	released    bool
	fellBack    bool
	inflight    int
	peak        int
	reqInflight map[int]int
	reqPeak     map[int]int
	starts      int
	ends        int
	returned    bool
	late        int
	arrived     chan struct{}

	headerReads   map[uint64]int
	headerOK      map[uint64]bool
	payloadByObj  map[uint64]int
	headerMisses  int
	failObject    uint64
	failOffset    uint32
	failErr       error
	truncate      bool
	corruptHeader map[uint64]bool
	payloads      []payloadRead
}

// payloadRead is one chunk payload request as the backend saw it.
type payloadRead struct {
	objectID uint64
	offset   uint32
	length   uint32
	// endsBefore is how many payload reads had finished when this one began.
	endsBefore int
}

var _ types.Backend = (*fetchGate)(nil)

func newFetchGate(inner types.Backend, target int) *fetchGate {
	return &fetchGate{
		Backend:       inner,
		target:        target,
		release:       make(chan struct{}),
		reqInflight:   map[int]int{},
		reqPeak:       map[int]int{},
		arrived:       make(chan struct{}, 1024),
		headerReads:   map[uint64]int{},
		headerOK:      map[uint64]bool{},
		payloadByObj:  map[uint64]int{},
		corruptHeader: map[uint64]bool{},
	}
}

func (g *fetchGate) openLocked() {
	if !g.released {
		g.released = true
		close(g.release)
	}
}

func (g *fetchGate) Read(fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	return g.ReadCtx(context.Background(), fileType, objectID, offset, length)
}

func (g *fetchGate) ReadCtx(ctx context.Context, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	if fileType != types.FileTypeChunk {
		return g.Backend.ReadCtx(ctx, fileType, objectID, offset, length)
	}
	if offset == 0 && length == 4 {
		return g.readHeader(ctx, objectID)
	}

	req, _ := ctx.Value(fragRequestKey{}).(int)
	g.mu.Lock()
	if g.returned {
		g.late++
	}
	if !g.headerOK[objectID] {
		g.headerMisses++
	}
	g.payloadByObj[objectID]++
	g.payloads = append(g.payloads, payloadRead{objectID, offset, length, g.ends})
	g.starts++
	g.inflight++
	g.peak = max(g.peak, g.inflight)
	g.reqInflight[req]++
	g.reqPeak[req] = max(g.reqPeak[req], g.reqInflight[req])
	if g.inflight >= g.target {
		g.openLocked()
	}
	fail := g.failErr != nil && objectID == g.failObject && offset == g.failOffset
	g.mu.Unlock()
	g.arrived <- struct{}{}

	defer func() {
		g.mu.Lock()
		g.inflight--
		g.reqInflight[req]--
		g.ends++
		g.mu.Unlock()
	}()

	if fail && !g.truncate {
		return nil, g.failErr
	}

	timer := time.NewTimer(gateFallback)
	defer timer.Stop()
	select {
	case <-g.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		g.mu.Lock()
		g.fellBack = true
		g.openLocked()
		g.mu.Unlock()
	}

	data, err := g.Backend.ReadCtx(ctx, fileType, objectID, offset, length)
	if err == nil && fail {
		data = data[:len(data)-1]
	}
	return data, err
}

func (g *fetchGate) readHeader(ctx context.Context, objectID uint64) ([]byte, error) {
	data, err := g.Backend.ReadCtx(ctx, types.FileTypeChunk, objectID, 0, 4)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.headerReads[objectID]++
	if err != nil {
		return nil, err
	}
	if g.corruptHeader[objectID] {
		return []byte("VBCH"), nil
	}
	g.headerOK[objectID] = true
	return data, nil
}

// reset clears the gate's counters and arms a new hold at target.
func (g *fetchGate) reset(target int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.target, g.peak, g.returned, g.late = target, 0, false, 0
	g.starts, g.ends, g.payloads = 0, 0, nil
	g.release, g.released, g.fellBack = make(chan struct{}), false, false
}

// markReturned records that the caller has its answer: a payload read
// arriving after this is a worker the read did not wait for.
func (g *fetchGate) markReturned() (inflight, starts, ends int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.returned = true
	return g.inflight, g.starts, g.ends
}

func (g *fetchGate) snapshot() (peak int, reqPeak map[int]int, fellBack bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.peak, maps.Clone(g.reqPeak), g.fellBack
}

// seedFragmentedVolume writes fragBlocks blocks as two chunks, evens then
// odds, and returns the backend root and the bytes of each block in order.
func seedFragmentedVolume(t *testing.T, volumeName string, key *masterkey.Key) (string, []byte) {
	t.Helper()
	var evens, odds []uint64
	for block := uint64(0); block < fragBlocks; block += 2 {
		evens, odds = append(evens, block), append(odds, block+1)
	}
	return seedVolumeChunks(t, volumeName, key, [][]uint64{evens, odds})
}

// seedVolumeChunks writes each list of blocks as one chunk, in order, and
// returns the backend root and the final bytes of each block.
func seedVolumeChunks(t *testing.T, volumeName string, key *masterkey.Key, chunks [][]uint64) (string, []byte) {
	t.Helper()

	root := t.TempDir()
	vbconfig := VB{
		VolumeName:      volumeName,
		VolumeSize:      fragVolumeBytes,
		BaseDir:         filepath.Join(root, "viperblock"),
		WALSyncInterval: -1,
		GCEnabled:       false,
		GCInterval:      -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},

		MasterKey:         key,
		EncryptionEnabled: key != nil,
	}
	writer, err := New(&vbconfig, FileBackend, file.FileConfig{
		VolumeName: volumeName,
		VolumeSize: fragVolumeBytes,
		BaseDir:    root,
	})
	require.NoError(t, err)
	require.NoError(t, writer.Backend.Init())
	require.NoError(t, writer.OpenWAL(&writer.WAL, filepath.Join(writer.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, writer.WAL.WallNum.Load(), writer.GetVolume()))))
	require.NoError(t, writer.OpenWAL(&writer.BlockToObjectWAL, filepath.Join(writer.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, writer.BlockToObjectWAL.WallNum.Load(), writer.GetVolume()))))

	bs := uint64(DefaultBlockSize)
	written := make([]byte, fragBlocks*bs)
	for _, blocks := range chunks {
		data := randomBlockData(uint64(len(blocks)))
		for i, block := range blocks {
			copy(written[block*bs:(block+1)*bs], data[uint64(i)*bs:uint64(i+1)*bs])
			require.NoError(t, writer.Write(block, written[block*bs:(block+1)*bs]))
		}
		require.NoError(t, writer.Flush())
		require.NoError(t, writer.WriteWALToChunk(true))
	}
	require.NoError(t, writer.Close())

	return root, written
}

// fragReader opens the seeded volume cold behind a fetchGate.
func fragReader(t *testing.T, root, volumeName string, key *masterkey.Key, cacheBlocks int, blockStore bool, target int) (*VB, *fetchGate) {
	t.Helper()

	vbconfig := VB{
		VolumeName:      volumeName,
		VolumeSize:      fragVolumeBytes,
		BaseDir:         filepath.Join(t.TempDir(), "viperblock"),
		WALSyncInterval: -1,
		GCEnabled:       false,
		GCInterval:      -1,
		Cache:           Cache{Config: CacheConfig{Size: cacheBlocks}},

		MasterKey:         key,
		EncryptionEnabled: key != nil,
	}
	vb, err := New(&vbconfig, FileBackend, file.FileConfig{
		VolumeName: volumeName,
		VolumeSize: fragVolumeBytes,
		BaseDir:    root,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, vb.RemoveLocalFiles()) })

	fb, ok := vb.Backend.(*file.Backend)
	require.True(t, ok)
	require.NoError(t, fb.Init())
	require.NoError(t, vb.LoadState())
	require.NoError(t, vb.LoadBlockState())
	vb.UseBlockStore = blockStore

	gate := newFetchGate(fb, target)
	vb.Backend = gate
	return vb, gate
}

func (vb *VB) cachedBlocks() int {
	if vb.Cache.lru == nil {
		return 0
	}
	return vb.Cache.lru.Len()
}

// locate returns the chunk and offset a block was persisted at.
func locate(t *testing.T, vb *VB, block uint64) (uint64, uint32) {
	t.Helper()
	objectID, offset, _, err := vb.LookupBlockToObject(block)
	require.NoError(t, err)
	return objectID, offset
}

type fragReadMode struct {
	name       string
	blockStore bool
	into       bool
}

var fragReadModes = []fragReadMode{
	{"blockstore", true, false},
	{"blockstore-into", true, true},
	{"legacy", false, false},
	{"legacy-into", false, true},
}

// fetchTarget is the gate target and expected peak for one request: the
// default block store fans out to the per-request bound, legacy stays serial.
func (m fragReadMode) fetchTarget() int {
	if m.blockStore {
		return fragRequestFetchBound
	}
	return 1
}

func (m fragReadMode) read(ctx context.Context, vb *VB, offset, length uint64) ([]byte, error) {
	if m.into {
		dst := make([]byte, length)
		if err := vb.ReadAtInto(offset, dst); err != nil {
			return nil, err
		}
		return dst, nil
	}
	return vb.ReadAtCtx(ctx, offset, length)
}

// TestFragmentedRead_OverlapsRunsOfOneRequest pins that a default block-store
// request fetches its runs together up to the per-request bound, that legacy
// reads stay serial, and that every block lands at its own position.
func TestFragmentedRead_OverlapsRunsOfOneRequest(t *testing.T) {
	for _, enc := range []bool{false, true} {
		var key *masterkey.Key
		name := "cleartext"
		if enc {
			key = testKey(t, 0x5a)
			name = "encrypted"
		}
		root, written := seedFragmentedVolume(t, "vol-frag-"+name, key)
		for _, mode := range fragReadModes {
			t.Run(name+"/"+mode.name, func(t *testing.T) {
				reader, gate := fragReader(t, root, "vol-frag-"+name, key, 0, mode.blockStore, mode.fetchTarget())
				bs := uint64(DefaultBlockSize)
				length := uint64(fragRequestBlocks) * bs

				got, err := mode.read(context.Background(), reader, 0, length)
				inflight, starts, ends := gate.markReturned()
				require.NoError(t, err)
				require.Equal(t, written[:length], got, "a run landed at the wrong position")
				assert.Zero(t, inflight, "the read returned with backend fetches still running")
				assert.Equal(t, starts, ends)
				assert.Equal(t, fragRequestBlocks, starts, "each block is its own run")

				peak, _, fellBack := gate.snapshot()
				assert.False(t, fellBack, "runs never reached the expected overlap, so the gate fell back")
				assert.Equal(t, mode.fetchTarget(), peak,
					"one request with %d runs should fetch %d at once", fragRequestBlocks, mode.fetchTarget())
				if enc {
					assert.Zero(t, gate.headerMisses, "a payload read started before its chunk header was checked")
				}
			})
		}
	}
}

// TestFragmentedRead_BoundIsSharedAcrossRequests pins that simultaneous
// requests to one volume share one fetch bound, rather than each bringing
// its own, and that the bound is returned for the next round.
func TestFragmentedRead_BoundIsSharedAcrossRequests(t *testing.T) {
	const requests = 4
	root, written := seedFragmentedVolume(t, "vol-frag-shared", nil)
	reader, gate := fragReader(t, root, "vol-frag-shared", nil, 0, true, fragVolumeFetchBound)
	bs := uint64(DefaultBlockSize)
	length := uint64(fragRequestBlocks) * bs

	for round := range 2 {
		if round > 0 {
			gate.mu.Lock()
			gate.release, gate.released, gate.peak = make(chan struct{}), false, 0
			gate.reqPeak = map[int]int{}
			gate.mu.Unlock()
		}
		var wg sync.WaitGroup
		errs := make([]error, requests)
		got := make([][]byte, requests)
		for r := range requests {
			wg.Go(func() {
				ctx := context.WithValue(context.Background(), fragRequestKey{}, r+1)
				got[r], errs[r] = reader.ReadAtCtx(ctx, uint64(r)*length, length)
			})
		}
		wg.Wait()

		for r := range requests {
			require.NoErrorf(t, errs[r], "round %d request %d", round, r)
			require.Equalf(t, written[uint64(r)*length:uint64(r+1)*length], got[r], "round %d request %d bytes", round, r)
		}
		peak, reqPeak, fellBack := gate.snapshot()
		assert.Falsef(t, fellBack, "round %d: the volume bound was never reached", round)
		assert.Equalf(t, fragVolumeFetchBound, peak,
			"round %d: %d requests of %d runs should share %d fetches", round, requests, fragRequestBlocks, fragVolumeFetchBound)
		for r, p := range reqPeak {
			assert.LessOrEqualf(t, p, fragRequestFetchBound, "round %d request %d exceeded the per-request bound", round, r)
		}
	}
}

// TestFragmentedRead_ColdEncryptedHeadersPrecedePayload pins that, with two
// requests touching the same cold chunks, no run is fetched before its chunk
// header has been checked. Duplicate header reads are logged, not asserted.
func TestFragmentedRead_ColdEncryptedHeadersPrecedePayload(t *testing.T) {
	key := testKey(t, 0x6b)
	root, written := seedFragmentedVolume(t, "vol-frag-hdr", key)
	reader, gate := fragReader(t, root, "vol-frag-hdr", key, 0, true, 2*fragRequestFetchBound)
	bs := uint64(DefaultBlockSize)
	length := uint64(fragRequestBlocks) * bs

	var wg sync.WaitGroup
	errs := make([]error, 2)
	got := make([][]byte, 2)
	for r := range 2 {
		wg.Go(func() {
			ctx := context.WithValue(context.Background(), fragRequestKey{}, r+1)
			got[r], errs[r] = reader.ReadAtCtx(ctx, uint64(r)*length, length)
		})
	}
	wg.Wait()

	for r := range 2 {
		require.NoError(t, errs[r])
		require.Equal(t, written[uint64(r)*length:uint64(r+1)*length], got[r])
	}
	gate.mu.Lock()
	misses, headers := gate.headerMisses, maps.Clone(gate.headerReads)
	gate.mu.Unlock()
	assert.Zero(t, misses, "a payload read started before its chunk header was checked")
	assert.Len(t, headers, 2, "the request spans exactly two chunks")
	t.Logf("header reads per chunk: %v", headers)

	peak, _, fellBack := gate.snapshot()
	assert.False(t, fellBack, "two cold encrypted requests never overlapped")
	assert.Equal(t, 2*fragRequestFetchBound, peak)
}

// TestFragmentedRead_BadHeaderFailsBeforeDecrypt pins that a chunk whose
// header check fails is never fetched for decryption, and the request fails.
func TestFragmentedRead_BadHeaderFailsBeforeDecrypt(t *testing.T) {
	key := testKey(t, 0x7c)
	root, _ := seedFragmentedVolume(t, "vol-frag-badhdr", key)
	for _, mode := range fragReadModes {
		t.Run(mode.name, func(t *testing.T) {
			reader, gate := fragReader(t, root, "vol-frag-badhdr", key, 32768, mode.blockStore, mode.fetchTarget())
			odd, _ := locate(t, reader, 1)
			gate.corruptHeader[odd] = true

			_, err := mode.read(context.Background(), reader, 0, uint64(fragRequestBlocks)*uint64(DefaultBlockSize))
			inflight, starts, ends := gate.markReturned()
			require.ErrorIs(t, err, ErrPreEncryptionFormat)
			assert.Zero(t, inflight)
			assert.Equal(t, starts, ends)
			gate.mu.Lock()
			assert.Zero(t, gate.payloadByObj[odd], "a run was fetched from a chunk whose header failed")
			gate.mu.Unlock()
			assert.Zero(t, reader.cachedBlocks(), "a failed request published cache entries")
		})
	}
}

// TestFragmentedRead_FailedRunFailsTheRequestAndDrains pins that a failing or
// short run fails the whole request, that the read waits for its other
// fetches before returning, publishes nothing, and leaves the bound whole.
func TestFragmentedRead_FailedRunFailsTheRequestAndDrains(t *testing.T) {
	root, written := seedFragmentedVolume(t, "vol-frag-fail", nil)
	cases := []struct {
		name     string
		err      error
		truncate bool
		want     error
	}{
		{"error", errInjectedRun, false, errInjectedRun},
		{"short", errInjectedRun, true, types.ErrShortRead},
	}
	for _, tc := range cases {
		for _, mode := range fragReadModes {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				reader, gate := fragReader(t, root, "vol-frag-fail", nil, 32768, mode.blockStore, mode.fetchTarget())
				bs := uint64(DefaultBlockSize)
				length := uint64(fragRequestBlocks) * bs
				gate.failObject, gate.failOffset = locate(t, reader, 5)
				gate.failErr, gate.truncate = tc.err, tc.truncate

				t.Run("drains", func(t *testing.T) {
					got, err := mode.read(context.Background(), reader, 0, length)
					inflight, starts, ends := gate.markReturned()
					require.ErrorIs(t, err, tc.want)
					assert.Nil(t, got)
					assert.Zero(t, inflight, "the read returned with backend fetches still running")
					assert.Equal(t, starts, ends)
					assert.Zero(t, reader.cachedBlocks(), "a failed request published cache entries")
				})

				t.Run("bound-returned", func(t *testing.T) {
					gate.mu.Lock()
					gate.failErr, gate.returned, gate.peak = nil, false, 0
					gate.release, gate.released, gate.fellBack = make(chan struct{}), false, false
					gate.mu.Unlock()
					got, err := mode.read(context.Background(), reader, 0, length)
					require.NoError(t, err)
					require.Equal(t, written[:length], got)
					peak, _, fellBack := gate.snapshot()
					assert.False(t, fellBack, "the next request could not reach its expected overlap")
					assert.Equal(t, mode.fetchTarget(), peak)
				})

				gate.mu.Lock()
				late := gate.late
				gate.mu.Unlock()
				assert.Zero(t, late, "a fetch started after its request had returned")
			})
		}
	}
}

// TestFragmentedRead_CancellationDrainsAndReturnsTheBound pins that
// cancelling a request ends its held fetches, returns only after they end,
// publishes nothing, and leaves the bound whole for the next request.
func TestFragmentedRead_CancellationDrainsAndReturnsTheBound(t *testing.T) {
	root, written := seedFragmentedVolume(t, "vol-frag-cancel", nil)
	for _, mode := range []fragReadMode{fragReadModes[0], fragReadModes[2]} {
		t.Run(mode.name, func(t *testing.T) {
			reader, gate := fragReader(t, root, "vol-frag-cancel", nil, 32768, mode.blockStore, fragBlocks)
			bs := uint64(DefaultBlockSize)
			length := uint64(fragRequestBlocks) * bs

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				_, err := reader.ReadAtCtx(ctx, 0, length)
				done <- err
			}()
			<-gate.arrived
			cancel()
			err := <-done
			inflight, starts, ends := gate.markReturned()
			require.ErrorIs(t, err, context.Canceled)
			assert.Zero(t, inflight, "the read returned with backend fetches still running")
			assert.Equal(t, starts, ends)
			assert.Zero(t, reader.cachedBlocks(), "a cancelled request published cache entries")

			gate.mu.Lock()
			gate.returned, gate.peak, gate.target = false, 0, mode.fetchTarget()
			gate.release, gate.released, gate.fellBack = make(chan struct{}), false, false
			gate.mu.Unlock()
			got, err := reader.ReadAtCtx(context.Background(), 0, length)
			require.NoError(t, err)
			require.Equal(t, written[:length], got)
			peak, _, fellBack := gate.snapshot()
			assert.False(t, fellBack, "the bound was not returned after cancellation")
			assert.Equal(t, mode.fetchTarget(), peak)

			gate.mu.Lock()
			assert.Zero(t, gate.late, "a fetch started after its request had returned")
			gate.mu.Unlock()
		})
	}
}

// chunkCallCounter counts every chunk read, header or payload, on both the
// own-volume and the source-volume paths.
type chunkCallCounter struct {
	types.Backend
	calls atomic.Int64
}

var _ types.Backend = (*chunkCallCounter)(nil)

func (c *chunkCallCounter) ReadCtx(ctx context.Context, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	if fileType == types.FileTypeChunk {
		c.calls.Add(1)
	}
	return c.Backend.ReadCtx(ctx, fileType, objectID, offset, length)
}

func (c *chunkCallCounter) ReadFromCtx(ctx context.Context, volume string, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	if fileType == types.FileTypeChunk {
		c.calls.Add(1)
	}
	return c.Backend.ReadFromCtx(ctx, volume, fileType, objectID, offset, length)
}

// admissionRead is one cold read whose chunk fetches all need a permit.
type admissionRead struct {
	name string
	open func(t *testing.T) (vb *VB, length uint64, want []byte)
}

func admissionReads() []admissionRead {
	var reads []admissionRead
	for _, blockStore := range []bool{true, false} {
		reads = append(reads, admissionRead{
			name: fmt.Sprintf("own/blockstore=%v", blockStore),
			open: func(t *testing.T) (*VB, uint64, []byte) {
				key := testKey(t, 0x5c)
				root, written := seedFragmentedVolume(t, "vol-admit-own", key)
				vb, _ := fragReader(t, root, "vol-admit-own", key, 0, blockStore, 1)
				bs := uint64(DefaultBlockSize)
				return vb, bs, written[:bs]
			},
		})
		for _, blocks := range []uint64{1, 2} {
			reads = append(reads, admissionRead{
				name: fmt.Sprintf("inherited/blockstore=%v/blocks=%d", blockStore, blocks),
				open: func(t *testing.T) (*VB, uint64, []byte) {
					_, child, original := createInheritedCacheFixture(t)
					child.UseBlockStore = blockStore
					return child, blocks * uint64(child.BlockSize), bytes.Repeat(original, int(blocks))
				},
			})
		}
	}
	return reads
}

// TestFetchAdmission_DoneContextMakesNoBackendCall pins that a read whose
// context is done never reaches the backend, with permits free or all held,
// and that a read waiting on a full volume ends at its deadline.
func TestFetchAdmission_DoneContextMakesNoBackendCall(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()

	cases := []struct {
		name string
		held bool
		ctx  func() (context.Context, context.CancelFunc)
		want error
	}{
		{"canceled/free", false, func() (context.Context, context.CancelFunc) { return canceled, func() {} }, context.Canceled},
		{"canceled/held", true, func() (context.Context, context.CancelFunc) { return canceled, func() {} }, context.Canceled},
		{"expired/free", false, func() (context.Context, context.CancelFunc) { return expired, func() {} }, context.DeadlineExceeded},
		{"expired/held", true, func() (context.Context, context.CancelFunc) { return expired, func() {} }, context.DeadlineExceeded},
		{"expires-waiting/held", true, func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 50*time.Millisecond)
		}, context.DeadlineExceeded},
	}
	for _, read := range admissionReads() {
		for _, tc := range cases {
			t.Run(read.name+"/"+tc.name, func(t *testing.T) {
				vb, length, want := read.open(t)
				counter := &chunkCallCounter{Backend: vb.Backend}
				vb.Backend = counter

				if tc.held {
					for range fragVolumeFetchBound {
						require.NoError(t, vb.fetchPermits.acquire(context.Background()))
					}
				}
				ctx, done := tc.ctx()
				_, err := vb.ReadAtCtx(ctx, 0, length)
				done()
				require.ErrorIs(t, err, tc.want)
				assert.Zero(t, counter.calls.Load(), "a read with a done context reached the backend")
				if tc.held {
					for range fragVolumeFetchBound {
						vb.fetchPermits.release()
					}
				}

				got, err := vb.ReadAtCtx(context.Background(), 0, length)
				require.NoError(t, err)
				require.Equal(t, want, got)
				assert.Positive(t, counter.calls.Load(), "the control read never reached the backend")
			})
		}
	}
}

// seedWidenableTail writes blocks 0-63 as one chunk, then rewrites odd
// blocks 1-15 to a second chunk. A read of blocks 1-16 then has 16 runs and
// its last run, block 16, can widen through block 63.
func seedWidenableTail(t *testing.T, volumeName string) (string, []byte) {
	t.Helper()
	all := make([]uint64, fragBlocks)
	for block := range all {
		all[block] = uint64(block)
	}
	var odds []uint64
	for block := uint64(1); block < 16; block += 2 {
		odds = append(odds, block)
	}
	return seedVolumeChunks(t, volumeName, nil, [][]uint64{all, odds})
}

// TestFragmentedRead_WidenedTailRunsAfterTheOtherRuns pins that a widened tail
// starts after the other runs end, is skipped with no readahead when an
// earlier run fails, and that a lone widened run is one backend read.
func TestFragmentedRead_WidenedTailRunsAfterTheOtherRuns(t *testing.T) {
	const volumeName = "vol-frag-tail"
	root, written := seedWidenableTail(t, volumeName)
	bs := uint64(DefaultBlockSize)
	const tailBlock, lastBlock = 16, fragBlocks - 1
	plainRuns := tailBlock - 1

	open := func(t *testing.T, mode fragReadMode, prime uint64) (*VB, *fetchGate) {
		t.Helper()
		reader, gate := fragReader(t, root, volumeName, nil, 256, mode.blockStore, 1)
		_, err := reader.ReadAtCtx(context.Background(), prime*bs, bs)
		require.NoError(t, err)
		gate.reset(mode.fetchTarget())
		return reader, gate
	}

	for _, mode := range []fragReadMode{fragReadModes[0], fragReadModes[2]} {
		t.Run(mode.name+"/succeeds", func(t *testing.T) {
			reader, gate := open(t, mode, 0)
			tailObject, tailOffset := locate(t, reader, tailBlock)
			got, err := reader.ReadAtCtx(context.Background(), bs, tailBlock*bs)
			require.NoError(t, err)
			require.Equal(t, written[bs:(tailBlock+1)*bs], got)

			gate.mu.Lock()
			defer gate.mu.Unlock()
			require.Len(t, gate.payloads, tailBlock)
			tail := gate.payloads[len(gate.payloads)-1]
			assert.Equal(t, payloadRead{tailObject, tailOffset, (lastBlock - tailBlock + 1) * reader.blockStride(), plainRuns}, tail,
				"the tail should widen to the chunk end and start after the other runs end")
			assert.False(t, gate.fellBack)
		})

		t.Run(mode.name+"/earlier-run-fails", func(t *testing.T) {
			reader, gate := open(t, mode, 0)
			tailObject, tailOffset := locate(t, reader, tailBlock)
			cachedBefore := reader.cachedBlocks()
			gate.mu.Lock()
			gate.failObject, gate.failOffset = locate(t, reader, 1)
			gate.failErr = errInjectedRun
			gate.mu.Unlock()

			got, err := reader.ReadAtCtx(context.Background(), bs, tailBlock*bs)
			inflight, starts, ends := gate.markReturned()
			require.ErrorIs(t, err, errInjectedRun)
			assert.Nil(t, got)
			assert.Zero(t, inflight, "the read returned with backend fetches still running")
			assert.Equal(t, starts, ends)
			assert.Equal(t, cachedBefore, reader.cachedBlocks(), "a failed request published cache entries")

			gate.mu.Lock()
			defer gate.mu.Unlock()
			for _, p := range gate.payloads {
				assert.False(t, p.objectID == tailObject && p.offset == tailOffset, "the widened tail ran after an earlier run failed")
			}
			assert.False(t, gate.fellBack)
		})

		t.Run(mode.name+"/single-widened-run", func(t *testing.T) {
			reader, gate := open(t, mode, tailBlock)
			object, offset := locate(t, reader, tailBlock+1)
			got, err := reader.ReadAtCtx(context.Background(), (tailBlock+1)*bs, bs)
			require.NoError(t, err)
			require.Equal(t, written[(tailBlock+1)*bs:(tailBlock+2)*bs], got)

			gate.mu.Lock()
			defer gate.mu.Unlock()
			assert.Equal(t, []payloadRead{{object, offset, (lastBlock - tailBlock) * reader.blockStride(), 0}}, gate.payloads)
		})
	}
}
