package viperblock

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pendingWALChunksLen reads vb.pendingWALChunks under its mutex, so tests
// don't reach into the field without the lock the production code requires.
func pendingWALChunksLen(vb *VB) int {
	vb.pendingWALMu.Lock()
	defer vb.pendingWALMu.Unlock()
	return len(vb.pendingWALChunks)
}

// TestRetryPendingWALChunksUploadsStrandedGeneration pins the core fix: a WAL
// generation rotated out by a drain whose chunk upload failed is not lost --
// it is retried by a later drain, actually lands in the backend, and its
// data reads back correctly.
func TestRetryPendingWALChunksUploadsStrandedGeneration(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)
	data := bytes.Repeat([]byte{0xAB}, int(blockSize))

	require.NoError(t, vb.WriteAt(0, data))

	backend.full.Store(true)
	err := vb.DrainToBackendCtx(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoSpace, "drain error must classify as ErrNoSpace")
	require.True(t, vb.backendFull.Load())

	require.Equal(t, 1, pendingWALChunksLen(vb), "the generation whose upload failed must be tracked for retry")

	// Not yet resolvable via BlockLookup -- the chunk never reached the
	// backend, so nothing but the in-memory pending-writes buffer has it.
	_, _, _, lookupErr := vb.LookupBlockToObject(0)
	assert.ErrorIs(t, lookupErr, ErrZeroBlock, "block must not resolve via BlockLookup until its chunk actually lands")

	backend.full.Store(false)
	require.NoError(t, vb.DrainToBackendCtx(context.Background()), "drain must retry and land the stranded generation now the backend has recovered")

	assert.Zero(t, pendingWALChunksLen(vb), "pendingWALChunks must be drained once the retry succeeds")

	_, _, _, lookupErr = vb.LookupBlockToObject(0)
	assert.NoError(t, lookupErr, "block must resolve via BlockLookup once the stranded chunk lands")

	readBack, err := vb.ReadAt(0, blockSize)
	require.NoError(t, err)
	assert.Equal(t, data, readBack, "data from the stranded generation must read back correctly")
}

// TestBackendFullLatchHoldsAcrossEmptyRetryDrain pins that the backendFull
// latch does not flap while a stranded generation still fails: a second
// drain against a still-full backend, with no new guest writes, must retry
// the stranded generation and fail again -- not spuriously succeed because
// the freshly rotated current generation happens to be empty.
func TestBackendFullLatchHoldsAcrossEmptyRetryDrain(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	backend.full.Store(true)
	require.Error(t, vb.DrainToBackendCtx(context.Background()))
	require.True(t, vb.backendFull.Load())
	require.Equal(t, 1, pendingWALChunksLen(vb))

	err := vb.DrainToBackendCtx(context.Background())
	require.Error(t, err, "a drain with no new writes must still retry the stranded generation and fail")
	assert.ErrorIs(t, err, ErrNoSpace)
	assert.True(t, vb.backendFull.Load(), "backendFull must stay latched while the stranded generation still fails")
	assert.Equal(t, 1, pendingWALChunksLen(vb), "the stranded generation must remain pending after a failed retry")
}

// TestBackendFullLatchClearsOnlyAfterStrandedGenerationLands pins that the
// latch clears only once the previously stranded generation actually
// reaches the backend, not on an intervening empty drain.
func TestBackendFullLatchClearsOnlyAfterStrandedGenerationLands(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))

	backend.full.Store(true)
	require.Error(t, vb.DrainToBackendCtx(context.Background()))
	require.True(t, vb.backendFull.Load())

	require.Error(t, vb.DrainToBackendCtx(context.Background()), "a second drain against a still-full backend must fail too")
	require.True(t, vb.backendFull.Load(), "latch must still be set before the backend recovers")

	backend.full.Store(false)
	require.NoError(t, vb.DrainToBackendCtx(context.Background()), "drain must succeed once the backend recovers and the stranded generation lands")
	assert.False(t, vb.backendFull.Load(), "latch must clear once the stranded generation actually lands")
	assert.Zero(t, pendingWALChunksLen(vb), "pendingWALChunks must be empty once the recovery drain succeeds")
}

// TestPendingWALChunksEmptyOnCleanDrain pins the no-regression case: a drain
// that never hits a backend failure must never populate pendingWALChunks.
func TestPendingWALChunksEmptyOnCleanDrain(t *testing.T) {
	vb, _ := newEnospcTestVB(t)
	blockSize := uint64(vb.BlockSize)

	require.NoError(t, vb.WriteAt(0, make([]byte, blockSize)))
	require.NoError(t, vb.DrainToBackendCtx(context.Background()))

	assert.Zero(t, pendingWALChunksLen(vb), "a successful drain must never populate pendingWALChunks")
	assert.False(t, vb.backendFull.Load())
}
