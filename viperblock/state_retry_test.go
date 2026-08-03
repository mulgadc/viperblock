// Unit tests for LoadStateRequestCtx's backend-path retry: a bounded,
// backed-off re-attempt of the VBState backend read that distinguishes a
// transient/truncated read from genuinely-missing state or a fail-closed
// crypto failure. See docs/development/bugs/recovery-vbstate-transient-read-misclassified.md.

package viperblock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flakyConfigBackend wraps a real backend and hands every FileTypeConfig
// ReadCtx call to mutate, which decides what the caller sees based on the
// 1-based call number. Every other file type, and any call once mutate is
// nil, passes straight through. Safe for concurrent use since only
// LoadStateRequestCtx's own retry loop calls it, but the counter is still
// mutex-guarded for clarity.
type flakyConfigBackend struct {
	types.Backend

	mu     sync.Mutex
	calls  int
	mutate func(call int, data []byte, err error) ([]byte, error)
}

func (b *flakyConfigBackend) ReadCtx(ctx context.Context, fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	data, err := b.Backend.ReadCtx(ctx, fileType, objectID, offset, length)
	if fileType != types.FileTypeConfig {
		return data, err
	}

	b.mu.Lock()
	b.calls++
	n := b.calls
	b.mu.Unlock()

	if b.mutate != nil {
		return b.mutate(n, data, err)
	}
	return data, err
}

func (b *flakyConfigBackend) Read(fileType types.FileType, objectID uint64, offset, length uint32) ([]byte, error) {
	return b.ReadCtx(context.Background(), fileType, objectID, offset, length)
}

func (b *flakyConfigBackend) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

// TestLoadStateRequestCtx_RetriesTruncatedBackendRead is the direct
// regression test for the recovery bug: a backend read that comes back
// truncated on the first attempt (the shape a single unretried GET raced
// against a warming-up predastore produces) must be retried, not
// immediately misclassified as permanent ErrIntegrity.
func TestLoadStateRequestCtx_RetriesTruncatedBackendRead(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-retry-succeed", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	flaky := &flakyConfigBackend{Backend: vb.Backend}
	flaky.mutate = func(call int, data []byte, err error) ([]byte, error) {
		if err != nil || call != 1 {
			return data, err
		}
		// Truncate the envelope so it fails JSON parsing, exactly like a
		// connection dropped mid-body -- not a bit flip, a short read.
		return data[:len(data)-5], nil
	}
	vb.Backend = flaky

	state, err := vb.LoadStateRequestCtx(context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, vb.VolumeName, state.VolumeName)
	assert.Equal(t, 2, flaky.callCount(), "must succeed on the second attempt, not the first")
}

// TestLoadStateRequestCtx_ExhaustsRetriesOnPersistentTruncation pins the
// exhaustion budget: a backend that always returns a truncated body burns
// exactly stateReadMaxAttempts attempts and the final error still wraps
// ErrIntegrity, so existing callers (errors.Is checks) are unaffected.
func TestLoadStateRequestCtx_ExhaustsRetriesOnPersistentTruncation(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-retry-exhaust", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	flaky := &flakyConfigBackend{Backend: vb.Backend}
	flaky.mutate = func(call int, data []byte, err error) ([]byte, error) {
		if err != nil {
			return data, err
		}
		return data[:len(data)-5], nil
	}
	vb.Backend = flaky

	_, err := vb.LoadStateRequestCtx(context.Background(), "")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIntegrity)
	assert.Equal(t, stateReadMaxAttempts, flaky.callCount())
}

// TestLoadStateRequestCtx_NoRetryOnEncryptionMismatch pins that a wrong-key
// open makes exactly one attempt: the key is wrong on every retry too, so
// retrying only burns the recovery window for a deterministic failure.
func TestLoadStateRequestCtx_NoRetryOnEncryptionMismatch(t *testing.T) {
	keyA := testKey(t, 0x01)
	keyB := testKey(t, 0x02)
	require.NotEqual(t, keyA.Fingerprint, keyB.Fingerprint)

	vb := newFileBackedVB(t, "vol-retry-fp-mismatch", keyA)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	flaky := &flakyConfigBackend{Backend: vb.Backend}
	vb.Backend = flaky

	// Swap in the wrong key after the sealed state is on the backend, same
	// shape as a node opening a volume it does not hold the right key for.
	vb.MasterKey = keyB

	_, err := vb.LoadStateRequestCtx(context.Background(), "")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEncryptionMismatch)
	assert.Equal(t, 1, flaky.callCount())
}

// TestLoadStateRequestCtx_NoRetryOnTagVerifyFailure pins that a genuine
// tag-verify failure (payload/tag tampering, not truncation) fails closed on
// the first attempt.
func TestLoadStateRequestCtx_NoRetryOnTagVerifyFailure(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-retry-tag-fail", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	flaky := &flakyConfigBackend{Backend: vb.Backend}
	flaky.mutate = func(call int, data []byte, err error) ([]byte, error) {
		if err != nil || len(data) <= metaPayloadOffset {
			return data, err
		}
		// Flip a byte inside the verbatim payload (metaPayloadOffset, shared
		// with encryption_meta_test.go's bit-flip test) rather than the
		// base64-encoded authtag: the tag's trailing char has unused
		// low-order bits that Go's permissive base64 decoder discards, so a
		// substitution there can silently decode back to the same tag bytes
		// and the AEAD open, verifyMeta, and the retry classification this
		// test exercises. The payload ships as raw JSON text, so any byte
		// flip there deterministically changes the AAD input without
		// touching JSON syntax (it lands inside a field name, not a
		// structural character) or affecting KeyFingerprint.
		tampered := append([]byte(nil), data...)
		tampered[metaPayloadOffset] ^= 0x01
		return tampered, nil
	}
	vb.Backend = flaky

	_, err := vb.LoadStateRequestCtx(context.Background(), "")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrIntegrity)
	assert.Equal(t, 1, flaky.callCount())
}

// TestLoadStateRequestCtx_ContextCancelledDuringBackoffAbortsPromptly pins
// that a cancelled context aborts the retry loop during backoff rather than
// waiting out the full budget, and that the context error is returned.
func TestLoadStateRequestCtx_ContextCancelledDuringBackoffAbortsPromptly(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-retry-cancel", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	flaky := &flakyConfigBackend{Backend: vb.Backend}
	flaky.mutate = func(call int, data []byte, err error) ([]byte, error) {
		// Always a retryable transport-shaped failure so the loop enters
		// backoff (100ms) after the first attempt.
		return nil, fmt.Errorf("simulated transport error: %w", errors.New("connection reset"))
	}
	vb.Backend = flaky

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := vb.LoadStateRequestCtx(ctx, "")
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 1, flaky.callCount(), "cancellation must land during the first backoff, before a second attempt")
	assert.Less(t, elapsed, 400*time.Millisecond,
		"cancellation must abort promptly, not wait out the full retry budget (~700ms)")
}
