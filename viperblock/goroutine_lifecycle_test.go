// Tests for the chunk-uploader and WAL-syncer goroutine lifecycle, covering
// the fix for the goroutine leak (mulga-0zzci): spinifex creates short-lived
// VBs that must stop background goroutines without calling Close().

package viperblock

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChunkUploaderStopWithoutClose verifies the cleanup pattern used in
// spinifex for short-lived VBs: calling StopChunkUploader + StopWALSyncer
// without Close(). Stop must complete (goroutine exits via done channel) and
// be safe to call a second time (matches the defer-then-explicit-stop pattern).
func TestChunkUploaderStopWithoutClose(t *testing.T) {
	dir := t.TempDir()
	vb := openEncryptedVBInDir(t, dir, "goroutine-lifecycle", nil)

	// Chunk uploader is started unconditionally by New(); the stop channel
	// must be non-nil before we stop it.
	require.NotNil(t, vb.chunkUploadStop, "chunk uploader must be running after New()")

	// Stop without Close — this is the spinifex pattern for short-lived VBs
	// (prepareRootVolume, snapshotVolume, CreateVolume, etc.). Each Stop call
	// waits on its done channel, so when it returns the goroutine has exited.
	vb.StopChunkUploader()
	vb.StopWALSyncer()

	assert.Nil(t, vb.chunkUploadStop, "chunkUploadStop must be nil after Stop (goroutine exited)")
	assert.Nil(t, vb.chunkUploadDone, "chunkUploadDone must be nil after Stop")

	// Second call must not panic or deadlock — matches the defer + explicit
	// stop pattern where both may fire for the same VB.
	assert.NotPanics(t, func() {
		vb.StopChunkUploader()
		vb.StopWALSyncer()
	}, "double-Stop must be idempotent")
}

// TestChunkUploaderStopEncrypted runs the same lifecycle check for an encrypted
// VB, since encrypted VBs go through the same uploader path.
func TestChunkUploaderStopEncrypted(t *testing.T) {
	dir := t.TempDir()
	key := testKey(t, 0x33)
	vb := openEncryptedVBInDir(t, dir, "goroutine-lifecycle-enc", key)

	require.NotNil(t, vb.chunkUploadStop, "chunk uploader must be running for encrypted VB")

	vb.StopChunkUploader()
	vb.StopWALSyncer()

	assert.Nil(t, vb.chunkUploadStop, "chunkUploadStop must be nil after Stop")
	assert.NotPanics(t, func() {
		vb.StopChunkUploader()
		vb.StopWALSyncer()
	}, "double-Stop must be idempotent for encrypted VB")
}
