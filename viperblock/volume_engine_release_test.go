package viperblock

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureHandler records the messages logged through it, so a test can assert
// on the open/close events themselves rather than on the flag that gates them.
type captureHandler struct {
	mu   sync.Mutex
	msgs []string
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) count(msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, m := range h.msgs {
		if m == msg {
			n++
		}
	}
	return n
}

const (
	volumeOpenedMsg = "viperblock volume opened"
	volumeClosedMsg = "viperblock volume closed"
)

// newCapturedVB builds a VB whose events land in the returned handler.
func newCapturedVB(t *testing.T) (*VB, *captureHandler) {
	t.Helper()

	tmpDir := t.TempDir()
	testVol := fmt.Sprintf("test_engine_release_%d", time.Now().UnixNano())
	capture := &captureHandler{}

	vb, err := New(&VB{
		VolumeName:          testVol,
		VolumeSize:          volumeSize,
		BaseDir:             fmt.Sprintf("%s/%s", tmpDir, "viperblock"),
		WALSyncInterval:     5 * time.Millisecond,
		ChunkUploadInterval: 10 * time.Millisecond,
		Logger:              slog.New(capture),
	}, FileBackend, file.FileConfig{
		VolumeName: testVol,
		VolumeSize: volumeSize,
		BaseDir:    tmpDir,
	})
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s/wal/chunks/wal.%08d.bin",
		vb.BaseDir, vb.GetVolume(), vb.WAL.WallNum.Load())))

	t.Cleanup(func() {
		vb.StopChunkUploader()
		vb.StopWALSyncer()
	})

	return vb, capture
}

// TestCloseEmitsOneVolumeClosed is the wiring. CloseCtx has several exits, so
// the release is deferred rather than sitting on the success path; and it must
// fire once, not once per exit.
func TestCloseEmitsOneVolumeClosed(t *testing.T) {
	vb, capture := newCapturedVB(t)
	require.Equal(t, 1, capture.count(volumeOpenedMsg), "New must record the open, or the close below has nothing to match")
	require.Zero(t, capture.count(volumeClosedMsg))

	require.NoError(t, vb.Close())
	assert.Equal(t, 1, capture.count(volumeClosedMsg), "Close must report the volume released exactly once")
}

// TestSecondCloseEmitsNothing covers a VB closed twice — nbdkit's Unload runs
// after Close on some shutdown paths. The engine count is a running total, so
// a repeated release would push it below zero and read as fewer holders than
// there are.
func TestSecondCloseEmitsNothing(t *testing.T) {
	vb, capture := newCapturedVB(t)
	require.NoError(t, vb.Close())
	require.Equal(t, 1, capture.count(volumeClosedMsg))

	vb.releaseEngine()
	assert.Equal(t, 1, capture.count(volumeClosedMsg), "a second release must not be reported")
}

// TestHandBuiltVBEmitsNoClose covers a VB assembled as a struct literal rather
// than through New — the read-only shapes that share another instance's
// backend. Those record no open, so releasing one would decrement a count it
// never incremented.
func TestHandBuiltVBEmitsNoClose(t *testing.T) {
	capture := &captureHandler{}
	vb := &VB{VolumeName: "vol-handbuilt", log: slog.New(capture)}

	vb.releaseEngine()
	assert.Zero(t, capture.count(volumeClosedMsg), "a VB that skipped New holds nothing to release")
}
