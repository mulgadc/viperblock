// The state-load WARN is the only log call in LoadState that never named its
// volume, so a 404 on a state object could not be tied to one. A first open
// also has no state object yet, which is an expected outcome rather than a
// problem worth a WARN.

package viperblock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failConfigReads makes every FileTypeConfig read fail with err.
func failConfigReads(vb *VB, err error) *flakyConfigBackend {
	flaky := &flakyConfigBackend{Backend: vb.Backend}
	flaky.mutate = func(_ int, _ []byte, _ error) ([]byte, error) {
		return nil, err
	}
	vb.Backend = flaky
	return flaky
}

// captureVBLogs points one instance's logger at a JSON buffer at Debug.
func captureVBLogs(vb *VB) *bytes.Buffer {
	var buf bytes.Buffer
	vb.log = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return &buf
}

func TestLoadState_BackendFaultWarnsWithTheVolumeName(t *testing.T) {
	vb := newFileBackedVB(t, "vol-state-fault", nil)
	vb.BlockSize = DefaultBlockSize
	vb.stateReadRetryBackoff = time.Microsecond
	require.NoError(t, vb.SaveState())
	require.FileExists(t, localStatePath(vb))

	failConfigReads(vb, errors.New("backend unreachable"))
	logs := captureVBLogs(vb)

	require.NoError(t, vb.LoadStateCtx(context.Background()),
		"a backend fault must not fail the open when local is readable")

	out := logs.String()
	assert.Contains(t, out, `"msg":"Failed to load state from backend"`)
	assert.Contains(t, out, `"volume":"vol-state-fault"`,
		"the WARN must name the volume or a 404 cannot be attributed")
}

func TestLoadState_AbsentBackendStateDoesNotWarn(t *testing.T) {
	vb := newFileBackedVB(t, "vol-state-absent", nil)
	vb.BlockSize = DefaultBlockSize
	vb.stateReadRetryBackoff = time.Microsecond
	require.NoError(t, vb.SaveState())
	require.FileExists(t, localStatePath(vb))

	// The shape predastore returns for a GetObject 404, as the S3 backend
	// wraps it.
	failConfigReads(vb, fmt.Errorf("%w: %w", os.ErrNotExist, errors.New("NoSuchKey")))
	logs := captureVBLogs(vb)

	require.NoError(t, vb.LoadStateCtx(context.Background()))

	out := logs.String()
	assert.NotContains(t, out, `"level":"WARN"`,
		"a volume with no state object yet is expected, not a problem")
	assert.Contains(t, out, `"volume":"vol-state-absent"`)
}
