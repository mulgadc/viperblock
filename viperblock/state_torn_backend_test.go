// Unit tests for the local-copy fallback when the backend's VBState is torn.
// persistStateLocal writes and fsyncs the local copy before pushStateToBackend
// sends it, so a partially-written backend object is recoverable: the local
// file is the authority. Tampering is not recoverable and must stay fatal.

package viperblock

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// truncateConfigReads makes every FileTypeConfig read come back short, which is
// what an interrupted PUT looks like to the reader.
func truncateConfigReads(vb *VB) *flakyConfigBackend {
	flaky := &flakyConfigBackend{Backend: vb.Backend}
	flaky.mutate = func(_ int, data []byte, err error) ([]byte, error) {
		if err != nil {
			return data, err
		}
		return data[:len(data)-5], nil
	}
	vb.Backend = flaky
	return flaky
}

// corruptConfigAuthTag flips one base64 character of the envelope's authtag,
// leaving the envelope well-formed. That is the tampering shape: the bytes
// arrived whole and failed to verify.
func corruptConfigAuthTag(vb *VB) *flakyConfigBackend {
	flaky := &flakyConfigBackend{Backend: vb.Backend}
	flaky.mutate = func(_ int, data []byte, err error) ([]byte, error) {
		if err != nil {
			return data, err
		}
		marker := []byte(`"authtag":"`)
		i := bytes.Index(data, marker)
		if i < 0 {
			return data, err
		}
		out := append([]byte(nil), data...)
		at := i + len(marker)
		if out[at] == 'A' {
			out[at] = 'B'
		} else {
			out[at] = 'A'
		}
		return out, nil
	}
	vb.Backend = flaky
	return flaky
}

func localStatePath(vb *VB) string {
	return fmt.Sprintf("%s/%s", vb.BaseDir, types.GetFilePath(types.FileTypeConfig, 0, vb.GetVolume()))
}

// The failure this restores: a seal interrupted mid-PUT left a torn backend
// object, and the open failed outright even though the local copy was intact
// and fsynced. Two guests were destroyed that way during a cluster upgrade.
func TestLoadState_TornBackendFallsBackToLocalCopy(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-torn-backend", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())
	require.FileExists(t, localStatePath(vb))

	truncateConfigReads(vb)

	require.NoError(t, vb.LoadStateCtx(context.Background()),
		"a torn backend copy must not fail the open when local is readable")
	assert.Equal(t, "vol-torn-backend", vb.VolumeName)
	assert.Equal(t, DefaultBlockSize, vb.BlockSize)
}

// The unencrypted path reaches truncation at the final JSON decode rather than
// the envelope split, so it needs its own cover.
func TestLoadState_TornPlainBackendFallsBackToLocalCopy(t *testing.T) {
	vb := newFileBackedVB(t, "vol-torn-plain", nil)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	truncateConfigReads(vb)

	require.NoError(t, vb.LoadStateCtx(context.Background()))
	assert.Equal(t, "vol-torn-plain", vb.VolumeName)
}

// A well-formed envelope whose tag does not verify is tampering, not a partial
// write. The local copy must not launder it.
func TestLoadState_TamperedBackendStaysFatal(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-tampered-backend", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())

	corruptConfigAuthTag(vb)

	err := vb.LoadStateCtx(context.Background())
	require.Error(t, err, "a failed tag verify must stay fail-closed")
	assert.ErrorIs(t, err, ErrIntegrity)
	assert.NotErrorIs(t, err, ErrStateTorn, "tampering is not torn")
}

// Falling back needs something to fall back to. With no local copy the torn
// backend is all there is, and the open must still fail rather than proceed on
// a zero-valued state.
func TestLoadState_TornBackendWithoutLocalCopyIsFatal(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-torn-no-local", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.SaveState())
	require.NoError(t, os.Remove(localStatePath(vb)))

	truncateConfigReads(vb)

	require.Error(t, vb.LoadStateCtx(context.Background()),
		"no local copy means the torn backend cannot be recovered from")
}
