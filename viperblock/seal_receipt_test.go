package viperblock

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteSealReceipt_ValidJSON(t *testing.T) {
	baseDir := t.TempDir()
	volume := "vol-abc123"

	require.NoError(t, WriteSealReceipt(baseDir, volume))

	path := filepath.Join(baseDir, volume+".sealed")
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var got sealReceipt
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, volume, got.Volume)
	assert.Equal(t, os.Getpid(), got.PID)

	sealedAt, err := time.Parse(time.RFC3339, got.SealedAt)
	require.NoError(t, err)
	assert.WithinDuration(t, time.Now().UTC(), sealedAt.UTC(), time.Minute)

	t.Logf("seal receipt JSON: %s", data)
}

func TestWriteSealReceipt_SiblingOfStateDir(t *testing.T) {
	baseDir := t.TempDir()
	volume := "vol-sibling"

	// A minimal VB with just the fields RemoveLocalFiles touches, so the
	// state dir it owns exists at BaseDir/<volume> exactly as production has it.
	vb := &VB{BaseDir: baseDir, VolumeName: volume}
	stateDir := filepath.Join(vb.BaseDir, vb.GetVolume())
	require.NoError(t, os.MkdirAll(stateDir, 0750))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "some.state"), []byte("x"), 0640))

	require.NoError(t, WriteSealReceipt(baseDir, volume))
	receiptPath := filepath.Join(baseDir, volume+".sealed")

	// The receipt must not live under the state directory RemoveLocalFiles
	// deletes, or it would be destroyed along with everything else.
	require.NoError(t, vb.RemoveLocalFiles())

	_, err := os.Stat(stateDir)
	assert.True(t, os.IsNotExist(err), "state dir should be removed")

	_, err = os.Stat(receiptPath)
	assert.NoError(t, err, "seal receipt should survive RemoveLocalFiles")
}

func TestWriteSealReceipt_UnwritableBaseDir(t *testing.T) {
	parent := t.TempDir()
	baseDir := filepath.Join(parent, "sealed-dir")
	require.NoError(t, os.MkdirAll(baseDir, 0500))
	t.Cleanup(func() { _ = os.Chmod(baseDir, 0750) })

	err := WriteSealReceipt(baseDir, "vol-noperm")
	require.Error(t, err)
}
