package viperblock

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sealReader loads the volume the way the provider seal path does, with a
// chunk-write counter in front of the backend.
func sealReader(t *testing.T, volume *mapVolume, epoch uint64) (*VB, *scanChunkWrites) {
	t.Helper()
	reader, _ := volume.engine(t, epoch)
	require.NoError(t, reader.LoadState())
	require.NoError(t, reader.LoadLiveCheckpoint())
	require.NoError(t, reader.ClaimMapCtx(t.Context()))
	writes := &scanChunkWrites{Backend: reader.Backend}
	reader.Backend = writes
	return reader, writes
}

// checkWALRetained reports, without stopping the test, whether the WAL is
// still on disk with the expected bytes, so a base run shows every symptom.
func checkWALRetained(t *testing.T, walName string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(walName)
	if assert.NoError(t, err, "recovery removed the only copy of acknowledged writes") {
		assert.Equal(t, want, got)
	}
}

// close removes the local volume tree, so a WAL that recovery skips is lost.
func TestSealRetainsWALWithDamagedHeader(t *testing.T) {
	seal := func(t *testing.T, reader *VB, recoverWALs func() error) error {
		t.Helper()
		if err := recoverWALs(); err != nil {
			return err
		}
		return reader.CloseCtx(t.Context())
	}
	setup := func(t *testing.T) (*mapVolume, string, []byte) {
		t.Helper()
		volume := newMapVolume(t, 128<<20, nil)
		writer, _ := volume.create(t, 1)
		t.Cleanup(func() { detachConsolidationVolume(writer) })
		require.NoError(t, writer.WriteAt(0, bytes.Repeat([]byte{0x2d}, 8192)))
		require.NoError(t, writer.Flush())
		walName := writer.WAL.DB[len(writer.WAL.DB)-1].Name()
		walBytes, err := os.ReadFile(walName)
		require.NoError(t, err)
		detachConsolidationVolume(writer)
		return volume, walName, walBytes
	}
	damages := map[string]func(header []byte){
		"magic":     func(header []byte) { copy(header[:4], "XXXX") },
		"version":   func(header []byte) { header[5] ^= 0xff },
		"blockSize": func(header []byte) { header[8] ^= 0x01 },
	}
	for name, damage := range damages {
		t.Run(name, func(t *testing.T) {
			volume, walName, walBytes := setup(t)
			damaged := bytes.Clone(walBytes)
			damage(damaged)
			require.NoError(t, os.WriteFile(walName, damaged, 0o600))
			reader, writes := sealReader(t, volume, 2)

			assert.Error(t, seal(t, reader, reader.RecoverLocalWALs))
			checkWALRetained(t, walName, damaged)
			assert.Zero(t, writes.count.Load())
		})
	}
	t.Run("unreadable", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root ignores file permissions")
		}
		volume, walName, walBytes := setup(t)
		reader, writes := sealReader(t, volume, 2)
		require.NoError(t, os.Chmod(walName, 0))
		err := seal(t, reader, reader.RecoverLocalWALs)
		require.NoError(t, os.Chmod(walName, 0o600))

		assert.ErrorIs(t, err, os.ErrPermission)
		checkWALRetained(t, walName, walBytes)
		assert.Zero(t, writes.count.Load())
	})
	t.Run("readError", func(t *testing.T) {
		volume, walName, walBytes := setup(t)
		reader, writes := sealReader(t, volume, 2)
		failedRead := errors.New("injected WAL header read error")

		err := seal(t, reader, func() error {
			return reader.recoverLocalWALs(func(path string) ([]Block, uint64, error) {
				require.Equal(t, walName, path)
				return nil, 0, fmt.Errorf("failed to read WAL header: %w", failedRead)
			})
		})
		assert.ErrorIs(t, err, failedRead)
		checkWALRetained(t, walName, walBytes)
		assert.Zero(t, writes.count.Load())
	})
}
