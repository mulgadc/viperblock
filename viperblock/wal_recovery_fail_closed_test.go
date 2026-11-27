package viperblock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"testing/iotest"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// format1WALVolume writes three distinct blocks to a format-1 volume's
// single-file WAL, flushes it and detaches, so the WAL is the only copy.
func format1WALVolume(t *testing.T) (*mapVolume, string, []byte) {
	t.Helper()
	volume := newMapVolume(t, 128<<20, nil)
	writer, _ := volume.engine(t, 1)
	require.NoError(t, writer.SaveState())
	require.False(t, writer.IsMapDelta())
	openMapWALs(t, writer)
	t.Cleanup(func() { detachConsolidationVolume(writer) })
	blockSize := int(writer.BlockSize)
	data := make([]byte, 0, 3*blockSize)
	for i := range 3 {
		data = append(data, bytes.Repeat([]byte{byte(0x41 + i)}, blockSize)...)
	}
	require.NoError(t, writer.WriteAt(0, data))
	require.NoError(t, writer.Flush())
	walName := writer.WAL.DB[len(writer.WAL.DB)-1].Name()
	walBytes, err := os.ReadFile(walName)
	require.NoError(t, err)
	require.Len(t, walBytes, writer.WALHeaderSize()+3*(28+blockSize))
	detachConsolidationVolume(writer)
	return volume, walName, walBytes
}

// sealReader loads the volume the way the provider seal path does, with a
// chunk-write counter in front of the backend.
func sealReader(t *testing.T, volume *mapVolume, epoch uint64) (*VB, *scanChunkWrites) {
	t.Helper()
	reader, _ := volume.engine(t, epoch)
	require.NoError(t, reader.LoadState())
	require.NoError(t, reader.LoadBlockState())
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

func TestFormat1ReopenRetainsWALWithBadMidFileRecord(t *testing.T) {
	t.Run("checksum", func(t *testing.T) {
		volume, walName, walBytes := format1WALVolume(t)
		reader, writes := sealReader(t, volume, 2)
		damaged := bytes.Clone(walBytes)
		damaged[reader.WALHeaderSize()+(28+int(reader.BlockSize))+24] ^= 0xff
		require.NoError(t, os.WriteFile(walName, damaged, 0o600))

		assert.ErrorIs(t, reader.RecoverLocalWALs(), ErrIntegrity)
		checkWALRetained(t, walName, damaged)
		assert.Zero(t, writes.count.Load(), "failed recovery cannot publish a prefix")
	})
	t.Run("readError", func(t *testing.T) {
		volume, walName, walBytes := format1WALVolume(t)
		reader, writes := sealReader(t, volume, 2)
		header := reader.WALHeaderSize()
		firstRecord := walBytes[header : header+28+int(reader.BlockSize)]
		failedRead := errors.New("injected WAL read error")

		err := reader.recoverLocalWALs(func(path string) ([]Block, uint64, error) {
			require.Equal(t, walName, path)
			return reader.readWALRecoveryRecords(io.MultiReader(bytes.NewReader(firstRecord), iotest.ErrReader(failedRead)), path)
		})
		assert.ErrorIs(t, err, ErrIntegrity)
		assert.ErrorIs(t, err, failedRead)
		checkWALRetained(t, walName, walBytes)
		assert.Zero(t, writes.count.Load(), "failed recovery cannot publish a prefix")
	})
}

func TestFormat1ReopenRetainsWALWithTornRecord(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
			var key *masterkey.Key
			if encrypted {
				key = testKey(t, 0x64)
			}
			volume := newMapVolume(t, 128<<20, key)
			writer, _ := volume.engine(t, 1)
			require.NoError(t, writer.SaveState())
			openMapWALs(t, writer)
			t.Cleanup(func() { detachConsolidationVolume(writer) })
			require.NoError(t, writer.WriteAt(0, bytes.Repeat([]byte{0x73}, 8192)))
			require.NoError(t, writer.Flush())
			wal := writer.WAL.DB[len(writer.WAL.DB)-1]
			info, err := wal.Stat()
			require.NoError(t, err)
			require.NoError(t, wal.Truncate(info.Size()-1))
			damaged, err := os.ReadFile(wal.Name())
			require.NoError(t, err)
			detachConsolidationVolume(writer)

			reader, writes := sealReader(t, volume, 2)
			err = reader.RecoverLocalWALs()
			assert.ErrorIs(t, err, ErrIntegrity)
			assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
			checkWALRetained(t, wal.Name(), damaged)
			assert.Zero(t, writes.count.Load(), "failed recovery cannot publish a prefix")
		})
	}
}

// liveCheckpointFailBackend fails every live-checkpoint write while fail is set.
type liveCheckpointFailBackend struct {
	types.Backend

	fail atomic.Bool
}

var _ types.Backend = (*liveCheckpointFailBackend)(nil)

func (b *liveCheckpointFailBackend) WriteCtx(ctx context.Context, typ types.FileType, id uint64, headers, data *[]byte) error {
	if typ == types.FileTypeBlockCheckpointLive && b.fail.Load() {
		return errors.New("injected live checkpoint write failure")
	}
	return b.Backend.WriteCtx(ctx, typ, id, headers, data)
}

// A stale live checkpoint outranks the numbered one on the next open, so a
// recovery that cannot refresh it must keep the WAL.
func TestFormat1RecoveryRetainsWALWhenLiveCheckpointFails(t *testing.T) {
	volume := newMapVolume(t, 128<<20, nil)
	writer, _ := volume.engine(t, 1)
	require.NoError(t, writer.SaveState())
	require.NoError(t, writer.SaveLiveCheckpoint())
	openMapWALs(t, writer)
	t.Cleanup(func() { detachConsolidationVolume(writer) })
	want := bytes.Repeat([]byte{0x5c}, 8192)
	require.NoError(t, writer.WriteAt(0, want))
	require.NoError(t, writer.Flush())
	walName := writer.WAL.DB[len(writer.WAL.DB)-1].Name()
	walBytes, err := os.ReadFile(walName)
	require.NoError(t, err)
	detachConsolidationVolume(writer)

	open := func(epoch uint64) (*VB, *liveCheckpointFailBackend) {
		vb, _ := volume.engine(t, epoch)
		require.NoError(t, vb.LoadState())
		require.NoError(t, vb.LoadLiveCheckpoint())
		require.NoError(t, vb.ClaimMapCtx(t.Context()))
		failing := &liveCheckpointFailBackend{Backend: vb.Backend}
		vb.Backend = failing
		return vb, failing
	}

	failed, failing := open(2)
	failing.fail.Store(true)
	failedErr := failed.RecoverLocalWALs()
	assert.Error(t, failedErr)
	checkWALRetained(t, walName, walBytes)
	failed.Detach()
	if failedErr == nil {
		return
	}

	retry, _ := open(3)
	require.NoError(t, retry.RecoverLocalWALs())
	retry.Detach()

	fresh, _ := volume.engine(t, 4)
	require.NoError(t, fresh.LoadState())
	require.NoError(t, fresh.LoadLiveCheckpoint())
	got, err := fresh.ReadAt(0, uint64(len(want)))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// The provider seal closes the volume when recovery succeeds, and a clean
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
			require.True(t, reader.IsMapDelta())

			assert.Error(t, seal(t, reader, reader.RecoverLocalWALs))
			checkWALRetained(t, walName, damaged)
			assert.Zero(t, writes.count.Load())
		})
	}
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
