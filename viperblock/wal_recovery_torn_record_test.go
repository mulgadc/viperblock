package viperblock

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/stretchr/testify/require"
)

func TestFormat2ReopenRetainsWALWithTornRecord(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
			var key *masterkey.Key
			if encrypted {
				key = testKey(t, 0x63)
			}
			volume := newMapVolume(t, 128<<20, key)
			writer, _ := volume.create(t, 1)
			t.Cleanup(func() { detachConsolidationVolume(writer) })
			require.NoError(t, writer.WriteAt(0, bytes.Repeat([]byte{0x72}, 8192)))
			require.NoError(t, writer.Flush())
			wal := writer.WAL.DB[len(writer.WAL.DB)-1]
			info, err := wal.Stat()
			require.NoError(t, err)
			require.NoError(t, wal.Truncate(info.Size()-1))
			damaged, err := os.ReadFile(wal.Name())
			require.NoError(t, err)
			detachConsolidationVolume(writer)

			reader, _ := volume.engine(t, 2)
			require.NoError(t, reader.LoadState())
			require.NoError(t, reader.LoadLiveCheckpoint())
			require.NoError(t, reader.ClaimMapCtx(t.Context()))
			writes := &scanChunkWrites{Backend: reader.Backend}
			reader.Backend = writes
			err = reader.RecoverLocalWALs()
			require.ErrorIs(t, err, ErrIntegrity)
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			retained, err := os.ReadFile(wal.Name())
			require.NoError(t, err)
			require.Equal(t, damaged, retained)
			require.Zero(t, writes.count.Load(), "failed recovery cannot publish a prefix")
		})
	}
}

func TestFormat2ReopenRetainsCorruptShardAndIntactSibling(t *testing.T) {
	volume := newMapVolume(t, 128<<20, nil)
	writer, _ := volume.engine(t, 1)
	require.NoError(t, writer.InitMapDeltaCtx(t.Context(), true))
	require.NoError(t, writer.SaveState())
	writer.UseShardedWAL = true
	writer.ShardedWAL = NewShardedWAL(writer.WAL.BaseDir, writer.WAL.WALMagic)
	require.NoError(t, writer.OpenShardedWAL())
	t.Cleanup(func() {
		writer.Detach()
		for _, shard := range writer.ShardedWAL.Shards {
			_ = shard.DB.Close()
		}
	})
	require.NoError(t, writer.WriteAt(0, bytes.Repeat([]byte{0x31}, 8192)))
	require.NoError(t, writer.Flush())

	intactName := writer.ShardedWAL.Shards[0].DB.Name()
	corruptName := writer.ShardedWAL.Shards[1].DB.Name()
	intact, err := os.ReadFile(intactName)
	require.NoError(t, err)
	corrupt, err := os.ReadFile(corruptName)
	require.NoError(t, err)
	corrupt[len(corrupt)-1] ^= 0x80
	edit, err := os.OpenFile(corruptName, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = edit.WriteAt(corrupt[len(corrupt)-1:], int64(len(corrupt)-1))
	require.NoError(t, err)
	require.NoError(t, edit.Close())
	writer.Detach()

	reader, _ := volume.engine(t, 2)
	require.NoError(t, reader.LoadState())
	require.NoError(t, reader.LoadLiveCheckpoint())
	require.NoError(t, reader.ClaimMapCtx(t.Context()))
	writes := &scanChunkWrites{Backend: reader.Backend}
	reader.Backend = writes
	require.ErrorIs(t, reader.RecoverLocalWALs(), ErrIntegrity)
	for path, want := range map[string][]byte{intactName: intact, corruptName: corrupt} {
		got, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	require.Zero(t, writes.count.Load(), "recovery cannot publish an intact sibling before rejecting the corrupt shard")
}
