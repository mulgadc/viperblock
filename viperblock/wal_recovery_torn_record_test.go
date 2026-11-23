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
