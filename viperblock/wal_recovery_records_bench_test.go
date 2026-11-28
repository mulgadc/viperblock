package viperblock

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BenchmarkWALRecoveryRecords times RecoverLocalWALs over one intact
// single-file WAL of a given record count, for both block-map formats, so the
// good path of recovery can be compared across changes.
func BenchmarkWALRecoveryRecords(b *testing.B) {
	silenceVBLogs()
	for _, format := range []int{1, 2} {
		for _, records := range []int{1 << 10, 64 << 10} {
			b.Run(fmt.Sprintf("format=%d/records=%d", format, records), func(b *testing.B) {
				benchWALRecoveryRecords(b, format, records)
			})
		}
	}
}

func benchWALRecoveryRecords(b *testing.B, format, records int) {
	root := filepath.Join(b.TempDir(), "volume")
	size := uint64(max(128<<20, 2*records*int(DefaultBlockSize)))
	data := make([]byte, records*int(DefaultBlockSize))
	for i := range data {
		data[i] = byte(i%251) + 1
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		require.NoError(b, os.MkdirAll(root, 0o700))
		volume := &mapVolume{
			root: root, name: "vol-recover", size: size,
			clock: &mapTestClock{now: time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)},
		}
		writer, _ := volume.engine(b, 1)
		writer.MaxPendingBytes = uint64(2 * len(data))
		if format == 2 {
			require.NoError(b, writer.InitMapDeltaCtx(b.Context(), true))
		}
		require.NoError(b, writer.SaveState())
		openMapWALs(b, writer)
		require.NoError(b, writer.WriteAt(0, data))
		require.NoError(b, writer.Flush())
		walName := writer.WAL.DB[len(writer.WAL.DB)-1].Name()
		info, err := os.Stat(walName)
		require.NoError(b, err)
		require.Equal(b, int64(writer.WALHeaderSize()+records*(28+int(writer.BlockSize))), info.Size())
		detachConsolidationVolume(writer)

		reader, _ := volume.engine(b, 2)
		require.NoError(b, reader.LoadState())
		require.NoError(b, reader.LoadLiveCheckpoint())
		require.NoError(b, reader.ClaimMapCtx(b.Context()))
		b.StartTimer()
		err = reader.RecoverLocalWALs()
		b.StopTimer()
		require.NoError(b, err)
		_, err = os.Stat(walName)
		require.ErrorIs(b, err, os.ErrNotExist, "a recovered WAL is removed")
		reader.Detach()
		require.NoError(b, os.RemoveAll(root))
		b.StartTimer()
	}
}
