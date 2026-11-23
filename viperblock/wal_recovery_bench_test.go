package viperblock

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

func BenchmarkFormat2WALReopen(b *testing.B) {
	silenceVBLogs()
	for _, size := range []int{4 << 10, 4 << 20, 16 << 20} {
		b.Run(fmt.Sprintf("bytes=%d", size), func(b *testing.B) {
			data := makeSyntheticImage(uint64(size))
			root := filepath.Join(b.TempDir(), "volume")
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				require.NoError(b, os.MkdirAll(root, 0700))
				writer := newConsolidationVolume(b, root, nil)
				require.NoError(b, writer.WriteAt(0, data))
				require.NoError(b, writer.Flush())
				detachConsolidationVolume(writer)
				reader, err := New(&VB{
					VolumeName: writer.VolumeName, VolumeSize: writer.VolumeSize, BaseDir: writer.BaseDir,
					WALSyncInterval: -1, ChunkUploadInterval: -1, LeaseEpoch: 2,
				}, FileBackend, file.FileConfig{VolumeName: writer.VolumeName, VolumeSize: writer.VolumeSize, BaseDir: root})
				require.NoError(b, err)
				require.NoError(b, reader.Backend.Init())
				require.NoError(b, reader.LoadState())
				require.NoError(b, reader.LoadLiveCheckpoint())
				require.NoError(b, reader.ClaimMapCtx(b.Context()))

				b.StartTimer()
				err = reader.RecoverLocalWALs()
				b.StopTimer()
				reader.Detach()
				require.NoError(b, err)
				require.NoError(b, os.RemoveAll(root))
				b.StartTimer()
			}
		})
	}
}
