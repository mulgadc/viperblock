package viperblock

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

func newConsolidationVolume(t testing.TB, root string, key *masterkey.Key) *VB {
	t.Helper()
	const size = 128 << 20
	vb, err := New(&VB{
		VolumeName: "wal-scan", VolumeSize: size, BaseDir: filepath.Join(root, "local"),
		WALSyncInterval: -1, ChunkUploadInterval: -1, LeaseEpoch: 1,
		MasterKey: key, EncryptionEnabled: key != nil,
	}, FileBackend, file.FileConfig{VolumeName: "wal-scan", VolumeSize: size, BaseDir: root})
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.InitMapDeltaCtx(t.Context(), true))
	require.NoError(t, vb.SaveState())
	openMapWALs(t, vb)
	return vb
}

func detachConsolidationVolume(vb *VB) {
	vb.Detach()
	for _, f := range vb.WAL.DB {
		_ = f.Close()
	}
	for _, f := range vb.BlockToObjectWAL.DB {
		_ = f.Close()
	}
}

func TestDrainRefusesIncompleteWALRecord(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(fmt.Sprintf("encrypted=%t", encrypted), func(t *testing.T) {
			var key *masterkey.Key
			if encrypted {
				key = testKey(t, 0x63)
			}
			vb := newConsolidationVolume(t, t.TempDir(), key)
			t.Cleanup(func() { detachConsolidationVolume(vb) })
			blockSize := int(vb.BlockSize)
			data := bytes.Repeat([]byte{0x72}, 2*blockSize)
			// A missing zero byte would pass the cleartext CRC if padded.
			data[len(data)-1] = 0
			require.NoError(t, vb.WriteAt(0, data))
			require.NoError(t, vb.Flush())
			wal := vb.WAL.DB[len(vb.WAL.DB)-1]
			info, err := wal.Stat()
			require.NoError(t, err)
			require.NoError(t, wal.Truncate(info.Size()-1))

			require.ErrorIs(t, vb.DrainToBackend(), io.ErrUnexpectedEOF)
			require.FileExists(t, wal.Name())
			_, _, _, err = vb.LookupBlockToObject(0)
			require.ErrorIs(t, err, ErrZeroBlock)

			replayed, _, err := vb.readWALFileForRecovery(wal.Name())
			require.NoError(t, err)
			require.Len(t, replayed, 1)
			require.Equal(t, data[:blockSize], replayed[0].Data)
		})
	}
}

func BenchmarkConsolidationScan(b *testing.B) {
	silenceVBLogs()
	for _, encrypted := range []bool{false, true} {
		for _, size := range []int{4 << 10, 4 << 20, 16 << 20} {
			b.Run(fmt.Sprintf("encrypted=%t/bytes=%d", encrypted, size), func(b *testing.B) {
				var key *masterkey.Key
				if encrypted {
					raw := bytes.Repeat([]byte{0x63}, masterkey.MasterKeySize)
					var err error
					key, err = masterkey.New(raw)
					require.NoError(b, err)
				}
				root := filepath.Join(b.TempDir(), "volume")
				data := makeSyntheticImage(uint64(size))
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					require.NoError(b, os.MkdirAll(root, 0700))
					vb := newConsolidationVolume(b, root, key)
					require.NoError(b, vb.WriteAt(0, data))
					require.NoError(b, vb.Flush())
					id, rotated, err := vb.rotateChunkWAL(true)
					require.NoError(b, err)
					require.True(b, rotated)
					b.StartTimer()
					err = vb.consolidateWALGeneration(b.Context(), id)
					b.StopTimer()
					detachConsolidationVolume(vb)
					require.NoError(b, os.RemoveAll(root))
					require.NoError(b, err)
					b.StartTimer()
				}
			})
		}
	}
}
