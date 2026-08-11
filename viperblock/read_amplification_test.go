package viperblock

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readAmpVolumeBytes and readAmpRequestBytes mirror the shape the live
// data-path measurement uses: a sequential stream of 64 KiB reads over a
// region several chunks wide.
const (
	readAmpVolumeBytes  = 16 << 20
	readAmpRequestBytes = 64 << 10
)

// chunkReadCounter counts backend reads of chunk objects, which is what a
// round trip to the object store costs on the read path.
type chunkReadCounter struct {
	*file.Backend

	reads atomic.Int64
}

var _ types.Backend = (*chunkReadCounter)(nil)

func (b *chunkReadCounter) ReadCtx(ctx context.Context, fileType types.FileType, objectId uint64, offset uint32, length uint32) ([]byte, error) {
	if fileType == types.FileTypeChunk {
		b.reads.Add(1)
	}
	return b.Backend.ReadCtx(ctx, fileType, objectId, offset, length)
}

// seedSequentialVolume writes readAmpVolumeBytes and pushes it all into
// chunks, returning the root the backend is stored under and the bytes
// written, so a reader can be checked against them rather than only counted.
func seedSequentialVolume(t *testing.T, volumeName string, key *masterkey.Key) (string, []byte) {
	t.Helper()

	root := t.TempDir()

	vbconfig := VB{
		VolumeName:      volumeName,
		VolumeSize:      readAmpVolumeBytes,
		BaseDir:         filepath.Join(root, "viperblock"),
		WALSyncInterval: -1,
		GCEnabled:       false,
		GCInterval:      -1,
		Cache:           Cache{Config: CacheConfig{Size: 0}},

		MasterKey:         key,
		EncryptionEnabled: key != nil,
	}
	writer, err := New(&vbconfig, FileBackend, file.FileConfig{
		VolumeName: volumeName,
		VolumeSize: readAmpVolumeBytes,
		BaseDir:    root,
	})
	require.NoError(t, err)
	require.NoError(t, writer.Backend.Init())
	require.NoError(t, writer.OpenWAL(&writer.WAL, filepath.Join(writer.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, writer.WAL.WallNum.Load(), writer.GetVolume()))))
	require.NoError(t, writer.OpenWAL(&writer.BlockToObjectWAL, filepath.Join(writer.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, writer.BlockToObjectWAL.WallNum.Load(), writer.GetVolume()))))

	written := make([]byte, 0, readAmpVolumeBytes)
	blocksPerWrite := uint64(readAmpRequestBytes) / uint64(DefaultBlockSize)
	for block := uint64(0); block < uint64(readAmpVolumeBytes)/uint64(DefaultBlockSize); block += blocksPerWrite {
		payload := randomBlockData(blocksPerWrite)
		require.NoError(t, writer.Write(block, payload))
		written = append(written, payload...)
	}
	require.NoError(t, writer.Flush())
	require.NoError(t, writer.WriteWALToChunk(true))
	// Close, not the individual saves: it persists the state and the numbered
	// checkpoint in the order a reopen expects to find them.
	require.NoError(t, writer.Close())

	return root, written
}

// coldReaderVB opens volumeName with an empty local directory and a cold
// cache, which is what every new NBD connection gets.
func coldReaderVB(t *testing.T, backendRoot, volumeName string, key *masterkey.Key) (*VB, *chunkReadCounter) {
	t.Helper()

	vbconfig := VB{
		VolumeName:      volumeName,
		VolumeSize:      readAmpVolumeBytes,
		BaseDir:         filepath.Join(t.TempDir(), "viperblock"),
		WALSyncInterval: -1,
		GCEnabled:       false,
		GCInterval:      -1,
		// The size spinifex opens data volumes with. DefaultCacheBlocks is 20,
		// far smaller than a readahead window, which would make the prefetched
		// blocks evict each other before the requests that want them arrive.
		Cache: Cache{Config: CacheConfig{Size: (128 << 20) / int(DefaultBlockSize)}},

		MasterKey:         key,
		EncryptionEnabled: key != nil,
	}

	vb, err := New(&vbconfig, FileBackend, file.FileConfig{
		VolumeName: volumeName,
		VolumeSize: readAmpVolumeBytes,
		BaseDir:    backendRoot,
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, vb.RemoveLocalFiles()) })

	fb, ok := vb.Backend.(*file.Backend)
	require.True(t, ok)
	require.NoError(t, fb.Init())
	require.NoError(t, vb.LoadState())
	require.NoError(t, vb.LoadBlockState())

	counter := &chunkReadCounter{Backend: fb}
	vb.Backend = counter

	return vb, counter
}

// readSequentially streams the whole seeded region and returns what it read
// plus the request count.
func readSequentially(t *testing.T, reader *VB) ([]byte, int) {
	t.Helper()
	ctx := context.Background()

	got := make([]byte, 0, readAmpVolumeBytes)
	requests := 0
	for offset := uint64(0); offset < readAmpVolumeBytes; offset += readAmpRequestBytes {
		data, err := reader.ReadAtCtx(ctx, offset, readAmpRequestBytes)
		require.NoErrorf(t, err, "read at offset %d", offset)
		require.Len(t, data, readAmpRequestBytes)
		got = append(got, data...)
		requests++
	}
	return got, requests
}

// TestSequentialRead_CostsFarFewerBackendReadsThanRequests is the point of
// readahead. Before it, a sequential stream cost one backend round trip per
// request — 256 reads for 4 chunks — and at the ~4.8 ms round trip measured
// on env19 that bounded a depth-1 sequential reader at 13 MiB/s.
//
// The two reference numbers are what make the assertion meaningful. Requests
// is what the client asked for; chunks touched is the floor any fetch
// strategy could reach.
func TestSequentialRead_CostsFarFewerBackendReadsThanRequests(t *testing.T) {
	volumeName := "vol-readamp"

	root, written := seedSequentialVolume(t, volumeName, nil)
	reader, counter := coldReaderVB(t, root, volumeName, nil)

	got, requests := readSequentially(t, reader)

	chunksTouched := int64(readAmpVolumeBytes / reader.ObjBlockSize)
	backendReads := counter.reads.Load()
	blocksPerRequest := int64(readAmpRequestBytes / DefaultBlockSize)

	require.Positive(t, backendReads, "no chunk was read from the backend, so this measures nothing")
	t.Logf("sequential stream: %d requests, %d chunks touched, %d backend chunk reads",
		requests, chunksTouched, backendReads)

	require.Equal(t, written, got, "readahead changed what the stream returned")

	// One fetch covers the request plus readAheadBlocks of stream, so the
	// round trips fall by about that ratio. Asserting the ratio rather than an
	// exact count leaves room for the run boundaries to land differently.
	ceiling := int64(requests)*blocksPerRequest/readAheadBlocks + chunksTouched + 1
	assert.LessOrEqual(t, backendReads, ceiling,
		"a sequential stream is not being widened: %d backend reads for %d requests", backendReads, requests)
	assert.GreaterOrEqual(t, backendReads, chunksTouched,
		"fewer backend reads than chunks touched is impossible, so the counter is wrong")
}

// TestSequentialRead_EncryptedStreamIsWidenedAndCorrect is the assertion that
// matters most on a production volume. Every block carries its own AEAD nonce
// and AAD derived from its SeqNum, so blocks pulled in by readahead have to
// carry theirs too. Getting that wrong fails the open rather than returning
// wrong plaintext, but only if something actually reads the widened blocks —
// which is what the byte comparison here does.
func TestSequentialRead_EncryptedStreamIsWidenedAndCorrect(t *testing.T) {
	volumeName := "vol-readamp-enc"
	key := testKey(t, 0x42)

	root, written := seedSequentialVolume(t, volumeName, key)
	reader, counter := coldReaderVB(t, root, volumeName, key)

	got, requests := readSequentially(t, reader)

	backendReads := counter.reads.Load()
	t.Logf("encrypted sequential stream: %d requests, %d backend chunk reads", requests, backendReads)

	require.Equal(t, written, got, "an encrypted stream came back wrong once readahead widened it")
	assert.Less(t, backendReads, int64(requests),
		"an encrypted sequential stream was not widened")
}

// TestRandomRead_IsNotWidened is the regression that matters more than the
// speedup. Readahead that fires on non-sequential access reads and decrypts
// blocks nobody asked for and evicts the cache doing it, which is worse than
// no readahead at all.
func TestRandomRead_IsNotWidened(t *testing.T) {
	ctx := context.Background()
	volumeName := "vol-readamp-random"

	root, written := seedSequentialVolume(t, volumeName, nil)
	reader, counter := coldReaderVB(t, root, volumeName, nil)

	// Strided far enough apart that no request continues the previous one.
	const stride = 1 << 20
	requests := 0
	for offset := uint64(0); offset+readAmpRequestBytes <= readAmpVolumeBytes; offset += stride {
		data, err := reader.ReadAtCtx(ctx, offset, readAmpRequestBytes)
		require.NoErrorf(t, err, "read at offset %d", offset)
		require.Equal(t, written[offset:offset+readAmpRequestBytes], data, "wrong data at offset %d", offset)
		requests++
	}

	assert.Equal(t, int64(requests), counter.reads.Load(),
		"a non-sequential reader paid for readahead it cannot use")
}

// TestReadahead_FirstReadIsNotAStream pins that an opening read is not
// mistaken for the continuation of one. Without the distinction every volume
// would prefetch on its very first request, including the single small reads
// that are all an EFI or cloud-init volume ever does.
func TestReadahead_FirstReadIsNotAStream(t *testing.T) {
	ctx := context.Background()
	volumeName := "vol-readamp-first"

	root, _ := seedSequentialVolume(t, volumeName, nil)
	reader, counter := coldReaderVB(t, root, volumeName, nil)

	_, err := reader.ReadAtCtx(ctx, 0, readAmpRequestBytes)
	require.NoError(t, err)

	assert.Equal(t, int64(1), counter.reads.Load(), "the first read of a volume was widened")
}

// TestReadahead_DisabledCacheDoesNotWiden covers the volumes spinifex opens
// with no cache at all. Widening a fetch whose extra blocks are dropped on the
// floor is pure waste.
func TestReadahead_DisabledCacheDoesNotWiden(t *testing.T) {
	ctx := context.Background()
	volumeName := "vol-readamp-nocache"

	root, _ := seedSequentialVolume(t, volumeName, nil)
	reader, counter := coldReaderVB(t, root, volumeName, nil)
	reader.Cache.Config.Size = 0

	requests := 0
	for offset := uint64(0); offset < 4*readAmpRequestBytes; offset += readAmpRequestBytes {
		_, err := reader.ReadAtCtx(ctx, offset, readAmpRequestBytes)
		require.NoError(t, err)
		requests++
	}

	assert.Equal(t, int64(requests), counter.reads.Load(),
		"a volume with no cache paid for readahead whose blocks it cannot keep")
}
