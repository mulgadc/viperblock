// Concurrency repro for the env19 AEAD regression: encrypted root volumes
// throwing "cipher: message authentication failed" on cloned volumes under
// load. Drives concurrent partial-block RMW writes (forcing read-modify-write
// reads), concurrent readers, and concurrent DrainToBackend calls (the
// background uploader + flush-handler + snapshot-listener drain vectors) at an
// encrypted file-backend VB, asserting zero ErrIntegrity. File backend is
// read-after-write correct, so a failure here localises the race to
// viperblock's WAL->chunk->map->read logic rather than the S3 backend.
package viperblock

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

func TestConcurrentRMWWriteVsDrainAEAD(t *testing.T) {
	dir := t.TempDir()
	key := testKey(t, 0x42)
	volName := "aead-race"
	cfg := file.FileConfig{BaseDir: dir, VolumeName: volName}
	vb, err := New(&VB{
		VolumeName:          volName,
		VolumeSize:          256 * 1024 * 1024,
		BaseDir:             dir,
		MasterKey:           key,
		EncryptionEnabled:   key != nil,
		WALSyncInterval:     -1, // driven manually below
		ChunkUploadInterval: -1, // driven manually below
		Cache:               Cache{Config: CacheConfig{Size: 8192}},
	}, "file", cfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	vb.BlockSize = DefaultBlockSize
	vb.ObjBlockSize = 8 * DefaultBlockSize // small chunks -> high createChunkFile churn
	require.NoError(t, vb.SaveState())
	require.NoError(t, vb.OpenWAL(&vb.WAL,
		fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL,
		fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))

	const numBlocks = 4096
	blockSize := uint64(vb.BlockSize)

	// Seed every block with a distinct pattern and drain to the backend so
	// later RMW reads resolve against real chunk objects, not the hot buffer.
	seed := make([]byte, numBlocks*blockSize)
	for b := range numBlocks {
		for i := range blockSize {
			seed[uint64(b)*blockSize+i] = byte(b) ^ byte(i)
		}
	}
	require.NoError(t, vb.WriteAt(0, seed))
	require.NoError(t, vb.DrainToBackend())

	var (
		wg       sync.WaitGroup
		stop     atomic.Bool
		aeadErr  atomic.Int64
		otherErr atomic.Int64
		errMu    sync.Mutex
		firstErr error
	)
	rec := func(where string, err error) {
		if err == nil {
			return
		}
		if errors.Is(err, ErrIntegrity) {
			aeadErr.Add(1)
		} else {
			otherErr.Add(1)
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", where, err)
		}
		errMu.Unlock()
	}

	// Partial-block writers: sub-block WriteAt forces the RMW read path.
	for w := range 8 {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed))
			payload := make([]byte, 256)
			for !stop.Load() {
				blk := uint64(rng.IntN(numBlocks))
				for i := range payload {
					payload[i] = byte(rng.IntN(256))
				}
				rec("write", vb.WriteAt(blk*blockSize+128, payload))
			}
		}(uint64(w) + 1)
	}

	// Readers: coalesced multi-block reads across the range.
	for r := range 8 {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed))
			for !stop.Load() {
				blk := uint64(rng.IntN(numBlocks - 16))
				span := uint64(1 + rng.IntN(16))
				_, err := vb.ReadAt(blk*blockSize, span*blockSize)
				rec("read", err)
			}
		}(uint64(r) + 101)
	}

	// Concurrent drainers: mimic the uploader ticker + flush + snapshot drains
	// all racing WriteWALToChunk/createChunkFile at once.
	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				rec("drain", vb.DrainToBackend())
				time.Sleep(2 * time.Millisecond)
			}
		})
	}

	time.Sleep(2 * time.Second)
	stop.Store(true)
	wg.Wait()

	// Final drain + full readback: every block must still decrypt.
	rec("final-drain", vb.DrainToBackend())
	for b := range numBlocks {
		_, err := vb.ReadAt(uint64(b)*blockSize, blockSize)
		rec("final-read", err)
	}

	t.Logf("AEAD(ErrIntegrity) failures=%d otherErrors=%d", aeadErr.Load(), otherErr.Load())
	require.Zero(t, aeadErr.Load(), "AEAD integrity failures under concurrent RMW+drain; first: %v", firstErr)
}
