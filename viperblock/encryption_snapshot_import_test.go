// Import-ordering snapshot/clone encryption tests. These reproduce the EKS
// AMI-clone AEAD failure ("cipher: message authentication failed") seen at
// runtime on encrypted volumes cloned from an imported AMI snapshot.
//
// ImportDiskImage builds the AMI by calling viperblock.New (which starts the
// 30s background chunk uploader), running a write loop that itself calls
// WriteWALToChunk, then CreateSnapshot — all BEFORE vb.Close() stops the
// uploader. So the import's WriteWALToChunk races the uploader's concurrent
// DrainToBackend. createChunkFile takes chunkIndex = ObjectNum.Load() and only
// ObjectNum.Add(1) at the end, with no mutex, so two concurrent sealers can
// grab the SAME ObjectID and one chunk overwrites the other in S3. The block
// map then records {ObjectID, offset, seqNum} for blocks whose ciphertext was
// clobbered, and every clone that reads those inherited blocks fails tag
// verify. TestSnapshotEncrypted_ImportRaceCorruptsBaseMap reproduces that race.

package viperblock

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// openImportOrderVBInDir builds an encrypted VB rooted at dir the way
// ImportDiskImage does: LoadState/LoadBlockState, OpenWAL for both WALs, then
// SaveState to mint the VolumeUUID before the first seal. The background chunk
// uploader is left RUNNING (as in production) so callers can exercise the race;
// pass a tiny uploadInterval to make ticks land during the write loop.
func openImportOrderVBInDir(t *testing.T, dir, volumeName string, key *masterkey.Key, uploadInterval time.Duration) *VB {
	t.Helper()
	cfg := file.FileConfig{BaseDir: dir, VolumeName: volumeName}
	vb, err := New(&VB{
		VolumeName:          volumeName,
		VolumeSize:          256 * 1024 * 1024,
		BaseDir:             dir,
		MasterKey:           key,
		EncryptionEnabled:   key != nil,
		WALSyncInterval:     -1,
		ChunkUploadInterval: uploadInterval,
	}, "file", cfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	vb.BlockSize = DefaultBlockSize
	vb.ObjBlockSize = 16 * DefaultBlockSize

	_ = vb.LoadState()
	require.NoError(t, vb.LoadBlockState())
	require.NoError(t, vb.OpenWAL(&vb.WAL,
		fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL,
		fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))
	if vb.EncryptionEnabled {
		require.NoError(t, vb.SaveState())
	}
	return vb
}

// TestSnapshotEncrypted_ImportRaceCorruptsBaseMap reproduces the AMI-import
// data race: the import write loop calls WriteWALToChunk while the background
// chunk uploader concurrently calls DrainToBackend on the same VB. Concurrent
// createChunkFile calls collide on ObjectNum, one chunk overwrites another, and
// the snapshot block map ends up pointing at clobbered ciphertext. A clone then
// fails to decrypt the inherited base blocks — exactly the runtime symptom.
func TestSnapshotEncrypted_ImportRaceCorruptsBaseMap(t *testing.T) {
	dir := t.TempDir()
	key := testKey(t, 0x77)

	// Uploader interval tiny so real background ticks race the write loop, and
	// a manual DrainToBackend hammer for deterministic collisions.
	src := openImportOrderVBInDir(t, dir, "src-import-race", key, time.Millisecond)

	bs := uint64(src.BlockSize)
	nBlocks := uint64(2000)
	plain := make([][]byte, nBlocks)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = src.DrainToBackend()
			}
		}
	}()

	// Import-style write loop: write each block, periodically WriteWALToChunk.
	for b := uint64(0); b < nBlocks; b++ {
		buf := make([]byte, bs)
		_, err := rand.Read(buf)
		require.NoError(t, err)
		plain[b] = buf
		require.NoError(t, src.WriteAt(b*bs, buf))
		if b%64 == 0 {
			require.NoError(t, src.Flush())
			require.NoError(t, src.WriteWALToChunk(true))
		}
	}

	// Quiesce all background drains BEFORE snapshotting, mirroring the fixed
	// ImportDiskImage flow. The corruption under test is baked in during the
	// concurrent write loop above (racing WriteWALToChunk calls colliding on
	// ObjectNum); CreateSnapshot merely freezes whatever block map resulted.
	close(stop)
	wg.Wait()
	// Stop the background goroutines WITHOUT Close(): on the file backend
	// Close()->RemoveLocalFiles() deletes <BaseDir>/<volume>, which holds the
	// chunk files the clone must read. In production chunks live in S3, so
	// RemoveLocalFiles only clears the local WAL cache.
	src.StopChunkUploader()
	src.StopWALSyncer()

	snapshotID := "snap-src-import-race"
	_, err := src.CreateSnapshot(snapshotID)
	require.NoError(t, err)

	// Clone and read every inherited base block in one multi-block read.
	clone := openEncryptedVBInDir(t, dir, "clone-import-race", key)
	require.NoError(t, clone.OpenFromSnapshot(snapshotID))

	got, err := clone.ReadAt(0, nBlocks*bs)
	require.NoError(t, err, "clone multi-block read of inherited base blocks failed")
	for b := uint64(0); b < nBlocks; b++ {
		require.True(t, bytes.Equal(plain[b], got[b*bs:(b+1)*bs]),
			"inherited base block %d plaintext mismatch (chunk overwrite corruption)", b)
	}
}
