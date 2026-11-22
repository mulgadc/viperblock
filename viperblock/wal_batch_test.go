package viperblock

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/require"
)

func TestRollbackFailureStopsAppendSyncAndRotation(t *testing.T) {
	vb, _ := newEnospcTestVB(t)
	vb.WAL.appendFile = func(f *os.File, data []byte) (int, error) {
		n, err := f.Write(data[:7])
		if err != nil {
			return n, err
		}
		return n, syscall.ENOSPC
	}
	vb.WAL.truncateFile = func(*os.File, int64) error { return syscall.EIO }

	payload := bytes.Repeat([]byte{0x42}, int(vb.BlockSize))
	require.NoError(t, vb.WriteAt(0, payload))
	require.NoError(t, vb.WriteAt(uint64(vb.BlockSize), payload))
	err := vb.Flush()
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.ErrorIs(t, err, syscall.EIO)
	require.ErrorContains(t, err, "WAL rollback")
	active := vb.WAL.DB[len(vb.WAL.DB)-1]
	info, err := active.Stat()
	require.NoError(t, err)

	err = vb.Flush()
	require.ErrorIs(t, err, syscall.EIO)
	require.ErrorIs(t, vb.syncWAL(), syscall.EIO)
	require.ErrorIs(t, vb.WriteWALToChunk(true), syscall.EIO)
	after, err := active.Stat()
	require.NoError(t, err)
	require.Equal(t, info.Size(), after.Size(), "the failed generation must receive no further writes")
	require.Len(t, vb.Writes.Blocks, 2)
	require.Empty(t, vb.PendingBackendWrites.Blocks)
}

func TestFailedSecondBatchRetainsRepeatedBlockForRetry(t *testing.T) {
	vb, _ := newEnospcTestVB(t)
	bs := uint64(vb.BlockSize)
	payload := bytes.Repeat([]byte{0x50}, int(bs))
	newer := bytes.Repeat([]byte{0x99}, int(bs))
	for i := range uint64(69) {
		require.NoError(t, vb.WriteAt(i*bs, payload))
	}
	require.NoError(t, vb.WriteAt(0, newer))

	appends := 0
	vb.WAL.appendFile = func(f *os.File, data []byte) (int, error) {
		appends++
		if appends == 1 {
			return f.Write(data)
		}
		n, err := f.Write(data[:19])
		if err != nil {
			return n, err
		}
		return n, syscall.ENOSPC
	}
	require.ErrorIs(t, vb.Flush(), syscall.ENOSPC)
	require.Len(t, vb.PendingBackendWrites.Blocks, 63)
	require.Len(t, vb.Writes.Blocks, 7)
	require.Equal(t, int64(70)*int64(bs), vb.pendingBytes.Load())
	got, err := vb.ReadAt(0, bs)
	require.NoError(t, err)
	require.Equal(t, newer, got)

	vb.WAL.appendFile = nil
	require.NoError(t, vb.Flush())
	require.Empty(t, vb.Writes.Blocks)
	require.Len(t, vb.PendingBackendWrites.Blocks, 70)
	require.NoError(t, vb.DrainToBackend())
	require.Empty(t, vb.PendingBackendWrites.Blocks)
	require.Zero(t, vb.pendingBytes.Load())
	got, err = vb.ReadAt(0, bs)
	require.NoError(t, err)
	require.Equal(t, newer, got)
}

func TestRotationFailureKeepsCurrentGenerationWritable(t *testing.T) {
	vb, _ := newEnospcTestVB(t)
	initial := vb.WAL.WallNum.Load()
	active := vb.WAL.DB[len(vb.WAL.DB)-1]
	next := filepath.Join(vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, initial+1, vb.GetVolume()))
	payload := bytes.Repeat([]byte{0x55}, int(vb.BlockSize))
	require.NoError(t, vb.WriteAt(0, payload))
	require.NoError(t, vb.Flush())

	require.NoError(t, os.WriteFile(next, []byte("occupied"), 0600))
	require.ErrorIs(t, vb.WriteWALToChunk(true), ErrWALAlreadyOpen)
	require.Equal(t, initial, vb.WAL.WallNum.Load())
	require.Same(t, active, vb.WAL.DB[len(vb.WAL.DB)-1])
	require.NoError(t, vb.WriteAt(uint64(vb.BlockSize), payload))
	require.NoError(t, vb.Flush())
	require.NoError(t, os.Remove(next))
	require.NoError(t, vb.DrainToBackend())

	got, err := vb.ReadAt(0, 2*uint64(vb.BlockSize))
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat(payload, 2), got)
}

func TestOlderUploadPreservesNewerPendingPayload(t *testing.T) {
	vb, backend := newEnospcTestVB(t)
	bs := uint64(vb.BlockSize)
	previous := bytes.Repeat([]byte{0x25}, int(bs))
	next := bytes.Repeat([]byte{0x35}, int(bs))
	require.NoError(t, vb.WriteAt(0, previous))
	require.NoError(t, vb.Flush())

	upload := make(chan struct{})
	resume := make(chan struct{})
	backend.afterWrite = func(kind types.FileType, err error) {
		if kind == types.FileTypeChunk && err == nil {
			close(upload)
			<-resume
		}
	}
	result := make(chan error, 1)
	go func() { result <- vb.WriteWALToChunk(true) }()
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()
	select {
	case <-upload:
	case <-time.After(3 * time.Second):
		t.Fatal("chunk upload did not start")
	}
	require.NoError(t, vb.WriteAt(0, next))
	require.NoError(t, vb.Flush())
	close(resume)
	require.NoError(t, <-result)
	backend.afterWrite = nil
	require.Len(t, vb.PendingBackendWrites.Blocks, 1)
	require.Equal(t, next, vb.PendingBackendWrites.Blocks[0].Data)
	require.Equal(t, int64(bs), vb.pendingBytes.Load())
	require.NoError(t, vb.DrainToBackend())
	require.Zero(t, vb.pendingBytes.Load())
	got, err := vb.ReadAt(0, bs)
	require.NoError(t, err)
	require.Equal(t, next, got)
}

func legacyWALRecord(vb *VB, block Block) []byte {
	headerSize := 28
	capacity := headerSize + len(block.Data)
	if vb.EncryptionEnabled {
		headerSize = 24
		capacity = headerSize + len(block.Data) + 16
	}
	record := make([]byte, headerSize, capacity)
	binary.BigEndian.PutUint64(record[0:8], block.SeqNum)
	binary.BigEndian.PutUint64(record[8:16], block.Block)
	binary.BigEndian.PutUint64(record[16:24], block.Len)
	if vb.EncryptionEnabled {
		nonce := makeNonce(block.SeqNum, vb.VolumeUUID, DomainWAL)
		var aad [AADLen]byte
		initAAD(&aad, vb.volumeNameHash)
		updateAAD(&aad, block.Block, block.SeqNum)
		return vb.aead.Seal(record, nonce[:], block.Data, aad[:])
	}
	checksum := crc32.ChecksumIEEE(record[:24])
	binary.BigEndian.PutUint32(record[24:28], crc32.Update(checksum, crc32.IEEETable, block.Data))
	return append(record, block.Data...)
}

func walTestBlocks(count int) []Block {
	blocks := make([]Block, count)
	for i := range blocks {
		blocks[i] = Block{Block: uint64(i), SeqNum: uint64(i + 1), Len: 4096, Data: bytes.Repeat([]byte{byte(i) + 1}, 4096)}
	}
	return blocks
}

func TestWALCodecExactBytes(t *testing.T) {
	vb := newEncryptedTamperEnv(t, "vol-wal-codec-review", testKey(t, 0x51)).vb
	blocks := walTestBlocks(64)
	for _, encrypted := range []bool{true, false} {
		vb.EncryptionEnabled = encrypted
		var batch, expected []byte
		for _, block := range blocks {
			batch = vb.appendWALRecord(batch, block)
			expected = append(expected, legacyWALRecord(vb, block)...)
		}
		require.Equal(t, expected, batch)
	}
	vb.EncryptionEnabled = true
}

func TestFailedSuccessorHeaderCanBeRetried(t *testing.T) {
	mv := newMapVolume(t, volumeSize, nil)
	vb, backend := mv.create(t, 5)
	initial := vb.WAL.WallNum.Load()
	active := vb.WAL.DB[len(vb.WAL.DB)-1].Name()
	next := filepath.Join(vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, initial+1, vb.GetVolume()))
	data := bytes.Repeat([]byte{0x79}, int(vb.BlockSize))
	require.NoError(t, vb.WriteAt(0, data))
	require.NoError(t, vb.Flush())

	vb.WAL.appendFile = func(f *os.File, body []byte) (int, error) {
		n, err := f.Write(body[:7])
		if err != nil {
			return n, err
		}
		return n, syscall.ENOSPC
	}
	require.ErrorIs(t, vb.WriteWALToChunk(true), syscall.ENOSPC)
	require.NoFileExists(t, next)
	require.FileExists(t, active)
	require.Equal(t, initial, vb.WAL.WallNum.Load())

	vb.WAL.appendFile = nil
	require.NoError(t, vb.WriteWALToChunk(true))
	require.FileExists(t, active, "upload alone cannot reclaim the WAL")
	backend.failHead.Store(true)
	require.Error(t, vb.DrainToBackend())
	require.FileExists(t, active, "a failed head publication cannot reclaim the WAL")
	backend.failHead.Store(false)
	require.NoError(t, vb.DrainToBackend())
	require.NoFileExists(t, active)
	vb.Detach()
	reopened, _, err := mv.open(t, 6)
	require.NoError(t, err)
	require.Equal(t, data, readBlocks(t, reopened, 0, 1))
}

func TestInitialOpenAcceptsEmptyWAL(t *testing.T) {
	vb, _ := newEnospcTestVB(t)
	path := filepath.Join(t.TempDir(), "wal")
	require.NoError(t, os.WriteFile(path, nil, 0600))
	wal := &WAL{WALMagic: vb.WAL.WALMagic}
	require.NoError(t, vb.OpenWAL(wal, path))
	t.Cleanup(func() { _ = wal.DB[0].Close() })
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(vb.WALHeaderSize()), info.Size())
}

func TestBarrierWaitingForSyncObservesRollbackFailure(t *testing.T) {
	vb, _ := newEnospcTestVB(t)
	bs := uint64(vb.BlockSize)
	data := bytes.Repeat([]byte{0xab}, int(bs))
	require.NoError(t, vb.WriteAt(0, data))
	appendFinished := make(chan struct{})
	appends := 0
	vb.WAL.appendFile = func(f *os.File, body []byte) (int, error) {
		appends++
		if appends == 1 {
			n, err := f.Write(body)
			close(appendFinished)
			return n, err
		}
		n, err := f.Write(body[:7])
		if err != nil {
			return n, err
		}
		return n, syscall.ENOSPC
	}
	vb.WAL.truncateFile = func(*os.File, int64) error { return syscall.EIO }
	vb.WAL.syncMu.Lock()
	barrier := make(chan error, 1)
	go func() { barrier <- vb.Flush() }()
	defer vb.WAL.syncMu.Unlock()
	select {
	case <-appendFinished:
	case <-time.After(3 * time.Second):
		t.Fatal("first flush never appended")
	}
	require.NoError(t, vb.WriteAt(bs, data))
	require.ErrorIs(t, vb.flushWrites(), syscall.EIO)
	hadDirty := vb.WAL.dirty.Swap(false)
	defer vb.WAL.dirty.Store(hadDirty)

	vb.WAL.syncMu.Unlock()
	err := <-barrier
	vb.WAL.syncMu.Lock()
	require.ErrorIs(t, err, syscall.EIO)
}

func TestFailedShardPublishesOnlyCompletedRecords(t *testing.T) {
	vb := newShardedFlushTestVB(t)
	bs := uint64(vb.BlockSize)
	first := bytes.Repeat([]byte{0x21}, int(bs))
	second := bytes.Repeat([]byte{0x22}, int(bs))
	require.NoError(t, vb.WriteAt(0, first))
	require.NoError(t, vb.WriteAt(0, second))
	calls := 0
	err := vb.flushShards(append([]Block(nil), vb.Writes.Blocks...), func(block Block) error {
		calls++
		if calls > 1 {
			return syscall.ENOSPC
		}
		return vb.WriteShardedWAL(block)
	})
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.Len(t, vb.PendingBackendWrites.Blocks, 1)
	require.Equal(t, first, vb.PendingBackendWrites.Blocks[0].Data)
	require.Len(t, vb.Writes.Blocks, 1)
	require.Equal(t, second, vb.Writes.Blocks[0].Data)
	require.NoError(t, vb.Flush())
	require.Len(t, vb.PendingBackendWrites.Blocks, 2)
	require.Equal(t, second, vb.PendingBackendWrites.Blocks[1].Data)
}

func TestLegacyReadAndPartialWriteBeforeChunkMapPublication(t *testing.T) {
	vb, _ := newEnospcTestVB(t)
	vb.UseBlockStore = false
	bs := uint64(vb.BlockSize)
	initial := bytes.Repeat([]byte{0x69}, int(bs))
	require.NoError(t, vb.WriteAt(0, initial))
	require.NoError(t, vb.Flush())

	arrived := make(chan struct{})
	resume := make(chan struct{})
	vb.beforeChunkMap = func() {
		close(arrived)
		<-resume
	}
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()
	upload := make(chan error, 1)
	go func() { upload <- vb.WriteWALToChunk(true) }()
	select {
	case <-arrived:
	case <-time.After(3 * time.Second):
		t.Fatal("upload did not reach the map handoff")
	}

	got, err := vb.ReadAt(0, bs)
	require.NoError(t, err)
	require.Equal(t, initial, got)
	patch := []byte{1, 2, 3, 4}
	require.NoError(t, vb.WriteAt(20, patch))
	close(resume)
	require.NoError(t, <-upload)
	vb.beforeChunkMap = nil

	require.NoError(t, vb.DrainToBackend())
	got, err = vb.ReadAt(0, bs)
	require.NoError(t, err)
	copy(initial[20:], patch)
	require.Equal(t, initial, got)
}
