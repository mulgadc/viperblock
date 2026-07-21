package viperblock

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// newRMWVB is a minimal local VB (file backend, no background drains) so the
// test isolates WriteAtCtx's read-modify-write and nothing else.
func newRMWVB(t *testing.T) *VB {
	t.Helper()
	root := t.TempDir()
	vol := fmt.Sprintf("rmw-%d", time.Now().UnixNano())
	cfg := VB{
		VolumeName:          vol,
		VolumeSize:          8 * 1024 * 1024,
		BaseDir:             root + "/viperblock",
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		Cache:               Cache{Config: CacheConfig{Size: 0}},
	}
	vb, err := New(&cfg, FileBackend, file.FileConfig{
		VolumeName: vol, VolumeSize: 8 * 1024 * 1024, BaseDir: root,
	})
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))
	return vb
}

// TestRMW_ConcurrentSubBlockWritesLoseAnUpdate drives two concurrent writes to
// DISJOINT halves of the SAME 4096-byte block -- the shape a guest filesystem
// produces whenever its block boundaries do not line up with viperblock's, so
// two adjacent page writebacks land inside one viperblock block.
//
// WriteAtCtx handles a sub-block write by reading the current block, splicing
// the new bytes in, and appending a whole new 4096-byte block. That read and
// the append are NOT performed under a common lock, so two writers can both
// read generation N, each splice their own half, and append; the higher SeqNum
// wins wholesale and the other writer's bytes are silently discarded.
//
// The surviving block is byte-exact with generation N over exactly the range
// the losing write covered -- the reported signature.
func TestRMW_ConcurrentSubBlockWritesLoseAnUpdate(t *testing.T) {
	vb := newRMWVB(t)
	bs := int(vb.BlockSize)
	half := bs / 2

	// Generation N: the whole block is 0x11.
	gen0 := bytes.Repeat([]byte{0x11}, bs)
	require.NoError(t, vb.WriteAt(0, append([]byte(nil), gen0...)))

	lower := bytes.Repeat([]byte{0xAA}, half) // written to [0, 2048)
	upper := bytes.Repeat([]byte{0xBB}, half) // written to [2048, 4096)

	// Release both writers at once so their RMW reads overlap.
	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	errs := make([]error, 2)

	done.Add(2)
	go func() {
		defer done.Done()
		start.Wait()
		errs[0] = vb.WriteAt(0, append([]byte(nil), lower...))
	}()
	go func() {
		defer done.Done()
		start.Wait()
		errs[1] = vb.WriteAt(uint64(half), append([]byte(nil), upper...))
	}()
	start.Done()
	done.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	got, err := vb.ReadAt(0, uint64(bs))
	require.NoError(t, err)

	lowerOK := bytes.Equal(got[:half], lower)
	upperOK := bytes.Equal(got[half:], upper)
	lowerReverted := bytes.Equal(got[:half], gen0[:half])
	upperReverted := bytes.Equal(got[half:], gen0[half:])

	t.Logf("lower half correct=%v reverted-to-previous-generation=%v", lowerOK, lowerReverted)
	t.Logf("upper half correct=%v reverted-to-previous-generation=%v", upperOK, upperReverted)

	if !lowerOK || !upperOK {
		t.Fatalf("LOST UPDATE: two concurrent sub-block writes to the same block, "+
			"only one survived (lowerOK=%v upperOK=%v); the losing write's range is "+
			"byte-exact with the previous generation (lowerReverted=%v upperReverted=%v)",
			lowerOK, upperOK, lowerReverted, upperReverted)
	}
}

// TestRMW_ConcurrentSubBlockWrites_Repeated runs the same race repeatedly to
// show how readily it lands, and reports the observed loss rate.
func TestRMW_ConcurrentSubBlockWrites_Repeated(t *testing.T) {
	const rounds = 200
	lost := 0

	for r := range rounds {
		vb := newRMWVB(t)
		bs := int(vb.BlockSize)
		half := bs / 2

		gen0 := bytes.Repeat([]byte{0x11}, bs)
		require.NoError(t, vb.WriteAt(0, append([]byte(nil), gen0...)))

		lower := bytes.Repeat([]byte{byte(0xA0 + r%16)}, half)
		upper := bytes.Repeat([]byte{byte(0xB0 + r%16)}, half)

		var start sync.WaitGroup
		start.Add(1)
		var done sync.WaitGroup
		done.Add(2)
		go func() { defer done.Done(); start.Wait(); _ = vb.WriteAt(0, append([]byte(nil), lower...)) }()
		go func() {
			defer done.Done()
			start.Wait()
			_ = vb.WriteAt(uint64(half), append([]byte(nil), upper...))
		}()
		start.Done()
		done.Wait()

		got, err := vb.ReadAt(0, uint64(bs))
		require.NoError(t, err)
		if !bytes.Equal(got[:half], lower) || !bytes.Equal(got[half:], upper) {
			lost++
		}
	}

	t.Logf("lost updates: %d of %d rounds (%.1f%%)", lost, rounds, 100*float64(lost)/float64(rounds))
	if lost > 0 {
		t.Fatalf("LOST UPDATE reproduced in %d of %d rounds", lost, rounds)
	}
}
