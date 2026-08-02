// Dedicated regression coverage for mulga-w1iu8 defect 2: openWALLocked used
// to unconditionally append a fresh WAL header on every open, even to an
// existing populated WAL. readWALFileForRecovery only validates the header
// once, at offset 0, then reads fixed-size records for the rest of the
// file -- a second header landing mid-stream desyncs every record after it.
// This is the same concurrent-open vector as defect 1 (two independently
// constructed *VB instances computing the same walNum and racing OpenWAL on
// one file), isolated here from the flock fix so it is provable on its own.
package viperblock

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenWAL_RefusesReopenOfNonEmptyFile is the direct A/B test for defect
// 2: a second OpenWAL against a file that already has header+content must
// be refused with ErrWALAlreadyOpen and must not touch the file, rather than
// silently appending a second header into the middle of the record stream.
func TestOpenWAL_RefusesReopenOfNonEmptyFile(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-wal-guard", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.EnsureVolumeUUID())

	walPath := filepath.Join(vb.BaseDir, types.GetFilePath(types.FileTypeWALChunk, 1, vb.GetVolume()))

	require.NoError(t, vb.OpenWAL(&vb.WAL, walPath))
	info, err := os.Stat(walPath)
	require.NoError(t, err)
	headerSize := info.Size()
	assert.Equal(t, int64(vb.WALHeaderSize()), headerSize, "first open must write exactly one header")

	// A second opener targeting the SAME file -- modelling the concurrent
	// double-open race -- must be refused outright.
	second := &WAL{WALMagic: vb.WAL.WALMagic, BaseDir: vb.WAL.BaseDir}
	err = vb.OpenWAL(second, walPath)
	require.Error(t, err, "a second open of an already header-stamped WAL must fail")
	assert.ErrorIs(t, err, ErrWALAlreadyOpen)

	// The refused second open must be a pure read: file untouched.
	info2, statErr := os.Stat(walPath)
	require.NoError(t, statErr)
	assert.Equal(t, headerSize, info2.Size(),
		"refused second open must not append or truncate the existing WAL")
}

// TestOpenWAL_ConcurrentDoubleOpenNeverCorrupts drives two goroutines at the
// SAME wal filename concurrently (no serialization between them, unlike the
// flock-gated integration test) and asserts the file is left in exactly one
// of two safe states: either one opener won and the other was cleanly
// refused, or (TOCTOU on the empty-file check) both raced through and wrote
// -- in which case readWALFileForRecovery must still not silently misparse
// the result into fabricated blocks. It must fail loudly (ErrIntegrity /
// version-mismatch/etc.) rather than return corrupt-but-plausible data.
func TestOpenWAL_ConcurrentDoubleOpenNeverCorrupts(t *testing.T) {
	key := testKey(t, 0x42)
	vb := newFileBackedVB(t, "vol-wal-race", key)
	vb.BlockSize = DefaultBlockSize
	require.NoError(t, vb.EnsureVolumeUUID())

	walPath := filepath.Join(vb.BaseDir, types.GetFilePath(types.FileTypeWALChunk, 1, vb.GetVolume()))

	results := make(chan error, 2)
	for range 2 {
		go func() {
			w := &WAL{WALMagic: vb.WAL.WALMagic, BaseDir: vb.WAL.BaseDir}
			results <- vb.OpenWAL(w, walPath)
		}()
	}

	var oks, refused int
	for range 2 {
		if err := <-results; err == nil {
			oks++
		} else {
			refused++
		}
	}

	// At least one must have won (created the file); the other either lost
	// cleanly or (rare TOCTOU) also "won" its own os.CreateTemp/Stat race.
	// Either way, the file must never end up unparseable-but-silent.
	assert.GreaterOrEqual(t, oks, 1)
	t.Logf("concurrent double-open: %d succeeded, %d refused", oks, refused)

	blocks, _, err := vb.readWALFileForRecovery(walPath)
	if err != nil {
		// A loud failure (header/version mismatch, integrity error) is the
		// acceptable failure mode -- never silent corruption.
		t.Logf("post-race readWALFileForRecovery reported (acceptable, fail-closed): %v", err)
		return
	}
	assert.Empty(t, blocks, "a header-only WAL must recover zero blocks, not fabricated ones")
}
