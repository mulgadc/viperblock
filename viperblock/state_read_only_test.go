// What separates reading a volume's state from opening the volume. LoadState
// claims a SeqNum window and durably persists it, which is correct for a
// writer and wrong for a describe: it makes a read cost a PutObject and puts
// that write on a node that does not own the volume.
package viperblock

import (
	"context"
	"testing"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/require"
)

// readerOnSharedStore builds a state reader against the shared store. It takes
// no cleanup because there is nothing to stop — NewStateReader starts no
// background goroutines.
func readerOnSharedStore(t *testing.T, store, localDir, volume string, key *masterkey.Key) *VB {
	t.Helper()
	vb, err := NewStateReader(&VB{
		VolumeName:        volume,
		VolumeSize:        sharedStoreVolumeSize,
		BaseDir:           localDir,
		MasterKey:         key,
		EncryptionEnabled: true,
	}, "file", file.FileConfig{BaseDir: store, VolumeName: volume})
	require.NoError(t, err)
	require.NoError(t, vb.InitBackendForRead(context.Background()))
	return vb
}

// persistedHighWater reads the durable SeqNum ceiling without disturbing it,
// which is only possible because ReadState does not disturb it.
func persistedHighWater(t *testing.T, store, volume string, key *masterkey.Key) uint64 {
	t.Helper()
	state, err := readerOnSharedStore(t, store, t.TempDir(), volume, key).ReadStateCtx(context.Background())
	require.NoError(t, err)
	return state.SeqNumHighWater
}

// TestReadState_ClaimsNoSeqNumWindow is the whole point of the read-only path.
// A reader issues no SeqNum, so it needs no reservation, and burning one on
// every describe costs a durable write to answer a question about capacity.
func TestReadState_ClaimsNoSeqNumWindow(t *testing.T) {
	store := t.TempDir()
	key := testKey(t, 0x53)
	const volume = "vol-readonly00001"

	seedSharedVolume(t, store, t.TempDir(), volume, key)

	// A real open first, so there is a claimed window for a reader to leave
	// alone rather than a pristine zero.
	opener := openOnSharedStore(t, store, t.TempDir(), volume, key)
	require.NoError(t, opener.LoadState())

	before := persistedHighWater(t, store, volume, key)
	require.NotZero(t, before, "the seeded volume was never opened for writing")

	for range 3 {
		state, err := readerOnSharedStore(t, store, t.TempDir(), volume, key).ReadStateCtx(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(sharedStoreVolumeSize), state.VolumeSize)
	}

	require.Equal(t, before, persistedHighWater(t, store, volume, key),
		"reading state advanced the durable SeqNum high-water, so a describe writes")
}

// TestReadState_StillRejectsTheWrongKey keeps the cheap path honest. It has to
// run the same envelope verification a full open does, or it is not reading
// the volume's state, it is reading whatever the store handed back.
func TestReadState_StillRejectsTheWrongKey(t *testing.T) {
	store := t.TempDir()
	key := testKey(t, 0x54)
	const volume = "vol-readonly00002"

	seedSharedVolume(t, store, t.TempDir(), volume, key)

	_, err := readerOnSharedStore(t, store, t.TempDir(), volume, testKey(t, 0x55)).ReadStateCtx(context.Background())
	require.ErrorIs(t, err, ErrEncryptionMismatch)
}
