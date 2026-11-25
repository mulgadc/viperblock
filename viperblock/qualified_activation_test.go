package viperblock

import (
	"context"
	"slices"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/require"
)

// A replica can omit an entire epoch from LIST while its objects remain
// accessible by key. Keep the omission scoped to listings of map objects.
type omitMapEpochFromList struct {
	types.Backend

	epoch uint64
}

func (b omitMapEpochFromList) ListObjectsCtx(ctx context.Context, prefix string) ([]string, error) {
	keys, err := b.Backend.ListObjectsCtx(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(keys, func(key string) bool {
		epoch, _, ok := parseMapObjectKey(key, mapHeadPrefix)
		return ok && epoch == b.epoch
	}), nil
}

func TestQualifiedActivationDoesNotRevertWhenListingOmitsEpoch(t *testing.T) {
	ctx := t.Context()
	mv := newMapVolume(t, volumeSize, nil)
	seed, _ := mv.create(t, 5)
	writeDrain(t, seed, 0, randomBlockData(1))
	seed.Detach()

	activated := claimQualified(t, mv, 6)
	newest := randomBlockData(1)
	writeDrain(t, activated, 0, newest)
	activated.Detach()

	successor, _ := mv.engine(t, 7)
	successor.Backend = omitMapEpochFromList{Backend: successor.Backend, epoch: 6}
	require.NoError(t, successor.LoadState())
	if err := successor.LoadLiveCheckpoint(); err != nil {
		return
	}
	if err := successor.ClaimMapCtx(ctx); err != nil {
		return
	}

	cold, _ := mv.engine(t, 8)
	require.NoError(t, cold.LoadState())
	require.NoError(t, cold.LoadLiveCheckpoint())
	require.True(t, cold.mapChain.qualified.Load(), "successor claim reverted activated format")
	require.Equal(t, newest, readBlocks(t, cold, 0, 1), "successor claim hid committed data")
}
