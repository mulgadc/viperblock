package viperblock

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/require"
)

// A replica can omit an entire epoch from LIST while its objects remain
// accessible by key. Keep the omission scoped to listings of map objects.
type omitMapEpochFromList struct {
	types.Backend

	epoch        uint64
	authorityErr error
	queries      *atomic.Int64
	deletes      *atomic.Int64
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

func (b omitMapEpochFromList) LatestMapHeadCtx(ctx context.Context, prefix string) (string, error) {
	if b.queries != nil {
		b.queries.Add(1)
	}
	if b.authorityErr != nil {
		return "", b.authorityErr
	}
	keys, err := b.Backend.ListObjectsCtx(ctx, strings.TrimSuffix(prefix, "head."))
	if err != nil {
		return "", err
	}
	var latest string
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) {
			latest = max(latest, key)
		}
	}
	return latest, nil
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

	t.Run("unconfirmed authority stops before claim", func(t *testing.T) {
		successor, _ := mv.engine(t, 7)
		successor.AuthoritativeMapHeads = true
		successor.Backend = omitMapEpochFromList{
			Backend: successor.Backend, epoch: 6, authorityErr: context.DeadlineExceeded,
		}
		before := mv.mapKeys(t, mapHeadPrefix)
		require.NoError(t, successor.LoadState())
		require.ErrorIs(t, successor.LoadLiveCheckpoint(), ErrMapHeadAuthority)
		require.Equal(t, before, mv.mapKeys(t, mapHeadPrefix))
	})

	successor, _ := mv.engine(t, 7)
	successor.AuthoritativeMapHeads = true
	successor.Backend = omitMapEpochFromList{Backend: successor.Backend, epoch: 6}
	require.NoError(t, successor.LoadState())
	require.NoError(t, successor.LoadLiveCheckpoint())
	require.NoError(t, successor.ClaimMapCtx(ctx))

	cold, _ := mv.engine(t, 8)
	cold.AuthoritativeMapHeads = true
	cold.Backend = omitMapEpochFromList{Backend: cold.Backend}
	require.NoError(t, cold.LoadState())
	require.NoError(t, cold.LoadLiveCheckpoint())
	require.True(t, cold.mapChain.qualified.Load(), "successor claim reverted activated format")
	require.Equal(t, newest, readBlocks(t, cold, 0, 1), "successor claim hid committed data")
}

func (b omitMapEpochFromList) DeleteKeyCtx(ctx context.Context, key string) error {
	if b.deletes != nil {
		b.deletes.Add(1)
	}
	return b.Backend.DeleteKeyCtx(ctx, key)
}

func (b omitMapEpochFromList) DeleteCtx(ctx context.Context, kind types.FileType, id uint64) error {
	if b.deletes != nil {
		b.deletes.Add(1)
	}
	return b.Backend.DeleteCtx(ctx, kind, id)
}

func TestMapAuthorityCleanupFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name         string
		authorityErr error
	}{
		{"higher epoch hidden from ordinary LIST", nil},
		{"authority unavailable", context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			mv := newMapVolume(t, volumeSize, nil)
			old, _ := mv.create(t, 5)
			writeDrain(t, old, 0, randomBlockData(1))
			writeDrain(t, old, 0, randomBlockData(1))
			old.sweepChunks(t.Context())

			newer := claimQualified(t, mv, 6)
			expected := randomBlockData(1)
			writeDrain(t, newer, 1, expected)
			newer.Detach()
			mv.clock.advance(DefaultMapDeleteDeferral)

			var queries, deletes atomic.Int64
			old.AuthoritativeMapHeads = true
			old.Backend = omitMapEpochFromList{
				Backend: old.Backend, epoch: 6, authorityErr: test.authorityErr,
				queries: &queries, deletes: &deletes,
			}
			old.deleteMapKeys(t.Context(), []string{mapObjectKey(mapHeadPrefix, 5, 0)})
			require.EqualValues(t, 1, queries.Load())
			old.sweepChunks(t.Context())
			require.EqualValues(t, 2, queries.Load(), "both deletion paths consulted authority")
			require.Zero(t, deletes.Load())

			cold, _ := mv.engine(t, 7)
			cold.WAL.BaseDir = t.TempDir()
			cold.AuthoritativeMapHeads = true
			cold.Backend = omitMapEpochFromList{Backend: cold.Backend}
			require.NoError(t, cold.LoadState())
			require.NoError(t, cold.LoadLiveCheckpoint())
			require.Equal(t, expected, readBlocks(t, cold, 1, 1))
		})
	}
}
