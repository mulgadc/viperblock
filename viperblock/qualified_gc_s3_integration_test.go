//go:build integration

package viperblock

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/require"
)

type wireGCSnapshotBackend struct {
	*s3.Backend

	hidden  string
	deletes atomic.Int64
}

func (b *wireGCSnapshotBackend) ListPrefixesCtx(ctx context.Context, prefix string) ([]string, error) {
	names, err := b.Backend.ListPrefixesCtx(ctx, prefix)
	return slices.DeleteFunc(names, func(name string) bool { return name == b.hidden }), err
}

func (b *wireGCSnapshotBackend) DeleteChunkCtx(ctx context.Context, ref types.ChunkRef) error {
	b.deletes.Add(1)
	return b.Backend.DeleteChunkCtx(ctx, ref)
}

func (b *wireGCSnapshotBackend) DeleteCtx(ctx context.Context, kind types.FileType, id uint64) error {
	if kind == types.FileTypeChunk {
		b.deletes.Add(1)
	}
	return b.Backend.DeleteCtx(ctx, kind, id)
}

func TestWireQualifiedSnapshotGC(t *testing.T) {
	endpoint := os.Getenv("VB_TEST_AUTHORITY_ENDPOINT")
	if endpoint == "" {
		t.Skip("set VB_TEST_AUTHORITY_ENDPOINT to a disposable authority-capable Predastore gateway")
	}
	ctx := t.Context()
	run := time.Now().UnixNano()
	ca, err := os.ReadFile(os.Getenv("VB_TEST_AUTHORITY_CA"))
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(ca))
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}}
	for _, closeSweep := range []bool{false, true} {
		t.Run(fmt.Sprintf("close=%v", closeSweep), func(t *testing.T) {
			vol := fmt.Sprintf("vol-gc-wire-%x-%v", run, closeSweep)
			snap := fmt.Sprintf("snap-gc-wire-%x-%v", run, closeSweep)
			now := time.Now()
			engine := func(name string, epoch uint64) *VB {
				vb, err := New(&VB{
					VolumeName: name, VolumeSize: volumeSize, BaseDir: t.TempDir(),
					WALSyncInterval: -1, ChunkUploadInterval: -1, GCInterval: -1, GCEnabled: true,
					QualifiedChunks: true, LeaseEpoch: epoch, LaunchChunkIDs: &atomic.Uint64{},
				}, S3Backend, s3.S3Config{
					VolumeName: name, VolumeSize: volumeSize, Region: "ap-southeast-2", Bucket: "predastore",
					AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"), SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
					Host: endpoint, HTTPClient: client,
				})
				require.NoError(t, err)
				t.Cleanup(vb.Detach)
				vb.mapClock = func() time.Time { return now }
				require.NoError(t, vb.Backend.Init())
				return vb
			}
			seed := engine(vol, 5)
			require.NoError(t, seed.InitMapDeltaCtx(ctx, true))
			require.NoError(t, seed.SaveState())
			seed.Detach()
			open := func(epoch uint64, claim bool) *VB {
				vb := engine(vol, epoch)
				require.NoError(t, vb.LoadState())
				require.NoError(t, vb.LoadLiveCheckpoint())
				if claim {
					require.NoError(t, vb.ClaimMapCtx(ctx))
					require.NoError(t, vb.RecoverLocalWALs())
					openMapWALs(t, vb)
				}
				return vb
			}
			first := open(6, true)
			before := randomBlockData(1)
			writeDrain(t, first, 0, before)
			pinned, _, _, err := first.lookupChunk(0)
			require.NoError(t, err)
			require.NotZero(t, pinned.Epoch)
			_, err = first.CreateSnapshot(snap)
			require.NoError(t, err)
			first.Detach()
			second := open(7, true)
			after := randomBlockData(1)
			writeDrain(t, second, 0, after)
			second.Detach()
			collector := open(8, true)
			raw := collector.Backend.(*s3.Backend)
			require.True(t, raw.MapHeadAuthorityRequired())
			backend := &wireGCSnapshotBackend{Backend: raw, hidden: snap}
			collector.Backend = backend
			names, err := raw.ListPrefixesCtx(ctx, snapPrefix)
			require.NoError(t, err)
			require.Contains(t, names, snap)
			visible, err := backend.ListPrefixesCtx(ctx, snapPrefix)
			require.NoError(t, err)
			require.NotContains(t, visible, snap)
			firstSeen := now
			collector.runGCSweep(ctx)
			require.Equal(t, firstSeen, collector.mapChain.chunkSeen[pinned], "pruned candidate must enter the deferral")
			now = now.Add(DefaultMapDeleteDeferral)
			if closeSweep {
				require.NoError(t, collector.CloseCtx(ctx))
			} else {
				collector.runGCSweep(ctx)
			}

			restore := engine("vol-restore-"+vol, 1)
			require.NoError(t, restore.OpenFromSnapshot(snap))
			got, err := restore.ReadAt(0, uint64(restore.BlockSize))
			require.NoError(t, err, "qualified chunk pinned by the snapshot was lost")
			require.Equal(t, before, got)
			require.Zero(t, backend.deletes.Load())
			cold := open(9, false)
			require.Equal(t, after, readBlocks(t, cold, 0, 1))
			stored, err := raw.ReadChunkFromCtx(ctx, vol, pinned, 0, 0)
			require.NoError(t, err)
			require.NotEmpty(t, stored)
			t.Logf("verified snapshot and live data; retained qualified object bytes=%d", len(stored))
		})
	}
}
