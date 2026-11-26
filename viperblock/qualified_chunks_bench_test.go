package viperblock

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/types"

	"github.com/stretchr/testify/require"
)

func BenchmarkChunkIdentity(b *testing.B) {
	silenceVBLogs()
	for _, workload := range []struct {
		operation string
		size      uint64
		label     string
	}{
		{"drain", 4 << 20, "4MiB"},
		{"drain", 32 << 20, "32MiB"},
		{"direct", 4 << 10, "4KiB"},
		{"direct", 4 << 20, "4MiB"},
		{"inherited", 4 << 10, "4KiB"},
		{"inherited", 4 << 20, "4MiB"},
		{"open", 4 << 10, "4KiB"},
		{"open", 4 << 20, "4MiB"},
	} {
		b.Run(workload.operation+"-"+workload.label, func(b *testing.B) {
			for _, qualified := range []bool{false, true} {
				mode := "numeric"
				if qualified {
					mode = "qualified"
				}
				b.Run(mode, func(b *testing.B) {
					benchmarkChunkIdentity(b, workload.operation, workload.size, qualified)
				})
			}
		})
	}
}

func BenchmarkFirstQualifiedClaim(b *testing.B) {
	silenceVBLogs()
	ctx := b.Context()
	mv := newMapVolume(b, volumeSize, nil)
	seed, _ := mv.create(b, 5)
	seed.Detach()
	vb, _ := mv.engine(b, 6)
	vb.QualifiedChunks = true
	require.NoError(b, vb.LoadState())
	require.NoError(b, vb.LoadLiveCheckpoint())
	b.ReportAllocs()
	for b.Loop() {
		vb.mapChain.claimed = false
		vb.mapChain.qualified.Store(false)
		if err := vb.ClaimMapCtx(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkChunkIdentity(b *testing.B, operation string, size uint64, qualified bool) {
	ctx := context.Background()
	mv := newMapVolume(b, max(volumeSize, size), nil)
	seed, _ := mv.create(b, 5)
	seed.Detach()
	vb, _ := mv.engine(b, 6)
	vb.QualifiedChunks = qualified
	require.NoError(b, vb.LoadState())
	require.NoError(b, vb.LoadLiveCheckpoint())
	require.NoError(b, vb.ClaimMapCtx(ctx))
	require.NoError(b, vb.RecoverLocalWALs())
	openMapWALs(b, vb)

	if operation == "drain" {
		data := makeSyntheticImage(size)
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		for b.Loop() {
			if err := vb.Write(0, data); err != nil {
				b.Fatal(err)
			}
			if err := vb.DrainToBackendCtx(ctx); err != nil {
				b.Fatal(err)
			}
		}
		return
	}

	if operation == "open" {
		require.NoError(b, vb.Write(0, makeSyntheticImage(size)))
		require.NoError(b, vb.DrainToBackendCtx(ctx))
		vb.Detach()
		reader, _ := mv.engine(b, 7)
		require.NoError(b, reader.LoadState())
		b.ReportAllocs()
		for b.Loop() {
			if err := reader.LoadLiveCheckpointCtx(ctx); err != nil {
				b.Fatal(err)
			}
		}
		return
	}

	data := makeSyntheticImage(size)
	require.NoError(b, vb.Write(0, data))
	require.NoError(b, vb.DrainToBackendCtx(ctx))
	if operation == "inherited" {
		_, err := vb.CreateSnapshot("bench-snapshot")
		require.NoError(b, err)
		cloneVolume := *mv
		cloneVolume.name = "vol-clone"
		vb, _ = cloneVolume.engine(b, 5)
		require.NoError(b, vb.OpenFromSnapshot("bench-snapshot"))
	}
	dst := make([]byte, len(data))
	b.SetBytes(int64(len(dst)))
	b.ReportAllocs()
	for b.Loop() {
		if err := vb.ReadAtInto(0, dst); err != nil {
			b.Fatal(err)
		}
	}
	require.Equal(b, data, dst)
}

type qualifiedSweepBenchBackend struct {
	omitMapEpochFromList
}

func (qualifiedSweepBenchBackend) DeleteChunkCtx(context.Context, types.ChunkRef) error {
	return nil
}

func BenchmarkQualifiedPredastoreSweep(b *testing.B) {
	silenceVBLogs()
	for _, count := range []int{1, 128} {
		b.Run(strconv.Itoa(count)+"-candidates", func(b *testing.B) {
			mv := newMapVolume(b, volumeSize, nil)
			seed, _ := mv.create(b, 5)
			seed.Detach()
			vb := claimQualified(b, mv, 6)
			vb.Backend = qualifiedSweepBenchBackend{omitMapEpochFromList{Backend: vb.Backend}}
			vb.gcReconciled.Store(true)
			firstSeen := mv.clock.get().Add(-DefaultMapDeleteDeferral)
			vb.mapChain.chunkSeen = make(map[types.ChunkRef]time.Time, count)
			b.ReportAllocs()
			for b.Loop() {
				for id := range count {
					ref := types.ChunkRef{Epoch: 5, ID: uint64(id)}
					vb.gcRefcount[ref] = 0
					vb.mapChain.chunkSeen[ref] = firstSeen
				}
				vb.sweepChunks(b.Context())
			}
		})
	}
}

func BenchmarkLandedMapClaimReconnect(b *testing.B) {
	silenceVBLogs()
	for _, objectCount := range []int{16, 1024} {
		b.Run(strconv.Itoa(objectCount)+"-objects", func(b *testing.B) {
			mv := newMapVolume(b, volumeSize, nil)
			seed, _ := mv.create(b, 5)
			seed.Detach()
			writer := claimQualified(b, mv, 6)
			headN, objN := writer.MapCounters()
			for n := range objectCount {
				require.NoError(b, writer.Backend.WriteKeyCtx(
					b.Context(), mapObjectKey(mapDeltaPrefix, 6, uint64(n)), []byte("unreferenced delta")))
			}
			writer.Detach()

			reader, _ := mv.engine(b, 6)
			reader.ResumeEpoch = true
			reader.ResumeHeadN = headN
			reader.ResumeObjN = objN
			reader.LaunchChunkIDs = &atomic.Uint64{}
			require.NoError(b, reader.LoadState())
			b.ReportAllocs()
			for b.Loop() {
				if err := reader.LoadLiveCheckpointCtx(b.Context()); err != nil {
					b.Fatal(err)
				}
				if err := reader.ClaimMapCtx(b.Context()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
