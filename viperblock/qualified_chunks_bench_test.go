package viperblock

import (
	"context"
	"testing"

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
