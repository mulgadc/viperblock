package telemetry

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric/noop"
)

// withNoopProvider pins an explicit no-op MeterProvider, matching the state of
// a process where Init found no OTLP endpoint and left the globals alone. This
// is the baseline the export-enabled benchmarks are compared against: the
// difference between the two is the cost of turning telemetry on.
func withNoopProvider(b *testing.B) {
	b.Helper()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(noop.NewMeterProvider())
	instrumentsOnce = sync.Once{}
	b.Cleanup(func() {
		otel.SetMeterProvider(prev)
		instrumentsOnce = sync.Once{}
	})
}

// BenchmarkRecordCacheLookup measures the only instrument on a per-4KB-block
// path (the read loop in viperblock.Read). At 4KB blocks a 1 GB/s read issues
// ~262k of these per second, so this is the one Record* call whose per-call
// cost is visible in data-path throughput. cacheHitOpt/cacheMissOpt are
// pre-built precisely so this allocates nothing; ReportAllocs holds that.
func BenchmarkRecordCacheLookup(b *testing.B) {
	ctx := context.Background()

	b.Run("noop", func(b *testing.B) {
		withNoopProvider(b)
		instruments()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			RecordCacheLookup(ctx, i%2 == 0)
		}
	})

	b.Run("sdk", func(b *testing.B) {
		withManualReader(b)
		instruments()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			RecordCacheLookup(ctx, i%2 == 0)
		}
	})
}

// BenchmarkRecordCacheLookupParallel measures the same call under contention.
// Concurrent guest reads land in the same sum aggregator, so the shared
// attribute-set map and its lock are the scaling limit, not the Add itself.
func BenchmarkRecordCacheLookupParallel(b *testing.B) {
	ctx := context.Background()
	withManualReader(b)
	instruments()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		hit := true
		for pb.Next() {
			RecordCacheLookup(ctx, hit)
			hit = !hit
		}
	})
}

// BenchmarkRecordBackendIO measures the per-chunk-object instrument. It builds
// a fresh attribute.NewSet of 4 KeyValues per call and drives three counters,
// so it is materially more expensive than RecordCacheLookup — but it fires
// once per 4MB object rather than once per 4KB block, which is the reason that
// cost is acceptable. The allocs/op figure quantifies the attribute set.
func BenchmarkRecordBackendIO(b *testing.B) {
	ctx := context.Background()
	const elapsed = 250 * time.Microsecond

	b.Run("noop", func(b *testing.B) {
		withNoopProvider(b)
		instruments()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			RecordBackendIO(ctx, "read", "s3", "vol-bench", "success", 4<<20, elapsed)
		}
	})

	b.Run("sdk", func(b *testing.B) {
		withManualReader(b)
		instruments()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			RecordBackendIO(ctx, "read", "s3", "vol-bench", "success", 4<<20, elapsed)
		}
	})
}
