package telemetry

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meterName identifies the viperblock meter, matching the package import
// path convention used by predastore/otelsetup.
const meterName = "github.com/mulgadc/viperblock/telemetry"

var (
	instrumentsOnce sync.Once

	backendIOCount    metric.Int64Counter
	backendIOBytes    metric.Int64Counter
	backendIODuration metric.Float64Histogram

	walOpCount    metric.Int64Counter
	walOpDuration metric.Float64Histogram

	cacheLookups metric.Int64Counter

	// cacheHitOpt/cacheMissOpt are pre-built so the per-block cache lookup
	// path (inside the read hot loop) allocates nothing beyond the record
	// call itself.
	cacheHitOpt  metric.AddOption
	cacheMissOpt metric.AddOption
)

// instruments lazily creates the shared instruments. The global meter
// delegates to the real provider once Init installs one; before that (or
// when export is disabled) every recorded call is a cheap no-op.
func instruments() {
	instrumentsOnce.Do(func() {
		m := otel.Meter(meterName)
		var err error

		backendIOCount, err = m.Int64Counter("viperblock.backend.io",
			metric.WithDescription("Count of block-storage backend read/write operations."),
			metric.WithUnit("{operation}"))
		if err != nil {
			otel.Handle(err)
		}
		backendIOBytes, err = m.Int64Counter("viperblock.backend.io.bytes",
			metric.WithDescription("Bytes transferred by block-storage backend read/write operations."),
			metric.WithUnit("By"))
		if err != nil {
			otel.Handle(err)
		}
		backendIODuration, err = m.Float64Histogram("viperblock.backend.io.duration",
			metric.WithDescription("Duration of block-storage backend read/write operations."),
			metric.WithUnit("s"))
		if err != nil {
			otel.Handle(err)
		}

		walOpCount, err = m.Int64Counter("viperblock.wal.operations",
			metric.WithDescription("Count of WAL flush/replay/consolidate operations."),
			metric.WithUnit("{operation}"))
		if err != nil {
			otel.Handle(err)
		}
		walOpDuration, err = m.Float64Histogram("viperblock.wal.operation.duration",
			metric.WithDescription("Duration of WAL flush/replay/consolidate operations."),
			metric.WithUnit("s"))
		if err != nil {
			otel.Handle(err)
		}

		cacheLookups, err = m.Int64Counter("viperblock.cache.lookups",
			metric.WithDescription("Count of block-cache lookups by hit/miss outcome."),
			metric.WithUnit("{lookup}"))
		if err != nil {
			otel.Handle(err)
		}

		cacheHitOpt = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "hit")))
		cacheMissOpt = metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "miss")))
	})
}

// RecordBackendIO records one backend chunk-object read or write: IOPS,
// bytes transferred, and latency. op is "read"/"write", backendType is
// "s3"/"file", outcome is "success"/"error". volume is omitted from
// attributes when empty.
func RecordBackendIO(ctx context.Context, op, backendType, volume, outcome string, bytesTransferred int, elapsed time.Duration) {
	instruments()
	attrs := []attribute.KeyValue{
		attribute.String("op", op),
		attribute.String("backend", backendType),
		attribute.String("outcome", outcome),
	}
	if volume != "" {
		attrs = append(attrs, attribute.String("volume.name", volume))
	}
	opt := metric.WithAttributeSet(attribute.NewSet(attrs...))

	if backendIOCount != nil {
		backendIOCount.Add(ctx, 1, opt)
	}
	if backendIOBytes != nil && bytesTransferred > 0 {
		backendIOBytes.Add(ctx, int64(bytesTransferred), opt)
	}
	if backendIODuration != nil {
		backendIODuration.Record(ctx, elapsed.Seconds(), opt)
	}
}

// RecordWALOp records one WAL lifecycle operation. phase is
// "flush"/"replay"/"consolidate", outcome is "success"/"error".
func RecordWALOp(ctx context.Context, phase, volume, outcome string, elapsed time.Duration) {
	instruments()
	attrs := []attribute.KeyValue{
		attribute.String("phase", phase),
		attribute.String("outcome", outcome),
	}
	if volume != "" {
		attrs = append(attrs, attribute.String("volume.name", volume))
	}
	opt := metric.WithAttributeSet(attribute.NewSet(attrs...))

	if walOpCount != nil {
		walOpCount.Add(ctx, 1, opt)
	}
	if walOpDuration != nil {
		walOpDuration.Record(ctx, elapsed.Seconds(), opt)
	}
}

// RecordCacheLookup records one block-cache lookup outcome ("hit"/"miss").
// Hot path: called per block in the read loop, so it uses a pre-built
// AddOption rather than allocating an attribute set per call.
func RecordCacheLookup(ctx context.Context, hit bool) {
	instruments()
	if cacheLookups == nil {
		return
	}
	if hit {
		cacheLookups.Add(ctx, 1, cacheHitOpt)
		return
	}
	cacheLookups.Add(ctx, 1, cacheMissOpt)
}
