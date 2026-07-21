package telemetry

import (
	"context"
	"os"
	"path/filepath"
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

	backendIOOps         metric.Int64Counter
	backendIOBytes       metric.Int64Counter
	backendIODurationSum metric.Float64Counter

	walOpCount       metric.Int64Counter
	walOpDurationSum metric.Float64Counter

	cacheLookups metric.Int64Counter

	rmwConflicts metric.Int64Counter
	volumeOpens  metric.Int64Counter

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

		// "io" is a namespace (ops/bytes/duration.sum siblings), not a leaf,
		// to avoid an ES leaf-vs-object mapping collision. Durations are
		// recorded as seconds-sum counters (not histograms) so avg latency
		// = sum/ops is computable in ES|QL; native ES histograms aren't.
		backendIOOps, err = m.Int64Counter("viperblock.backend.io.ops",
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
		backendIODurationSum, err = m.Float64Counter("viperblock.backend.io.duration.sum",
			metric.WithDescription("Cumulative seconds spent in block-storage backend read/write operations."),
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
		walOpDurationSum, err = m.Float64Counter("viperblock.wal.operation.duration.sum",
			metric.WithDescription("Cumulative seconds spent in WAL flush/replay/consolidate operations."),
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

		rmwConflicts, err = m.Int64Counter("viperblock.write.rmw_conflicts",
			metric.WithDescription("Partial writes that found another write already rebuilding the same block. Non-zero means guest I/O produces same-block write concurrency."),
			metric.WithUnit("{conflict}"))
		if err != nil {
			otel.Handle(err)
		}

		volumeOpens, err = m.Int64Counter("viperblock.volume.opens",
			metric.WithDescription("Volume opens, attributed by owning process identity, pid and role. Two distinct pids/roles reporting opens for one volume is a dual-open: more than one engine holds the volume."),
			metric.WithUnit("{open}"))
		if err != nil {
			otel.Handle(err)
		}
	})
}

// RecordRMWConflict counts one read-modify-write conflict: a partial write
// that had to wait because another write was already rebuilding the same
// block. Before per-block RMW serialization this was the exact condition
// under which one of the two writes was silently discarded, so a non-zero
// count is the signal that the workload can produce that class of loss.
func RecordRMWConflict(ctx context.Context, volume string) {
	instruments()
	if rmwConflicts == nil {
		return
	}
	attrs := []attribute.KeyValue{}
	if volume != "" {
		attrs = append(attrs, attribute.String("volume", volume))
	}
	rmwConflicts.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordVolumeOpen emits one volume-open event carrying the opening process's
// identity: role ("nbdkit" for the data-path plugin, "daemon" for a
// control-plane import, or "" when unset), executable name and pid.
//
// viperblock currently runs as BOTH an nbdkit plugin and a Go module imported
// into the spinifex daemon, so a volume can be held by more than one engine
// with no arbitration between them. mulga-t1sch removes the second engine; the
// invariant it establishes is "one owner per volume". This metric is how that
// invariant is observed: opens for a single volume carrying two different pids
// or roles are a dual-open. It is both the evidence for the current bug class
// and the permanent regression alarm for the plan's end state -- after
// t1sch lands, any such series is a regression.
func RecordVolumeOpen(ctx context.Context, volume, role string) {
	instruments()
	if volumeOpens == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.Int("pid", os.Getpid()),
		attribute.String("process", filepath.Base(os.Args[0])),
	}
	if volume != "" {
		attrs = append(attrs, attribute.String("volume", volume))
	}
	if role != "" {
		attrs = append(attrs, attribute.String("role", role))
	}
	volumeOpens.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordBackendIO records one backend chunk-object read or write: op count,
// bytes transferred, and cumulative duration (as duration.sum, added to on
// every call). op is "read"/"write", backendType is "s3"/"file", outcome is
// "success"/"error". volume is omitted from attributes when empty.
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

	if backendIOOps != nil {
		backendIOOps.Add(ctx, 1, opt)
	}
	if backendIOBytes != nil && bytesTransferred > 0 {
		backendIOBytes.Add(ctx, int64(bytesTransferred), opt)
	}
	if backendIODurationSum != nil {
		backendIODurationSum.Add(ctx, elapsed.Seconds(), opt)
	}
}

// RecordWALOp records one WAL lifecycle operation: op count and cumulative
// duration (as duration.sum, added to on every call). phase is
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
	if walOpDurationSum != nil {
		walOpDurationSum.Add(ctx, elapsed.Seconds(), opt)
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
