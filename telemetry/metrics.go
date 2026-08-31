// Package telemetry holds viperblock's own OpenTelemetry instruments: backend
// I/O, WAL, block cache, RMW conflicts and volume opens. The OTel bootstrap
// itself lives in bluebottle/pkg/otelsetup, which entrypoints call directly.
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
// path convention used by bluebottle/pkg/otelsetup.
const meterName = "github.com/mulgadc/viperblock/telemetry"

var (
	instrumentsOnce sync.Once

	backendIOOps         metric.Int64Counter
	backendIOBytes       metric.Int64Counter
	backendIODurationSum metric.Float64Counter

	walOpCount       metric.Int64Counter
	walOpDurationSum metric.Float64Counter

	cacheLookups metric.Int64Counter

	readOps         metric.Int64Counter
	readDurationSum metric.Float64Counter
	readInflightSum metric.Int64Counter

	rmwConflicts  metric.Int64Counter
	volumeOpens   metric.Int64Counter
	volumeEngines metric.Int64UpDownCounter

	// cacheHitOpts/cacheMissOpts are pre-built as slices, not bare options, so
	// the per-block cache lookup path (inside the read hot loop) allocates
	// nothing at all. Passing a bare AddOption to the variadic Add still heap-
	// allocates the 16-byte backing slice on every call; passing a pre-built
	// slice with ... reuses it.
	cacheHitOpts  []metric.AddOption
	cacheMissOpts []metric.AddOption
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

		cacheHitOpts = []metric.AddOption{
			metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "hit"))),
		}
		cacheMissOpts = []metric.AddOption{
			metric.WithAttributeSet(attribute.NewSet(attribute.String("result", "miss"))),
		}

		// A guest read is the only latency the guest actually experiences, and
		// nothing measured it: backend.io covers the S3 round trip alone, so a
		// read spending its time above that -- queued, or waiting on a lock --
		// was invisible. The difference between these two means is that time.
		readOps, err = m.Int64Counter("viperblock.read.ops",
			metric.WithDescription("Count of guest block reads served."),
			metric.WithUnit("{operation}"))
		if err != nil {
			otel.Handle(err)
		}
		readDurationSum, err = m.Float64Counter("viperblock.read.duration.sum",
			metric.WithDescription("Cumulative seconds spent serving guest block reads, measured end to end. Divided by read.ops this is mean guest read latency; compare against backend.io.duration.sum/ops to separate storage from everything above it."),
			metric.WithUnit("s"))
		if err != nil {
			otel.Handle(err)
		}
		readInflightSum, err = m.Int64Counter("viperblock.read.inflight.sum",
			metric.WithDescription("Cumulative count of reads already in flight when each read began. Divided by read.ops this is mean read concurrency, which is what separates a slow backend from a serialised one: the same throughput loss looks identical from latency alone."),
			metric.WithUnit("{read}"))
		if err != nil {
			otel.Handle(err)
		}

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

		volumeEngines, err = m.Int64UpDownCounter("viperblock.volume.engines",
			metric.WithDescription("Engines currently holding a volume: incremented on open, decremented on close. Summed across pids for one volume, a value above 1 is a dual-open happening now, without reconstructing intervals from open events."),
			metric.WithUnit("{engine}"))
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
// viperblock runs as both an nbdkit plugin and a Go module importable by a
// control plane, so a volume can be held by more than one engine. The
// invariant is one owner per volume, and this is how it is observed: opens
// for one volume carrying two pids or roles are a dual-open.
func RecordVolumeOpen(ctx context.Context, volume, role string) {
	instruments()
	attrs := volumeIdentityAttrs(volume, role)
	if volumeOpens != nil {
		volumeOpens.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if volumeEngines != nil {
		volumeEngines.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// RecordVolumeClose releases the volume-open recorded by RecordVolumeOpen,
// decrementing the engine count under identical attributes so the two cancel.
//
// Without it, opens for one volume are indistinguishable from two engines
// holding it at once: nbdkit opening after the control plane released is a
// normal handover and produces the same two events as a genuine dual-open.
// The engine count is what separates them, and it only means anything if
// every open is eventually matched.
//
// A process that dies never gets here, so its open stays outstanding. That is
// deliberate: the volume lock and the control-plane lease are what reclaim a
// dead holder, and an unmatched open is a signal worth keeping rather than
// papering over.
func RecordVolumeClose(ctx context.Context, volume, role string) {
	instruments()
	if volumeEngines == nil {
		return
	}
	volumeEngines.Add(ctx, -1, metric.WithAttributes(volumeIdentityAttrs(volume, role)...))
}

// volumeIdentityAttrs builds the attributes identifying which engine holds a
// volume. Shared by open and close so the two carry an identical attribute
// set; any divergence would leave the engine count unable to cancel them out.
func volumeIdentityAttrs(volume, role string) []attribute.KeyValue {
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
	return attrs
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

// RecordRead records one guest block read: how long it took end to end, and
// how many other reads were already in flight when it started. Attribute-free
// on purpose -- it is called once per guest I/O, so a per-volume attribute set
// would allocate on the hottest path there is, and the volume is already the
// dimension the process itself is scoped to.
func RecordRead(ctx context.Context, inflight int64, elapsed time.Duration) {
	instruments()
	if readOps != nil {
		readOps.Add(ctx, 1)
	}
	if readDurationSum != nil {
		readDurationSum.Add(ctx, elapsed.Seconds())
	}
	if readInflightSum != nil {
		readInflightSum.Add(ctx, inflight)
	}
}

// RecordCacheLookup records one block-cache lookup outcome ("hit"/"miss").
// Hot path: called per block in the read loop, so it passes a pre-built option
// slice rather than allocating an attribute set or a variadic slice per call.
func RecordCacheLookup(ctx context.Context, hit bool) {
	instruments()
	if cacheLookups == nil {
		return
	}
	if hit {
		cacheLookups.Add(ctx, 1, cacheHitOpts...)
		return
	}
	cacheLookups.Add(ctx, 1, cacheMissOpts...)
}
