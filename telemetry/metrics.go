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

	"github.com/mulgadc/bluebottle/pkg/safecast"
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

	guestIOOps         metric.Int64Counter
	guestIOBytes       metric.Int64Counter
	guestIODurationSum metric.Float64Counter

	cacheLookups metric.Int64Counter

	backpressureWaits       metric.Int64Counter
	backpressureDurationSum metric.Float64Counter
	backpressurePending     metric.Int64Gauge
	backpressureHigh        metric.Int64Gauge
	backpressureWaiters     metric.Int64UpDownCounter
	backpressureSlowWaits   metric.Int64Counter

	backpressurePhaseOps         metric.Int64Counter
	backpressurePhaseDurationSum metric.Float64Counter
	backpressureDrainedBytes     metric.Int64Counter

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

		// The guest boundary, as distinct from the WAL and backend instruments
		// below and above it. A guest fsync is a write followed by a flush, two
		// separate NBD requests, and only the flush reaches the WAL timer — so
		// without these the cost the guest actually waits on is unattributable.
		guestIOOps, err = m.Int64Counter("viperblock.guest.io.ops",
			metric.WithDescription("Count of NBD requests served to the guest, by op."),
			metric.WithUnit("{operation}"))
		if err != nil {
			otel.Handle(err)
		}
		guestIOBytes, err = m.Int64Counter("viperblock.guest.io.bytes",
			metric.WithDescription("Bytes transferred by NBD requests served to the guest."),
			metric.WithUnit("By"))
		if err != nil {
			otel.Handle(err)
		}
		guestIODurationSum, err = m.Float64Counter("viperblock.guest.io.duration.sum",
			metric.WithDescription("Cumulative seconds the guest spent waiting on NBD requests. Divided by ops this is the latency the guest observes, which for a flush is what a datastore's commit latency is made of."),
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

		// Counted only when a write actually blocked, so waits/guest-write-ops
		// is the fraction of writes that stalled and duration.sum against the
		// guest write duration is how much of guest write latency is this and
		// not the write itself.
		backpressureWaits, err = m.Int64Counter("viperblock.write.backpressure.waits",
			metric.WithDescription("Guest writes that blocked because buffered bytes crossed the high-watermark. Zero means the backend kept up."),
			metric.WithUnit("{wait}"))
		if err != nil {
			otel.Handle(err)
		}
		backpressureDurationSum, err = m.Float64Counter("viperblock.write.backpressure.duration.sum",
			metric.WithDescription("Cumulative seconds guest writes spent blocked on backpressure, waiting for the backend to drain."),
			metric.WithUnit("s"))
		if err != nil {
			otel.Handle(err)
		}

		// The two levels the gate compares. Both were previously inferable only
		// from a wait count, which is how a wrong watermark went unnoticed
		// across three runs; the gap between them is what a stalled write pays.
		backpressurePending, err = m.Int64Gauge("viperblock.write.backpressure.pending_bytes",
			metric.WithDescription("Buffered bytes not yet durable in a backend chunk, as the backpressure gate sees them. Approaching the high-watermark means the background uploader is not keeping up with the guest."),
			metric.WithUnit("By"))
		if err != nil {
			otel.Handle(err)
		}
		backpressureHigh, err = m.Int64Gauge("viperblock.write.backpressure.high_watermark_bytes",
			metric.WithDescription("Runtime high-watermark the gate blocks at, after the WAL-device free-space clamp. Not the 256MB default unless the device has room, so it must be read rather than assumed."),
			metric.WithUnit("By"))
		if err != nil {
			otel.Handle(err)
		}

		// Waits are recorded per blocked writer, so duration.sum counts one
		// stall once per writer that sat through it. Without this, N writers
		// blocked on a single event are indistinguishable from N events.
		backpressureWaiters, err = m.Int64UpDownCounter("viperblock.write.backpressure.waiters",
			metric.WithDescription("Guest writes currently blocked on backpressure for this volume. A value above 1 means duration.sum is counting one stall several times over."),
			metric.WithUnit("{writer}"))
		if err != nil {
			otel.Handle(err)
		}

		// A mean cannot show a tail, and the tail is the failure: etcd loses
		// its leader on one stall over the election timeout, however good the
		// average was.
		backpressureSlowWaits, err = m.Int64Counter("viperblock.write.backpressure.slow_waits",
			metric.WithDescription("Guest writes that blocked for longer than the bucket's lower bound, by threshold. The 1s bucket is the one that costs a datastore its leader."),
			metric.WithUnit("{wait}"))
		if err != nil {
			otel.Handle(err)
		}

		// Splits a wait into the part this writer spent driving a drain and
		// the part it spent polling while another writer drove one. Phase sums
		// on the drain itself cannot do this: the guest flush path calls the
		// same Flush(), so those counters mix both callers.
		backpressurePhaseOps, err = m.Int64Counter("viperblock.write.backpressure.phase.ops",
			metric.WithDescription("Occurrences of each phase within a backpressure wait: drain (this writer drove one) or poll (it waited on another writer's)."),
			metric.WithUnit("{operation}"))
		if err != nil {
			otel.Handle(err)
		}
		backpressurePhaseDurationSum, err = m.Float64Counter("viperblock.write.backpressure.phase.duration.sum",
			metric.WithDescription("Cumulative seconds spent in each phase within a backpressure wait. Against waits.duration.sum this is where a stall actually goes."),
			metric.WithUnit("s"))
		if err != nil {
			otel.Handle(err)
		}
		backpressureDrainedBytes, err = m.Int64Counter("viperblock.write.backpressure.drained_bytes",
			metric.WithDescription("Bytes pendingBytes fell by during drains driven from the stall path. Divided by the drain phase duration this is the net drain rate under guest load, which is what the watermark gap is actually divided by."),
			metric.WithUnit("By"))
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

// RecordWriteBackpressure records one guest write that blocked waiting for the
// backend to drain, and how long it waited. Only blocked writes are recorded:
// the unblocked path is the common case, and counting it would bury the stalls
// this exists to surface in a mean dominated by zeros.
func RecordWriteBackpressure(ctx context.Context, volume string, elapsed time.Duration) {
	instruments()
	var attrs []attribute.KeyValue
	if volume != "" {
		attrs = append(attrs, attribute.String("volume.name", volume))
	}
	opt := metric.WithAttributeSet(attribute.NewSet(attrs...))

	if backpressureWaits != nil {
		backpressureWaits.Add(ctx, 1, opt)
	}
	if backpressureDurationSum != nil {
		backpressureDurationSum.Add(ctx, elapsed.Seconds(), opt)
	}
}

// backpressureSlowThresholds are the tail buckets a wait is counted into. A
// wait longer than several of them is counted in each, so a bucket reads as
// "waits at least this long".
var backpressureSlowThresholds = []struct {
	label string
	limit time.Duration
}{
	{"100ms", 100 * time.Millisecond},
	{"1s", time.Second},
	{"5s", 5 * time.Second},
}

// BackpressureWaiterScope marks a writer as blocked for as long as the
// returned function is uncalled, so concurrent waiters are countable. Call the
// returned function once, on the way out of the wait.
func BackpressureWaiterScope(ctx context.Context, volume string) func() {
	instruments()
	if backpressureWaiters == nil {
		return func() {}
	}
	opt := metric.WithAttributeSet(attribute.NewSet(volumeAttrs(volume)...))
	backpressureWaiters.Add(ctx, 1, opt)
	return func() { backpressureWaiters.Add(ctx, -1, opt) }
}

// RecordBackpressurePhase records one phase within a backpressure wait: phase
// is "drain" when this writer drove one itself, "poll" when it slept while
// another writer's drain ran. drainedBytes is how far pendingBytes fell, and
// is meaningful for the drain phase only.
func RecordBackpressurePhase(ctx context.Context, volume, phase string, elapsed time.Duration, drainedBytes int64) {
	instruments()
	attrs := volumeAttrs(volume)
	attrs = append(attrs, attribute.String("phase", phase))
	opt := metric.WithAttributeSet(attribute.NewSet(attrs...))

	if backpressurePhaseOps != nil {
		backpressurePhaseOps.Add(ctx, 1, opt)
	}
	if backpressurePhaseDurationSum != nil {
		backpressurePhaseDurationSum.Add(ctx, elapsed.Seconds(), opt)
	}
	if backpressureDrainedBytes != nil && drainedBytes > 0 {
		backpressureDrainedBytes.Add(ctx, drainedBytes, opt)
	}
}

// RecordBackpressureTail counts one completed wait into every tail bucket it
// exceeds, so the shape of the tail is readable without percentiles, which
// counter sums cannot express.
func RecordBackpressureTail(ctx context.Context, volume string, elapsed time.Duration) {
	instruments()
	if backpressureSlowWaits == nil {
		return
	}
	for _, t := range backpressureSlowThresholds {
		if elapsed < t.limit {
			continue
		}
		attrs := volumeAttrs(volume)
		attrs = append(attrs, attribute.String("threshold", t.label))
		backpressureSlowWaits.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attrs...)))
	}
}

// volumeAttrs builds the common per-volume attribute slice, omitting the
// attribute entirely when the volume is unnamed.
func volumeAttrs(volume string) []attribute.KeyValue {
	if volume == "" {
		return nil
	}
	return []attribute.KeyValue{attribute.String("volume.name", volume)}
}

// RecordBackpressureLevels samples the two levels the gate compares: the
// buffered bytes it watches and the runtime high-watermark it blocks at.
// Sampled on the write path whether or not the write blocked, so a volume
// tracking just under the watermark is visible before it starts stalling.
func RecordBackpressureLevels(ctx context.Context, volume string, pending, high uint64) {
	instruments()
	var attrs []attribute.KeyValue
	if volume != "" {
		attrs = append(attrs, attribute.String("volume.name", volume))
	}
	opt := metric.WithAttributeSet(attribute.NewSet(attrs...))

	if backpressurePending != nil {
		backpressurePending.Record(ctx, safecast.Uint64ToInt64(pending), opt)
	}
	if backpressureHigh != nil {
		backpressureHigh.Record(ctx, safecast.Uint64ToInt64(high), opt)
	}
}

// RecordGuestIO records one NBD request served to the guest: op count, bytes
// and the wall time the guest waited. op is "read"/"write"/"flush"/"zero"/
// "trim", outcome is "success"/"error". bytesTransferred is 0 for a flush.
//
// This is the only measurement taken where the guest feels it. Everything else
// times an internal stage, and a stage that looks fast can still leave the
// guest waiting on queueing or lock contention in front of it.
func RecordGuestIO(ctx context.Context, op, volume, outcome string, bytesTransferred int, elapsed time.Duration) {
	instruments()
	attrs := []attribute.KeyValue{
		attribute.String("op", op),
		attribute.String("outcome", outcome),
	}
	if volume != "" {
		attrs = append(attrs, attribute.String("volume.name", volume))
	}
	opt := metric.WithAttributeSet(attribute.NewSet(attrs...))

	if guestIOOps != nil {
		guestIOOps.Add(ctx, 1, opt)
	}
	if guestIOBytes != nil && bytesTransferred > 0 {
		guestIOBytes.Add(ctx, int64(bytesTransferred), opt)
	}
	if guestIODurationSum != nil {
		guestIODurationSum.Add(ctx, elapsed.Seconds(), opt)
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
