package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// histogramPoint returns the single data point of a float64 histogram whose
// attributes carry every key/value in want.
func histogramPoint(t *testing.T, metrics map[string]any, name string, want map[string]string) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	h, ok := metrics[name].(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s = %#v, want a float64 histogram", name, metrics[name])
	}
	for _, dp := range h.DataPoints {
		if attrsMatch(dp.Attributes, want) {
			return dp
		}
	}
	t.Fatalf("%s has no data point with attributes %v", name, want)
	return metricdata.HistogramDataPoint[float64]{}
}

func attrsMatch(set attribute.Set, want map[string]string) bool {
	for k, v := range want {
		got, ok := set.Value(attribute.Key(k))
		if !ok || got.AsString() != v {
			return false
		}
	}
	return true
}

func TestRecordWALOpRecordsCheckpointLatencyBesideItsSum(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordWALOp(ctx, "checkpoint", "vol-1", "success", 2*time.Millisecond)
	RecordWALOp(ctx, "checkpoint", "vol-1", "success", 300*time.Millisecond)
	RecordWALOp(ctx, "flush", "vol-1", "success", time.Millisecond)

	metrics := collect(t, reader)
	dp := histogramPoint(t, metrics, "viperblock.wal.operation.latency",
		map[string]string{"phase": "checkpoint", "outcome": "success", "volume.name": "vol-1"})
	if dp.Count != 2 {
		t.Errorf("checkpoint latency count = %d, want 2", dp.Count)
	}
	if !approx(dp.Sum, 0.302) {
		t.Errorf("checkpoint latency sum = %v, want 0.302 seconds", dp.Sum)
	}
	if got, set := dp.Max.Value(); !set || got < 0.3 {
		t.Errorf("checkpoint latency max = %v, want the 300ms sample", got)
	}
	if _, ok := metrics["viperblock.wal.operation.duration.sum"].(metricdata.Sum[float64]); !ok {
		t.Fatalf("the duration.sum counter must stay alongside the histogram")
	}
}

func TestRecordWriteBackpressureRecordsLatencyAndTail(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordWriteBackpressure(ctx, "vol-1", 50*time.Millisecond)
	RecordWriteBackpressure(ctx, "vol-1", 1500*time.Millisecond)
	RecordWriteBackpressure(ctx, "vol-1", 6*time.Second)

	metrics := collect(t, reader)
	dp := histogramPoint(t, metrics, "viperblock.write.backpressure.latency", map[string]string{"volume.name": "vol-1"})
	if dp.Count != 3 {
		t.Errorf("backpressure latency count = %d, want 3", dp.Count)
	}
	if !approx(dp.Sum, 7.55) {
		t.Errorf("backpressure latency sum = %v, want 7.55 seconds", dp.Sum)
	}

	slow, ok := metrics["viperblock.write.backpressure.slow_waits"].(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("slow_waits = %#v, want an int64 sum", metrics["viperblock.write.backpressure.slow_waits"])
	}
	byThreshold := map[string]int64{}
	for _, p := range slow.DataPoints {
		wantAttr(t, p.Attributes, "volume.name", "vol-1")
		v, _ := p.Attributes.Value("threshold")
		byThreshold[v.AsString()] = p.Value
	}
	// 50ms reaches no bucket, 1.5s reaches 100ms and 1s, and 6s reaches all three.
	want := map[string]int64{"100ms": 2, "1s": 2, "5s": 1}
	for k, v := range want {
		if byThreshold[k] != v {
			t.Errorf("slow_waits[%s] = %d, want %d", k, byThreshold[k], v)
		}
	}
	if len(byThreshold) != len(want) {
		t.Errorf("slow_waits thresholds = %v, want exactly %v", byThreshold, want)
	}
}

func TestRecordBackpressurePhaseSplitsDrainAndPoll(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordBackpressurePhase(ctx, "vol-1", "drain", 200*time.Millisecond, 4096)
	RecordBackpressurePhase(ctx, "vol-1", "poll", 10*time.Millisecond, 0)
	RecordBackpressurePhase(ctx, "vol-1", "poll", 20*time.Millisecond, 0)

	metrics := collect(t, reader)
	ops := metrics["viperblock.write.backpressure.phase.ops"].(metricdata.Sum[int64])
	gotOps := map[string]int64{}
	for _, p := range ops.DataPoints {
		v, _ := p.Attributes.Value("phase")
		gotOps[v.AsString()] = p.Value
	}
	if gotOps["drain"] != 1 || gotOps["poll"] != 2 {
		t.Errorf("phase ops = %v, want drain 1, poll 2", gotOps)
	}

	poll := histogramPoint(t, metrics, "viperblock.write.backpressure.phase.latency", map[string]string{"phase": "poll", "volume.name": "vol-1"})
	if poll.Count != 2 || !approx(poll.Sum, 0.03) {
		t.Errorf("poll latency count = %d sum = %v, want 2 and 0.03 seconds", poll.Count, poll.Sum)
	}
	drain := histogramPoint(t, metrics, "viperblock.write.backpressure.phase.latency", map[string]string{"phase": "drain", "volume.name": "vol-1"})
	if drain.Count != 1 || !approx(drain.Sum, 0.2) {
		t.Errorf("drain latency count = %d sum = %v, want 1 and 0.2 seconds", drain.Count, drain.Sum)
	}

	sums := metrics["viperblock.write.backpressure.phase.duration.sum"].(metricdata.Sum[float64])
	gotSums := map[string]float64{}
	for _, p := range sums.DataPoints {
		wantAttr(t, p.Attributes, "volume.name", "vol-1")
		v, _ := p.Attributes.Value("phase")
		gotSums[v.AsString()] = p.Value
	}
	if !approx(gotSums["drain"], 0.2) || !approx(gotSums["poll"], 0.03) {
		t.Errorf("phase duration.sum = %v, want drain 0.2s and poll 0.03s", gotSums)
	}

	drained := metrics["viperblock.write.backpressure.drained_bytes"].(metricdata.Sum[int64])
	if len(drained.DataPoints) != 1 || drained.DataPoints[0].Value != 4096 {
		t.Fatalf("drained_bytes = %#v, want one drain point of 4096", drained.DataPoints)
	}
	wantAttr(t, drained.DataPoints[0].Attributes, "phase", "drain")
}

func TestBackpressureLevelsAndWaiters(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordBackpressureLevels(ctx, "vol-1", 3<<20, 64<<20)
	releaseA := BackpressureWaiterScope(ctx, "vol-1")
	releaseB := BackpressureWaiterScope(ctx, "vol-1")
	releaseA()

	metrics := collect(t, reader)
	pending := metrics["viperblock.write.backpressure.pending_bytes"].(metricdata.Gauge[int64])
	wantAttr(t, pending.DataPoints[0].Attributes, "volume.name", "vol-1")
	if pending.DataPoints[0].Value != 3<<20 {
		t.Errorf("pending_bytes = %d, want %d", pending.DataPoints[0].Value, 3<<20)
	}
	high := metrics["viperblock.write.backpressure.high_watermark_bytes"].(metricdata.Gauge[int64])
	wantAttr(t, high.DataPoints[0].Attributes, "volume.name", "vol-1")
	if high.DataPoints[0].Value != 64<<20 {
		t.Errorf("high_watermark_bytes = %d, want %d", high.DataPoints[0].Value, 64<<20)
	}
	waiters := metrics["viperblock.write.backpressure.waiters"].(metricdata.Sum[int64])
	wantAttr(t, waiters.DataPoints[0].Attributes, "volume.name", "vol-1")
	if waiters.DataPoints[0].Value != 1 {
		t.Errorf("waiters = %d, want 1 still blocked", waiters.DataPoints[0].Value)
	}

	releaseB()
	metrics = collect(t, reader)
	waiters = metrics["viperblock.write.backpressure.waiters"].(metricdata.Sum[int64])
	if waiters.DataPoints[0].Value != 0 {
		t.Errorf("waiters = %d after every writer left, want 0", waiters.DataPoints[0].Value)
	}
}

// approx compares seconds recorded from durations, which round in float64.
func approx(got, want float64) bool {
	d := got - want
	return d < 1e-9 && d > -1e-9
}
