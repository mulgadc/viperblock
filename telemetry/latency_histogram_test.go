package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// histogramPoint pulls the single data point of a named histogram out of a
// collected snapshot.
func histogramPoint(t *testing.T, metrics map[string]any, name string) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	h, ok := metrics[name].(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s = %#v, want a float64 histogram", name, metrics[name])
	}
	if len(h.DataPoints) != 1 {
		t.Fatalf("%s has %d data points, want 1", name, len(h.DataPoints))
	}
	return h.DataPoints[0]
}

// TestGuestIOLatencyHistogramCapturesTheTail is the whole reason the
// histograms exist: two fast flushes and one slow one have a mean under the
// 10 ms an etcd-class guest is specified against, while the sample that
// misses it is still visible in the distribution.
func TestGuestIOLatencyHistogramCapturesTheTail(t *testing.T) {
	reader := withManualReader(t)

	ctx := context.Background()
	RecordGuestIO(ctx, "flush", "vol-1", "success", 0, 2*time.Millisecond)
	RecordGuestIO(ctx, "flush", "vol-1", "success", 0, 3*time.Millisecond)
	RecordGuestIO(ctx, "flush", "vol-1", "success", 0, 400*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	dp := histogramPoint(t, metrics, "viperblock.guest.io.latency")
	if dp.Count != 3 {
		t.Errorf("count = %d, want 3", dp.Count)
	}
	if got, ok := dp.Max.Value(); !ok || got < 0.4 {
		t.Errorf("max = %v (set=%v), want the 400ms sample", got, ok)
	}

	// The mean the sum-counter reports hides that sample entirely.
	mean := dp.Sum / float64(dp.Count)
	if mean >= 0.4 {
		t.Fatalf("mean = %v, expected it to sit well under the slow sample", mean)
	}

	wantAttr(t, dp.Attributes, "op", "flush")
	wantAttr(t, dp.Attributes, "volume.name", "vol-1")
}

// TestLatencyHistogramsAccompanyTheirSums pins that each histogram is
// additive rather than a replacement: ES|QL reads the sums for means and
// cannot read a histogram field at all.
func TestLatencyHistogramsAccompanyTheirSums(t *testing.T) {
	reader := withManualReader(t)

	ctx := context.Background()
	RecordGuestIO(ctx, "flush", "vol-1", "success", 0, 5*time.Millisecond)
	RecordWALOp(ctx, "flush", "vol-1", "success", 5*time.Millisecond)
	RecordWriteBackpressure(ctx, "vol-1", 5*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	for _, pair := range [][2]string{
		{"viperblock.guest.io.latency", "viperblock.guest.io.duration.sum"},
		{"viperblock.wal.operation.latency", "viperblock.wal.operation.duration.sum"},
		{"viperblock.write.backpressure.latency", "viperblock.write.backpressure.duration.sum"},
	} {
		dp := histogramPoint(t, metrics, pair[0])
		if dp.Count != 1 {
			t.Errorf("%s count = %d, want 1", pair[0], dp.Count)
		}
		if _, ok := metrics[pair[1]].(metricdata.Sum[float64]); !ok {
			t.Errorf("%s = %#v, want the sum counter kept alongside %s", pair[1], metrics[pair[1]], pair[0])
		}
	}
}

// TestLatencyHistogramsNameThemselvesLatencyNotDuration pins the naming
// constraint. "duration" is already an object in Elasticsearch because
// "duration.sum" is a leaf under it, so a histogram called "duration" would
// collide on the mapping and be dropped at ingest.
func TestLatencyHistogramsNameThemselvesLatencyNotDuration(t *testing.T) {
	reader := withManualReader(t)

	ctx := context.Background()
	RecordGuestIO(ctx, "flush", "vol-1", "success", 0, time.Millisecond)
	RecordWALOp(ctx, "flush", "vol-1", "success", time.Millisecond)
	RecordWriteBackpressure(ctx, "vol-1", time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	for _, name := range []string{
		"viperblock.guest.io.duration",
		"viperblock.wal.operation.duration",
		"viperblock.write.backpressure.duration",
	} {
		if _, exists := metrics[name]; exists {
			t.Errorf("%s exists and collides with %s.sum; name it .latency", name, name)
		}
	}
}
