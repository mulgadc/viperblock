package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestGuestIOLatencyHistogramCapturesTheTail is the whole reason the histogram
// exists alongside the sum: two fast flushes and one slow one have a mean well
// under the 10 ms an etcd-class guest is specified against, while the sample
// that misses it stays visible in the distribution.
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

	h, ok := metrics["viperblock.guest.io.latency"].(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("viperblock.guest.io.latency = %#v, want a float64 histogram", metrics["viperblock.guest.io.latency"])
	}
	if len(h.DataPoints) != 1 {
		t.Fatalf("got %d data points, want 1", len(h.DataPoints))
	}
	dp := h.DataPoints[0]

	if dp.Count != 3 {
		t.Errorf("count = %d, want 3", dp.Count)
	}
	if got, set := dp.Max.Value(); !set || got < 0.4 {
		t.Errorf("max = %v (set=%v), want the 400ms sample", got, set)
	}
	if mean := dp.Sum / float64(dp.Count); mean >= 0.4 {
		t.Fatalf("mean = %v, expected it to sit well under the slow sample", mean)
	}

	wantAttr(t, dp.Attributes, "op", "flush")
	wantAttr(t, dp.Attributes, "volume.name", "vol-1")
}

// TestGuestIOLatencyAccompaniesItsSum pins that the histogram is additive
// rather than a replacement: ES|QL reads the sum for means and cannot read a
// histogram field at all.
func TestGuestIOLatencyAccompaniesItsSum(t *testing.T) {
	reader := withManualReader(t)

	ctx := context.Background()
	RecordGuestIO(ctx, "flush", "vol-1", "success", 0, 5*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	if _, ok := metrics["viperblock.guest.io.latency"].(metricdata.Histogram[float64]); !ok {
		t.Errorf("histogram missing: %#v", metrics["viperblock.guest.io.latency"])
	}
	if _, ok := metrics["viperblock.guest.io.duration.sum"].(metricdata.Sum[float64]); !ok {
		t.Errorf("sum counter must be kept alongside the histogram: %#v", metrics["viperblock.guest.io.duration.sum"])
	}

	// "duration" must stay an object with "duration.sum" beneath it. A
	// histogram of that name would collide on the Elasticsearch mapping.
	if _, exists := metrics["viperblock.guest.io.duration"]; exists {
		t.Error("viperblock.guest.io.duration collides with duration.sum; the histogram is named .latency")
	}
}
