package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecordWriteBackpressureCountsWaitAndDuration(t *testing.T) {
	reader := withManualReader(t)

	RecordWriteBackpressure(context.Background(), "vol-1", 30*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	waits, ok := metrics["viperblock.write.backpressure.waits"].(metricdata.Sum[int64])
	if !ok || len(waits.DataPoints) != 1 || waits.DataPoints[0].Value != 1 {
		t.Fatalf("waits = %#v, want one point of 1", metrics["viperblock.write.backpressure.waits"])
	}
	durSum, ok := metrics["viperblock.write.backpressure.duration.sum"].(metricdata.Sum[float64])
	if !ok || len(durSum.DataPoints) != 1 || durSum.DataPoints[0].Value != 0.03 {
		t.Fatalf("duration = %#v, want 0.03s", metrics["viperblock.write.backpressure.duration.sum"])
	}
	wantAttr(t, waits.DataPoints[0].Attributes, "volume.name", "vol-1")
}

// The counters are what make backpressure separable from the write itself, so
// they must accumulate across waits rather than report only the latest.
func TestRecordWriteBackpressureAccumulates(t *testing.T) {
	reader := withManualReader(t)

	RecordWriteBackpressure(context.Background(), "vol-1", 10*time.Millisecond)
	RecordWriteBackpressure(context.Background(), "vol-1", 40*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	waits := metrics["viperblock.write.backpressure.waits"].(metricdata.Sum[int64])
	if waits.DataPoints[0].Value != 2 {
		t.Errorf("waits = %d, want 2", waits.DataPoints[0].Value)
	}
	durSum := metrics["viperblock.write.backpressure.duration.sum"].(metricdata.Sum[float64])
	if got := durSum.DataPoints[0].Value; got != 0.05 {
		t.Errorf("duration sum = %v, want 0.05", got)
	}
}
