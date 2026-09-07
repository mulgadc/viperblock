package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestRecordGuestIOEmitsCounterBytesAndDuration(t *testing.T) {
	reader := withManualReader(t)

	RecordGuestIO(context.Background(), "write", "vol-1", "success", 4096, 7*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	sum, ok := metrics["viperblock.guest.io.ops"].(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Fatalf("viperblock.guest.io.ops = %#v, want one point of 1", metrics["viperblock.guest.io.ops"])
	}

	bytesSum, ok := metrics["viperblock.guest.io.bytes"].(metricdata.Sum[int64])
	if !ok || len(bytesSum.DataPoints) != 1 || bytesSum.DataPoints[0].Value != 4096 {
		t.Fatalf("viperblock.guest.io.bytes = %#v, want 4096", metrics["viperblock.guest.io.bytes"])
	}

	durSum, ok := metrics["viperblock.guest.io.duration.sum"].(metricdata.Sum[float64])
	if !ok || len(durSum.DataPoints) != 1 {
		t.Fatalf("viperblock.guest.io.duration.sum = %#v, want one point", metrics["viperblock.guest.io.duration.sum"])
	}
	if got := durSum.DataPoints[0].Value; got != 0.007 {
		t.Errorf("duration sum = %v seconds, want 0.007", got)
	}

	attrs := sum.DataPoints[0].Attributes
	wantAttr(t, attrs, "op", "write")
	wantAttr(t, attrs, "outcome", "success")
	wantAttr(t, attrs, "volume.name", "vol-1")
}

// A flush moves no bytes but is the op whose latency matters most, so it must
// still record an op and a duration. Suppressing the whole record on zero bytes
// would erase exactly the measurement this instrument exists for.
func TestRecordGuestIOFlushCarriesNoBytesButIsStillTimed(t *testing.T) {
	reader := withManualReader(t)

	RecordGuestIO(context.Background(), "flush", "vol-1", "success", 0, 11*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	if bytesSum, ok := metrics["viperblock.guest.io.bytes"].(metricdata.Sum[int64]); ok && len(bytesSum.DataPoints) != 0 {
		t.Errorf("flush recorded %d bytes data points, want none", len(bytesSum.DataPoints))
	}

	sum, ok := metrics["viperblock.guest.io.ops"].(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
		t.Fatalf("flush was not counted: %#v", metrics["viperblock.guest.io.ops"])
	}
	durSum, ok := metrics["viperblock.guest.io.duration.sum"].(metricdata.Sum[float64])
	if !ok || len(durSum.DataPoints) != 1 || durSum.DataPoints[0].Value != 0.011 {
		t.Fatalf("flush duration = %#v, want 0.011s", metrics["viperblock.guest.io.duration.sum"])
	}
	wantAttr(t, sum.DataPoints[0].Attributes, "op", "flush")
}

// A failed request still consumed the guest's time. Recording only successes
// would make a volume that errors quickly look faster than one that works.
func TestRecordGuestIOSeparatesFailuresByOutcome(t *testing.T) {
	reader := withManualReader(t)

	RecordGuestIO(context.Background(), "flush", "vol-1", "success", 0, 2*time.Millisecond)
	RecordGuestIO(context.Background(), "flush", "vol-1", "error", 0, 90*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	sum, ok := metrics["viperblock.guest.io.ops"].(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 2 {
		t.Fatalf("want two series split by outcome, got %#v", metrics["viperblock.guest.io.ops"])
	}
	for _, dp := range sum.DataPoints {
		if dp.Value != 1 {
			t.Errorf("outcome series counted %d, want 1", dp.Value)
		}
	}
}
