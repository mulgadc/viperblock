package telemetry

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// withManualReader installs a real MeterProvider backed by a ManualReader
// for the duration of the test, and restores the previous global provider
// on cleanup. It also resets instrumentsOnce so the test's Record* calls
// create fresh instruments bound to the manual reader — otherwise the
// package-level Once fired by an earlier test would keep pointing at
// whatever provider was live when it first ran.
func withManualReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	instrumentsOnce = sync.Once{}
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		instrumentsOnce = sync.Once{}
	})
	return reader
}

// collectMetrics flattens a ResourceMetrics snapshot into name -> data
// keyed on the metric name, for simple by-name assertions.
func collectMetrics(t *testing.T, rm metricdata.ResourceMetrics) map[string]any {
	t.Helper()
	out := map[string]any{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m.Data
		}
	}
	return out
}

func wantAttr(t *testing.T, attrs attribute.Set, key, want string) {
	t.Helper()
	v, ok := attrs.Value(attribute.Key(key))
	if !ok {
		t.Errorf("attribute %s missing, want %q", key, want)
		return
	}
	if v.AsString() != want {
		t.Errorf("attribute %s = %q, want %q", key, v.AsString(), want)
	}
}

func TestRecordBackendIOEmitsCounterBytesAndDuration(t *testing.T) {
	reader := withManualReader(t)

	RecordBackendIO(context.Background(), "read", "s3", "vol-1", "success", 4096, 12*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	sum, ok := metrics["viperblock.backend.io"].(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 {
		t.Fatalf("viperblock.backend.io = %#v, want one int64 sum point", metrics["viperblock.backend.io"])
	}
	if sum.DataPoints[0].Value != 1 {
		t.Errorf("io count = %d, want 1", sum.DataPoints[0].Value)
	}

	bytesSum, ok := metrics["viperblock.backend.io.bytes"].(metricdata.Sum[int64])
	if !ok || len(bytesSum.DataPoints) != 1 || bytesSum.DataPoints[0].Value != 4096 {
		t.Fatalf("viperblock.backend.io.bytes = %#v, want 4096", metrics["viperblock.backend.io.bytes"])
	}

	hist, ok := metrics["viperblock.backend.io.duration"].(metricdata.Histogram[float64])
	if !ok || len(hist.DataPoints) != 1 || hist.DataPoints[0].Count != 1 {
		t.Fatalf("viperblock.backend.io.duration = %#v, want one histogram point with count 1", metrics["viperblock.backend.io.duration"])
	}

	attrs := sum.DataPoints[0].Attributes
	wantAttr(t, attrs, "op", "read")
	wantAttr(t, attrs, "backend", "s3")
	wantAttr(t, attrs, "outcome", "success")
	wantAttr(t, attrs, "volume.name", "vol-1")
}

func TestRecordBackendIOZeroBytesAndEmptyVolume(t *testing.T) {
	reader := withManualReader(t)

	RecordBackendIO(context.Background(), "write", "file", "", "error", 0, time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	if bytesSum, ok := metrics["viperblock.backend.io.bytes"].(metricdata.Sum[int64]); ok && len(bytesSum.DataPoints) != 0 {
		t.Errorf("expected no bytes data points recorded for a zero-byte op, got %d", len(bytesSum.DataPoints))
	}

	sum, ok := metrics["viperblock.backend.io"].(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 {
		t.Fatalf("viperblock.backend.io = %#v, want one int64 sum point", metrics["viperblock.backend.io"])
	}
	wantAttr(t, sum.DataPoints[0].Attributes, "outcome", "error")
	if _, ok := sum.DataPoints[0].Attributes.Value(attribute.Key("volume.name")); ok {
		t.Error("expected no volume.name attribute for an empty volume name")
	}
}

func TestRecordWALOpEmitsCounterAndDuration(t *testing.T) {
	reader := withManualReader(t)

	RecordWALOp(context.Background(), "consolidate", "vol-9", "success", 5*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	sum, ok := metrics["viperblock.wal.operations"].(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) != 1 {
		t.Fatalf("viperblock.wal.operations = %#v, want one point", metrics["viperblock.wal.operations"])
	}
	wantAttr(t, sum.DataPoints[0].Attributes, "phase", "consolidate")
	wantAttr(t, sum.DataPoints[0].Attributes, "volume.name", "vol-9")

	hist, ok := metrics["viperblock.wal.operation.duration"].(metricdata.Histogram[float64])
	if !ok || len(hist.DataPoints) != 1 {
		t.Fatalf("viperblock.wal.operation.duration = %#v, want one point", metrics["viperblock.wal.operation.duration"])
	}
}

func TestRecordCacheLookupHitAndMiss(t *testing.T) {
	reader := withManualReader(t)

	RecordCacheLookup(context.Background(), true)
	RecordCacheLookup(context.Background(), true)
	RecordCacheLookup(context.Background(), false)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	metrics := collectMetrics(t, rm)

	sum, ok := metrics["viperblock.cache.lookups"].(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("viperblock.cache.lookups missing or wrong type: %#v", metrics["viperblock.cache.lookups"])
	}

	var hits, misses int64
	for _, dp := range sum.DataPoints {
		result, _ := dp.Attributes.Value(attribute.Key("result"))
		switch result.AsString() {
		case "hit":
			hits += dp.Value
		case "miss":
			misses += dp.Value
		}
	}
	if hits != 2 {
		t.Errorf("hits = %d, want 2", hits)
	}
	if misses != 1 {
		t.Errorf("misses = %d, want 1", misses)
	}
}

// TestRecordFunctionsAreNoopWithoutRealProvider proves the hot-path record
// calls never panic — the global meter provider stays the SDK no-op until
// Init installs a real one, and Record* must tolerate that.
func TestRecordFunctionsAreNoopWithoutRealProvider(t *testing.T) {
	prev := otel.GetMeterProvider()
	instrumentsOnce = sync.Once{}
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		instrumentsOnce = sync.Once{}
	})

	RecordBackendIO(context.Background(), "read", "s3", "vol-1", "success", 4096, time.Millisecond)
	RecordWALOp(context.Background(), "flush", "vol-1", "success", time.Millisecond)
	RecordCacheLookup(context.Background(), true)
}
