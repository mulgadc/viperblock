package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// engineCount sums viperblock.volume.engines across every attribute set whose
// volume matches, which is how a reader asks "how many engines hold this
// volume right now" without caring which process each one is.
func engineCount(t *testing.T, metrics map[string]any, volume string) int64 {
	t.Helper()
	sum, ok := metrics["viperblock.volume.engines"].(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("viperblock.volume.engines missing or wrong type: %#v", metrics["viperblock.volume.engines"])
	}
	var held int64
	for _, dp := range sum.DataPoints {
		if v, found := dp.Attributes.Value(attribute.Key("volume")); found && v.AsString() == volume {
			held += dp.Value
		}
	}
	return held
}

func collect(t *testing.T, reader interface {
	Collect(context.Context, *metricdata.ResourceMetrics) error
}) map[string]any {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return collectMetrics(t, rm)
}

// TestVolumeEnginesNetsToZero is the whole point of the close event: an open
// followed by its close must leave nothing held. Without it, the open alone
// looks identical to an engine that never let go.
func TestVolumeEnginesNetsToZero(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordVolumeOpen(ctx, "vol-net", "nbdkit")
	if held := engineCount(t, collect(t, reader), "vol-net"); held != 1 {
		t.Fatalf("engines held after open = %d, want 1", held)
	}

	RecordVolumeClose(ctx, "vol-net", "nbdkit")
	if held := engineCount(t, collect(t, reader), "vol-net"); held != 0 {
		t.Errorf("engines held after close = %d, want 0", held)
	}
}

// TestVolumeEnginesSeesConcurrentHolders is the condition being hunted: two
// engines holding one volume at the same time. Both opens land before either
// close, so a handover cannot produce this reading.
func TestVolumeEnginesSeesConcurrentHolders(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordVolumeOpen(ctx, "vol-dual", "nbdkit")
	RecordVolumeOpen(ctx, "vol-dual", "daemon")

	if held := engineCount(t, collect(t, reader), "vol-dual"); held != 2 {
		t.Fatalf("engines held = %d, want 2 — a dual-open must be readable directly", held)
	}

	RecordVolumeClose(ctx, "vol-dual", "daemon")
	if held := engineCount(t, collect(t, reader), "vol-dual"); held != 1 {
		t.Errorf("engines held after one release = %d, want 1", held)
	}
}

// TestVolumeEnginesHandoverIsNotADualOpen pins the distinction the open
// counter alone cannot make. The same two opens as above, with a close in
// between, never reads above one.
func TestVolumeEnginesHandoverIsNotADualOpen(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordVolumeOpen(ctx, "vol-handover", "daemon")
	RecordVolumeClose(ctx, "vol-handover", "daemon")
	RecordVolumeOpen(ctx, "vol-handover", "nbdkit")

	metrics := collect(t, reader)
	if held := engineCount(t, metrics, "vol-handover"); held != 1 {
		t.Errorf("engines held after handover = %d, want 1", held)
	}

	opens, ok := metrics["viperblock.volume.opens"].(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("viperblock.volume.opens missing: %#v", metrics["viperblock.volume.opens"])
	}
	var total int64
	for _, dp := range opens.DataPoints {
		if v, found := dp.Attributes.Value(attribute.Key("volume")); found && v.AsString() == "vol-handover" {
			total += dp.Value
		}
	}
	if total != 2 {
		t.Errorf("opens = %d, want 2 — the two readings must disagree, or the engine count adds nothing", total)
	}
}

// TestRecordVolumeCloseIsNoopWithoutRealProvider mirrors the open path: the
// global meter stays a no-op until Init installs a provider, and a close on
// shutdown must tolerate that rather than panic.
func TestRecordVolumeCloseIsNoopWithoutRealProvider(t *testing.T) {
	RecordVolumeClose(context.Background(), "vol-1", "nbdkit")
}
