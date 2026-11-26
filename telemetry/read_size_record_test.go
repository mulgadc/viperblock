package telemetry

import (
	"context"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// A recorded size lands in the bucket that separates it from its neighbours:
// the encrypted-chunk probe, a plain and an encrypted block, and a whole chunk.
func TestRecordBackendReadSizeBuckets(t *testing.T) {
	reader := withManualReader(t)
	readSizeOnce = sync.Once{}
	t.Cleanup(func() { readSizeOnce = sync.Once{} })
	ctx := context.Background()
	for _, n := range []int{4, 4096, 4112, 4<<20 + 10} {
		RecordBackendReadSize(ctx, "chunk", "own", "vol-1", n)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	h, ok := collectMetrics(t, rm)["viperblock.backend.read.size"].(metricdata.Histogram[int64])
	if !ok || len(h.DataPoints) != 1 {
		t.Fatalf("viperblock.backend.read.size = %#v, want one int64 histogram point", h)
	}
	p := h.DataPoints[0]
	wantAttr(t, p.Attributes, "file_type", "chunk")
	wantAttr(t, p.Attributes, "source", "own")
	wantAttr(t, p.Attributes, "volume.name", "vol-1")
	want := []uint64{1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0}
	for i, c := range p.BucketCounts {
		if c != want[i] {
			t.Fatalf("bucket counts %v, want %v", p.BucketCounts, want)
		}
	}
}
