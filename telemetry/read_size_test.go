package telemetry_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/mulgadc/viperblock/types"
	s3backend "github.com/mulgadc/viperblock/viperblock/backends/s3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	readSizeEnv  = "VIPERBLOCK_READ_SIZE_METRICS"
	childModeEnv = "VB_READ_SIZE_TEST_CHILD"
	readSizeName = "viperblock.backend.read.size"
	readerVolume = "vol-reader"
	sourceVolume = "vol-source"
)

var wantBounds = []float64{4, 4096, 4112, 16384, 65536, 262144, 1048576, 2097152, 4194304, 4194314}

// objectTransport answers S3 GETs from an in-memory map of keys, honouring a
// single byte range. A key in short answers any range with its stored bytes,
// the way a store clamps a range that runs past the object's end.
type objectTransport struct {
	objects map[string][]byte
	short   map[string]bool
}

func (o *objectTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	body, ok := o.objects[key]
	if !ok {
		return respond(r, http.StatusNotFound, []byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`), nil), nil
	}
	rng := r.Header.Get("Range")
	if rng == "" || o.short[key] {
		status := http.StatusOK
		if rng != "" {
			status = http.StatusPartialContent
		}
		return respond(r, status, body, nil), nil
	}
	var start, end int
	if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil {
		return nil, err
	}
	h := http.Header{"Content-Range": {fmt.Sprintf("bytes %d-%d/%d", start, end, len(body))}}
	return respond(r, http.StatusPartialContent, body[start:end+1], h), nil
}

func respond(r *http.Request, status int, body []byte, h http.Header) *http.Response {
	if h == nil {
		h = http.Header{}
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))
	return &http.Response{
		StatusCode:    status,
		Header:        h,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       r,
	}
}

func newBackend(t *testing.T, tr http.RoundTripper) *s3backend.Backend {
	t.Helper()
	b, err := s3backend.New(s3backend.S3Config{
		VolumeName: readerVolume,
		Bucket:     "bucket",
		Region:     "us-east-1",
		AccessKey:  "test-access",
		SecretKey:  "test-secret",
		Host:       "http://store.invalid",
		HTTPClient: &http.Client{Transport: tr},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.InitReadOnlyCtx(context.Background()); err != nil {
		t.Fatal(err)
	}
	return b
}

// runChild re-runs this test binary for one child test with the read-size
// switch set or unset, so each setting is read at a fresh process start.
func runChild(t *testing.T, name string, on bool) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.v")
	env := []string{childModeEnv + "=1"}
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, readSizeEnv+"=") {
			env = append(env, kv)
		}
	}
	if on {
		env = append(env, readSizeEnv+"=1")
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil || !bytes.Contains(out, []byte("--- PASS: "+name)) {
		t.Fatalf("child %s (switch on=%v) failed: %v\n%s", name, on, err, out)
	}
}

func childOnly(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	if os.Getenv(childModeEnv) != "1" {
		t.Skip("runs only as a child process")
	}
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	return reader
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) (map[string]metricdata.Aggregation, []metricdata.HistogramDataPoint[int64]) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	all := map[string]metricdata.Aggregation{}
	var points []metricdata.HistogramDataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			all[m.Name] = m.Data
			if m.Name == readSizeName {
				h, ok := m.Data.(metricdata.Histogram[int64])
				if !ok {
					t.Fatalf("%s is %T, want an int64 histogram", readSizeName, m.Data)
				}
				points = h.DataPoints
			}
		}
	}
	return all, points
}

func attr(set attribute.Set, key string) string {
	v, _ := set.Value(attribute.Key(key))
	return v.AsString()
}

// pointFor returns the data point with the given attributes, or fails.
func pointFor(t *testing.T, points []metricdata.HistogramDataPoint[int64], fileType, source, volume string) metricdata.HistogramDataPoint[int64] {
	t.Helper()
	for _, p := range points {
		if attr(p.Attributes, "file_type") == fileType && attr(p.Attributes, "source") == source && attr(p.Attributes, "volume.name") == volume {
			return p
		}
	}
	t.Fatalf("no %s point for file_type=%s source=%s volume.name=%s in %+v", readSizeName, fileType, source, volume, points)
	return metricdata.HistogramDataPoint[int64]{}
}

func bucketOf(size float64) int {
	for i, b := range wantBounds {
		if size <= b {
			return i
		}
	}
	return len(wantBounds)
}

func wantSamples(t *testing.T, p metricdata.HistogramDataPoint[int64], sizes ...float64) {
	t.Helper()
	if fmt.Sprint(p.Bounds) != fmt.Sprint(wantBounds) {
		t.Fatalf("bounds %v, want %v", p.Bounds, wantBounds)
	}
	want := make([]uint64, len(wantBounds)+1)
	sum := 0.0
	for _, s := range sizes {
		want[bucketOf(s)]++
		sum += s
	}
	if p.Count != uint64(len(sizes)) || float64(p.Sum) != sum || fmt.Sprint(p.BucketCounts) != fmt.Sprint(want) {
		t.Fatalf("count %d sum %d buckets %v, want count %d sum %.0f buckets %v", p.Count, p.Sum, p.BucketCounts, len(sizes), sum, want)
	}
}

func readerObjects() *objectTransport {
	chunk := bytes.Repeat([]byte{7}, 4194314)
	return &objectTransport{
		objects: map[string][]byte{
			types.GetFilePath(types.FileTypeChunk, 1, readerVolume):  chunk,
			types.GetFilePath(types.FileTypeConfig, 0, sourceVolume): []byte(`{"name":"vol-source"}`),
			types.GetFilePath(types.FileTypeChunk, 2, sourceVolume):  bytes.Repeat([]byte{1}, 100),
			readerVolume + "/maps/head.1":                            bytes.Repeat([]byte{3}, 300),
		},
		short: map[string]bool{types.GetFilePath(types.FileTypeChunk, 2, sourceVolume): true},
	}
}

// doReads issues every read the tests count, and checks what each returns,
// which the histogram must not change.
func doReads(t *testing.T, b *s3backend.Backend) {
	t.Helper()
	ctx := context.Background()
	for _, r := range []struct{ off, n uint32 }{{10, 4096}, {10, 4}, {14, 4112}} {
		data, err := b.ReadCtx(ctx, types.FileTypeChunk, 1, r.off, r.n)
		if err != nil || len(data) != int(r.n) {
			t.Fatalf("ReadCtx(%d, %d) = %d bytes, %v", r.off, r.n, len(data), err)
		}
	}
	if _, err := b.ReadCtx(ctx, types.FileTypeChunk, 99, 0, 4096); err == nil {
		t.Fatal("ReadCtx of a missing chunk succeeded")
	}
	cfg, err := b.ReadFromCtx(ctx, sourceVolume, types.FileTypeConfig, 0, 0, 0)
	if err != nil || len(cfg) != 21 {
		t.Fatalf("ReadFromCtx(config) = %d bytes, %v", len(cfg), err)
	}
	short, err := b.ReadFromCtx(ctx, sourceVolume, types.FileTypeChunk, 2, 0, 4096)
	if err != nil || len(short) != 100 {
		t.Fatalf("short ReadFromCtx = %d bytes, %v; want the 100 bytes the store sent", len(short), err)
	}
	m, err := b.ReadKeyCtx(ctx, "maps/head.1")
	if err != nil || len(m) != 300 {
		t.Fatalf("ReadKeyCtx = %d bytes, %v", len(m), err)
	}
}

func TestReadSizeHistogramWhenOn(t *testing.T)  { runChild(t, "TestReadSizeChildOn", true) }
func TestReadSizeHistogramWhenOff(t *testing.T) { runChild(t, "TestReadSizeChildOff", false) }

func TestReadSizeChildOn(t *testing.T) {
	reader := childOnly(t)
	b := newBackend(t, readerObjects())
	doReads(t, b)

	_, points := collect(t, reader)
	wantSamples(t, pointFor(t, points, "chunk", "own", readerVolume), 4096, 4, 4112)
	wantSamples(t, pointFor(t, points, "config", "inherited", sourceVolume), 21)
	wantSamples(t, pointFor(t, points, "chunk", "inherited", sourceVolume), 4096)
	wantSamples(t, pointFor(t, points, "map", "own", readerVolume), 300)
	if len(points) != 4 {
		t.Fatalf("%d points, want 4; a failed GET must not be counted: %+v", len(points), points)
	}
}

// A ranged read the store answers short still fails ReadCtx, and is counted
// at the size it asked for.
func TestReadSizeChildShortOwnRead(t *testing.T) {
	reader := childOnly(t)
	tr := readerObjects()
	key := types.GetFilePath(types.FileTypeChunk, 1, readerVolume)
	tr.objects[key] = bytes.Repeat([]byte{7}, 100)
	tr.short[key] = true
	b := newBackend(t, tr)

	_, err := b.ReadCtx(context.Background(), types.FileTypeChunk, 1, 0, 4096)
	if !errors.Is(err, types.ErrShortRead) {
		t.Fatalf("short ReadCtx err = %v, want ErrShortRead", err)
	}
	_, points := collect(t, reader)
	wantSamples(t, pointFor(t, points, "chunk", "own", readerVolume), 4096)
}

func TestReadSizeHistogramShortOwnRead(t *testing.T) {
	runChild(t, "TestReadSizeChildShortOwnRead", true)
}

func TestReadSizeChildOff(t *testing.T) {
	reader := childOnly(t)
	b := newBackend(t, readerObjects())
	doReads(t, b)

	all, points := collect(t, reader)
	if _, ok := all["viperblock.backend.io.ops"]; !ok {
		t.Fatal("viperblock.backend.io.ops not recorded, so the reader is not live")
	}
	if len(points) != 0 {
		t.Fatalf("switch off recorded %d %s points: %+v", len(points), readSizeName, points)
	}
}
