package telemetry_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/mulgadc/viperblock/types"
	s3backend "github.com/mulgadc/viperblock/viperblock/backends/s3"
)

// memTransport answers every GET from one in-memory object, honouring a single
// byte range, so a read benchmark measures the backend and the SDK and not a
// socket, a server or the scheduler.
type memTransport struct{ object []byte }

func (m memTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	body, status := m.object, http.StatusOK
	h := http.Header{}
	if rng := r.Header.Get("Range"); rng != "" {
		var start, end int
		if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err != nil {
			return nil, err
		}
		body, status = m.object[start:end+1], http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(m.object)))
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: r}, nil
}

func benchBackend(b *testing.B) *s3backend.Backend {
	b.Helper()
	backend, err := s3backend.New(s3backend.S3Config{
		VolumeName: "vol-bench",
		Bucket:     "bucket",
		Region:     "us-east-1",
		AccessKey:  "bench-access",
		SecretKey:  "bench-secret",
		Host:       "http://store.invalid",
		HTTPClient: &http.Client{Transport: memTransport{object: bytes.Repeat([]byte{7}, 4194314)}},
	})
	if err != nil {
		b.Fatal(err)
	}
	backend.SetLogger(slog.New(slog.DiscardHandler))
	if err := backend.InitReadOnlyCtx(context.Background()); err != nil {
		b.Fatal(err)
	}
	return backend
}

// BenchmarkS3ReadCtx measures one object-store read through the S3 backend at
// the sizes a guest issues: a 4 KiB block, a whole chunk, and 16 readers of blocks.
// It lives here because this test binary starts no store in the background.
func BenchmarkS3ReadCtx(b *testing.B) {
	ctx := context.Background()
	b.Run("4KiB", func(b *testing.B) {
		backend := benchBackend(b)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := backend.ReadCtx(ctx, types.FileTypeChunk, 1, 10, 4096); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("whole-chunk", func(b *testing.B) {
		backend := benchBackend(b)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := backend.ReadCtx(ctx, types.FileTypeChunk, 1, 0, 0); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("4KiB-16-readers", func(b *testing.B) {
		const readers = 16
		backend := benchBackend(b)
		b.ReportAllocs()
		b.ResetTimer()
		var wg sync.WaitGroup
		for r := range readers {
			wg.Go(func() {
				for i := r; i < b.N; i += readers {
					if _, err := backend.ReadCtx(ctx, types.FileTypeChunk, 1, 10, 4096); err != nil {
						b.Error(err)
						return
					}
				}
			})
		}
		wg.Wait()
	})
}
