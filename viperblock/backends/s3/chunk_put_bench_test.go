package s3

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/viperblock/types"
)

// BenchmarkChunkWriteCtx is one 4 MiB chunk PUT through WriteCtx to a TLS
// endpoint that only drains the body, so it measures what the sender costs:
// signing, TLS and whatever the backend computes over the body before it sends.
func BenchmarkChunkWriteCtx(b *testing.B) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Method == http.MethodPut {
			w.Header().Set("ETag", `"0123456789abcdef0000000000000004"`)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	b.Cleanup(srv.Close)
	backend, err := New(S3Config{
		VolumeName: "vol-bench00000000",
		Bucket:     "bucket",
		Region:     "us-east-1",
		AccessKey:  "bench-access",
		SecretKey:  "bench-secret",
		Host:       srv.URL,
		HTTPClient: srv.Client(),
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := backend.InitCtx(context.Background()); err != nil {
		b.Fatal(err)
	}
	headers := []byte("VBCHUNK\x00\x01\x02")
	data := bytes.Repeat([]byte{7}, 4<<20)
	b.SetBytes(int64(len(headers) + len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := backend.WriteCtx(context.Background(), types.FileTypeChunk, uint64(i), &headers, &data); err != nil {
			b.Fatal(err)
		}
	}
}
