package s3

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/minio/crc64nvme"
	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type capturedPut struct {
	header http.Header
	body   []byte
}

// putCapturingServer records every PUT it is sent and answers anything else as
// an empty bucket listing, which is all Init asks for.
func putCapturingServer(t *testing.T) (*httptest.Server, func() []capturedPut) {
	t.Helper()
	var mu sync.Mutex
	var puts []capturedPut
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			puts = append(puts, capturedPut{header: r.Header.Clone(), body: body})
			mu.Unlock()
			w.Header().Set("ETag", `"0123456789abcdef0000000000000004"`)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	t.Cleanup(srv.Close)
	return srv, func() []capturedPut {
		mu.Lock()
		defer mu.Unlock()
		return append([]capturedPut(nil), puts...)
	}
}

func crc64Base64(body []byte) string {
	sum := make([]byte, 8)
	binary.BigEndian.PutUint64(sum, crc64nvme.Checksum(body))
	return base64.StdEncoding.EncodeToString(sum)
}

// TestChunkWritesCarryCRC64NVMEAndTheEBSOptIn pins what predastore needs to
// check a chunk with CRC64NVME instead of MD5: the checksum of exactly the
// bytes sent, as one header and no trailer, and the opt-in.
func TestChunkWritesCarryCRC64NVMEAndTheEBSOptIn(t *testing.T) {
	srv, puts := putCapturingServer(t)
	backend, err := New(S3Config{
		VolumeName: "vol-crc64nvme0000",
		Bucket:     "bucket",
		Region:     "us-east-1",
		AccessKey:  "crc-access",
		SecretKey:  "crc-secret",
		Host:       srv.URL,
	})
	require.NoError(t, err)
	require.NoError(t, backend.InitCtx(context.Background()))

	headers := []byte("VBCHUNK\x00\x01\x02")
	data := bytes.Repeat([]byte{7}, 4<<20)
	ctx := context.Background()
	require.NoError(t, backend.WriteCtx(ctx, types.FileTypeChunk, 1, &headers, &data))
	require.NoError(t, backend.WriteToCtx(ctx, "vol-other00000000", types.FileTypeChunk, 2, &headers, &data))

	got := puts()
	require.Len(t, got, 2)
	for _, p := range got {
		assert.Equal(t, crc64Base64(p.body), p.header.Get("X-Amz-Checksum-Crc64nvme"),
			"the checksum is over the bytes on the wire, header included")
		assert.Equal(t, "true", p.header.Get("X-Spinifex-Ebs"))
		assert.Empty(t, p.header.Get("X-Amz-Trailer"), "a buffered body needs no trailer")
		for name := range p.header {
			if strings.HasPrefix(strings.ToLower(name), "x-amz-checksum-") {
				assert.Equal(t, "X-Amz-Checksum-Crc64nvme", name, "exactly one checksum")
			}
		}
	}
}

// TestMetadataWritesKeepTheirRequest pins that only chunks opt in: a config
// write goes out as it did before, with no checksum and no opt-in.
func TestMetadataWritesKeepTheirRequest(t *testing.T) {
	srv, puts := putCapturingServer(t)
	backend, err := New(S3Config{
		VolumeName: "vol-crc64nvme0000",
		Bucket:     "bucket",
		Region:     "us-east-1",
		AccessKey:  "crc-access",
		SecretKey:  "crc-secret",
		Host:       srv.URL,
	})
	require.NoError(t, err)
	require.NoError(t, backend.InitCtx(context.Background()))

	data := []byte(`{"volume":"vol-crc64nvme0000"}`)
	require.NoError(t, backend.WriteCtx(context.Background(), types.FileTypeConfig, 0, nil, &data))

	got := puts()
	require.Len(t, got, 1)
	assert.Empty(t, got[0].header.Get("X-Spinifex-Ebs"))
	assert.Empty(t, got[0].header.Get("X-Amz-Checksum-Crc64nvme"))
}
