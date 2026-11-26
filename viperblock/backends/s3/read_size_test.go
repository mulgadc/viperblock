package s3

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/require"
)

// Every object type has its own read-size label, so no two kinds of read are
// counted together.
func TestFileTypeNamesAreDistinct(t *testing.T) {
	seen := map[string]types.FileType{}
	for ft := types.FileTypeConfig; ft <= types.FileTypeSnapshotMarker; ft++ {
		name := fileTypeName(ft)
		if name == "other" {
			t.Errorf("file type %d has no name", ft)
		}
		if prev, dup := seen[name]; dup {
			t.Errorf("file types %d and %d share the name %q", prev, ft, name)
		}
		seen[name] = ft
	}
	if fileTypeName(types.FileTypeSnapshotMarker+1) != "other" {
		t.Error("an unknown file type is not named other")
	}
}

// storeReply answers every GET with body, and fails part way through it when
// broken is set, as a dropped connection does.
type storeReply struct {
	body   []byte
	broken bool
}

func (s storeReply) RoundTrip(r *http.Request) (*http.Response, error) {
	var body io.Reader = bytes.NewReader(s.body)
	if s.broken {
		body = io.MultiReader(bytes.NewReader(s.body[:len(s.body)/2]), errReader{})
	}
	h := http.Header{"Content-Length": {strconv.Itoa(len(s.body))}}
	return &http.Response{StatusCode: http.StatusOK, Header: h, Body: io.NopCloser(body), ContentLength: int64(len(s.body)), Request: r}, nil
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func replyBackend(t *testing.T, reply storeReply) *Backend {
	t.Helper()
	b, err := New(S3Config{VolumeName: "vol-a", Bucket: "bucket", Region: "us-east-1",
		AccessKey: "test-access", SecretKey: "test-secret", Host: "http://store.invalid",
		HTTPClient: &http.Client{Transport: reply}})
	require.NoError(t, err)
	b.SetLogger(slog.New(slog.DiscardHandler))
	require.NoError(t, b.InitReadOnlyCtx(context.Background()))
	return b
}

// What each read returns when the store answers short or drops the body is
// what callers rely on, and the read-size count must leave it alone.
func TestReadsReturnWhatTheStoreSent(t *testing.T) {
	ctx := context.Background()
	short := replyBackend(t, storeReply{body: bytes.Repeat([]byte{1}, 100)})
	got, err := short.ReadFromCtx(ctx, "vol-b", types.FileTypeChunk, 1, 0, 4096)
	require.NoError(t, err, "an inherited read returns a short body for its caller to judge")
	require.Len(t, got, 100)
	_, err = short.ReadCtx(ctx, types.FileTypeChunk, 1, 0, 4096)
	require.ErrorIs(t, err, types.ErrShortRead)

	broken := replyBackend(t, storeReply{body: bytes.Repeat([]byte{2}, 8192), broken: true})
	_, err = broken.ReadCtx(ctx, types.FileTypeChunk, 1, 0, 8192)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	_, err = broken.ReadFromCtx(ctx, "vol-b", types.FileTypeChunk, 1, 0, 8192)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	_, err = broken.ReadKeyCtx(ctx, "maps/head.1")
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	whole := replyBackend(t, storeReply{body: []byte("map")})
	got, err = whole.ReadKeyCtx(ctx, "maps/head.1")
	require.NoError(t, err)
	require.Equal(t, []byte("map"), got)
}
