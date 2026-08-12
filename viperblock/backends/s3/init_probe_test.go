package s3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// probeServer answers a list with an empty bucket and records what was asked
// for, so the probe's scope is asserted from the wire rather than from the
// call site.
type probeServer struct {
	*httptest.Server

	mu      sync.Mutex
	queries []map[string][]string
	status  int
}

func newProbeServer(t *testing.T, status int) *probeServer {
	t.Helper()
	p := &probeServer{status: status}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.queries = append(p.queries, r.URL.Query())
		p.mu.Unlock()
		if p.status != http.StatusOK {
			w.WriteHeader(p.status)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	t.Cleanup(p.Close)
	return p
}

func (p *probeServer) lastQuery(t *testing.T) map[string][]string {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.NotEmpty(t, p.queries, "the backend never issued a request")
	return p.queries[len(p.queries)-1]
}

func (p *probeServer) requestCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queries)
}

func probeBackend(host string) *Backend {
	return New(S3Config{
		VolumeName: "vol-probe0000000001",
		Bucket:     "bucket",
		Region:     "us-east-1",
		AccessKey:  "probe-access",
		SecretKey:  "probe-secret",
		Host:       host,
	})
}

// TestInitProbeIsScopedToTheVolume is the whole point of the probe's shape.
// An unscoped list is O(bucket) against predastore, which resolves object
// metadata per key, so a probe that omits the prefix gets slower as the store
// fills and takes every engine open with it.
func TestInitProbeIsScopedToTheVolume(t *testing.T) {
	server := newProbeServer(t, http.StatusOK)
	backend := probeBackend(server.URL)
	require.NoError(t, backend.InitCtx(context.Background()))

	query := server.lastQuery(t)
	require.Equal(t, []string{"vol-probe0000000001/"}, query["prefix"],
		"the reachability probe listed the whole bucket")
	require.Equal(t, []string{"1"}, query["max-keys"])
}

// TestInitReadOnlyIssuesNoProbe is the saving a read-only open exists for.
// Even scoped, the probe is a full round trip and predastore answers it by
// resolving metadata for every object under the volume's prefix — real work
// to learn something the state read that follows would report anyway.
func TestInitReadOnlyIssuesNoProbe(t *testing.T) {
	server := newProbeServer(t, http.StatusOK)
	backend := probeBackend(server.URL)
	require.NoError(t, backend.InitReadOnlyCtx(context.Background()))
	require.Zero(t, server.requestCount(), "a read-only init went to the backend")
}

// TestInitProbeStillFailsOnAnUnusableBackend keeps the probe worth issuing.
// Scoping it must not turn an unreachable or unauthorised backend into a
// successful open that fails later, somewhere less obvious.
func TestInitProbeStillFailsOnAnUnusableBackend(t *testing.T) {
	server := newProbeServer(t, http.StatusForbidden)
	backend := probeBackend(server.URL)
	require.Error(t, backend.InitCtx(context.Background()))
}
