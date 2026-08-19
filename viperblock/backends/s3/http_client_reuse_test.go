// What a backend costs before it has transferred a byte. Each http.Client
// carries its own connection pool, so a client per volume is a TCP connect and
// a TLS handshake per volume — invisible in a per-request timing and dominant
// in a verb that issues one request.
package s3

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// connCountingServer answers any list and counts how many distinct connections
// it was reached over, which is the thing sharing a client is supposed to
// change.
type connCountingServer struct {
	*httptest.Server

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func newConnCountingServer(t *testing.T) *connCountingServer {
	t.Helper()
	s := &connCountingServer{conns: map[net.Conn]struct{}{}}
	s.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>bucket</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	s.Config.ConnState = func(c net.Conn, state http.ConnState) {
		if state != http.StateNew {
			return
		}
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()
	}
	s.Start()
	t.Cleanup(s.Close)
	return s
}

func (s *connCountingServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// openBackends runs Init on n backends against host, each configured with the
// supplied client. A nil client is the per-backend default.
func openBackends(t *testing.T, host string, client *http.Client, n int) {
	t.Helper()
	for i := range n {
		backend, err := New(S3Config{
			VolumeName: "vol-reuse000000000" + string(rune('0'+i)),
			Bucket:     "bucket",
			Region:     "us-east-1",
			AccessKey:  "reuse-access",
			SecretKey:  "reuse-secret",
			Host:       host,
			HTTPClient: client,
		})
		require.NoError(t, err)
		require.NoError(t, backend.InitCtx(context.Background()))
	}
}

// TestSharedHTTPClientReusesOneConnection is the fix. Three backends handed one
// client must reach the endpoint over one connection, so only the first pays
// for setting it up.
func TestSharedHTTPClientReusesOneConnection(t *testing.T) {
	server := newConnCountingServer(t)
	openBackends(t, server.URL, NewHTTPClient(), 3)
	require.Equal(t, 1, server.count(),
		"backends sharing an http.Client opened more than one connection, so the pool is not being shared")
}

// TestUnsharedHTTPClientsEachConnect is the same measurement without the fix,
// so the test above is known to be measuring the client and not something the
// server or the SDK would have done anyway.
func TestUnsharedHTTPClientsEachConnect(t *testing.T) {
	server := newConnCountingServer(t)
	openBackends(t, server.URL, nil, 3)
	require.Equal(t, 3, server.count(),
		"backends building their own clients somehow shared a connection")
}
