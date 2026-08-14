// Which HTTP version the S3 transport actually negotiates. The engine issues
// small ranged GETs concurrently with 4 MiB chunk PUTs, and HTTP/2 puts all of
// them on one connection where a single chunk body exhausts the flow-control
// window. These assert the negotiated protocol against a real TLS server
// rather than the transport fields, because ALPN is what decides it.
package s3

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// newALPNServer starts a TLS server offering h2 ahead of http/1.1, the way
// predastore's gate does when HTTP/2 is enabled, and echoes the version it
// ended up speaking.
func newALPNServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.Proto))
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// protoAgainst drives one request over tr and returns the protocol the server
// saw, which is authoritative in a way the transport's own fields are not.
func protoAgainst(t *testing.T, tr *http.Transport, srv *httptest.Server) string {
	t.Helper()
	tr.TLSClientConfig.RootCAs = srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs

	resp, err := (&http.Client{Transport: tr}).Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	buf := make([]byte, 64)
	n, _ := resp.Body.Read(buf)
	return string(buf[:n])
}

func TestS3TransportSpeaksHTTP11AgainstAnH2Server(t *testing.T) {
	// The server offers h2 first. Declining it in ALPN is what keeps the
	// transport on a pool of connections instead of one multiplexed stream.
	require.Equal(t, "HTTP/1.1", protoAgainst(t, newS3Transport(false), newALPNServer(t)))
}

func TestS3TransportOptsIntoHTTP2(t *testing.T) {
	require.Equal(t, "HTTP/2.0", protoAgainst(t, newS3Transport(true), newALPNServer(t)))
}

func TestHTTP2EnabledReadsTheEnvironment(t *testing.T) {
	t.Setenv(http2Env, "1")
	require.True(t, http2Enabled())

	t.Setenv(http2Env, "")
	require.False(t, http2Enabled())

	// Anything other than an explicit 1 leaves the default in place, so a
	// stray "true" does not silently re-enable multiplexing.
	t.Setenv(http2Env, "true")
	require.False(t, http2Enabled())
}

func TestNewHTTPClientDefaultsToHTTP11(t *testing.T) {
	t.Setenv(http2Env, "")
	require.NotNil(t, NewHTTPClient())
	require.False(t, http2Enabled())
}

func TestS3TransportPoolIsWideEnoughToReplaceMultiplexing(t *testing.T) {
	tr := newS3Transport(false)

	// Without h2 the pool is the only source of concurrency, and Go's default
	// of 2 would serialise everything an nbdkit process has in flight.
	require.GreaterOrEqual(t, tr.MaxIdleConnsPerHost, 200)
	require.Zero(t, tr.MaxConnsPerHost, "an upper bound here would cap in-flight requests")
	require.Nil(t, tr.Proxy, "a proxy from the environment would capture the endpoint")

	// A 4 MiB chunk body against the stdlib's 4 KiB default is a thousand
	// syscalls per PUT.
	require.GreaterOrEqual(t, tr.WriteBufferSize, 64<<10)
	require.GreaterOrEqual(t, tr.ReadBufferSize, 64<<10)
}

func TestS3TransportTLSConfigIsNotShared(t *testing.T) {
	a, b := newS3Transport(false), newS3Transport(false)

	// Each transport owns its session cache and ALPN list; a shared one would
	// let a caller mutating one change every other client's TLS.
	require.NotSame(t, a.TLSClientConfig, b.TLSClientConfig)
}
