package s3

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/internal/predastoretest"
	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file exercises the s3 backend's Delete/List surface (added for
// viperblock chunk GC) against a real predastore instance rather than a
// hand-rolled mock. That matters specifically for Delete/DeleteCtx:
// predastore, unlike real S3, errors on deleting an already-missing key
// instead of treating it as an idempotent success (see wrapNotFound's doc
// comment) -- a synthetic mock could easily get that quirk wrong and hide
// exactly the bug that would make chunk GC's "already gone == success"
// contract silently false.

// sharedTestServer holds the predastore cluster started once for this
// package's tests in TestMain.
var sharedTestServer *predastoretest.Server

func TestMain(m *testing.M) {
	os.Exit(func() int {
		dir, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get working directory: %v\n", err)
			return 1
		}
		// backends/s3 -> backends -> viperblock -> module root, where
		// config/server.{pem,key} live (mirrors viperblock package's own
		// TestMain, one directory shallower).
		repoRoot := filepath.Join(dir, "..", "..", "..")
		certPath := filepath.Join(repoRoot, "config", "server.pem")

		// predastore's internal s3db + QUIC clients (and the raft transport
		// between nodes) verify TLS strictly against the OS trust store, not
		// just the S3-facing HTTP client this test's own requests use. Point
		// SystemCertPool at the self-signed test cert so node-to-node and
		// s3db verification succeeds without installing the cert
		// system-wide. Must be set before any TLS handshake in this
		// process — mirrors the viperblock package's own TestMain.
		if err := os.Setenv("SSL_CERT_FILE", certPath); err != nil {
			fmt.Fprintf(os.Stderr, "failed to set SSL_CERT_FILE: %v\n", err)
			return 1
		}

		dataDir, err := os.MkdirTemp("", "viperblock-s3backend-predastore-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
			return 1
		}
		defer os.RemoveAll(dataDir)

		srv, err := predastoretest.Start(predastoretest.Options{
			DataDir:    dataDir,
			CertPath:   certPath,
			KeyPath:    filepath.Join(repoRoot, "config", "server.key"),
			BucketName: "predastore",
			AccountID:  "123456789012",
			Region:     "ap-southeast-2",
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to start predastore test cluster: %v\n", err)
			return 1
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if shutdownErr := srv.Shutdown(shutdownCtx); shutdownErr != nil {
				fmt.Fprintf(os.Stderr, "predastore shutdown error: %v\n", shutdownErr)
			}
		}()

		sharedTestServer = srv

		return m.Run()
	}())
}

// insecureTestHTTPClient skips TLS verification for the self-signed test
// predastore cert, matching the viperblock package's own test setup.
var insecureTestHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	},
}

// newGCTestBackend returns an initialized S3 backend pointed at the shared
// test predastore cluster, scoped to volumeName.
func newGCTestBackend(t *testing.T, volumeName string) *Backend {
	t.Helper()

	backend := New(S3Config{
		VolumeName: volumeName,
		VolumeSize: 8 * 1024 * 1024,
		Bucket:     sharedTestServer.Bucket,
		Region:     sharedTestServer.Region,
		AccessKey:  sharedTestServer.AccessKey,
		SecretKey:  sharedTestServer.SecretKey,
		Host:       sharedTestServer.Endpoint,
		HTTPClient: insecureTestHTTPClient,
	})
	require.NoError(t, backend.Init())
	return backend
}

// uniqueVolumeName returns a volume name scoped to the running test, so
// parallel/sequential tests in this file never collide on the shared
// cluster's key space.
func uniqueVolumeName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("gctest-%s-%d", t.Name(), time.Now().UnixNano())
}

func TestS3DeleteAndDeleteCtx(t *testing.T) {
	backend := newGCTestBackend(t, uniqueVolumeName(t))

	headers := []byte{}
	data := []byte("chunk-data")
	require.NoError(t, backend.Write(types.FileTypeChunk, 0, &headers, &data))
	require.NoError(t, backend.Write(types.FileTypeChunk, 1, &headers, &data))

	require.NoError(t, backend.Delete(types.FileTypeChunk, 0))
	_, err := backend.Read(types.FileTypeChunk, 0, 0, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)

	// The sibling object is untouched.
	got, err := backend.Read(types.FileTypeChunk, 1, 0, uint32(len(data)))
	require.NoError(t, err)
	assert.Equal(t, data, got)

	require.NoError(t, backend.DeleteCtx(context.Background(), types.FileTypeChunk, 1))
	_, err = backend.Read(types.FileTypeChunk, 1, 0, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

// TestS3DeleteAlreadyMissingReturnsErrNotExist pins the specific predastore
// quirk wrapNotFound exists to paper over: deleting an already-absent key
// errors instead of succeeding idempotently the way real S3 does. Chunk
// GC's sweep depends on errors.Is(err, os.ErrNotExist) here to treat a
// re-swept or already-reclaimed chunk as success, not a retry-worthy
// failure.
func TestS3DeleteAlreadyMissingReturnsErrNotExist(t *testing.T) {
	backend := newGCTestBackend(t, uniqueVolumeName(t))

	err := backend.Delete(types.FileTypeChunk, 999)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist, "expected os.ErrNotExist for delete of never-written key, got %v", err)

	err = backend.DeleteCtx(context.Background(), types.FileTypeChunk, 999)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist, "expected os.ErrNotExist for delete of never-written key, got %v", err)

	// Double-delete of a key this backend itself just removed must also
	// report ErrNotExist, not a transient/internal error.
	headers := []byte{}
	data := []byte("x")
	require.NoError(t, backend.Write(types.FileTypeChunk, 5, &headers, &data))
	require.NoError(t, backend.Delete(types.FileTypeChunk, 5))
	err = backend.Delete(types.FileTypeChunk, 5)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestS3ListPrefixesAndListPrefixesCtx(t *testing.T) {
	base := uniqueVolumeName(t)
	alpha := "snap-" + base + "-alpha"
	beta := "snap-" + base + "-beta"
	other := "other-" + base

	backend := newGCTestBackend(t, base)

	headers := []byte{}
	data := []byte("x")
	require.NoError(t, backend.WriteTo(alpha, types.FileTypeConfig, 0, &headers, &data))
	require.NoError(t, backend.WriteTo(beta, types.FileTypeConfig, 0, &headers, &data))
	require.NoError(t, backend.WriteTo(other, types.FileTypeConfig, 0, &headers, &data))

	names, err := backend.ListPrefixes("snap-" + base)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{alpha, beta}, names)

	names, err = backend.ListPrefixesCtx(context.Background(), "snap-"+base)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{alpha, beta}, names)
}

func TestS3ListObjectsAndListObjectsCtx(t *testing.T) {
	volumeName := uniqueVolumeName(t)
	backend := newGCTestBackend(t, volumeName)

	headers := []byte{}
	data := []byte("chunk")
	require.NoError(t, backend.Write(types.FileTypeChunk, 0, &headers, &data))
	require.NoError(t, backend.Write(types.FileTypeChunk, 1, &headers, &data))
	require.NoError(t, backend.Write(types.FileTypeChunk, 2, &headers, &data))

	keys, err := backend.ListObjects(volumeName + "/chunks/")
	require.NoError(t, err)
	assert.Len(t, keys, 3)

	keys, err = backend.ListObjectsCtx(context.Background(), volumeName+"/chunks/")
	require.NoError(t, err)
	assert.Len(t, keys, 3)

	require.NoError(t, backend.Delete(types.FileTypeChunk, 1))
	keys, err = backend.ListObjects(volumeName + "/chunks/")
	require.NoError(t, err)
	assert.Len(t, keys, 2)
}

func TestS3ListObjectsMissingPrefixIsEmptyNotError(t *testing.T) {
	backend := newGCTestBackend(t, uniqueVolumeName(t))

	keys, err := backend.ListObjects("no-such-volume-" + uniqueVolumeName(t) + "/chunks/")
	require.NoError(t, err)
	assert.Empty(t, keys)
}

// TestS3DeleteAndListRequireInitializedClient pins the guard-clause
// behavior when a backend's Init/InitCtx was never called: every new
// Delete/List method must fail closed with a clear error rather than a nil
// pointer panic.
func TestS3DeleteAndListRequireInitializedClient(t *testing.T) {
	backend := New(S3Config{VolumeName: "uninitialized", Bucket: "predastore"})

	err := backend.Delete(types.FileTypeChunk, 0)
	require.Error(t, err)

	err = backend.DeleteCtx(context.Background(), types.FileTypeChunk, 0)
	require.Error(t, err)

	_, err = backend.ListPrefixes("snap-")
	require.Error(t, err)

	_, err = backend.ListPrefixesCtx(context.Background(), "snap-")
	require.Error(t, err)

	_, err = backend.ListObjects("vol/chunks/")
	require.Error(t, err)

	_, err = backend.ListObjectsCtx(context.Background(), "vol/chunks/")
	require.Error(t, err)
}
