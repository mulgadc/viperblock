package s3

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Deliberately distinctive so a match cannot be a coincidence, and obviously
// fake so the assertion failure message is harmless.
const (
	fakeAccessKey = "AKIA-NOTAREALACCESSKEY-0001"
	fakeSecretKey = "s3cr3t-NOTAREALSECRETKEY-0002"
)

// TestInitDoesNotLogCredentials pins the fix for a backend init that logged
// S3Config wholesale. S3Config carries static credentials, so logging the
// struct writes the secret key in plaintext to wherever the embedder's logger
// points — for spinifex, the journal and the log shipper behind it.
func TestInitDoesNotLogCredentials(t *testing.T) {
	server := newProbeServer(t, http.StatusOK)

	var logged bytes.Buffer
	backend, err := New(S3Config{
		VolumeName: "vol-logging00000001",
		Bucket:     "bucket",
		Region:     "us-east-1",
		AccessKey:  fakeAccessKey,
		SecretKey:  fakeSecretKey,
		Host:       server.URL,
	})
	require.NoError(t, err)
	backend.SetLogger(slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))

	require.NoError(t, backend.InitCtx(context.Background()))

	out := logged.String()
	require.NotEmpty(t, out, "the backend logged nothing, so this asserts nothing")
	require.NotContains(t, out, fakeSecretKey, "backend init logged the secret key")
	require.NotContains(t, out, fakeAccessKey, "backend init logged the access key")

	// The line still has to be worth emitting, or a future change could satisfy
	// the assertions above by removing it.
	require.True(t, strings.Contains(out, "vol-logging00000001") && strings.Contains(out, "bucket"),
		"the init log no longer identifies which backend it is about")
}
