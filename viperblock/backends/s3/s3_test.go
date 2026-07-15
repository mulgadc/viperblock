package s3

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
)

// TestNormalizeEndpoint pins Host's accepted input space. Callers pass a bare
// host:port (spinifex's admin.DialTarget yields one) as well as a full URL, and
// the SDK rejects a schemeless endpoint outright, so both shapes must resolve.
func TestNormalizeEndpoint(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			// The shape admin.DialTarget and Predastore.Host produce.
			name: "bare_host_port_gets_https",
			in:   "192.168.1.29:8443",
			want: "https://192.168.1.29:8443",
		},
		{
			name: "https_url_unchanged",
			in:   "https://127.0.0.1:8443",
			want: "https://127.0.0.1:8443",
		},
		{
			// An explicit http endpoint must not be silently upgraded.
			name: "http_url_unchanged",
			in:   "http://127.0.0.1:8443",
			want: "http://127.0.0.1:8443",
		},
		{
			name: "bare_hostname_gets_https",
			in:   "predastore.internal",
			want: "https://predastore.internal",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, normalizeEndpoint(tc.in))
		})
	}
}

// TestWrapNotFound pins the AWS-error → os.ErrNotExist mapping that callers
// (e.g. viperblock.LoadState) rely on to tell "object missing" apart from
// "backend unreachable". Required by the medium-term fix.
func TestWrapNotFound(t *testing.T) {
	cases := []struct {
		name         string
		in           error
		wantNotFound bool
	}{
		{
			name:         "nil_passes_through",
			in:           nil,
			wantNotFound: false,
		},
		{
			name:         "no_such_key_wraps_not_exist",
			in:           &s3types.NoSuchKey{Message: aws.String("key missing")},
			wantNotFound: true,
		},
		{
			// GetObject models only NoSuchKey and InvalidObjectState, so a read
			// against a missing bucket arrives unmodeled, carrying just the code.
			// This is the shape ReadCtx actually sees, and viperblock.LoadState
			// depends on it meaning "not exist".
			name:         "no_such_bucket_wraps_not_exist",
			in:           &smithy.GenericAPIError{Code: "NoSuchBucket", Message: "bucket missing"},
			wantNotFound: true,
		},
		{
			// Modeled errors report their code through ErrorCode() like any
			// other API error, so they match the same switch. Pins that, and
			// guards the day the SDK starts modeling NoSuchBucket on GetObject.
			name:         "no_such_bucket_typed_wraps_not_exist",
			in:           &s3types.NoSuchBucket{Message: aws.String("bucket missing")},
			wantNotFound: true,
		},
		{
			// A 404 whose body yields no code deserializes to the status text
			// as the code. Viperblock never calls HeadObject, so this — not the
			// modeled NotFound type — is how a bodyless 404 reaches ReadCtx.
			name:         "bodyless_404_wraps_not_exist",
			in:           &smithy.GenericAPIError{Code: "NotFound", Message: "404"},
			wantNotFound: true,
		},
		{
			// No typed error exists for NoSuchVersion, so it can only match via
			// the API error code switch.
			name:         "no_such_version_wraps_not_exist",
			in:           &smithy.GenericAPIError{Code: "NoSuchVersion", Message: "version missing"},
			wantNotFound: true,
		},
		{
			name:         "internal_error_stays_transient",
			in:           &smithy.GenericAPIError{Code: "InternalError", Message: "5xx"},
			wantNotFound: false,
		},
		{
			name:         "raw_network_error_stays_transient",
			in:           fmt.Errorf("connection refused"),
			wantNotFound: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := wrapNotFound(tc.in)
			if tc.in == nil {
				assert.NoError(t, got)
				return
			}
			assert.Error(t, got)
			if tc.wantNotFound {
				assert.True(t, errors.Is(got, os.ErrNotExist),
					"want os.ErrNotExist for %v, got %v", tc.in, got)
			} else {
				assert.False(t, errors.Is(got, os.ErrNotExist),
					"unexpected os.ErrNotExist for %v", tc.in)
			}
		})
	}
}
