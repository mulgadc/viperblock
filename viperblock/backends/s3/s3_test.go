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
			name:         "no_such_bucket_typed_wraps_not_exist",
			in:           &s3types.NoSuchBucket{Message: aws.String("bucket missing")},
			wantNotFound: true,
		},
		{
			name:         "head_object_404_wraps_not_exist",
			in:           &s3types.NotFound{Message: aws.String("404")},
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
