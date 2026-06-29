// Copyright 2026 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package s3

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
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
			in:           awserr.New(awss3.ErrCodeNoSuchKey, "key missing", nil),
			wantNotFound: true,
		},
		{
			name:         "no_such_bucket_wraps_not_exist",
			in:           awserr.New(awss3.ErrCodeNoSuchBucket, "bucket missing", nil),
			wantNotFound: true,
		},
		{
			name:         "head_object_404_wraps_not_exist",
			in:           awserr.New("NotFound", "404", nil),
			wantNotFound: true,
		},
		{
			name:         "internal_error_stays_transient",
			in:           awserr.New("InternalError", "5xx", nil),
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
