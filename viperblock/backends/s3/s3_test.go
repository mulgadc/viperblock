package s3

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newResponseError builds a *smithyhttp.ResponseError carrying the given HTTP
// status code, the same concrete type the aws-sdk-go-v2 wraps every non-2xx
// PutObject response in — this is what errors.As in classifyWriteErr unwraps
// to.
func newResponseError(statusCode int) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: statusCode},
		},
		Err: &smithy.GenericAPIError{Code: "SomeError", Message: "backend rejected the request"},
	}
}

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
				assert.ErrorIs(t, got, os.ErrNotExist,
					"want os.ErrNotExist for %v, got %v", tc.in, got)
			} else {
				assert.NotErrorIs(t, got, os.ErrNotExist,
					"unexpected os.ErrNotExist for %v", tc.in)
			}
		})
	}
}

// TestClassifyWriteErr pins the backend-full detection PutObject errors flow
// through: predastore signals "filesystem nearly full" as HTTP 507
// (Insufficient Storage), and 503 (Service Unavailable) can carry the same
// meaning from a fronting proxy — both must classify as types.ErrNoSpace so
// viperblock's drain path can latch backendFull and fail guest writes fast.
// Any other status, or an error with no HTTP response attached at all, must
// pass through unchanged so viperblock does not misclassify an unrelated
// failure (e.g. a transient 500) as out-of-space and refuse writes that
// would otherwise have succeeded on retry.
func TestClassifyWriteErr(t *testing.T) {
	cases := []struct {
		name        string
		in          error
		wantNoSpace bool
	}{
		{
			name:        "nil_passes_through",
			in:          nil,
			wantNoSpace: false,
		},
		{
			name:        "507_insufficient_storage_maps_to_no_space",
			in:          newResponseError(http.StatusInsufficientStorage),
			wantNoSpace: true,
		},
		{
			name:        "503_service_unavailable_maps_to_no_space",
			in:          newResponseError(http.StatusServiceUnavailable),
			wantNoSpace: true,
		},
		{
			name:        "500_internal_error_stays_unclassified",
			in:          newResponseError(http.StatusInternalServerError),
			wantNoSpace: false,
		},
		{
			name:        "404_not_found_stays_unclassified",
			in:          newResponseError(http.StatusNotFound),
			wantNoSpace: false,
		},
		{
			// A generic API error with no HTTP response attached (e.g. a
			// modeled SDK error constructed without going through the HTTP
			// transport) must not accidentally satisfy errors.As for
			// *smithyhttp.ResponseError.
			name:        "generic_api_error_without_response_stays_unclassified",
			in:          &smithy.GenericAPIError{Code: "InternalError", Message: "5xx"},
			wantNoSpace: false,
		},
		{
			name:        "raw_network_error_stays_unclassified",
			in:          fmt.Errorf("connection refused"),
			wantNoSpace: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyWriteErr(tc.in)
			if tc.in == nil {
				assert.NoError(t, got)
				return
			}
			assert.Error(t, got)
			if tc.wantNoSpace {
				assert.ErrorIs(t, got, types.ErrNoSpace,
					"want types.ErrNoSpace for %v, got %v", tc.in, got)
			} else {
				assert.NotErrorIs(t, got, types.ErrNoSpace,
					"unexpected types.ErrNoSpace for %v", tc.in)
			}
		})
	}
}

// TestClassifyWriteErrNilIsNoOp guards the trivial nil case explicitly:
// callers pass classifyWriteErr's result straight to log/return sites, so a
// nil input must yield a nil, not a wrapped "nil: nil"-shaped error.
func TestClassifyWriteErrNilIsNoOp(t *testing.T) {
	assert.NoError(t, classifyWriteErr(nil))
}

// TestClassifyWriteErrPreservesUnderlyingError ensures a classified error
// still unwraps to the original SDK error, so log lines and any future
// caller that inspects the specific AWS error code (not just ErrNoSpace)
// keep working.
func TestClassifyWriteErrPreservesUnderlyingError(t *testing.T) {
	original := newResponseError(http.StatusInsufficientStorage)
	got := classifyWriteErr(original)

	assert.ErrorIs(t, got, types.ErrNoSpace)

	var respErr *smithyhttp.ResponseError
	if assert.ErrorAs(t, got, &respErr, "classified error should still unwrap to the original *smithyhttp.ResponseError") {
		assert.Equal(t, http.StatusInsufficientStorage, respErr.HTTPStatusCode())
	}
}

// ctxWithOperationName returns a context carrying operationName the same way
// the AWS SDK's own RegisterServiceMetadata initialize-step middleware sets
// it for a real request, so newPoolPressureMiddleware's
// awsmiddleware.GetOperationName(ctx) check can be exercised without
// standing up a full S3 client.
func ctxWithOperationName(t *testing.T, operationName string) context.Context {
	t.Helper()
	rsm := awsmiddleware.RegisterServiceMetadata{OperationName: operationName}
	var got context.Context
	_, _, err := rsm.HandleInitialize(context.Background(), middleware.InitializeInput{}, middleware.InitializeHandlerFunc(
		func(ctx context.Context, in middleware.InitializeInput) (middleware.InitializeOutput, middleware.Metadata, error) {
			got = ctx //nolint:fatcontext // test-only: capturing ctx past the closure is the point, we assert on it below
			return middleware.InitializeOutput{}, middleware.Metadata{}, nil
		}))
	require.NoError(t, err)
	return got
}

// fakeDeserializeHandler returns a fixed DeserializeOutput/error from
// HandleDeserialize, standing in for "the rest of the SDK's deserialize
// chain" so newPoolPressureMiddleware can be exercised as the one link that
// wraps it.
type fakeDeserializeHandler struct {
	out middleware.DeserializeOutput
	err error
}

func (f fakeDeserializeHandler) HandleDeserialize(_ context.Context, _ middleware.DeserializeInput) (middleware.DeserializeOutput, middleware.Metadata, error) {
	return f.out, middleware.Metadata{}, f.err
}

// responseWithPressureHeader builds a *smithyhttp.Response carrying
// X-Predastore-Pool-Pressure set to headerValue, or no header at all when
// headerValue is empty -- the two shapes newPoolPressureMiddleware must tell
// apart.
func responseWithPressureHeader(headerValue string) *smithyhttp.Response {
	header := http.Header{}
	if headerValue != "" {
		header.Set(poolPressureHeader, headerValue)
	}
	return &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusOK, Header: header}}
}

// TestPoolPressureMiddlewareSetsNearFullOnHeader pins the wire contract:
// X-Predastore-Pool-Pressure: nearfull on a PutObject response must set
// NearFull(), which is what feeds viperblock's early backpressure gate.
func TestPoolPressureMiddlewareSetsNearFullOnHeader(t *testing.T) {
	backend := New(S3Config{VolumeName: "vol", Bucket: "bucket"})
	mw := backend.newPoolPressureMiddleware()

	ctx := ctxWithOperationName(t, putObjectOperationName)
	_, _, err := mw.HandleDeserialize(ctx, middleware.DeserializeInput{}, fakeDeserializeHandler{
		out: middleware.DeserializeOutput{RawResponse: responseWithPressureHeader(poolPressureNearFull)},
	})
	require.NoError(t, err)
	assert.True(t, backend.NearFull(), "nearfull header on a PutObject response must set NearFull")
}

// TestPoolPressureMiddlewareClearsNearFullWhenHeaderAbsent proves the
// self-clearing behavior the drain loop depends on: a later PutObject
// response with no pool-pressure header (the pool dropped back under
// nearfull) must flip NearFull() back to false.
func TestPoolPressureMiddlewareClearsNearFullWhenHeaderAbsent(t *testing.T) {
	backend := New(S3Config{VolumeName: "vol", Bucket: "bucket"})
	backend.backendNearFull.Store(true) // simulate a prior nearfull observation

	mw := backend.newPoolPressureMiddleware()
	ctx := ctxWithOperationName(t, putObjectOperationName)
	_, _, err := mw.HandleDeserialize(ctx, middleware.DeserializeInput{}, fakeDeserializeHandler{
		out: middleware.DeserializeOutput{RawResponse: responseWithPressureHeader("")},
	})
	require.NoError(t, err)
	assert.False(t, backend.NearFull(), "an absent header must clear a previously observed nearfull")
}

// TestPoolPressureMiddlewareIgnoresNonPutObjectOperations proves the
// operation-name scoping: a GET/List response must never touch the flag, so
// a concurrent read cannot clobber what a PutObject just observed.
func TestPoolPressureMiddlewareIgnoresNonPutObjectOperations(t *testing.T) {
	backend := New(S3Config{VolumeName: "vol", Bucket: "bucket"})
	backend.backendNearFull.Store(true)

	mw := backend.newPoolPressureMiddleware()
	ctx := ctxWithOperationName(t, "GetObject")
	_, _, err := mw.HandleDeserialize(ctx, middleware.DeserializeInput{}, fakeDeserializeHandler{
		// Even if a GetObject response somehow carried the header, it must
		// be ignored -- only PutObject responses carry pool-pressure meaning.
		out: middleware.DeserializeOutput{RawResponse: responseWithPressureHeader("")},
	})
	require.NoError(t, err)
	assert.True(t, backend.NearFull(), "a non-PutObject response must not modify NearFull")
}

// TestPoolPressureMiddlewareClearsOnFailedPutObject proves the FULL-path
// interaction: a PutObject that fails with 507 (no pool-pressure header,
// since predastore only sets it on 2xx) must still resolve to "not
// nearfull" and must preserve the underlying error unchanged, so
// classifyWriteErr upstream still sees and classifies it.
func TestPoolPressureMiddlewareClearsOnFailedPutObject(t *testing.T) {
	backend := New(S3Config{VolumeName: "vol", Bucket: "bucket"})
	backend.backendNearFull.Store(true)

	mw := backend.newPoolPressureMiddleware()
	ctx := ctxWithOperationName(t, putObjectOperationName)
	wantErr := newResponseError(http.StatusInsufficientStorage)
	_, _, err := mw.HandleDeserialize(ctx, middleware.DeserializeInput{}, fakeDeserializeHandler{
		out: middleware.DeserializeOutput{RawResponse: responseWithPressureHeader("")},
		err: wantErr,
	})
	assert.Same(t, wantErr, err, "the middleware must pass the underlying error through unchanged")
	assert.False(t, backend.NearFull(), "a failed PutObject with no pool-pressure header must clear NearFull")
}
