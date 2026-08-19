package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awsretry "github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/mulgadc/viperblock/telemetry"
	"github.com/mulgadc/viperblock/types"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

// poolPressureHeader is the response header predastore sets on a successful
// PutObject once its storage pool nears FULL (FULL itself is instead
// signalled via HTTP 507, see classifyWriteErr).
const poolPressureHeader = "X-Predastore-Pool-Pressure"

// poolPressureNearFull is the only header value this backend acts on.
const poolPressureNearFull = "nearfull"

// putObjectOperationName scopes the pool-pressure observer to PutObject
// responses only, via awsmiddleware.GetOperationName.
const putObjectOperationName = "PutObject"

// schemeRE matches a leading URI scheme.
var schemeRE = regexp.MustCompile("^[^:]+://")

// normalizeEndpoint returns host with an https scheme when it carries none.
// Callers pass Host as a bare "host:port" as often as a full URL, and the SDK
// requires the endpoint to be a valid URI — it fails endpoint resolution
// outright on a schemeless value rather than assuming one.
func normalizeEndpoint(host string) string {
	if schemeRE.MatchString(host) {
		return host
	}
	return "https://" + host
}

// wrapNotFound returns err wrapped with os.ErrNotExist when the AWS error
// indicates the requested object is genuinely absent (NoSuchKey, 404 NotFound,
// NoSuchBucket). Callers can detect "missing" vs "transient" via
// errors.Is(err, os.ErrNotExist) without taking an AWS SDK dependency.
func wrapNotFound(err error) error {
	if err == nil {
		return nil
	}

	// Match on the wire code rather than the modeled types. GetObject models
	// only NoSuchKey, so a missing bucket arrives as a generic API error, and
	// NoSuchVersion and a bodyless 404 have no modeled type at all. The modeled
	// types report these same codes via ErrorCode(), so matching the code
	// covers both shapes.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NoSuchBucket", "NotFound", "NoSuchVersion":
			return fmt.Errorf("%w: %w", os.ErrNotExist, err)
		}
	}
	return err
}

// isNonRetryableBackendError identifies failures that cannot recover without
// changing the request or DNS configuration. Throttling remains retryable.
func isNonRetryableBackendError(err error) bool {
	var statusErr interface{ HTTPStatusCode() int }
	if errors.As(err, &statusErr) {
		status := statusErr.HTTPStatusCode()
		return status >= http.StatusBadRequest && status < http.StatusInternalServerError && status != http.StatusTooManyRequests
	}

	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr) && dnsErr.IsNotFound
}

// newRetryer preserves the SDK retry policy while overriding deterministic
// client and DNS failures before the default connection checks see them.
func newRetryer() aws.Retryer {
	return awsretry.NewStandard(func(options *awsretry.StandardOptions) {
		classifier := awsretry.IsErrorRetryableFunc(func(err error) aws.Ternary {
			var statusErr interface{ HTTPStatusCode() int }
			if errors.As(err, &statusErr) && statusErr.HTTPStatusCode() == http.StatusTooManyRequests {
				return aws.TrueTernary
			}
			if isNonRetryableBackendError(err) {
				return aws.FalseTernary
			}
			return aws.UnknownTernary
		})
		options.Retryables = append([]awsretry.IsErrorRetryable{classifier}, options.Retryables...)
	})
}

// classifyReadErr exposes deterministic failures to Viperblock's outer retry
// loops while preserving the established object-not-found contract.
func classifyReadErr(err error) error {
	err = wrapNotFound(err)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return err
	}
	if isNonRetryableBackendError(err) {
		return fmt.Errorf("%w: %w", types.ErrBackendNonRetryable, err)
	}
	return err
}

// classifyWriteErr maps a PutObject error into types.ErrNoSpace ONLY when the
// HTTP status is 507 (Insufficient Storage) -- predastore's single signal that
// the store is genuinely full. Every other error, including 503, passes
// through unchanged.
//
// 503 must NOT be treated as out-of-space: predastore returns 503 SlowDown
// from its PutObject rate limiter, which is transient backpressure ("retry
// with backoff"), not a full store. Mapping it to ErrNoSpace latched the
// backendFull flag, and since every retry re-tripped the rate limit the latch
// never cleared -- wedging the volume permanently under sustained churn. Left
// as an ordinary error, a persistent 503 surfaces as a failed drain the
// uploader retries on its next tick while write backpressure throttles the
// guest, so the volume self-throttles instead of failing.
func classifyWriteErr(err error) error {
	if err == nil {
		return nil
	}

	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		if respErr.HTTPStatusCode() == http.StatusInsufficientStorage {
			return fmt.Errorf("%w: %w", types.ErrNoSpace, err)
		}
	}

	return err
}

// 2. Define config structs.
type S3Config struct {
	VolumeName string
	VolumeSize uint64

	Region    string
	Bucket    string
	AccessKey string
	SecretKey string

	Host string

	// s3Client is set by InitCtx and read by this backend's own methods. It is
	// unexported to keep the SDK type out of viperblock's public API.
	s3Client   *s3.Client
	HTTPClient *http.Client // Optional: override the default HTTP client (e.g. for tests)
}

type S3Backend struct {
	config S3Config
	log    *slog.Logger

	// backendNearFull mirrors the last observed X-Predastore-Pool-Pressure
	// header from a PutObject response. Updated by the deserialize middleware
	// registered in InitCtx; read via NearFull().
	backendNearFull atomic.Bool
}

type Backend struct {
	S3Backend

	Config S3Config
}

var _ types.Backend = (*Backend)(nil)

func New(config any) (backend *Backend, err error) {
	cfg, ok := config.(S3Config)
	if !ok {
		return nil, fmt.Errorf("%w: s3 backend expected S3Config, got %T", types.ErrBackendConfig, config)
	}
	return &Backend{S3Backend: S3Backend{config: cfg, log: slog.Default()}}, nil
}

// SetLogger installs the logger this backend uses for its own log lines.
// Never calls slog.SetDefault; nil falls back to slog.Default().
func (backend *Backend) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	backend.log = logger
}

// NearFull reports whether the most recently observed PutObject response
// carried X-Predastore-Pool-Pressure: nearfull, so callers can back off
// before the backend hits FULL. False until a PutObject has been observed.
func (backend *Backend) NearFull() bool {
	return backend.backendNearFull.Load()
}

// newPoolPressureMiddleware returns a deserialize-step middleware that records
// whether each PutObject response carries X-Predastore-Pool-Pressure: nearfull
// into backend.backendNearFull. Split out so tests can exercise it directly.
func (backend *Backend) newPoolPressureMiddleware() middleware.DeserializeMiddleware {
	return middleware.DeserializeMiddlewareFunc("PoolPressureObserver",
		func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (
			middleware.DeserializeOutput, middleware.Metadata, error,
		) {
			out, metadata, err := next.HandleDeserialize(ctx, in)

			// Scope to PutObject only: GET/List responses carry no pool-pressure
			// semantics and must not clobber the flag a concurrent PutObject set.
			if awsmiddleware.GetOperationName(ctx) != putObjectOperationName {
				return out, metadata, err
			}

			// The header is only set on a 2xx response; a failed PutObject
			// (including the 507/503 FULL path) resolves to "not nearfull" here.
			nearFull := false
			if resp, ok := out.RawResponse.(*smithyhttp.Response); ok && resp != nil {
				nearFull = resp.Header.Get(poolPressureHeader) == poolPressureNearFull
			}
			backend.backendNearFull.Store(nearFull)

			return out, metadata, err
		})
}

func (backend *Backend) Init() error {
	return backend.InitCtx(context.Background())
}

func (backend *Backend) InitCtx(ctx context.Context) error {
	if err := backend.InitReadOnlyCtx(ctx); err != nil {
		return err
	}

	// Reachability probe: prove the bucket exists, the credentials sign and
	// the endpoint answers. Scoped to this volume because an unscoped list is
	// O(bucket) against predastore, which resolves object metadata per key and
	// truncates only the reported count. An empty result is still a success.
	_, err := backend.config.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(backend.config.Bucket),
		Prefix:  aws.String(backend.config.VolumeName + "/"),
		MaxKeys: aws.Int32(1),
	})

	if err != nil {
		backend.log.ErrorContext(ctx, "Error listing objects", "error", err)
		return err
	}

	return nil
}

// http2Env opts the S3 transport back into HTTP/2 when set to "1". The data
// path runs inside an nbdkit process that has no config file of its own, so an
// environment variable is the only way to flip this without a rebuild.
const http2Env = "VB_S3_HTTP2"

// transferBufferSize sizes the transport's socket buffers. The stdlib default
// is 4 KiB, which splits a 4 MiB chunk body across a thousand syscalls.
const transferBufferSize = 256 << 10

// NewHTTPClient builds the HTTP client the S3 backend uses when S3Config
// leaves one unset. A caller that opens many volumes against one endpoint
// should build one of these and share it through S3Config.HTTPClient: each
// client keeps its own connection pool, so a client per volume turns into a
// TLS handshake per volume.
func NewHTTPClient() *http.Client {
	return newHTTPClient(http2Enabled())
}

// http2Enabled reports whether the deployment has opted back into HTTP/2.
func http2Enabled() bool {
	return os.Getenv(http2Env) == "1"
}

// newHTTPClient wraps the transport for tracing. The otelhttp round tripper it
// returns does not expose the transport underneath, so anything asserting on
// the transport itself builds one with newS3Transport.
func newHTTPClient(http2 bool) *http.Client {
	return &http.Client{
		// otelhttp emits a client span per S3 request, but only when the
		// request context already carries a span: background chunk I/O and
		// guest block reads would otherwise root a trace per S3 call.
		Transport: otelhttp.NewTransport(newS3Transport(http2), otelhttp.WithFilter(func(r *http.Request) bool {
			return trace.SpanFromContext(r.Context()).SpanContext().IsValid()
		})),
		Timeout: 120 * time.Second,
	}
}

// newS3Transport builds the transport with HTTP/2 explicitly on or off, so a
// test can exercise both without touching the environment.
func newS3Transport(http2 bool) *http.Transport {
	// HTTP/1.1 with a wide pool, not HTTP/2. The engine issues many small
	// ranged GETs concurrently with 4 MiB chunk PUTs, and h2 funnels all of
	// them onto one connection: a single chunk body exceeds the connection's
	// flow-control window and every GET sharing it waits. Pooling gives each
	// in-flight request its own socket, which is what this workload wants.
	alpn := []string{"http/1.1"}
	if http2 {
		alpn = []string{"h2", "http/1.1"}
	}

	// Protocols governs the transport's own wiring; NextProtos governs what
	// ALPN offers. Both derive from the same flag so they cannot disagree.
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(http2)

	tr := &http.Transport{
		Protocols: &protocols,
		TLSClientConfig: &tls.Config{
			// Enable TLS session resumption for faster reconnects
			ClientSessionCache: tls.NewLRUClientSessionCache(256),
			NextProtos:         alpn,
		},

		// Explicitly nil: the endpoint is reached directly, and a proxy
		// inherited from the environment would capture it.
		Proxy: nil,

		// The pool is what provides concurrency without h2 multiplexing, so it
		// must stay at least as wide as the caller's in-flight request count.
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     120 * time.Second,

		// Keep-alive settings
		DisableKeepAlives: false,

		WriteBufferSize: transferBufferSize,
		ReadBufferSize:  transferBufferSize,

		// Timeouts
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return tr
}

// InitReadOnlyCtx builds the client without the reachability probe. A caller
// that only reads state does not need it: the read that follows fails on an
// unreachable or unauthorised backend just as the probe would, so paying for
// a list as well is a round trip spent to learn nothing.
func (backend *Backend) InitReadOnlyCtx(ctx context.Context) error {
	// Log only the fields that identify the backend. S3Config carries the
	// static credentials, so logging the struct wholesale would write the
	// secret key in plaintext to wherever the embedder's logger points.
	backend.log.InfoContext(ctx, "Initializing S3 backend",
		"volumeName", backend.config.VolumeName,
		"bucket", backend.config.Bucket,
		"region", backend.config.Region,
		"host", backend.config.Host,
	)

	client := backend.config.HTTPClient
	if client == nil {
		client = NewHTTPClient()
	}

	// Use the AWS SDK to initialize the S3 backend.
	//
	// ContinueHeaderThresholdBytes: the SDK adds "Expect: 100-continue" to PUTs
	// at or above this threshold, defaulting to 2 MiB when left zero
	// (service/internal/s3shared/s3100continue.go). Chunk writes are 4 MiB, so
	// every chunk PUT would qualify. -1 skips the header entirely, for two
	// reasons. Under HTTP/2 Go's server strips it before handlers see it
	// (x/net/http2 server behavior) and canonicalizes "expect:" as empty, while
	// the signer includes "expect" in SignedHeaders signed with value
	// "100-continue" — a signature mismatch surfacing as an unretried 403. Under
	// HTTP/1.1, which is now the default, the header is honoured and costs a
	// round trip ahead of every chunk body to guard against a rejection this
	// backend does not expect.
	backend.config.s3Client = s3.New(s3.Options{
		BaseEndpoint:                 aws.String(normalizeEndpoint(backend.config.Host)),
		UsePathStyle:                 true,
		ContinueHeaderThresholdBytes: -1,
		Region:                       backend.config.Region,
		HTTPClient:                   client,
		Credentials:                  credentials.NewStaticCredentialsProvider(backend.config.AccessKey, backend.config.SecretKey, ""),
		Retryer:                      newRetryer(),
		// Registers the pool-pressure observer after the SDK's own deserialize
		// middleware, so RawResponse is already populated when it runs.
		APIOptions: []func(*middleware.Stack) error{
			func(stack *middleware.Stack) error {
				return stack.Deserialize.Add(backend.newPoolPressureMiddleware(), middleware.After)
			},
		},
	})

	return nil
}

func (backend *Backend) Open(fname string) error {
	return nil
}

func (backend *Backend) Read(fileType types.FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error) {
	return backend.ReadCtx(context.Background(), fileType, objectId, offset, length)
}

func (backend *Backend) ReadCtx(ctx context.Context, fileType types.FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error) {
	backend.log.DebugContext(ctx, "[S3 READ] Reading object", "objectId", objectId, "offset", offset, "length", length)
	start := time.Now()
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		telemetry.RecordBackendIO(ctx, "read", "s3", backend.config.VolumeName, outcome, len(data), time.Since(start))
	}()

	if backend.config.s3Client == nil {
		return nil, fmt.Errorf("S3 client not initialized")
	}

	filename := types.GetFilePath(fileType, objectId, backend.config.VolumeName)

	// Fetch the object from S3 with a byte range
	requestObject := &s3.GetObjectInput{
		Bucket: aws.String(backend.config.Bucket),
		Key:    aws.String(filename),
	}

	// Use Range header for partial reads, but skip for full file reads (length=0)
	// When length=0, read the entire file (used for config.json and other metadata)
	if length > 0 {
		// Request exactly the bytes we need: offset to offset+length-1
		requestObject.Range = aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
		backend.log.DebugContext(ctx, "[S3 READ] Requesting range", "range", *requestObject.Range)
	} else {
		backend.log.DebugContext(ctx, "[S3 READ] Reading entire file", "key", filename)
	}

	textResult, err := backend.config.s3Client.GetObject(ctx, requestObject)

	if err != nil {
		return nil, classifyReadErr(err)
	}
	defer textResult.Body.Close()

	res, err := io.ReadAll(textResult.Body)

	if err != nil {
		return nil, err
	}

	// A ranged GET whose range starts inside the object but runs past its end
	// is answered with a CLAMPED 206 -- a short body with a matching
	// Content-Length, so io.ReadAll returns it without error. Callers copy the
	// result into a full-size, zero-initialised buffer, so an unchecked short
	// body becomes a silently zero-filled tail that is then cached as valid.
	// Verified against predastore: asking for 1024 bytes past EOF returns
	// exactly the available bytes with no error. Refuse it here instead.
	if length > 0 && len(res) != int(length) {
		return nil, fmt.Errorf("%w: %s offset %d: backend returned %d bytes, expected %d",
			types.ErrShortRead, filename, offset, len(res), length)
	}

	// A full-object read has no range to check against, so a body truncated
	// mid-transfer reads back clean. Compare against the response's own
	// Content-Length; a chunked response has none and promises nothing.
	if length == 0 && textResult.ContentLength != nil && *textResult.ContentLength >= 0 &&
		len(res) != int(*textResult.ContentLength) {
		return nil, fmt.Errorf("%w: %s: backend returned %d bytes, expected %d",
			types.ErrShortRead, filename, len(res), *textResult.ContentLength)
	}

	return res, nil
}

func (backend *Backend) Write(fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) (err error) {
	return backend.WriteCtx(context.Background(), fileType, objectId, headers, data)
}

func (backend *Backend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) (err error) {
	start := time.Now()
	bodyLen := 0
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		telemetry.RecordBackendIO(ctx, "write", "s3", backend.config.VolumeName, outcome, bodyLen, time.Since(start))
	}()

	if backend.config.s3Client == nil {
		return fmt.Errorf("S3 client not initialized")
	}

	filename := types.GetFilePath(fileType, objectId, backend.config.VolumeName)

	// Combine headers and data to match file backend behavior
	// The BlockLookup offsets include header size, so we must write headers+data
	var body []byte
	if headers != nil && len(*headers) > 0 {
		dataLen := 0
		if data != nil {
			dataLen = len(*data)
		}
		body = make([]byte, len(*headers)+dataLen)
		copy(body[:len(*headers)], *headers)
		if data != nil {
			copy(body[len(*headers):], *data)
		}
	} else if data != nil {
		body = *data
	}
	bodyLen = len(body)

	// Create a new S3 object
	object := &s3.PutObjectInput{
		Bucket: aws.String(backend.config.Bucket),
		Key:    aws.String(filename),
		Body:   bytes.NewReader(body),
	}

	_, err = backend.config.s3Client.PutObject(ctx, object)

	if err != nil {
		err = classifyWriteErr(err)
		backend.log.ErrorContext(ctx, "Error writing object", "error", err)
		return err
	}

	return nil
}

func (backend *Backend) ReadFrom(volumeName string, fileType types.FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error) {
	return backend.ReadFromCtx(context.Background(), volumeName, fileType, objectId, offset, length)
}

func (backend *Backend) ReadFromCtx(ctx context.Context, volumeName string, fileType types.FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error) {
	backend.log.DebugContext(ctx, "[S3 READFROM] Reading object", "volumeName", volumeName, "objectId", objectId, "offset", offset, "length", length)

	if backend.config.s3Client == nil {
		return nil, fmt.Errorf("S3 client not initialized")
	}

	filename := types.GetFilePath(fileType, objectId, volumeName)

	requestObject := &s3.GetObjectInput{
		Bucket: aws.String(backend.config.Bucket),
		Key:    aws.String(filename),
	}

	if length > 0 {
		requestObject.Range = aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	}

	textResult, err := backend.config.s3Client.GetObject(ctx, requestObject)
	if err != nil {
		return nil, classifyReadErr(err)
	}
	defer textResult.Body.Close()

	res, err := io.ReadAll(textResult.Body)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (backend *Backend) WriteTo(volumeName string, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) (err error) {
	return backend.WriteToCtx(context.Background(), volumeName, fileType, objectId, headers, data)
}

func (backend *Backend) WriteToCtx(ctx context.Context, volumeName string, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) (err error) {
	if backend.config.s3Client == nil {
		return fmt.Errorf("S3 client not initialized")
	}

	filename := types.GetFilePath(fileType, objectId, volumeName)

	var body []byte
	if headers != nil && len(*headers) > 0 {
		dataLen := 0
		if data != nil {
			dataLen = len(*data)
		}
		body = make([]byte, len(*headers)+dataLen)
		copy(body[:len(*headers)], *headers)
		if data != nil {
			copy(body[len(*headers):], *data)
		}
	} else if data != nil {
		body = *data
	}

	object := &s3.PutObjectInput{
		Bucket: aws.String(backend.config.Bucket),
		Key:    aws.String(filename),
		Body:   bytes.NewReader(body),
	}

	_, err = backend.config.s3Client.PutObject(ctx, object)
	if err != nil {
		err = classifyWriteErr(err)
		backend.log.ErrorContext(ctx, "Error writing object", "error", err)
		return err
	}

	return nil
}

func (backend *Backend) Delete(fileType types.FileType, objectId uint64) (err error) {
	return backend.DeleteCtx(context.Background(), fileType, objectId)
}

// DeleteCtx removes the object identified by fileType/objectId from this
// backend's own volume. wrapNotFound is required here because predastore,
// unlike real S3, errors on deleting an already-missing key.
func (backend *Backend) DeleteCtx(ctx context.Context, fileType types.FileType, objectId uint64) (err error) {
	start := time.Now()
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		telemetry.RecordBackendIO(ctx, "delete", "s3", backend.config.VolumeName, outcome, 0, time.Since(start))
	}()

	if backend.config.s3Client == nil {
		return fmt.Errorf("S3 client not initialized")
	}

	filename := types.GetFilePath(fileType, objectId, backend.config.VolumeName)

	_, err = backend.config.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(backend.config.Bucket),
		Key:    aws.String(filename),
	})
	if err != nil {
		return wrapNotFound(err)
	}
	return nil
}

func (backend *Backend) ListPrefixes(prefix string) (names []string, err error) {
	return backend.ListPrefixesCtx(context.Background(), prefix)
}

// ListPrefixesCtx returns the top-level "directory" names under prefix,
// paginating through all results. Bucket-wide, not scoped to this backend's
// own VolumeName.
func (backend *Backend) ListPrefixesCtx(ctx context.Context, prefix string) (names []string, err error) {
	if backend.config.s3Client == nil {
		return nil, fmt.Errorf("S3 client not initialized")
	}

	var continuationToken *string
	for {
		out, listErr := backend.config.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(backend.config.Bucket),
			Prefix:            aws.String(prefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: continuationToken,
		})
		if listErr != nil {
			return nil, listErr
		}

		for _, cp := range out.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			names = append(names, strings.TrimSuffix(*cp.Prefix, "/"))
		}

		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	return names, nil
}

func (backend *Backend) ListObjects(prefix string) (keys []string, err error) {
	return backend.ListObjectsCtx(context.Background(), prefix)
}

// ListObjectsCtx returns every object's full key under prefix, recursively
// (no Delimiter), paginating through every page of results.
func (backend *Backend) ListObjectsCtx(ctx context.Context, prefix string) (keys []string, err error) {
	if backend.config.s3Client == nil {
		return nil, fmt.Errorf("S3 client not initialized")
	}

	var continuationToken *string
	for {
		out, listErr := backend.config.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(backend.config.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if listErr != nil {
			return nil, listErr
		}

		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			keys = append(keys, *obj.Key)
		}

		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	return keys, nil
}

func (backend *Backend) Sync() {
}

func (backend *Backend) GetBackendType() string {
	return "s3"
}

func (backend *Backend) SetConfig(config any) error {
	cfg, ok := config.(S3Config)
	if !ok {
		return fmt.Errorf("%w: s3 backend expected S3Config, got %T", types.ErrBackendConfig, config)
	}
	backend.config = cfg
	return nil
}

func (backend *Backend) GetHost() string {
	return backend.config.Host
}
