package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/mulgadc/viperblock/telemetry"
	"github.com/mulgadc/viperblock/types"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

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
}

type Backend struct {
	S3Backend

	Config S3Config
}

var _ types.Backend = (*Backend)(nil)

func New(config any) (backend *Backend) {
	cfg, ok := config.(S3Config)
	if !ok {
		panic("s3 backend: expected S3Config")
	}
	return &Backend{S3Backend: S3Backend{config: cfg, log: slog.Default()}}
}

// SetLogger installs the logger this backend uses for its own log lines.
// Never calls slog.SetDefault; nil falls back to slog.Default().
func (backend *Backend) SetLogger(logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	backend.log = logger
}

func (backend *Backend) Init() error {
	return backend.InitCtx(context.Background())
}

func (backend *Backend) InitCtx(ctx context.Context) error {
	backend.log.InfoContext(ctx, "Initializing S3 backend", "config", backend.config)

	client := backend.config.HTTPClient
	if client == nil {
		// HTTP/2 multiplexes requests over a single TCP connection, avoiding a
		// TLS handshake per request. ForceAttemptHTTP2 enables stdlib ALPN h2
		// negotiation against an h2-capable server
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				// Enable TLS session resumption for faster reconnects if HTTP/2 fails
				ClientSessionCache: tls.NewLRUClientSessionCache(256),
				// Ensure HTTP/2 ALPN is advertised
				NextProtos: []string{"h2", "http/1.1"},
			},

			// Connection pool settings - still useful as HTTP/2 fallback
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
			MaxConnsPerHost:     0,
			IdleConnTimeout:     120 * time.Second,

			// Keep-alive settings
			DisableKeepAlives: false,
			ForceAttemptHTTP2: true,

			// Timeouts
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 60 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}

		client = &http.Client{
			// otelhttp emits a client span per S3 request, but only when the
			// request context already carries a span: background chunk I/O and
			// guest block reads would otherwise root a trace per S3 call.
			Transport: otelhttp.NewTransport(tr, otelhttp.WithFilter(func(r *http.Request) bool {
				return trace.SpanFromContext(r.Context()).SpanContext().IsValid()
			})),
			Timeout: 120 * time.Second,
		}
	}

	// Use the AWS SDK to initialize the S3 backend.
	//
	// ContinueHeaderThresholdBytes: the SDK adds "Expect: 100-continue" to PUTs
	// at or above this threshold, defaulting to 2 MiB when left zero
	// (service/internal/s3shared/s3100continue.go). Chunk writes are 4 MiB, so
	// every chunk PUT would qualify. Under HTTP/2 Go's server strips the Expect
	// header before handlers see it (x/net/http2 server behavior) and
	// canonicalizes "expect:" as empty, while the signer includes "expect" in
	// SignedHeaders signed with value "100-continue" — a signature mismatch that
	// surfaces as an AccessDenied 403 that is not retried. -1 skips the header
	// entirely; it's a no-op under HTTP/2 anyway.
	backend.config.s3Client = s3.New(s3.Options{
		BaseEndpoint:                 aws.String(normalizeEndpoint(backend.config.Host)),
		UsePathStyle:                 true,
		ContinueHeaderThresholdBytes: -1,
		Region:                       backend.config.Region,
		HTTPClient:                   client,
		Credentials:                  credentials.NewStaticCredentialsProvider(backend.config.AccessKey, backend.config.SecretKey, ""),
	})

	_, err := backend.config.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(backend.config.Bucket),
	})

	if err != nil {
		backend.log.ErrorContext(ctx, "Error listing objects", "error", err)
		return err
	}

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

	// TODO: Add retry from S3 timeout/500/etc
	textResult, err := backend.config.s3Client.GetObject(ctx, requestObject)

	if err != nil {
		return nil, wrapNotFound(err)
	}
	defer textResult.Body.Close()

	res, err := io.ReadAll(textResult.Body)

	if err != nil {
		return nil, err
	}

	// The response should contain exactly the bytes we requested
	// No slicing needed since we requested the exact range
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
		return nil, wrapNotFound(err)
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
		backend.log.ErrorContext(ctx, "Error writing object", "error", err)
		return err
	}

	return nil
}

func (backend *Backend) Sync() {
}

func (backend *Backend) GetBackendType() string {
	return "s3"
}

func (backend *Backend) SetConfig(config any) {
	cfg, ok := config.(S3Config)
	if !ok {
		panic("s3 backend: expected S3Config")
	}
	backend.config = cfg
}

func (backend *Backend) GetHost() string {
	return backend.config.Host
}
