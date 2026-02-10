// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package s3

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mulgadc/viperblock/types"
	"golang.org/x/net/http2"
)

// 2. Define config structs
type S3Config struct {
	VolumeName string
	VolumeSize uint64

	Region    string
	Bucket    string
	AccessKey string
	SecretKey string

	Host string

	S3Client *s3.S3
}

type S3Backend struct {
	config S3Config
}

type Backend struct {
	S3Backend
	Config S3Config
}

func New(config any) (backend *Backend) {
	return &Backend{S3Backend: S3Backend{config: config.(S3Config)}}
}

func (backend *Backend) Init() error {

	slog.Info("Initializing S3 backend", "config", backend.config)

	// Create HTTP client with HTTP/2 support for connection multiplexing.
	// HTTP/2 allows multiple requests over a single TCP connection, eliminating
	// TLS handshake overhead for each request.
	//
	// IMPORTANT: When using a custom TLSClientConfig, you MUST call
	// http2.ConfigureTransport() to properly enable HTTP/2.
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
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

		// ForceAttemptHTTP2 alone is NOT enough with custom TLSClientConfig!
		// We must also call http2.ConfigureTransport() below.
		ForceAttemptHTTP2: true,

		// Timeouts
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// CRITICAL: Configure HTTP/2 support with custom TLS config
	// Without this, the transport falls back to HTTP/1.1
	if err := http2.ConfigureTransport(tr); err != nil {
		slog.Warn("Failed to configure HTTP/2, falling back to HTTP/1.1", "error", err)
	}

	client := &http.Client{
		Transport: tr,
		Timeout:   120 * time.Second,
	}

	// Use the AWS SDK to initialize the S3 backend
	sess, err := session.NewSession(&aws.Config{
		// Specify the endpoint
		Endpoint:         aws.String(backend.config.Host),
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String(backend.config.Region),
		HTTPClient:       client,
		Credentials:      credentials.NewStaticCredentials(backend.config.AccessKey, backend.config.SecretKey, ""),
	})

	if err != nil {
		slog.Error("Error creating session", "error", err)
		return err
	}

	backend.config.S3Client = s3.New(sess)

	_, err = backend.config.S3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(backend.config.Bucket),
	})

	if err != nil {
		slog.Error("Error listing objects", "error", err)
		return err
	}

	return nil
}

func (backend *Backend) Open(fname string) error {
	return nil
}

func (backend *Backend) Read(fileType types.FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error) {

	slog.Info("[S3 READ] Reading object", "objectId", objectId, "offset", offset, "length", length)

	if backend.config.S3Client == nil {
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
		slog.Debug("[S3 READ] Requesting range", "range", *requestObject.Range)
	} else {
		slog.Debug("[S3 READ] Reading entire file", "key", filename)
	}

	// TODO: Add ctx support and retry from S3 timeout/500/etc
	textResult, err := backend.config.S3Client.GetObject(requestObject)

	if err != nil {
		return nil, err
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

	if backend.config.S3Client == nil {
		return fmt.Errorf("S3 client not initialized")
	}

	filename := types.GetFilePath(fileType, objectId, backend.config.VolumeName)

	// Combine headers and data to match file backend behavior
	// The BlockLookup offsets include header size, so we must write headers+data
	var body []byte
	if headers != nil && len(*headers) > 0 {
		body = make([]byte, len(*headers)+len(*data))
		copy(body[:len(*headers)], *headers)
		copy(body[len(*headers):], *data)
	} else {
		body = *data
	}

	// Create a new S3 object
	object := &s3.PutObjectInput{
		Bucket: aws.String(backend.config.Bucket),
		Key:    aws.String(filename),
		Body:   bytes.NewReader(body),
	}

	_, err = backend.config.S3Client.PutObject(object)

	//slog.Info("Write object", "output", output)

	if err != nil {
		slog.Error("Error writing object", "error", err)
		return err
	}

	return nil
}

func (backend *Backend) ReadFrom(volumeName string, fileType types.FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error) {
	slog.Info("[S3 READFROM] Reading object", "volumeName", volumeName, "objectId", objectId, "offset", offset, "length", length)

	if backend.config.S3Client == nil {
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

	textResult, err := backend.config.S3Client.GetObject(requestObject)
	if err != nil {
		return nil, err
	}
	defer textResult.Body.Close()

	res, err := io.ReadAll(textResult.Body)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (backend *Backend) WriteTo(volumeName string, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) (err error) {
	if backend.config.S3Client == nil {
		return fmt.Errorf("S3 client not initialized")
	}

	filename := types.GetFilePath(fileType, objectId, volumeName)

	var body []byte
	if headers != nil && len(*headers) > 0 {
		body = make([]byte, len(*headers)+len(*data))
		copy(body[:len(*headers)], *headers)
		copy(body[len(*headers):], *data)
	} else {
		body = *data
	}

	object := &s3.PutObjectInput{
		Bucket: aws.String(backend.config.Bucket),
		Key:    aws.String(filename),
		Body:   bytes.NewReader(body),
	}

	_, err = backend.config.S3Client.PutObject(object)
	if err != nil {
		slog.Error("Error writing object", "error", err)
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
	backend.config = config.(S3Config)
}

func (backend *Backend) GetHost() string {
	return backend.config.Host
}
