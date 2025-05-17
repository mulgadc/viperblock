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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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

func New(config interface{}) (backend *Backend) {
	return &Backend{S3Backend: S3Backend{config: config.(S3Config)}}
}

func (backend *Backend) Init() error {

	slog.Info("Initializing S3 backend", "config", backend.config)

	// Create HTTP client (skip TLS verification if requested)
	client := &http.Client{}

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}

	client = &http.Client{Transport: tr}

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

func (backend *Backend) GetVolumeSize() uint64 {
	return backend.config.VolumeSize
}

func (backend *Backend) GetVolume() string {
	return backend.config.VolumeName
}

func (backend *Backend) Open(fname string) error {
	return nil
}

func (backend *Backend) Read(objectId uint64, offset uint32, length uint32) (data []byte, err error) {

	slog.Info("[S3 READ] Reading object", "objectId", objectId, "offset", offset, "length", length)

	if backend.config.S3Client == nil {
		return nil, fmt.Errorf("S3 client not initialized")
	}

	data = make([]byte, length)
	// Open the specified file
	filename := fmt.Sprintf("%s/chunk.%08d.bin", backend.config.VolumeName, objectId)

	// Fetch the object from S3 with a byte range
	requestObject := &s3.GetObjectInput{
		Bucket: aws.String(backend.config.Bucket),
		Key:    aws.String(filename),
		// Account for the 10 byte header metadata at the beginning of each block
		Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset-10, offset+length-1)),
	}

	textResult, err := backend.config.S3Client.GetObject(requestObject)

	if err != nil {
		return nil, err
	}

	res, err := io.ReadAll(textResult.Body)

	if err != nil {
		return nil, err
	}

	// Copy the res to the data for the specified length
	copy(data, res)

	return data, nil
}

func (backend *Backend) Write(objectId uint64, headers *[]byte, data *[]byte) (err error) {

	if backend.config.S3Client == nil {
		return fmt.Errorf("S3 client not initialized")
	}

	// Open the specified file
	filename := fmt.Sprintf("%s/chunk.%08d.bin", backend.config.VolumeName, objectId)

	// Create a new S3 object
	object := &s3.PutObjectInput{
		Bucket: aws.String(backend.config.Bucket),
		Key:    aws.String(filename),
		Body:   bytes.NewReader(*data),
	}

	output, err := backend.config.S3Client.PutObject(object)

	slog.Info("Write object", "output", output)

	if err != nil {
		slog.Error("Error writing object", "error", err)
		return err
	}

	return nil
}

func (backend *Backend) Sync() {
	return
}

func (backend *Backend) GetBackendType() string {
	return "s3"
}

func (backend *Backend) SetConfig(config interface{}) {
	backend.config = config.(S3Config)
}

func (backend *Backend) GetHost() string {
	return backend.config.Host
}
