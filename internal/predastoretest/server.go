// Package predastoretest provides an in-process distributed predastore
// (Raft + QUIC + Badger) for viperblock tests.
//
// Predastore v1.2+ removed the single-process filesystem backend. The S3
// server panics if started without [[nodes]] in its config (httpserver.go
// leaves s.backend nil and the first listObjects request derefs it).
// This helper boots a full 3-node distributed cluster on loopback ports
// so viperblock's S3-backend tests have a working endpoint without
// dragging the test code into predastore internals.
package predastoretest

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"

	predastoresvc "github.com/mulgadc/predastore/s3"
)

// Options configures a test predastore cluster. Required fields: DataDir,
// CertPath, KeyPath. Everything else has sensible defaults for viperblock.
type Options struct {
	// DataDir is the writable working directory used for master key, config
	// file, Badger / Raft data dirs and per-node QUIC store directories.
	// Caller owns lifecycle (usually t.TempDir or os.MkdirTemp).
	DataDir string

	// CertPath / KeyPath point at a TLS keypair used by both the S3 server
	// and the embedded s3db (Raft IAM) servers.
	CertPath string
	KeyPath  string

	BucketName string
	AccessKey  string
	SecretKey  string
	AccountID  string
	Region     string

	// DataShards + ParityShards configure Reed-Solomon; defaults 2/1.
	DataShards   int
	ParityShards int

	// NodeCount is the number of DB + QUIC nodes; default 3.
	NodeCount int
}

// Server is a running in-process predastore cluster. Endpoint is the
// "host:port" of the S3 HTTPS listener (no scheme). Shutdown tears down
// the HTTP server, distributed backend, Raft cluster and QUIC servers.
type Server struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string

	srv *predastoresvc.Server
}

// Shutdown stops the cluster. Safe to call once.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	err := s.srv.Shutdown(ctx)
	s.srv = nil
	return err
}

const configTmpl = `version = "1.0"
region = "{{.Region}}"

[rs]
data = {{.DataShards}}
parity = {{.ParityShards}}

{{range .DBNodes}}
[[db]]
id = {{.ID}}
host = "127.0.0.1"
port = {{.HTTPPort}}
raft_port = {{.RaftPort}}
path = "db/node-{{.ID}}"
access_key_id = "{{$.AccessKey}}"
secret_access_key = "{{$.SecretKey}}"
{{if .Leader}}leader = true{{end}}
{{end}}

{{range .QuicNodes}}
[[nodes]]
id = {{.ID}}
host = "127.0.0.1"
port = {{.Port}}
path = "store/node-{{.ID}}"
{{end}}

[[auth]]
access_key_id = "{{.AccessKey}}"
secret_access_key = "{{.SecretKey}}"
account_id = "{{.AccountID}}"
policy = [
  { bucket = "*", actions = ["s3:*"] },
]
`

type dbNodeTmpl struct {
	ID       int
	HTTPPort int
	RaftPort int
	Leader   bool
}

type quicNodeTmpl struct {
	ID   int
	Port int
}

type configTmplData struct {
	Region       string
	DataShards   int
	ParityShards int
	AccessKey    string
	SecretKey    string
	AccountID    string
	DBNodes      []dbNodeTmpl
	QuicNodes    []quicNodeTmpl
}

// Start brings up a distributed predastore cluster and pre-creates the
// configured bucket. The returned Server.Endpoint is suitable for use as
// the Host field of viperblock's s3.S3Config (prepend "https://").
func Start(opts Options) (*Server, error) {
	if opts.DataDir == "" {
		return nil, fmt.Errorf("predastoretest: DataDir is required")
	}
	if opts.CertPath == "" || opts.KeyPath == "" {
		return nil, fmt.Errorf("predastoretest: CertPath and KeyPath are required")
	}
	if opts.BucketName == "" {
		opts.BucketName = "predastore"
	}
	if opts.AccessKey == "" {
		opts.AccessKey = "AKIAIOSFODNN7EXAMPLE"
	}
	if opts.SecretKey == "" {
		opts.SecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	}
	if opts.AccountID == "" {
		opts.AccountID = "123456789012"
	}
	if opts.Region == "" {
		opts.Region = "ap-southeast-2"
	}
	if opts.DataShards == 0 {
		opts.DataShards = 2
	}
	if opts.ParityShards == 0 {
		opts.ParityShards = 1
	}
	if opts.NodeCount == 0 {
		opts.NodeCount = opts.DataShards + opts.ParityShards
	}

	// Per-node ports.
	tmplData := configTmplData{
		Region:       opts.Region,
		DataShards:   opts.DataShards,
		ParityShards: opts.ParityShards,
		AccessKey:    opts.AccessKey,
		SecretKey:    opts.SecretKey,
		AccountID:    opts.AccountID,
	}
	for i := 1; i <= opts.NodeCount; i++ {
		dbHTTP, err := freeTCPPort()
		if err != nil {
			return nil, fmt.Errorf("alloc db http port: %w", err)
		}
		dbRaft, err := freeTCPPort()
		if err != nil {
			return nil, fmt.Errorf("alloc db raft port: %w", err)
		}
		quicPort, err := freeUDPPort()
		if err != nil {
			return nil, fmt.Errorf("alloc quic port: %w", err)
		}
		tmplData.DBNodes = append(tmplData.DBNodes, dbNodeTmpl{
			ID:       i,
			HTTPPort: dbHTTP,
			RaftPort: dbRaft,
			Leader:   i == 1,
		})
		tmplData.QuicNodes = append(tmplData.QuicNodes, quicNodeTmpl{
			ID:   i,
			Port: quicPort,
		})
	}

	s3Port, err := freeTCPPort()
	if err != nil {
		return nil, fmt.Errorf("alloc s3 https port: %w", err)
	}

	// Render config TOML.
	cfgPath := filepath.Join(opts.DataDir, "predastore.toml")
	tmpl, err := template.New("predastore").Parse(configTmpl)
	if err != nil {
		return nil, fmt.Errorf("parse config template: %w", err)
	}
	cfgFile, err := os.Create(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("create config file: %w", err)
	}
	if err := tmpl.Execute(cfgFile, tmplData); err != nil {
		cfgFile.Close()
		return nil, fmt.Errorf("render config: %w", err)
	}
	if err := cfgFile.Close(); err != nil {
		return nil, fmt.Errorf("close config file: %w", err)
	}

	// Generate a fresh master key per cluster.
	keyPath := filepath.Join(opts.DataDir, "master.key")
	if err := writeRandomKey(keyPath, 32); err != nil {
		return nil, fmt.Errorf("write master key: %w", err)
	}

	srv, err := predastoresvc.NewServer(
		predastoresvc.WithConfigPath(cfgPath),
		predastoresvc.WithAddress("127.0.0.1", s3Port),
		predastoresvc.WithTLS(opts.CertPath, opts.KeyPath),
		predastoresvc.WithBasePath(opts.DataDir),
		predastoresvc.WithBackend(predastoresvc.BackendDistributed),
		predastoresvc.WithEncryptionKeyFile(keyPath),
	)
	if err != nil {
		return nil, fmt.Errorf("predastore NewServer: %w", err)
	}

	if err := srv.ListenAndServeAsync(); err != nil {
		shutdownBestEffort(srv)
		return nil, fmt.Errorf("predastore ListenAndServeAsync: %w", err)
	}

	endpoint := fmt.Sprintf("127.0.0.1:%d", s3Port)
	if err := waitForHTTPS("https://"+endpoint, 30*time.Second); err != nil {
		shutdownBestEffort(srv)
		return nil, fmt.Errorf("predastore did not become ready: %w", err)
	}

	if err := createBucket(endpoint, opts); err != nil {
		shutdownBestEffort(srv)
		return nil, fmt.Errorf("create bucket %q: %w", opts.BucketName, err)
	}

	return &Server{
		Endpoint:  endpoint,
		Bucket:    opts.BucketName,
		AccessKey: opts.AccessKey,
		SecretKey: opts.SecretKey,
		Region:    opts.Region,
		srv:       srv,
	}, nil
}

func shutdownBestEffort(srv *predastoresvc.Server) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func writeRandomKey(path string, size int) error {
	key := make([]byte, size)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	return os.WriteFile(path, key, 0o600)
}

func freeTCPPort() (int, error) {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("predastoretest: TCP listener returned %T, want *net.TCPAddr", l.Addr())
	}
	return addr.Port, nil
}

func freeUDPPort() (int, error) {
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer pc.Close()
	addr, ok := pc.LocalAddr().(*net.UDPAddr)
	if !ok {
		return 0, fmt.Errorf("predastoretest: UDP listener returned %T, want *net.UDPAddr", pc.LocalAddr())
	}
	return addr.Port, nil
}

func waitForHTTPS(url string, timeout time.Duration) error {
	client := &http.Client{
		Timeout: 500 * time.Millisecond,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test-only: self-signed cert on loopback
		},
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			return nil
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("timeout")
	}
	return lastErr
}

// createBucket issues PUT /bucket via the S3 API so the bucket lands in
// Raft global state (the [[buckets]] config block is no longer wired to
// distributed bucket creation in v1.2+).
func createBucket(endpoint string, opts Options) error {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test-only: self-signed cert on loopback
		},
	}
	sess, err := session.NewSession(&awssdk.Config{
		Region:           awssdk.String(opts.Region),
		Endpoint:         awssdk.String("https://" + endpoint),
		Credentials:      credentials.NewStaticCredentials(opts.AccessKey, opts.SecretKey, ""),
		S3ForcePathStyle: awssdk.Bool(true),
		HTTPClient:       httpClient,
	})
	if err != nil {
		return fmt.Errorf("new aws session: %w", err)
	}
	client := awss3.New(sess)
	_, err = client.CreateBucket(&awss3.CreateBucketInput{
		Bucket: awssdk.String(opts.BucketName),
	})
	if err != nil {
		// Idempotent: an already-existing bucket is fine (re-running tests
		// against a recovered cluster, or a future-state where [[buckets]]
		// auto-creates again).
		if strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") ||
			strings.Contains(err.Error(), "BucketAlreadyExists") {
			return nil
		}
		return err
	}
	return nil
}
