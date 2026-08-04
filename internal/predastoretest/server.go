// Package predastoretest provides an in-process distributed predastore
// (Raft + QUIC + Badger) for viperblock tests.
//
// Predastore v1.2+ removed the single-process filesystem backend. The S3
// server panics if started without a wired backend in its config (httpserver.go
// leaves s.backend nil and the first listObjects request derefs it). Since the
// v5 cluster topology, that backend must be built and run separately via the
// clusterrun package and handed to the S3 frontend with WithPreparedBackend.
// This helper boots a full cluster runtime (shard-storage + state-replica
// nodes, all colocated in this process over the in-process pipe transport,
// so no network ports beyond the S3 HTTPS listener are needed) so
// viperblock's S3-backend tests have a working endpoint without dragging the
// test code into predastore internals.
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

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/mulgadc/predastore/clusterrun"
	"github.com/mulgadc/predastore/pkg/masterkey"
	predastoresvc "github.com/mulgadc/predastore/s3"
)

// Options configures a test predastore cluster. Required fields: DataDir,
// CertPath, KeyPath. Everything else has sensible defaults for viperblock.
type Options struct {
	// DataDir is the writable working directory used for master key, config
	// file, and per-node cluster data directories (Raft state, shard store).
	// Caller owns lifecycle (usually t.TempDir or os.MkdirTemp).
	DataDir string

	// CertPath / KeyPath point at a TLS keypair used by the S3 server's HTTPS
	// frontend. The cluster runtime itself needs no TLS material here: every
	// node runs colocated in this process over the in-process pipe.
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

	// NodeCount is the number of shard-storage nodes; default
	// DataShards+ParityShards. A fixed number of Raft state-replica nodes
	// (stateReplicaCount) is always added on top of these.
	NodeCount int
}

// Server is a running in-process predastore cluster. Endpoint is the
// "host:port" of the S3 HTTPS listener (no scheme). Shutdown tears down
// the HTTP server and the cluster runtime (Raft replicas, shard stores).
type Server struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string

	srv *predastoresvc.Server

	// rtCancel stops the cluster runtime's Run goroutine; rtDone reports its
	// exit so Shutdown can drain it instead of leaking it across tests.
	rtCancel context.CancelFunc
	rtDone   <-chan error
}

// Shutdown stops the cluster. Safe to call once.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	err := s.srv.Shutdown(ctx)
	s.srv = nil

	if s.rtCancel != nil {
		s.rtCancel()
	}
	if s.rtDone != nil {
		select {
		case rtErr := <-s.rtDone:
			if rtErr != nil && err == nil {
				err = fmt.Errorf("predastore cluster runtime: %w", rtErr)
			}
		case <-ctx.Done():
			if err == nil {
				err = ctx.Err()
			}
		}
		s.rtDone = nil
	}
	return err
}

// stateReplicaCount is the number of Raft state-replica nodes in the test
// cluster. Fixed rather than derived from NodeCount: Raft quorum wants an
// odd replica count, independent of how many RS shards the caller asked for.
const stateReplicaCount = 3

const configTmpl = `version = "1.0"
region = "{{.Region}}"

[rs]
data = {{.DataShards}}
parity = {{.ParityShards}}

[[host]]
id = 1
bind_addr = "127.0.0.1:{{.HostPort}}"
public_addr = "127.0.0.1:{{.HostPort}}"
data_dir = "{{.ClusterDataDir}}"

{{range .StorageNodes}}
[[node]]
id = {{.ID}}
host_id = 1
role = "shard-storage"
{{end}}
{{range .ReplicaNodes}}
[[node]]
id = {{.ID}}
host_id = 1
role = "state-replica"
{{end}}

[[auth]]
access_key_id = "{{.AccessKey}}"
secret_access_key = "{{.SecretKey}}"
account_id = "{{.AccountID}}"
policy = [
  { bucket = "*", actions = ["s3:*"] },
]
`

// nodeIDTmpl renders a single [[node]] table entry; role is fixed by which
// slice (StorageNodes vs ReplicaNodes) the template range is over.
type nodeIDTmpl struct {
	ID int
}

type configTmplData struct {
	Region         string
	DataShards     int
	ParityShards   int
	AccessKey      string
	SecretKey      string
	AccountID      string
	HostPort       int
	ClusterDataDir string
	StorageNodes   []nodeIDTmpl
	ReplicaNodes   []nodeIDTmpl
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

	// The topology needs a host address even though nothing binds it: every
	// node here is local, so the cluster runtime never opens the network
	// transport. A real free port just keeps the config honest.
	hostPort, err := freeTCPPort()
	if err != nil {
		return nil, fmt.Errorf("alloc cluster host port: %w", err)
	}

	tmplData := configTmplData{
		Region:         opts.Region,
		DataShards:     opts.DataShards,
		ParityShards:   opts.ParityShards,
		AccessKey:      opts.AccessKey,
		SecretKey:      opts.SecretKey,
		AccountID:      opts.AccountID,
		HostPort:       hostPort,
		ClusterDataDir: filepath.Join(opts.DataDir, "cluster"),
	}
	for i := 1; i <= opts.NodeCount; i++ {
		tmplData.StorageNodes = append(tmplData.StorageNodes, nodeIDTmpl{ID: i})
	}
	// Node ids are unique across roles, so replicas continue numbering after
	// the storage nodes rather than restarting at 1.
	for i := 1; i <= stateReplicaCount; i++ {
		tmplData.ReplicaNodes = append(tmplData.ReplicaNodes, nodeIDTmpl{ID: opts.NodeCount + i})
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
	key, err := masterkey.Load(keyPath)
	if err != nil {
		return nil, fmt.Errorf("load master key: %w", err)
	}

	cfg := &predastoresvc.Config{ConfigPath: cfgPath, BasePath: opts.DataDir}
	if err := cfg.ReadConfig(); err != nil {
		return nil, fmt.Errorf("read predastore config: %w", err)
	}

	// Build the cluster runtime (shard stores + Raft replicas) and run it in
	// the background; the S3 frontend below only wraps its backend.
	rt, err := clusterrun.Build(cfg, clusterrun.AllNodeIDs(cfg), opts.CertPath, opts.KeyPath, key)
	if err != nil {
		return nil, fmt.Errorf("build predastore cluster runtime: %w", err)
	}
	rtCtx, rtCancel := context.WithCancel(context.Background())
	rtDone := make(chan error, 1)
	go func() {
		rtDone <- rt.Run(rtCtx)
	}()

	// Writes need a committed leader; starting the S3 frontend before one
	// exists would fail the bucket creation below for no reason other than
	// timing.
	if err := rt.WaitReady(30 * time.Second); err != nil {
		rtCancel()
		<-rtDone
		return nil, fmt.Errorf("predastore cluster did not elect a leader: %w", err)
	}

	srv, err := predastoresvc.NewServer(
		predastoresvc.WithConfigPath(cfgPath),
		predastoresvc.WithAddress("127.0.0.1", s3Port),
		predastoresvc.WithTLS(opts.CertPath, opts.KeyPath),
		predastoresvc.WithBasePath(opts.DataDir),
		predastoresvc.WithEncryptionKeyFile(keyPath),
		predastoresvc.WithPreparedBackend(rt.Backend),
	)
	if err != nil {
		rtCancel()
		<-rtDone
		return nil, fmt.Errorf("predastore NewServer: %w", err)
	}

	if err := srv.ListenAndServeAsync(); err != nil {
		shutdownBestEffort(srv)
		rtCancel()
		<-rtDone
		return nil, fmt.Errorf("predastore ListenAndServeAsync: %w", err)
	}

	endpoint := fmt.Sprintf("127.0.0.1:%d", s3Port)
	if err := waitForHTTPS("https://"+endpoint, 30*time.Second); err != nil {
		shutdownBestEffort(srv)
		rtCancel()
		<-rtDone
		return nil, fmt.Errorf("predastore did not become ready: %w", err)
	}

	if err := createBucket(endpoint, opts); err != nil {
		shutdownBestEffort(srv)
		rtCancel()
		<-rtDone
		return nil, fmt.Errorf("create bucket %q: %w", opts.BucketName, err)
	}

	return &Server{
		Endpoint:  endpoint,
		Bucket:    opts.BucketName,
		AccessKey: opts.AccessKey,
		SecretKey: opts.SecretKey,
		Region:    opts.Region,
		srv:       srv,
		rtCancel:  rtCancel,
		rtDone:    rtDone,
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
	client := awss3.New(awss3.Options{
		Region:       opts.Region,
		BaseEndpoint: awssdk.String("https://" + endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(opts.AccessKey, opts.SecretKey, ""),
		UsePathStyle: true,
		HTTPClient:   httpClient,
	})
	_, err := client.CreateBucket(context.Background(), &awss3.CreateBucketInput{
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
