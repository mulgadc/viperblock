// Package predastoretest provides an in-process predastore cluster for
// viperblock tests.
//
// Predastore has no single-process filesystem backend: an S3 gate serves in
// front of erasure-coded blob nodes and a Raft-replicated metadata plane, and
// a gate without those behind it stores nothing. This helper boots one host
// running the whole set, so viperblock's S3-backend tests have a working
// endpoint without dragging the test code into predastore internals. Every
// node is colocated, so they talk over predastore's in-process pipe and the
// gate is the only one that binds a socket.
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
	"strconv"
	"strings"
	"text/template"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	pds "github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/pkg/masterkey"
)

// Options configures a test predastore cluster. Required fields: DataDir,
// CertPath, KeyPath. Everything else has sensible defaults for viperblock.
type Options struct {
	// DataDir is the writable working directory holding the master key, the
	// config file and each node's data directory. It must be absolute, and the
	// caller owns its lifecycle (usually t.TempDir or os.MkdirTemp).
	DataDir string

	// CertPath / KeyPath point at a TLS keypair the S3 gate serves under; both
	// must be absolute. Nothing else needs TLS material: intra-cluster traffic
	// never leaves the process.
	CertPath string
	KeyPath  string

	BucketName string
	AccessKey  string
	SecretKey  string
	AccountID  string
	Region     string

	// DataShards + ParityShards configure Reed-Solomon; defaults 2/1. A stripe
	// spreads over distinct blob nodes, so the cluster runs one blob node per
	// shard.
	DataShards   int
	ParityShards int

	// MetaNodes is the number of Raft replicas holding global state; default 1,
	// which bootstraps straight to a leader and is all a test needs.
	MetaNodes int
}

// Server is a running in-process predastore cluster. Endpoint is the
// "host:port" of the S3 HTTPS listener (no scheme). Shutdown stops every node
// and waits for them to drain.
type Server struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
	Region    string

	stop    context.CancelFunc
	drained <-chan error
}

// Shutdown stops the cluster and waits for it to drain, bounded by ctx.
// Calling it more than once is a no-op.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.stop == nil {
		return nil
	}
	s.stop()
	s.stop = nil

	select {
	case err := <-s.drained:
		return err
	case <-ctx.Done():
		return fmt.Errorf("predastoretest: cluster did not drain: %w", ctx.Err())
	}
}

// testHost is the address every node is reached on: one host runs the lot, so
// the cluster never leaves loopback.
const testHost = "127.0.0.1"

// testHostID is the [[host]] this process runs, and gateNodeID the node the
// roles that keep state are numbered after.
const (
	testHostID = 1
	gateNodeID = 1
)

const configTmpl = `version = 1
region = "{{.Region}}"

[rs]
data = {{.DataShards}}
parity = {{.ParityShards}}

[[host]]
id = {{.HostID}}
bind_addr = "{{.Host}}"
addr = "{{.Host}}"
data_dir = "{{.DataDir}}"
tls_cert = "{{.CertPath}}"
tls_key = "{{.KeyPath}}"
encryption_key = "{{.EncryptionKey}}"
{{range .Nodes}}
[[host.node]]
id = {{.ID}}
role = "{{.Role}}"
port = {{.Port}}
{{end}}
[[auth]]
access_key_id = "{{.AccessKey}}"
secret_access_key = "{{.SecretKey}}"
account_id = "{{.AccountID}}"
`

// nodeTmpl renders one [[host.node]] table.
type nodeTmpl struct {
	ID   int
	Role string
	Port int
}

type configTmplData struct {
	HostID        int
	Host          string
	Region        string
	DataDir       string
	CertPath      string
	KeyPath       string
	EncryptionKey string
	DataShards    int
	ParityShards  int
	AccessKey     string
	SecretKey     string
	AccountID     string
	Nodes         []nodeTmpl
}

// Start brings up a predastore cluster and pre-creates the configured bucket.
// The returned Server.Endpoint is suitable for use as the Host field of
// viperblock's s3.S3Config (prepend "https://").
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
	// Predastore admits zero parity only at one data shard, where there is
	// nothing to stripe; anything wider must carry parity.
	if opts.ParityShards == 0 && opts.DataShards != 1 {
		opts.ParityShards = 1
	}
	if opts.MetaNodes == 0 {
		opts.MetaNodes = 1
	}

	// The gate is the only node that binds, but a port identifies every node to
	// the pipe transport, so each still needs one no sibling has claimed.
	blobNodes := opts.DataShards + opts.ParityShards
	ports, err := freeTCPPorts(1 + blobNodes + opts.MetaNodes)
	if err != nil {
		return nil, fmt.Errorf("alloc node ports: %w", err)
	}

	encryptionKeyPath := filepath.Join(opts.DataDir, "master.key")
	tmplData := configTmplData{
		HostID:        testHostID,
		Host:          testHost,
		Region:        opts.Region,
		DataDir:       opts.DataDir,
		CertPath:      opts.CertPath,
		KeyPath:       opts.KeyPath,
		EncryptionKey: encryptionKeyPath,
		DataShards:    opts.DataShards,
		ParityShards:  opts.ParityShards,
		AccessKey:     opts.AccessKey,
		SecretKey:     opts.SecretKey,
		AccountID:     opts.AccountID,
	}
	// Node ids are unique across roles, so each role continues numbering after
	// the last rather than restarting.
	tmplData.Nodes = append(tmplData.Nodes, nodeTmpl{ID: gateNodeID, Role: "gate", Port: ports[0]})
	for i := range blobNodes {
		id := gateNodeID + 1 + i
		tmplData.Nodes = append(tmplData.Nodes, nodeTmpl{ID: id, Role: "blob", Port: ports[id-1]})
	}
	for i := range opts.MetaNodes {
		id := gateNodeID + 1 + blobNodes + i
		tmplData.Nodes = append(tmplData.Nodes, nodeTmpl{ID: id, Role: "meta", Port: ports[id-1]})
	}

	// The config goes through the real file and the real loader, so the helper
	// trips over the same strict decode and topology validation an install does.
	cfgPath := filepath.Join(opts.DataDir, "predastore.toml")
	if err := writeConfig(cfgPath, tmplData); err != nil {
		return nil, err
	}

	// Predastore mandates a 32-byte master key at mode 0600: masterkey.Load is
	// fail-closed on anything group- or other-readable.
	if err := writeRandomKey(encryptionKeyPath, 32); err != nil {
		return nil, fmt.Errorf("write master key: %w", err)
	}

	cfg, err := pds.LoadConfig(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("load predastore config: %w", err)
	}
	key, err := masterkey.Load(encryptionKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load master key: %w", err)
	}

	// Run blocks for as long as it serves and drains everything it started on
	// the way out, so the cancel and the done channel are the whole handle
	// Shutdown needs.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		err := pds.Run(ctx, pds.Options{
			Config:    cfg,
			HostID:    pds.HostID(testHostID),
			MasterKey: key,
		})
		// A cancelled cluster stopped because Shutdown asked it to, so its
		// error is the request rather than a failure to report.
		if ctx.Err() != nil {
			err = nil
		}
		done <- err
	}()

	srv := &Server{
		Endpoint:  net.JoinHostPort(testHost, strconv.Itoa(ports[0])),
		Bucket:    opts.BucketName,
		AccessKey: opts.AccessKey,
		SecretKey: opts.SecretKey,
		Region:    opts.Region,
		stop:      cancel,
		drained:   done,
	}

	// The gate holds off serving until the local Raft quorum has a leader, so
	// an answered request also means writes will commit.
	if err := waitForHTTPS("https://"+srv.Endpoint, 30*time.Second, done); err != nil {
		shutdownBestEffort(srv)
		return nil, fmt.Errorf("predastore did not become ready: %w", err)
	}

	if err := createBucket(srv.Endpoint, opts); err != nil {
		shutdownBestEffort(srv)
		return nil, fmt.Errorf("create bucket %q: %w", opts.BucketName, err)
	}

	return srv, nil
}

func writeConfig(path string, data configTmplData) error {
	tmpl, err := template.New("predastore").Parse(configTmpl)
	if err != nil {
		return fmt.Errorf("parse config template: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create config file: %w", err)
	}
	if err := tmpl.Execute(f, data); err != nil {
		f.Close()
		return fmt.Errorf("render config: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close config file: %w", err)
	}
	return nil
}

func shutdownBestEffort(srv *Server) {
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

// freeTCPPorts returns n distinct free ports. Every listener is held open
// until the last port is allocated, because closing each in turn would let the
// kernel hand the same one back for the next.
func freeTCPPorts(n int) ([]int, error) {
	listeners := make([]net.Listener, 0, n)
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()

	ports := make([]int, 0, n)
	for range n {
		l, err := net.Listen("tcp4", net.JoinHostPort(testHost, "0"))
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, l)
		addr, ok := l.Addr().(*net.TCPAddr)
		if !ok {
			return nil, fmt.Errorf("predastoretest: TCP listener returned %T, want *net.TCPAddr", l.Addr())
		}
		ports = append(ports, addr.Port)
	}
	return ports, nil
}

// waitForHTTPS polls the endpoint until it answers, the cluster exits or
// timeout elapses. A cluster that failed to start reports on done, which is
// the difference between failing at once and waiting out the timeout.
func waitForHTTPS(url string, timeout time.Duration, done <-chan error) error {
	client := &http.Client{
		Timeout: 500 * time.Millisecond,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test-only: self-signed cert on loopback
		},
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			return fmt.Errorf("cluster exited before serving: %w", err)
		default:
		}
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

// createBucket issues PUT /bucket via the S3 API so the bucket lands in the
// meta plane; a config-defined [[bucket]] is not visible to ListBuckets.
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
		// Idempotent: an already-existing bucket is fine when a cluster is
		// started against a data directory it already wrote.
		if strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") ||
			strings.Contains(err.Error(), "BucketAlreadyExists") {
			return nil
		}
		return err
	}
	return nil
}
