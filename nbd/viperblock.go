package main

import (
	"C"
	"strconv"
	"sync/atomic"
	"unsafe"

	"libguestfs.org/nbdkit"
)
import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/bluebottle/pkg/otelsetup"
	"github.com/mulgadc/viperblock/telemetry"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"

	_ "github.com/mulgadc/bluebottle/pkg/fipsboot"
)

var pluginName = "viperblock"

// serviceName identifies this process to OTLP (resource service.name) and
// to the otelslog bridge, matching the metrics-apm.app.<name> data stream
// dashboards are built against.
const serviceName = "viperblockd"

// shutdownDrainTimeout bounds the backend half of connection close and plugin
// unload. An aborted drain is safe: committed writes are in the fsync'd local
// WAL and replay on the next attach.
const shutdownDrainTimeout = 20 * time.Second

// openLockWait bounds how long an arriving connection waits for the volume
// lock. It covers a handover behind the previous connection's drain, whose
// own bound is shutdownDrainTimeout.
const openLockWait = shutdownDrainTimeout

// mapClaimTimeout bounds the first head's write at open to the volume lease
// TTL, so a launch that cannot commit it never serves IO.
const mapClaimTimeout = 45 * time.Second

// otelShutdown flushes the OTLP exporters on Unload, if otelsetup.Init
// configured real ones. nil (a no-op) when OTLP export is not configured.
var otelShutdown func(context.Context) error

type ViperBlockPlugin struct {
	nbdkit.Plugin
}

var _ nbdkit.PluginInterface = (*ViperBlockPlugin)(nil)

type ViperBlockConnection struct {
	nbdkit.Connection
	vb *viperblock.VB
	// readonly mirrors the flag nbdkit passes to Open (set by its -r). The
	// write paths refuse when it is set, so a read-only export stays read-only
	// even for a client that ignores the transmission flag.
	readonly bool
}

// activeVB holds the VB for the current open connection. CanMultiConn returns
// false so at most one connection exists at a time. Unload uses this as a
// fallback flush when nbdkit exits without calling the per-connection Close
// (observed when the NBD client is killed while nbdkit is already in SIGTERM
// shutdown mode).
var activeVB *viperblock.VB

// volumeLock holds the flock'd file for the currently-open volume, admission
// control for the race CanMultiConn's false return only advises about (it
// does not gate connection admission — nbdkit's Go binding hard-codes
// NBDKIT_THREAD_MODEL_PARALLEL). Acquired in Open before New() constructs a
// VB, released in Close/Unload. See viperblock.AcquireVolumeLock.
var volumeLock *os.File

// snapshotListener is the unix socket that spinifex connects to before calling
// CreateSnapshot. The handler calls DrainToBackend and acks "OK\n", ensuring S3
// state is current without requiring every guest fsync to hit S3.
var snapshotListener net.Listener
var snapshotListenerDone chan struct{}

var size uint64

var volume string
var bucket string
var region string
var access_key string
var secret_key string
var host string
var base_dir string

// wal_base_dir mirrors viperblock.VB.WALBaseDir. Optional: empty routes the
// WAL back onto base_dir, the same as before this parameter existed.
var wal_base_dir string

var cache_size int = viperblock.DefaultCacheBlocks
var max_pending_bytes uint64 = 0
var upload_workers int = 16
var use_shardwal bool = false

// lease_epoch orders this launch's map heads above every earlier launch's.
var lease_epoch uint64
var qualified_chunks bool

// launchChunkIDs reserves qualified payload IDs across all opens under lease_epoch.
var launchChunkIDs atomic.Uint64

// mapClaimed records that this process consumed a head ordinal, so a later
// connection can resume its epoch even if the backend reply was lost.
var mapClaimed atomic.Bool

// mapHeadN and mapObjN carry map counters across opens, ambiguous writes included.
var mapHeadN, mapObjN atomic.Uint64

// The volume lock serializes this update with the next engine's Open.
func rememberMapCounters(vb *viperblock.VB) {
	if !vb.IsMapDelta() {
		return
	}
	headN, objN := vb.MapCounters()
	mapHeadN.Store(max(mapHeadN.Load(), headN))
	mapObjN.Store(max(mapObjN.Load(), objN))
	mapClaimed.Store(true)
}

func closeStartupWALs(vb *viperblock.VB) {
	for _, wal := range []*viperblock.WAL{&vb.WAL, &vb.BlockToObjectWAL} {
		for _, file := range wal.DB {
			if err := file.Close(); err != nil {
				slog.Warn("failed to close startup WAL", "err", err)
			}
		}
		wal.DB = nil
	}
	if vb.ShardedWAL != nil {
		for _, shard := range vb.ShardedWAL.Shards {
			if shard.DB == nil {
				continue
			}
			if err := shard.DB.Close(); err != nil {
				slog.Warn("failed to close startup WAL shard", "err", err)
			}
			shard.DB = nil
		}
	}
}

var encryption_key_file string

// gc_enabled mirrors viperblock.VB.GCEnabled. Defaults false so chunk GC
// stays off until the spawning process explicitly opts in.
var gc_enabled bool = false

var disk []byte

// loadedMasterKey is populated once in ConfigComplete from
// -encryption-key-file / ENCRYPTION_KEY_FILE so per-connection Open does not
// re-read the key file on every NBD client attach. nil iff encryption is
// disabled for this plugin instance.
var loadedMasterKey *masterkey.Key

// accessKeyEnv and secretKeyEnv name the environment variables that supply
// S3 credentials when nbdkit's plugin argv did not set them. This keeps
// credentials out of argv, which is world-readable via /proc/<pid>/cmdline
// for the life of the process.
const (
	accessKeyEnv = "VB_ACCESS_KEY"
	secretKeyEnv = "VB_SECRET_KEY"
)

// credentialFromArgOrEnv resolves a credential, preferring the nbdkit argv
// value and falling back to the named environment variable when argv did
// not set it. Mirrors LoadMasterKeyFromFlagOrEnv's flag-then-env precedence.
func credentialFromArgOrEnv(argValue, envName string) string {
	if argValue != "" {
		return argValue
	}
	return os.Getenv(envName)
}

// NOTE: For profiling viperblock/nbdkit, use Linux perf instead of Go pprof.
// Go's pprof does NOT work correctly when running as a CGO shared library.
//
// Usage:
//   perf record -g -p $(pgrep -f nbdkit) -- sleep 60
//   perf report

func (p *ViperBlockPlugin) Config(key string, value string) error {

	// Config options
	// size (bytes)
	// volume (string)
	// bucket (string)

	if key == "size" {
		var err error
		size, err = strconv.ParseUint(value, 0, 64)
		if err != nil {
			return err
		}
		return nil
	} else if key == "volume" {
		volume = value
		return nil
	} else if key == "bucket" {
		bucket = value
		return nil
	} else if key == "region" {
		region = value
		return nil
	} else if key == "access_key" {
		access_key = value
		return nil
	} else if key == "secret_key" {
		secret_key = value
	} else if key == "host" {
		host = value
		return nil
	} else if key == "base_dir" {
		base_dir = value
		return nil
	} else if key == "wal_base_dir" {
		wal_base_dir = value
		return nil
	} else if key == "cache_size" {
		var err error
		cache_size, err = strconv.Atoi(value)
		if err != nil {
			return err
		}
		return nil
	} else if key == "max_pending_bytes" {
		var err error
		max_pending_bytes, err = strconv.ParseUint(value, 0, 64)
		if err != nil {
			return err
		}
		return nil
	} else if key == "upload_workers" {
		var err error
		upload_workers, err = strconv.Atoi(value)
		if err != nil {
			return err
		}
		return nil
	} else if key == "shardwal" {
		use_shardwal = value == "true" || value == "1"
		return nil
	} else if key == "lease_epoch" {
		var err error
		lease_epoch, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
		return nil
	} else if key == "qualified_chunks" {
		var err error
		qualified_chunks, err = strconv.ParseBool(value)
		return err
	} else if key == "gc_enabled" {
		gc_enabled = value == "true" || value == "1"
		return nil
	} else if key == "encryption_key_file" {
		encryption_key_file = value
		return nil
	} else {
		return nbdkit.PluginError{Errmsg: "unknown parameter"}
	}

	return nil
}

func (p *ViperBlockPlugin) ConfigComplete() error {
	// Fall back to the environment when nbdkit's argv did not supply
	// credentials, so spinifex can stop passing them as literal argv.
	access_key = credentialFromArgOrEnv(access_key, accessKeyEnv)
	secret_key = credentialFromArgOrEnv(secret_key, secretKeyEnv)

	if size == 0 {
		return nbdkit.PluginError{Errmsg: "size parameter is required"}
	} else if volume == "" {
		return nbdkit.PluginError{Errmsg: "volume parameter is required"}
	} else if bucket == "" {
		return nbdkit.PluginError{Errmsg: "bucket parameter is required"}
	} else if region == "" {
		return nbdkit.PluginError{Errmsg: "region parameter is required"}
	} else if access_key == "" {
		return nbdkit.PluginError{Errmsg: "access_key parameter is required"}
	} else if secret_key == "" {
		return nbdkit.PluginError{Errmsg: "secret_key parameter is required"}
	} else if base_dir == "" {
		return nbdkit.PluginError{Errmsg: "base_dir parameter is required"}
	} else if host == "" {
		return nbdkit.PluginError{Errmsg: "host parameter is required"}
	}

	// OTLP export starts in AfterFork, not here: its exporters run background
	// goroutines, and nbdkit resets stdio after this returns on the assumption
	// that nothing else is allocating descriptors.
	level := slog.LevelInfo
	if os.Getenv("VIPERBLOCK_DEBUG") == "1" {
		level = slog.LevelDebug
	}
	// Stderr, not stdout: nbdkit repoints a plugin's fd 1 at /dev/null but
	// leaves fd 2 on the parent's journald socket.
	otelsetup.SetDefaultJSONLogger(serviceName, level, otelsetup.WithWriter(os.Stderr))

	// Resolve and load the master key once at plugin startup so per-NBD-
	// connection Open avoids a disk read + permission check on every attach.
	mkey, err := viperblock.LoadMasterKeyFromFlagOrEnv(encryption_key_file)
	if err != nil {
		return nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not load encryption key: %v", err)}
	}
	loadedMasterKey = mkey
	if use_shardwal && loadedMasterKey != nil {
		return nbdkit.PluginError{Errmsg: "shardwal is incompatible with encryption: sharded WAL is not supported on encrypted volumes"}
	}
	return nil
}

func (p *ViperBlockPlugin) GetReady() error {
	return nil
}

// AfterFork wires OTLP export and the slog bridge, both no-ops without
// OTEL_EXPORTER_OTLP_* configured. This is the only place viperblock's
// process-wide telemetry is touched, never viperblock.New, which an embedder calls.
func (p *ViperBlockPlugin) AfterFork() error {
	// No TracerProvider: the plugin has no server loop to root spans from, and
	// when viperblock is embedded instead the host (spx) already owns tracing.
	shutdown, err := otelsetup.Init(context.Background(), serviceName,
		otelsetup.WithoutTracing(), otelsetup.WithoutRuntimeMetrics())
	if err != nil {
		slog.Error("otel telemetry init failed, continuing without OTLP export", "err", err)
		return nil
	}
	otelShutdown = shutdown
	return nil
}

func (p *ViperBlockPlugin) Open(readonly bool) (nbdkit.ConnectionInterface, error) {
	return p.open(readonly, func(cfg *viperblock.VB, backend s3.S3Config) (*viperblock.VB, error) {
		return viperblock.New(cfg, "s3", backend)
	}, (*viperblock.VB).OpenWAL)
}

func (p *ViperBlockPlugin) open(readonly bool,
	newVolume func(*viperblock.VB, s3.S3Config) (*viperblock.VB, error),
	openBlockWAL func(*viperblock.VB, *viperblock.WAL, string) error,
) (nbdkit.ConnectionInterface, error) {

	// Admission control: at most one Open() proceeds past this point per
	// volume. Acquired before viperblock.New() so a second concurrent
	// connection (nbdkit's parallel thread model) never races the first
	// through LoadState/OpenWAL/persistStateLocal — the vector that tears
	// config.json and corrupts WAL headers.
	//
	// Waits rather than failing fast, because the common case is handover:
	// a reconnecting client arrives while the previous connection's Close is
	// still draining. Genuine contention still fails, just later.
	lock, err := viperblock.AcquireVolumeLockWait(base_dir, volume, openLockWait)
	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not open volume %q: %v", volume, err)}
	}
	volumeLock = lock

	// Release the lock on every early return below — those are failed Open
	// attempts, and the caller (nbdkit, retried by the client) must not be
	// wedged behind a half-finished Open. success is only set true once the
	// connection is fully constructed and handed back, at which point the
	// lock's lifetime transfers to Close/Unload.
	success := false
	defer func() {
		if success {
			return
		}
		if relErr := viperblock.ReleaseVolumeLock(volumeLock); relErr != nil {
			slog.Error("Open: failed to release volume lock after error", "err", relErr)
		}
		volumeLock = nil
	}()

	cfg := s3.S3Config{
		VolumeName: volume,
		VolumeSize: size,
		Bucket:     bucket,
		Region:     region,
		AccessKey:  access_key,
		SecretKey:  secret_key,
		Host:       host,
	}

	vbconfig := viperblock.VB{
		VolumeName:          volume,
		VolumeSize:          size,
		BaseDir:             base_dir,
		WALBaseDir:          wal_base_dir,
		WALSyncInterval:     -1,
		ChunkUploadInterval: -1,
		GCInterval:          -1,
		// Data-path engine. Recorded on the volume-open metric so a volume
		// simultaneously held by a control-plane import is visible.
		Role:            "nbdkit",
		LeaseEpoch:      lease_epoch,
		QualifiedChunks: qualified_chunks,
		LaunchChunkIDs:  &launchChunkIDs,
		ResumeEpoch:     mapClaimed.Load(),
		ResumeHeadN:     mapHeadN.Load(),
		ResumeObjN:      mapObjN.Load(),
		Cache: viperblock.Cache{
			Config: viperblock.CacheConfig{
				Size: cache_size,
			},
		},
		MaxPendingBytes:   max_pending_bytes,
		UploadWorkers:     upload_workers,
		MasterKey:         loadedMasterKey,
		EncryptionEnabled: loadedMasterKey != nil,
		// Set here, not post-construction like UseShardedWAL below: New
		// copies GCEnabled once at construction time.
		GCEnabled: gc_enabled,
	}

	// Credentials on cfg are deliberately not logged.
	slog.Info("creating viperblock backend", "backend", "s3", "volume", cfg.VolumeName,
		"bucket", cfg.Bucket, "region", cfg.Region, "endpoint", cfg.Host)
	vb, err := newVolume(&vbconfig, cfg)
	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not create Viperblock backend: %v", err)}
	}

	defer func() {
		if !success {
			vb.Detach()
			if mapClaimed.Load() {
				rememberMapCounters(vb)
			}
			closeStartupWALs(vb)
		}
	}()

	// Apply nbdkit config override for sharded WAL
	vb.UseShardedWAL = use_shardwal

	// Debug logging is configured once for the whole process in
	// ConfigComplete via otelsetup.SetDefaultJSONLogger, not per-connection.

	// Set 20% LRU memory from host
	//vb.SetCacheSize(0, 20)

	// Generate a temp directory that ends in viperblock
	//voldata := "/tmp/viperblock"

	// Initialize the backend
	err = vb.Backend.Init()
	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: "Could not initialize backend"}
	}

	var walNum uint64

	// Share one deadline through discovery and claim. Load the published map
	// before writing startup state or replaying the local WAL.
	claimCtx, cancelClaim := context.WithTimeout(context.Background(), mapClaimTimeout)
	err = vb.LoadStateAndLiveCheckpointCtx(claimCtx)
	if err != nil {
		cancelClaim()
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not load block state: %v", err)}
	}

	// Mint the UUID before recovery or guest I/O can seal a block.
	if err = vb.EnsureVolumeUUID(); err != nil {
		cancelClaim()
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not mint volume UUID: %v", err)}
	}

	previousHead, _ := vb.MapCounters()
	err = vb.ClaimMapCtx(claimCtx)
	cancelClaim()
	nextHead, _ := vb.MapCounters()
	if err == nil || nextHead > previousHead {
		rememberMapCounters(vb)
	}
	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not claim block map: %v", err)}
	}

	err = vb.RecoverLocalWALs()
	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not recover local WALs: %v", err)}
	}

	walNum = vb.WAL.WallNum.Add(1)
	slog.Info("Loaded WAL number", "walNum", walNum)

	// Open the chunk WAL (sharded or legacy)
	if vb.UseShardedWAL {
		vb.ShardedWAL = viperblock.NewShardedWAL(vb.WAL.BaseDir, vb.WAL.WALMagic)
		vb.ShardedWAL.WallNum.Store(vb.WAL.WallNum.Load())
		err = vb.OpenShardedWAL()
	} else {
		err = vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume())))
	}

	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not open WAL: %v", err)}
	}

	// Open the block to object WAL
	err = openBlockWAL(vb, &vb.BlockToObjectWAL, filepath.Join(vb.WAL.BaseDir, types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume())))

	if err != nil {
		return &ViperBlockConnection{}, nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not open Block WAL: %v", err)}
	}

	vb.WALSyncInterval = viperblock.DefaultWALSyncInterval
	vb.ChunkUploadInterval = viperblock.DefaultChunkUploadInterval
	vb.GCInterval = viperblock.DefaultGCInterval
	vb.StartWALSyncer()
	vb.StartChunkUploader()
	vb.StartChunkGC()

	activeVB = vb
	snapshotListenerDone = startSnapshotListener(vb.DrainToBackend, filepath.Join(base_dir, volume, "snapshot.sock"))
	success = true
	return &ViperBlockConnection{vb: vb, readonly: readonly}, nil
}

// startSnapshotListener opens a unix socket that spinifex connects to before
// calling CreateSnapshot. The handler calls DrainToBackend — flushing WAL
// chunks and the live checkpoint to S3 — then acks "OK\n". Returns a channel
// closed when the goroutine exits.
func startSnapshotListener(drain func() error, sockPath string) chan struct{} {
	done := make(chan struct{})

	// Remove any stale socket file from a previous run
	_ = os.Remove(sockPath)

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		slog.Error("snapshot socket: listen failed", "path", sockPath, "err", err)
		close(done)
		return done
	}
	snapshotListener = ln
	var draining atomic.Bool

	go func() {
		defer close(done)
		defer os.Remove(sockPath)
		for {
			conn, err := ln.Accept()
			if err != nil {
				// Listener closed — normal shutdown path
				return
			}
			// A timed-out client can leave a drain in flight. Refuse
			// overlap before queuing work behind the engine's drain lock.
			if !draining.CompareAndSwap(false, true) {
				_, _ = conn.Write([]byte("ERR\n"))
				_ = conn.Close()
				continue
			}
			go func(c net.Conn) {
				defer c.Close()
				err := drain()
				draining.Store(false)
				if err != nil {
					slog.Error("snapshot socket: DrainToBackend failed", "err", err)
					_, _ = c.Write([]byte("ERR\n"))
					return
				}
				_, _ = c.Write([]byte("OK\n"))
			}(conn)
		}
	}()

	slog.Info("snapshot socket listening", "path", sockPath)
	return done
}

// stopSnapshotListener closes the unix socket and waits for the goroutine to exit.
func stopSnapshotListener() {
	if snapshotListener == nil {
		return
	}
	snapshotListener.Close()
	if snapshotListenerDone != nil {
		<-snapshotListenerDone
	}
	snapshotListener = nil
	snapshotListenerDone = nil
}

// backendErrToPluginError wraps a viperblock write-path error as an
// nbdkit.PluginError, mapping viperblock.ErrNoSpace onto ENOSPC so the guest
// kernel reports "no space left on device" instead of a generic I/O error.
func backendErrToPluginError(msgPrefix string, err error) nbdkit.PluginError {
	perr := nbdkit.PluginError{Errmsg: fmt.Sprintf("%s: %v", msgPrefix, err)}
	if errors.Is(err, viperblock.ErrNoSpace) {
		perr.Errno = syscall.ENOSPC
	}
	return perr
}

func (c *ViperBlockConnection) GetSize() (uint64, error) {
	return c.vb.GetVolumeSize(), nil

}

// CanMultiConn reports whether clients may safely make multiple connections.
// TODO: confirm changes
func (c *ViperBlockConnection) CanMultiConn() (bool, error) {
	return false, nil

}

// recordGuest reports one served NBD request. Deferred by each handler so the
// timing covers the whole call including any error return, and so a handler
// that grows an early return cannot silently stop being measured.
func (c *ViperBlockConnection) recordGuest(op string, bytes int, start time.Time, err *error) {
	outcome := "success"
	if *err != nil {
		outcome = "error"
	}
	telemetry.RecordGuestIO(context.Background(), op, c.vb.VolumeName, outcome, bytes, time.Since(start))
}

func (c *ViperBlockConnection) PRead(buf []byte, offset uint64, flags uint32) (err error) {
	defer c.recordGuest("read", len(buf), time.Now(), &err)

	slog.Debug("PREAD:", "offset", offset, "len", len(buf))
	readErr := c.vb.ReadAtInto(offset, buf)
	if readErr != nil && readErr != viperblock.ErrZeroBlock {
		err = nbdkit.PluginError{Errmsg: fmt.Sprintf("Could not read data: %v", readErr)}
		return err
	}

	return nil
}

// CanWrite is required in golang plugins, otherwise PWrite
// will never be called.
func (c *ViperBlockConnection) CanWrite() (bool, error) {
	return !c.readonly, nil
}

func (c *ViperBlockConnection) PWrite(buf []byte, offset uint64,
	flags uint32) (err error) {
	defer c.recordGuest("write", len(buf), time.Now(), &err)

	if c.readonly {
		err = nbdkit.PluginError{Errmsg: "write to a read-only export", Errno: syscall.EROFS}
		return err
	}

	// WriteAt copies the borrowed nbdkit buffer into engine-owned blocks
	// synchronously without mutating or retaining the input.
	if writeErr := c.vb.WriteAt(offset, buf); writeErr != nil {
		err = backendErrToPluginError("Could not write data", writeErr)
		return err
	}

	return nil
}

func (c *ViperBlockConnection) CanZero() (bool, error) {
	return !c.readonly, nil
}

func (c *ViperBlockConnection) Zero(count uint32, offset uint64, flags uint32) error {

	slog.Debug("ZERO:", "len", count, "offset", offset)

	if c.readonly {
		return nbdkit.PluginError{Errmsg: "zero on a read-only export", Errno: syscall.EROFS}
	}

	if count == 0 {
		return nil
	}

	// nbdkit checks the range before it calls Zero; this guard keeps a direct
	// caller from writing a prefix, and subtracts so it cannot overflow.
	size := c.vb.GetVolumeSize()
	if offset > size {
		return backendErrToPluginError("Could not write zero data", viperblock.ErrRequestTooLarge)
	}
	if uint64(count) > size-offset {
		return backendErrToPluginError("Could not write zero data", viperblock.ErrRequestOutOfRange)
	}
	// Each write ends on a block edge. A block larger than the buffer is
	// written in pieces that stay inside it, so a write queues one block.
	blockSize := uint64(c.vb.BlockSize)
	step := uint64(zeroChunkBytes)
	if blockSize <= step {
		step -= step % blockSize
	}
	var wholeBlock []byte
	for end := offset + uint64(count); offset < end; {
		n := min(end-offset, step-offset%step)
		buf := zeroChunk[:]
		if blockSize > step {
			n = min(end-offset, step, blockSize-offset%blockSize)
			// A whole block is replaced in one write, so its old data is never
			// read; the request still holds at most one block.
			if offset%blockSize == 0 && end-offset >= blockSize {
				if wholeBlock == nil {
					wholeBlock = make([]byte, blockSize)
				}
				n, buf = blockSize, wholeBlock
			}
		}
		if err := c.vb.WriteAt(offset, buf[:n]); err != nil {
			return backendErrToPluginError("Could not write zero data", err)
		}
		offset += n
	}
	return nil
}

// zeroChunkBytes is the most of a zero request buffered at a time. Splits fall
// on absolute multiples of the largest whole-block step that fits in it.
const zeroChunkBytes = 4 << 20

// zeroChunk is never written: WriteAt copies its input.
var zeroChunk [zeroChunkBytes]byte

func (c *ViperBlockConnection) CanTrim() (bool, error) {
	return !c.readonly, nil
}

func (c *ViperBlockConnection) Trim(count uint32, offset uint64, flags uint32) error {

	slog.Debug("TRIM:", "len", count, "offset", offset)

	if c.readonly {
		return nbdkit.PluginError{Errmsg: "trim on a read-only export", Errno: syscall.EROFS}
	}

	return nil
}

// CanFlush is required in golang plugins that implement
// Flush(), otherwise Flush() will never be called.
func (c *ViperBlockConnection) CanFlush() (bool, error) {
	return true, nil
}

// Flush honours the NBD durability contract to the level this deployment
// promises: on success, every write already acknowledged to the guest is
// fsynced to the local WAL and survives host crash or power loss. It does not
// wait for the data to reach predastore, so it does not survive loss of the
// node itself.
func (c *ViperBlockConnection) Flush(flags uint32) (err error) {
	defer c.recordGuest("flush", 0, time.Now(), &err)

	if flushErr := c.vb.Flush(); flushErr != nil {
		err = backendErrToPluginError("Flush failed", flushErr)
		return err
	}
	return nil
}

func (c *ViperBlockConnection) Close() {
	slog.Info("Close, flushing block state to disk")

	stopSnapshotListener()

	// One budget for the drain and the close together, not one each: systemd's
	// TimeoutStopSec backstops this, and two full budgets in series would
	// overrun it and turn the backstop into a SIGKILL.
	ctx, cancel := context.WithTimeout(context.Background(), shutdownDrainTimeout)
	defer cancel()

	// Stop background map reservations before draining.
	c.vb.StopMapBackground()
	// DrainToBackend before Close so the live checkpoint is current. Close
	// will also flush WAL chunks, but does not save the live checkpoint.
	if err := c.vb.DrainToBackendCtx(ctx); err != nil {
		slog.Error("Close: DrainToBackend failed", "err", err)
	}

	if err := c.vb.CloseCtx(ctx); err != nil {
		slog.Error("Could not close VB", "err", err)
	} else if err := viperblock.WriteSealReceipt(base_dir, volume); err != nil {
		// Best effort: the seal already succeeded, so a missing receipt only
		// costs the consumer a WARN it would otherwise have skipped.
		slog.Error("Close: could not write seal receipt", "err", err)
	}
	rememberMapCounters(c.vb)
	activeVB = nil

	// Release the volume lock last, after the VB is fully drained and
	// closed, so a racing opener that was refused admission cannot start
	// until this connection has genuinely finished with the volume.
	if err := viperblock.ReleaseVolumeLock(volumeLock); err != nil {
		slog.Error("Close: failed to release volume lock", "err", err)
	}
	volumeLock = nil
}

// Unload is called by nbdkit immediately before the process exits. It flushes
// and saves block state if Close was not called on the active connection — this
// happens when the NBD client is killed abruptly while nbdkit is already in
// SIGTERM shutdown mode, which causes nbdkit to skip the per-connection close
// callback.
func (p *ViperBlockPlugin) Unload() {
	defer func() {
		if otelShutdown == nil {
			return
		}
		if err := otelShutdown(context.Background()); err != nil {
			slog.Error("Unload: otel shutdown failed", "err", err)
		}
	}()

	if activeVB == nil {
		// Close may have already run (lock already released), or Open may
		// have failed after acquiring the lock but before its own defer ran
		// (process exiting mid-Open). Release defensively either way so
		// Unload never leaves a lock behind — ReleaseVolumeLock(nil) is a
		// no-op if there is nothing to release.
		if err := viperblock.ReleaseVolumeLock(volumeLock); err != nil {
			slog.Error("Unload: failed to release volume lock", "err", err)
		}
		volumeLock = nil
		return
	}
	slog.Info("Unload: Close was not called, flushing block state now")
	stopSnapshotListener()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownDrainTimeout)
	defer cancel()

	// Stop background map reservations before draining.
	activeVB.StopMapBackground()
	if err := activeVB.DrainToBackendCtx(ctx); err != nil {
		slog.Error("Unload: DrainToBackend failed", "err", err)
	}
	if err := activeVB.CloseCtx(ctx); err != nil {
		slog.Error("Unload: could not close VB", "err", err)
	} else if err := viperblock.WriteSealReceipt(base_dir, volume); err != nil {
		// Best effort: the seal already succeeded, so a missing receipt only
		// costs the consumer a WARN it would otherwise have skipped.
		slog.Error("Unload: could not write seal receipt", "err", err)
	}
	activeVB = nil
	if err := viperblock.ReleaseVolumeLock(volumeLock); err != nil {
		slog.Error("Unload: failed to release volume lock", "err", err)
	}
	volumeLock = nil
}

//----------------------------------------------------------------------
//
// The boilerplate below this line is required by all golang plugins,
// as well as importing "C" and "unsafe" modules at the top of the
// file.

//export plugin_init
func plugin_init() unsafe.Pointer {
	// If your plugin needs to do any initialization, you can
	// either put it here or implement a Load() method.
	// ...

	// Then you must call the following function.
	return nbdkit.PluginInitialize(pluginName, &ViperBlockPlugin{})
}

// This is never(?) called, but must exist.
func main() {}
