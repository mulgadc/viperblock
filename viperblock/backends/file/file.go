// Package file is a viperblock backend that stores volume objects as files
// under a local base directory.
package file

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/mulgadc/bluebottle/pkg/safecast"
	"github.com/mulgadc/viperblock/telemetry"
	"github.com/mulgadc/viperblock/types"
)

// FileConfig is the file backend configuration.
type FileConfig struct {
	VolumeName string
	VolumeSize uint64
	BaseDir    string
}

type FileBackend struct {
	config FileConfig
	log    *slog.Logger
}

type Backend struct {
	FileBackend
}

var _ types.Backend = (*Backend)(nil)

// InitCtx and the other Ctx variants satisfy types.Backend's context-aware half;
// local file I/O has no cancellation points so they delegate to the plain methods.
func (backend *Backend) InitCtx(_ context.Context) error {
	return backend.Init()
}

func (backend *Backend) ReadCtx(ctx context.Context, fileType types.FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error) {
	start := time.Now()
	data, err = backend.Read(fileType, objectId, offset, length)
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	telemetry.RecordBackendIO(ctx, "read", "file", backend.config.VolumeName, outcome, len(data), time.Since(start))
	return data, err
}

func (backend *Backend) WriteCtx(ctx context.Context, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	start := time.Now()
	err := backend.Write(fileType, objectId, headers, data)
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	telemetry.RecordBackendIO(ctx, "write", "file", backend.config.VolumeName, outcome, writeLen(headers, data), time.Since(start))
	return err
}

// classifyWriteErr maps a local write failure into types.ErrNoSpace when the
// underlying syscall reports ENOSPC (disk full). Any other error passes
// through unchanged.
func classifyWriteErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ENOSPC) {
		return fmt.Errorf("%w: %w", types.ErrNoSpace, err)
	}
	return err
}

// writeLen returns the combined byte length of headers+data as actually
// written by Write, without allocating a copy.
func writeLen(headers, data *[]byte) int {
	n := 0
	if headers != nil {
		n += len(*headers)
	}
	if data != nil {
		n += len(*data)
	}
	return n
}

func (backend *Backend) ReadFromCtx(_ context.Context, volumeName string, fileType types.FileType, objectId uint64, offset uint32, length uint32) ([]byte, error) {
	return backend.ReadFrom(volumeName, fileType, objectId, offset, length)
}

func (backend *Backend) WriteToCtx(_ context.Context, volumeName string, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) error {
	return backend.WriteTo(volumeName, fileType, objectId, headers, data)
}

// New returns a file backend configured from a FileConfig.
func New(config any) (backend *Backend, err error) {
	cfg, ok := config.(FileConfig)
	if !ok {
		return nil, fmt.Errorf("%w: file backend expected FileConfig, got %T", types.ErrBackendConfig, config)
	}
	return &Backend{FileBackend: FileBackend{config: cfg, log: slog.Default()}}, nil
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
	backend.log.Info("Init for file backend", "volumeName", backend.config.VolumeName)

	// Check if the directory exists
	if _, err := os.Stat(backend.config.BaseDir); os.IsNotExist(err) {
		backend.log.Error("Directory does not exist", "error", err)
		return err
	}

	dirPath := fmt.Sprintf("%s/%s", backend.config.BaseDir, backend.config.VolumeName)

	// Create the directory structure
	err := os.MkdirAll(dirPath, 0750)

	if err != nil {
		backend.log.Error("Failed to create directory", "error", err)
		return err
	}

	// Create the directory structure

	dirs := []string{
		"chunks",
		"checkpoints",
		"wal",
		"wal/chunks",
		"wal/blocks",
	}

	for _, dir := range dirs {
		dirPath := fmt.Sprintf("%s/%s/%s", backend.config.BaseDir, backend.config.VolumeName, dir)
		// Create all parent dirs
		err := os.MkdirAll(dirPath, 0750)
		if err != nil {
			backend.log.Error("Failed to create directory", "error", err)
			return err
		}
	}

	// MkdirAll leaves each new directory entry only in its parent's page
	// cache. Write fsyncs the directory it renames an object into, which does
	// not help if that directory's own entry is lost, so a power cut could
	// take the whole subtree after reclaim had deleted the local WAL.
	// Provisioning may have created the base directory and some of its
	// ancestors together without syncing any of them, and an entry lost
	// anywhere on that chain takes everything below it.
	// A symlinked base has two chains that matter: the target's ancestors hold
	// the objects, and the ancestors of the link itself are what keep the base
	// reachable at all.
	chains := []string{filepath.Dir(backend.config.BaseDir)}
	resolved, resolveErr := filepath.EvalSymlinks(backend.config.BaseDir)
	if resolveErr != nil {
		backend.log.Error("Failed to resolve the base directory", "dir", backend.config.BaseDir, "error", resolveErr)
		return classifyWriteErr(resolveErr)
	}
	if resolvedParent := filepath.Dir(resolved); resolvedParent != chains[0] {
		chains = append(chains, resolvedParent)
	}
	for _, chain := range chains {
		if err := syncAncestry(chain); err != nil {
			backend.log.Error("Failed to sync the base directory's ancestry", "dir", chain, "error", err)
			return classifyWriteErr(err)
		}
	}

	for _, dir := range []string{
		backend.config.BaseDir,
		filepath.Join(backend.config.BaseDir, backend.config.VolumeName),
		filepath.Join(backend.config.BaseDir, backend.config.VolumeName, "wal"),
	} {
		if err := syncDirectory(dir); err != nil {
			backend.log.Error("Failed to sync directory after create", "dir", dir, "error", err)
			return classifyWriteErr(err)
		}
	}

	return nil
}

func (backend *Backend) Open(fname string) (err error) {
	/*
		backend.filename, err = os.Open(fname)

		if err != nil {
			return
		}

		fmt.Println("Open specified file")
	*/
	return err
}

func (backend *Backend) Read(fileType types.FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error) {
	// Open the specified file
	//filename := fmt.Sprintf("%s/%s/chunk.%08d.bin", backend.config.BaseDir, backend.config.VolumeName, objectId)
	filename := fmt.Sprintf("%s/%s", backend.config.BaseDir, types.GetFilePath(fileType, objectId, backend.config.VolumeName))

	f, err := os.OpenFile(filename, os.O_RDONLY, 0600)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	// If the length is undefined, read the entire file (e.g config file, or block2object state)
	if length == 0 {
		stat, err := os.Stat(filename)
		if err != nil {
			return nil, err
		}
		length = safecast.Int64ToUint32(stat.Size())
	}

	// Read the specified block for the length
	data = make([]byte, length)
	_, err = f.ReadAt(data, int64(offset))

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (backend *Backend) Write(fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) (err error) {
	// TODO Improve
	//filename := fmt.Sprintf("%s/%s/chunk.%08d.bin", backend.config.BaseDir, backend.config.VolumeName, objectId)

	filename := fmt.Sprintf("%s/%s", backend.config.BaseDir, types.GetFilePath(fileType, objectId, backend.config.VolumeName))
	var h, d []byte
	if headers != nil {
		h = *headers
	}
	if data != nil {
		d = *data
	}
	return backend.writeFileDurable(filename, h, d)
}

// writeFileDurable writes headers then data to filename and makes the result
// durable before returning.
func (backend *Backend) writeFileDurable(filename string, headers, data []byte) (err error) {
	// Write to a temporary neighbour, fsync it, then rename into place and
	// fsync the directory. Truncating the destination with os.Create and
	// closing without a sync returned success while the bytes were still only
	// in page cache, so a caller that reclaims its local WAL on that success
	// could lose both copies to a power cut.
	tmp, err := os.CreateTemp(filepath.Dir(filename), stagingPrefix+filepath.Base(filename)+stagingSuffix)
	if err != nil {
		err = classifyWriteErr(err)
		backend.log.Error("Failed to create chunk file", "error", err)
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			tmp.Close()
			if rmErr := os.Remove(tmpName); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
				backend.log.Warn("Failed to remove partial chunk file", "file", tmpName, "error", rmErr)
			}
		}
	}()

	if _, err = tmp.Write(headers); err != nil {
		err = classifyWriteErr(err)
		backend.log.Error("Failed to write headers", "error", err)
		return err
	}

	if _, err = tmp.Write(data); err != nil {
		err = classifyWriteErr(err)
		backend.log.Error("Failed to write data", "error", err)
		return err
	}

	if err = tmp.Sync(); err != nil {
		err = classifyWriteErr(err)
		backend.log.Error("Failed to sync chunk file", "error", err)
		return err
	}

	if err = tmp.Close(); err != nil {
		err = classifyWriteErr(err)
		backend.log.Error("Failed to close file", "error", err)
		return err
	}

	if err = os.Rename(tmpName, filename); err != nil {
		err = classifyWriteErr(err)
		backend.log.Error("Failed to rename chunk file into place", "error", err)
		return err
	}

	dir := filepath.Dir(filename)
	if err = syncDirectory(dir); err != nil {
		err = classifyWriteErr(err)
		backend.log.Error("Failed to sync directory after rename", "dir", dir, "error", err)
		return err
	}

	return nil
}

// A staging file is named so it is recognisable without knowing the object it
// is becoming: dot-prefixed to keep it out of a casual listing, and carrying
// the marker ListObjects filters on.
const (
	stagingPrefix = "."
	stagingSuffix = ".tmp"
)

// isStagingName reports whether a directory entry is one of Write's in-flight
// neighbours rather than an object.
func isStagingName(name string) bool {
	return strings.HasPrefix(name, stagingPrefix) && strings.Contains(name, stagingSuffix)
}

// syncAncestry fsyncs dir and each ancestor above it, stopping at the
// filesystem boundary. An fsync cannot make an entry durable in a filesystem
// it does not reach, so the mount point is where the chain genuinely ends.
func syncAncestry(dir string) error {
	for {
		if err := syncDirectory(dir); err != nil {
			return err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		same, err := sameFilesystem(dir, parent)
		if err != nil || !same {
			return err
		}
		dir = parent
	}
}

// sameFilesystem reports whether two paths live on one device, which is how a
// mount point is recognised without parsing the mount table.
func sameFilesystem(a, b string) (bool, error) {
	devOf := func(path string) (uint64, error) {
		info, err := os.Stat(path)
		if err != nil {
			return 0, err
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return 0, fmt.Errorf("stat %s: no device number available", path)
		}
		return st.Dev, nil
	}
	devA, err := devOf(a)
	if err != nil {
		return false, err
	}
	devB, err := devOf(b)
	if err != nil {
		return false, err
	}
	return devA == devB, nil
}

// syncDirectory fsyncs a directory so a rename into it survives a power cut.
//
// Both failures are returned rather than logged. The object is visible either
// way, so this reads like an over-strict error, but a caller that reclaims its
// local WAL on a successful Write has no second copy: reporting success for a
// rename that is not yet on the device is how both copies are lost at once.
//
// A variable so a test can inject the I/O failure a healthy filesystem does
// not produce, which is the only way to prove Write propagates it.
var syncDirectory = func(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Sync is a no-op because Write is already durable: it fsyncs each object and
// the directory it renames that object into before returning success.
func (backend *Backend) Sync() {
}

func (backend *Backend) GetBackendType() string {
	return "file"
}

func (backend *Backend) SetConfig(config any) error {
	cfg, ok := config.(FileConfig)
	if !ok {
		return fmt.Errorf("%w: file backend expected FileConfig, got %T", types.ErrBackendConfig, config)
	}
	backend.config = cfg
	return nil
}

func (backend *Backend) ReadFrom(volumeName string, fileType types.FileType, objectId uint64, offset uint32, length uint32) (data []byte, err error) {
	filename := fmt.Sprintf("%s/%s", backend.config.BaseDir, types.GetFilePath(fileType, objectId, volumeName))

	f, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if length == 0 {
		stat, err := os.Stat(filename)
		if err != nil {
			return nil, err
		}
		length = safecast.Int64ToUint32(stat.Size())
	}

	data = make([]byte, length)
	_, err = f.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (backend *Backend) WriteTo(volumeName string, fileType types.FileType, objectId uint64, headers *[]byte, data *[]byte) (err error) {
	filename := fmt.Sprintf("%s/%s", backend.config.BaseDir, types.GetFilePath(fileType, objectId, volumeName))

	if err := os.MkdirAll(filepath.Dir(filename), 0750); err != nil {
		return fmt.Errorf("failed to create directory for %s: %w", filename, err)
	}

	file, err := os.Create(filename)
	if err != nil {
		err = classifyWriteErr(err)
		backend.log.Error("Failed to create file", "error", err)
		return err
	}

	if headers != nil && len(*headers) > 0 {
		if _, err = file.Write(*headers); err != nil {
			if cerr := file.Close(); cerr != nil {
				backend.log.Warn("failed to close file during cleanup", "error", cerr)
			}
			return classifyWriteErr(err)
		}
	}

	if data != nil {
		if _, err = file.Write(*data); err != nil {
			if cerr := file.Close(); cerr != nil {
				backend.log.Warn("failed to close file during cleanup", "error", cerr)
			}
			return classifyWriteErr(err)
		}
	}

	return classifyWriteErr(file.Close())
}

// Delete removes the object identified by fileType/objectId from this
// backend's own volume. os.Remove already reports errors.Is(err,
// os.ErrNotExist) for a missing object, so no wrapping is needed here.
func (backend *Backend) Delete(fileType types.FileType, objectId uint64) (err error) {
	filename := fmt.Sprintf("%s/%s", backend.config.BaseDir, types.GetFilePath(fileType, objectId, backend.config.VolumeName))
	return os.Remove(filename)
}

func (backend *Backend) DeleteCtx(ctx context.Context, fileType types.FileType, objectId uint64) (err error) {
	start := time.Now()
	err = backend.Delete(fileType, objectId)
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	telemetry.RecordBackendIO(ctx, "delete", "file", backend.config.VolumeName, outcome, 0, time.Since(start))
	return err
}

// ListPrefixes returns the directory entries under BaseDir whose name has
// prefix, one level deep. BaseDir is the backend's root, not this backend's
// own VolumeName directory, so sibling volumes/snapshots are visible too.
func (backend *Backend) ListPrefixes(prefix string) (names []string, err error) {
	entries, err := os.ReadDir(backend.config.BaseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), prefix) {
			names = append(names, entry.Name())
		}
	}
	return names, nil
}

func (backend *Backend) ListPrefixesCtx(_ context.Context, prefix string) (names []string, err error) {
	return backend.ListPrefixes(prefix)
}

// ListObjects returns every regular file's key (path relative to BaseDir)
// under prefix, walked recursively. Missing directories are not an error,
// matching ListPrefixes.
func (backend *Backend) ListObjects(prefix string) (keys []string, err error) {
	root := filepath.Join(backend.config.BaseDir, prefix)

	walkErr := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		// A staging file is a write in flight or one a crash abandoned, never
		// an object. Listing it invites a caller to read a partial write or to
		// treat it as a WAL generation.
		if isStagingName(entry.Name()) {
			return nil
		}
		rel, relErr := filepath.Rel(backend.config.BaseDir, path)
		if relErr != nil {
			return relErr
		}
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	if walkErr != nil && !os.IsNotExist(walkErr) {
		return nil, walkErr
	}

	return keys, nil
}

func (backend *Backend) ListObjectsCtx(_ context.Context, prefix string) (keys []string, err error) {
	return backend.ListObjects(prefix)
}

func (backend *Backend) GetHost() string {
	return ""
}

// ReadKeyCtx reads the whole object at key, relative to this backend's volume.
func (backend *Backend) ReadKeyCtx(ctx context.Context, key string) (data []byte, err error) {
	start := time.Now()
	data, err = os.ReadFile(backend.keyPath(key))
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	telemetry.RecordBackendIO(ctx, "read", "file", backend.config.VolumeName, outcome, len(data), time.Since(start))
	return data, err
}

// WriteKeyCtx writes data at key durably, with the same staging, fsync and
// rename as Write, creating the key's directory if needed.
func (backend *Backend) WriteKeyCtx(ctx context.Context, key string, data []byte) (err error) {
	start := time.Now()
	defer func() {
		outcome := "success"
		if err != nil {
			outcome = "error"
		}
		telemetry.RecordBackendIO(ctx, "write", "file", backend.config.VolumeName, outcome, len(data), time.Since(start))
	}()
	filename := backend.keyPath(key)
	if err = os.MkdirAll(filepath.Dir(filename), 0o750); err != nil {
		return classifyWriteErr(err)
	}
	return backend.writeFileDurable(filename, nil, data)
}

// DeleteKeyCtx removes the object at key.
func (backend *Backend) DeleteKeyCtx(ctx context.Context, key string) (err error) {
	start := time.Now()
	err = os.Remove(backend.keyPath(key))
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	telemetry.RecordBackendIO(ctx, "delete", "file", backend.config.VolumeName, outcome, 0, time.Since(start))
	return err
}

func (backend *Backend) keyPath(key string) string {
	return filepath.Join(backend.config.BaseDir, backend.config.VolumeName, filepath.FromSlash(key))
}
