package file

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/mulgadc/viperblock/telemetry"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/utils"
)

// 2. Define config structs.
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

// Ctx variants satisfy the context-aware half of types.Backend; local file
// I/O has no cancellation points so they delegate to the plain methods.
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

// 3. Implement WithConfig for each backend.
func New(config any) (backend *Backend) {
	cfg, ok := config.(FileConfig)
	if !ok {
		panic("file backend: expected FileConfig")
	}
	return &Backend{FileBackend: FileBackend{config: cfg, log: slog.Default()}}
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
		length = utils.SafeInt64ToUint32(stat.Size())
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
	//fmt.Println("CREATING CHUNK FILE:", filename)

	file, err := os.Create(filename)

	if err != nil {
		backend.log.Error("Failed to create chunk file", "error", err)
		return err
	}

	//fmt.Println("Writing headers & block")
	_, err = file.Write(*headers)

	if err != nil {
		backend.log.Error("Failed to write headers", "error", err)
		return err
	}

	_, err = file.Write(*data)

	if err != nil {
		backend.log.Error("Failed to write data", "error", err)
		return err
	}

	err = file.Close()

	if err != nil {
		backend.log.Error("Failed to close file", "error", err)
		return err
	}

	return nil
}

func (backend *Backend) Sync() {

	//fmt.Println("Syncing block")

}

func (backend *Backend) GetBackendType() string {
	return "file"
}

func (backend *Backend) SetConfig(config any) {
	cfg, ok := config.(FileConfig)
	if !ok {
		panic("file backend: expected FileConfig")
	}
	backend.config = cfg
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
		length = utils.SafeInt64ToUint32(stat.Size())
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
		backend.log.Error("Failed to create file", "error", err)
		return err
	}

	if headers != nil && len(*headers) > 0 {
		if _, err = file.Write(*headers); err != nil {
			if cerr := file.Close(); cerr != nil {
				backend.log.Warn("failed to close file during cleanup", "error", cerr)
			}
			return err
		}
	}

	if data != nil {
		if _, err = file.Write(*data); err != nil {
			if cerr := file.Close(); cerr != nil {
				backend.log.Warn("failed to close file during cleanup", "error", cerr)
			}
			return err
		}
	}

	return file.Close()
}

func (backend *Backend) GetHost() string {
	return ""
}
