package file

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBackend(t *testing.T) *Backend {
	t.Helper()
	backend := New(FileConfig{
		VolumeName: "test-vol",
		VolumeSize: 1024 * 1024,
		BaseDir:    t.TempDir(),
	})
	require.NoError(t, backend.Init())
	return backend
}

func TestNewDefaultsLogger(t *testing.T) {
	backend := New(FileConfig{VolumeName: "v", VolumeSize: 1, BaseDir: t.TempDir()})
	assert.NotNil(t, backend.log, "New must default the instance logger")
	assert.Same(t, slog.Default(), backend.log)
}

func TestSetLogger(t *testing.T) {
	backend := New(FileConfig{VolumeName: "v", VolumeSize: 1, BaseDir: t.TempDir()})

	var buf bytes.Buffer
	custom := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	backend.SetLogger(custom)
	assert.Same(t, custom, backend.log)

	// nil resets to slog.Default() rather than leaving the backend loggerless.
	backend.SetLogger(nil)
	assert.Same(t, slog.Default(), backend.log)
}

func TestInitAndInitCtx(t *testing.T) {
	backend := New(FileConfig{VolumeName: "v", VolumeSize: 1, BaseDir: t.TempDir()})
	require.NoError(t, backend.Init())

	backend2 := New(FileConfig{VolumeName: "v2", VolumeSize: 1, BaseDir: t.TempDir()})
	require.NoError(t, backend2.InitCtx(context.Background()))
}

func TestInitMissingBaseDirFails(t *testing.T) {
	backend := New(FileConfig{VolumeName: "v", VolumeSize: 1, BaseDir: "/nonexistent/base/dir"})
	assert.Error(t, backend.Init())
}

func TestWriteReadRoundtrip(t *testing.T) {
	backend := newTestBackend(t)

	headers := []byte("hdr!")
	data := []byte("hello world")

	require.NoError(t, backend.Write(types.FileTypeChunk, 0, &headers, &data))

	got, err := backend.Read(types.FileTypeChunk, 0, 0, uint32(len(headers)+len(data)))
	require.NoError(t, err)
	assert.Equal(t, append(append([]byte{}, headers...), data...), got)

	// length=0 reads the whole file.
	got, err = backend.Read(types.FileTypeChunk, 0, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, append(append([]byte{}, headers...), data...), got)
}

func TestWriteCtxAndReadCtx(t *testing.T) {
	backend := newTestBackend(t)

	headers := []byte{}
	data := []byte("ctx payload")

	require.NoError(t, backend.WriteCtx(context.Background(), types.FileTypeChunk, 1, &headers, &data))

	got, err := backend.ReadCtx(context.Background(), types.FileTypeChunk, 1, 0, uint32(len(data)))
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestWriteLen(t *testing.T) {
	headers := []byte("abc")
	data := []byte("de")
	assert.Equal(t, 5, writeLen(&headers, &data))
	assert.Equal(t, 3, writeLen(&headers, nil))
	assert.Equal(t, 2, writeLen(nil, &data))
	assert.Equal(t, 0, writeLen(nil, nil))
}

func TestReadFromAndReadFromCtx(t *testing.T) {
	backend := newTestBackend(t)

	data := []byte("from-volume")
	empty := []byte{}
	require.NoError(t, backend.WriteTo("other-vol", types.FileTypeChunk, 0, &empty, &data))

	got, err := backend.ReadFrom("other-vol", types.FileTypeChunk, 0, 0, uint32(len(data)))
	require.NoError(t, err)
	assert.Equal(t, data, got)

	got, err = backend.ReadFromCtx(context.Background(), "other-vol", types.FileTypeChunk, 0, 0, uint32(len(data)))
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestWriteToAndWriteToCtx(t *testing.T) {
	backend := newTestBackend(t)

	headers := []byte("H")
	data := []byte("D")
	require.NoError(t, backend.WriteTo("vol-a", types.FileTypeChunk, 0, &headers, &data))

	got, err := backend.ReadFrom("vol-a", types.FileTypeChunk, 0, 0, 2)
	require.NoError(t, err)
	assert.Equal(t, []byte("HD"), got)

	require.NoError(t, backend.WriteToCtx(context.Background(), "vol-b", types.FileTypeChunk, 0, &headers, &data))
	got, err = backend.ReadFrom("vol-b", types.FileTypeChunk, 0, 0, 2)
	require.NoError(t, err)
	assert.Equal(t, []byte("HD"), got)
}

func TestMiscBackendMethods(t *testing.T) {
	backend := newTestBackend(t)

	assert.Equal(t, "file", backend.GetBackendType())
	assert.Equal(t, "", backend.GetHost())
	assert.NoError(t, backend.Open("ignored"))
	backend.Sync() // no-op, just must not panic

	backend.SetConfig(FileConfig{VolumeName: "renamed", VolumeSize: 2, BaseDir: t.TempDir()})
	assert.Equal(t, "renamed", backend.config.VolumeName)
}

func TestNewPanicsOnWrongConfigType(t *testing.T) {
	assert.Panics(t, func() {
		New("not-a-file-config")
	})
}

func TestSetConfigPanicsOnWrongConfigType(t *testing.T) {
	backend := newTestBackend(t)
	assert.Panics(t, func() {
		backend.SetConfig("not-a-file-config")
	})
}
