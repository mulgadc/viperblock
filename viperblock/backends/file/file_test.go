package file

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
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
	assert.Empty(t, backend.GetHost())
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

func TestDeleteAndDeleteCtx(t *testing.T) {
	backend := newTestBackend(t)

	headers := []byte{}
	data := []byte("chunk-data")
	require.NoError(t, backend.Write(types.FileTypeChunk, 0, &headers, &data))
	require.NoError(t, backend.Write(types.FileTypeChunk, 1, &headers, &data))

	// Delete removes the object; a subsequent read fails with ErrNotExist.
	require.NoError(t, backend.Delete(types.FileTypeChunk, 0))
	_, err := backend.Read(types.FileTypeChunk, 0, 0, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)

	// The sibling object is untouched.
	got, err := backend.Read(types.FileTypeChunk, 1, 0, uint32(len(data)))
	require.NoError(t, err)
	assert.Equal(t, data, got)

	// DeleteCtx behaves identically for a fresh object.
	require.NoError(t, backend.DeleteCtx(context.Background(), types.FileTypeChunk, 1))
	_, err = backend.Read(types.FileTypeChunk, 1, 0, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestDeleteAlreadyMissingReturnsErrNotExist(t *testing.T) {
	backend := newTestBackend(t)

	// Matches the s3 backend's wrapNotFound contract that chunk GC's sweep
	// relies on to treat "already gone" as success.
	err := backend.Delete(types.FileTypeChunk, 42)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)

	err = backend.DeleteCtx(context.Background(), types.FileTypeChunk, 42)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestListPrefixesAndListPrefixesCtx(t *testing.T) {
	backend := newTestBackend(t)

	// ListPrefixes lists directories under BaseDir, not under the backend's
	// own VolumeName, so write two sibling "volumes" directly under BaseDir.
	headers := []byte{}
	data := []byte("x")
	require.NoError(t, backend.WriteTo("snap-alpha", types.FileTypeConfig, 0, &headers, &data))
	require.NoError(t, backend.WriteTo("snap-beta", types.FileTypeConfig, 0, &headers, &data))
	require.NoError(t, backend.WriteTo("other-vol", types.FileTypeConfig, 0, &headers, &data))

	names, err := backend.ListPrefixes("snap-")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"snap-alpha", "snap-beta"}, names)

	names, err = backend.ListPrefixesCtx(context.Background(), "snap-")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"snap-alpha", "snap-beta"}, names)
}

func TestListPrefixesMissingBaseDirIsEmptyNotError(t *testing.T) {
	backend := New(FileConfig{VolumeName: "v", VolumeSize: 1, BaseDir: filepath.Join(t.TempDir(), "does-not-exist")})

	names, err := backend.ListPrefixes("snap-")
	require.NoError(t, err)
	assert.Empty(t, names)
}

func TestListObjectsAndListObjectsCtx(t *testing.T) {
	backend := newTestBackend(t)

	headers := []byte{}
	data := []byte("chunk")
	require.NoError(t, backend.Write(types.FileTypeChunk, 0, &headers, &data))
	require.NoError(t, backend.Write(types.FileTypeChunk, 1, &headers, &data))
	require.NoError(t, backend.Write(types.FileTypeChunk, 2, &headers, &data))

	keys, err := backend.ListObjects(backend.config.VolumeName + "/chunks/")
	require.NoError(t, err)
	assert.Len(t, keys, 3)
	for _, k := range keys {
		assert.Contains(t, k, "chunks/chunk.")
	}

	keys, err = backend.ListObjectsCtx(context.Background(), backend.config.VolumeName+"/chunks/")
	require.NoError(t, err)
	assert.Len(t, keys, 3)

	// Deleting one object drops it from a subsequent listing.
	require.NoError(t, backend.Delete(types.FileTypeChunk, 1))
	keys, err = backend.ListObjects(backend.config.VolumeName + "/chunks/")
	require.NoError(t, err)
	assert.Len(t, keys, 2)
}

func TestListObjectsMissingPrefixIsEmptyNotError(t *testing.T) {
	backend := newTestBackend(t)

	keys, err := backend.ListObjects("no-such-volume/chunks/")
	require.NoError(t, err)
	assert.Empty(t, keys)
}

// TestClassifyWriteErr pins ENOSPC detection at any wrap depth, and that an
// unrelated syscall.Errno passes through unclassified.
func TestClassifyWriteErr(t *testing.T) {
	cases := []struct {
		name        string
		in          error
		wantNoSpace bool
	}{
		{
			name:        "nil_passes_through",
			in:          nil,
			wantNoSpace: false,
		},
		{
			name:        "bare_enospc_maps_to_no_space",
			in:          syscall.ENOSPC,
			wantNoSpace: true,
		},
		{
			name: "path_error_wrapped_enospc_maps_to_no_space",
			in: &os.PathError{
				Op:   "write",
				Path: "/tmp/some/chunk.bin",
				Err:  syscall.ENOSPC,
			},
			wantNoSpace: true,
		},
		{
			name:        "unrelated_errno_stays_unclassified",
			in:          syscall.EACCES,
			wantNoSpace: false,
		},
		{
			name:        "unrelated_error_stays_unclassified",
			in:          os.ErrPermission,
			wantNoSpace: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyWriteErr(tc.in)
			if tc.in == nil {
				assert.NoError(t, got)
				return
			}
			assert.Error(t, got)
			if tc.wantNoSpace {
				assert.ErrorIs(t, got, types.ErrNoSpace,
					"want types.ErrNoSpace for %v, got %v", tc.in, got)
			} else {
				assert.NotErrorIs(t, got, types.ErrNoSpace,
					"unexpected types.ErrNoSpace for %v", tc.in)
			}
		})
	}
}

// TestWriteRealDiskFullMapsToNoSpace exercises Write's real error path against
// a size-capped tmpfs. Requires CAP_SYS_ADMIN to mount tmpfs; skips rather than
// failing when unavailable.
func TestWriteRealDiskFullMapsToNoSpace(t *testing.T) {
	mountPoint := t.TempDir()

	if err := syscall.Mount("tmpfs", mountPoint, "tmpfs", 0, "size=65536"); err != nil {
		t.Skipf("cannot mount a size-capped tmpfs (need CAP_SYS_ADMIN): %v", err)
	}
	t.Cleanup(func() {
		_ = syscall.Unmount(mountPoint, 0)
	})

	backend := New(FileConfig{
		VolumeName: "enospc-vol",
		VolumeSize: 1024 * 1024 * 1024,
		BaseDir:    mountPoint,
	})
	require.NoError(t, backend.Init())

	headers := []byte("hdr")
	data := make([]byte, 8192)

	var writeErr error
	for i := range uint64(64) {
		if writeErr = backend.Write(types.FileTypeChunk, i, &headers, &data); writeErr != nil {
			break
		}
	}

	require.Error(t, writeErr, "writes should eventually exhaust the 64KiB tmpfs")
	assert.ErrorIs(t, writeErr, types.ErrNoSpace,
		"a real disk-full write error must classify as types.ErrNoSpace, got: %v", writeErr)
	assert.ErrorIs(t, writeErr, syscall.ENOSPC,
		"classification must preserve the underlying syscall.ENOSPC")
}
