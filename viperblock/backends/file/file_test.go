package file

import (
	"bytes"
	"context"
	"errors"
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
	backend, err := New(FileConfig{
		VolumeName: "test-vol",
		VolumeSize: 1024 * 1024,
		BaseDir:    t.TempDir(),
	})
	require.NoError(t, err)
	require.NoError(t, backend.Init())
	return backend
}

func TestNewDefaultsLogger(t *testing.T) {
	backend, err := New(FileConfig{VolumeName: "v", VolumeSize: 1, BaseDir: t.TempDir()})
	require.NoError(t, err)
	assert.NotNil(t, backend.log, "New must default the instance logger")
	assert.Same(t, slog.Default(), backend.log)
}

func TestNewWrongConfigTypeReturnsError(t *testing.T) {
	backend, err := New("not-a-file-config")
	require.ErrorIs(t, err, types.ErrBackendConfig)
	assert.Nil(t, backend)
}

func TestSetLogger(t *testing.T) {
	backend, err := New(FileConfig{VolumeName: "v", VolumeSize: 1, BaseDir: t.TempDir()})
	require.NoError(t, err)

	var buf bytes.Buffer
	custom := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	backend.SetLogger(custom)
	assert.Same(t, custom, backend.log)

	// nil resets to slog.Default() rather than leaving the backend loggerless.
	backend.SetLogger(nil)
	assert.Same(t, slog.Default(), backend.log)
}

func TestInitAndInitCtx(t *testing.T) {
	backend, err := New(FileConfig{VolumeName: "v", VolumeSize: 1, BaseDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, backend.Init())

	backend2, err := New(FileConfig{VolumeName: "v2", VolumeSize: 1, BaseDir: t.TempDir()})
	require.NoError(t, err)
	require.NoError(t, backend2.InitCtx(context.Background()))
}

func TestInitMissingBaseDirFails(t *testing.T) {
	backend, err := New(FileConfig{VolumeName: "v", VolumeSize: 1, BaseDir: "/nonexistent/base/dir"})
	require.NoError(t, err)
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

	require.NoError(t, backend.SetConfig(FileConfig{VolumeName: "renamed", VolumeSize: 2, BaseDir: t.TempDir()}))
	assert.Equal(t, "renamed", backend.config.VolumeName)
}

// TestSetConfigReturnsErrorOnWrongConfigType pins that a config value of the
// wrong concrete type fails the one call. SetConfig takes any, so the mismatch
// is only detectable at runtime; it used to panic, which would have handed the
// first production caller a process kill as the method's only failure mode.
func TestSetConfigReturnsErrorOnWrongConfigType(t *testing.T) {
	backend := newTestBackend(t)

	var err error
	require.NotPanics(t, func() {
		err = backend.SetConfig("not-a-file-config")
	})
	require.ErrorIs(t, err, types.ErrBackendConfig)
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
	backend, err := New(FileConfig{VolumeName: "v", VolumeSize: 1, BaseDir: filepath.Join(t.TempDir(), "does-not-exist")})
	require.NoError(t, err)

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

	backend, err := New(FileConfig{
		VolumeName: "enospc-vol",
		VolumeSize: 1024 * 1024 * 1024,
		BaseDir:    mountPoint,
	})
	require.NoError(t, err)
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

// TestWriteReplacesTheObjectAtomically covers the property reclaim depends on.
// The previous path truncated the destination with os.Create and wrote into
// it, so between the truncate and the last write the object at that key was
// short, and a failure there left it that way. A caller that deletes its local
// WAL on a successful Write has no second copy to fall back on.
//
// Replacement by rename is observable as a change of inode; truncate in place
// is not. fsync itself is not observable without a power cut, so this gates
// the atomicity and the write path carries the sync alongside it.
func TestWriteReplacesTheObjectAtomically(t *testing.T) {
	backend := newTestBackend(t)

	headers := []byte("hdr!")
	first := []byte("the original object")
	require.NoError(t, backend.Write(types.FileTypeChunk, 7, &headers, &first))

	name := filepath.Join(backend.config.BaseDir, types.GetFilePath(types.FileTypeChunk, 7, backend.config.VolumeName))
	before, err := os.Stat(name)
	require.NoError(t, err)

	second := []byte("the replacement object, longer than the first")
	require.NoError(t, backend.Write(types.FileTypeChunk, 7, &headers, &second))

	after, err := os.Stat(name)
	require.NoError(t, err)
	assert.False(t, os.SameFile(before, after),
		"a rewrite must publish a new file by rename: truncating the destination in place leaves the object short until the last write lands")

	got, err := backend.Read(types.FileTypeChunk, 7, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, append(append([]byte{}, headers...), second...), got)

	entries, err := os.ReadDir(filepath.Dir(name))
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".tmp", "a successful write must leave no temporary file behind")
	}
}

// TestWriteFailsWhenTheDirectorySyncFails is the gate on Write propagating
// that failure rather than logging it. The object is visible after the rename
// whether or not the directory fsync lands, so a Write that returns success
// there tells its caller the bytes are durable when they are not. The caller
// then deletes its local WAL, and a power cut takes both copies.
//
// Injected, because a healthy filesystem does not fail this call.
func TestWriteFailsWhenTheDirectorySyncFails(t *testing.T) {
	backend := newTestBackend(t)

	headers := []byte("hdr!")
	original := []byte("the durable original")
	require.NoError(t, backend.Write(types.FileTypeChunk, 3, &headers, &original))

	injected := errors.New("injected directory sync failure")
	restore := syncDirectory
	syncDirectory = func(string) error { return injected }
	t.Cleanup(func() { syncDirectory = restore })

	replacement := []byte("a write whose rename is not on the device")
	err := backend.Write(types.FileTypeChunk, 3, &headers, &replacement)
	require.Error(t, err, "a failed directory fsync must fail the write: reporting success is what lets the caller delete its only other copy")
	assert.ErrorIs(t, err, injected)

	// No temporary file is left behind, and the object that was already
	// durable is still readable.
	syncDirectory = restore
	name := filepath.Join(backend.config.BaseDir, types.GetFilePath(types.FileTypeChunk, 3, backend.config.VolumeName))
	entries, readErr := os.ReadDir(filepath.Dir(name))
	require.NoError(t, readErr)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".tmp", "a failed write must not leave a temporary file behind")
	}
	got, readErr := backend.Read(types.FileTypeChunk, 3, 0, 0)
	require.NoError(t, readErr)
	assert.Equal(t, append(append([]byte{}, headers...), replacement...), got,
		"the rename itself succeeded, so the new bytes are visible; what failed is the promise that they are on the device")
}

// TestInitFailsWhenADirectorySyncFails gates the directory entries Init
// creates being durable before anything writes into them. Write fsyncs the
// directory it renames an object into, which is no help if that directory's
// own entry is still only in page cache: a power cut takes the subtree, and
// the caller has already reclaimed its local WAL on Write's success.
//
// The injection is the only way to produce the failure, since a healthy
// filesystem does not fail fsync on a directory it just created.
func TestInitFailsWhenADirectorySyncFails(t *testing.T) {
	base := t.TempDir()
	injected := errors.New("injected directory sync failure")

	synced := []string{}
	original := syncDirectory
	syncDirectory = func(dir string) error {
		synced = append(synced, dir)
		return injected
	}
	t.Cleanup(func() { syncDirectory = original })

	backend, err := New(FileConfig{BaseDir: base, VolumeName: "init-sync", VolumeSize: 1})
	require.NoError(t, err)

	err = backend.Init()
	assert.ErrorIs(t, err, injected,
		"a directory whose entry is not durable must fail Init rather than let a volume open on it")
	assert.Equal(t, []string{base}, synced,
		"the first failure must stop Init rather than continue past it")
}

// TestInitSyncsTheParentOfEveryDirectoryItCreates names the three directories
// whose entries hold the rest: the base holds the volume, the volume holds
// chunks, checkpoints and wal, and wal holds its own two.
func TestInitSyncsTheParentOfEveryDirectoryItCreates(t *testing.T) {
	base := t.TempDir()

	synced := []string{}
	original := syncDirectory
	syncDirectory = func(dir string) error {
		synced = append(synced, dir)
		return original(dir)
	}
	t.Cleanup(func() { syncDirectory = original })

	backend, err := New(FileConfig{BaseDir: base, VolumeName: "init-sync-all", VolumeSize: 1})
	require.NoError(t, err)
	require.NoError(t, backend.Init())

	vol := filepath.Join(base, "init-sync-all")
	assert.Equal(t, []string{base, vol, filepath.Join(vol, "wal")}, synced)
}
