package viperblock

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/viperblock/viperblock/backends/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLifecycleTestVB builds a VB with both background goroutines running on a
// fast cadence, so a test can stop them without waiting on production
// intervals. New() starts the syncer and the uploader itself.
func newLifecycleTestVB(t *testing.T) *VB {
	t.Helper()

	tmpDir := t.TempDir()
	testVol := fmt.Sprintf("test_bg_lifecycle_%d", time.Now().UnixNano())

	backendConfig := file.FileConfig{
		VolumeName: testVol,
		VolumeSize: volumeSize,
		BaseDir:    tmpDir,
	}

	vbconfig := VB{
		VolumeName:          testVol,
		VolumeSize:          volumeSize,
		BaseDir:             fmt.Sprintf("%s/%s", tmpDir, "viperblock"),
		WALSyncInterval:     5 * time.Millisecond,
		ChunkUploadInterval: 10 * time.Millisecond,
	}

	vb, err := New(&vbconfig, FileBackend, backendConfig)
	require.NoError(t, err)

	require.NoError(t, vb.Backend.Init())
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s/wal/chunks/wal.%08d.bin",
		vb.BaseDir, vb.GetVolume(), vb.WAL.WallNum.Load())))

	t.Cleanup(func() {
		vb.StopChunkUploader()
		vb.StopWALSyncer()
	})

	return vb
}

// Both goroutines must actually be running, or every test below passes
// vacuously against a VB that had nothing to stop.
func requireBackgroundRunning(t *testing.T, vb *VB) {
	t.Helper()
	require.NotNil(t, vb.chunkUploadStop, "chunk uploader should be running")
	require.NotNil(t, vb.walSyncStop, "WAL syncer should be running")
}

// The regression gate. On dev both stoppers are an unsynchronised
// check-then-act, so racing callers either close an already-closed channel or
// receive from one a competing caller has nil'd. Run under -race.
func TestBackgroundLifecycle_ConcurrentStopIsSafe(t *testing.T) {
	vb := newLifecycleTestVB(t)
	requireBackgroundRunning(t, vb)

	const stoppers = 16

	// Release every goroutine at once so they collide inside the stop
	// functions rather than arriving in a queue.
	start := make(chan struct{})
	var wg sync.WaitGroup

	for range stoppers {
		wg.Go(func() {
			<-start
			vb.StopChunkUploader()
			vb.StopWALSyncer()
		})
	}
	close(start)

	// A nil-channel receive in the buggy version hangs forever, so bound the
	// wait rather than letting the whole suite time out on it.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent StopChunkUploader/StopWALSyncer did not all return: a caller is blocked on a nil channel")
	}

	assert.Nil(t, vb.chunkUploadStop, "uploader fields should be cleared after stop")
	assert.Nil(t, vb.walSyncStop, "syncer fields should be cleared after stop")
}

func TestBackgroundLifecycle_StopIsIdempotent(t *testing.T) {
	vb := newLifecycleTestVB(t)
	requireBackgroundRunning(t, vb)

	for range 3 {
		vb.StopChunkUploader()
		vb.StopWALSyncer()
	}

	assert.Nil(t, vb.chunkUploadStop)
	assert.Nil(t, vb.chunkUploadDone)
	assert.Nil(t, vb.chunkUploadTicker)
	assert.Nil(t, vb.gcTicker)
	assert.Nil(t, vb.walSyncStop)
	assert.Nil(t, vb.walSyncDone)
	assert.Nil(t, vb.walSyncTicker)
}

func TestBackgroundLifecycle_RestartAfterStop(t *testing.T) {
	vb := newLifecycleTestVB(t)
	requireBackgroundRunning(t, vb)

	vb.StopChunkUploader()
	vb.StopWALSyncer()

	vb.StartChunkUploader()
	vb.StartWALSyncer()
	requireBackgroundRunning(t, vb)

	// A second start must be a no-op, not a second goroutine: the orphan would
	// keep running against fields the next stop nils.
	uploaderStop, syncerStop := vb.chunkUploadStop, vb.walSyncStop
	vb.StartChunkUploader()
	vb.StartWALSyncer()
	assert.Equal(t, uploaderStop, vb.chunkUploadStop, "second StartChunkUploader replaced the running goroutine's stop channel")
	assert.Equal(t, syncerStop, vb.walSyncStop, "second StartWALSyncer replaced the running goroutine's stop channel")

	vb.StopChunkUploader()
	vb.StopWALSyncer()
	assert.Nil(t, vb.chunkUploadStop)
	assert.Nil(t, vb.walSyncStop)
}

// Stop must mean stopped, not merely signalled: callers such as
// viperblockd.shutdownVolumes flush the WAL immediately afterwards.
func TestBackgroundLifecycle_StopHaltsTheGoroutine(t *testing.T) {
	vb := newLifecycleTestVB(t)
	requireBackgroundRunning(t, vb)

	// Captured before the stop nils them — these are the goroutines' own exit
	// signals, so a closed channel here is direct proof each one returned.
	uploaderDone, syncerDone := vb.chunkUploadDone, vb.walSyncDone

	vb.StopChunkUploader()
	vb.StopWALSyncer()

	for name, ch := range map[string]chan struct{}{"uploader": uploaderDone, "syncer": syncerDone} {
		select {
		case <-ch:
		default:
			t.Fatalf("%s goroutine still running after its stop returned", name)
		}
	}

	// And it stays stopped: writes after the stop must not be drained by a
	// goroutine that outlived it.
	objectsAfterStop := vb.ObjectNum.Load()

	data := make([]byte, DefaultBlockSize)
	copy(data, "post-stop write")
	require.NoError(t, vb.WriteAt(0, data))

	time.Sleep(100 * time.Millisecond) // 10x ChunkUploadInterval

	assert.Equal(t, objectsAfterStop, vb.ObjectNum.Load(), "a drain ran after the uploader was stopped")
}

// Close stops both goroutines itself, which is the overlap that bites in
// production: a NATS unmount handler calls Close while the SIGTERM sweep is
// stopping the same VB.
func TestBackgroundLifecycle_StopRacesClose(t *testing.T) {
	vb := newLifecycleTestVB(t)
	requireBackgroundRunning(t, vb)

	data := make([]byte, DefaultBlockSize)
	copy(data, "close race")
	require.NoError(t, vb.WriteAt(0, data))

	start := make(chan struct{})
	var wg sync.WaitGroup
	var closeErr error

	wg.Go(func() {
		<-start
		closeErr = vb.Close()
	})
	wg.Go(func() {
		<-start
		vb.StopChunkUploader()
		vb.StopWALSyncer()
	})

	close(start)
	wg.Wait()

	require.NoError(t, closeErr, "Close failed while racing a concurrent stop")
}
