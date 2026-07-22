//go:build integration

package viperblock

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/s3"
	"github.com/stretchr/testify/require"
)

// These tests model the deployed topology: viperblock runs BOTH as the nbdkit
// plugin (owning the guest data path) and as a library inside the spinifex
// daemon, in a DIFFERENT PROCESS, against the SAME volume and the SAME shared
// local BaseDir. Every guard viperblock has -- createChunkFile's SeqNum check,
// drainMu, CanMultiConn=false -- is an in-process construct and is invisible
// across that boundary. There is no lock file, lease, or handshake that
// establishes ownership of a volume.
//
// Two independent *VB values in one test process are a faithful model: they
// share no mutexes, no atomics and no maps, exactly like two processes.

func dualOpenVB(t *testing.T, vol string, volSize uint64, baseDir string, key *masterkey.Key, chunkUpload time.Duration, cacheSize int) *VB {
	t.Helper()
	cfg := VB{
		VolumeName:          vol,
		VolumeSize:          volSize,
		BaseDir:             baseDir,
		WALSyncInterval:     -1,
		ChunkUploadInterval: chunkUpload,
		MaxPendingBytes:     2 * 1024 * 1024,
		UploadWorkers:       4,
		MasterKey:           key,
		EncryptionEnabled:   key != nil,
		Cache:               Cache{Config: CacheConfig{Size: cacheSize}},
	}
	bcfg := s3.S3Config{
		VolumeName: vol, VolumeSize: volSize, Region: "ap-southeast-2",
		Bucket: sharedBucket, AccessKey: AccessKey, SecretKey: SecretKey,
		Host: fmt.Sprintf("https://%s", sharedServerHost), HTTPClient: testHTTPClient,
	}
	vb, err := New(&cfg, "s3", bcfg)
	require.NoError(t, err)
	require.NoError(t, vb.Backend.Init())
	return vb
}

func openWALs(t *testing.T, vb *VB) {
	t.Helper()
	require.NoError(t, vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
		types.GetFilePath(types.FileTypeWALChunk, vb.WAL.WallNum.Load(), vb.GetVolume()))))
	require.NoError(t, vb.OpenWAL(&vb.BlockToObjectWAL, fmt.Sprintf("%s/%s", vb.BlockToObjectWAL.BaseDir,
		types.GetFilePath(types.FileTypeWALBlock, vb.BlockToObjectWAL.WallNum.Load(), vb.GetVolume()))))
}

func countRevertedBlocks(t *testing.T, want, got []byte, bs int) (int, string) {
	t.Helper()
	bad, detail := 0, ""
	for b := 0; b*bs < len(want); b++ {
		s, e := b*bs, (b+1)*bs
		if bytes.Equal(want[s:e], got[s:e]) {
			continue
		}
		bad++
		if bad <= 5 {
			lo, hi := -1, -1
			for i := s; i < e; i++ {
				if want[i] != got[i] {
					if lo < 0 {
						lo = i - s
					}
					hi = i - s
				}
			}
			allZero := true
			for i := s + lo; i <= s+hi; i++ {
				if got[i] != 0 {
					allZero = false
					break
				}
			}
			detail += fmt.Sprintf("\n  block %d (off %#x): bytes [%d,%d] wrong (%d), all-zero=%v",
				b, s, lo, hi, hi-lo+1, allZero)
		}
	}
	return bad, detail
}

// TestDualOpen_DaemonSideInstanceDuringGuestWrites models the ebs.mount
// topology: the daemon holds a second VB on the volume for the lifetime of the
// mount ("tracks state only"), while nbdkit serves guest writes. The daemon VB
// stops its chunk uploader -- viperblockd does this deliberately, with the
// comment "so it cannot overwrite the live checkpoint every 30s (AEAD
// corruption)" -- so this asserts that mitigation actually holds.
func TestDualOpen_DaemonSideInstanceDuringGuestWrites(t *testing.T) {
	const volSize = 32 * 1024 * 1024
	const total = 6 * 1024 * 1024
	const piece = 3072

	key := testKey(t, 0x42)
	vol := fmt.Sprintf("dual-mount-%d", time.Now().UnixNano())
	shared := t.TempDir() + "/viperblock" // daemon and nbdkit SHARE this

	// nbdkit-role instance.
	nbd := dualOpenVB(t, vol, volSize, shared, key, time.Millisecond, 4096)
	require.NoError(t, nbd.SaveState())
	require.NoError(t, nbd.EnsureVolumeUUID())
	openWALs(t, nbd)

	src := make([]byte, total)
	for i := range src {
		src[i] = byte(i*7 + 3)
	}
	// Seed and drain so there is a real block map on the backend.
	require.NoError(t, nbd.WriteAt(0, append([]byte(nil), src...)))
	require.NoError(t, nbd.DrainToBackend())

	// Daemon-role instance opens the SAME volume, mid-flight.
	daemon := dualOpenVB(t, vol, volSize, shared, key, 30*time.Second, 0)
	daemon.StopChunkUploader() // what viperblockd does
	require.NoError(t, daemon.LoadState())
	require.NoError(t, daemon.LoadLiveCheckpoint())

	// Guest keeps writing while the daemon instance is held open.
	for off := 0; off < total; off += piece {
		end := min(off+piece, total)
		for i := off; i < end; i++ {
			src[i] = byte(i*13 + 101)
		}
		require.NoError(t, nbd.WriteAt(uint64(off), append([]byte(nil), src[off:end]...)))
	}
	require.NoError(t, nbd.DrainToBackend())

	// Daemon instance goes away (unmount path).
	daemon.StopWALSyncer()

	nbd.StopChunkUploader()
	require.NoError(t, nbd.Close())

	cold := dualOpenVB(t, vol, volSize, t.TempDir()+"/viperblock", key, -1, 0)
	require.NoError(t, cold.LoadState())
	require.NoError(t, cold.LoadLiveCheckpoint())
	got, err := cold.ReadAt(0, total)
	require.NoError(t, err)
	if bad, detail := countRevertedBlocks(t, src, got, int(cold.BlockSize)); bad > 0 {
		t.Fatalf("DUAL-OPEN (mount topology): %d of %d blocks reverted%s",
			bad, total/int(cold.BlockSize), detail)
	}
}

// TestDualOpen_DaemonSnapshotDuringGuestWrites models snapshotVolume: the
// daemon opens a second VB, loads the live checkpoint at some instant, and
// calls CreateSnapshot -- which in viperblock runs WriteWALToChunk(true) then
// SaveLiveCheckpoint, i.e. it WRITES the source volume's live checkpoint from
// a map loaded earlier, from another process, while nbdkit keeps writing.
//
// Note viperblock.New STARTS the chunk uploader, and snapshotVolume only
// defers StopChunkUploader to function exit, so the daemon-side uploader is
// live for the whole body -- including a socket handshake with a 30s deadline.
func TestDualOpen_DaemonSnapshotDuringGuestWrites(t *testing.T) {
	const volSize = 32 * 1024 * 1024
	const total = 6 * 1024 * 1024
	const piece = 3072

	key := testKey(t, 0x42)
	vol := fmt.Sprintf("dual-snap-%d", time.Now().UnixNano())
	shared := t.TempDir() + "/viperblock"

	nbd := dualOpenVB(t, vol, volSize, shared, key, time.Millisecond, 4096)
	require.NoError(t, nbd.SaveState())
	require.NoError(t, nbd.EnsureVolumeUUID())
	openWALs(t, nbd)

	src := make([]byte, total)
	for i := range src {
		src[i] = byte(i*5 + 11)
	}
	require.NoError(t, nbd.WriteAt(0, append([]byte(nil), src...)))
	require.NoError(t, nbd.DrainToBackend())

	// Daemon opens and snapshots the map AS OF NOW.
	daemon := dualOpenVB(t, vol, volSize, shared, key, 30*time.Second, 0)
	require.NoError(t, daemon.LoadState())
	require.NoError(t, daemon.LoadLiveCheckpoint())

	// Guest writes land AFTER the daemon captured its map.
	for off := 0; off < total; off += piece {
		end := min(off+piece, total)
		for i := off; i < end; i++ {
			src[i] = byte(i*17 + 29)
		}
		require.NoError(t, nbd.WriteAt(uint64(off), append([]byte(nil), src[off:end]...)))
	}
	require.NoError(t, nbd.DrainToBackend())

	// Now the daemon publishes, from its stale map. This is the cross-process
	// stale checkpoint publish.
	_, err := daemon.CreateSnapshot(fmt.Sprintf("snap-%d", time.Now().UnixNano()))
	require.NoError(t, err)
	daemon.StopChunkUploader()
	daemon.StopWALSyncer()

	nbd.StopChunkUploader()
	require.NoError(t, nbd.Close())

	cold := dualOpenVB(t, vol, volSize, t.TempDir()+"/viperblock", key, -1, 0)
	require.NoError(t, cold.LoadState())
	require.NoError(t, cold.LoadLiveCheckpoint())
	got, err := cold.ReadAt(0, total)
	require.NoError(t, err)
	if bad, detail := countRevertedBlocks(t, src, got, int(cold.BlockSize)); bad > 0 {
		t.Fatalf("DUAL-OPEN (snapshot topology): %d of %d blocks reverted%s",
			bad, total/int(cold.BlockSize), detail)
	}
}

// TestDualOpen_DaemonRemovesSharedLocalFiles models cloneAMIToVolume, which
// ends in destVb.RemoveLocalFiles() -- an os.RemoveAll of
// {BaseDir}/{volume}. viperblockd and nbdkit SHARE that BaseDir (viperblockd
// itself notes "no process writes the shared BaseDir"), so if this lands while
// nbdkit holds that volume open, it deletes the live WAL out from under it.
func TestDualOpen_DaemonRemovesSharedLocalFiles(t *testing.T) {
	const volSize = 32 * 1024 * 1024
	const total = 4 * 1024 * 1024
	const piece = 3072

	key := testKey(t, 0x42)
	vol := fmt.Sprintf("dual-rm-%d", time.Now().UnixNano())
	shared := t.TempDir() + "/viperblock"

	nbd := dualOpenVB(t, vol, volSize, shared, key, time.Hour, 4096) // no auto-drain
	require.NoError(t, nbd.SaveState())
	require.NoError(t, nbd.EnsureVolumeUUID())
	openWALs(t, nbd)

	src := make([]byte, total)
	for i := range src {
		src[i] = byte(i*3 + 7)
	}
	// Guest writes sit in the WAL, NOT yet drained to chunks.
	for off := 0; off < total; off += piece {
		end := min(off+piece, total)
		require.NoError(t, nbd.WriteAt(uint64(off), append([]byte(nil), src[off:end]...)))
	}
	require.NoError(t, nbd.Flush()) // durable in the local WAL

	// Daemon-side clone of a DIFFERENT volume finishing its work wipes the
	// shared local dir for this one.
	daemon := dualOpenVB(t, vol, volSize, shared, key, -1, 0)
	require.NoError(t, daemon.RemoveLocalFiles())

	// nbdkit now drains what it believes is durable. The data is gone either
	// way -- the point of this test is that the loss must be SURFACED, never
	// silent. Silent loss here would be indistinguishable from the siv-482
	// field signature.
	drainErr := nbd.DrainToBackend()
	t.Logf("nbdkit drain after daemon wiped the shared WAL dir: err=%v", drainErr)
	nbd.StopChunkUploader()
	closeErr := nbd.Close()
	t.Logf("nbdkit close: err=%v", closeErr)

	require.True(t, drainErr != nil || closeErr != nil,
		"daemon deleted nbdkit's live WAL directory out from under it and NEITHER "+
			"the drain nor the close reported an error -- acknowledged writes would be "+
			"lost silently")
}

// TestDualOpen_LaunchClonePathDoesNotDirtyCheckpoint is the decisive test for
// mulga-t1sch's load-bearing claim:
//
//	"Today this does not corrupt only because SaveLiveCheckpointCtx is
//	 dirty-gated and the snapshot instance never writes."
//
// The plan reasons about the SNAPSHOT path. env19's sink evidence is about
// instance LAUNCH (handlers/ec2/instance/service_impl.go:1387 ->
// cloneAMIToVolume -> OpenFromSnapshot). This drives that path: a daemon-role
// instance runs the launch/clone sequence on a volume an nbdkit-role instance
// is actively serving, with its chunk uploader RUNNING, and is then forced to
// drain -- modelling the uploader firing before the deferred StopChunkUploader.
//
// It asserts the dirty gate actually holds on the launch path, i.e. that the
// daemon-side instance publishes NO live checkpoint and reverts nothing.
func TestDualOpen_LaunchClonePathDoesNotDirtyCheckpoint(t *testing.T) {
	const volSize = 32 * 1024 * 1024
	const total = 4 * 1024 * 1024
	const piece = 3072

	key := testKey(t, 0x42)
	stamp := time.Now().UnixNano()
	srcVol := fmt.Sprintf("launch-src-%d", stamp)
	cloneVol := fmt.Sprintf("launch-clone-%d", stamp)
	snapID := fmt.Sprintf("snap-launch-%d", stamp)
	shared := t.TempDir() + "/viperblock"

	// Build a source volume and snapshot it, so there is a real clone base.
	src := dualOpenVB(t, srcVol, volSize, t.TempDir()+"/viperblock", key, -1, 0)
	require.NoError(t, src.SaveState())
	require.NoError(t, src.EnsureVolumeUUID())
	openWALs(t, src)
	seed := make([]byte, total)
	for i := range seed {
		seed[i] = byte(i*11 + 5)
	}
	require.NoError(t, src.WriteAt(0, append([]byte(nil), seed...)))
	require.NoError(t, src.DrainToBackend())
	_, err := src.CreateSnapshot(snapID)
	require.NoError(t, err)
	src.StopChunkUploader()
	require.NoError(t, src.Close())

	// nbdkit-role instance materialises the clone and serves guest writes.
	nbd := dualOpenVB(t, cloneVol, volSize, shared, key, time.Millisecond, 4096)
	require.NoError(t, nbd.OpenFromSnapshot(snapID))
	require.NoError(t, nbd.SaveState())
	require.NoError(t, nbd.EnsureVolumeUUID())
	openWALs(t, nbd)

	want := append([]byte(nil), seed...)
	for off := 0; off < total; off += piece {
		end := min(off+piece, total)
		for i := off; i < end; i++ {
			want[i] = byte(i*23 + 91)
		}
		require.NoError(t, nbd.WriteAt(uint64(off), append([]byte(nil), want[off:end]...)))
	}
	require.NoError(t, nbd.DrainToBackend())

	// Daemon-role instance runs the LAUNCH path on the same volume. New()
	// starts a chunk uploader; prepareRootVolume/cloneAMIToVolume only defer
	// the stop, so it is live for the whole body.
	daemon := dualOpenVB(t, cloneVol, volSize, shared, key, time.Millisecond, 0)
	// New() has ALREADY started a chunk uploader at this point. Stopping it
	// before LoadState mirrors what viperblockd's ebs.mount path does
	// (viperblockd.go:773) and, separately, avoids a PRE-EXISTING data race:
	// the uploader's DrainToBackendCtx reads VB fields that LoadStateCtx
	// concurrently writes. The five control-plane call sites the EBSProvider
	// plan (mulga-t1sch) removes do NOT stop it first -- they only defer the
	// stop -- so they run that race for real. See
	// TestNewStartsUploaderBeforeLoadState_KnownRace.
	daemon.StopChunkUploader()
	require.NoError(t, daemon.LoadState()) // triggers OpenFromSnapshot internally

	dirtyAfterLoad := daemon.BlocksToObject.dirty.Load()
	t.Logf("daemon-side BlocksToObject.dirty after launch-path LoadState = %v", dirtyAfterLoad)

	// Worst case: force the drain the background uploader would have run.
	require.NoError(t, daemon.DrainToBackendCtx(t.Context()))
	dirtyAfterDrain := daemon.BlocksToObject.dirty.Load()
	t.Logf("daemon-side BlocksToObject.dirty after forced drain = %v", dirtyAfterDrain)

	daemon.StopChunkUploader()
	daemon.StopWALSyncer()

	nbd.StopChunkUploader()
	require.NoError(t, nbd.Close())

	cold := dualOpenVB(t, cloneVol, volSize, t.TempDir()+"/viperblock", key, -1, 0)
	require.NoError(t, cold.LoadState())
	require.NoError(t, cold.LoadLiveCheckpoint())
	got, err := cold.ReadAt(0, total)
	require.NoError(t, err)

	if bad, detail := countRevertedBlocks(t, want, got, int(cold.BlockSize)); bad > 0 {
		t.Fatalf("LAUNCH-PATH DUAL-OPEN: daemon-side clone instance reverted %d of %d blocks "+
			"-- the dirty gate does NOT hold on the launch path%s",
			bad, total/int(cold.BlockSize), detail)
	}
	require.False(t, dirtyAfterLoad,
		"launch-path LoadState dirtied the block map: the dirty gate would let a "+
			"daemon-side instance publish a stale live checkpoint")
}

// TestNewStartsUploaderBeforeLoadState_KnownRace documents a PRE-EXISTING data
// race, unrelated to the RMW work: viperblock.New() starts the background chunk
// uploader before the caller can call LoadState(), so the uploader's
// DrainToBackendCtx reads VB fields (UseShardedWAL, SeqNum, ObjectNum,
// VolumeUUID, ...) while LoadStateCtx is still writing them.
//
// It reproduces on a pristine tree; run with -race and remove the Skip:
//
//	WARNING: DATA RACE
//	  ...LoadStateCtx()            <- write
//	  Previous read ...DrainToBackendCtx() <- StartChunkUploader.func1()
//
// This is direct evidence for mulga-t1sch's thesis that every viperblock.New()
// is a WRITER, not a reader. viperblockd's ebs.mount path happens to be safe
// because it calls StopChunkUploader() BEFORE loading state; the five
// control-plane handlers the plan deletes only DEFER the stop, so they hold a
// live uploader across their whole body.
func TestNewStartsUploaderBeforeLoadState_KnownRace(t *testing.T) {
	t.Skip("KNOWN PRE-EXISTING RACE (evidence for mulga-t1sch): New() starts the " +
		"chunk uploader before LoadState() can run. Remove this Skip and run with " +
		"-race to observe it.")

	key := testKey(t, 0x42)
	vol := fmt.Sprintf("newrace-%d", time.Now().UnixNano())
	vb := dualOpenVB(t, vol, 32*1024*1024, t.TempDir()+"/viperblock", key, time.Millisecond, 0)
	require.NoError(t, vb.SaveState())
	// Uploader is already running here; LoadState races it.
	require.NoError(t, vb.LoadState())
	vb.StopChunkUploader()
}
