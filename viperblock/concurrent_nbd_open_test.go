// Repro + regression coverage for mulga-w1iu8: concurrent raw NBD connections
// against the SAME nbdkit socket race independent viperblock.New()/
// LoadState()/EnsureVolumeUUID() sequences.
//
// nbdkit's Go plugin wrapper defaults every plugin to
// NBDKIT_THREAD_MODEL_PARALLEL (nbd/libguestfs.org/nbdkit/nbdkit.go:535) and
// the viperblock plugin (nbd/viperblock.go) never overrides ThreadModel().
// CanMultiConn() returning false only affects the cache-coherency flag nbdkit
// reports to the NBD client -- it does NOT admission-control connections.
// So N simultaneous client connections (e.g. concurrent qemu-io processes,
// exactly the bead's repro steps) drove N concurrent calls to
// ViperBlockPlugin.Open(), each constructing an independent *viperblock.VB
// against the identical BaseDir + volume name, with no shared lock, lease, or
// exclusivity between them.
//
// Three defects combined to produce the corruption:
//  1. writeFileAtomic (viperblock.go) used a FIXED "path.tmp" scratch name,
//     so racing persistStateLocal calls tore each other's config.json.
//  2. openWALLocked appended a fresh WAL header on every open, even to a
//     WAL another racer had already header-stamped, desyncing the record
//     stream (see wal_double_open_test.go for the isolated regression).
//  3. Nothing enforced single-writer admission: two independently
//     constructed *VB values could run New -> LoadState -> ... -> OpenWAL
//     concurrently with no shared lock between them.
//
// Fixing 1 and 2 alone is not sufficient: two live writers still race
// last-writer-wins renames of VBState, so residual live AEAD failures
// persisted (~15% of rounds, measured while validating this fix, down from
// the original ~5.8%-and-climbing-with-conn-count baseline). Only adding the
// admission-control lock (AcquireVolumeLock/ReleaseVolumeLock, the same
// primitive nbd/viperblock.go's ViperBlockPlugin.Open/Close now wrap around
// the New()..OpenWAL() sequence) eliminates the race outright, which is why
// this test wraps every simulated "NBD connection" attempt with the same
// lock nbdkit's Open() takes. See volumelock_test.go for the lock primitive
// tested in isolation, and encryption_test.go's
// TestWriteFileAtomic_ConcurrentWritersNeverTear for defect 1 in isolation.
package viperblock

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/mulgadc/viperblock/types"
	"github.com/mulgadc/viperblock/viperblock/backends/file"
)

// nbdOpenRound seeds one fresh volume (mirroring CreateVolume's New+Init+
// SaveState) then fires numConns concurrent "NBD connection" Opens at it,
// each gated by AcquireVolumeLock exactly as nbd/viperblock.go's Open() now
// gates the real sequence. Returns how many connections were admitted, how
// many were cleanly refused admission (ErrVolumeLocked), and how many
// observed ErrIntegrity directly, plus whether the subsequent cold reopen
// (the bead's "reopen shortly after" step) still authenticates.
func nbdOpenRound(t *testing.T, volName string, numConns int) (admitted, refused, integrityErrs int, coldErr error) {
	t.Helper()
	dir := t.TempDir()
	key := testKey(t, 0x42)
	const volSize = 4 * 1024 * 1024

	seedCfg := file.FileConfig{BaseDir: dir, VolumeName: volName}
	seedVB, err := New(&VB{
		VolumeName:        volName,
		VolumeSize:        volSize,
		BaseDir:           dir,
		MasterKey:         key,
		EncryptionEnabled: true,
	}, "file", seedCfg)
	if err != nil {
		t.Fatalf("seed New: %v", err)
	}
	if err := seedVB.Backend.Init(); err != nil {
		t.Fatalf("seed Backend.Init: %v", err)
	}
	if err := seedVB.SaveState(); err != nil {
		t.Fatalf("seed SaveState: %v", err)
	}
	seedVB.StopChunkUploader()
	seedVB.StopWALSyncer()

	var wg sync.WaitGroup
	errs := make([]error, numConns)
	admittedFlags := make([]bool, numConns)

	for i := range numConns {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// The admission-control gate every real NBD connection now goes
			// through in ViperBlockPlugin.Open() before New() runs.
			lock, lockErr := AcquireVolumeLock(dir, volName)
			if lockErr != nil {
				if errors.Is(lockErr, ErrVolumeLocked) {
					return // cleanly refused admission -- expected outcome
				}
				errs[idx] = fmt.Errorf("AcquireVolumeLock: %w", lockErr)
				return
			}
			admittedFlags[idx] = true
			defer func() { _ = ReleaseVolumeLock(lock) }()

			cfg := file.FileConfig{BaseDir: dir, VolumeName: volName}
			vbcfg := VB{
				VolumeName:        volName,
				VolumeSize:        volSize,
				BaseDir:           dir,
				MasterKey:         key,
				EncryptionEnabled: true,
				Role:              "nbdkit",
			}
			vb, err := New(&vbcfg, "file", cfg)
			if err != nil {
				errs[idx] = fmt.Errorf("New: %w", err)
				return
			}
			if err := vb.Backend.Init(); err != nil {
				errs[idx] = fmt.Errorf("Backend.Init: %w", err)
				return
			}
			if err := vb.LoadState(); err != nil {
				errs[idx] = fmt.Errorf("LoadState: %w", err)
				return
			}
			if err := vb.EnsureVolumeUUID(); err != nil {
				errs[idx] = fmt.Errorf("EnsureVolumeUUID: %w", err)
				return
			}
			if err := vb.LoadLiveCheckpoint(); err != nil {
				errs[idx] = fmt.Errorf("LoadLiveCheckpoint: %w", err)
				return
			}
			if err := vb.RecoverLocalWALs(); err != nil {
				errs[idx] = fmt.Errorf("RecoverLocalWALs: %w", err)
				return
			}
			walNum := vb.WAL.WallNum.Add(1)
			if err := vb.OpenWAL(&vb.WAL, fmt.Sprintf("%s/%s", vb.WAL.BaseDir,
				types.GetFilePath(types.FileTypeWALChunk, walNum, vb.GetVolume()))); err != nil {
				errs[idx] = fmt.Errorf("OpenWAL: %w", err)
				return
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if admittedFlags[i] {
			admitted++
		} else if err == nil {
			refused++
		}
		if err != nil && errors.Is(err, ErrIntegrity) {
			integrityErrs++
			t.Logf("round %s connection %d: %v", volName, i, err)
		}
	}

	cfg := file.FileConfig{BaseDir: dir, VolumeName: volName}
	cold, err := New(&VB{
		VolumeName:        volName,
		VolumeSize:        volSize,
		BaseDir:           dir,
		MasterKey:         key,
		EncryptionEnabled: true,
	}, "file", cfg)
	if err != nil {
		t.Fatalf("cold New: %v", err)
	}
	if err := cold.Backend.Init(); err != nil {
		t.Fatalf("cold Backend.Init: %v", err)
	}
	coldErr = cold.LoadState()
	return admitted, refused, integrityErrs, coldErr
}

// TestConcurrentNBDConnections_RaceOpenSaveState runs many independent rounds
// (fresh volume each time) of numConns concurrent NBD-style opens and
// asserts the fixed sequence -- New..OpenWAL gated by AcquireVolumeLock,
// mirroring the real ViperBlockPlugin.Open()/Close() -- never produces an
// observable AEAD/integrity failure, live or on a subsequent cold reopen
// (the bead's literal repro: "Reopen the volume shortly after").
func TestConcurrentNBDConnections_RaceOpenSaveState(t *testing.T) {
	const rounds = 40
	const numConns = 8

	roundsWithLiveIntegrityErr := 0
	roundsWithColdIntegrityErr := 0
	totalIntegrityErrs := 0
	totalAdmitted := 0
	totalRefused := 0

	for r := range rounds {
		volName := fmt.Sprintf("vol-concurrent-nbd-%d", r)
		admitted, refused, integrityErrs, coldErr := nbdOpenRound(t, volName, numConns)
		totalAdmitted += admitted
		totalRefused += refused
		if admitted+refused != numConns {
			t.Fatalf("round %s: %d admitted + %d refused != %d connections -- some connection neither succeeded nor was cleanly refused",
				volName, admitted, refused, numConns)
		}
		if admitted == 0 {
			t.Fatalf("round %s: no connection was ever admitted -- the lock wedged every opener", volName)
		}
		if integrityErrs > 0 {
			roundsWithLiveIntegrityErr++
			totalIntegrityErrs += integrityErrs
		}
		if coldErr != nil && errors.Is(coldErr, ErrIntegrity) {
			roundsWithColdIntegrityErr++
			t.Logf("round %s: cold reopen failed to authenticate: %v", volName, coldErr)
		}
	}

	t.Logf("mulga-w1iu8: %d/%d rounds hit a LIVE AEAD/integrity failure "+
		"during concurrent open (%d total), %d/%d rounds left a COLD-UNREADABLE "+
		"config.json behind -- %d connections admitted, %d cleanly refused across %d rounds",
		roundsWithLiveIntegrityErr, rounds, totalIntegrityErrs,
		roundsWithColdIntegrityErr, rounds, totalAdmitted, totalRefused, rounds)

	if roundsWithLiveIntegrityErr > 0 || roundsWithColdIntegrityErr > 0 {
		t.Fatalf("mulga-w1iu8 REGRESSED: concurrent NBD-style opens against one volume "+
			"produced an AEAD/integrity failure in %d/%d rounds (live) and %d/%d rounds "+
			"(cold reopen)", roundsWithLiveIntegrityErr, rounds, roundsWithColdIntegrityErr, rounds)
	}
}
