package viperblock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// ErrVolumeLocked is returned by AcquireVolumeLock when another opener
// already holds the volume's exclusive lock.
var ErrVolumeLocked = errors.New("viperblock: volume is locked by another opener")

// volumeLockFilePrefix names the per-volume lock file, which sits in the base
// directory rather than inside the volume's own. Close removes the volume
// directory and only then releases, so a lock file inside it would be
// unlinked while still held: the next opener would create a fresh file at the
// same path and flock a different inode, and both would believe they held the
// volume exclusively. Contents are never read or written.
const volumeLockFilePrefix = ".viperblock-"

const volumeLockFileSuffix = ".lock"

// volumeLockPath is where the lock file for one volume lives.
func volumeLockPath(baseDir, volume string) string {
	return filepath.Join(baseDir, volumeLockFilePrefix+volume+volumeLockFileSuffix)
}

// AcquireVolumeLock takes an exclusive, non-blocking flock on a per-volume
// lock file, admitting only one caller through New..OpenWAL at a time. This
// is storage-layer defence-in-depth, not control-plane ownership.
//
// flock is per-open-file-description, so same-process callers (nbdkit's
// parallel thread model) contend like separate processes would. The kernel
// drops it on last-fd-close or process death, so crashes can't wedge the
// lock -- unlike a PID lockfile, which needs its own staleness detection.
//
// Returns the open, locked file; caller releases it via ReleaseVolumeLock.
// Non-blocking: a lock already held fails fast with ErrVolumeLocked.
func AcquireVolumeLock(baseDir, volume string) (*os.File, error) {
	return AcquireVolumeLockWait(baseDir, volume, 0)
}

// volumeLockPollInterval is how often AcquireVolumeLockWait retries. flock's
// blocking mode cannot be cancelled once entered, so waiting is a poll.
const volumeLockPollInterval = 20 * time.Millisecond

// AcquireVolumeLockWait is AcquireVolumeLock with a bound on how long it will
// wait for a lock another opener still holds. It exists for handover: the
// previous holder releases only after its drain and close, so an arriving
// opener that would succeed a moment later is otherwise refused outright.
//
// Exclusion is unchanged — only one caller ever holds the lock. wait <= 0 is
// the non-blocking form. On expiry the error is still ErrVolumeLocked.
func AcquireVolumeLockWait(baseDir, volume string, wait time.Duration) (*os.File, error) {
	deadline := time.Now().Add(wait)
	for {
		f, err := tryAcquireVolumeLock(baseDir, volume)
		if err == nil || !errors.Is(err, ErrVolumeLocked) || !time.Now().Before(deadline) {
			return f, err
		}
		time.Sleep(volumeLockPollInterval)
	}
}

func tryAcquireVolumeLock(baseDir, volume string) (*os.File, error) {
	if err := os.MkdirAll(baseDir, 0750); err != nil {
		return nil, fmt.Errorf("AcquireVolumeLock: create base dir %s: %w", baseDir, err)
	}

	path := volumeLockPath(baseDir, volume)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("AcquireVolumeLock: open %s: %w", path, err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: volume %q, lock file %s", ErrVolumeLocked, volume, path)
		}
		return nil, fmt.Errorf("AcquireVolumeLock: flock %s: %w", path, err)
	}

	return f, nil
}

// ReleaseVolumeLock unlocks and closes a lock file returned by
// AcquireVolumeLock. Safe to call with nil, so callers can defer it
// unconditionally on every return path, including ones where the lock was
// never acquired.
func ReleaseVolumeLock(f *os.File) error {
	if f == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	closeErr := f.Close()
	if unlockErr != nil {
		return fmt.Errorf("ReleaseVolumeLock: unlock: %w", unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("ReleaseVolumeLock: close: %w", closeErr)
	}
	return nil
}
