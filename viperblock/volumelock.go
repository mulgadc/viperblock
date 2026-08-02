package viperblock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ErrVolumeLocked is returned by AcquireVolumeLock when another opener
// already holds the volume's exclusive lock.
var ErrVolumeLocked = errors.New("viperblock: volume is locked by another opener")

// volumeLockFileName is a fixed sibling of config.json under the volume's
// local directory. Its only purpose is to be flock'd; contents are never
// read or written.
const volumeLockFileName = ".viperblock.lock"

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
	dir := filepath.Join(baseDir, volume)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("AcquireVolumeLock: create volume dir %s: %w", dir, err)
	}

	path := filepath.Join(dir, volumeLockFileName)
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
