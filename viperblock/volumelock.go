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
// lock file under baseDir/volume, so at most one caller at a time can
// proceed through the New -> LoadState -> ... -> OpenWAL sequence for a
// given volume. This is defence-in-depth at the storage layer: it does not
// replace control-plane single-owner-per-volume enforcement, it stops a
// second local opener from tearing WAL/state files underneath the first.
//
// flock is per-open-file-description, so two separate callers in the SAME
// process (nbdkit's parallel thread model admitting concurrent connections)
// contend for the lock exactly like two different processes would — this is
// the proven repro vector for mulga-w1iu8. The kernel drops the lock
// automatically when every file descriptor referencing this open file
// description closes, including on process death, so a crashed holder can
// never leave a stale lock that wedges the next attach. That property is why
// flock is used here instead of a PID lockfile, which needs its own
// (fallible) staleness-detection logic.
//
// Returns the open, locked *os.File; the caller must keep it open for the
// lifetime of the volume and release it via ReleaseVolumeLock. Does not
// block: a lock already held by another opener fails fast with
// ErrVolumeLocked rather than queuing.
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
