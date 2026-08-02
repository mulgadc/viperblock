//go:build unix

package viperblock

import "golang.org/x/sys/unix"

// walDeviceFreeBytes reports the space available to an unprivileged writer on
// the filesystem holding path. Zero means "unknown" -- callers must treat that
// as "do not clamp" rather than as "no space".
func walDeviceFreeBytes(path string) uint64 {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0
	}
	if st.Bsize <= 0 {
		return 0
	}
	return st.Bavail * uint64(st.Bsize)
}
