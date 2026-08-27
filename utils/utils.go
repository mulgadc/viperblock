package utils

import (
	"fmt"
)

// HumanBytes formats a byte count using IEC binary suffixes (KiB, MiB, ...).
// Values below 1024 render as exact bytes. It is a pure formatter carrying no
// terminal-UI dependency, so progress callbacks can render byte counts without
// pulling a rendering library into the storage module.
func HumanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPEZY"[exp])
}
