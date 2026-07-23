package utils

import (
	"fmt"
	"math"
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

// SafeInt64ToUint64 converts int64 to uint64, returning 0 if negative.
func SafeInt64ToUint64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// SafeIntToUint64 converts int to uint64, returning 0 if negative.
func SafeIntToUint64(v int) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// SafeUint64ToInt64 converts uint64 to int64, capping at math.MaxInt64.
func SafeUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// SafeUint64ToUint32 converts uint64 to uint32, capping at math.MaxUint32.
func SafeUint64ToUint32(v uint64) uint32 {
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

// SafeUint64ToInt converts uint64 to int, capping at math.MaxInt.
func SafeUint64ToInt(v uint64) int {
	if v > math.MaxInt {
		return math.MaxInt
	}
	return int(v)
}

// SafeInt64ToUint32 converts int64 to uint32, returning 0 if negative and capping at math.MaxUint32.
func SafeInt64ToUint32(v int64) uint32 {
	if v < 0 {
		return 0
	}
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

// SafeIntToUint32 converts int to uint32, returning 0 if negative.
func SafeIntToUint32(v int) uint32 {
	if v < 0 {
		return 0
	}
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

// SafeIntToUint16 converts int to uint16, returning 0 if negative and capping at math.MaxUint16.
func SafeIntToUint16(v int) uint16 {
	if v < 0 {
		return 0
	}
	if v > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(v)
}
