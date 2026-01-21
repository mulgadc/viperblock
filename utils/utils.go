package utils

import "math"

// SafeInt64ToUint64 converts int64 to uint64, returning 0 if negative
func SafeInt64ToUint64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// SafeIntToUint64 converts int to uint64, returning 0 if negative
func SafeIntToUint64(v int) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// SafeUint64ToInt64 converts uint64 to int64, capping at math.MaxInt64
func SafeUint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// SafeUint64ToUint32 converts uint64 to uint32, capping at math.MaxUint32
func SafeUint64ToUint32(v uint64) uint32 {
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

// SafeUint64ToInt converts uint64 to int, capping at math.MaxInt
func SafeUint64ToInt(v uint64) int {
	if v > math.MaxInt {
		return math.MaxInt
	}
	return int(v)
}

// SafeInt64ToUint32 converts int64 to uint32, returning 0 if negative and capping at math.MaxUint32
func SafeInt64ToUint32(v int64) uint32 {
	if v < 0 {
		return 0
	}
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

// SafeIntToUint32 converts int to uint32, returning 0 if negative
func SafeIntToUint32(v int) uint32 {
	if v < 0 {
		return 0
	}
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

// SafeIntToUint16 converts int to uint16, returning 0 if negative and capping at math.MaxUint16
func SafeIntToUint16(v int) uint16 {
	if v < 0 {
		return 0
	}
	if v > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(v)
}
