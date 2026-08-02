//go:build !unix

package viperblock

// walDeviceFreeBytes has no portable implementation off unix. Zero means
// "unknown", so the backpressure budget is left at its configured value.
func walDeviceFreeBytes(_ string) uint64 { return 0 }
