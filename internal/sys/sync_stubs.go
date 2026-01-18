//go:build !linux && !darwin

package sys

// OpenFlags returns the OS-specific flags to add to the open() call.
// On unsupported platforms, no special flags are available.
// The caller must explicitly call Fdatasync after writes.
func (f OpenFlag) OpenFlags() int {
	return 0
}
