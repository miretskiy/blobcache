//go:build linux

package sys

import "golang.org/x/sys/unix"

// OpenFlags returns the OS-specific flags to add to the open() call.
// On Linux, this maps FlDirectIO to O_DIRECT and FlDSync/FlSync to O_DSYNC/O_SYNC.
func (f OpenFlag) OpenFlags() int {
	var flags int
	if f&FlDirectIO != 0 {
		flags |= unix.O_DIRECT
	}
	if f&FlDSync != 0 {
		flags |= unix.O_DSYNC
	}
	if f&FlSync != 0 {
		flags |= unix.O_SYNC
	}
	return flags
}
