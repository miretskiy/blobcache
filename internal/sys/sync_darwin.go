//go:build darwin

package sys

// OpenFlags returns the OS-specific flags to add to the open() call.
// On Darwin, FlDirectIO is handled via fcntl (F_NOCACHE) after open.
// FlDSync/FlSync are not supported at open time; explicit Fdatasync/Sync is needed.
func (f OpenFlag) OpenFlags() int {
	// Darwin doesn't support O_DIRECT, O_DSYNC, or O_SYNC at open time.
	// FlDirectIO is applied via fcntl(F_NOCACHE) in OpenDirect.
	return 0
}
