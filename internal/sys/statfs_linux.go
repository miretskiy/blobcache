//go:build linux

package sys

import "golang.org/x/sys/unix"

// Statfs returns the available and total bytes for the filesystem containing path.
func Statfs(path string) (available, total uint64, err error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	bsize := uint64(stat.Bsize)
	return stat.Bavail * bsize, stat.Blocks * bsize, nil
}
