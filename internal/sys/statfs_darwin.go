//go:build darwin

package sys

import "syscall"

// Statfs returns the available and total bytes for the filesystem containing path.
func Statfs(path string) (available, total uint64, err error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	bsize := uint64(stat.Bsize)
	return stat.Bavail * bsize, stat.Blocks * bsize, nil
}
