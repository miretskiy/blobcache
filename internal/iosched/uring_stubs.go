//go:build !linux

package iosched

import "errors"

// IOUringAvailable reports whether io_uring is supported on the running
// platform and kernel. Always false on non-Linux.
const IOUringAvailable = false

// NewURingScheduler returns an error on non-Linux platforms.
func NewURingScheduler(_ URingConfig) (IOScheduler, error) {
	return nil, errors.New("iosched: io_uring requires Linux")
}
