//go:build !linux

package iosched

import "errors"

// Available reports whether io_uring is supported on the running kernel.
// Always returns false on non-Linux platforms.
func Available() bool { return false }

// URingConfig configures the io_uring scheduler.
// On non-Linux platforms, NewURingScheduler always returns an error.
type URingConfig struct {
	RingDepth  uint32
	ChanBuffer int
	BatchSize  int
	SQPOLL     bool
}

// URingScheduler is not available on non-Linux platforms.
type URingScheduler struct{}

// NewURingScheduler returns an error on non-Linux platforms.
func NewURingScheduler(_ URingConfig) (*URingScheduler, error) {
	return nil, errors.New("iosched: io_uring requires Linux")
}
