package iosched

import "syscall"

// PreadScheduler is a synchronous IOScheduler backed by pread(2).
// Zero overhead — each ReadAt call maps directly to one syscall.
type PreadScheduler struct{}

// NewPreadScheduler returns a synchronous pread-based scheduler.
func NewPreadScheduler() *PreadScheduler { return &PreadScheduler{} }

func (p *PreadScheduler) ReadAt(fd int, buf []byte, offset int64) (int, error) {
	return syscall.Pread(fd, buf, offset)
}

func (p *PreadScheduler) Close() error { return nil }
