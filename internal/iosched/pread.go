package iosched

import (
	"syscall"
	"time"
)

// PreadScheduler is a synchronous IOScheduler backed by pread(2).
// Zero overhead — each ReadAt call maps directly to one syscall.
type PreadScheduler struct {
	readLatency
}

// NewPreadScheduler returns a synchronous pread-based scheduler.
func NewPreadScheduler() (*PreadScheduler, error) {
	p := &PreadScheduler{}
	p.initLatency()
	return p, nil
}

func (p *PreadScheduler) ReadAt(fd int, buf []byte, offset int64) (int, error) {
	start := time.Now()
	n, err := syscall.Pread(fd, buf, offset)
	p.recordRead(start)
	return n, err
}

func (p *PreadScheduler) Stats() Stats {
	return Stats{ReadLatency: p.latencySnapshot()}
}

func (p *PreadScheduler) Close() error { return nil }
