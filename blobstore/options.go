package blobstore

import (
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
)

type config struct {
	segmentSize  int64
	ringDepth    uint32
	directWrites bool
	directReads  bool
	readHandles  int
	maxSize      int64
	onEvict      func(Eviction)

	preSubmit func(iosched.Op) iosched.Op
}

func defaultConfig() config {
	return config{
		segmentSize:  256 << 20,
		ringDepth:    256,
		directWrites: true,
		directReads:  true,
		readHandles:  4096,
	}
}

// Largest segment size: a segment's last record may run past the segment size
// by up to a record, at most the segment size itself, and a Location's 32-bit
// offset and size must still address it.
const maxSegmentSize = 1 << 31

func (c *config) validate() error {
	if c.segmentSize < 16*align.BlockSize || c.segmentSize > maxSegmentSize || c.segmentSize%align.BlockSize != 0 {
		return fmt.Errorf("blobstore: segment size %d must be a multiple of %d between %d and %d",
			c.segmentSize, align.BlockSize, 16*align.BlockSize, int64(maxSegmentSize))
	}
	if c.readHandles < 1 || uint64(c.readHandles) > uint64(^uint32(0))-writeSlots {
		return fmt.Errorf("blobstore: invalid read handle count %d", c.readHandles)
	}
	if c.maxSize < 0 || (c.maxSize != 0 && c.maxSize < 2*c.segmentSize) {
		return fmt.Errorf("blobstore: maximum size must be zero or at least two segments")
	}
	if c.directReads && !c.directWrites {
		return errors.New("blobstore: direct reads need direct writes: buffered records are not page-aligned")
	}
	return nil
}

// Option configures a Store.
type Option interface {
	apply(*config)
}

type funcOpt func(*config)

func (f funcOpt) apply(c *config) { f(c) }

// WithSegmentSize sets the size of each segment file. Default: 256 MiB.
func WithSegmentSize(bytes int64) Option {
	return funcOpt(func(c *config) { c.segmentSize = bytes })
}

// WithRingDepth sets the io_uring submission queue depth. Default: 256.
func WithRingDepth(n uint32) Option {
	return funcOpt(func(c *config) { c.ringDepth = n })
}

// WithDirectWrites chooses between O_DIRECT writes (the default) and writes
// through the kernel page cache, for every record of the store. A direct
// record starts on a page boundary and is padded to a page multiple, at most
// 4 KiB more per record (about 0.2% of a 1 MB value), so that it is written
// from the caller's memory with no copy and read back with O_DIRECT. A
// buffered record is not padded; it is copied into the page cache, and dirty
// pages are written back by the kernel. Buffered writes require buffered reads
// (WithDirectReads(false)), because buffered records are not page-aligned.
func WithDirectWrites(enabled bool) Option {
	return funcOpt(func(c *config) { c.directWrites = enabled })
}

// WithDirectReads chooses between O_DIRECT reads (the default) and reads
// through the kernel page cache. Direct reads copy nothing but need Read's
// memory to be page-aligned (see Read); buffered reads take any memory, copy
// each record out of the page cache, and let the page cache serve repeated
// reads of records it still holds. Direct reads need direct writes.
func WithDirectReads(enabled bool) Option {
	return funcOpt(func(c *config) { c.directReads = enabled })
}

// WithChecksum is retained for compatibility. Every record now carries a
// value checksum, independently of this option.
func WithChecksum() Option { return funcOpt(func(*config) {}) }

// WithMaxSize targets disk usage for retained segments and reservations. Zero
// disables eviction. The limit must fit at least two segments. Rotation retires
// oldest segments in batches, targeting 80% usage or two segments of headroom.
// Unlinks are best effort: pending or failed removals can exceed the target.
func WithMaxSize(bytes int64) Option {
	return funcOpt(func(c *config) { c.maxSize = bytes })
}

// WithEvictionCallback receives retired segment IDs once per batch, on the
// caller's goroutine after releasing store locks. fn owns the slice and may
// schedule index cleanup elsewhere. Concurrent notifications may arrive out
// of order. Unlinks are submitted through dio before fn, without waiting for
// completion. fn must not call Close or ReadIndex.
func WithEvictionCallback(fn func(Eviction)) Option {
	return funcOpt(func(c *config) { c.onEvict = fn })
}

// WithMaxReadHandles bounds the segment files the store keeps open for reads,
// in io_uring virtual descriptor slots. A read of a segment that is not open
// opens it, closing the least recently used one (see readSlots). Default:
// 4096.
func WithMaxReadHandles(n int) Option {
	return funcOpt(func(c *config) { c.readHandles = n })
}

// TestingWithPreSubmit is a hook for tests: the store passes every operation
// it is about to submit through fn and submits what fn returns. fn may block,
// to hold an operation back, or return another operation, to make it fail.
// Not for production use.
func TestingWithPreSubmit(fn func(iosched.Op) iosched.Op) Option {
	return funcOpt(func(c *config) { c.preSubmit = fn })
}

var logger atomic.Pointer[slog.Logger]

// log returns the package's logger: the one set by SetLogger, or slog's
// default.
func log() *slog.Logger {
	if l := logger.Load(); l != nil {
		return l
	}
	return slog.Default()
}

// SetLogger sets the logger for every Store. It is safe to call at any time.
func SetLogger(l *slog.Logger) { logger.Store(l) }
