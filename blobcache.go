// Package blobcache stores blobs in a log on a bounded NVMe device range.
// NewWriter prepares a download destination; Put publishes it under its key.
// Values up to the inline threshold stay in bounded mmap memory until evicted.
// Larger values use the same memory budget for I/O and are fetched from disk on
// reads. Read and ReadRange return a reader that the caller must close,
// including after EOF.
package blobcache

import (
	"errors"
	"io"
	"math"
	"sync"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/nvme"
)

const MaxKeyLen = blobstore.MaxKeyLen

var (
	// ErrRejected means I/O exceeded a configured scheduler admission limit.
	// Put still consumes its writer; readers must still be closed.
	ErrRejected       = blobstore.ErrRejected
	ErrEmptyKey       = blobstore.ErrEmptyKey
	ErrKeyTooLarge    = blobstore.ErrKeyTooLarge
	ErrNotFound       = blobstore.ErrNotFound
	ErrInvalidRange   = blobstore.ErrInvalidRange
	ErrCorrupt        = blobstore.ErrCorrupt
	ErrRecordTooLarge = blobstore.ErrRecordTooLarge
	ErrUnformatted    = blobstore.ErrUnformatted
	// ErrBusy means memory is unavailable.
	ErrBusy = alloc.ErrExhausted
)

// Writer accepts one value. Put consumes it, including on failure. Discard
// writers that are never passed to Put. Methods must not race Cache.Close.
type Writer = blobstore.Writer

// Ticket exposes only Wait() error. Waiting is optional; internal completion
// updates and buffer release happen even when the caller does not wait.
type Ticket = blobstore.Ticket

// RejectedError carries a suggested delay before offering more I/O.
// It wraps ErrRejected; the delay does not reserve admission capacity.
type RejectedError = iosched.RejectedError

// Reader owns its buffers until Close, even after EOF. Next returns borrowed,
// immutable chunks; Read copies into the supplied destination. Disk I/O starts
// in Cache.ReadRange; its errors are reported on first access. Close does not
// wait for pending I/O.
// Methods on one Reader must not race each other or Cache.Close. Next, Read
// and WriteTo require an open Reader.
type Reader interface {
	io.ReadCloser
	io.WriterTo
	Size() int64
	// Next consumes a contiguous borrowed chunk, or returns io.EOF at the end.
	// It shares its cursor with Read and WriteTo. Do not modify returned bytes.
	// A chunk stays valid until Close, or for a disk range longer than the read
	// window, until the next call.
	Next() ([]byte, error)
}

// minMemory leaves room for resident values and concurrent I/O buffers.
const minMemory = 4 << 20

// Cache is a disk cache. All methods are safe for concurrent use, except that
// Close must not race other calls. Close readers and discard abandoned writers first.
// Methods other than Close require an open Cache.
type Cache struct {
	store  *blobstore.Store
	memory *alloc.Allocator
	values memoryCache
	closed bool

	reclaimRequests chan int64
	reclaimWG       sync.WaitGroup
}

// Format initializes an empty cache on device's range, discarding the previous
// cache's records. Use an exclusively owned range with every cache closed.
func Format(device *nvme.Device) error { return blobstore.Format(device) }

// New opens the cache that Format initialized on device's range, recovering
// the records listed by its sealed segments; resident value buffers start
// empty. The cache opens its own handle; the caller retains device. The range
// must have one owner.
func New(device *nvme.Device, opts ...Option) (*Cache, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if cfg.memory < minMemory {
		return nil, errors.New("blobcache: memory must be at least 4 MiB")
	}
	if cfg.inline < 0 {
		return nil, errors.New("blobcache: invalid inline threshold")
	}
	c := &Cache{values: newMemoryCache(cfg.inline)}
	// The allocator uses whole spans; round down to stay within the budget.
	// Writers initialize their contents and padding; readers fill every exposed
	// byte from disk. Avoid clearing the full inline capacity on every fill.
	allocatorOpts := append([]alloc.Option{alloc.WithoutZeroing()}, cfg.allocator...)
	memory := alloc.NewAllocator(cfg.memory/alloc.MaxAllocationSize*alloc.MaxAllocationSize, allocatorOpts...)
	c.memory = memory
	storeOpts := append([]blobstore.Option(nil), cfg.store...)
	storeOpts = append(storeOpts, blobstore.WithMissFunc(func(h blobstore.RecordHeader) {
		c.values.retire(h.KeyHash(), h.Position)
	}))
	st, err := blobstore.Open(device, c.memory, storeOpts...)
	if err != nil {
		memory.Close()
		return nil, err
	}
	c.store = st
	c.values.live = st.Live
	for h := range st.Records() {
		c.values.replace(h.KeyHash(), nil, h, false)
	}
	c.reclaimRequests = make(chan int64, 1)
	c.reclaimWG.Go(c.reclaimLoop)
	return c, nil
}

func (c *Cache) NewWriter() (*Writer, error) {
	w, err := c.store.NewWriter()
	c.startCacheReclaim(0)
	if err != nil {
		metrics.WriterErrors.Add(1)
	}
	return w, err
}

// Put takes ownership of w on entry, including on failure. Do not use w after
// calling Put. A value within the inline threshold is published in memory
// before disk submission and remains readable on I/O failure. A larger value
// is published only after successful I/O, preserving the previous value on
// failure. The returned ticket allows the
// caller to wait for disk I/O and all completion updates; waiting is optional.
// Preparation and submission errors return an invalid ticket.
// Internal completion uses SubmitNotify and holds the buffer until I/O finishes.
func (c *Cache) Put(key []byte, w *Writer) (Ticket, error) {
	started := time.Now()
	defer func() { metrics.PutDuration.RecordValue(int64(time.Since(started))) }()
	if err := w.Seal(key); err != nil {
		w.Discard()
		metrics.PutErrors.Add(1)
		return Ticket{}, err
	}
	h, buffer := w.Header(), w.Buffer()
	c.values.sweep(8)
	resident := c.values.resident(h)
	if resident {
		// Pending entries borrow Put's buffer; completion transfers its ownership.
		c.values.replace(h.KeyHash(), buffer, h, true)
	}
	return c.store.Put(w, func(err error) {
		h := w.Header()
		if err != nil {
			metrics.PutErrors.Add(1)
		}
		buffer := w.TakeBuffer() // Move Put's existing ownership; never retain it.
		if !resident {
			if err == nil {
				c.values.replace(h.KeyHash(), nil, h, false)
			}
			buffer.Release()
		} else if !c.values.complete(h, err) {
			buffer.Release() // Replaced while in flight; the cache no longer wants it.
		} else {
			c.startCacheReclaim(0) // Reclaim skips pending values; this one is now evictable.
		}
		w.Discard()
	})
}

// Read returns the whole value for key; see ReadRange.
func (c *Cache) Read(key []byte) (Reader, error) { return c.ReadRange(key, 0, math.MaxInt64) }

// Stat reports the size of key's value from the index, without I/O. A later
// read can still miss.
func (c *Cache) Stat(key []byte) (int64, error) {
	if len(key) == 0 {
		return 0, ErrEmptyKey
	}
	buffer, header, ok := c.values.lookup(blobstore.HashKey(key))
	if buffer != nil {
		buffer.Release()
	} else if !ok || header.Flags.IsDeleted() || !c.store.Live(header) {
		return 0, ErrNotFound
	}
	return int64(header.LogicalLen), nil
}

// ReadRange returns n bytes of key's value from off, clamped to the value's
// end, or ErrNotFound; Size reports the range length. Resident values stay
// pinned until the reader is closed. Other values are read from disk, one read
// window of the pages covering the range at a time; a record the log
// overwrites before its read completes is ErrNotFound. Close the reader after
// EOF or an error, before closing the cache. Allocation pressure may return
// ErrBusy.
func (c *Cache) ReadRange(key []byte, off, n int64) (Reader, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	if len(key) > MaxKeyLen {
		return nil, ErrKeyTooLarge
	}
	hash := blobstore.HashKey(key)
	buffer, header, ok := c.values.lookup(hash)
	if !ok {
		return nil, ErrNotFound
	}
	if buffer != nil {
		return newMemoryReader(key, header, buffer, off, n)
	}
	if len(key) != int(header.KeyLen) || header.Flags.IsDeleted() {
		return nil, ErrNotFound
	}
	r, err := c.store.NewRangeReader(header, off, n)
	if err != nil {
		return nil, err
	}
	c.startCacheReclaim(0)
	return r, nil
}

// IOSchedulerStats reports each scheduler's role and write budget, in creation
// order.
func (c *Cache) IOSchedulerStats() []blobstore.SchedulerStats {
	return c.store.IOSchedulerStats()
}

// IODiskStats reports outstanding admission charges across all rings.
func (c *Cache) IODiskStats() iosched.DiskStats { return c.store.IODiskStats() }

// Close drains cleanup, joins the memory reclaimer, which completions
// signal, and releases cached buffers before closing the allocator. Close readers and discard abandoned
// writers first; Put owns submitted writers through completion. It must not race cache, reader,
// or writer calls. Repeated calls are harmless.
func (c *Cache) Close() (err error) {
	if c.closed {
		return nil
	}
	c.closed = true
	err = c.store.Close()
	close(c.reclaimRequests)
	c.reclaimWG.Wait()
	c.values.clear()
	c.memory.Close()
	return err
}
