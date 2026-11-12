// Package blobcache writes blobs to data.db, index.db, and dedicated object
// files. NewWriter prepares a download destination; Put publishes it under its
// key. Inline records stay in bounded mmap memory until evicted. Larger values
// use object files and the same memory budget.
// Read returns a reader that the caller must close, including after EOF.
package blobcache

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
)

const MaxKeyLen = blobstore.MaxKeyLen

var (
	// ErrRejected means I/O exceeded a configured scheduler admission limit.
	// Put still consumes its writer; readers must still be closed.
	ErrRejected = blobstore.ErrRejected
	// ErrNoDescriptors means too many external readers and writers are open
	// on one scheduler. It wraps syscall.ENFILE.
	ErrNoDescriptors = blobstore.ErrNoDescriptors
	ErrEmptyKey      = blobstore.ErrEmptyKey
	ErrKeyTooLarge   = blobstore.ErrKeyTooLarge
	ErrNotFound      = blobstore.ErrNotFound
	ErrCorrupt       = blobstore.ErrCorrupt
	// ErrBusy means memory is unavailable or disk reuse overlaps an open writer.
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

// Reader owns its buffer until Close, even after EOF. Next returns borrowed,
// immutable chunks valid until Close; Read copies into the supplied destination.
// Disk I/O starts in Cache.Read; its errors and checksum verification are deferred to
// first access. Close does not wait for pending I/O. Resident values are trusted.
// Methods on one Reader must not race each other or Cache.Close. Next, Read
// and WriteTo require an open Reader.
type Reader interface {
	io.ReadCloser
	io.WriterTo
	Size() int64
	// Next consumes a contiguous borrowed chunk, or returns io.EOF at the end.
	// It shares its cursor with Read and WriteTo. Do not modify returned bytes.
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

// New opens or creates data.db, index.db, and objects/. The disk index recovers
// surviving key metadata; resident value buffers start empty. The directory must
// have one owner.
func New(path string, opts ...Option) (*Cache, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if cfg.memory < minMemory {
		return nil, errors.New("blobcache: memory must be at least 4 MiB")
	}
	c := &Cache{values: newMemoryCache()}
	// The allocator uses whole spans; round down to stay within the budget.
	// Writers initialize their contents and padding; readers fill every exposed
	// byte from disk. Avoid clearing the full inline capacity on every fill.
	allocatorOpts := append([]alloc.Option{alloc.WithoutZeroing()}, cfg.allocator...)
	memory := alloc.NewAllocator(cfg.memory/alloc.MaxAllocationSize*alloc.MaxAllocationSize, allocatorOpts...)
	c.memory = memory
	storeOpts := append([]blobstore.Option(nil), cfg.store...)
	storeOpts = append(storeOpts, blobstore.WithRetireFunc(func(h blobstore.RecordHeader) {
		c.values.retire(h.KeyHash(), h.ObjectID)
	}), blobstore.WithReadFunc(c.values.admit))
	st, err := blobstore.Open(path, c.memory, storeOpts...)
	if err != nil {
		memory.Close()
		return nil, err
	}
	for h := range st.Records() {
		if h.Flags.HasXXH3KeyHash() {
			c.values.replace(h.KeyHash(), nil, h, false)
		}
	}
	c.store = st
	c.reclaimRequests = make(chan int64, 1)
	c.reclaimWG.Go(c.reclaimLoop)
	return c, nil
}

// NewWriter allocates a destination for a value whose size and key may be
// unknown. Put supplies the key and takes ownership after writing. Discard
// the writer if writing fails or the value is abandoned. When using io.Copy,
// check its source error before calling Put; the writer only sees write errors.
func (c *Cache) NewWriter() (*Writer, error) {
	w, err := c.store.NewWriter()
	c.startCacheReclaim(0)
	if err != nil {
		metrics.WriterErrors.Add(1)
	}
	return w, err
}

// Put takes ownership of w on entry, including on failure. Do not use w after
// calling Put. Inline memory is published before disk submission and remains
// readable on I/O failure. External values are published only after successful
// I/O, preserving the previous value on failure. The returned ticket allows the
// caller to wait for disk I/O and all completion updates; waiting is optional.
// Preparation and submission errors return an invalid ticket.
// Internal completion uses SubmitNotify and holds the buffer until I/O finishes.
func (c *Cache) Put(key []byte, w *Writer) (Ticket, error) {
	started := time.Now()
	defer func() { metrics.PutDuration.RecordValue(int64(time.Since(started))) }()
	if err := w.Seal(key); err != nil {
		w.Discard()
		metrics.PutErrors.Add(1)
		if errors.Is(err, blobstore.ErrBusy) {
			err = ErrBusy
		}
		return Ticket{}, err
	}
	h, buffer := w.Header(), w.Buffer()
	if !h.Flags.IsExternal() {
		// Pending entries borrow Put's buffer; completion transfers its ownership.
		c.values.replace(h.KeyHash(), buffer, h, true)
	}
	return c.store.Put(w, func(err error) {
		h := w.Header()
		if err != nil {
			metrics.PutErrors.Add(1)
		}
		buffer := w.TakeBuffer() // Move Put's existing ownership; never retain it.
		if h.Flags.IsExternal() {
			if err == nil {
				c.values.replace(h.KeyHash(), nil, h, false)
			}
			buffer.Release()
		} else if !c.values.complete(h, err) {
			buffer.Release() // Replaced while in flight; the cache no longer wants it.
		}
		w.Discard()
	})
}

// Read returns the value for key, or ErrNotFound. Resident values stay pinned
// until the reader is closed; other values are read from disk in one submission
// using memory for the whole frame. Disk validation errors are deferred to first
// access. Successfully read inline values re-enter the resident cache using
// that same buffer. Close the reader after EOF or an error, before closing the cache.
// Allocation pressure may return ErrBusy.
func (c *Cache) Read(key []byte) (Reader, error) {
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
		return newMemoryReader(key, header, buffer)
	}
	r, err := c.store.NewReader(key, header)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			c.values.retire(hash, header.ObjectID)
		}
		return nil, err
	}
	c.startCacheReclaim(0)
	return r, nil
}

// IOSchedulerStats reports each scheduler's role, backend, requested CPU
// affinity, and write budget, in WithSchedulerCPUs order.
func (c *Cache) IOSchedulerStats() []blobstore.SchedulerStats {
	return c.store.IOSchedulerStats()
}

// IODiskStats reports outstanding admission charges across all rings.
func (c *Cache) IODiskStats() iosched.DiskStats { return c.store.IODiskStats() }

// Close joins the memory reclaimer, drains cleanup, and releases cached
// buffers before closing the allocator. Close readers and discard abandoned
// writers first; Put owns submitted writers through completion. It must not race cache, reader,
// or writer calls. Repeated calls are harmless.
func (c *Cache) Close() (err error) {
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.reclaimRequests)
	c.reclaimWG.Wait()
	err = c.store.Close()
	c.values.clear()
	c.memory.Close()
	return err
}
