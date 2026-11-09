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
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
)

const MaxKeyLen = blobstore.MaxKeyLen

var (
	// ErrRejected means I/O exceeded a configured scheduler admission limit.
	// Close the writer/reader normally; a rejected fill is not published.
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

// Writer accepts one value. Close it before closing the Cache; Close does
// nothing after Put has closed it. Methods on one writer must not race each
// other or Cache.Close.
type Writer = blobstore.Writer

// Reader owns its buffer until Close, even after EOF. Close may be called early;
// it releases resources without draining the value. WithVerifyChecksums verifies
// disk values as they are consumed. Resident values are trusted.
// Methods on one Reader must not race each other or Cache.Close. Read and WriteTo
// require an open Reader.
type Reader interface {
	io.ReadCloser
	io.WriterTo
	Size() int64
}

// minMemory leaves room for resident values and concurrent staging buffers.
const minMemory = 4 << 20

// Cache is a disk cache. All methods are safe for concurrent use, except that
// Close must not race other calls. Callers must close all readers and writers first.
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
	// Keep the default staging buffer usable even with the smallest budget.
	storeOpts := append([]blobstore.Option{
		blobstore.WithStagingSize(min(256<<10, c.memory.Capacity()-align.BlockSize)),
	}, cfg.store...)
	storeOpts = append(storeOpts, blobstore.WithRetireFunc(func(h blobstore.RecordHeader) {
		c.values.retire(h.KeyHash(), h.ObjectID)
	}))
	st, err := blobstore.Open(path, c.memory, storeOpts...)
	if err != nil {
		memory.Close()
		return nil, err
	}
	for h := range st.Records() {
		if h.Flags.HasXXH3KeyHash() {
			c.values.replace(h.KeyHash(), blobstore.Memory{}, h, false)
		}
	}
	c.store = st
	c.reclaimRequests = make(chan int64, 1)
	c.reclaimWG.Go(c.reclaimLoop)
	return c, nil
}

// NewWriter copies key and allocates capacity for a complete inline value.
// The value size need not be known. Put reserves storage using the number of bytes written.
// Defer Close immediately.
func (c *Cache) NewWriter(key []byte) (*Writer, error) {
	w, err := c.store.NewWriter(key)
	c.startCacheReclaim(0)
	if err != nil {
		metrics.WriterErrors.Add(1)
	}
	return w, err
}

// Put publishes inline memory immediately and submits its disk write without
// waiting. External values become readable after their writes complete. Disk
// failures for accepted inline values are recorded in PutErrors; their memory
// stays readable. Put consumes and closes w.
func (c *Cache) Put(w *Writer) error {
	started := time.Now()
	defer func() { metrics.PutDuration.RecordValue(int64(time.Since(started))) }()
	err := c.store.Put(w, func(h blobstore.RecordHeader, memory blobstore.Memory) {
		c.values.replace(h.KeyHash(), memory, h, isResident(memory))
	}, func(h blobstore.RecordHeader, err error) {
		if err != nil {
			metrics.PutErrors.Add(1)
		}
		c.values.complete(h, err)
	})
	if err != nil {
		metrics.PutErrors.Add(1)
		if errors.Is(err, blobstore.ErrBusy) {
			return ErrBusy
		}
	}
	return err
}

// Read returns the value for key, or ErrNotFound. Resident values stay pinned
// until the reader is closed; other values stream from disk in bounded staging
// memory. The caller must Close the reader, including after EOF or a read error,
// before closing the cache. Allocation pressure may return ErrBusy.
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
	if isResident(buffer) {
		return newMemoryReader(key, header, buffer)
	}
	r, err := c.store.NewReader(key, header)
	c.startCacheReclaim(0)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			c.values.retire(hash, header.ObjectID)
		}
		return nil, err
	}
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
// buffers before closing the allocator. The caller must close all readers
// and writers first; Close does not track them. It must not race cache, reader,
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
