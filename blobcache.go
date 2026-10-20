// Package blobcache writes blobs to data.db, index.db, and dedicated object
// files. NewWriter prepares a download destination; Put publishes it under its
// key. Inline records stay in bounded mmap memory until evicted. Larger and
// unknown-size values stream to object files using the same memory budget.
// Read returns a reader that the caller must close, including after EOF.
package blobcache

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
)

const (
	MaxKeyLen = blobstore.MaxKeyLen
	// SizeUnknown selects streaming to an object file without a promised size.
	SizeUnknown = blobstore.SizeUnknown
)

var (
	ErrEmptyKey    = blobstore.ErrEmptyKey
	ErrKeyTooLarge = blobstore.ErrKeyTooLarge
	ErrNotFound    = blobstore.ErrNotFound
	ErrCorrupt     = blobstore.ErrCorrupt
	// ErrBusy means memory is unavailable or disk reuse overlaps an unfinished write.
	ErrBusy = alloc.ErrExhausted
)

// Writer accepts one value. Call Close on success or failure, including after
// Put, before closing the Cache. Methods on a single writer must not race each
// other or Cache.Close.
// Write and ReadFrom are no longer allowed after Put. Flush waits for data I/O
// without publishing the record or syncing the file.
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

// Cache is a disk cache. All methods are safe for concurrent use, except that
// Close must not race other calls. Callers must close all readers and writers first.
// Methods other than Close require an open Cache.
type Cache struct {
	store  *blobstore.Store
	memory *cacheAllocator
	values memoryCache
	closed bool

	reclaimRequests     chan reclaimRequest
	diskReclaimRequests chan struct{}
	diskReclaimTrigger  atomic.Uint64
	reclaimWG           sync.WaitGroup
}

// New opens or creates data.db, index.db, and objects/. The disk index recovers
// surviving key metadata; resident value buffers start empty. The directory must
// have one owner.
func New(path string, opts ...Option) (*Cache, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if cfg.memory < 128<<10 || cfg.memory%align.BlockSize != 0 {
		return nil, errors.New("blobcache: memory must be a page multiple of at least 128 KiB")
	}
	memory, err := alloc.NewSlabAllocator(cfg.memory, cfg.allocator...)
	if err != nil {
		return nil, err
	}
	c := &Cache{values: newMemoryCache()}
	c.memory = &cacheAllocator{Allocator: memory, capacity: cfg.memory, reclaim: c.startCacheReclaim}
	// Keep the default staging buffer usable even with the smallest budget.
	storeOpts := append([]blobstore.Option{
		blobstore.WithStagingSize(min(256<<10, cfg.memory-align.BlockSize)),
	}, cfg.store...)
	st, err := blobstore.Open(path, c.memory, c.memory, storeOpts...)
	if err != nil {
		memory.Close()
		return nil, err
	}
	st.OnRetire = c.values.retire
	st.Recover(func(h blobstore.RecordHeader) {
		c.values.replace(h.Hash, nil, h)
	})
	c.store = st
	c.reclaimRequests = make(chan reclaimRequest, 1)
	c.diskReclaimRequests = make(chan struct{}, 1)
	target := st.TargetSize()
	c.diskReclaimTrigger.Store(target - target/20)
	c.reclaimWG.Go(c.reclaimLoop)
	c.reclaimWG.Go(c.diskReclaimLoop)
	c.startDiskReclaim()
	return c, nil
}

// NewWriter copies key and reserves storage for size bytes, or SizeUnknown.
// At 80% memory usage, allocations signal background reclamation. Insufficient
// capacity waits for a reclamation pass before attempting allocation. Each shard
// uses SIEVE to meet its proportional byte target; only final-reference releases
// count as reclaimed space. Keys and disk metadata remain indexed.
// ErrBusy means memory is still unavailable after reclamation, or disk reuse
// overlaps an unfinished write. The caller should immediately defer Close.
func (c *Cache) NewWriter(key []byte, size int64) (*Writer, error) {
	w, err := c.store.NewWriter(key, size)
	if err == blobstore.ErrBusy {
		return nil, ErrBusy
	}
	return w, err
}

// Put submits a filled writer. Data and index completion publish the value in
// the cache; a Read before then may miss or see the previous value. Late I/O
// failures are logged and leave the new record unpublished. Call Put once, then
// Close the writer. Neither waits for Linux I/O completion.
func (c *Cache) Put(w *Writer) error {
	return c.store.Put(w, c.publish)
}

func (c *Cache) publish(header blobstore.RecordHeader, buffer *alloc.Buffer) {
	c.values.replace(header.Hash, buffer, header)
	c.startDiskReclaim()
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
	return r, nil
}

// Close drains accepted puts, joins both reclamation workers, and releases cached
// buffers before closing the allocator. The caller must close all readers
// and writers first; Close does not track them. It must not race cache, reader,
// or writer calls. Repeated calls are harmless.
func (c *Cache) Close() (err error) {
	if c.closed {
		return nil
	}
	c.closed = true
	c.store.Wait() // Publication can still signal the reclamation workers.
	close(c.reclaimRequests)
	close(c.diskReclaimRequests)
	c.reclaimWG.Wait()
	err = c.store.Close()
	c.values.clear()
	c.memory.Close()
	return err
}
