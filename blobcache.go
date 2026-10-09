// Package blobcache is a disk cache for large blobs, such as chunks of objects
// downloaded from cloud storage.
//
// A Cache is an index and a memory tier over a blobstore.Store, which does
// the I/O. Value memory grows on demand up to WithMemory bytes, in blocks
// shared by contiguous records. Values are never moved. A caller downloads into
// memory lent by Alloc and hands it back with Put; that memory is the cached value. The store
// frames the record in place and writes it with one O_DIRECT write through an
// io_uring scheduler. Put does not wait for disk I/O. Get lends
// a value in memory to a callback where it lies, or reads it from disk into
// cache memory with one I/O, lends it, and keeps it. Memory is reclaimed
// from the oldest blocks; retirement prevents new readers immediately, while
// existing users delay storage reuse. When no memory can be reclaimed,
// Alloc and Get return ErrBusy.
//
// Records are appended to preallocated segment files. When a segment fills,
// its index is written as a footer at its end and the segment is never
// modified again. WithMaxSize targets disk usage by evicting whole segments
// in FIFO order; memory and index management stay in this package.
package blobcache

import (
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/blobstore"
)

var (
	// ErrClosed is returned by operations on a closed cache.
	ErrClosed = blobstore.ErrClosed
	// ErrEmptyKey is returned for an empty key.
	ErrEmptyKey = blobstore.ErrEmptyKey
	// ErrKeyTooLarge is returned for a key longer than MaxKeyLen.
	ErrKeyTooLarge = blobstore.ErrKeyTooLarge
	// ErrValueTooLarge is returned for a record that cannot fit in a segment.
	ErrValueTooLarge = blobstore.ErrValueTooLarge
	// ErrBusy is returned when the cache cannot do something right now
	// without waiting: no memory block can be reclaimed, too
	// many writes are in flight, or no segment file is ready for the next
	// write. The caller decides whether to skip caching or retry.
	ErrBusy = blobstore.ErrBusy
	// ErrNotFound is returned by Get on a miss: a key that is absent, whose
	// segment file is gone, or whose record failed verification.
	ErrNotFound = errors.New("blobcache: not found")
)

// MaxKeyLen is the longest key Put accepts.
const MaxKeyLen = blobstore.MaxKeyLen

// RecordSize returns the memory to Alloc for a value of valueLen bytes under
// a key of keyLen bytes: the value and the record's framing, padded to a page
// multiple with direct writes (the default). Put writes the record in place.
func (c *Cache) RecordSize(keyLen, valueLen int) int { return c.store.RecordSize(keyLen, valueLen) }

// SetLogger sets the logger for every Cache and its store. It is safe to call
// at any time.
func SetLogger(l *slog.Logger) {
	logger.Store(l)
	blobstore.SetLogger(l)
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

// Cache is a disk cache. All methods are safe for concurrent use, except that
// Close must not race other calls.
type Cache struct {
	store      *blobstore.Store
	index      index
	mem        *memory
	memoryHits bool
	inflight   inflight
	completer  sync.WaitGroup
	closed     atomic.Bool
	stats      counters
	reads      struct {
		sync.Mutex
		pending map[readKey]*readFlight
	}
}

type counters struct {
	puts, putErrors  atomic.Uint64
	hits, memoryHits atomic.Uint64
	misses, corrupt  atomic.Uint64
}

// Stats is a point-in-time snapshot of cache counters.
type Stats struct {
	Items    int // keys in the index
	Segments int // segment files

	Puts      uint64 // accepted writes
	PutErrors uint64 // writes that failed after acceptance

	Hits       uint64 // lookups that found a value
	MemoryHits uint64 // hits served from memory, without I/O
	Misses     uint64 // lookups that found nothing, or a segment file that is gone
	Corrupt    uint64 // records that failed verification (served as misses)

	MemoryUsed int64 // mapped/reserved backing bytes, including free slots

	FailedSegments  uint64 // seal submissions that reported errors
	DiskBytes       int64  // retained files and reservations; excludes pending/failed unlinks
	EvictedSegments uint64
	EvictionErrors  uint64

	WritesInFlight int // writes accepted by Put and not yet completed
}

// New opens the cache in the directory path, which must exist (see
// blobstore.Open), and reads the index of what is on disk.
func New(path string, opts ...Option) (*Cache, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	mem, err := newMemory(cfg.memory)
	if err != nil {
		return nil, err
	}
	c := &Cache{
		index:      newIndex(0),
		mem:        mem,
		memoryHits: cfg.memoryHits,
	}
	c.inflight.init(maxWritesInFlight)
	mem.onEvict = c.index.evictMemory
	cfg.store = append(cfg.store, blobstore.WithEvictionCallback(c.index.evicted))
	store, err := blobstore.Open(path, cfg.store...)
	if err != nil {
		return nil, errors.Join(err, mem.close())
	}
	if err := store.ReadIndex(c.index.loaded); err != nil {
		return nil, errors.Join(err, store.Close(), mem.close())
	}
	c.store = store
	c.completer.Go(c.complete)
	return c, nil
}

// maxWritesInFlight sizes the queue of writes in flight (see inflight).
const maxWritesInFlight = 4096

// Close waits for every accepted write, then closes the store and releases
// the cache's memory. Every buffer from Alloc must have been given back; if
// one has not, Close reports it and leaves the memory mapped. Close must not
// race other calls.
func (c *Cache) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.inflight.queue)
	c.completer.Wait()
	return errors.Join(c.store.Close(), c.mem.close())
}

// Drain waits until every write accepted before the call has completed:
// landed on disk, or failed. Drain is the completion barrier for callers
// that need one, such as the end of a batch.
func (c *Cache) Drain() {
	if !c.closed.Load() {
		c.inflight.drain()
	}
}

// Stats returns a snapshot of the cache's counters.
func (c *Cache) Stats() Stats {
	st := c.store.Stats()
	return Stats{
		Items:           c.index.len(),
		Segments:        st.Segments,
		Puts:            c.stats.puts.Load(),
		PutErrors:       c.stats.putErrors.Load(),
		Hits:            c.stats.hits.Load(),
		MemoryHits:      c.stats.memoryHits.Load(),
		Misses:          c.stats.misses.Load(),
		Corrupt:         c.stats.corrupt.Load(),
		MemoryUsed:      c.mem.used(),
		FailedSegments:  st.FailedSegments,
		DiskBytes:       st.DiskBytes,
		EvictedSegments: st.EvictedSegments,
		EvictionErrors:  st.EvictionErrors,
		WritesInFlight:  c.inflight.inFlight(),
	}
}

// --- Completer ---

// complete is the completer goroutine. It finishes writes in submission
// order: it waits for each, records errors, and releases the
// write's pin on the value's memory, which becomes reclaimable.
func (c *Cache) complete() {
	for w := range c.inflight.queue {
		if w.drained != nil {
			close(w.drained)
			continue
		}
		_, err := w.ticket.Wait()
		if err != nil {
			c.stats.putErrors.Add(1)
			log().Error("blob write failed", "error", err)
		}
		c.index.completed(w.hash, w.ticket.Location())
		w.mem.unpin()
		c.inflight.unreserve()
	}
}
