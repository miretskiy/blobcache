package blobcache

import (
	"errors"
	"fmt"

	"github.com/miretskiy/blobcache/blobstore"
)

// Alloc lends the caller size bytes of the cache's memory, page-aligned, for
// a value to be stored with Put: download into it, then hand it back. For a
// value of n bytes under key, size is c.RecordSize(len(key), n).
//
// The memory is the caller's until it gives it back, exactly once: with Put,
// which stores it, or with Free. Alloc never waits for borrowers: when it cannot reclaim
// enough memory, because all reclaimable blocks are still in use, it returns
// ErrBusy.
func (c *Cache) Alloc(size int) ([]byte, error) {
	if c.closed.Load() {
		return nil, ErrClosed
	}
	_, buf, err := c.mem.alloc(size, true, nil)
	return buf, err
}

// Free gives back memory from Alloc that is not going to be stored.
func (c *Cache) Free(buf []byte) error {
	mem, err := c.mem.take(buf, nil)
	if err != nil {
		return err
	}
	mem.unpin()
	return nil
}

// Put stores buf[:n] as the value of key. buf is memory from Alloc, and Put
// takes it back unless it returns ErrBusy, ErrClosed or ErrForeignMemory.
// On ErrBusy the caller may retry with the same buffer, or Free it. After the
// transfer, the caller must not touch it again.
// The value stays where it is: Get serves it from that memory at once, while
// it is written to disk with one O_DIRECT write and for as long as memory
// allows after. Put frames the record in buf after the value (padding, the
// key and a trailer), so len(buf) must be at least c.RecordSize(len(key), n).
//
// Put never waits for the disk. The ticket reports whether the write reached
// the disk; waiting on it is optional. Memory that did not come from Alloc,
// or that has already been given back, is refused with ErrForeignMemory.
func (c *Cache) Put(key, buf []byte, n int) (Ticket, error) {
	if c.closed.Load() {
		return Ticket{}, ErrClosed
	}
	if !c.inflight.reserve() {
		return Ticket{}, ErrBusy
	}
	h := blobstore.HashKey(key)
	mem, err := c.mem.take(buf, &h) // the caller's pin is now the write's
	if err != nil {
		c.inflight.unreserve()
		return Ticket{}, err
	}
	// Reject insufficient framing space rather than trigger blobstore's
	// allocation-and-copy fallback. The cache always writes its own buffer.
	if n >= 0 && n <= len(buf) && len(key) > 0 && len(key) <= MaxKeyLen && c.RecordSize(len(key), n) > len(buf) {
		mem.unpin()
		c.inflight.unreserve()
		return Ticket{}, fmt.Errorf("blobcache: buffer lacks record framing space; allocate RecordSize bytes")
	}
	ticket, err := c.store.Write(key, buf, n)
	if err != nil {
		if errors.Is(err, ErrBusy) {
			c.mem.restore(buf, mem)
		} else {
			mem.unpin()
		}
		c.inflight.unreserve()
		return Ticket{}, err
	}
	mem.data = mem.data[:n:n]
	c.index.put(h, mem, ticket.Location())
	// The completer enables disk reads and releases the I/O pin. Memory
	// eviction may happen earlier; a pending disk write is then a cache miss.
	c.inflight.push(pendingWrite{hash: h, mem: mem, ticket: ticket})
	c.stats.puts.Add(1)
	return Ticket{ticket}, nil
}

// Ticket reports whether a Put's write reached the disk.
type Ticket struct{ t blobstore.Ticket }

// Wait waits for the write and returns its error, if any.
func (t Ticket) Wait() error {
	_, err := t.t.Wait()
	return err
}

// Get finds key's value and lends it to fn, returning fn's error. The value
// is valid only while fn runs: fn must not modify or keep it, and copies it if it needs
// it after. Nothing is copied to call fn.
//
// A value in memory is lent where it is: memory from a Put, from the moment
// Put returns, or memory a previous Get read it into. Otherwise Get reads the
// record from disk, with one read, into memory of exactly the record's size,
// lends it, and keeps it in memory for later Gets. While fn runs, the memory
// cannot be reclaimed, so fn should be brief.
//
// Get returns ErrNotFound for a key that is absent, whose segment file is
// gone, or whose record fails verification on disk (dropped from the
// index; a value checksum failure also carries a *base.ChecksumError). It returns ErrBusy, without waiting, if the value
// must be read from disk and no memory can be reclaimed for it. It waits for
// the shared read for that blob, without holding an index or allocator lock.
func (c *Cache) Get(key []byte, fn func(value []byte) error) error {
	_, err := c.get(key, fn)
	return err
}

// get is Get, also reporting whether the value came from memory.
func (c *Cache) get(key []byte, fn func(value []byte) error) (fromMemory bool, err error) {
	if c.closed.Load() {
		return false, ErrClosed
	}
	h := blobstore.HashKey(key)
	it, ok := c.index.get(h)
	if !ok {
		c.stats.misses.Add(1)
		return false, ErrNotFound
	}
	if it.mem.block != nil && c.memoryHits {
		if it.mem.pin() {
			c.stats.hits.Add(1)
			c.stats.memoryHits.Add(1)
			defer it.mem.unpin()
			return true, fn(it.mem.data)
		}
	}
	if !it.onDisk() {
		// A Put whose memory is not lent (see withoutMemoryHits) and whose
		// write is still in flight.
		c.stats.misses.Add(1)
		return false, ErrNotFound
	}
	return false, c.readDisk(h, key, it.loc, fn)
}

// A flight holds one block pin until every participating callback returns.
// The leader performs I/O on its caller's goroutine; followers share its buffer.
type readKey struct {
	hash Key
	loc  blobstore.Location
}

type readFlight struct {
	done  chan struct{}
	users int // protected by Cache.reads
	mem   memoryValue
	err   error
}

func (c *Cache) readDisk(h Key, key []byte, loc blobstore.Location, fn func([]byte) error) error {
	k := readKey{h, loc}
	c.reads.Lock()
	f := c.reads.pending[k]
	if f != nil {
		select {
		case <-f.done:
			// An old callback may still hold a completed read. Once its block
			// is retired, new callers must start a fresh disk read.
			if f.mem.block != nil && f.mem.block.refs.Load() < 0 {
				f = nil
			}
		default: // share the disk I/O that is still in progress
		}
	}
	leader := f == nil
	if leader {
		if c.reads.pending == nil {
			c.reads.pending = make(map[readKey]*readFlight)
		}
		f = &readFlight{done: make(chan struct{})}
		c.reads.pending[k] = f
	}
	f.users++
	c.reads.Unlock()
	defer func() {
		c.reads.Lock()
		f.users--
		last := f.users == 0
		if last && c.reads.pending[k] == f {
			delete(c.reads.pending, k)
		}
		c.reads.Unlock()
		if last && f.mem.block != nil {
			f.mem.unpin()
		}
	}()
	if leader {
		f.mem, f.err = c.load(h, key, loc)
		close(f.done)
	} else {
		<-f.done
	}
	if f.err != nil {
		if errors.Is(f.err, ErrNotFound) {
			c.stats.misses.Add(1)
		}
		return f.err
	}
	c.stats.hits.Add(1)
	return fn(f.mem.data)
}

// load returns a pinned value. Its caller releases the pin after every reader
// sharing this load has finished. There is no payload copy or extra goroutine.
func (c *Cache) load(h Key, key []byte, loc blobstore.Location) (memoryValue, error) {
	// Another flight may have completed after this caller's initial lookup.
	if c.memoryHits {
		if it, ok := c.index.get(h); ok && it.loc == loc && it.mem.pin() {
			return it.mem, nil
		}
	}
	mem, buf, err := c.mem.alloc(loc.Size(), false, &h)
	if err != nil {
		return memoryValue{}, err
	}
	value, err := c.store.Read(loc, key, buf)
	if err == nil {
		mem.data = value
		if c.memoryHits {
			c.index.cache(h, loc, mem)
		}
		return mem, nil
	}
	mem.unpin()
	if errors.Is(err, ErrBusy) || errors.Is(err, ErrClosed) {
		return memoryValue{}, err
	}
	c.index.deleteIfAt(h, loc)
	if errors.Is(err, blobstore.ErrCorrupt) {
		c.stats.corrupt.Add(1)
	}
	if errors.Is(err, blobstore.ErrNotFound) || err == blobstore.ErrCorrupt {
		return memoryValue{}, ErrNotFound
	}
	return memoryValue{}, errors.Join(ErrNotFound, err)
}
