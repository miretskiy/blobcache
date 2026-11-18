package blobcache

import (
	"math/bits"
	"sync/atomic"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/collections"
)

// Metadata stays in the sharded map by value, including disk-only records.
// resident identifies a slot in the shard's SIEVE queue; zero means disk-only.
// A deleted header marks a resident value without a usable disk copy. An entry
// whose position the log has overwritten is dead unless it is resident.
// Neither metadata nor queue entries require an individual heap allocation.
type memoryEntry struct {
	header   blobstore.RecordHeader
	resident collections.Handle
}

// Only resident records occupy queue slots. Handles survive slice growth;
// pointers to residentEntry must not escape the shard lock. Reads atomically
// mark visited and pin buffer while holding the shard's read lock.
// Writers hold the exclusive lock, so they may move slots and clear visited.
type residentEntry struct {
	key     blobstore.KeyHash
	buffer  *alloc.ReadOnlyBuffer
	visited uint32
	pending bool // Borrowed from Put until completion transfers ownership to this entry.
}

type memoryQueue struct {
	list          collections.List[residentEntry]
	hand          collections.Handle
	residentBytes int64
	pendingBytes  int64    // resident bytes still borrowed by Put: not evictable
	_             [24]byte // Together with the shard's lock and map: 128 bytes.
}

type memoryCache struct {
	entries   *collections.ShardedMap[blobstore.KeyHash, memoryEntry, memoryQueue]
	next      atomic.Uint64
	sweepNext atomic.Uint64
	inline    int64                             // largest value kept resident
	live      func(blobstore.RecordHeader) bool // the log still holds the record
}

func newMemoryCache(inline int64) memoryCache {
	return memoryCache{entries: collections.NewShardedMapWithExtra[blobstore.KeyHash, memoryEntry](
		4, memoryQueue{},
		collections.WithHasher(func(key blobstore.KeyHash) uint64 { return key }),
	), inline: inline, live: func(blobstore.RecordHeader) bool { return true }}
}

// resident reports whether a value of this record's size stays in memory.
func (m *memoryCache) resident(h blobstore.RecordHeader) bool {
	return int64(h.PhysicalLen) <= m.inline
}

// sweep drops up to n disk-only entries the log has overwritten, from one
// shard in turn. Map iteration starts at a random entry, so repeated sweeps
// sample whole shards; Put calls it, which keeps dead entries a small share
// of the map without a full pass or a per-record eviction structure.
func (m *memoryCache) sweep(n int) {
	s := m.entries.ShardAt(int(m.sweepNext.Add(1) % uint64(m.entries.ShardCount())))
	s.Lock()
	defer s.Unlock()
	for key, e := range s.Items {
		if n == 0 {
			return
		}
		n--
		if e.resident == 0 && !m.live(e.header) {
			delete(s.Items, key)
		}
	}
}

// lookup pins the resident buffer before releasing the shard lock, so neither
// replacement nor reclamation can release it between lookup and acquisition.
// A nil buffer means the record is disk-only.
func (m *memoryCache) lookup(key blobstore.KeyHash) (*alloc.ReadOnlyBuffer, blobstore.RecordHeader, bool) {
	s := m.entries.Shard(key)
	s.RLock()
	defer s.RUnlock()
	e, ok := s.Items[key]
	var buffer *alloc.ReadOnlyBuffer
	if e.resident != 0 {
		resident := s.Extra.list.Value(e.resident)
		buffer = resident.buffer
		buffer.Retain()
		atomic.StoreUint32(&resident.visited, 1)
	}
	return buffer, e.header, ok
}

// replace borrows a pending buffer; complete accepts ownership after I/O.
// Nonpending buffers transfer ownership immediately. Replacing a pending entry
// leaves its buffer with Put, whose completion will release it.
func (m *memoryCache) replace(key blobstore.KeyHash, buffer *alloc.ReadOnlyBuffer, header blobstore.RecordHeader, pending bool) {
	s := m.entries.Shard(key)
	s.Lock()
	defer s.Unlock()
	q := &s.Extra
	old := s.Items[key]
	if old.header.Position > header.Position {
		if buffer != nil && !pending {
			buffer.Release()
		}
		return // An older write completed after its replacement.
	}
	if old.resident != 0 {
		owned := !q.list.Value(old.resident).pending
		oldBuffer := q.remove(old.resident)
		if owned {
			oldBuffer.Release()
		}
	}
	e := memoryEntry{header: header}
	if buffer != nil {
		e.resident = q.list.PushBack(residentEntry{key: key, buffer: buffer, pending: pending})
		q.residentBytes += int64(buffer.Cap())
		if pending {
			q.pendingBytes += int64(buffer.Cap())
		}
	}
	s.Items[key] = e
}

// complete accepts Put's allocation ownership if this is still the current
// inline value. A failed disk write remains readable until normal eviction.
// False leaves ownership with Put, which releases a replaced buffer.
func (m *memoryCache) complete(h blobstore.RecordHeader, err error) bool {
	s := m.entries.Shard(h.KeyHash())
	s.Lock()
	defer s.Unlock()
	e, ok := s.Items[h.KeyHash()]
	if !ok || e.header.Position != h.Position || e.resident == 0 {
		return false
	}
	resident := s.Extra.list.Value(e.resident)
	resident.pending = false
	s.Extra.pendingBytes -= int64(resident.buffer.Cap())
	if err != nil {
		e.header.Flags |= blobstore.FlagDeleted
		s.Items[h.KeyHash()] = e
	}
	return true
}

// retire forgets this disk record without removing a newer mapping for its key.
// Resident bytes remain valid; their metadata leaves when the cache drops its pin.
func (m *memoryCache) retire(key blobstore.KeyHash, position uint64) {
	s := m.entries.Shard(key)
	s.Lock()
	defer s.Unlock()
	e, ok := s.Items[key]
	if !ok || e.header.Position != position {
		return
	}
	if e.resident == 0 {
		delete(s.Items, key)
	} else {
		e.header.Flags |= blobstore.FlagDeleted
		s.Items[key] = e
	}
}

func (q *memoryQueue) evictableBytes() int64 { return q.residentBytes - q.pendingBytes }

// remove transfers the queue's buffer reference to the caller.
func (q *memoryQueue) remove(handle collections.Handle) *alloc.ReadOnlyBuffer {
	if q.hand == handle {
		q.hand, _ = q.list.Next(handle)
	}
	entry := q.list.Value(handle)
	buffer := entry.buffer
	if entry.pending {
		q.pendingBytes -= int64(buffer.Cap())
	}
	q.list.Remove(handle)
	q.residentBytes -= int64(buffer.Cap())
	return buffer
}

// reclaimCacheMemory distributes a byte target in proportion to each shard's
// evictable bytes, using SIEVE within each shard. Values whose disk write is
// pending are borrowed by Put and not evictable, so a cache full of them costs
// a read of each shard's counters rather than a walk of its queue. Only
// final-reference releases count; pinned victims are demoted too but
// contribute no reusable space. Whole buffers can overshoot each shard's share.
// Disk metadata stays indexed.
//
// Unmet shares are redistributed among remaining residents. At most ShardCount
// passes run, each visiting at most twice each shard's initial resident count,
// so concurrent insertions cannot keep reclamation running indefinitely. Byte
// snapshots can change concurrently; the result does not reserve released space.
func (m *memoryCache) reclaimCacheMemory(bytes int64) (released int64) {
	metrics.ReclaimCalls.Add(1)
	metrics.ReclaimRequestedBytes.Add(max(0, bytes))
	defer func() { metrics.ReclaimReleasedBytes.Add(released) }()
	count := m.entries.ShardCount()
	start := int((m.next.Add(1) - 1) % uint64(count))
	for pass := 0; pass < count && released < bytes; pass++ {
		var total int64
		for i := range count {
			s := m.entries.ShardAt(i)
			s.RLock()
			total += s.Extra.evictableBytes()
			s.RUnlock()
		}
		if total == 0 {
			break
		}
		target := min(bytes-released, total)
		for i := range count {
			s := m.entries.ShardAt((start + i) % count)
			s.Lock()
			// ceil(target * evictable / total), without overflowing the
			// multiplication for large memory budgets.
			hi, lo := bits.Mul64(uint64(target), uint64(s.Extra.evictableBytes()))
			share, remainder := bits.Div64(hi, lo, uint64(total))
			if remainder != 0 {
				share++
			}
			released += m.reclaimShard(s, int64(share))
			s.Unlock()
		}
	}
	return released
}

// reclaimShard runs with the shard locked by the caller.
func (m *memoryCache) reclaimShard(s *collections.MapShard[blobstore.KeyHash, memoryEntry, memoryQueue], target int64) (released int64) {
	q := &s.Extra
	var visited, evicted, spared, pending, pinned int64
	defer func() {
		metrics.ReclaimVisited.Add(visited)
		metrics.ReclaimEvicted.Add(evicted)
		metrics.ReclaimSpared.Add(spared)
		metrics.ReclaimPending.Add(pending)
		metrics.ReclaimPinned.Add(pinned)
	}()
	for remaining := 2 * q.list.Len(); remaining > 0 && q.list.Len() != 0 && released < target; remaining-- {
		visited++
		if q.hand == 0 {
			q.hand, _ = q.list.Front()
		}
		handle := q.hand
		e := q.list.Value(handle)
		q.hand, _ = q.list.Next(handle)
		if e.pending {
			pending++
			continue
		}
		if e.visited != 0 {
			e.visited = 0
			spared++
			continue
		}
		entry := s.Items[e.key]
		if entry.header.Flags.IsDeleted() || !m.live(entry.header) {
			delete(s.Items, e.key)
		} else {
			entry.resident = 0
			s.Items[e.key] = entry
		}
		freed := q.remove(handle).Release()
		evicted++
		if freed == 0 {
			pinned++
		}
		released += freed
	}
	return released
}

func (m *memoryCache) clear() {
	for i := range m.entries.ShardCount() {
		s := m.entries.ShardAt(i)
		s.Lock()
		for _, e := range s.Extra.list.All() {
			if !e.pending {
				e.buffer.Release()
			}
		}
		clear(s.Items)
		s.Extra = memoryQueue{}
		s.Unlock()
	}
}

// startCacheReclaim never blocks; a queued request covers this one.
func (c *Cache) startCacheReclaim(size int64) {
	select {
	case c.reclaimRequests <- size:
	default:
	}
}

func (c *Cache) reclaimLoop() {
	for size := range c.reclaimRequests {
		capacity := c.memory.Capacity()
		available := c.memory.Available()
		if size == 0 && available >= capacity/5 {
			continue
		}
		target := min(capacity, max(capacity/4, size))
		if needed := target - available; needed > 0 {
			c.values.reclaimCacheMemory(needed)
		}
	}
}
