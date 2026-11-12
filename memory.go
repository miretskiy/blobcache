package blobcache

import (
	"math/bits"
	"sync/atomic"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/collections"
)

// Metadata stays in the sharded map by value, including disk-only records.
// resident identifies a slot in the shard's SIEVE queue; zero means disk-only.
// A deleted header marks a resident value whose disk record has been retired.
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
	_             [32]byte // Together with the shard's lock and map: 128 bytes.
}

type memoryCache struct {
	entries *collections.ShardedMap[blobstore.KeyHash, memoryEntry, memoryQueue]
	next    atomic.Uint64
}

func newMemoryCache() memoryCache {
	return memoryCache{entries: collections.NewShardedMapWithExtra[blobstore.KeyHash, memoryEntry](
		4, memoryQueue{},
		collections.WithHasher(func(key blobstore.KeyHash) uint64 { return key }),
	)}
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
	if old.header.ObjectID > header.ObjectID {
		if buffer != nil && !pending {
			buffer.Release()
		}
		return // An older write completed after its replacement.
	}
	if old.resident != 0 {
		owned := !q.list.Value(old.resident).pending
		oldBuffer, _ := q.remove(old.resident)
		if owned {
			oldBuffer.Release()
		}
	}
	e := memoryEntry{header: header}
	if buffer != nil {
		e.resident = q.list.PushBack(residentEntry{key: key, buffer: buffer, pending: pending})
		q.residentBytes += int64(buffer.Cap())
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
	if !ok || e.header.ObjectID != h.ObjectID || e.resident == 0 {
		return false
	}
	s.Extra.list.Value(e.resident).pending = false
	if err != nil {
		e.header.Flags |= blobstore.FlagDeleted
		s.Items[h.KeyHash()] = e
	}
	return true
}

// admit shares a validated inline read's buffer with the cache. Only the first
// reader of the current disk-only record adds a reference; replacement and
// retirement must not be undone by a read that was already in flight.
func (m *memoryCache) admit(h blobstore.RecordHeader, buffer *alloc.ReadOnlyBuffer) {
	if h.Flags.IsExternal() {
		return
	}
	s := m.entries.Shard(h.KeyHash())
	s.Lock()
	defer s.Unlock()
	e, ok := s.Items[h.KeyHash()]
	if !ok || e.header.ObjectID != h.ObjectID || e.resident != 0 {
		return
	}
	q := &s.Extra
	e.resident = q.list.PushBack(residentEntry{key: h.KeyHash(), buffer: buffer.Retain(), visited: 1})
	q.residentBytes += int64(buffer.Cap())
	s.Items[h.KeyHash()] = e
}

// retire forgets this disk record without removing a newer mapping for its key.
// Resident bytes remain valid; their metadata leaves when the cache drops its pin.
func (m *memoryCache) retire(key blobstore.KeyHash, objectID uint64) {
	s := m.entries.Shard(key)
	s.Lock()
	defer s.Unlock()
	e, ok := s.Items[key]
	if !ok || e.header.ObjectID != objectID {
		return
	}
	if e.resident == 0 {
		delete(s.Items, key)
	} else {
		e.header.Flags |= blobstore.FlagDeleted
		s.Items[key] = e
	}
}

// remove transfers the queue's buffer reference and its byte count to the caller.
func (q *memoryQueue) remove(handle collections.Handle) (*alloc.ReadOnlyBuffer, int64) {
	if q.hand == handle {
		q.hand, _ = q.list.Next(handle)
	}
	buffer := q.list.Value(handle).buffer
	size := int64(buffer.Cap())
	q.list.Remove(handle)
	q.residentBytes -= size
	return buffer, size
}

// reclaimCacheMemory distributes a byte target in proportion to each shard's
// resident bytes, using SIEVE within each shard. Only final-reference releases
// count; pinned victims are demoted too but contribute no reusable space.
// Pending disk writes are skipped. Whole buffers can overshoot each shard's
// share. Disk metadata stays indexed.
//
// Unmet shares are redistributed among remaining residents. At most ShardCount
// passes run, each visiting at most twice each shard's initial resident count,
// so concurrent insertions cannot keep reclamation running indefinitely. Byte
// snapshots can change concurrently; the result does not reserve released space.
func (m *memoryCache) reclaimCacheMemory(bytes int64) (released int64) {
	count := m.entries.ShardCount()
	start := int((m.next.Add(1) - 1) % uint64(count))
	for pass := 0; pass < count && released < bytes; pass++ {
		var total int64
		for i := range count {
			s := m.entries.ShardAt(i)
			s.RLock()
			total += s.Extra.residentBytes
			s.RUnlock()
		}
		if total == 0 {
			break
		}
		target := min(bytes-released, total)
		for i := range count {
			s := m.entries.ShardAt((start + i) % count)
			s.Lock()
			// ceil(target * residentBytes / total), without overflowing the
			// multiplication for large memory budgets.
			hi, lo := bits.Mul64(uint64(target), uint64(s.Extra.residentBytes))
			share, remainder := bits.Div64(hi, lo, uint64(total))
			if remainder != 0 {
				share++
			}
			released += reclaimShardMemory(s, int64(share))
			s.Unlock()
		}
	}
	return released
}

// reclaimShardMemory runs with the shard locked by the caller.
func reclaimShardMemory(s *collections.MapShard[blobstore.KeyHash, memoryEntry, memoryQueue], target int64) (released int64) {
	q := &s.Extra
	for remaining := 2 * q.list.Len(); remaining > 0 && q.list.Len() != 0 && released < target; remaining-- {
		if q.hand == 0 {
			q.hand, _ = q.list.Front()
		}
		handle := q.hand
		e := q.list.Value(handle)
		q.hand, _ = q.list.Next(handle)
		if e.pending {
			continue
		}
		if e.visited != 0 {
			e.visited = 0
			continue
		}
		entry := s.Items[e.key]
		if entry.header.Flags.IsDeleted() {
			delete(s.Items, e.key)
		} else {
			entry.resident = 0
			s.Items[e.key] = entry
		}
		buffer, _ := q.remove(handle)
		released += buffer.Release()
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
