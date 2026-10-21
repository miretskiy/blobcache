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
	buffer  *alloc.Buffer
	visited uint32
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
		4, 0, func(key blobstore.KeyHash) uint64 { return key.Lo }, memoryQueue{},
	)}
}

// lookup pins the resident buffer before releasing the shard lock, so neither
// replacement nor reclamation can release it between lookup and acquisition.
func (m *memoryCache) lookup(key blobstore.KeyHash) (*alloc.Buffer, blobstore.RecordHeader, bool) {
	s := m.entries.Shard(key)
	s.RLock()
	defer s.RUnlock()
	e, ok := s.Items[key]
	var buffer *alloc.Buffer
	if e.resident != 0 {
		resident := s.Extra.list.Value(e.resident)
		buffer = resident.buffer
		buffer.TryInc() // The queue owns a reference while the shard is locked.
		atomic.StoreUint32(&resident.visited, 1)
	}
	return buffer, e.header, ok
}

// replace takes ownership of buffer and updates disk metadata even if buffer
// is nil. Replacing or demoting a resident record returns its slot for reuse.
func (m *memoryCache) replace(key blobstore.KeyHash, buffer *alloc.Buffer, header blobstore.RecordHeader) {
	s := m.entries.Shard(key)
	s.Lock()
	defer s.Unlock()
	q := &s.Extra
	old := s.Items[key]
	if old.header.ObjectID > header.ObjectID {
		if buffer != nil {
			buffer.Unpin()
		}
		return // An older write completed after its replacement.
	}
	if old.resident != 0 {
		oldBuffer, _ := q.remove(old.resident)
		oldBuffer.Unpin()
	}
	e := memoryEntry{header: header}
	if buffer != nil {
		e.resident = q.list.PushBack(residentEntry{key: key, buffer: buffer})
		q.residentBytes += buffer.Cap()
	}
	s.Items[key] = e
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
func (q *memoryQueue) remove(handle collections.Handle) (*alloc.Buffer, int64) {
	if q.hand == handle {
		q.hand, _ = q.list.Next(handle)
	}
	buffer := q.list.Value(handle).buffer
	size := buffer.Cap()
	q.list.Remove(handle)
	q.residentBytes -= size
	return buffer, size
}

// reclaimCacheMemory distributes a byte target in proportion to each shard's
// resident bytes, using SIEVE within each shard. Only final-reference releases
// count; pinned victims are demoted too but contribute no reusable space.
// Whole buffers can overshoot each shard's share. Disk metadata stays indexed.
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
		buffer, size := q.remove(handle)
		if buffer.Unpin() {
			released += size
		}
	}
	return released
}

func (m *memoryCache) clear() {
	for i := range m.entries.ShardCount() {
		s := m.entries.ShardAt(i)
		s.Lock()
		for _, e := range s.Extra.list.All() {
			e.buffer.Unpin()
		}
		clear(s.Items)
		s.Extra = memoryQueue{}
		s.Unlock()
	}
}
