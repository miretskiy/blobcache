package blobcache

import (
	"slices"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/internal/xmap"
)

// Key is the 128-bit hash of a user key that the index is keyed by.
type Key = blobstore.KeyHash

// item locates a value on disk, in a pinned memory block, or both. The slice
// may only be used after pinning its block; eviction clears it before reuse.
type item struct {
	mem     memoryValue
	loc     blobstore.Location
	pending bool // disk I/O is still using the write buffer
}

func (it item) onDisk() bool { return !it.pending && it.loc != (blobstore.Location{}) }

// index maps key hashes to items: a 256-way sharded hash table.
type index struct {
	m *xmap.Map[item, indexShard]
}

// The retirement boundary shares the existing shard lock and padding.
type indexShard struct {
	before uint64
	_      [24]byte
}

func newIndex(capacity int) index {
	return index{m: xmap.New[item, indexShard](
		xmap.WithShardShift(8),
		xmap.WithInitialCapacity(capacity),
	)}
}

func (x *index) get(h Key) (item, bool) { return x.m.Get(h) }

func (x *index) len() int { return x.m.Len() }

// loaded installs a record found on disk by ReadIndex, which reports records in
// write order, so a later record of a key replaces an earlier one, as it
// would after another restart.
func (x *index) loaded(h Key, loc blobstore.Location) {
	s := x.m.Shard(h)
	s.Lock()
	if loc.Segment() >= s.Extra.before {
		s.Items[h] = item{loc: loc}
	}
	s.Unlock()
}

// put publishes the reserved disk location immediately. It is a candidate
// for verified reads after completion. Eviction can clear memory immediately;
// until the write completes such an entry is a miss, not readable disk data.
func (x *index) put(h Key, mem memoryValue, loc blobstore.Location) {
	s := x.m.Shard(h)
	s.Lock()
	if loc.Segment() >= s.Extra.before {
		if mem.block.refs.Load() < 0 {
			mem = memoryValue{}
		}
		s.Items[h] = item{mem: mem, loc: loc, pending: true}
	}
	s.Unlock()
}

// completed makes the disk candidate readable, conditionally on location.
// Eviction and newer writes cannot be undone by a delayed completion.
func (x *index) completed(h Key, loc blobstore.Location) {
	s := x.m.Shard(h)
	s.Lock()
	if it, ok := s.Items[h]; ok && it.loc == loc {
		it.pending = false
		s.Items[h] = it
	}
	s.Unlock()
}

// evictMemory clears references to an entire batch of retired memory blocks.
// Group outside locks; each affected index shard is locked only once. A later
// publication from another block survives, and disk locations remain indexed.
func (x *index) evictMemory(blocks []*memoryBlock) {
	type removal struct {
		key   Key
		block *memoryBlock
	}
	groups := make([][]removal, x.m.ShardCount())
	for _, b := range blocks {
		for _, key := range b.keys {
			i := key.Lo & uint64(len(groups)-1)
			groups[i] = append(groups[i], removal{key, b})
		}
	}
	for i, entries := range groups {
		if len(entries) == 0 {
			continue
		}
		shard := x.m.ShardAt(i)
		shard.Lock()
		for _, entry := range entries {
			it, ok := shard.Items[entry.key]
			if !ok || it.mem.block != entry.block {
				continue
			}
			it.mem = memoryValue{}
			shard.Items[entry.key] = it
		}
		shard.Unlock()
	}
}

// cache records that the record at loc is now also in memory at mem, read
// there from disk, unless the key has changed since.
func (x *index) cache(h Key, loc blobstore.Location, mem memoryValue) {
	s := x.m.Shard(h)
	s.Lock()
	if it, ok := s.Items[h]; ok && it.loc == loc && mem.block.refs.Load() > 0 {
		it.mem = mem
		s.Items[h] = it
	}
	s.Unlock()
}

// deleteIfAt removes h only if it still names the record at loc, which
// failed to read. Every background removal uses it, so it
// never discards a newer write of the key.
func (x *index) deleteIfAt(h Key, loc blobstore.Location) {
	s := x.m.Shard(h)
	s.Lock()
	if it, ok := s.Items[h]; ok && it.loc == loc {
		delete(s.Items, h)
	}
	s.Unlock()
}

// evicted is the index-owned callback. It schedules one sweep for the entire
// retired segment batch; blobstore knows nothing about its threading or shards.
// Each shard boundary prevents late completions from restoring swept locations.
func (x *index) evicted(segments blobstore.Eviction) {
	if len(segments) == 0 {
		return
	}
	// FIFO batches retire a prefix; gaps in IDs and notification reordering
	// do not require a set or another index.
	before := slices.Max(segments) + 1
	go func() {
		for i := 0; i < x.m.ShardCount(); i++ {
			shard := x.m.ShardAt(i)
			shard.Lock()
			if before <= shard.Extra.before {
				shard.Unlock()
				continue
			}
			shard.Extra.before = before
			for h, it := range shard.Items {
				if it.loc.Segment() < before {
					delete(shard.Items, h)
				}
			}
			shard.Unlock()
		}
	}()
}
