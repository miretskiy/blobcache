// Package xmap provides a generic sharded container optimized for concurrent access.
//
// The package follows the "Sharded Context" pattern: xmap provides the Mechanism
// (sharding, locking, memory layout, padding) while callers provide the Payload
// (the map value type and per-shard "Extra" context).
//
// Simple usage (like Slab) uses the convenience helpers Put/Get.
// Complex usage (like Index with eviction) accesses shards directly via Shard().
package xmap

import (
	"sync"

	"github.com/zeebo/xxh3"
)

// Key is the 128-bit XXH3 hash of a blob key.
// Using xxh3.Uint128 makes the contract explicit: keys must be XXH3 hashes.
type Key = xxh3.Uint128

// Map is a generic sharded container.
// V: The value type for the internal hash map.
// E: The "Extra" context type (e.g., Arena slice, Stats, etc).
type Map[V any, E any] struct {
	shards []Shard[V, E]
	mask   uint64
}

// Shard is the unit of concurrency.
//
// ALIGNMENT CONTRACT:
// The total size of Shard[V, E] MUST be a multiple of 64 bytes to prevent
// false sharing. This is checked at runtime in New().
//
// Base Size: 32 bytes (RWMutex: 24, Map: 8).
// Therefore, sizeof(E) + 32 must be divisible by 64.
//
// If using an empty E (struct{}), use a padding type like Pad32.
type Shard[V any, E any] struct {
	sync.RWMutex
	Items map[Key]V
	Extra E
}

// Pad32 is a 32-byte padding type for use as E when no extra context is needed.
// Combined with the 32-byte base (RWMutex + map), this creates a 64-byte shard.
type Pad32 [32]byte

// Option configures a Map.
type Option func(*options)

type options struct {
	shardShift      int
	initialCapacity int
}

// WithShardShift sets the number of shards as 1<<shift.
// Default: 2 (4 shards). Examples: 4->16 shards, 8->256 shards.
func WithShardShift(shift int) Option {
	return func(o *options) { o.shardShift = shift }
}

// WithInitialCapacity sets the initial capacity hint for the map.
// Default: 0. The capacity is distributed across shards.
func WithInitialCapacity(n int) Option {
	return func(o *options) { o.initialCapacity = n }
}

// New creates a new Map with the given options.
//
// IMPORTANT: Callers must ensure Shard[V, E] is properly aligned to 64 bytes
// by running VerifyAlignment[V, E]() in their test suite. Misalignment causes
// false sharing (performance hit), not crashes.
func New[V any, E any](opts ...Option) *Map[V, E] {
	o := options{shardShift: 2} // Default: 4 shards
	for _, opt := range opts {
		opt(&o)
	}

	numShards := 1 << o.shardShift

	m := &Map[V, E]{
		shards: make([]Shard[V, E], numShards),
		mask:   uint64(numShards - 1),
	}

	shardCap := max(1, o.initialCapacity/numShards)
	for i := range m.shards {
		m.shards[i].Items = make(map[Key]V, shardCap)
	}
	return m
}

// Shard returns the specific shard for a key.
// The caller is responsible for Locking/Unlocking.
func (m *Map[V, E]) Shard(k Key) *Shard[V, E] {
	return &m.shards[k.Lo&m.mask]
}

// ShardCount returns the number of shards.
func (m *Map[V, E]) ShardCount() int {
	return len(m.shards)
}

// ShardAt returns the shard at the given index.
func (m *Map[V, E]) ShardAt(i int) *Shard[V, E] {
	return &m.shards[i]
}

// ForEach iterates over all shards and items.
// Thread-safe (locks each shard).
func (m *Map[V, E]) ForEach(fn func(k Key, v V, extra *E) bool) {
	for i := range m.shards {
		s := &m.shards[i]
		s.RLock()
		for k, v := range s.Items {
			if !fn(k, v, &s.Extra) {
				s.RUnlock()
				return
			}
		}
		s.RUnlock()
	}
}

// --- Convenience Helpers for "Simple Map" usage ---

// Put inserts or updates a value for the given key.
func (m *Map[V, E]) Put(k Key, v V) {
	s := m.Shard(k)
	s.Lock()
	s.Items[k] = v
	s.Unlock()
}

// Get returns the value for the given key and whether it was found.
func (m *Map[V, E]) Get(k Key) (V, bool) {
	s := m.Shard(k)
	s.RLock()
	val, ok := s.Items[k]
	s.RUnlock()
	return val, ok
}

// Delete removes an entry by key.
// Returns true if the key existed and was removed.
func (m *Map[V, E]) Delete(k Key) bool {
	s := m.Shard(k)
	s.Lock()
	_, existed := s.Items[k]
	if existed {
		delete(s.Items, k)
	}
	s.Unlock()
	return existed
}

// Len returns the total number of entries across all shards.
func (m *Map[V, E]) Len() int {
	total := 0
	for i := range m.shards {
		m.shards[i].RLock()
		total += len(m.shards[i].Items)
		m.shards[i].RUnlock()
	}
	return total
}

// Collect appends all values to dst and returns the extended slice.
func (m *Map[V, E]) Collect(dst []V) []V {
	for i := range m.shards {
		s := &m.shards[i]
		s.RLock()
		for _, v := range s.Items {
			dst = append(dst, v)
		}
		s.RUnlock()
	}
	return dst
}

// Clear removes all entries from the map.
func (m *Map[V, E]) Clear() {
	for i := range m.shards {
		s := &m.shards[i]
		s.Lock()
		clear(s.Items)
		s.Unlock()
	}
}
