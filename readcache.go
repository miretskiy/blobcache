package blobcache

import (
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/zeebo/xxh3"
)

// prefetchChunkSize is the alignment granularity for Direct I/O read-ahead.
// 64KB aligns with NVMe optimal I/O size. Blobs smaller than this benefit
// from temporal prefetch (neighboring records in the same chunk are also cached).
const prefetchChunkSize = 64 * 1024

// ReadCacheStats holds a point-in-time snapshot of read cache counters.
type ReadCacheStats struct {
	Hits      int64 // Lookup found the key in a sealed or active slab
	Misses    int64 // Lookup called (cache miss, attempted disk fetch)
	Inserts   int64 // Records successfully inserted into the cache
	Evictions int64 // Slabs evicted to free pool arenas
	Skipped   int64 // Items rejected by admission policy (too large)
	Slabs     int   // Current number of sealed slabs
}

// ReadCache is an optional second-tier user-space read cache for disk-resident
// blobs. Disabled by default.
//
// The primary in-memory read path is Librarian, which provides zero-copy access
// to recently written data still in mmap'd slabs (~seconds of writes). Librarian
// covers the hot write-after-read window with sub-microsecond latency and needs
// no additional memory beyond the write-path arenas.
//
// ReadCache exists for a narrower scenario: reads of data that has already fallen
// out of the Librarian window but is still accessed frequently enough to justify
// caching in user-space rather than relying solely on the kernel page cache.
// Typical use case: temporally distant reads-after-write, or workloads where the
// kernel page cache is under pressure from other processes.
//
// Internally, ReadCache composes a Librarian for its sealed slab list, giving it
// the same lock-free Acquire and FIFO eviction. The only addition is an active
// slab for inserting records populated from disk reads (via Archivist).
//
// Thread safety:
//   - Acquire is lock-free for sealed slabs (delegated to Librarian)
//   - Insert acquires mu for active slab writes
type ReadCache struct {
	// Sealed slab storage with FIFO eviction (lock-free Acquire).
	lib *Librarian

	// Active slab for inserting new entries.
	mu     sync.Mutex
	active *ActiveSlab

	// Dedicated arena pool (separate from write-path pool).
	pool        *MmapPool
	slabSize    int64
	maxItemSize int64 // Items larger than this bypass the cache (0 = no limit)

	// Dependencies.
	errReporter ErrorReporter

	// Stats counters (atomics for lock-free updates).
	// Evictions are read from lib.
	hits    atomic.Int64
	misses  atomic.Int64
	inserts atomic.Int64
	skipped atomic.Int64

	closed atomic.Bool
}

// NewReadCache creates a read cache with the given configuration.
// slabSize: size of each mmap arena (typically WriteBufferSize).
// maxSlabs: total number of arenas in the pool (1 active + rest sealed).
// Total read cache memory = maxSlabs * slabSize.
func NewReadCache(
	slabSize int64,
	maxSlabs int,
	maxItemSize int64,
	reporter ErrorReporter,
) *ReadCache {
	rc := &ReadCache{
		// maxSlabs-1 sealed slots (1 reserved for active).
		lib: NewLibrarian(maxSlabs-1, reporter),
		// +1 over-provision: during eviction we hold the victim slab open
		// while writing into the new active slab, requiring maxSlabs+1 arenas.
		pool:        NewMmapPool("readcache", slabSize, maxSlabs+1),
		slabSize:    slabSize,
		maxItemSize: maxItemSize,
		errReporter: reporter,
	}

	rc.active = rc.newActiveSlab()
	return rc
}

// newActiveSlab allocates a fresh slab from the pool.
// Unlike write-path slabs, read cache slabs do NOT reserve FileHeaderSize
// at the start — they are never flushed to segment files.
func (rc *ReadCache) newActiveSlab() *ActiveSlab {
	buf := rc.pool.Acquire()
	return &ActiveSlab{
		SharedSlab: SharedSlab{
			buf:   buf,
			index: xmap.New[SlabEntry, xmap.Pad32](xmap.WithShardShift(4)), // 16 shards
			bloom: bloom.New(32_000, 0.01),                                 // 32k entries, 1% FPR
		},
		wPos:       0, // No file header for read cache slabs
		writesDone: newSignal(),
	}
}

// Acquire searches the read cache for the given key.
// Returns (data, storedKey, releaser, found).
//
// Lock-free for sealed slabs (delegated to Librarian).
// Briefly acquires mu to read the active slab pointer.
func (rc *ReadCache) Acquire(hashKey Key) ([]byte, []byte, Releaser, bool) {
	// 1. Check sealed slabs via Librarian (newest first, lock-free).
	if data, keyBytes, rel, found := rc.lib.Acquire(hashKey); found {
		return data, keyBytes, rel, true
	}

	// 2. Check active slab. We briefly hold mu to snapshot the pointer.
	// This is safe because Insert writes data (copy) BEFORE populating
	// bloom/index, so any entry visible to Acquire has complete data.
	// xmap.Map and bloom.Filter are both safe for concurrent Get/Put.
	rc.mu.Lock()
	active := rc.active
	rc.mu.Unlock()

	if active != nil {
		data, keyBytes, releaser, ok, errno := active.SharedSlab.Acquire(hashKey)
		if errno != base.ErrNone {
			rc.errReporter.ReportBlobError(hashKey, errno)
			return nil, nil, Releaser{}, false
		}
		if ok {
			return data, keyBytes, releaser, true
		}
	}

	return nil, nil, Releaser{}, false
}

// Insert copies a raw on-disk record into the read cache.
// rawRecord must be a complete record: [Header(42)][Key][Value].
// hashKey is the 128-bit xxhash of the key bytes.
//
// Thread-safe: acquires mu for slab allocation.
// Returns false if the record is too large for a slab or cache is closed.
func (rc *ReadCache) Insert(hashKey Key, rawRecord []byte) bool {
	if rc.closed.Load() {
		return false
	}

	recordLen := len(rawRecord)
	if int64(recordLen) > rc.slabSize {
		return false // Record larger than slab
	}
	if rc.maxItemSize > 0 && int64(recordLen) > rc.maxItemSize {
		rc.skipped.Add(1)
		return false // Rejected by admission policy
	}

	hdr, err := record.DecodeHeader(rawRecord[:record.HeaderSize])
	if err != nil {
		return false
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Try to allocate in the active slab.
	buf, pos := rc.active.Alloc(recordLen)
	if buf == nil {
		// Active slab full — seal and rotate.
		rc.sealAndRotateLocked()
		buf, pos = rc.active.Alloc(recordLen)
		if buf == nil {
			return false // Should not happen unless record > slabSize
		}
	}

	// Copy raw record into the mmap arena (zero-GC: data lives in mmap).
	copy(buf, rawRecord)

	// Build index entry. Reuse SlabEntry (same struct as write-path slabs).
	entry := SlabEntry{
		Header: hdr,
		Pos:    pos,
	}

	// Bloom BEFORE index (prevents false negatives during concurrent reads).
	rc.active.bloom.Add(hashKey)
	rc.active.index.Put(hashKey, entry)

	rc.inserts.Add(1)
	return true
}

// sealAndRotateLocked seals the current active slab, publishes it to the
// Librarian (which handles FIFO eviction), and acquires a new active slab.
//
// MUST be called with rc.mu held.
func (rc *ReadCache) sealAndRotateLocked() {
	old := rc.active
	if old == nil {
		return
	}

	// Freeze bloom for faster lookups (direct reads, no atomics).
	old.bloom.Freeze()

	// Publish to Librarian. It handles TryInc + FIFO eviction + Unpin victim.
	rc.lib.Publish(&old.SharedSlab)

	// Release our active reference.
	old.buf.Unpin()

	// Acquire new active slab from pool.
	rc.active = rc.newActiveSlab()
}

// Admissible returns true if a blob of the given physical size should be
// cached. Returns false for blobs that exceed the slab size or the
// configured maxItemSize admission threshold.
func (rc *ReadCache) Admissible(physLen int64) bool {
	if physLen > rc.slabSize {
		return false
	}
	if rc.maxItemSize > 0 && physLen > rc.maxItemSize {
		rc.skipped.Add(1)
		return false
	}
	return true
}

// PopulateChunk scans a raw disk buffer for valid records and inserts each
// one into the cache. Best-effort: parse errors are silently skipped.
//
// Records in segments are densely packed. The buffer may start mid-record,
// so we scan for RecordMagic (0xB10BCAFE) + HeaderCRC verification to find
// valid record boundaries. False positive rate is ~1/2^64 (Magic + CRC32).
//
// The caller (Archivist) owns the buffer and performs the single disk read.
// PopulateChunk does NO disk I/O.
func (rc *ReadCache) PopulateChunk(buf []byte) {
	bufLen := len(buf)
	offset := 0
	for offset+record.HeaderSize <= bufLen {
		hdr, err := record.DecodeHeader(buf[offset : offset+record.HeaderSize])
		if err != nil || !hdr.IsValid() {
			offset += 4
			continue
		}

		totalSize := hdr.TotalSize()
		if totalSize <= 0 || offset+totalSize > bufLen {
			break
		}

		rawRecord := buf[offset : offset+totalSize]
		keyStart := record.HeaderSize
		keyEnd := keyStart + int(hdr.KeyLen)
		if keyEnd > totalSize {
			offset += 4
			continue
		}

		h := xxh3.Hash128(rawRecord[keyStart:keyEnd])

		if !hdr.IsDeleted() && !hdr.HasError() {
			rc.Insert(h, rawRecord)
		}

		offset += totalSize
	}
}

// Invalidate removes a key from all slabs (sealed and active).
// Called by Cache.Delete to prevent serving stale data.
func (rc *ReadCache) Invalidate(key Key) {
	rc.lib.Invalidate(key)

	rc.mu.Lock()
	if rc.active != nil {
		rc.active.SharedSlab.Invalidate(key)
	}
	rc.mu.Unlock()
}

// Close releases all resources. All slabs are unpinned and arenas returned
// to the pool. The pool is then closed (munmaps all memory).
func (rc *ReadCache) Close() {
	if !rc.closed.CompareAndSwap(false, true) {
		return
	}

	// Release sealed slabs via Librarian.
	rc.lib.Close()

	// Release active slab.
	rc.mu.Lock()
	if rc.active != nil {
		rc.active.buf.Unpin()
		rc.active = nil
	}
	rc.mu.Unlock()

	// Release pool memory.
	rc.pool.Close()
}

// Stats returns a point-in-time snapshot of read cache counters.
func (rc *ReadCache) Stats() ReadCacheStats {
	return ReadCacheStats{
		Hits:      rc.hits.Load(),
		Misses:    rc.misses.Load(),
		Inserts:   rc.inserts.Load(),
		Evictions: rc.lib.evictions.Load(),
		Skipped:   rc.skipped.Load(),
		Slabs:     len(*rc.lib.view.Load()),
	}
}
