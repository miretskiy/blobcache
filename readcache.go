package blobcache

import (
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/zeebo/xxh3"
)

// prefetchChunkSize is the alignment granularity for Direct I/O read-ahead.
// 64KB aligns with NVMe optimal I/O size. Blobs smaller than this benefit
// from temporal prefetch (neighboring records in the same chunk are also cached).
const prefetchChunkSize = 64 * 1024

// ChunkReadFunc reads raw bytes from a segment at the given offset.
// buf must be page-aligned for Direct I/O. Returns bytes read.
type ChunkReadFunc func(segID uint32, buf []byte, offset int64) (int, error)

// ReadCacheStats holds a point-in-time snapshot of read cache counters.
type ReadCacheStats struct {
	Hits      int64 // Lookup found the key in a sealed or active slab
	Misses    int64 // Lookup called (cache miss, attempted disk fetch)
	Inserts   int64 // Records successfully inserted into the cache
	Evictions int64 // Slabs evicted to free pool arenas
	Compacted int64 // Items preserved via CompactStrategy during eviction
	Skipped   int64 // Items rejected by admission policy (too large)
	Slabs     int   // Current number of sealed slabs
}

// ReadCache is a user-space read cache backed by mmap arenas.
// It sits between the index lookup and disk I/O in the read path.
//
// Architecture:
//   - One "active" slab for writing new entries (sequential Alloc)
//   - A list of sealed ReadSlabs for lock-free lookups
//   - A dedicated MmapPool providing bounded mmap arenas
//   - Slab-level eviction: coldest slab is evicted when pool is exhausted
//
// Thread safety:
//   - Acquire is lock-free (atomic pointer to sealed list + TryInc on buffers)
//   - Insert acquires mu for active slab writes
type ReadCache struct {
	// Sealed slabs visible to readers. Atomic swap for lock-free reads.
	// List is ordered newest-first (same pattern as Librarian).
	sealed atomic.Pointer[[]*ReadSlab]

	// Active slab for inserting new entries.
	mu     sync.Mutex
	active *ActiveSlab

	// Dedicated arena pool (separate from write-path pool).
	pool        *MmapPool
	slabSize    int64
	maxSlabs    int
	maxItemSize int64 // Items larger than this bypass the cache (0 = no limit)

	// Sharded coalescing map for thundering herd protection.
	flights *inflightGroup

	// Pluggable eviction strategy (default: DropStrategy).
	evictor EvictionStrategy

	// Dependencies.
	readChunk   ChunkReadFunc
	errReporter ErrorReporter

	// Stats counters (atomics for lock-free updates).
	hits      atomic.Int64
	misses    atomic.Int64
	inserts   atomic.Int64
	evictions atomic.Int64
	compacted atomic.Int64
	skipped   atomic.Int64

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
	readChunk ChunkReadFunc,
	reporter ErrorReporter,
	evictor EvictionStrategy,
) *ReadCache {
	if evictor == nil {
		evictor = CompactStrategy{}
	}

	rc := &ReadCache{
		// +1 over-provision: during compaction we hold the victim slab open
		// while writing into the new active slab, requiring maxSlabs+1 arenas.
		pool:        NewMmapPool("readcache", slabSize, maxSlabs+1),
		slabSize:    slabSize,
		maxSlabs:    maxSlabs,
		maxItemSize: maxItemSize,
		flights:     newInflightGroup(),
		evictor:     evictor,
		readChunk:   readChunk,
		errReporter: reporter,
	}

	empty := make([]*ReadSlab, 0)
	rc.sealed.Store(&empty)
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
// Lock-free for sealed slabs (atomic pointer + TryInc).
// Briefly acquires mu to read the active slab pointer.
func (rc *ReadCache) Acquire(hashKey Key) ([]byte, []byte, Releaser, bool) {
	// 1. Check sealed slabs (newest first, same pattern as Librarian).
	list := *rc.sealed.Load()
	for _, slab := range list {
		data, keyBytes, releaser, ok, errno := slab.SharedSlab.Acquire(hashKey)
		if errno != base.ErrNone {
			rc.errReporter.ReportBlobError(hashKey, errno)
			return nil, nil, Releaser{}, false
		}
		if ok {
			slab.RecordHit(hashKey)
			return data, keyBytes, releaser, true
		}
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
// sealed list, and acquires a new active slab from the pool.
//
// If the sealed list reaches maxSlabs-1, the coldest slab is evicted to
// free a pool arena.
//
// MUST be called with rc.mu held.
func (rc *ReadCache) sealAndRotateLocked() {
	old := rc.active
	if old == nil {
		return
	}

	// Freeze bloom for faster lookups (direct reads, no atomics).
	old.bloom.Freeze()

	totalItems := int32(old.index.Len())
	rs := newReadSlab(old.SharedSlab, totalItems)

	// Publish to sealed list. Since we hold mu, we are the only writer —
	// Store is sufficient (no CAS needed).
	oldList := *rc.sealed.Load()
	newList := make([]*ReadSlab, 0, len(oldList)+1)
	newList = append(newList, rs) // Newest first
	newList = append(newList, oldList...)

	// Evict coldest slab if pool is exhausted.
	// Reserve 1 slot for the new active slab we're about to acquire.
	if len(newList) >= rc.maxSlabs {
		victimIdx := selectVictim(newList)
		if victimIdx >= 0 {
			victim := newList[victimIdx]
			newList = append(newList[:victimIdx], newList[victimIdx+1:]...)

			// Publish updated sealed list BEFORE acquiring new active.
			// Readers can no longer see the victim in the sealed list.
			rc.sealed.Store(&newList)

			// Acquire new active slab. The pool is over-provisioned by +1,
			// so this succeeds even while the victim is still pinned.
			rc.active = rc.newActiveSlab()

			// Compact: copy visited (proven-hot) items from victim into
			// the fresh active slab. Items start with visited=0 — must
			// prove themselves hot again (SIEVE invariant).
			preserved := rc.evictor.BeforeEvict(victim, rc.active)
			rc.compacted.Add(int64(preserved))

			// Release victim arena back to pool.
			victim.buf.Unpin()
			rc.evictions.Add(1)
			return
		}
	}

	rc.sealed.Store(&newList)

	// Acquire new active slab. Normally instant due to pool over-provision.
	// May block briefly if a reader still holds a TryInc reference on a
	// recently evicted slab — bounded by reader latency.
	rc.active = rc.newActiveSlab()
}

// Lookup checks the cache for the item. On hit, returns the cached data.
// On miss, populates the cache (for future reads) and returns found=false
// so the caller falls back to direct disk I/O.
func (rc *ReadCache) Lookup(item index.Item) ([]byte, []byte, Releaser, bool) {
	// Check cache.
	if data, storedKey, rel, found := rc.Acquire(item.Key); found {
		rc.hits.Add(1)
		return data, storedKey, rel, true
	}

	rc.misses.Add(1)
	blobLen := int64(item.PhysicalLen)

	// Admission check — skip items too large for the cache.
	if blobLen > rc.slabSize {
		return nil, nil, Releaser{}, false
	}
	if rc.maxItemSize > 0 && blobLen > rc.maxItemSize {
		rc.skipped.Add(1)
		return nil, nil, Releaser{}, false
	}

	// Populate cache for future reads (best-effort, current request
	// falls back to disk via Archivist.readBlobFromDisk).
	if blobLen > prefetchChunkSize {
		rc.populateLargeBlob(item)
	} else {
		rc.populateWithPrefetch(item)
	}

	return nil, nil, Releaser{}, false
}

// populateWithPrefetch reads a 64KB-aligned chunk from disk, parses all valid
// records in the chunk, and inserts them into the read cache. Populate-only:
// does not return the data — caller re-checks via Acquire.
func (rc *ReadCache) populateWithPrefetch(item index.Item) {
	blobOffset := int64(item.Offset)

	// Compute the 64KB-aligned region containing the blob.
	alignedOff := blobOffset &^ (prefetchChunkSize - 1)

	// Coalesce concurrent misses for the same chunk.
	key := flightKey(item.SegmentID, alignedOff)
	rc.flights.DoOnce(key, func() {
		// Page-align for Direct I/O compatibility.
		dioOff, dioLen := sys.AlignRange(alignedOff, prefetchChunkSize)
		rc.fetchChunk(item.SegmentID, dioOff, int(dioLen))
	})
}

// populateLargeBlob handles blobs that are larger than the prefetch chunk size
// but still fit in a slab. Reads the exact blob's on-disk bytes and inserts
// them into the read cache. Populate-only.
func (rc *ReadCache) populateLargeBlob(item index.Item) {
	// Coalesce on the blob's own (segID, offset) to prevent thundering herd.
	key := flightKey(item.SegmentID, int64(item.Offset))
	rc.flights.DoOnce(key, func() {
		rc.insertFromDisk(item)
	})
}

// insertFromDisk reads a single blob's raw on-disk bytes and inserts them
// into the read cache. Used for large blobs that don't benefit from chunk prefetch.
func (rc *ReadCache) insertFromDisk(item index.Item) {
	// Page-align the read for Direct I/O.
	dioOff, dioLen := sys.AlignRange(int64(item.Offset), int(item.PhysicalLen))

	handle := AcquireAlignedBuffer(int(dioLen), int(dioLen))
	defer handle.Release()
	buf := handle.Bytes()

	n, err := rc.readChunk(item.SegmentID, buf, dioOff)
	if err != nil || n < int(dioLen) {
		return
	}

	// Extract the exact record bytes from the aligned read.
	lead := int(int64(item.Offset) - dioOff)
	if lead+int(item.PhysicalLen) > len(buf) {
		return
	}
	rawRecord := buf[lead : lead+int(item.PhysicalLen)]

	// Verify header before inserting.
	if len(rawRecord) < record.HeaderSize {
		return
	}
	hdr, err := record.DecodeHeader(rawRecord[:record.HeaderSize])
	if err != nil || !hdr.IsValid() {
		return
	}

	// Compute hash from the key bytes in the record.
	keyEnd := record.HeaderSize + int(hdr.KeyLen)
	if keyEnd > len(rawRecord) {
		return
	}
	h := xxh3.Hash128(rawRecord[record.HeaderSize:keyEnd])

	rc.Insert(h, rawRecord)
}

// fetchChunk reads a page-aligned chunk from a segment and inserts all valid
// records into the read cache. Best-effort: parse errors are silently skipped.
//
// Records in segments are densely packed. The chunk may start mid-record, so
// we scan for RecordMagic (0xB10BCAFE) + HeaderCRC verification to find valid
// record boundaries. False positive rate is ~1/2^64 (Magic + CRC32).
func (rc *ReadCache) fetchChunk(segID uint32, alignedOff int64, alignedLen int) {
	handle := AcquireAlignedBuffer(alignedLen, alignedLen)
	defer handle.Release()
	buf := handle.Bytes()

	n, err := rc.readChunk(segID, buf, alignedOff)
	if err != nil || n < alignedLen {
		return
	}

	// Scan for valid records.
	offset := 0
	for offset+record.HeaderSize <= alignedLen {
		hdr, err := record.DecodeHeader(buf[offset : offset+record.HeaderSize])
		if err != nil || !hdr.IsValid() {
			// Not a valid record start. Advance past potential Magic alignment.
			offset += 4
			continue
		}

		totalSize := hdr.TotalSize()
		if totalSize <= 0 || offset+totalSize > alignedLen {
			break // Record extends beyond chunk or corrupt size
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
	list := *rc.sealed.Load()
	for _, slab := range list {
		slab.SharedSlab.Invalidate(key)
	}

	rc.mu.Lock()
	if rc.active != nil {
		rc.active.SharedSlab.Invalidate(key)
	}
	rc.mu.Unlock()
}

// DecayVisitedCounts halves the visitedCount on all sealed slabs.
// Called periodically from the maintenance worker to prevent ColdScore
// convergence — without decay, long-lived slabs accumulate visits and
// become unevictable even after their data goes cold.
func (rc *ReadCache) DecayVisitedCounts() {
	decayVisitedCounts(*rc.sealed.Load())
}

// Close releases all resources. All slabs are unpinned and arenas returned
// to the pool. The pool is then closed (munmaps all memory).
func (rc *ReadCache) Close() {
	if !rc.closed.CompareAndSwap(false, true) {
		return
	}

	// Release sealed slabs.
	list := *rc.sealed.Load()
	for _, slab := range list {
		slab.buf.Unpin()
	}
	empty := make([]*ReadSlab, 0)
	rc.sealed.Store(&empty)

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
		Evictions: rc.evictions.Load(),
		Compacted: rc.compacted.Load(),
		Skipped:   rc.skipped.Load(),
		Slabs:     len(*rc.sealed.Load()),
	}
}
