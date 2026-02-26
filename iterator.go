package blobcache

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/sys"
)

// Iterator provides ordered iteration over cache entries using the global
// Pebble key index. Keys are yielded in user key order (lexicographic).
// Only live entries — present in the RAM index and not deleted — are visible.
//
// Concurrent writes, evictions, and compactions do not affect the iterator's
// view (Pebble snapshot isolation), though newly written keys may not be
// visible and evicted keys are filtered out via RAM index liveness checks.
//
// Must call Close() when done to release the Pebble snapshot.
type Iterator struct {
	cache *Cache
	snap  *pebble.Snapshot
	iter  *pebble.Iterator

	// Current position.
	key   []byte     // current user key (copy)
	hash  Key        // current 128-bit hash (decoded from value)
	item  index.Item // resolved by filterAndSet(), used by View()
	valid bool
	err   error

	// Read-ahead prefetch buffer — reused across View() calls.
	// When the iterator detects sequential on-disk layout (next item
	// contiguous in the same segment), it over-reads by readAheadFor()
	// bytes so subsequent View() calls serve from the buffer.
	prefetch    BufferHandle // aligned buffer (or nil)
	prefetchSeg uint32       // segment the buffer covers
	prefetchOff int64        // start offset in segment
	prefetchEnd int64        // end offset (prefetchOff + valid bytes)

	// Stats for debugging and testing read-ahead effectiveness.
	Stats IteratorStats
}

// IteratorStats tracks read-ahead effectiveness.
type IteratorStats struct {
	PrefetchHits   int64 // View() served from prefetch buffer
	PrefetchMisses int64 // View() required a disk read
	ReadAheadBytes int64 // total extra bytes read for prefetch
}

// NewIterator creates an iterator over all live keys in [lower, upper).
// Both bounds may be nil for unbounded iteration.
func (c *Cache) NewIterator(lower, upper []byte) (*Iterator, error) {
	if c.keyIndex == nil {
		return nil, errors.New("key index not available")
	}

	snap := c.keyIndex.NewSnapshot()

	// Build Pebble iteration bounds in the 0x01 (userKey→hash) namespace.
	var lowerBound, upperBound []byte
	if lower != nil {
		lowerBound = make([]byte, 1+len(lower))
		lowerBound[0] = nsKeyToHash
		copy(lowerBound[1:], lower)
	} else {
		lowerBound = []byte{nsKeyToHash}
	}
	if upper != nil {
		upperBound = make([]byte, 1+len(upper))
		upperBound[0] = nsKeyToHash
		copy(upperBound[1:], upper)
	} else {
		// Stop before the next namespace (0x02).
		upperBound = []byte{nsKeyToHash + 1}
	}

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		_ = snap.Close()
		return nil, err
	}

	return &Iterator{
		cache: c,
		snap:  snap,
		iter:  iter,
	}, nil
}

// First positions the iterator at the first live key.
func (it *Iterator) First() bool {
	if it.err != nil {
		return false
	}
	if !it.iter.First() {
		it.valid = false
		it.err = it.iter.Error()
		return false
	}
	return it.filterAndSet()
}

// Next advances the iterator to the next live key.
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	if !it.iter.Next() {
		it.valid = false
		it.err = it.iter.Error()
		return false
	}
	return it.filterAndSet()
}

// SeekGE positions the iterator at the first live key >= target.
func (it *Iterator) SeekGE(target []byte) bool {
	if it.err != nil {
		return false
	}
	seekKey := make([]byte, 1+len(target))
	seekKey[0] = nsKeyToHash
	copy(seekKey[1:], target)

	if !it.iter.SeekGE(seekKey) {
		it.valid = false
		it.err = it.iter.Error()
		return false
	}
	return it.filterAndSet()
}

// filterAndSet checks the current Pebble position against the RAM index
// for liveness. Skips dead/evicted keys by advancing the iterator.
func (it *Iterator) filterAndSet() bool {
	for {
		// Decode hash from the value (16 bytes: Lo(8) + Hi(8)).
		val := it.iter.Value()
		if len(val) < hashSize {
			// Corrupt entry — skip.
			if !it.iter.Next() {
				it.valid = false
				it.err = it.iter.Error()
				return false
			}
			continue
		}
		h := index.Key{
			Lo: binary.LittleEndian.Uint64(val[0:8]),
			Hi: binary.LittleEndian.Uint64(val[8:16]),
		}

		// Check RAM index for liveness.
		item, found := it.cache.index.Get(h)
		if found && !item.IsDeleted() {
			// Live entry — extract user key (strip 0x01 prefix).
			pebbleKey := it.iter.Key()
			it.key = make([]byte, len(pebbleKey)-1)
			copy(it.key, pebbleKey[1:])
			it.hash = h
			it.item = item
			it.valid = true
			return true
		}

		// Dead or evicted — advance.
		if !it.iter.Next() {
			it.valid = false
			it.err = it.iter.Error()
			return false
		}
	}
}

// View provides scoped access to the current entry's blob data.
// The data slice is valid only for the duration of fn.
// Returns false if the blob is no longer accessible (e.g., evicted between
// positioning and read).
//
// Unlike Cache.View(), this uses the index.Item already resolved by
// filterAndSet() — bypassing the bloom filter and index lookup (~200ns
// savings per key). When blobs are contiguous on disk, it reads ahead
// to serve subsequent calls from the prefetch buffer.
func (it *Iterator) View(fn func(data []byte)) bool {
	if !it.valid {
		return false
	}

	// 1. Try prefetch buffer hit.
	if it.tryPrefetchHit(fn) {
		it.Stats.PrefetchHits++
		return true
	}
	it.Stats.PrefetchMisses++

	// 2. Determine read-ahead from next entry.
	// Always read enough to fully cover the next item so that the next
	// View() call can be served from the prefetch buffer.
	readExtra := 0
	if nextItem, ok := it.peekNextItem(); ok {
		if nextItem.SegmentID == it.item.SegmentID &&
			nextItem.Offset == it.item.Offset+it.item.PhysicalLen {
			// Cover the next item completely, plus adaptive read-ahead
			// past it for the item after that.
			nextLen := int(nextItem.PhysicalLen)
			readExtra = nextLen + readAheadFor(nextLen)
		}
	}

	// 3. Read from disk with optional read-ahead.
	it.Stats.ReadAheadBytes += int64(readExtra)
	return it.readAndServe(fn, readExtra)
}

// tryPrefetchHit checks if the current item's bytes are in the prefetch
// buffer. On hit, parses the record and calls fn. Returns false on miss.
func (it *Iterator) tryPrefetchHit(fn func(data []byte)) bool {
	if it.prefetch.Bytes() == nil {
		return false
	}
	if it.item.SegmentID != it.prefetchSeg {
		return false
	}
	itemOff := int64(it.item.Offset)
	itemEnd := itemOff + int64(it.item.PhysicalLen)
	if itemOff < it.prefetchOff || itemEnd > it.prefetchEnd {
		return false
	}

	// Hit — parse record from buffer.
	buf := it.prefetch.Bytes()
	lead := int(itemOff - it.prefetchOff)
	rec := buf[lead : lead+int(it.item.PhysicalLen)]

	verifyKey := it.key
	if it.cache.TrustHash {
		verifyKey = nil
	}

	// Pass no-op releaser: the prefetch buffer is owned by the iterator,
	// not the caller. For compressed data, parseRecord allocates a
	// separate decompression buffer with its own releaser.
	data, rel, err := it.cache.archivist.parseRecord(
		rec, it.item, verifyKey, Releaser{}, func() {})
	if err != nil {
		return false
	}
	defer rel.Release()
	fn(data)
	return true
}

// peekNextItem looks at the next Pebble entry and resolves its index.Item
// without advancing the iterator position. Returns false if there is no
// next entry or the next entry is dead/evicted.
func (it *Iterator) peekNextItem() (index.Item, bool) {
	if !it.iter.Next() {
		return index.Item{}, false
	}
	defer func() { it.iter.Prev() }()

	val := it.iter.Value()
	if len(val) < hashSize {
		return index.Item{}, false
	}
	h := index.Key{
		Lo: binary.LittleEndian.Uint64(val[0:8]),
		Hi: binary.LittleEndian.Uint64(val[8:16]),
	}

	next, found := it.cache.index.Get(h)
	if !found || next.IsDeleted() {
		return index.Item{}, false
	}
	return next, true
}

// readAndServe reads the current item from disk with optional read-ahead,
// parses the record, calls fn, and stashes excess bytes in the prefetch
// buffer for subsequent View() calls.
func (it *Iterator) readAndServe(fn func(data []byte), readExtra int) bool {
	a := it.cache.archivist
	segID := it.item.SegmentID

	// Hold segment lock during the I/O to prevent deletion mid-read.
	shard := it.cache.index.SegmentLockShard(segID)
	shard.RLock()

	sf, err := a.getSegmentFile(segID)
	if err != nil {
		shard.RUnlock()
		return false // TOCTOU: segment drained/compacted.
	}

	blobOff := int64(it.item.Offset)
	blobLen := int(it.item.PhysicalLen)
	readLen := blobLen + readExtra

	var readOff int64
	if a.IO.DirectIORead {
		alignedOff, alignedLen := sys.AlignRange(blobOff, readLen)
		it.releasePrefetch()
		it.prefetch = AcquireAlignedBuffer(int(alignedLen), int(alignedLen))
		readOff = alignedOff
	} else {
		it.releasePrefetch()
		it.prefetch = AcquireBuffer(readLen, readLen)
		readOff = blobOff
	}
	buf := it.prefetch.Bytes()

	n, err := a.sched.ReadAt(int(sf.Fd()), buf, readOff)
	shard.RUnlock()

	// Verify we read enough for the current record.
	lead := int(blobOff - readOff)
	if err != nil || n < lead+blobLen {
		it.releasePrefetch()
		return false
	}

	// Update prefetch tracking (read-ahead bytes may be partial at EOF).
	it.prefetchSeg = segID
	it.prefetchOff = readOff
	it.prefetchEnd = readOff + int64(n)

	// Parse the record.
	rec := buf[lead : lead+blobLen]
	verifyKey := it.key
	if it.cache.TrustHash {
		verifyKey = nil
	}

	data, rel, err := a.parseRecord(rec, it.item, verifyKey, Releaser{}, func() {})
	if err != nil {
		it.err = fmt.Errorf("iterator read: %w", err)
		return false
	}
	defer rel.Release()
	fn(data)
	return true
}

// releasePrefetch releases the prefetch buffer and resets tracking state.
func (it *Iterator) releasePrefetch() {
	it.prefetch.Release()
	it.prefetch = BufferHandle{}
	it.prefetchSeg = 0
	it.prefetchOff = 0
	it.prefetchEnd = 0
}

// readAheadFor returns adaptive read-ahead bytes based on current blob size.
// Scales linearly with blob size (next blob is likely similar size), with a
// 64KB floor and 1MB cap.
func readAheadFor(blobSize int) int {
	const (
		minReadAhead = 64 << 10 // 64KB floor
		maxReadAhead = 1 << 20  // 1MB cap
	)
	return max(minReadAhead, min(blobSize, maxReadAhead))
}

// Key returns the current user key. Only valid when Valid() returns true.
func (it *Iterator) Key() []byte {
	return it.key
}

// HashKey returns the 128-bit hash of the current key.
func (it *Iterator) HashKey() Key {
	return it.hash
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *Iterator) Valid() bool {
	return it.valid
}

// Error returns any error encountered during iteration.
func (it *Iterator) Error() error {
	return it.err
}

// Close releases the prefetch buffer, Pebble iterator, and snapshot.
func (it *Iterator) Close() error {
	it.releasePrefetch()
	var errs []error
	if it.iter != nil {
		if err := it.iter.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if it.snap != nil {
		if err := it.snap.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	it.valid = false
	return errors.Join(errs...)
}
