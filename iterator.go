package blobcache

import (
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/miretskiy/blobcache/internal/index"
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
	key   []byte // current user key (copy)
	hash  Key    // current 128-bit hash (decoded from value)
	valid bool
	err   error
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
func (it *Iterator) View(fn func(data []byte)) bool {
	if !it.valid {
		return false
	}
	return it.cache.View(it.key, fn)
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

// Close releases the Pebble iterator and snapshot.
func (it *Iterator) Close() error {
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
