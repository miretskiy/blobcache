package blobcache

import (
	"bytes"
	"fmt"
	"os"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/miretskiy/blobcache/internal/index"
	"github.com/zeebo/xxh3"
)

// Iterator provides ordered iteration over cache entries using per-segment SSTables.
// Keys are yielded in user key order (bytes.Compare). Only live entries — present
// in the RAM index and not deleted — are visible.
//
// The iterator captures a snapshot of segment IDs at creation time. Concurrent
// writes, evictions, and compactions do not affect the iterator's view, though
// newly written keys may not be visible and evicted keys are filtered out.
//
// Must call Close() when done to release SSTable readers.
type Iterator struct {
	cache *Cache
	merge *mergingIter

	// Readers held open for the lifetime of the iterator.
	readers []*sstable.Reader

	// Current position.
	key   []byte
	hash  Key
	valid bool
	err   error
}

// NewIterator creates an iterator over all live keys in [lower, upper).
// Both bounds may be nil for unbounded iteration.
//
// The iterator opens SSTable files for each registered segment. Segments without
// .sst files (pre-migration) are silently skipped.
func (c *Cache) NewIterator(lower, upper []byte) (*Iterator, error) {
	segIDs := c.index.SnapshotSegmentIDs()

	var sources []mergeSource
	var readers []*sstable.Reader

	cleanup := func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}

	for _, segID := range segIDs {
		sstPath := SegmentSSTPath(getSegmentPath(c.Path, c.Shards, segID))
		if _, err := os.Stat(sstPath); os.IsNotExist(err) {
			continue
		}

		f, err := os.Open(sstPath)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("open sst %d: %w", segID, err)
		}

		readable, err := sstable.NewSimpleReadable(f)
		if err != nil {
			_ = f.Close()
			cleanup()
			return nil, fmt.Errorf("sst readable %d: %w", segID, err)
		}

		reader, err := sstable.NewReader(readable, sstable.ReaderOptions{
			Comparer: pebble.DefaultComparer,
		})
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("sst reader %d: %w", segID, err)
		}
		readers = append(readers, reader)

		// Create Pebble iterator WITHOUT bounds. Pebble's sstable.Iterator panics
		// on First() when a lower bound is set (requires SeekGE which needs
		// unexportable base.SeekGEFlags). We handle bounds in the adapter instead.
		pebbleIter, err := reader.NewIter(nil, nil)
		if err != nil {
			_ = reader.Close()
			cleanup()
			return nil, fmt.Errorf("sst iter %d: %w", segID, err)
		}

		sources = append(sources, mergeSource{
			segID: segID,
			iter:  &pebbleIterAdapter{iter: pebbleIter, lower: lower, upper: upper},
		})
	}

	return &Iterator{
		cache:   c,
		merge:   newMergingIter(sources, bytes.Compare),
		readers: readers,
	}, nil
}

// First positions the iterator at the first live key.
func (it *Iterator) First() bool {
	if it.err != nil {
		return false
	}
	key, val, ok := it.merge.First()
	if !ok {
		it.valid = false
		it.err = it.merge.Error()
		return false
	}
	return it.filterAndSet(key, val)
}

// Next advances the iterator to the next live key.
func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	key, val, ok := it.merge.Next()
	if !ok {
		it.valid = false
		it.err = it.merge.Error()
		return false
	}
	return it.filterAndSet(key, val)
}

// SeekGE positions the iterator at the first live key >= target.
func (it *Iterator) SeekGE(target []byte) bool {
	if it.err != nil {
		return false
	}
	key, val, ok := it.merge.SeekGE(target)
	if !ok {
		it.valid = false
		it.err = it.merge.Error()
		return false
	}
	return it.filterAndSet(key, val)
}

// filterAndSet checks the key against the RAM index for liveness.
// Skips dead/evicted keys by advancing the merging iterator.
func (it *Iterator) filterAndSet(key []byte, _ sstValue) bool {
	for {
		h := xxh3.Hash128(key)
		hashKey := index.Key(h)

		item, found := it.cache.index.Get(hashKey)
		if found && !item.IsDeleted() {
			it.key = key
			it.hash = hashKey
			it.valid = true
			return true
		}

		// Key not in RAM or deleted — skip.
		var ok bool
		key, _, ok = it.merge.Next()
		if !ok {
			it.valid = false
			it.err = it.merge.Error()
			return false
		}
	}
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

// Close releases all SSTable readers and iterators.
func (it *Iterator) Close() error {
	mergeErr := it.merge.Close()
	for _, r := range it.readers {
		if err := r.Close(); err != nil && mergeErr == nil {
			mergeErr = err
		}
	}
	it.valid = false
	return mergeErr
}

// --- Pebble SSTable Iterator Adapter ---

// pebbleIterAdapter wraps a Pebble sstable.Iterator to implement sstableIter.
// Bounds are handled in the adapter because Pebble's sstable.Iterator panics on
// First() when created with a lower bound (it requires SeekGE with unexportable
// base.SeekGEFlags). The iterator is created without bounds; this adapter enforces
// [lower, upper) filtering.
type pebbleIterAdapter struct {
	iter  sstable.Iterator
	lower []byte // inclusive lower bound (nil = unbounded)
	upper []byte // exclusive upper bound (nil = unbounded)
	err   error
}

func (a *pebbleIterAdapter) First() ([]byte, []byte, bool) {
	if a.lower != nil {
		return a.SeekGE(a.lower)
	}
	key, val := a.iter.First()
	if key == nil {
		return nil, nil, false
	}
	if a.upper != nil && bytes.Compare(key.UserKey, a.upper) >= 0 {
		return nil, nil, false
	}
	valBytes, _, err := val.Value(nil)
	if err != nil {
		a.err = err
		return nil, nil, false
	}
	return key.UserKey, valBytes, true
}

func (a *pebbleIterAdapter) Next() ([]byte, []byte, bool) {
	key, val := a.iter.Next()
	if key == nil {
		return nil, nil, false
	}
	if a.upper != nil && bytes.Compare(key.UserKey, a.upper) >= 0 {
		return nil, nil, false
	}
	valBytes, _, err := val.Value(nil)
	if err != nil {
		a.err = err
		return nil, nil, false
	}
	return key.UserKey, valBytes, true
}

func (a *pebbleIterAdapter) SeekGE(target []byte) ([]byte, []byte, bool) {
	// Scan from the beginning to find first key >= target.
	// This is O(n) but SSTables are small (~500 entries).
	key, val := a.iter.First()
	for key != nil {
		if bytes.Compare(key.UserKey, target) >= 0 {
			if a.upper != nil && bytes.Compare(key.UserKey, a.upper) >= 0 {
				return nil, nil, false
			}
			valBytes, _, err := val.Value(nil)
			if err != nil {
				a.err = err
				return nil, nil, false
			}
			return key.UserKey, valBytes, true
		}
		key, val = a.iter.Next()
	}
	return nil, nil, false
}

func (a *pebbleIterAdapter) Close() error {
	return a.iter.Close()
}

func (a *pebbleIterAdapter) Error() error {
	return a.err
}
