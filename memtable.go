package blobcache

import (
	"context"
	"errors"
	"sync"
)

// MemTable provides async write buffering via Go channels
// Emulates RocksDB memtable concept but uses simpler channel-based design
type MemTable struct {
	entries chan memTableEntry
	stopCh  chan struct{}
	wg      sync.WaitGroup
	cache   *Cache
}

type memTableEntry struct {
	key   []byte
	value []byte
}

// newMemTable creates a memtable with specified channel capacity
// capacity < 0: memtable disabled (synchronous writes)
// capacity = 0: unbuffered channel (synchronous handoff)
// capacity > 0: buffered channel (async up to capacity entries)
func (c *Cache) newMemTable(capacity int) *MemTable {
	if capacity < 0 {
		return nil // Memtable disabled
	}

	mt := &MemTable{
		entries: make(chan memTableEntry, capacity),
		stopCh:  make(chan struct{}),
		cache:   c,
	}

	mt.wg.Add(1)
	go mt.worker()

	return mt
}

// worker drains entries from channel and writes to disk
func (mt *MemTable) worker() {
	defer mt.wg.Done()
	ctx := context.Background()

	for {
		select {
		case entry := <-mt.entries:
			// Write to disk synchronously in background goroutine
			mt.cache.writeToDisk(ctx, entry.key, entry.value)

		case <-mt.stopCh:
			return // Exit immediately, don't drain
		}
	}
}

// Add copies key and value, then enqueues for background write
// Safe to mutate key/value after return
func (mt *MemTable) Add(key, value []byte) error {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return mt.UnsafeAdd(keyCopy, valueCopy)
}

// UnsafeAdd enqueues without copying
// Caller MUST NOT mutate key or value after calling this
func (mt *MemTable) UnsafeAdd(key, value []byte) error {
	select {
	case mt.entries <- memTableEntry{key, value}:
		return nil
	case <-mt.stopCh:
		return ErrClosed
	}
}

// Close shuts down memtable worker
// Does NOT drain pending writes - call Drain() first if needed
func (mt *MemTable) Close() error {
	select {
	case <-mt.stopCh:
		return nil // Already closed
	default:
		close(mt.stopCh)
	}
	mt.wg.Wait()
	return nil
}

// Drain processes all pending writes immediately
func (mt *MemTable) Drain() {
	ctx := context.Background()
	for {
		select {
		case entry := <-mt.entries:
			mt.cache.writeToDisk(ctx, entry.key, entry.value)
		default:
			return // Channel empty
		}
	}
}

// Common errors
var (
	ErrClosed = errors.New("memtable closed")
)
