package blobcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/index"
	"github.com/miretskiy/blobcache/metadata"
)

// Key is a cache key providing type safety (strong type, not alias)
type Key = index.Key

const (
	// evictionHysteresis is the target fraction of MaxSize to evict to
	// Evicting to 98% of limit (not 100%) avoids re-triggering on small additions
	// Example: 1TB cache evicts to 980GB (20GB buffer)
	evictionHysteresis = 0.98
)

// Cache is a high-performance blob storage with bloom filter optimization
type Cache struct {
	config
	index    *index.Index
	storage  *Storage
	bloom    atomic.Pointer[bloom.Filter]
	memTable *MemTable

	// Size tracking for reactive eviction
	approxSize      atomic.Int64 // Approximate total size (updated during flush/eviction)
	evictionRunning atomic.Bool  // Prevents concurrent evictions

	// Background error tracking
	bgError atomic.Pointer[error] // First background error (nil = healthy)

	// Background workers
	evictionTrigger chan struct{} // Capacity 1: trigger eviction, blocks when eviction running
	stopCh          chan struct{}
	wg              sync.WaitGroup
}

// degradedMode interface allows memtable to check/set degraded state
// without direct dependency on Cache
type degradedMode interface {
	isDegraded() bool
	setDegraded(error)
}

// Cache implements degradedMode interface
func (c *Cache) isDegraded() bool {
	return c.bgError.Load() != nil
}

func (c *Cache) setDegraded(err error) {
	if c.bgError.CompareAndSwap(nil, &err) {
		log.Error("entering degraded mode (memory-only)", "error", err)
	}
}

// BGError returns any background error (nil if healthy)
// Once set, this is permanent until cache restart
func (c *Cache) BGError() error {
	if ptr := c.bgError.Load(); ptr != nil {
		return *ptr
	}
	return nil
}

// New creates a Cache at the specified path with optional configuration
// Does NOT start background workers - call Start() to begin operations
func New(path string, opts ...Option) (*Cache, error) {
	cfg := defaultConfig(path)
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	// Ensure directory structure exists and validate configuration
	idx, err := checkOrInitialize(cfg)
	if err != nil {
		return nil, fmt.Errorf("initialization failed: %w", err)
	}

	// Create new bloom filter and figure out how much data on disk from segment meta.
	// Skip deleted blobs when building bloom filter
	var totalSize int64
	filter := bloom.New(uint(cfg.BloomEstimatedKeys), cfg.BloomFPRate)
	if err := idx.ForEachSegment(func(segment metadata.SegmentRecord) bool {
		for _, rec := range segment.Records {
			if !rec.IsDeleted() {
				filter.Add(rec.Hash)
				totalSize += rec.Size
			}
		}
		return true
	}); err != nil {
		return nil, err
	}

	c := &Cache{
		config:          cfg,
		index:           idx,
		storage:         NewStorage(cfg, idx),
		evictionTrigger: make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}
	c.bloom.Store(filter)
	c.approxSize.Store(totalSize)

	c.memTable = c.newMemTable(c.config, c.storage)

	return c, nil
}

// Start begins background operations (eviction worker)
// Returns a closer function for graceful shutdown
func (c *Cache) Start() (func() error, error) {
	// Start eviction worker (checks every 60 seconds)
	c.wg.Add(1)
	go c.evictionWorker()

	// Return closer function
	return c.Close, nil
}

// Close gracefully shuts down all background workers and saves state
// Safe to call multiple times and safe to call even if Start() was never called
// Does NOT drain memtable - caller should call Drain() first if needed
func (c *Cache) Close() error {
	// Signal workers to stop (idempotent)
	select {
	case <-c.stopCh:
		return nil
	default:
		close(c.stopCh)
	}

	// Close memtable (does NOT drain)
	c.memTable.Close()
	c.wg.Wait()

	// Collect all close errors
	return errors.Join(
		c.storage.Close(),
		c.index.Close(),
	)
}

// Drain waits for all pending memtable writes to complete
// No-op if memtable is disabled
func (c *Cache) Drain() {
	c.memTable.Drain()
}

// checkOrInitialize ensures directory structure exists and validates configuration
// Uses empty .initialized marker file for fast idempotency check
func checkOrInitialize(cfg config) (*index.Index, error) {
	markerPath := filepath.Join(cfg.Path, ".initialized")

	// Check if already initialized
	if _, err := os.Stat(markerPath); err == nil {
		// Already initialized
		return index.NewIndex(cfg.Path)
	}

	// Not initialized - create directory structure
	// Create shard directories for blobs
	for i := 0; i < max(1, cfg.Shards); i++ {
		shardDir := filepath.Join(cfg.Path, "segments", fmt.Sprintf("%04d", i))
		if err := os.MkdirAll(shardDir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create %04d: %w", i, err)
		}
	}

	idx, err := index.NewIndex(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open index: %w", err)
	}
	// Touch empty marker file
	if err := os.WriteFile(markerPath, []byte{}, 0o644); err != nil {
		return nil, fmt.Errorf("failed to write marker: %w", err)
	}

	return idx, nil
}

// rebuildBloom scans all keys from index and builds new bloom filter
func (c *Cache) rebuildBloom() error {
	// Create new bloom filter
	filter := bloom.New(uint(c.BloomEstimatedKeys), c.BloomFPRate)

	// Arrange for the current bloom filter to capture any additions
	// while we work on rebuilding (only if bloom already exists)
	var stopRecording func()
	var consumeRecording func(bloom.HashConsumer)
	if oldFilter := c.bloom.Load(); oldFilter != nil {
		stopRecording, consumeRecording = oldFilter.RecordAdditions()
		defer stopRecording()
	}

	if err := c.index.ForEachSegment(func(segment metadata.SegmentRecord) bool {
		for _, rec := range segment.Records {
			if !rec.IsDeleted() {
				filter.Add(rec.Hash)
			}
		}
		return true
	}); err != nil {
		return err
	}

	// Replay recorded additions (if any)
	if consumeRecording != nil {
		consumeRecording(filter.AddHash)
	}

	c.bloom.Store(filter)

	// Defensive: catch any additions between consume and swap
	if consumeRecording != nil {
		consumeRecording(filter.AddHash)
	}

	return nil
}

// evictionWorker handles eviction requests and periodic compaction
func (c *Cache) evictionWorker() {
	defer c.wg.Done()

	// Compaction runs less frequently (every hour)
	compactionTicker := time.NewTicker(60 * time.Minute)
	defer compactionTicker.Stop()

	for {
		select {
		case <-c.evictionTrigger:
			// Eviction requested (triggered by PutBatch)
			if c.MaxSize > 0 && !c.isDegraded() {
				if err := c.runEvictionSieve(c.MaxSize); err != nil {
					c.setDegraded(err)
					return // Stop worker permanently
				}
			}

		case <-compactionTicker.C:
			// Periodic compaction of sparse segments
			if !c.isDegraded() {
				if err := c.maybeCompactSegments(); err != nil {
					log.Warn("compaction failed", "error", err)
					// Non-fatal: continue running
				}
			}

		case <-c.stopCh:
			return
		}
	}
}

// runEvictionSieve evicts blobs using Sieve algorithm until under size limit
func (c *Cache) runEvictionSieve(maxCacheSize int64) error {
	// Testing: inject eviction error
	if c.testingInjectEvictErr != nil {
		if err := c.testingInjectEvictErr(); err != nil {
			return err
		}
	}

	// Prevent concurrent evictions
	if !c.evictionRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer c.evictionRunning.Store(false)

	// Compute total size
	var totalSize int64
	c.index.ForEachBlob(func(kv index.KeyValue) bool {
		totalSize += kv.Val.Size
		return true
	})

	if totalSize <= maxCacheSize {
		return nil // Under limit
	}

	// Evict to evictionHysteresis * maxCacheSize to avoid re-triggering
	target := int64(float64(maxCacheSize) * evictionHysteresis)
	toEvictBytes := totalSize - target
	evictedBytes := int64(0)
	evictedCount := 0

	// Evict blobs one by one using Sieve
	for evictedBytes < toEvictBytes {
		key, victim, err := c.index.SieveScan()
		if err != nil {
			return err
		}

		// Punch hole in segment file (reclaim space immediately)
		// This also updates segment stats (totalBytes/deletedBytes in SegmentFile)
		if err := c.storage.HolePunchBlob(victim.SegmentID, victim.Pos, victim.Size); err != nil {
			log.Warn("hole punch failed",
				"segment", victim.SegmentID,
				"offset", victim.Pos,
				"size", victim.Size,
				"error", err)
			// Non-fatal: continue evicting, compaction will reclaim space eventually
		}

		// Remove from index (also removes from FIFO queue and recycles Value)
		c.index.DeleteBlob(key)

		evictedBytes += victim.Size
		evictedCount++
	}

	log.Info("eviction completed",
		"evicted_blobs", evictedCount,
		"evicted_mb", evictedBytes/(1024*1024))

	// Update approxSize to reflect evicted bytes
	c.approxSize.Add(-evictedBytes)

	// Rebuild bloom filter after eviction
	if evictedCount > 0 {
		if err := c.rebuildBloom(); err != nil {
			return fmt.Errorf("failed to rebuild bloom after eviction: %w", err)
		}
	}

	return nil
}

// maybeCompactSegments checks for sparse segments and compacts them
func (c *Cache) maybeCompactSegments() error {
	// TODO: Implement segment compaction
	// 1. Scan segmentStats for segments with FullnessPct < 0.20
	// 2. Compact 1-2 segments per cycle (rate limited)
	// 3. Read live blobs, re-insert to memtable, delete old segment
	return nil
}

// Put stores a key-value pair (makes copies for safety)
func (c *Cache) Put(key []byte, value []byte) {
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	c.UnsafePut(key, valueCopy)
}

// UnsafePut stores key-value without copying
// Caller must ensure key and value are not modified after this call
func (c *Cache) UnsafePut(key []byte, value []byte) {
	h := c.KeyHasher(key)
	c.memTable.Put(Key(h), value)
	c.bloom.Load().Add(h)
}

// PutChecksummed stores key-value with an explicit checksum (makes copies for safety)
// The checksum will be validated if WithVerifyOnRead is enabled
func (c *Cache) PutChecksummed(key []byte, value []byte, checksum uint32) {
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	c.UnsafePutChecksummed(key, valueCopy, checksum)
}

// UnsafePutChecksummed stores key-value with checksum without copying
// Caller must ensure key and value are not modified after this call
func (c *Cache) UnsafePutChecksummed(key []byte, value []byte, checksum uint32) {
	h := c.KeyHasher(key)
	c.memTable.PutChecksummed(Key(h), value, checksum)
	c.bloom.Load().Add(h)
}

type batcher interface {
	PutBatch([]index.KeyValue) error
}

func (c *Cache) PutBatch(kvs []index.KeyValue) error {
	if err := c.index.PutBatch(kvs); err != nil {
		return err
	}

	// Phase 3: Update size tracking and trigger reactive eviction if needed
	var addedBytes int64
	for _, rec := range kvs {
		addedBytes += rec.Val.Size
	}
	newSize := c.approxSize.Add(addedBytes)

	// Trigger eviction if over limit
	// First write sends to channel (non-blocking), subsequent writes block until eviction completes
	if c.MaxSize > 0 && newSize > c.MaxSize && !c.isDegraded() {
		c.evictionTrigger <- struct{}{}
	}
	return nil
}

// Get retrieves a value by key as a Reader
// Returns (reader, true) if found, (nil, false) if not found
// IncludeChecksums are verified internally if WithVerifyOnRead is enabled
func (c *Cache) Get(key []byte) (io.Reader, bool) {
	// 1. Check bloom filter (lock-free)
	h := c.KeyHasher(key)
	bloom := c.bloom.Load()
	if !bloom.Test(h) {
		return nil, false
	}

	// 2. Check memtable (recent writes)
	if value, found := c.memTable.Get(Key(h)); found {
		return bytes.NewReader(value), true
	}

	// 3. Read from disk using BlobReader
	// Reader handles index lookup and checksum verification internally
	return c.storage.Get(Key(h))
}
