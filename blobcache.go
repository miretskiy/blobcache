package blobcache

import (
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

type Key = uint64

const (
	// evictionHysteresis is the target fraction of MaxSize to evict to.
	evictionHysteresis = 0.95
)

// Cache is a high-performance blob storage with bloom filter optimization
type Cache struct {
	config
	index   *index.Index
	storage *Storage
	bloom   struct {
		atomic.Pointer[bloom.Filter]
		hits        atomic.Uint64             // Bloom filter said "yes"
		ghosts      atomic.Uint64             // Bloom said yes, but index said no.
		deletions   atomic.Int64              // Track cumulative deletions since last rebuild
		lastRebuild atomic.Pointer[time.Time] // When the last rebuild happened.
		running     atomic.Bool
	}
	memTable *MemTable
	
	// LogicalSize tracking for reactive eviction
	approxSize      atomic.Int64 // Approximate total size (updated during flush/eviction)
	evictionRunning atomic.Bool  // Prevents concurrent evictions
	
	// Background error tracking
	bgError atomic.Pointer[error] // First background error (nil = healthy)
	
	// Background workers
	evictionTrigger chan struct{} // Capacity 1: trigger eviction, blocks when eviction running
	stopCh          chan struct{}
	wg              sync.WaitGroup
}

// ErrorRporter interface allows memtable to check/set degraded state
// without direct dependency on Cache
type ErrorRporter interface {
	IsDegraded() bool
	ReportError(error)
}

// Cache implements ErrorRporter interface
func (c *Cache) IsDegraded() bool {
	return c.bgError.Load() != nil
}

func (c *Cache) ReportError(err error) {
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
				totalSize += rec.LogicalSize
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
	
	c.memTable = NewMemTable(c.config, c, c)
	
	return c, nil
}

// Start begins background operations (eviction worker)
// Returns a closer function for graceful shutdown
func (c *Cache) Start() {
	c.wg.Add(1)
	go c.evictionWorker()
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

func (c *Cache) rebuildBloom() error {
	// 1. Create the new filter skeleton
	newFilter := bloom.New(uint(c.BloomEstimatedKeys), c.BloomFPRate)
	
	// 2. Start recording on the OLD filter if it exists
	var stopRecording func()
	var consumeRecording func(bloom.HashConsumer)
	
	if oldFilter := c.bloom.Load(); oldFilter != nil {
		// We use a large buffer to ensure we don't block the hot path
		// during the index scan.
		stopRecording, consumeRecording = oldFilter.RecordAdditions()
	}
	
	// 3. Scan the index and populate the NEW filter
	// This is the long-running part.
	err := c.index.ForEachSegment(func(segment metadata.SegmentRecord) bool {
		for _, rec := range segment.Records {
			if !rec.IsDeleted() {
				newFilter.AddHash(rec.Hash) // Direct bit-set, no recording needed here
			}
		}
		return true
	})
	if err != nil {
		if stopRecording != nil {
			stopRecording()
		}
		return err
	}
	
	// 4. ATOMIC SWAP: The handover moment.
	// From this line forward, c.Put() calls hit the newFilter.
	oldFilter := c.bloom.Swap(newFilter)
	
	// 4. THE DRAIN: Catch the "In-Flight" additions
	if oldFilter != nil && stopRecording != nil {
		// Stop recording first: This closes the channel.
		stopRecording()
		
		// Replay the final batch captured during the scan into the NEW filter.
		consumeRecording(newFilter.AddHash)
	}
	
	return nil
}

// evictionWorker handles eviction requests and periodic compaction
func (c *Cache) evictionWorker() {
	defer c.wg.Done()
	
	compactionTicker := time.NewTicker(10 * time.Minute)
	defer compactionTicker.Stop()
	
	for {
		select {
		case <-c.evictionTrigger:
			// Eviction requested (triggered by PutBatch)
			if c.MaxSize > 0 && !c.IsDegraded() {
				if err := c.runEvictionSieve(c.MaxSize); err != nil {
					c.ReportError(err)
					return // Stop worker permanently
				}
			}
		
		case <-compactionTicker.C:
			// Periodic compaction of sparse segments
			if !c.IsDegraded() {
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
	if c.testingInjectEvictErr != nil {
		if err := c.testingInjectEvictErr(); err != nil {
			return err
		}
	}
	if !c.evictionRunning.CompareAndSwap(false, true) {
		return nil
	}
	defer c.evictionRunning.Store(false)
	
	currentSize := c.approxSize.Load()
	if currentSize <= maxCacheSize {
		return nil
	}
	
	target := int64(float64(maxCacheSize) * evictionHysteresis)
	toEvictBytes := currentSize - target
	
	var (
		victims      []index.Entry
		evictedBytes int64
	)
	
	// 1. SELECTION PHASE: High-speed RAM eviction.
	// Index.Evict() now unlinks from FIFO, removes from skipmap,
	// and recycles the node, returning only the Entry copy.
	for evictedBytes < toEvictBytes {
		victim, err := c.index.Evict()
		if err != nil {
			break // No more victims available
		}
		victims = append(victims, victim)
		evictedBytes += victim.LogicalSize
	}
	
	if len(victims) == 0 {
		return nil
	}
	
	// 2. COMMIT PHASE: Sync metadata to Disk (Bitcask).
	// One transaction per segment for the entire batch.
	if err := c.index.DeleteBlobs(victims...); err != nil {
		// If metadata sync fails, we have a problem.
		// RAM is already updated, but durable index isn't.
		return fmt.Errorf("eviction durability sync failed: %w", err)
	}
	
	// 3. RECLAMATION PHASE: Physical Disk Space.
	// Performed AFTER metadata is durable.
	for _, v := range victims {
		_ = c.storage.HolePunchBlob(v.SegmentID, v.Pos, v.LogicalSize)
	}
	
	// 4. METRICS & MAINTENANCE
	c.approxSize.Add(-evictedBytes)
	evictedCount := len(victims)
	
	log.Info("eviction completed",
		"evicted_count", evictedCount,
		"evicted_mb", evictedBytes/(1024*1024),
		"remaining_mb", c.approxSize.Load()/(1024*1024))
	
	c.bloom.deletions.Add(int64(evictedCount))
	
	if err := c.maybeTriggerBloomRebuild(); err != nil {
		log.Error("bloom rebuild failed", "error", err)
	}
	
	return nil
}

func (c *Cache) maybeTriggerBloomRebuild() error {
	// 1. Cooldown Guard (e.g., 5 minutes)
	last := c.bloom.lastRebuild.Load()
	if last != nil && time.Since(*last) < 5*time.Minute {
		return nil
	}
	
	shouldRebuild := false
	
	// 2. Proactive: Cumulative Staleness check
	// If the number of deletions reaches a threshold (e.g. 10% of total capacity), rebuild.
	staleCount := c.bloom.deletions.Load()
	threshold := int64(float64(c.BloomEstimatedKeys) * 0.10)
	if staleCount > threshold {
		shouldRebuild = true
	}
	
	// 3. Reactive: Observed FPR check (if we haven't hit volume threshold yet)
	if !shouldRebuild {
		hits := c.bloom.hits.Load()
		ghosts := c.bloom.ghosts.Load()
		if hits > 2000 {
			observedFPR := float64(ghosts) / float64(hits)
			if observedFPR > (c.config.BloomFPRate * 5.0) {
				shouldRebuild = true
			}
		}
	}
	
	if shouldRebuild {
		return c.rebuildBloom()
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

// UnsafePut stores key-value without copying
// Caller must ensure key and value are not modified after this call
func (c *Cache) Put(key []byte, value []byte) {
	h := c.KeyHasher(key)
	c.memTable.Put(h, value)
	c.bloom.Load().Add(h)
}

// UnsafePutChecksummed stores key-value with checksum without copying
// Caller must ensure key and value are not modified after this call
func (c *Cache) PutChecksummed(key []byte, value []byte, checksum uint32) {
	h := c.KeyHasher(key)
	c.memTable.PutChecksummed(h, value, checksum)
	c.bloom.Load().Add(h)
}

type Batcher interface {
	PutBatch(segID int64, records []metadata.BlobRecord) error
}

func (c *Cache) PutBatch(segID int64, records []metadata.BlobRecord) error {
	if err := c.index.IngestBatch(segID, records); err != nil {
		return err
	}
	
	// Phase 3: Update size tracking and trigger reactive eviction if needed
	var addedBytes int64
	for _, rec := range records {
		addedBytes += rec.LogicalSize
	}
	newSize := c.approxSize.Add(addedBytes)
	
	// Trigger eviction if over limit
	// First write sends to channel (non-blocking), subsequent writes block until eviction completes
	if c.MaxSize > 0 && newSize > c.MaxSize && !c.IsDegraded() {
		c.triggerEviction()
	}
	return nil
}

func (c *Cache) triggerEviction() {
	select {
	case c.evictionTrigger <- struct{}{}:
	default:
	}
}

// Get retrieves a value by key as a Reader
// Returns (reader, true) if found, (nil, false) if not found
// IncludeChecksums are verified internally if WithVerifyOnRead is enabled
func (c *Cache) Get(key []byte) (io.Reader, bool) {
	h := c.KeyHasher(key)
	// 1. Bloom Filter (Fastest "No")
	if !c.bloom.Load().Test(h) {
		return nil, false
	}
	
	// If we are here, the Bloom filter said "Yes"
	c.bloom.hits.Add(1)
	
	// 2. Check memtable (recent writes)
	if value, found := c.memTable.Get(Key(h)); found {
		return value, true
	}
	
	// 3. RAM Index Check (The Truth)
	// index.Get(h) marks the entry as 'visited' for the Sieve algorithm internally.
	entry, ok := c.index.Get(h)
	if !ok {
		// Bloom said YES, but Index says NO: This is a Ghost (False Positive).
		c.bloom.ghosts.Add(1)
		return nil, false
	}
	
	// 3. Read from disk using BlobReader
	// Reader handles index lookup and checksum verification internally
	reader, err := c.storage.ReadBlob(entry)
	if err != nil {
		if IsTransientIOError(err) {
			// SYSTEM ISSUE: The file is likely there, but we can't get it right now.
			// We keep the index entry and just return a miss to the caller.
			log.Error("transient storage error (skipping)", "hash", h, "error", err)
			return nil, false
		}
		
		// DATA ISSUE: The error is permanent (e.g., os.ErrNotExist).
		// The index is desynced from the disk. Self-heal by removing the stale entry.
		log.Warn("permanent storage failure: removing stale index entry",
			"hash", h, "error", err)
		
		err := c.index.DeleteBlobs(entry)
		if err == nil {
			c.approxSize.Add(-entry.LogicalSize)
			// Update Bloom metrics: This key is now a "Ghost" in the current filter
			// and its removal counts toward the staleness/rebuild threshold.
			c.bloom.ghosts.Add(1)
			c.bloom.deletions.Add(1)
		} else {
			log.Warn("index update failure: removing stale index entry", "hash", h, "error", err)
		}
		
		return nil, false
	}
	return reader, true
}
