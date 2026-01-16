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

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/bloom"
	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/miretskiy/blobcache/internal/wal"
	"github.com/zeebo/xxh3"
)

// Key is the 128-bit hash of a blob key.
// Lo is used for bloom filter; full 128-bit for index to avoid collisions.
type Key = xxh3.Uint128

const (
	// evictionHysteresis is the target fraction of MaxSize to evict to.
	evictionHysteresis = 0.93
)

// Cache is a high-performance blob storage with bloom filter optimization
type Cache struct {
	config
	index   *index.DurableIndex
	storage *Storage
	wal     *wal.WAL // nil if WAL disabled
	bloom   struct {
		atomic.Pointer[bloom.Filter]
		hits        atomic.Uint64             // Bloom filter said "yes"
		ghosts      atomic.Uint64             // Bloom said yes, but index said no.
		deletions   atomic.Int64              // Track cumulative deletions since last rebuild
		lastRebuild atomic.Pointer[time.Time] // When the last rebuild happened.
	}

	// --- ARCHITECTURE COMPONENTS ---
	memTable  *MemTable  // The Write Engine (Producer)
	librarian *Librarian // The Read Cache (Consumer)

	// Global monotonic sequence counter for operation ordering.
	// Initialized to time.Now().UnixNano() for continuity across restarts.
	// This ensures sequences are always increasing even after crashes,
	// without needing to scan WAL/segments for the last sequence.
	globalSeq atomic.Uint64

	// LogicalSize tracking for reactive eviction
	approxSize      atomic.Int64 // Approximate total size (updated during flush/eviction)
	evictionRunning atomic.Bool  // Prevents concurrent evictions

	// Background error tracking
	bgError atomic.Pointer[error] // First background error (nil = healthy)

	// Background workers
	evictionTrigger chan struct{} // Capacity 1: trigger eviction, blocks when eviction running
	stopCh          chan struct{}
	wg              sync.WaitGroup

	// Knobs provides testing hooks. Set directly in tests: c.Knobs = &TestingKnobs{...}
	Knobs *TestingKnobs
}

// ErrorReporter interface allows memtable to check/set degraded state
// without direct dependency on Cache
type ErrorReporter interface {
	IsDegraded() bool
	ReportError(error)
	ReportBlobError(key Key, errno base.BlobErrno)
}

// SequenceVendor provides monotonic sequence IDs for write ordering.
// Implement this interface to override the default sequence generation in tests.
type SequenceVendor interface {
	NextSeq() uint64
}

func (c *Cache) IsDegraded() bool {
	return c.bgError.Load() != nil
}

func (c *Cache) ReportError(err error) {
	if c.bgError.CompareAndSwap(nil, &err) {
		log.Error("entering degraded mode (memory-only)", "error", err)
	}
}

func (c *Cache) ReportBlobError(key Key, errno base.BlobErrno) {
	if errno == base.ErrNone {
		return
	}
	log.Warn("blob error reported", "key", key, "errno", errno)
	c.index.SetBlobErrno(key, errno)
}

// nextSeq atomically increments and returns the next sequence ID.
// This is THE source of truth for operation ordering.
func (c *Cache) nextSeq() uint64 {
	if c.Knobs != nil && c.Knobs.SequenceVendor != nil {
		return c.Knobs.SequenceVendor.NextSeq()
	}
	return c.globalSeq.Add(1)
}

// BGError returns any background error (nil if healthy)
func (c *Cache) BGError() error {
	if ptr := c.bgError.Load(); ptr != nil {
		return *ptr
	}
	return nil
}

// New creates a Cache at the specified path with optional configuration.
// Uses "crash-only" initialization: if WAL recovery is needed, we recover,
// flush to segments, close, and re-open cleanly. This ensures the cache
// always starts in a consistent state without "mixed mode" initialization.
func New(path string, opts ...Option) (*Cache, error) {
	cfg := defaultConfig(path)
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	// Iterative open with hard limit of 2 attempts:
	// - Attempt 1: If WAL exists, recover it, flush to segments, close, restart
	// - Attempt 2: Should be clean (no WAL). If WAL still exists, fail.
	const maxAttempts = 2
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c, recovered, err := open(cfg)
		if err != nil {
			return nil, err
		}

		if !recovered {
			// Clean open - no WAL recovery needed
			return c, nil
		}

		// WAL recovery happened - close and restart fresh
		log.Info("WAL recovery completed, restarting for clean state", "attempt", attempt)
		if closeErr := c.Close(); closeErr != nil {
			return nil, fmt.Errorf("close after recovery: %w", closeErr)
		}

		// Safety check: on second attempt, WAL should be gone
		if attempt == maxAttempts-1 {
			// Next iteration is the last - if we still have WAL files, that's a bug
			continue
		}
	}

	return nil, fmt.Errorf("WAL files still present after %d recovery attempts (bug?)", maxAttempts)
}

// open is the internal initialization function.
// Returns (cache, recovered, error) where recovered=true means WAL recovery happened.
func open(cfg config) (*Cache, bool, error) {
	// Ensure directory structure exists and validate configuration
	idx, err := checkOrInitialize(cfg)
	if err != nil {
		return nil, false, fmt.Errorf("initialization failed: %w", err)
	}

	// Create new bloom filter and figure out how much data on disk from segment meta.
	var totalSize int64
	filter := bloom.New(uint(cfg.BloomEstimatedKeys), cfg.BloomFPRate)
	if err := idx.ForEachSegment(func(m index.DurableBatch) bool {
		for _, item := range m.Items {
			if !item.IsDeleted() {
				filter.Add(item.Key)                 // Full 128-bit key
				totalSize += int64(item.PhysicalLen) // Track on-disk size
			}
		}
		return true
	}); err != nil {
		return nil, false, err
	}

	c := &Cache{
		config:          cfg,
		index:           idx,
		storage:         NewStorage(cfg, idx),
		evictionTrigger: make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}

	// Initialize WAL if enabled
	if cfg.WAL.Enabled {
		w, walErr := c.initWAL()
		if walErr != nil {
			return nil, false, fmt.Errorf("WAL initialization failed: %w", walErr)
		}
		c.wal = w
	}

	// Initialize sequence counter with current nanosecond timestamp.
	// This guarantees monotonicity across process restarts:
	// - UnixNano gives ~292 years before overflow
	// - Even if we restart 1 second later, new sequences are guaranteed higher
	// - Clock skew is acceptable (we just need monotonicity within a run)
	c.globalSeq.Store(uint64(time.Now().UnixNano()))

	c.librarian = NewLibrarian(cfg.MaxCachedSlabs, c)
	c.bloom.Store(filter)
	c.approxSize.Store(totalSize)
	c.Knobs = cfg.knobs
	c.memTable = NewMemTable(c.config, c, c, c.librarian, c.wal)
	c.memTable.Knobs = c.Knobs

	// Run WAL recovery after memtable is initialized
	var recovered bool
	if c.wal != nil {
		var recoveryErr error
		recovered, recoveryErr = c.runWALRecovery()
		if recoveryErr != nil {
			// Close what we've opened so far
			c.wal.Close()
			return nil, false, fmt.Errorf("WAL recovery failed: %w", recoveryErr)
		}
	}

	return c, recovered, nil
}

// Start begins background operations (eviction worker).
func (c *Cache) Start() {
	c.wg.Add(1)
	go c.evictionWorker()
}

// Close gracefully shuts down all background workers and saves state
func (c *Cache) Close() error {
	// Signal workers to stop (idempotent)
	select {
	case <-c.stopCh:
		return nil
	default:
		close(c.stopCh)
	}

	// 1. Close Write Path (Stops new slabs)
	c.memTable.Close()

	// 2. Close Read Path (Releases pinned slabs back to pool)
	c.librarian.Close()

	// 3. Release pool memory (must be after librarian returns slabs)
	c.memTable.ClosePools()

	c.wg.Wait()

	// Collect all close errors (WAL may be nil if disabled)
	var walErr error
	if c.wal != nil {
		walErr = c.wal.Close()
	}

	return errors.Join(
		walErr,
		c.storage.Close(),
		c.index.Close(),
	)
}

// Drain waits for all pending memtable writes to complete
func (c *Cache) Drain() {
	c.memTable.Drain()
}

// checkOrInitialize ensures directory structure exists and validates configuration
func checkOrInitialize(cfg config) (*index.DurableIndex, error) {
	markerPath := filepath.Join(cfg.Path, ".initialized")

	// Use BloomEstimatedKeys as index capacity hint
	capacityHint := cfg.BloomEstimatedKeys
	if capacityHint < 1024 {
		capacityHint = 1024
	}

	// Check if already initialized
	if _, err := os.Stat(markerPath); err == nil {
		return index.Open(cfg.Path, capacityHint)
	}

	// Not initialized - create directory structure
	for i := 0; i < max(1, cfg.Shards); i++ {
		shardDir := filepath.Join(cfg.Path, "segments", fmt.Sprintf("%04d", i))
		if err := os.MkdirAll(shardDir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create %04d: %w", i, err)
		}
	}

	idx, err := index.Open(cfg.Path, capacityHint)
	if err != nil {
		return nil, fmt.Errorf("failed to open index: %w", err)
	}
	// Touch empty marker file
	if err := os.WriteFile(markerPath, []byte{}, 0o644); err != nil {
		return nil, fmt.Errorf("failed to write marker: %w", err)
	}

	return idx, nil
}

// --- UNIFIED LOOKUP PIPELINE ---

// search attempts to locate the blob in RAM (returning data) or Disk (returning r).
// It acts as the Single Source of Truth for Bloom metrics (Hits vs Ghosts).
func (c *Cache) search(key []byte) (data []byte, r io.Reader, rel Releaser, ok bool) {
	h := xxh3.Hash128(key)

	// 1. Bloom Filter Gate (full 128-bit key)
	if !c.bloom.Load().Test(h) {
		return nil, nil, Releaser{}, false
	}
	c.bloom.hits.Add(1) // Bloom said "yes"

	// 2. RAM Hit (Librarian)
	if ramData, releaser, found := c.librarian.Acquire(h); found {
		return ramData, nil, releaser, true
	}

	// 3. Disk Hit (Storage)
	entry, found := c.index.Get(h)
	if !found {
		// BLOOM GHOST: Bloom said yes, Index said no.
		c.bloom.ghosts.Add(1)
		return nil, nil, Releaser{}, false
	}

	// 4. Check corruption flag
	if entry.HasError() {
		log.Debug("blob marked as corrupt", "hash", h, "errno", entry.Errno())
		return nil, nil, Releaser{}, false
	}

	// 5. Read from disk (with key verification for collision detection)
	diskReader, diskReleaser, err := c.storage.ReadBlob(entry, key)
	if err != nil {
		diskReleaser.Release()
		c.handleStorageError(h, entry, err)
		return nil, nil, Releaser{}, false
	}

	return nil, diskReader, diskReleaser, true
}

// ZeroCopyView provides a unified reader for both RAM and Disk hits.
// If found is true, the caller MUST call the returned Releaser expediently.
func (c *Cache) ZeroCopyView(key []byte) (io.Reader, Releaser, bool) {
	data, r, rel, ok := c.search(key)
	if !ok {
		return nil, Releaser{}, false
	}

	if data != nil {
		// RAM Hit: Wrap raw bytes in a reader.
		return bytes.NewReader(data), rel, true
	}

	// Disk Hit: Reader is already set.
	return r, rel, true
}

func (c *Cache) Append(key []byte, dst []byte) ([]byte, bool) {
	data, r, rel, ok := c.search(key)
	if !ok {
		return dst, false
	}
	defer rel.Release()

	if data != nil {
		// Fast Path: Direct append (Zero Alloc)
		return append(dst, data...), true
	}

	// Slow Path: Disk Reader
	buf := bytes.NewBuffer(dst)
	buf.Reset()
	if _, err := io.Copy(buf, r); err != nil {
		return dst, false
	}
	return buf.Bytes(), true
}

// View provides scoped access to a value via an io.Reader.
func (c *Cache) View(key []byte, fn func(r io.Reader)) bool {
	data, r, rel, ok := c.search(key)
	if !ok {
		return false
	}
	defer rel.Release()

	if data != nil {
		fn(bytes.NewReader(data))
	} else {
		fn(r)
	}
	return true
}

func (c *Cache) Get(key []byte) ([]byte, bool) {
	return c.Append(key, nil)
}

// ErrEmptyKey is returned when Put is called with an empty key.
var ErrEmptyKey = errors.New("blobcache: empty key not allowed")

func (c *Cache) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	h := xxh3.Hash128(key)
	c.bloom.Load().Add(h)
	c.putWithRetry(h, key, value, nil)
	return nil
}

func (c *Cache) PutChecksummed(key []byte, value []byte, checksum uint32) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	h := xxh3.Hash128(key)
	c.bloom.Load().Add(h)
	c.putWithRetry(h, key, value, &checksum)
	return nil
}

// putWithRetry handles the zombie writer resurrection protocol.
// If a write is rejected (seqID too old due to slab rotation), we check
// if a newer version already exists. If so, we return success (idempotent).
// If not, we acquire a fresh seqID and retry.
func (c *Cache) putWithRetry(h Key, keyBytes, value []byte, checksum *uint32) {
	seqID := c.nextSeq()

	for {
		var err error
		if checksum != nil {
			err = c.memTable.PutChecksummed(seqID, h, keyBytes, value, *checksum)
		} else {
			err = c.memTable.Put(seqID, h, keyBytes, value)
		}

		if err == nil {
			return
		}

		if !errors.Is(err, errSequenceTooOld) {
			c.ReportError(err)
			return
		}

		// Zombie Investigation: Check if a version exists in the global index.
		// If it does, we "succeeded" (last write wins, our data is obsolete).
		// Note: SeqID is not stored in RAM index, so we just check existence.
		if _, found := c.index.Get(h); found {
			return
		}

		// Resurrection: Acquire fresh seqID and retry
		seqID = c.nextSeq()
	}
}

type Batcher interface {
	PutBatch(segID uint32, items []index.Item, maxSeqID uint64) error
}

func (c *Cache) PutBatch(segID uint32, items []index.Item, maxSeqID uint64) error {
	// Phase 1: Ingest into Index
	if err := c.index.IngestBatch(segID, items, maxSeqID); err != nil {
		return err
	}

	// Phase 2: Update size tracking (using PhysicalLen = on-disk size)
	var addedBytes int64
	for _, item := range items {
		addedBytes += int64(item.PhysicalLen)
	}
	newSize := c.approxSize.Add(addedBytes)

	// Phase 3: Trigger eviction if over limit
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

func (c *Cache) handleStorageError(h Key, e index.Item, err error) {
	// 1. Transient errors: Skip and retry later
	if sys.IsTransientIOError(err) {
		log.Error("transient storage error (skipping)", "hash", h, "error", err)
		return
	}

	// 2. Non-transient errors: Mark blob as corrupt
	// By definition, any non-transient error is permanent.
	// We mark the blob with errno but keep metadata for observability.
	// Let normal eviction handle cleanup if needed.
	errno := base.ToErrno(err)
	log.Warn("permanent blob error detected", "hash", h, "errno", errno, "error", err)
	c.ReportBlobError(h, errno)
}

func (c *Cache) rebuildBloom() error {
	newFilter := bloom.New(uint(c.BloomEstimatedKeys), c.BloomFPRate)

	var stopRecording func()
	var consumeRecording func(bloom.KeyConsumer)

	if oldFilter := c.bloom.Load(); oldFilter != nil {
		stopRecording, consumeRecording = oldFilter.RecordAdditions()
	}

	err := c.index.ForEachSegment(func(m index.DurableBatch) bool {
		for _, item := range m.Items {
			if !item.IsDeleted() {
				newFilter.AddHash(item.Key)
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

	oldFilter := c.bloom.Swap(newFilter)

	if oldFilter != nil && stopRecording != nil {
		stopRecording()
		consumeRecording(newFilter.AddHash)
	}

	c.bloom.deletions.Store(0)
	now := time.Now()
	c.bloom.lastRebuild.Store(&now)

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
	staleCount := c.bloom.deletions.Load()
	threshold := int64(float64(c.BloomEstimatedKeys) * 0.10)
	if staleCount > threshold {
		shouldRebuild = true
	}

	// 3. Reactive: Observed FPR check
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

// initWAL opens the WAL for writing.
// WAL files are named by the first SeqID written to them, so no pre-initialization needed.
func (c *Cache) initWAL() (*wal.WAL, error) {
	if c.WAL.Dir == "" {
		c.WAL.Dir = filepath.Join(c.Path, "wal")
	}
	return wal.Open(c.WAL.Config)
}

// cacheReplayer implements wal.Replayer, wrapping Cache for WAL recovery.
type cacheReplayer struct {
	cache *Cache
}

func (r *cacheReplayer) ReplayRecord(rec record.Record) error {
	// Update bloom filter
	h := xxh3.Hash128(rec.Key)
	r.cache.bloom.Load().Add(h)

	if rec.IsDeleted() {
		// Tombstone - for now we just skip (future: implement Delete)
		return nil
	}

	// Replay the Put directly:
	// - Use original SeqID (not new sequence)
	// - Use original CRC (already verified during WAL read)
	// - Bypass compression (value is already in final form)
	// - Write record as-is to slab
	return r.cache.memTable.ReplayRecord(h, rec)
}

func (r *cacheReplayer) Flush() {
	r.cache.memTable.Flush()
}

func (r *cacheReplayer) Drain() {
	r.cache.memTable.Drain()
}

// runWALRecovery replays uncommitted WAL entries into the memtable.
// Returns (recovered, error) where recovered=true if any WAL files were replayed.
func (c *Cache) runWALRecovery() (bool, error) {
	// Compute checkpoint: max SeqID across all committed segments.
	// Any WAL file with firstSeqID <= checkpoint has already been flushed.
	var checkpoint uint64
	if err := c.index.ForEachSegment(func(m index.DurableBatch) bool {
		if m.MaxSeqID > checkpoint {
			checkpoint = m.MaxSeqID
		}
		return true
	}); err != nil {
		return false, fmt.Errorf("scan segments: %w", err)
	}

	// isCommitted returns true if the WAL file's data is already in a segment.
	// Since each WAL file contains a contiguous range [firstSeqID, maxSeqID],
	// if firstSeqID <= checkpoint, all its data was already flushed.
	isCommitted := func(firstSeqID uint64) bool {
		return firstSeqID <= checkpoint
	}

	replayer := &cacheReplayer{cache: c}
	recovered, err := c.wal.Recover(replayer, isCommitted)
	if err != nil {
		return false, err
	}
	return recovered, nil
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
			// Periodic compaction
			if !c.IsDegraded() {
				if err := c.maybeCompactSegments(); err != nil {
					log.Warn("compaction failed", "error", err)
				}
			}

		case <-c.stopCh:
			return
		}
	}
}

// runEvictionSieve evicts blobs using Sieve algorithm until under size limit
func (c *Cache) runEvictionSieve(maxCacheSize int64) error {
	evictionStart := time.Now()

	if c.Knobs != nil && c.Knobs.InjectEvictErr != nil {
		if err := c.Knobs.InjectEvictErr(); err != nil {
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
		victims             []index.Item
		evictedBytes        int64
		physicallyReclaimed int64
	)

	// 1. SELECTION PHASE
	for evictedBytes < toEvictBytes {
		victim, err := c.index.Evict()
		if err != nil {
			break
		}
		victims = append(victims, victim)
		evictedBytes += int64(victim.PhysicalLen)
	}

	if len(victims) == 0 {
		return nil
	}

	// 2. COMMIT PHASE
	if err := c.index.DeleteBlobs(victims...); err != nil {
		return fmt.Errorf("eviction durability sync failed: %w", err)
	}

	// 3. RECLAMATION PHASE
	for _, v := range victims {
		reclaimed, _ := c.storage.HolePunchBlob(v.SegmentID, v.Offset, v.PhysicalLen)
		physicallyReclaimed += reclaimed
	}

	// 4. METRICS & MAINTENANCE
	c.approxSize.Add(-evictedBytes)
	evictedCount := len(victims)

	log.Info("eviction completed",
		"duration", time.Since(evictionStart),
		"evicted_count", evictedCount,
		"evicted_mb", evictedBytes/(1024*1024),
		"reclaimed_mb", physicallyReclaimed/(1024*1024),
		"reclaim_pct", 100*float64(physicallyReclaimed)/float64(evictedBytes),
		"remaining_mb", c.approxSize.Load()/(1024*1024))

	c.bloom.deletions.Add(int64(evictedCount))

	if err := c.maybeTriggerBloomRebuild(); err != nil {
		log.Error("bloom rebuild failed", "error", err)
	}

	return nil
}

func (c *Cache) maybeCompactSegments() error {
	// Placeholder
	return nil
}
