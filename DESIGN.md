# BlobCache Design Document

## Project Scope

**What it is:** High-performance blob cache with bloom filter optimization for fast negative lookups.

**What it is NOT:** General-purpose storage engine.

**Use case:** Time-series blob cache (logs, metrics, traces) with size-bounded FIFO eviction.

---

## Design Goals

1. **Extremely high write throughput** (800+ MB/s on NVMe)
2. **Extremely fast negative lookups** (<1 µs via bloom filter)
3. **Minimal locking** (locks OK, excessive locking not OK)
4. **Fast startup** (<100ms for 4M keys)
5. **Simple implementation** (no compaction, no range queries)

**Target:** 98% CPU reduction vs RocksDB FIFO (5,000 → 100 cores fleet-wide)

---

## Architecture

### Directory Structure

```
/data/blobcache/
├── db/
│   └── index.duckdb          # DuckDB index
├── blooms/
│   └── global.bloom          # Single bloom filter (~6 MB)
└── blobs/
    ├── shard-000/
    │   ├── 1.blob            # One blob per file (immutable)
    │   ├── 2.blob
    │   └── ...               # ~16K files per shard
    ├── shard-001/
    └── ...                   # 256 shards total
```

**Sharding:** Filesystem performance (not lock contention)
- NVMe handles small files well
- 256 shards × 16K files = filesystem happy
- Shard count configurable ONLY for new database

---

## Database Schema

```sql
CREATE TABLE entries (
    key BLOB PRIMARY KEY,
    shard_id INTEGER NOT NULL,
    file_id INTEGER NOT NULL,
    size INTEGER NOT NULL,
    ctime BIGINT NOT NULL,     -- Creation time
    mtime BIGINT NOT NULL      -- Last modification time
);

CREATE INDEX idx_ctime ON entries(ctime);  -- For ctime-based FIFO
CREATE INDEX idx_mtime ON entries(mtime);  -- For mtime-based FIFO
```

**File mapping:** `blobs/shard-{shardID}/{fileID}.blob` = one blob

**No offsets:** Each blob is its own file (immutable)

---

## API

### Core Operations

```go
// Write (caller handles compression if needed)
Put(ctx context.Context, key, value []byte) error

// Write streaming (for huge blobs)
PutReader(ctx context.Context, key string, reader io.Reader) error

// Read
Get(ctx context.Context, key []byte) ([]byte, error)

// Delete individual key
Delete(ctx context.Context, key []byte) error

// Close (graceful shutdown, saves bloom filter)
Close() error
```

**Errors:**
- `ErrNotFound` - Key doesn't exist
- `ErrCorrupted` - Checksum mismatch (if verification enabled)

### Configuration (Options Pattern)

```go
// Zero-config (sensible defaults)
cache, err := blobcache.Open()

// With options
cache, err := blobcache.Open(
    blobcache.WithPath("/data/cache"),
    blobcache.WithMaxSize(1 * blobcache.TB),
    blobcache.WithShards(256),
    blobcache.WithEvictionStrategy(blobcache.EvictByCTime),
    blobcache.WithChecksums(true),        // Default
    blobcache.WithFsync(false),           // Default
    blobcache.WithVerifyOnRead(false),    // Default
    blobcache.WithMetrics(datadogClient), // Datadog integration
)
```

**Defaults:**
- Path: `./blobcache-data`
- MaxSize: 80% of disk capacity
- Shards: 256
- Eviction: EvictByCTime (oldest first)
- Checksums: true (detect corruption)
- Fsync: false (cache semantics, fast)
- VerifyOnRead: false (opt-in for paranoid)

---

## Write Path

```go
func (c *BlobCache) Put(ctx context.Context, key, value []byte) error {
    // 1. Add checksum if enabled
    data := value
    if c.opts.Checksums {
        checksum := crc32.ChecksumIEEE(value)
        data = append(value, make([]byte, 4)...)
        binary.LittleEndian.PutUint32(data[len(value):], checksum)
    }

    // 2. Select shard (filesystem sharding)
    shardID := int(xxhash.Sum64(key) % uint64(c.opts.Shards))
    shard := c.shards[shardID]
    shardDir := filepath.Join(c.opts.Path, "blobs", fmt.Sprintf("shard-%03d", shardID))

    // 3. Write to temporary file
    tempPath := filepath.Join(shardDir, randomID()+".tmp")
    if err := os.WriteFile(tempPath, data, 0644); err != nil {
        return err
    }

    // 4. Generate unique file ID (atomic, lock-free)
    fileID := shard.nextFileID.Add(1)
    finalPath := filepath.Join(shardDir, fmt.Sprintf("%d.blob", fileID))

    // 5. Atomic rename (crash-safe)
    if err := os.Rename(tempPath, finalPath); err != nil {
        os.Remove(tempPath)
        return err
    }

    if c.opts.Fsync {
        syncDirectory(shardDir)  // Persist metadata
    }

    // 6. Update index (DuckDB thread-safe)
    now := time.Now().UnixNano()
    _, err := c.index.db.ExecContext(ctx,
        `INSERT INTO entries (key, shard_id, file_id, size, ctime, mtime)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET
           shard_id = excluded.shard_id,
           file_id = excluded.file_id,
           size = excluded.size,
           mtime = excluded.mtime`,
        key, shardID, fileID, len(data), now, now)

    if err != nil {
        // Blob file orphaned (cleaned up by daily scanner)
        return err
    }

    // 7. Update bloom filter
    c.bloom.mu.Lock()
    c.bloom.filter.Add(key)
    c.bloom.mu.Unlock()

    // 8. Emit metrics
    if c.metrics != nil {
        c.metrics.RecordPut(len(data))
    }

    return nil
}
```

---

## Read Path

```go
func (c *BlobCache) Get(ctx context.Context, key []byte) ([]byte, error) {
    // 1. Bloom filter check (~0.1 µs)
    c.bloom.mu.RLock()
    maybe := c.bloom.filter.Test(key)
    c.bloom.mu.RUnlock()

    if !maybe {
        c.metrics.RecordBloomReject()
        return nil, ErrNotFound
    }

    // 2. Index lookup
    var shardID, fileID, size int
    err := c.index.db.QueryRowContext(ctx,
        "SELECT shard_id, file_id, size FROM entries WHERE key = ?",
        key).Scan(&shardID, &fileID, &size)

    if err == sql.ErrNoRows {
        c.metrics.RecordBloomFalsePositive()
        return nil, ErrNotFound
    }
    if err != nil {
        return nil, err
    }

    // 3. Read blob file
    path := filepath.Join(c.opts.Path, "blobs",
        fmt.Sprintf("shard-%03d", shardID),
        fmt.Sprintf("%d.blob", fileID))

    data, err := os.ReadFile(path)
    if err != nil {
        if os.IsNotExist(err) {
            c.metrics.RecordEvictionRace()
            return nil, ErrNotFound  // File evicted (benign race)
        }
        return nil, err
    }

    // 4. Verify checksum (if opted in)
    if c.opts.VerifyOnRead && c.opts.Checksums {
        if len(data) < 4 {
            return nil, ErrCorrupted
        }
        stored := binary.LittleEndian.Uint32(data[len(data)-4:])
        computed := crc32.ChecksumIEEE(data[:len(data)-4])
        if stored != computed {
            c.metrics.RecordCorruption()
            return nil, ErrCorrupted
        }
        data = data[:len(data)-4]  // Strip checksum
    }

    c.metrics.RecordGet(len(data))
    return data, nil
}
```

---

## Eviction

### Background Worker

```go
func (c *BlobCache) evictionWorker() {
    ticker := time.NewTicker(60 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            c.evictIfNeeded(context.Background())
        case <-c.stopCh:
            return
        }
    }
}
```

### Eviction Logic

```go
func (c *BlobCache) evictIfNeeded(ctx context.Context) error {
    // 1. Get current size (DuckDB aggregation)
    var totalSize int64
    c.index.db.QueryRow("SELECT COALESCE(SUM(size), 0) FROM entries").Scan(&totalSize)

    if totalSize <= c.opts.MaxSize {
        return nil
    }

    bytesToFree := totalSize - c.opts.MaxSize

    // 2. Query oldest files (configurable strategy)
    orderBy := "ctime"
    if c.opts.EvictionStrategy == EvictByMTime {
        orderBy = "mtime"
    }

    rows, _ := c.index.db.QueryContext(ctx, fmt.Sprintf(`
        SELECT shard_id, file_id, SUM(size) as file_size
        FROM entries
        GROUP BY shard_id, file_id
        ORDER BY MIN(%s) ASC
    `, orderBy))

    defer rows.Close()

    // 3. Collect files until enough space freed
    var toDelete []struct{ shardID, fileID int }
    var freed int64

    for rows.Next() && freed < bytesToFree {
        var shardID, fileID int
        var fileSize int64
        rows.Scan(&shardID, &fileID, &fileSize)
        toDelete = append(toDelete, struct{ shardID, fileID int }{shardID, fileID})
        freed += fileSize
    }

    // 4. Delete (atomic)
    c.metrics.RecordEviction(len(toDelete), freed)
    return c.deleteFiles(ctx, toDelete)
}
```

### Atomic Deletion

```go
func (c *BlobCache) deleteFiles(ctx context.Context, files []struct{ shardID, fileID int }) error {
    // CRITICAL ORDER: Database first, then filesystem
    //
    // Why:
    //   1. DB delete → Get() returns ErrNotFound (correct)
    //   2. FS delete → Frees disk space
    //
    // If reversed:
    //   - Get() would return I/O errors instead of ErrNotFound
    //
    // Crash between steps:
    //   - Orphaned blob files (cleaned up by daily scanner)
    //   - No correctness issue

    // 1. Delete from database (atomic transaction)
    tx, _ := c.index.db.BeginTx(ctx, nil)
    defer tx.Rollback()

    for _, f := range files {
        tx.Exec("DELETE FROM entries WHERE shard_id = ? AND file_id = ?",
            f.shardID, f.fileID)
    }

    tx.Commit()

    // 2. Delete from filesystem
    for _, f := range files {
        path := filepath.Join(c.opts.Path, "blobs",
            fmt.Sprintf("shard-%03d", f.shardID),
            fmt.Sprintf("%d.blob", f.fileID))
        os.Remove(path)
    }

    return nil
}
```

**Bloom filter:** NOT updated on delete (accept false positives, rebuild periodically)

---

## Bloom Filter Management

### Global Filter

```go
type BloomFilter struct {
    filter *bloom.BloomFilter  // github.com/bits-and-blooms/bloom
    mu     sync.RWMutex
}

func newBloomFilter(estimatedKeys int, fpRate float64) *BloomFilter {
    filter := bloom.NewWithEstimates(uint(estimatedKeys), fpRate)
    // 4M keys @ 1% FP = ~6 MB memory

    return &BloomFilter{filter: filter}
}
```

### Rebuild & Persist

```go
func (c *BlobCache) rebuildBloom(ctx context.Context) error {
    // Scan all keys (DuckDB: 4M rows in ~40ms)
    rows, _ := c.index.db.QueryContext(ctx, "SELECT key FROM entries")
    defer rows.Close()

    newFilter := bloom.NewWithEstimates(uint(c.opts.BloomEstimatedKeys), c.opts.BloomFPRate)
    for rows.Next() {
        var key []byte
        rows.Scan(&key)
        newFilter.Add(key)
    }

    // Atomic swap
    c.bloom.mu.Lock()
    c.bloom.filter = newFilter
    c.bloom.mu.Unlock()

    // Persist immediately
    c.persistBloom()
    return nil
}

func (c *BlobCache) persistBloom() error {
    c.bloom.mu.RLock()
    data, _ := c.bloom.filter.MarshalBinary()
    c.bloom.mu.RUnlock()

    path := filepath.Join(c.opts.Path, "blooms", "global.bloom")
    return os.WriteFile(path, data, 0644)
}
```

**Frequency:**
- Persist: Every 5 minutes
- Rebuild: Every 24 hours (or on significant FP rate increase)

---

## Crash Recovery & Startup

### Fast Startup (<100ms)

```go
func Open(opts ...Option) (*BlobCache, error) {
    options := defaultOptions()
    for _, opt := range opts {
        opt(&options)
    }

    // 1. Open DuckDB (auto-recovery via WAL)
    db, _ := sql.Open("duckdb", dbPath)

    // 2. Load bloom filter
    bloomPath := filepath.Join(options.Path, "blooms", "global.bloom")
    data, err := os.ReadFile(bloomPath)
    if err != nil {
        // Rebuild from index (40ms for 4M keys)
        rebuildBloom()
    } else {
        bloom.filter.UnmarshalBinary(data)
    }

    // 3. Start background workers
    go c.evictionWorker()
    go c.bloomPersistWorker()
    go c.orphanScannerWorker()

    // Ready! No filesystem scan.
    return c, nil
}
```

### Orphan Cleanup (Background, Not Startup)

```go
func (c *BlobCache) orphanScannerWorker() {
    // Run once per day
    ticker := time.NewTicker(24 * time.Hour)

    for {
        select {
        case <-ticker.C:
            c.cleanOrphans()
        case <-c.stopCh:
            return
        }
    }
}

func (c *BlobCache) cleanOrphans() {
    for shardID := 0; shardID < c.opts.Shards; shardID++ {
        shardDir := filepath.Join(c.opts.Path, "blobs", fmt.Sprintf("shard-%03d", shardID))
        files, _ := os.ReadDir(shardDir)

        for _, file := range files {
            // Remove temp files
            if strings.HasSuffix(file.Name(), ".tmp") {
                os.Remove(filepath.Join(shardDir, file.Name()))
                continue
            }

            // Check if blob file exists in index
            var fileID int
            fmt.Sscanf(file.Name(), "%d.blob", &fileID)

            var exists int
            c.index.db.QueryRow(
                "SELECT 1 FROM entries WHERE shard_id = ? AND file_id = ? LIMIT 1",
                shardID, fileID).Scan(&exists)

            if exists == 0 {
                // Orphaned blob (crash between write and DB insert)
                os.Remove(filepath.Join(shardDir, file.Name()))
                c.metrics.RecordOrphanCleaned()
            }
        }
    }
}
```

**System is immediately usable on startup. Cleanup happens in background.**

---

## Metrics Integration (Datadog)

```go
type Metrics interface {
    RecordPut(bytes int)
    RecordGet(bytes int)
    RecordBloomReject()
    RecordBloomFalsePositive()
    RecordEviction(files int, bytes int64)
    RecordEvictionRace()
    RecordCorruption()
    RecordOrphanCleaned()
}

// Datadog implementation
type DatadogMetrics struct {
    client *statsd.Client
}

func (m *DatadogMetrics) RecordPut(bytes int) {
    m.client.Count("blobcache.put", 1, nil, 1)
    m.client.Count("blobcache.bytes_written", int64(bytes), nil, 1)
}

// ... more methods

// Usage:
ddClient, _ := statsd.New("127.0.0.1:8125")
cache, _ := blobcache.Open(
    blobcache.WithMetrics(&DatadogMetrics{client: ddClient}),
)
```

**Metrics exported:**
- `blobcache.put` - Put operations
- `blobcache.get` - Get operations
- `blobcache.bloom_rejects` - Bloom filter fast rejections
- `blobcache.bloom_false_positives` - Bloom FPs
- `blobcache.evictions` - Files evicted
- `blobcache.bytes_written` - Total bytes written
- `blobcache.bytes_read` - Total bytes read
- `blobcache.corruption_detected` - Checksum failures

---

## Concurrency & Locking

### Read Path (High Concurrency)

```
Bloom: RWMutex (many readers, single writer on rebuild)
Index: DuckDB internal locking
Blob files: Read-only, no locking
```

**Scalability:** O(cores)

### Write Path (Sharded)

```
File ID: Atomic increment (lock-free)
Temp write: No lock (unique temp names)
Index: DuckDB locking (optimized)
Bloom: Mutex on Add (brief)
```

**256 shards = 256 concurrent writers.**

### Benign Races (Acceptable)

```
✓ Read during eviction → ErrNotFound
✓ Bloom stale during rebuild → Extra index queries
✓ Orphans between write and DB → Daily cleanup
✓ Duplicate reads of same key → Extra I/O (fine)

✗ NOT acceptable:
  - Data corruption
  - Lost writes
  - Crashes
```

---

## Future Extensions

### Custom Eviction Logic

```go
type EvictionPolicy interface {
    ShouldEvict(entry Entry) bool
}

blobcache.WithCustomEviction(policy EvictionPolicy)

// Example: Evict by size + age combo
type SizeAgePolicy struct {
    maxAge time.Duration
    minSize int
}

func (p *SizeAgePolicy) ShouldEvict(e Entry) bool {
    age := time.Now().UnixNano() - e.CTime
    return age > p.maxAge.Nanoseconds() && e.Size < p.minSize
}
```

### Bloom Hash Tracking (For Individual Deletes)

```sql
ALTER TABLE entries ADD COLUMN bloom_hash BIGINT;

-- On delete:
SELECT COUNT(*) FROM entries WHERE bloom_hash = ?;
-- If 1: Clear bloom bits
-- If >1: Keep (collision)
```

**When useful:** High-frequency individual Delete() operations.

---

## Performance Projections

### vs RocksDB FIFO (Production Workload)

| Metric | RocksDB | BlobCache | Improvement |
|--------|---------|-----------|-------------|
| **Negative lookup** | 45 µs | 0.1 µs | 450× faster |
| **Positive lookup** | 2-3 ms | 2-3 ms | Same |
| **Write throughput** | 800 MB/s | 800 MB/s | Same |
| **CPU (reads)** | 5,000 cores | 100 cores | **98% reduction** |
| **Memory** | 6 GB memtables | 6 MB bloom | **99.9% reduction** |
| **Complexity** | High (compaction) | Low (FIFO delete) | Simpler |

---

## Implementation Phases

### Phase 1: Core (2 weeks)
- Options pattern
- DuckDB index layer
- Single-shard Put/Get
- Checksums
- Unit tests

### Phase 2: Scaling (2 weeks)
- Multi-shard support
- Global bloom filter
- FIFO eviction
- Benchmarks

### Phase 3: Production (4 weeks)
- Crash recovery
- Orphan cleanup
- Metrics integration (Datadog)
- Stress tests
- Documentation

**Total: 2-3 months**

---

## Out of Scope

**Not implemented:**
- ❌ Range queries (not needed for blob cache)
- ❌ Sorted iteration (not needed)
- ❌ Compression (caller-controlled)
- ❌ Compaction (FIFO deletion only)
- ❌ Resharding existing data (require empty DB)

---

## Decision Log

**Reviewed decisions:**
1. ✅ One blob per file (no offsets, no packing)
2. ✅ Global bloom filter (not per-shard)
3. ✅ Checksums enabled by default (CRC32)
4. ✅ Fsync disabled by default (cache semantics)
5. ✅ DB-first deletion order (then filesystem)
6. ✅ Fast startup (no filesystem scan)
7. ✅ Daily orphan cleanup (not on startup)
8. ✅ Datadog metrics integration
9. ✅ filepath.Join for all paths
10. ✅ Simple over clever

**Design approved for implementation.**
