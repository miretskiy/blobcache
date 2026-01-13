# BlobCache: WAL and Compaction Implementation Plan

This document outlines the implementation plan for three major features:
0. **Sequence IDs** - Foundational ordering for consistency (PREREQUISITE)
1. **Write-Ahead Log (WAL)** - Transform blobcache into a Content Addressable Storage (CAS) system
2. **Segment Compaction** - Efficient merging of sparse segments

---

## Part 0: Sequence IDs (Foundational - PREREQUISITE)

### 0.1 Overview

Sequence IDs provide total ordering for all operations, enabling:

1. **WAL Rotation**: Know which sequences are committed to determine when WAL files can be truncated
2. **CAS Semantics**: Newer writes always win - sequence IDs provide definitive ordering
3. **Consistency Guarantees**: Even in pure cache mode, prevent serving stale values
4. **Recovery Correctness**: During WAL replay, detect and skip "ghost writes"

### 0.2 The Problem: Why Sequence IDs Are Essential

Without sequence IDs, two critical race conditions can corrupt data:

#### Race Condition 1: "Time Travel Bug" (Cross-File)

This occurs when a slow old write lands in a new file, effectively hiding a newer write.

```
Timeline:
┌─────────────────────────────────────────────────────────────────┐
│ 1. Thread A calls Put(key). Gets next operation slot.           │
│    → OS scheduler PAUSES Thread A (GC, context switch, etc.)    │
│                                                                 │
│ 2. Thread B calls Put(key). Writes successfully to ActiveSlab.  │
│                                                                 │
│ 3. ActiveSlab becomes full → System SEALS it, creates NEW slab. │
│    → Sealed slab contains Thread B's write (the "latest")       │
│                                                                 │
│ 4. Thread A WAKES UP. Still thinks it's writing to old context. │
│    → Writes to NEW slab (after rotation!)                       │
│                                                                 │
│ 5. User calls Get(key):                                         │
│    → Checks Active Slab first → Finds Thread A's OLD data       │
│    → Never looks at Sealed Slab with Thread B's NEWER data      │
│                                                                 │
│ RESULT: System reverted to stale state!                         │
└─────────────────────────────────────────────────────────────────┘
```

#### Race Condition 2: "Check-Then-Act Bug" (Same-File)

This occurs when two threads update the same key and both read stale state.

```
Timeline (without per-key locking):
┌─────────────────────────────────────────────────────────────────┐
│ Initial: Index[key] = {SeqID: 50, ...}                          │
│                                                                 │
│ Thread A (SeqID=100):           Thread B (SeqID=101):           │
│ ─────────────────────           ─────────────────────           │
│ 1. READ: "Current is 50"                                        │
│                                 2. READ: "Current is 50"        │
│ 3. LOGIC: "100 > 50, update!"                                   │
│                                 4. LOGIC: "101 > 50, update!"   │
│                                 5. STORE: Index[key] = 101 ✓    │
│ 6. STORE: Index[key] = 100 ✗                                    │
│                                                                 │
│ RESULT: Index shows SeqID=100, but SeqID=101 was the latest!    │
└─────────────────────────────────────────────────────────────────┘
```

### 0.3 Two-Layer Protection Design

```
┌─────────────────────────────────────────────────────────────────┐
│                    SEQUENCE ID PROTECTION LAYERS                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  LAYER 1: LIFECYCLE GUARD (Global)                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Purpose: Prevent "Time Travel" across file boundaries   │   │
│  │                                                          │   │
│  │  Mechanism:                                              │   │
│  │  • Track maxSealedSeq: highest SeqID in last sealed slab │   │
│  │  • On Put: if seqID <= maxSealedSeq → DROP immediately   │   │
│  │                                                          │   │
│  │  Why it works:                                           │   │
│  │  • Sealed slab with SeqID=5000 guarantees all ≤5000 done │   │
│  │  • Thread waking with SeqID=4999 is definitively stale   │   │
│  │  • Safe to drop without checking anything else           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  LAYER 2: CONCURRENCY GUARD (Sharded Per-Key)                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Purpose: Prevent "Check-Then-Act" races within slab     │   │
│  │                                                          │   │
│  │  Mechanism:                                              │   │
│  │  • Sharded locks: indexLocks[hash & 1023]               │   │
│  │  • Hold lock during: READ existing → CHECK → STORE new   │   │
│  │  • If existing.SeqID >= incoming.SeqID → DROP           │   │
│  │                                                          │   │
│  │  Why sharded (not global):                               │   │
│  │  • 1024 shards = near-zero contention for random keys    │   │
│  │  • Only same-key writes serialize (which is correct)     │   │
│  │  • Different keys proceed fully in parallel              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 0.4 Data Structure Changes

#### BlobRecord: Add SeqID Field

```go
// metadata/record.go

// Current: 40 bytes (5 × uint64)
// New:     48 bytes (6 × uint64)

const (
    // EncodedBlobRecordSize updated from 40 to 48 bytes:
    // Hash(8) + Pos(8) + LogicalSize(8) + PhysicalSize(8) + Flags(8) + SeqID(8)
    EncodedBlobRecordSize = 48
)

type BlobRecord struct {
    Hash         uint64 // xxhash of key
    Pos          int64  // Offset within segment
    LogicalSize  int64  // Original uncompressed size
    PhysicalSize int64  // Actual size on disk (compressed)
    Flags        uint64 // Metadata, status, and checksum flags
    SeqID        uint64 // Monotonic sequence ID for ordering (NEW)
}
```

**Design Decision**: Adding a dedicated field vs packing into Flags:
- **Dedicated field chosen** because:
  - SeqID needs full 64-bit range (initialized from nanosecond timestamp)
  - Flags already heavily used (checksum, compression, errno, deleted)
  - 8 bytes overhead is negligible vs correctness benefits
  - Simpler debugging and inspection

#### Serialization Updates

```go
// metadata/record.go

func AppendBlobRecord(buf []byte, rec BlobRecord) []byte {
    buf = binary.LittleEndian.AppendUint64(buf, rec.Hash)
    buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.Pos))
    buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.LogicalSize))
    buf = binary.LittleEndian.AppendUint64(buf, uint64(rec.PhysicalSize))
    buf = binary.LittleEndian.AppendUint64(buf, rec.Flags)
    buf = binary.LittleEndian.AppendUint64(buf, rec.SeqID) // NEW
    return buf
}

func DecodeBlobRecord(buf []byte) (BlobRecord, error) {
    if len(buf) < EncodedBlobRecordSize {
        return BlobRecord{}, fmt.Errorf("buffer too small for blob record")
    }
    return BlobRecord{
        Hash:         binary.LittleEndian.Uint64(buf[0:8]),
        Pos:          int64(binary.LittleEndian.Uint64(buf[8:16])),
        LogicalSize:  int64(binary.LittleEndian.Uint64(buf[16:24])),
        PhysicalSize: int64(binary.LittleEndian.Uint64(buf[24:32])),
        Flags:        binary.LittleEndian.Uint64(buf[32:40]),
        SeqID:        binary.LittleEndian.Uint64(buf[40:48]), // NEW
    }, nil
}
```

### 0.5 Global Sequence Counter

```go
// blobcache.go

type Cache struct {
    // ... existing fields ...

    // Global monotonic sequence counter.
    // Initialized to time.Now().UnixNano() for continuity across restarts.
    // This ensures sequences are always increasing even after crashes,
    // without needing to scan WAL/segments for the last sequence.
    globalSeq atomic.Uint64
}

func Open(path string, opts ...Option) (*Cache, error) {
    c := &Cache{
        // ...
    }

    // Initialize sequence counter with current nanosecond timestamp.
    // This guarantees monotonicity across process restarts:
    // - UnixNano gives ~292 years before overflow
    // - Even if we restart 1 second later, new sequences are guaranteed higher
    // - Clock skew is acceptable (we just need monotonicity within a run)
    c.globalSeq.Store(uint64(time.Now().UnixNano()))

    return c, nil
}

// nextSeq atomically increments and returns the next sequence ID.
// This is THE source of truth for operation ordering.
func (c *Cache) nextSeq() uint64 {
    return c.globalSeq.Add(1)
}
```

**Why UnixNano initialization?**
- Provides automatic continuity across restarts without scanning storage
- Even with 1ns resolution, provides ~292 years before overflow
- Clock skew between restarts is acceptable - we only need monotonicity within a process lifetime
- Simpler than persisting and recovering the last sequence ID

### 0.6 MemTable Integration

```go
// memtable.go

const (
    // Power of 2 for fast modulo via bitmask
    numIndexShards = 1024
    indexShardMask = numIndexShards - 1
)

type MemTable struct {
    // ... existing fields ...

    // Sharded locks for per-key consistency within active slab.
    // Usage: indexLocks[hash & indexShardMask].Lock()
    indexLocks [numIndexShards]sync.Mutex

    // Highest SeqID in the last sealed slab.
    // Used by Lifecycle Guard to drop stale writes.
    maxSealedSeq atomic.Uint64

    // Highest SeqID seen in current active slab.
    // Captured during rotation to update maxSealedSeq.
    currentMaxSeq atomic.Uint64
}

// Put is called from Cache.Put with the pre-assigned sequence ID.
// The seqID parameter is the "ticket" establishing this write's place in history.
func (mt *MemTable) Put(seqID uint64, key Key, value []byte) {
    mt.putWithChecksum(seqID, key, value, nil)
}

func (mt *MemTable) putActive(seqID uint64, key Key, value []byte, checksum *uint32) {
    // 1. Compress before lock (parallel compression)
    c := mt.maybeCompress(value)
    defer c.Release()
    mt.putActiveCompressed(seqID, key, value, checksum, c)
}

func (mt *MemTable) putActiveCompressed(
    seqID uint64, key Key, value []byte, checksum *uint32, compressed BufferHandle,
) {
    mt.mu.Lock()

    // ─────────────────────────────────────────────────────────────
    // LAYER 1: LIFECYCLE GUARD (Global Check)
    // ─────────────────────────────────────────────────────────────
    // If this write's sequence is older than the last sealed slab,
    // it's a "zombie" - drop it immediately.
    if seqID <= mt.maxSealedSeq.Load() {
        mt.mu.Unlock()
        return // Silently drop stale write
    }

    // Track highest SeqID for this active slab (for future seal)
    if seqID > mt.currentMaxSeq.Load() {
        mt.currentMaxSeq.Store(seqID)
    }

    // 1. Wait for Rotation (Backpressure) - existing logic
    if mt.mu.activeReady != nil {
        wait := mt.mu.activeReady
        mt.mu.Unlock()
        <-wait
        // IMPORTANT: Re-check lifecycle guard after waking up!
        // Rotation may have happened while we waited.
        mt.putActiveCompressed(seqID, key, value, checksum, compressed)
        return
    }

    active := mt.mu.active
    writeSize := int64(len(value))
    if !compressed.IsZero() {
        writeSize = int64(len(compressed.Bytes()))
    }

    // 2. Check Capacity & Rotate - existing logic with seq tracking
    if active.wPos+writeSize > int64(active.buf.Cap()) {
        rotateUnlocked := mt.prepareRotationLocked()
        mt.mu.Unlock()
        rotateUnlocked()
        mt.putActiveCompressed(seqID, key, value, checksum, compressed)
        return
    }

    // 3. Reservation
    active.pendingWrites.Add(1)
    wPos := active.wPos
    active.wPos += writeSize
    mt.mu.Unlock()

    // 4. Write data (no lock - I/O parallelism)
    if compressed.IsZero() {
        active.buf.WriteAt(value, wPos)
    } else {
        active.buf.WriteAt(compressed.Bytes(), wPos)
    }

    // ─────────────────────────────────────────────────────────────
    // LAYER 2: CONCURRENCY GUARD (Per-Key Check)
    // ─────────────────────────────────────────────────────────────
    // Serialize index updates for THIS key to prevent check-then-act race.
    shard := key & indexShardMask
    mt.indexLocks[shard].Lock()

    // Check if a newer write already exists for this key
    existing, found := active.index.Load(key)
    if found && existing.SeqID >= seqID {
        // A newer version exists - this write is a "zombie"
        // The data at wPos becomes a "ghost blob" (acceptable overhead)
        mt.indexLocks[shard].Unlock()
        active.completePendingWrite()
        return
    }

    // Create and store the new record with SeqID
    record := makeEntry(seqID, key, wPos, value, compressed.Bytes(),
                        mt.Compression.Codec, mt.Resilience.ChecksumHasher, checksum)
    active.index.Store(key, record)

    mt.indexLocks[shard].Unlock()

    // 5. Complete - existing logic
    active.completePendingWrite()
}

func (mt *MemTable) prepareRotationLocked() func() {
    old := mt.mu.active

    // Capture the highest SeqID in this slab BEFORE rotation
    sealedMaxSeq := mt.currentMaxSeq.Load()

    // 1. Setup Barrier (Block other writers)
    mt.mu.activeReady = make(chan struct{})

    // 2. Seal & Retire Old Slab
    old.buf.Seal(old.wPos)
    old.retired.Store(true)

    // ... existing rotation logic ...

    return func() {
        // ... existing rotation logic ...

        // CRITICAL: Update maxSealedSeq BEFORE installing new slab
        // This ensures the Lifecycle Guard kicks in for stale threads
        mt.maxSealedSeq.Store(sealedMaxSeq)

        // Reset currentMaxSeq for the new slab
        mt.currentMaxSeq.Store(0)

        // ... rest of rotation ...
    }
}

// Updated makeEntry to include SeqID
func makeEntry(
    seqID uint64, key Key, offset int64, original []byte, compressed []byte,
    codec compression.Codex, hasher Hasher, checksum *uint32,
) metadata.BlobRecord {
    // ... existing logic ...

    entry := metadata.BlobRecord{
        Hash:         key,
        Pos:          offset,
        LogicalSize:  logicalSize,
        PhysicalSize: physicalSize,
        Flags:        metadata.InvalidChecksum,
        SeqID:        seqID, // NEW: Include sequence ID
    }

    // ... existing checksum logic ...

    return entry
}
```

### 0.7 Cache.Put Integration

```go
// blobcache.go

func (c *Cache) Put(key, value []byte) error {
    h := c.config.KeyHasher(key)

    // 1. ORDERING: Establish this write's place in history
    // This is atomic and happens FIRST - before any I/O or state change.
    seqID := c.nextSeq()

    // 2. VISIBILITY: Update bloom filter
    c.bloom.Load().Add(h)

    // 3. (Future) DURABILITY: WAL commit would go here
    // if c.wal != nil {
    //     if err := c.wal.Commit(wal.Entry{SeqID: seqID, ...}); err != nil {
    //         return err
    //     }
    // }

    // 4. VISIBILITY: MemTable flow (now with SeqID)
    c.memTable.Put(seqID, h, value)

    return nil
}
```

### 0.8 Read Path Impact (MINIMAL)

**Critical Design Goal**: The read path must NOT be slowed down by sequence IDs.

```go
// The read path is UNCHANGED:
// - Bloom filter check (fast)
// - Librarian (RAM) lookup
// - Index.Get() lookup
// - Storage read

// SeqID is stored but NOT checked during reads.
// Why? Because the write path guarantees "latest wins" via the two guards.
// By the time a record is visible in the index, it's definitively the latest.

func (idx *Index) Get(hash uint64) (Entry, bool) {
    n, ok := idx.blobs.Load(hash)
    if !ok {
        return Entry{}, false
    }
    n.visited.Store(true)
    return n.entry, true // SeqID is in entry but not checked
}
```

### 0.9 Recovery Considerations

During WAL recovery (Phase 1), sequence IDs enable correct replay:

```go
// Future: WAL recovery uses SeqID to detect "ghost writes"
func (c *Cache) replayPut(entry wal.Entry) {
    // Check if index already has a newer version
    existing, found := c.index.Get(entry.KeyHash)
    if found && existing.SeqID >= entry.SeqID {
        // This WAL entry is stale - a newer version was already persisted
        // to segments before the crash. Skip replay.
        return
    }

    // Replay the Put
    c.memTable.Put(entry.SeqID, entry.KeyHash, entry.Value)
}
```

### 0.10 Testing Strategy

#### Unit Tests

1. **Lifecycle Guard Tests** (`memtable_seq_test.go`):
   ```go
   // Test: Writes with SeqID <= maxSealedSeq are dropped
   func TestLifecycleGuard_DropsStaleWrites(t *testing.T)

   // Test: Rotation correctly updates maxSealedSeq
   func TestLifecycleGuard_RotationUpdatesSeq(t *testing.T)

   // Test: Wake-up after rotation re-checks guard
   func TestLifecycleGuard_ReCheckAfterWakeup(t *testing.T)
   ```

2. **Concurrency Guard Tests** (`memtable_seq_test.go`):
   ```go
   // Test: Higher SeqID wins for same key
   func TestConcurrencyGuard_HigherSeqWins(t *testing.T)

   // Test: Sharded locks don't block unrelated keys
   func TestConcurrencyGuard_ShardedLockIndependence(t *testing.T)

   // Test: Check-then-act race is prevented
   func TestConcurrencyGuard_RacePrevention(t *testing.T)
   ```

3. **Serialization Tests** (`metadata/record_test.go`):
   ```go
   // Test: BlobRecord with SeqID roundtrips correctly
   func TestBlobRecord_SeqIDRoundtrip(t *testing.T)

   // Test: Segment footer with SeqID fields validates
   func TestSegmentRecord_SeqIDInFooter(t *testing.T)
   ```

#### Integration Tests

1. **Time Travel Prevention** (`integration_test.go`):
   ```go
   // Simulate slow writer + rotation + fast writer
   func TestIntegration_NoTimeTravelBug(t *testing.T)
   ```

2. **Concurrent Same-Key Writes** (`integration_test.go`):
   ```go
   // Multiple goroutines writing same key with different values
   func TestIntegration_LatestValueWins(t *testing.T)
   ```

#### Benchmarks

```go
// Ensure SeqID doesn't regress write performance
func BenchmarkPut_WithSeqID(b *testing.B)

// Ensure read path is unchanged
func BenchmarkGet_WithSeqID(b *testing.B)

// Measure sharded lock contention
func BenchmarkPut_SameKey_Contention(b *testing.B)
```

### 0.11 Migration Considerations

**Backward Compatibility**:
- Old segment files have 40-byte records (no SeqID)
- New segment files have 48-byte records (with SeqID)

**Migration Strategy**:
```go
// During segment read, detect record size and handle both formats
func DecodeBlobRecord(buf []byte) (BlobRecord, error) {
    // Support both old (40-byte) and new (48-byte) formats
    if len(buf) >= 48 {
        // New format with SeqID
        return BlobRecord{
            // ... all fields including SeqID ...
        }, nil
    } else if len(buf) >= 40 {
        // Old format without SeqID - assign SeqID=0
        // This is safe because:
        // 1. Old records are already persisted (won't race)
        // 2. New writes always have SeqID > 0 (initialized from UnixNano)
        return BlobRecord{
            // ... fields without SeqID ...
            SeqID: 0, // Legacy record
        }, nil
    }
    return BlobRecord{}, fmt.Errorf("buffer too small")
}
```

### 0.12 Implementation Checklist

- [ ] Add `SeqID` field to `metadata.BlobRecord`
- [ ] Update `EncodedBlobRecordSize` constant (40 → 48)
- [ ] Update `AppendBlobRecord` serialization
- [ ] Update `DecodeBlobRecord` deserialization (with backward compat)
- [ ] Add `globalSeq atomic.Uint64` to `Cache` struct
- [ ] Initialize `globalSeq` with `time.Now().UnixNano()` in `Open()`
- [ ] Add `nextSeq()` method to `Cache`
- [ ] Add `indexLocks [1024]sync.Mutex` to `MemTable`
- [ ] Add `maxSealedSeq atomic.Uint64` to `MemTable`
- [ ] Add `currentMaxSeq atomic.Uint64` to `MemTable`
- [ ] Update `MemTable.Put()` signature to include `seqID`
- [ ] Implement Lifecycle Guard in `putActiveCompressed()`
- [ ] Implement Concurrency Guard in `putActiveCompressed()`
- [ ] Update `prepareRotationLocked()` to capture/update `maxSealedSeq`
- [ ] Update `makeEntry()` to include `seqID`
- [ ] Update `Cache.Put()` to call `nextSeq()` and pass to MemTable
- [ ] Add unit tests for both guards
- [ ] Add integration tests for race prevention
- [ ] Add benchmarks to verify no regression
- [ ] Update DESIGN.md with sequence ID documentation

---

## Part 1: Write-Ahead Log (WAL) Implementation

### 1.1 Overview

The WAL transforms blobcache from a cache with size-based eviction into a CAS system with explicit deletion. Key characteristics:

- **Group Commit**: Multiple concurrent writers batch into single disk sync (RocksDB-style)
- **Double Buffering**: Ping-pong buffer swap eliminates allocation during hot path
- **Efficient I/O**: `net.Buffers` (writev/scatter-gather) for batched writes
- **Configurable Durability**: `fdatasync` vs `fsync` options
- **Crash Recovery**: WAL replay reconciles uncommitted operations

### 1.2 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    WRITE PATH (with WAL)                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Put(key, value)                                                │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  WAL Manager (Group Commit)                              │   │
│  │                                                          │   │
│  │  ┌──────────────┐     ┌──────────────┐                  │   │
│  │  │   pending    │ ◄── │   flushing   │  (ping-pong)     │   │
│  │  │   []*req     │     │   []*req     │                  │   │
│  │  └──────────────┘     └──────────────┘                  │   │
│  │         │                                                │   │
│  │         ▼ (Leader election via writerBusy)              │   │
│  │  ┌──────────────────────────────────────────────────┐   │   │
│  │  │  syncToDisk(batch)                                │   │   │
│  │  │   1. Encode entries to net.Buffers                │   │   │
│  │  │   2. writev() - scatter/gather I/O                │   │   │
│  │  │   3. fdatasync/fsync                              │   │   │
│  │  └──────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
│       │                                                         │
│       ▼ (After WAL commit)                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  MemTable.Put() (existing flow)                          │   │
│  │   → Slab packing → Flush workers → Segments              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 WAL Entry Format

```
┌─────────────────────────────────────────────────────────────────┐
│                    WAL ENTRY FORMAT                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Header (24 bytes)                                         │  │
│  │  ├─ EntryType    (1 byte)   : PUT=0x01, DELETE=0x02      │  │
│  │  ├─ Flags        (1 byte)   : Reserved for future use     │  │
│  │  ├─ KeyLen       (2 bytes)  : Length of key              │  │
│  │  ├─ ValueLen     (4 bytes)  : Length of value (0 for DEL)│  │
│  │  ├─ SeqID        (8 bytes)  : Monotonic sequence ID      │  │
│  │  └─ KeyHash      (8 bytes)  : xxhash64 of key            │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Payload                                                   │  │
│  │  ├─ Key          (KeyLen bytes)                          │  │
│  │  └─ Value        (ValueLen bytes, omitted for DELETE)    │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Footer (4 bytes)                                          │  │
│  │  └─ CRC32        (4 bytes)  : CRC32 of header + payload  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Total: 28 + KeyLen + ValueLen bytes per entry                 │
│                                                                 │
│  SeqID enables:                                                 │
│  • WAL rotation: Know which sequences are committed             │
│  • Recovery dedup: Skip replaying already-persisted entries     │
│  • Ordering: Guarantee "latest wins" semantics                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.4 WAL File Layout

```
┌─────────────────────────────────────────────────────────────────┐
│                    WAL FILE LAYOUT                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ File Header (32 bytes)                                    │  │
│  │  ├─ Magic        (8 bytes)  : 0x424C4F4257414C31 "BLOBWAL1"│
│  │  ├─ Version      (4 bytes)  : Format version (1)          │  │
│  │  ├─ CreatedAt    (8 bytes)  : Unix timestamp (nanos)      │  │
│  │  ├─ Flags        (4 bytes)  : Reserved                    │  │
│  │  └─ HeaderCRC    (4 bytes)  : CRC32 of header fields      │  │
│  │  └─ Padding      (4 bytes)  : Align to 32 bytes           │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Entry 1                                                   │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │ Entry 2                                                   │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │ ...                                                       │  │
│  ├──────────────────────────────────────────────────────────┤  │
│  │ Entry N                                                   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Note: No alignment padding between entries (sequential scan)  │
│  WAL files are rotated when reaching configurable size limit   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.5 Implementation Steps

#### Step 1: Core WAL Types (`wal/types.go`)

```go
package wal

// EntryType identifies the operation type
type EntryType byte

const (
    EntryTypePut    EntryType = 0x01
    EntryTypeDelete EntryType = 0x02
)

// Entry represents a single WAL entry
type Entry struct {
    Type    EntryType
    SeqID   uint64   // Monotonic sequence ID (from Phase 0)
    KeyHash uint64   // xxhash64 of key
    Key     []byte
    Value   []byte   // nil for DELETE
}

// request is the internal ticket for group commit
type request struct {
    entry Entry
    done  bool
    err   error
}
```

#### Step 2: WAL Manager (`wal/manager.go`)

```go
package wal

type Manager struct {
    mu   sync.Mutex
    cond *sync.Cond

    // Double buffering (ping-pong)
    pending  []*request
    flushing []*request

    writerBusy bool

    // I/O
    file       *os.File
    buffers    net.Buffers  // Reused for writev
    scratchBuf []byte       // Pre-allocated encoding buffer

    // Config
    syncMode   SyncMode     // FDataSync, FSync, or None
    maxSize    int64        // Rotate when exceeded
    currentPos int64        // Current write position

    // Metrics
    commitLatency histogram
    batchSize     histogram
}

type SyncMode int

const (
    SyncModeNone     SyncMode = iota // No sync (test only)
    SyncModeFDataSync                 // fdatasync (data only)
    SyncModeFSync                     // fsync (data + metadata)
)
```

#### Step 3: Group Commit Implementation

```go
func (w *Manager) Commit(entry Entry) error {
    req := &request{entry: entry}

    w.mu.Lock()
    w.pending = append(w.pending, req)

    for {
        if req.done {
            w.mu.Unlock()
            return req.err
        }

        if w.writerBusy {
            w.cond.Wait()
            continue
        }

        // Become leader
        w.writerBusy = true

        // Ping-pong swap
        toFlush := w.pending
        w.pending = w.flushing[:0] // Reuse capacity
        w.flushing = toFlush

        w.mu.Unlock()

        // I/O without lock
        err := w.syncToDisk(toFlush)

        w.mu.Lock()
        w.writerBusy = false

        // Complete all requests in batch
        for _, r := range toFlush {
            r.err = err
            r.done = true
        }

        w.cond.Broadcast()
    }
}
```

#### Step 4: Efficient Disk Sync (`wal/io.go`)

```go
func (w *Manager) syncToDisk(batch []*request) error {
    if len(batch) == 0 {
        return nil
    }

    // Build net.Buffers for scatter/gather I/O
    w.buffers = w.buffers[:0]

    for _, req := range batch {
        encoded := w.encodeEntry(&req.entry)
        w.buffers = append(w.buffers, encoded)
    }

    // writev - single syscall for all entries
    n, err := w.buffers.WriteTo(w.file)
    if err != nil {
        return fmt.Errorf("writev failed: %w", err)
    }
    w.currentPos += n

    // Sync based on configured mode
    switch w.syncMode {
    case SyncModeFDataSync:
        if err := fdatasync(w.file); err != nil {
            return fmt.Errorf("fdatasync failed: %w", err)
        }
    case SyncModeFSync:
        if err := w.file.Sync(); err != nil {
            return fmt.Errorf("fsync failed: %w", err)
        }
    }

    return nil
}

func (w *Manager) encodeEntry(e *Entry) []byte {
    // Use scratch buffer to avoid allocations
    // Header(24) + Key + Value + CRC(4)
    size := 28 + len(e.Key) + len(e.Value)
    if cap(w.scratchBuf) < size {
        w.scratchBuf = make([]byte, size)
    }
    buf := w.scratchBuf[:size]

    // Header (24 bytes)
    buf[0] = byte(e.Type)
    buf[1] = 0 // flags (reserved)
    binary.LittleEndian.PutUint16(buf[2:4], uint16(len(e.Key)))
    binary.LittleEndian.PutUint32(buf[4:8], uint32(len(e.Value)))
    binary.LittleEndian.PutUint64(buf[8:16], e.SeqID)    // SeqID for ordering
    binary.LittleEndian.PutUint64(buf[16:24], e.KeyHash) // Hash for fast lookup

    // Payload
    copy(buf[24:], e.Key)
    copy(buf[24+len(e.Key):], e.Value)

    // CRC32 footer (covers header + payload)
    payloadEnd := 24 + len(e.Key) + len(e.Value)
    crc := crc32.ChecksumIEEE(buf[:payloadEnd])
    binary.LittleEndian.PutUint32(buf[payloadEnd:], crc)

    return buf
}
```

#### Step 5: Recovery (`wal/recovery.go`)

```go
type RecoveryResult struct {
    Puts    []Entry
    Deletes []Entry
    Corrupt int // Number of corrupt entries skipped
}

func (w *Manager) Recover() (*RecoveryResult, error) {
    result := &RecoveryResult{}

    // Seek past header
    if _, err := w.file.Seek(WalHeaderSize, io.SeekStart); err != nil {
        return nil, err
    }

    reader := bufio.NewReader(w.file)

    for {
        entry, err := w.readEntry(reader)
        if err == io.EOF {
            break
        }
        if err != nil {
            // CRC mismatch or truncated entry
            result.Corrupt++
            // Try to resync to next valid entry
            if !w.resync(reader) {
                break // Can't recover further
            }
            continue
        }

        switch entry.Type {
        case EntryTypePut:
            result.Puts = append(result.Puts, entry)
        case EntryTypeDelete:
            result.Deletes = append(result.Deletes, entry)
        }
    }

    return result, nil
}
```

#### Step 6: Configuration Options (`options.go`)

```go
// WAL Configuration
type WALConfig struct {
    Enabled   bool      // Enable WAL (default: false for cache mode)
    Dir       string    // WAL directory (default: same as data)
    SyncMode  SyncMode  // Sync strategy (default: FDataSync)
    MaxSize   int64     // Max WAL file size before rotation (default: 256MB)
    BufferCap int       // Pre-allocated request buffer capacity (default: 4096)
}

// Options
func WithWAL(cfg WALConfig) Option {
    return func(c *config) {
        c.WAL = cfg
    }
}

func WithWALEnabled() Option {
    return func(c *config) {
        c.WAL.Enabled = true
    }
}

func WithWALDir(dir string) Option {
    return func(c *config) {
        c.WAL.Dir = dir
    }
}

func WithWALSyncMode(mode SyncMode) Option {
    return func(c *config) {
        c.WAL.SyncMode = mode
    }
}
```

#### Step 7: Delete API (`blobcache.go`)

```go
// Delete removes or tombstones a key from the cache.
// Returns true if the key existed, false otherwise.
func (c *Cache) Delete(key []byte) (bool, error) {
    h := c.config.KeyHasher(key)

    // 1. ORDERING: Assign sequence ID FIRST (even for deletes)
    seqID := c.nextSeq()

    // Check if key exists
    _, found := c.index.Get(h)
    if !found {
        return false, nil
    }

    // WAL commit (if enabled)
    if c.wal != nil {
        if err := c.wal.Commit(wal.Entry{
            Type:    wal.EntryTypeDelete,
            SeqID:   seqID,  // Include SeqID for recovery ordering
            KeyHash: h,
            Key:     key,
        }); err != nil {
            return false, fmt.Errorf("WAL commit failed: %w", err)
        }
    }

    // Remove from index (with SeqID check for concurrent deletes)
    entry, deleted := c.index.DeleteIfOlder(h, seqID)
    if !deleted {
        return false, nil
    }

    // Update approximate size
    c.approxSize.Add(-entry.LogicalSize)

    // Mark segment region for hole punching (background)
    c.storage.MarkDeleted(entry.SegmentID, entry.Pos, entry.PhysicalSize)

    return true, nil
}
```

#### Step 8: Integration with Put (`blobcache.go`)

```go
func (c *Cache) Put(key, value []byte) error {
    h := c.config.KeyHasher(key)

    // 1. ORDERING: Establish this write's place in history FIRST
    seqID := c.nextSeq()

    // 2. Update bloom filter
    c.bloom.Load().Add(h)

    // 3. WAL commit (if enabled) - BEFORE visibility
    if c.wal != nil {
        if err := c.wal.Commit(wal.Entry{
            Type:    wal.EntryTypePut,
            SeqID:   seqID,  // Include SeqID for recovery ordering
            KeyHash: h,
            Key:     key,
            Value:   value,
        }); err != nil {
            return fmt.Errorf("WAL commit failed: %w", err)
        }
    }

    // 4. MemTable flow with SeqID (Phase 0 integration)
    c.memTable.Put(seqID, h, value)
    return nil
}
```

#### Step 9: Startup Recovery (`blobcache.go`)

```go
func Open(path string, opts ...Option) (*Cache, error) {
    // ... existing setup ...

    if cfg.WAL.Enabled {
        walPath := cfg.WAL.Dir
        if walPath == "" {
            walPath = filepath.Join(path, "wal")
        }

        walMgr, err := wal.Open(walPath, cfg.WAL)
        if err != nil {
            return nil, fmt.Errorf("open WAL: %w", err)
        }

        // Check for recovery
        if walMgr.NeedsRecovery() {
            result, err := walMgr.Recover()
            if err != nil {
                return nil, fmt.Errorf("WAL recovery failed: %w", err)
            }

            // Replay operations
            for _, entry := range result.Puts {
                c.replayPut(entry)
            }
            for _, entry := range result.Deletes {
                c.replayDelete(entry)
            }

            // Truncate WAL after successful recovery
            if err := walMgr.Truncate(); err != nil {
                return nil, fmt.Errorf("WAL truncate failed: %w", err)
            }
        }

        c.wal = walMgr
    }

    return c, nil
}
```

### 1.6 Testing Strategy

#### Unit Tests
- `wal/manager_test.go`: Group commit correctness
- `wal/encoding_test.go`: Entry encode/decode roundtrip
- `wal/recovery_test.go`: Recovery with various corruption patterns

#### Integration Tests
- Crash simulation with fault injection
- Concurrent Put/Delete consistency
- WAL rotation under load

#### Benchmarks
- Group commit throughput (varying batch sizes)
- WAL vs non-WAL latency comparison
- Recovery time vs WAL size

---

## Part 2: Segment Compaction Implementation

### 2.1 Overview

Segment compaction merges sparse segments (those with significant deleted space) into dense segments. Key characteristics:

- **Zero Read/Write Cache Pollution**: Uses `copy_file_range` on Linux for kernel-level data transfer
- **Configurable Trigger**: Sparseness threshold (default: 20-30% live bytes)
- **Multi-Candidate Selection**: Merge multiple sparse segments simultaneously
- **Atomic Index Updates**: Index updated only after successful merge

### 2.2 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    COMPACTION PIPELINE                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. CANDIDATE SELECTION                                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  For each segment:                                       │   │
│  │    sparseness = 1 - (liveBytes / totalBytes)            │   │
│  │    if sparseness > threshold (e.g., 0.7):               │   │
│  │      candidates.add(segment)                             │   │
│  │                                                          │   │
│  │  Sort by sparseness (most sparse first)                 │   │
│  │  Select top N candidates for merge                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  2. LIVE BLOB ENUMERATION                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  For each candidate segment:                             │   │
│  │    liveBlobs = index.GetLiveBlobsInSegment(segID)       │   │
│  │    Sort by position (for sequential I/O)                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  3. EFFICIENT DATA TRANSFER                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Linux:   copy_file_range(src, dst, len)                │   │
│  │           └─ Kernel-level, zero user-space copies       │   │
│  │                                                          │   │
│  │  Non-Linux: io.CopyN(dst, src, len)                     │   │
│  │           └─ Fallback with sendfile optimization        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  4. INDEX UPDATE (Atomic)                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Begin transaction:                                      │   │
│  │    1. Update all blob positions to new segment          │   │
│  │    2. Mark old segments as deleted                      │   │
│  │  Commit transaction                                      │   │
│  │                                                          │   │
│  │  Remove old segment files                               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 copy_file_range Implementation

#### Linux Implementation (`compaction_linux.go`)

```go
//go:build linux

package blobcache

import (
    "os"
    "golang.org/x/sys/unix"
)

// copyRange copies n bytes from src to dst using copy_file_range.
// This is a kernel-level operation that doesn't pollute user-space caches.
func copyRange(dst, src *os.File, srcOff, dstOff, n int64) (int64, error) {
    var written int64

    for written < n {
        chunk := n - written
        if chunk > maxCopyChunk {
            chunk = maxCopyChunk // 1GB max per syscall
        }

        srcOffset := srcOff + written
        dstOffset := dstOff + written

        copied, err := unix.CopyFileRange(
            int(src.Fd()), &srcOffset,
            int(dst.Fd()), &dstOffset,
            int(chunk), 0,
        )
        if err != nil {
            if err == unix.ENOSYS || err == unix.EXDEV {
                // Fallback to user-space copy
                return copyRangeFallback(dst, src, srcOff+written, dstOff+written, n-written)
            }
            return written, err
        }

        written += int64(copied)
        if copied == 0 {
            break // EOF
        }
    }

    return written, nil
}

const maxCopyChunk = 1 << 30 // 1GB
```

#### Fallback Implementation (`compaction_other.go`)

```go
//go:build !linux

package blobcache

import (
    "io"
    "os"
)

func copyRange(dst, src *os.File, srcOff, dstOff, n int64) (int64, error) {
    return copyRangeFallback(dst, src, srcOff, dstOff, n)
}

func copyRangeFallback(dst, src *os.File, srcOff, dstOff, n int64) (int64, error) {
    // Seek source to correct position
    if _, err := src.Seek(srcOff, io.SeekStart); err != nil {
        return 0, err
    }

    // Seek destination to correct position
    if _, err := dst.Seek(dstOff, io.SeekStart); err != nil {
        return 0, err
    }

    // Use sendfile optimization if available
    reader := io.LimitReader(src, n)
    return io.Copy(dst, reader)
}
```

### 2.4 Compaction Manager

```go
package blobcache

type CompactionConfig struct {
    Enabled           bool    // Enable compaction (default: true for CAS mode)
    SparseTrigger     float64 // Sparseness threshold (default: 0.7 = 30% live)
    MaxCandidates     int     // Max segments to merge at once (default: 4)
    MinLiveBytes      int64   // Don't compact if live bytes below threshold
    CheckInterval     time.Duration // How often to check (default: 10m)
}

type CompactionManager struct {
    config   CompactionConfig
    storage  *Storage
    index    *index.Index

    // Metrics
    compactedSegments atomic.Int64
    compactedBytes    atomic.Int64
    reclaimedBytes    atomic.Int64
}

type compactionCandidate struct {
    segmentID  int64
    liveBytes  int64
    totalBytes int64
    sparseness float64
    liveBlobs  []BlobRecord
}
```

### 2.5 Candidate Selection

```go
func (m *CompactionManager) selectCandidates() []compactionCandidate {
    var candidates []compactionCandidate

    // Iterate all segments
    m.storage.ForEachSegment(func(segID int64, info SegmentInfo) bool {
        // Get live blobs in this segment
        liveBlobs := m.index.GetBlobsInSegment(segID)

        var liveBytes int64
        for _, blob := range liveBlobs {
            liveBytes += blob.PhysicalSize
        }

        // Calculate sparseness
        sparseness := 1.0 - float64(liveBytes)/float64(info.Size)

        if sparseness >= m.config.SparseTrigger && liveBytes >= m.config.MinLiveBytes {
            candidates = append(candidates, compactionCandidate{
                segmentID:  segID,
                liveBytes:  liveBytes,
                totalBytes: info.Size,
                sparseness: sparseness,
                liveBlobs:  liveBlobs,
            })
        }

        return true // continue iteration
    })

    // Sort by sparseness (most sparse first)
    slices.SortFunc(candidates, func(a, b compactionCandidate) int {
        if a.sparseness > b.sparseness {
            return -1
        }
        return 1
    })

    // Limit to MaxCandidates
    if len(candidates) > m.config.MaxCandidates {
        candidates = candidates[:m.config.MaxCandidates]
    }

    return candidates
}
```

### 2.6 Compaction Execution

```go
func (m *CompactionManager) compact(candidates []compactionCandidate) error {
    if len(candidates) == 0 {
        return nil
    }

    // Calculate total live bytes
    var totalLiveBytes int64
    for _, c := range candidates {
        totalLiveBytes += c.liveBytes
    }

    // Create new segment
    newSegID := m.storage.NextSegmentID()
    newSegFile, err := m.storage.CreateSegment(newSegID, totalLiveBytes)
    if err != nil {
        return fmt.Errorf("create compaction segment: %w", err)
    }
    defer func() {
        if err != nil {
            newSegFile.Close()
            m.storage.RemoveSegment(newSegID)
        }
    }()

    // Track new positions for index update
    type blobMapping struct {
        oldSegID int64
        hash     uint64
        newPos   int64
    }
    var mappings []blobMapping

    var currentPos int64

    // Copy live blobs from each candidate
    for _, candidate := range candidates {
        srcFile, err := m.storage.OpenSegment(candidate.segmentID)
        if err != nil {
            return fmt.Errorf("open source segment %d: %w", candidate.segmentID, err)
        }

        // Sort blobs by position for sequential I/O
        slices.SortFunc(candidate.liveBlobs, func(a, b BlobRecord) int {
            return int(a.Pos - b.Pos)
        })

        for _, blob := range candidate.liveBlobs {
            // Copy blob data using copy_file_range (or fallback)
            copied, err := copyRange(newSegFile, srcFile, blob.Pos, currentPos, blob.PhysicalSize)
            if err != nil {
                srcFile.Close()
                return fmt.Errorf("copy blob: %w", err)
            }
            if copied != blob.PhysicalSize {
                srcFile.Close()
                return fmt.Errorf("short copy: got %d, want %d", copied, blob.PhysicalSize)
            }

            mappings = append(mappings, blobMapping{
                oldSegID: candidate.segmentID,
                hash:     blob.Hash,
                newPos:   currentPos,
            })

            currentPos += blob.PhysicalSize
        }

        srcFile.Close()
    }

    // Align to 4KB and write segment footer
    if err := m.storage.FinalizeSegment(newSegFile, newSegID, currentPos); err != nil {
        return fmt.Errorf("finalize segment: %w", err)
    }

    // fsync new segment before updating index
    if err := newSegFile.Sync(); err != nil {
        return fmt.Errorf("sync new segment: %w", err)
    }

    // Atomic index update
    if err := m.index.RelocateBlobs(mappings, newSegID); err != nil {
        return fmt.Errorf("update index: %w", err)
    }

    // Remove old segments (after index update)
    for _, candidate := range candidates {
        if err := m.storage.RemoveSegment(candidate.segmentID); err != nil {
            // Log but don't fail - orphaned segment will be cleaned up later
            log.Printf("warning: failed to remove segment %d: %v", candidate.segmentID, err)
        }
    }

    // Update metrics
    for _, candidate := range candidates {
        m.compactedSegments.Add(1)
        m.reclaimedBytes.Add(candidate.totalBytes - candidate.liveBytes)
    }
    m.compactedBytes.Add(totalLiveBytes)

    return nil
}
```

### 2.7 Index Relocation

```go
// RelocateBlobs atomically updates blob positions after compaction.
// This is a critical operation - if it fails, the compaction is aborted.
func (idx *Index) RelocateBlobs(mappings []blobMapping, newSegID int64) error {
    // Build batch update
    var records []BlobRecord

    for _, m := range mappings {
        // Get current entry
        entry, found := idx.blobs.Load(m.hash)
        if !found {
            continue // Blob was deleted during compaction - skip
        }

        // Create updated record
        newRecord := entry.BlobRecord
        newRecord.Pos = m.newPos
        records = append(records, newRecord)
    }

    // Persist to Bitcask (atomic batch)
    if err := idx.segments.RelocateBatch(newSegID, records); err != nil {
        return err
    }

    // Update in-memory skipmap
    for _, m := range mappings {
        entry, found := idx.blobs.Load(m.hash)
        if !found {
            continue
        }

        // Atomic update
        newEntry := entry
        newEntry.SegmentID = newSegID
        newEntry.BlobRecord.Pos = m.newPos
        idx.blobs.Store(m.hash, newEntry)
    }

    // Mark old segments as deleted in persistence
    oldSegIDs := make(map[int64]struct{})
    for _, m := range mappings {
        oldSegIDs[m.oldSegID] = struct{}{}
    }
    for segID := range oldSegIDs {
        idx.segments.DeleteSegment(segID)
    }

    return nil
}
```

### 2.8 Configuration Options

```go
func WithCompaction(cfg CompactionConfig) Option {
    return func(c *config) {
        c.Compaction = cfg
    }
}

func WithCompactionEnabled() Option {
    return func(c *config) {
        c.Compaction.Enabled = true
    }
}

func WithSparseTrigger(threshold float64) Option {
    return func(c *config) {
        c.Compaction.SparseTrigger = threshold
    }
}

func WithCompactionInterval(d time.Duration) Option {
    return func(c *config) {
        c.Compaction.CheckInterval = d
    }
}
```

### 2.9 Testing Strategy

#### Unit Tests
- `compaction_test.go`: Candidate selection logic
- `copyrange_test.go`: copy_file_range correctness (Linux only)
- `index_relocate_test.go`: Atomic relocation

#### Integration Tests
- Compaction during concurrent reads/writes
- Crash during compaction (recovery)
- Cross-filesystem copy (fallback path)

#### Benchmarks
- copy_file_range vs io.Copy throughput
- Compaction impact on read latency
- Memory usage during compaction

---

## Part 3: DESIGN.md Updates

### 3.1 New Sections to Add

```markdown
## Sequence IDs and Consistency

BlobCache assigns a monotonically increasing sequence ID to every operation
(Put, Delete). This provides total ordering and prevents two critical race
conditions that can cause data corruption or stale reads.

### Why Sequence IDs?

1. **WAL Rotation**: Determines which WAL entries are safely persisted
2. **CAS Semantics**: Guarantees "latest write wins" for the same key
3. **Recovery**: Enables correct WAL replay without duplicating entries
4. **Cache Consistency**: Even without WAL, prevents serving stale values

### The Two Race Conditions

#### "Time Travel Bug" (Cross-File Race)

Without sequence IDs, a slow writer can land in a NEW file after rotation,
hiding a newer write in an OLD (sealed) file:

```
1. Thread A starts Put(key), gets paused by OS
2. Thread B completes Put(key) to ActiveSlab
3. ActiveSlab rotates → Thread B's write is in SealedSlab
4. Thread A wakes up, writes to NEW ActiveSlab
5. Get(key) checks ActiveSlab first → Returns Thread A's OLDER data!
```

**Fix**: The "Lifecycle Guard" - track `maxSealedSeq` and drop any write
with `seqID <= maxSealedSeq`.

#### "Check-Then-Act Bug" (Same-File Race)

Without per-key locking, two threads can both see stale state and overwrite
each other:

```
1. Thread A (SeqID=100) reads Index[key] = 50
2. Thread B (SeqID=101) reads Index[key] = 50
3. Thread B stores Index[key] = 101 (correct)
4. Thread A stores Index[key] = 100 (WRONG - overwrites newer!)
```

**Fix**: The "Concurrency Guard" - sharded per-key locks serialize the
check-and-update operation.

### Performance Impact

- **Write Path**: One atomic increment + sharded lock acquisition
- **Read Path**: Zero impact - SeqID stored but not checked (write guards
  guarantee correctness by the time data is visible)

## Write-Ahead Log (WAL)

BlobCache supports an optional Write-Ahead Log for durability guarantees
beyond the default crash-consistent segment format.

### When to Use WAL

- **CAS Mode**: When using BlobCache as Content Addressable Storage with
  explicit Delete operations, WAL ensures delete markers survive crashes.
- **Strong Durability**: When every Put must be durable before returning.
- **Transactional Semantics**: When you need guaranteed recovery.

### Group Commit

WAL uses a group commit strategy inspired by RocksDB:

1. Multiple concurrent writers add requests to a pending queue
2. One writer "becomes leader" and flushes the entire batch
3. All writers in the batch share the single fsync cost
4. Double-buffering (ping-pong) eliminates allocation during hot path

### Performance Characteristics

| Scenario | Throughput Impact | Latency Impact |
|----------|------------------|----------------|
| High concurrency (100+ writers) | ~5% reduction | Amortized fsync |
| Single writer | ~50% reduction | fsync per write |
| Batch writes | Minimal | Single fsync per batch |

## Segment Compaction

Sparse segments (those with significant deleted space) are periodically
compacted to reclaim disk space and improve read performance.

### Trigger Conditions

- Segment sparseness exceeds threshold (default: 70% deleted)
- Minimum live bytes above threshold (avoids compacting tiny segments)
- Background check interval (default: 10 minutes)

### Zero-Copy Optimization

On Linux, compaction uses `copy_file_range()` for kernel-level data
transfer that bypasses user-space entirely:

- No page cache pollution during compaction
- No memory pressure on application
- Optimal I/O scheduling by kernel

### Atomic Index Updates

Compaction follows a safe protocol:
1. Create new segment with compacted data
2. fsync new segment
3. Atomically update index (all-or-nothing)
4. Remove old segments

Crash at any point leaves system in consistent state.
```

---

## Part 4: Implementation Order

### Phase 0: Sequence IDs (PREREQUISITE - Must Complete First)
**Goal**: Establish foundational ordering infrastructure before any WAL or compaction work.

1. **Data Structure Changes**
   - Add `SeqID uint64` field to `metadata.BlobRecord`
   - Update `EncodedBlobRecordSize` constant (40 → 48 bytes)
   - Update `AppendBlobRecord` and `DecodeBlobRecord` serialization
   - Add backward compatibility for old 40-byte records

2. **Global Sequence Counter**
   - Add `globalSeq atomic.Uint64` to `Cache` struct
   - Initialize with `time.Now().UnixNano()` in `Open()`
   - Add `nextSeq()` method

3. **MemTable Guards**
   - Add `indexLocks [1024]sync.Mutex` for Concurrency Guard
   - Add `maxSealedSeq atomic.Uint64` for Lifecycle Guard
   - Add `currentMaxSeq atomic.Uint64` for tracking
   - Update `Put()` signature to include `seqID`
   - Implement both guards in `putActiveCompressed()`
   - Update `prepareRotationLocked()` for seq tracking

4. **Testing**
   - Unit tests for Lifecycle Guard (time travel prevention)
   - Unit tests for Concurrency Guard (check-then-act prevention)
   - Integration tests for race prevention
   - Benchmarks to verify no performance regression

5. **Documentation**
   - Update DESIGN.md with sequence ID explanation
   - Document the two race conditions and their solutions

### Phase 1: WAL Foundation (Depends on Phase 0)
1. Core WAL types and encoding (with SeqID)
2. Group commit manager
3. Unit tests for encoding/decoding
4. Basic integration with Put (using SeqID from Phase 0)

### Phase 2: WAL Recovery (Depends on Phase 1)
1. Recovery implementation (using SeqID for deduplication)
2. Fault injection tests
3. Delete API implementation (with SeqID)
4. Integration tests

### Phase 3: Compaction Foundation (Depends on Phase 0)
1. copy_file_range implementation
2. Candidate selection
3. Basic compaction flow
4. Unit tests

### Phase 4: Compaction Integration (Depends on Phase 3)
1. Index relocation (preserving SeqIDs)
2. Background compaction worker
3. Crash recovery tests
4. Performance benchmarks

### Phase 5: Polish (After All Phases)
1. DESIGN.md updates (comprehensive)
2. Configuration documentation
3. Metrics and observability
4. Edge case handling

### Dependency Graph

```
            ┌─────────────────┐
            │    Phase 0      │
            │  Sequence IDs   │
            │  (Foundation)   │
            └────────┬────────┘
                     │
         ┌───────────┴───────────┐
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌─────────────────┐
│    Phase 1      │    │    Phase 3      │
│ WAL Foundation  │    │   Compaction    │
│                 │    │   Foundation    │
└────────┬────────┘    └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌─────────────────┐
│    Phase 2      │    │    Phase 4      │
│  WAL Recovery   │    │   Compaction    │
│                 │    │   Integration   │
└────────┬────────┘    └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     │
                     ▼
            ┌─────────────────┐
            │    Phase 5      │
            │     Polish      │
            └─────────────────┘
```

---

## Part 5: Risk Mitigation

### Phase 0: Sequence ID Risks

| Risk | Mitigation |
|------|------------|
| Clock skew after restart | Initialize with UnixNano - new sequences always higher after any realistic restart delay |
| Sharded lock contention | 1024 shards provides near-zero contention for uniformly distributed hashes |
| Record size increase (40→48 bytes) | 20% increase is acceptable; benefit of correctness outweighs storage cost |
| Backward compatibility | DecodeBlobRecord handles both 40 and 48 byte formats; old records get SeqID=0 |
| Ghost blobs (rejected writes) | Acceptable overhead; compaction will reclaim space eventually |
| Performance regression | Atomic increment is ~10ns; sharded lock is ~20ns uncontended; total <100ns |

### WAL Risks

| Risk | Mitigation |
|------|------------|
| WAL corruption | CRC32 per entry + resync algorithm |
| WAL growth unbounded | Rotation + truncation after checkpoint |
| Performance regression | Configurable sync mode (including none for tests) |
| Recovery takes too long | Periodic WAL truncation |

### Compaction Risks

| Risk | Mitigation |
|------|------------|
| Crash during compaction | New segment finalized before index update |
| Read during compaction | Old segment remains valid until index update |
| copy_file_range unsupported | Automatic fallback to io.Copy |
| Compaction storms | Rate limiting + minimum interval |
| Memory pressure | Zero-copy design, no buffering |

---

## Part 6: Success Metrics

### Phase 0: Sequence ID Success Criteria
- [ ] No "Time Travel Bug": Slow writer after rotation never overwrites newer data
- [ ] No "Check-Then-Act Bug": Concurrent same-key writes always yield latest value
- [ ] Read path latency unchanged (no SeqID checks on read)
- [ ] Write path overhead < 100ns per operation (atomic increment + sharded lock)
- [ ] Backward compatibility: Old 40-byte records load correctly with SeqID=0
- [ ] All existing tests pass with new SeqID infrastructure

### WAL Success Criteria
- [ ] Group commit reduces fsync calls by 10x under high concurrency
- [ ] Recovery time < 1 second for 256MB WAL
- [ ] No data loss in crash tests (1000+ iterations)
- [ ] Delete operations are crash-consistent
- [ ] WAL rotation correctly based on committed SeqIDs

### Compaction Success Criteria
- [ ] copy_file_range achieves 2GB/s+ on NVMe
- [ ] No read latency impact during compaction
- [ ] Disk space reclaimed within 2x compaction intervals
- [ ] No memory growth during compaction
- [ ] SeqIDs preserved correctly during blob relocation
