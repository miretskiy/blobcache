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

### 0.4 Implementation Receipts (COMPLETE)

The following table maps PLAN concepts to actual implementation locations:

| Concept | File:Line | Notes |
|---------|-----------|-------|
| **Data Structures** | | |
| FooterEntry with SeqID | `internal/record/segment.go:178-185` | 48-byte record |
| FooterEntrySize = 48 | `internal/record/segment.go:156` | |
| Header.SeqID | `internal/record/record.go:79` | |
| **Global Sequence Counter** | | |
| globalSeq atomic.Uint64 | `blobcache.go:49` | |
| nextSeq() method | `blobcache.go:101-106` | With testing hook |
| UnixNano initialization | `blobcache.go:152-157` | |
| **MemTable Guards** | | |
| numIndexShards = 256 | `memtable.go:19` | PLAN said 1024, refined to 256 |
| indexLocks | `memtable.go:57` | Sharded mutex array |
| maxSealedSeq | `memtable.go:47-50` | Inside mu struct |
| Lifecycle Guard check | `memtable.go:215-221` | Returns errSequenceTooOld |
| Concurrency Guard | `memtable.go:267-275` | Per-key serialization |
| Rotation handoff | `memtable.go:294-298` | Updates maxSealedSeq |
| currentMaxSeq | `memtable.go:252-254` | Per-slab tracking |
| **Cache Integration** | | |
| putWithRetry | `blobcache.go:354-383` | Zombie resurrection loop |
| **Tests** | | |
| TestMemTable_LifecycleGuard | `sequence_test.go:13` | |
| TestMemTable_ConcurrencyGuard | `sequence_test.go:46` | |
| TestMemTable_ConcurrentWritesSameKey | `sequence_test.go:82` | |
| TestMemTable_RotationUpdatesMaxSealedSeq | `sequence_test.go:130` | |
| TestCache_RetryLoop_ZombieResurrection | `sequence_test.go:182` | |
| TestCache_RetryLoop_IdempotentSuccess | `sequence_test.go:236` | |
| **Documentation** | | |
| DESIGN.md section 4.6 | Lines 171-192 | Race conditions + guards |

#### Implementation Refinements

The actual implementation refined several PLAN details:

1. **256 shards vs 1024**: Sufficient for 32 concurrent writers (~2% collision)
2. **maxSealedSeq inside mu**: Protected by mutex, atomic not needed
3. **currentMaxSeq per-slab**: Cleaner tracking during rotation
4. **errSequenceTooOld**: Returns error instead of silent drop (enables retry)
5. **putWithRetry loop**: Zombie resurrection protocol not in original PLAN

### 0.5 Implementation Status: COMPLETE

Phase 0 is fully implemented. The sequence ID infrastructure provides correctness guarantees that benefit blobcache even in pure cache mode, without WAL enabled.

#### Why Sequence IDs Matter for a Cache

It may seem counterintuitive to add ordering infrastructure to a cache—after all, caches are ephemeral. However, blobcache's architecture creates subtle race conditions that can violate user expectations even in cache mode.

Consider a user who calls `Put("config", v2)` to update a configuration blob. They expect subsequent `Get("config")` calls to return `v2`. Without sequence IDs, a slow writer from a previous `Put("config", v1)` could wake up after rotation, write to the new active slab, and cause `Get` to return `v1`—the user's update appears to have been silently dropped.

The sequence ID infrastructure prevents this by establishing a total order on operations. The overhead is approximately 50ns per write (one atomic increment plus a sharded lock acquisition) but guarantees that users never observe stale data due to write reordering.

The same infrastructure enables future WAL support without retrofitting—sequence IDs in the WAL allow recovery to correctly skip entries that were already persisted to segments before a crash.

---

## Part 1: Write-Ahead Log (WAL) Implementation

> **STATUS: NOT IMPLEMENTED** - This section contains the design for future WAL implementation.

<!-- REMOVED: Detailed pseudocode sections 0.4-0.11 -->
<!-- These were implementation guides that are now complete. See receipts table above. -->

<!--
Original sections removed (now implemented):
- 0.4 Data Structure Changes (BlobRecord with SeqID)
- 0.5 Global Sequence Counter
- 0.6 MemTable Integration
- 0.7 Cache.Put Integration
- 0.8 Read Path Impact
- 0.9 Recovery Considerations
- 0.10 Testing Strategy
- 0.11 Migration Considerations
- 0.12 Implementation Status (checklist)
-->

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

### 1.3 WAL Entry Format (Unified with Segments)

WAL entries use the **same `record.Record` format** as segment files. This enables:
- Shared encoding/decoding code
- Consistent tooling for debugging and recovery
- Simpler testing (one format to verify)

```
┌─────────────────────────────────────────────────────────────────┐
│                    WAL ENTRY FORMAT                             │
│           (Same as record.Record - 35-byte header)              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Header (35 bytes) - record.Header                         │  │
│  │  ├─ Magic        (1 byte)   : 0xBB=valid, 0x00=hole      │  │
│  │  ├─ Flags        (8 bytes)  : Compression, CRC, Deleted  │  │
│  │  │     └─ Bits 31-0: CRC32 of key+value                  │  │
│  │  │     └─ Bit 33:    FlagDeleted (tombstone)             │  │
│  │  │     └─ Bits 63-60: Compression codec                  │  │
│  │  ├─ SeqID        (8 bytes)  : Monotonic sequence ID      │  │
│  │  ├─ KeyLen       (2 bytes)  : Length of key              │  │
│  │  ├─ PhysicalSize (8 bytes)  : Value size on disk         │  │
│  │  └─ LogicalSize  (8 bytes)  : Uncompressed value size    │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Payload                                                   │  │
│  │  ├─ Key          (KeyLen bytes) : Original key bytes     │  │
│  │  └─ Value        (PhysicalSize bytes, empty for DELETE)  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Total: 35 + KeyLen + PhysicalSize bytes per entry             │
│                                                                 │
│  DELETE entries:                                                │
│  • FlagDeleted set in Flags                                    │
│  • PhysicalSize = 0 (no value payload)                         │
│  • LogicalSize = 0                                             │
│                                                                 │
│  SeqID enables:                                                 │
│  • WAL recovery checkpoint: max(segment.MaxSeqID)              │
│  • Recovery rule: only replay if seqID > checkpoint            │
│  • Ordering: Guarantee "latest wins" semantics                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Key Insight**: Using the unified format means WAL entries can be read with `record.DecodeRecord()`
and written with `record.AppendRecord()`. No separate WAL-specific serialization needed.

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

#### Step 1: Core WAL Types (`wal/wal.go`)

Since we use the unified record format, WAL needs minimal type definitions:

```go
package wal

import (
    "os"
    "sync"
    "sync/atomic"

    "github.com/miretskiy/blobcache/internal/record"
)

// Config configures WAL behavior.
type Config struct {
    Dir           string        // Directory for WAL files
    MaxFileSize   int64         // Max size per WAL file before rotation (default: 64MB)
    SyncMode      SyncMode      // fdatasync, fsync, or none
    GroupCommitNS int64         // Max nanoseconds to wait for group commit (default: 1ms)
}

type SyncMode int

const (
    SyncNone     SyncMode = iota // No sync (testing only)
    SyncData                     // fdatasync (metadata not synced)
    SyncFull                     // fsync (full durability)
)

// request is the internal ticket for group commit.
// Each Put/Delete caller creates one and waits on done channel.
type request struct {
    rec  record.Record // Unified record format
    done chan error    // Signaled when batch is synced
}

// WAL implements write-ahead logging with group commit.
type WAL struct {
    cfg Config

    mu         sync.Mutex
    file       *os.File
    fileSize   int64
    fileSeqNum uint64 // WAL file sequence number for rotation

    // Double-buffered pending queues (ping-pong)
    pending  []*request
    flushing []*request

    // Leader election: only one goroutine writes at a time
    writerBusy atomic.Bool

    // Metrics
    writtenBytes  atomic.Int64
    writtenBlobs  atomic.Int64
    syncCount     atomic.Int64
    groupCommits  atomic.Int64
}
```

#### Step 2: Group Commit Manager (`wal/group_commit.go`)

```go
// Write adds a record to the WAL and blocks until the batch is synced.
// Multiple concurrent callers will batch together for a single fsync.
func (w *WAL) Write(rec record.Record) error {
    // Create request ticket
    req := &request{
        rec:  rec,
        done: make(chan error, 1),
    }

    // Add to pending queue (under lock)
    w.mu.Lock()
    w.pending = append(w.pending, req)
    pendingCount := len(w.pending)
    w.mu.Unlock()

    // Try to become the leader (only one goroutine writes)
    if w.writerBusy.CompareAndSwap(false, true) {
        // We are the leader - flush the batch
        w.flushBatch()
        w.writerBusy.Store(false)
    }

    // Wait for our batch to complete (leader will signal us)
    return <-req.done
}

// flushBatch writes all pending requests to disk with a single sync.
// Called by the leader goroutine only.
func (w *WAL) flushBatch() {
    // Swap pending and flushing queues (under lock)
    w.mu.Lock()
    if len(w.pending) == 0 {
        w.mu.Unlock()
        return
    }
    w.pending, w.flushing = w.flushing[:0], w.pending
    w.mu.Unlock()

    // Check if we need to rotate
    if err := w.maybeRotate(); err != nil {
        w.signalBatch(err)
        return
    }

    // Build scatter-gather buffers (net.Buffers)
    var totalSize int64
    buffers := make(net.Buffers, 0, len(w.flushing))
    for _, req := range w.flushing {
        encoded := record.AppendRecord(nil, req.rec)
        buffers = append(buffers, encoded)
        totalSize += int64(len(encoded))
    }

    // Write all buffers with writev (single syscall)
    n, err := buffers.WriteTo(w.file)
    if err != nil {
        w.signalBatch(err)
        return
    }
    w.fileSize += n

    // Sync to disk
    if err := w.sync(); err != nil {
        w.signalBatch(err)
        return
    }

    // Update metrics
    w.writtenBytes.Add(totalSize)
    w.writtenBlobs.Add(int64(len(w.flushing)))
    w.syncCount.Add(1)
    w.groupCommits.Add(1)

    // Signal all waiters in the batch
    w.signalBatch(nil)
}

func (w *WAL) signalBatch(err error) {
    for _, req := range w.flushing {
        req.done <- err
    }
}

func (w *WAL) sync() error {
    switch w.cfg.SyncMode {
    case SyncData:
        return sys.Fdatasync(w.file)
    case SyncFull:
        return w.file.Sync()
    default:
        return nil
    }
}
```

#### Step 3: WAL Rotation (`wal/rotation.go`)

```go
// maybeRotate creates a new WAL file if current exceeds MaxFileSize.
func (w *WAL) maybeRotate() error {
    if w.file != nil && w.fileSize < w.cfg.MaxFileSize {
        return nil
    }

    // Close current file (if any)
    if w.file != nil {
        if err := w.sync(); err != nil {
            return fmt.Errorf("sync before rotate: %w", err)
        }
        if err := w.file.Close(); err != nil {
            return fmt.Errorf("close WAL: %w", err)
        }
    }

    // Create new WAL file
    w.fileSeqNum++
    path := filepath.Join(w.cfg.Dir, fmt.Sprintf("wal-%08d.log", w.fileSeqNum))

    f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil {
        return fmt.Errorf("create WAL: %w", err)
    }

    // Write file header
    hdr := walFileHeader{
        Magic:     WALMagic,
        Version:   1,
        CreatedAt: time.Now().UnixNano(),
    }
    if _, err := hdr.WriteTo(f); err != nil {
        f.Close()
        return fmt.Errorf("write WAL header: %w", err)
    }

    w.file = f
    w.fileSize = WALHeaderSize
    return nil
}
```

#### Step 4: Recovery Checkpoint Logic (`wal/recovery.go`)

```go
// RecoveryCheckpoint computes the recovery checkpoint from segment metadata.
// Only WAL entries with SeqID > checkpoint need to be replayed.
func ComputeRecoveryCheckpoint(segments []record.SegmentEnvelope) uint64 {
    var maxSeqID uint64
    for _, seg := range segments {
        if seg.MaxSeqID > maxSeqID {
            maxSeqID = seg.MaxSeqID
        }
    }
    return maxSeqID
}

// Recover replays WAL entries that were not persisted to segments.
// Returns the number of entries replayed.
func (w *WAL) Recover(checkpoint uint64, applyFn func(record.Record) error) (int, error) {
    // Find all WAL files
    files, err := w.listWALFiles()
    if err != nil {
        return 0, err
    }

    var replayed int
    for _, path := range files {
        n, err := w.recoverFile(path, checkpoint, applyFn)
        if err != nil {
            return replayed, fmt.Errorf("recover %s: %w", path, err)
        }
        replayed += n
    }

    return replayed, nil
}

// recoverFile replays a single WAL file.
func (w *WAL) recoverFile(path string, checkpoint uint64, applyFn func(record.Record) error) (int, error) {
    f, err := os.Open(path)
    if err != nil {
        return 0, err
    }
    defer f.Close()

    // Validate header
    hdr, err := readWALHeader(f)
    if err != nil {
        return 0, fmt.Errorf("invalid WAL header: %w", err)
    }

    var replayed int
    reader := bufio.NewReader(f)

    for {
        // Read record header first
        headerBuf := make([]byte, record.HeaderSize)
        if _, err := io.ReadFull(reader, headerBuf); err != nil {
            if err == io.EOF {
                break // Clean EOF
            }
            return replayed, fmt.Errorf("read header: %w", err)
        }

        hdr, err := record.DecodeHeader(headerBuf)
        if err != nil {
            // Corruption or incomplete write - stop recovery here
            log.Warn("WAL recovery stopped at corruption", "path", path, "err", err)
            break
        }

        // Read payload
        payloadBuf := make([]byte, hdr.PayloadSize())
        if _, err := io.ReadFull(reader, payloadBuf); err != nil {
            log.Warn("WAL recovery stopped at incomplete record", "path", path)
            break
        }

        // Decode full record
        fullBuf := append(headerBuf, payloadBuf...)
        rec, err := record.DecodeRecord(fullBuf, true) // Verify CRC
        if err != nil {
            log.Warn("WAL recovery CRC mismatch", "path", path, "err", err)
            break
        }

        // Skip if already persisted to segments
        if rec.SeqID <= checkpoint {
            continue
        }

        // Apply the record (Put or Delete based on FlagDeleted)
        if err := applyFn(rec); err != nil {
            return replayed, fmt.Errorf("apply record seqID=%d: %w", rec.SeqID, err)
        }
        replayed++
    }

    return replayed, nil
}
```

#### Step 5: Integration with Cache (`blobcache.go` changes)

```go
// Put writes a key-value pair, optionally through WAL.
func (c *Cache) Put(key, value []byte) error {
    seqID := c.nextSeq()
    hash := hashKey(key)

    // If WAL enabled, write to WAL first (durable)
    if c.wal != nil {
        rec := record.NewRecord(seqID, key, value, int64(len(value)))
        if err := c.wal.Write(rec); err != nil {
            return fmt.Errorf("WAL write: %w", err)
        }
    }

    // Then write to MemTable (in-memory, eventually flushed to segments)
    return c.putWithRetry(seqID, hash, key, value)
}

// Delete marks a key as deleted (tombstone).
// Only available when WAL is enabled (CAS mode).
func (c *Cache) Delete(key []byte) error {
    if c.wal == nil {
        return ErrDeleteRequiresWAL
    }

    seqID := c.nextSeq()
    hash := hashKey(key)

    // Create tombstone record
    rec := record.Record{
        Header: record.Header{
            Magic:        record.RecordMagic,
            Flags:        record.FlagDeleted, // Tombstone marker
            SeqID:        seqID,
            KeyLen:       uint16(len(key)),
            PhysicalSize: 0, // No value for deletes
            LogicalSize:  0,
        },
        Key:   key,
        Value: nil,
    }
    rec.SetCRC(record.ComputeCRC(key, nil))

    // Write tombstone to WAL
    if err := c.wal.Write(rec); err != nil {
        return fmt.Errorf("WAL write tombstone: %w", err)
    }

    // Update index to mark as deleted
    return c.index.Delete(hash, seqID)
}

// Open with WAL recovery
func Open(dir string, opts ...Option) (*Cache, error) {
    // ... existing open logic ...

    // If WAL enabled, recover
    if c.wal != nil {
        // Compute checkpoint from segment envelopes
        envelopes, err := c.loadSegmentEnvelopes()
        if err != nil {
            return nil, fmt.Errorf("load segment envelopes: %w", err)
        }
        checkpoint := wal.ComputeRecoveryCheckpoint(envelopes)

        // Replay WAL entries above checkpoint
        replayed, err := c.wal.Recover(checkpoint, func(rec record.Record) error {
            if rec.IsDeleted() {
                return c.index.Delete(hashKey(rec.Key), rec.SeqID)
            }
            return c.putWithRetry(rec.SeqID, hashKey(rec.Key), rec.Key, rec.Value)
        })
        if err != nil {
            return nil, fmt.Errorf("WAL recovery: %w", err)
        }
        log.Info("WAL recovery complete", "replayed", replayed, "checkpoint", checkpoint)
    }

    return c, nil
}
```

#### Step 6: WAL Truncation (`wal/truncate.go`)

```go
// Truncate removes WAL files whose entries are fully persisted to segments.
// Called after segment flush completes.
func (w *WAL) Truncate(checkpoint uint64) error {
    files, err := w.listWALFiles()
    if err != nil {
        return err
    }

    for _, path := range files {
        // Check if all entries in this file are below checkpoint
        maxSeq, err := w.scanMaxSeqID(path)
        if err != nil {
            log.Warn("cannot scan WAL file", "path", path, "err", err)
            continue
        }

        // Safe to delete if all entries are persisted
        if maxSeq <= checkpoint {
            if err := os.Remove(path); err != nil {
                log.Warn("cannot remove WAL file", "path", path, "err", err)
            } else {
                log.Info("truncated WAL file", "path", path, "maxSeq", maxSeq)
            }
        }
    }

    return nil
}

// scanMaxSeqID finds the highest SeqID in a WAL file.
func (w *WAL) scanMaxSeqID(path string) (uint64, error) {
    f, err := os.Open(path)
    if err != nil {
        return 0, err
    }
    defer f.Close()

    // Skip header
    if _, err := f.Seek(WALHeaderSize, io.SeekStart); err != nil {
        return 0, err
    }

    var maxSeq uint64
    headerBuf := make([]byte, record.HeaderSize)

    for {
        if _, err := io.ReadFull(f, headerBuf); err != nil {
            break
        }
        hdr, err := record.DecodeHeader(headerBuf)
        if err != nil {
            break
        }
        if hdr.SeqID > maxSeq {
            maxSeq = hdr.SeqID
        }
        // Skip payload
        if _, err := f.Seek(int64(hdr.PayloadSize()), io.SeekCurrent); err != nil {
            break
        }
    }

    return maxSeq, nil
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

> **STATUS: NOT IMPLEMENTED** - This section contains the design for future compaction implementation.
> Current code has only a placeholder: `maybeCompactSegments()` returns nil.

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

> **STATUS: PARTIALLY COMPLETE**
> - ✅ Sequence IDs: Added to DESIGN.md section 4.6 (lines 171-192)
> - ⏳ WAL: Not added yet (implementation not started)
> - ⏳ Compaction: Placeholder in section 10.2 (implementation not started)

### 3.1 Sequence IDs Section

> **✅ COMPLETE** - Already in DESIGN.md section 4.6

The following content is now in DESIGN.md (not needed in PLAN anymore):

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

### Phase 0: Sequence ID Success Criteria ✓ COMPLETE
- [x] No "Time Travel Bug": Slow writer after rotation never overwrites newer data
- [x] No "Check-Then-Act Bug": Concurrent same-key writes always yield latest value
- [x] Read path latency unchanged (no SeqID checks on read)
- [x] Write path overhead < 100ns per operation (atomic increment + sharded lock)
- [x] Backward compatibility: Old 40-byte records load correctly with SeqID=0
- [x] All existing tests pass with new SeqID infrastructure
- [x] Bonus: Fixed latent wPos synchronization bug in MmapBuffer

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
