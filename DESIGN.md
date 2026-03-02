# BlobCache Design Document

## 1. Executive Summary
BlobCache is a specialized storage engine optimized for high-throughput, append-heavy blob workloads (100KB–10MB). By bypassing the kernel's page cache and implementing a custom user-space memory hierarchy, BlobCache provides predictable performance and maximizes NVMe bandwidth while maintaining a minimal CPU and GC footprint.

> **Verified Performance (1M ops, m7gd.8xlarge):** Sustained 1.1 GB/s logical throughput with WAL enabled, 5.76GB stable RSS, GET p50: 7.8µs, PUT p50: 28ms. Write amplification ratio of 1.00 (zero double-writes) achieved via WAL-rename strategy.

### 1.1 Project Ferrum: Unified Log-Structured Architecture

Ferrum eliminates the distinction between a "Log" and a "Segment." Data is serialized to disk exactly once using a single, authoritative binary format.

- **Immutable Segments:** Every persisted file is treated as an immutable block of records.
- **One-Way Flow:** Data flows MemTable → Disk.
- **Unified Writer:** The `wal` package is the sole authority for writing data.

---

## 2. High-Level Architecture: The Tiered Storage Pipeline

BlobCache is engineered as a tiered storage pipeline that moves data from a highly concurrent, volatile "Hot" zone (RAM) to a stable, append-only "Cold" zone (NVMe).



```text
+------------+      +------------+      +-------------+      +------------+
|   PUT(K,V) | ---> |  MemTable  | ---> | Slab (RAM)  | ---> | Flush Pool |
+------------+      +------------+      +-------------+      +------------+
|                    |                   |
v                    v                   v
Lock-free CAS        Frozen Slab        Direct I/O Write
Reservation          Handover           to NVMe Segment
```

### 2.1 The Concurrent Ingestion Zone (MemTable)
When a caller executes a `Put()`, the data enters the **MemTable**. This component is the primary orchestrator of the system’s memory. It manages hardware-aligned "Slabs" (`MmapBuffer`) where blobs are packed together using a lock-free reservation system. This ensures that the `Put` operation is effectively a high-speed memory-copy, releasing the caller instantly while the system assumes responsibility for eventual durability.

### 2.2 The Retrieval Accelerator (Unified Bloom Filter)
In workloads with high miss rates (e.g., >50%), traditional caches suffer from cascading lookup penalties. BlobCache utilizes a **Unified Bloom Filter** to provide a "Fast Reject" path.
* **Scale:** Optimized for 1M to 8M entries, protecting up to 1TB of blobs with just a few megabytes of RAM.
* **Proactive Rebuilds:** Because Bloom filters cannot handle deletions natively, BlobCache monitors "Staleness." When deletions (ghosts) cross a 10% threshold or the Observed False Positive Rate (FPR) spikes, the system triggers a background rebuild of the filter from the live index.

### 2.3 The Background Persistence Pipeline
Once a slab is full, it is "frozen" and handed off to background **Flush Workers**.
* **Segments:** MemTable-sized append-only files (~64MB, matching `WriteBufferSize`) that amortize the overhead of filesystem syscalls by packing blobs into sequential write streams.
* **Durable Index:** A sharded arena-backed hash table for O(1) retrieval, with per-segment `.meta` files for crash recovery and durable metadata tracking.
* **Key Index (Pebble):** A rebuildable Pebble DB mapping user keys to 128-bit hashes and vice versa, enabling ordered iteration without storing user key bytes in RAM. Updated during flush, eviction, drain, and compaction. See Section 4.8.

### 2.4 Intelligent Reclamation: SIEVE Eviction & Pressure-Driven Drain
Unlike simple FIFO caches that must delete entire files to reclaim space, BlobCache uses **SIEVE eviction** coupled with **pressure-driven Segment Drain** for zero-write-amplification space reclamation.

* **SIEVE Eviction:** When the cache hits `MaxSize`, the **SIEVE algorithm** scans across all 256 shards to identify cold items and evict them in batches (up to 64 items per shard lock hold). Victims may span multiple segments. Evicted items are also removed from the Pebble KeyIndex via reverse hash lookup (Section 4.8).
* **Durable Commitment:** Victims are marked as tombstones in the segment's `.meta` file and removed from the RAM index.
* **Pressure-Driven Drain:** When total on-disk footprint exceeds `MaxSize`, the sparsest segments (least live data) are deleted until disk usage fits within budget. Zero write amplification — no data is rewritten, only deleted.

---

## 3. System Orchestration: The Hierarchy of Rejection

With the high-level flow established, the following sections detail the low-level mechanics that enable BlobCache to saturate NVMe bandwidth. The architecture is built on a hierarchy of "increasingly expensive" checks. Each layer is designed to protect the one below it from unnecessary work:

1.  **The Unified Bloom Filter** protects the CPU and Memory Bus from searching for keys that aren't there. Rejections occur in $\approx 1ns$.
2.  **The Durable Index (Sharded Arena)** protects the Physical Segments by providing exact coordinates for retrieval, ensuring we only hit the disk when a result is guaranteed.
3.  **The MemTable** protects the Disk from random write pressure by aggregating data into aligned RAM slabs. It also functions as a "Controlled Page Cache" for fast retrieval of recently written data.

```text
USER REQUEST (GET)
|
v
+-----------------------+
|  Unified Bloom Filter |  <-- [CHEAPEST] Reject 99% of misses in ~1ns
+-----------------------+
| (Hit)
v
+-----------------------+
|  Index (Sharded Arena)|  <-- [FAST] O(1) RAM lookup for coordinates
+-----------------------+
| (Found)
v
+-----------------------+
| MemTable / Slab Pool  |  <-- [FAST] Return from RAM if not yet flushed
+-----------------------+
| (Miss)
v
+-----------------------+
|   NVMe Segment File   |  <-- [EXPENSIVE] Single pread() from Disk
+-----------------------+
```

---

## 4. The Index: RAM, Persistence, and Sieve Coordination

The Index is the "control plane" of BlobCache. It coordinates between sub-nanosecond RAM lookups and per-segment `.meta` file persistence. It is designed to be highly concurrent, crash-consistent, and memory-efficient.

### 4.1 High-Speed Lookup (Sharded Arena Index)

At the core of the Index is a **256-Shard Arena-Backed Hash Table** (`internal/index.BlobIndex`).

**Why Sharded Arenas?**

* **GC Optimization**: Go's garbage collector must scan all heap pointers. A skipmap with millions of `*node` pointers creates substantial GC pressure (scan time grows linearly with pointer count). The arena design eliminates heap pointers entirely—nodes use `uint32` indices into pre-allocated slices, making the data structure "scan-free" from the GC's perspective.

* **Cache Efficiency**: Arena-backed nodes are contiguous in memory, improving CPU cache hit rates during iteration (eviction scans). Pointer-chasing in a skipmap causes cache misses on every hop.

* **Bounded Lock Hold Times**: Each shard has its own `RWMutex`. With 256 shards and XXHash3's uniform distribution, contention is near-zero even under high concurrency. The eviction algorithm limits work per lock hold to 64 items (~13µs), preventing priority inversion.

**The Architecture**:

```go
type BlobIndex struct {
    shards [256]shard  // Independent locks + arenas
}

type shard struct {
    mu       sync.RWMutex
    items    map[Key]uint32  // Key -> Arena Index (O(1) lookup)
    nodes    []node          // Arena: contiguous, no pointers
    freeHead uint32          // Free list for node reuse
    hand     uint32          // SIEVE cursor position
    head     uint32          // Circular list head (newest)
}
```

**Key Operations**:
- `Get()`: RLock + map lookup + atomic visited bit (~20ns uncontended)
- `Put()`: Lock + map insert + arena alloc (~50ns uncontended)
- `EvictBatch()`: Hybrid strategy (Random Greedy for small targets, Proportional Fair for large)



### 4.2 Durable Metadata (Per-Segment `.meta` Files)

**Why Self-Describing Segments?**

BlobCache learned from an earlier global Bitcask index that a single centralized metadata store creates a divergence risk: if the central index disagrees with what's on disk, the entire cache is corrupt. Self-describing segments (`.meta` files) make each segment independently recoverable. The primary index (sharded arena) is treated as a **rebuildable acceleration layer** — if it's lost or corrupt, it can be reconstructed by scanning `.meta` files. This same design principle applies to the Pebble KeyIndex (Section 4.8): any derived data structure that can be rebuilt from segments is explicitly non-authoritative.

While blobs are stored in Segment files, their metadata is persisted in **per-segment `.meta` files** stored alongside each `.seg` file:

* **Segment Footer Snapshots:** Each `.meta` file contains a segment footer block (array of `FooterEntry` records) capturing the initial state of all items in the segment at flush time.
* **Incremental Tombstone Logs:** Deletions and evictions append tombstone batches to the `.meta` file without rewriting the footer. Over time: `[footer block] [tombstone batch 1] [batch 2] ...`.
* **Tombstone Compaction:** When tombstone appendages accumulate past a threshold, the maintenance worker collapses them back into the footer entries (metadata-only rewrite, no blob data touched). See Section 10.6.
* **In-Memory Manifest Cache:** At startup, footer entries are loaded into RAM (`SegmentMetadata.Entries`) for zero disk I/O during segment drain and metadata operations.

### 4.3 The Sieve Eviction Policy
BlobCache implements the **SIEVE/Clock Algorithm** (a modern "Cache-Conscious" alternative to LRU) to manage the RAM footprint.

**The Arena-Backed Sieve Structure**

Each shard maintains a circular doubly-linked list of nodes using arena indices (not pointers). The `hand` cursor walks the list looking for "cold" victims.

```go
type node struct {
    item    Item             // Blob coordinates including Key (32 bytes)
    next    uint32           // Arena index (not pointer!)
    prev    uint32           // Arena index (not pointer!)
    visited atomic.Uint32    // 0=cold (evictable), 1=hot (skip)
}
```

**Hybrid Eviction Strategy**:
- **Small targets (<64KB)**: Random Greedy—pick a random start shard, evict until target met
- **Large targets (≥64KB)**: Proportional Fair—each shard pays its fair share of the quota

**The Eviction Algorithm (The "Hand")**

```text
STEP 1: SCANNING
(Hand moves right, clearing "V" (Visited) flags)

      TAIL                                         HEAD
      [n1|V] <-> [n2|V] <-> [n3|_] <-> [n4|V] <-> [n5|V]
         ^
         |
       HAND (Is n1 Visited? Yes. Set V=False, move right)


STEP 2: VICTIM FOUND
(Hand hits n3 where V is already False)

      TAIL                                         HEAD
      [n1|_] <-> [n2|_] <-> [n3|_] <-> [n4|V] <-> [n5|V]
                     ^           ^
                     |           |
                   Hand moved  VICTIM! (V is False. Unlink n3)


STEP 3: POST-EVICTION
(n3 removed, Hand lands on n4 - which might have V=True)

      TAIL                              HEAD
      [n1|_] <-> [n2|_] <-> [n4|V] <-> [n5|V]
                                       ^
       n3 node recycled (arena free list)|
                            HAND (Points to next live node)
```

### 4.4 Memory Requirements (Index Overhead)

The primary constraint on blobcache capacity is the **Item Count**, not the total storage size. The In-Memory Index requires a fixed amount of RAM (~76 bytes) for every object stored to maintain O(1) lookup speeds.

**The Constant:** Allocated memory grows at a linear rate of **~76 MB per 1 Million Items**.

#### Index Memory vs. Storage Capacity

This table correlates the Index RAM Overhead (RSS) with the amount of disk space those items would consume at different average blob sizes.

| Item Count | Index RAM Overhead | Storage @ 4KB (Tiny) | Storage @ 64KB (Avg) | Storage @ 128KB (Mod) |
|------------|-------------------|----------------------|----------------------|-----------------------|
| 1 Million | 76 MB | 4 GB | 64 GB | 128 GB |
| 10 Million | 760 MB | 40 GB | 640 GB | 1.28 TB |
| 50 Million | 3.8 GB | 200 GB | 3.2 TB | 6.4 TB |
| 100 Million | 7.6 GB | 400 GB | 6.4 TB | 12.8 TB |
| 250 Million | 19 GB | 1 TB | 16 TB | 32 TB |

#### Workload Analysis

**RAM-Bound Workload (Tiny Blobs):**
- If you store 4 KB blobs, a 1 TB drive holds ~250 Million items.
- **Result:** You will need ~19 GB of RAM just for the index.
- **Constraint:** You will run out of RAM before you run out of Disk.

**Disk-Bound Workload (Moderate Blobs):**
- If you store 128 KB blobs, a 1 TB drive holds only ~8 Million items.
- **Result:** You only need ~608 MB of RAM for the index.
- **Constraint:** You will run out of Disk long before you feel any memory pressure.

#### Calculating Your Requirement

To estimate the memory overhead for your specific workload:

$$\text{Index RAM (MB)} \approx \frac{\text{Disk Capacity (MB)}}{\text{Avg Blob Size (MB)}} \times 0.000076$$

**Example:**
- Disk: 4 TB
- Blob Size: 128 KB
- Items: 4,000,000 MB / 0.128 MB ≈ 31.25 Million
- Required Index RAM: 31.25 × 76 MB ≈ **2.4 GB**

#### Per-Entry Memory Breakdown

| Component | Size | Notes |
|-----------|------|-------|
| `index.Item` | 32 bytes | Key (16B) + SegmentID (4B) + Offset (4B) + PhysicalLen (4B) + Flags (4B) |
| Arena node overhead | 12 bytes | next (4B) + prev (4B) + visited (4B) |
| Map entry overhead | ~32 bytes | Key (16B) + uint32 (4B) + bucket overhead (~12B) |
| **Total per entry** | ~76 bytes | Note: Key is stored twice (Item + map) - 16B redundancy |

**GC Benefit:** Near-zero scan time (no heap pointers in arena design).

> **Future Optimization:** The Key stored in `Item` is redundant with the map key, adding 16 bytes of overhead per entry. A future refactor could store the Key only in the `node` struct (for eviction) and reduce `Item` to 16-byte coordinates, saving ~11% memory.

### 4.5 The Archivist (Segment Store) & Index Semantics

The **Archivist** manages read-only access to persisted segments using a strict Index Item contract:

```go
type Item struct {
    Key         Key    // 128-bit XXH3 hash
    SegmentID   uint32 // File ID
    Offset      uint32 // Absolute byte offset to the START of the Record (Points to Magic)
    PhysicalLen uint32 // Total on-disk size: 42 (Header) + KeyLen + PhysSize
    Flags       uint32 // Status flags
}
```

**Critical Semantics:**

- **Offset:** Points strictly to the first byte of the Magic Number (`0xB10BCAFE`). The Archivist does not scan; it seeks directly to coordinates.
- **PhysicalLen:** Does not include alignment padding. The Index knows exactly where the data ends; no heuristics required.
- **Zero Magic:** The Archivist trusts the Index for location but verifies the Record for safety (HeaderCRC validation before allocation).

**ReadBlob Safety Protocol:**

1. Seek to `Item.Offset`
2. Read exactly 42 bytes (Header)
3. **Verify HeaderCRC** (Crucial: do NOT allocate based on PhysSize if CRC fails)
4. Allocate buffer of size `KeyLen + PhysicalSize`
5. Read Key + Value

**Integration with ReadCache and IOScheduler:**

The Archivist composes an optional `ReadCache` (Section 11.2) and a pluggable `IOScheduler` (Section 11.4). When a ReadCache is configured, `ReadBlob` checks the cache first; on a miss for an admissible blob, the disk read may be widened to a 64KB chunk and all valid records in the chunk are populated into the cache inline. Flight coalescing via `inflightGroup` ensures that concurrent cache misses for the same disk region result in exactly one I/O operation (Section 11.3). All disk reads are issued through the IOScheduler, which defaults to synchronous `pread(2)` but can optionally use `io_uring` for batched asynchronous I/O on Linux.

### 4.6 Startup and Crash Recovery
`OpenIndex` performs a **Persistence Scan**:
1. It discovers all `.meta` files across shard directories via `scanAll`.
2. For each segment, reads the `.meta` file (footer + any tombstone appendages), merging tombstones into footer entries.
3. Populates the sharded arena index and SIEVE list in "Birth Order," ensuring the SIEVE "Hand" is positioned correctly for immediate eviction logic upon startup.
4. Caches raw `FooterEntry` slices in `SegmentMetadata.Entries` for zero disk I/O during segment drain and metadata operations.
5. **KeyIndex Reconciliation:** After the RAM index is populated, the Pebble KeyIndex is opened and reconciled. For each registered segment, the system checks for a sentinel in Pebble. Missing sentinels trigger a targeted rebuild from the `.seg` file (see Section 4.8).

```go
err = p.scanAll(func(m DurableBatch) bool {
    if m.Entries != nil {
        idx.AddSegmentFromEntries(m.SegmentID, m.Entries)
    } else {
        idx.AddSegment(m.SegmentID, m.Items)
    }
    return true
})
```

If a `.meta` file is missing or corrupt, `scanAll` falls back to scanning the `.seg` file record-by-record and optionally rebuilds the `.meta` file for future fast startup.

### 4.7 Write Ordering and Sequence IDs

A cache might seem like a system where "eventual consistency" is acceptable—after all, cached data is ephemeral by nature. However, users have a reasonable expectation that updating a key will make the new value visible, not resurrect some old value that was written earlier. When a user calls `Put("config", v2)` to update a configuration blob, subsequent `Get("config")` calls should return `v2`, not `v1` from a previous write that happened to complete out of order.

Without explicit ordering, two race conditions can cause stale data to appear newer than it actually is:

**The Time Travel Bug** occurs when a slow writer lands in a *new* file after rotation, effectively hiding a newer write that exists in an *old* (sealed) file. Consider a thread that begins a `Put` operation, gets preempted by the OS scheduler for a few milliseconds, and wakes up after the active slab has rotated. If it proceeds to write to the new slab, its stale data will be found first during `Get` operations (which check the active slab before sealed slabs), causing the system to return outdated values.

**The Check-Then-Act Bug** occurs when two concurrent writers for the same key both read stale index state and proceed to update it. The slower writer (with older data) can overwrite the faster writer's entry, causing the index to point to stale data even though newer data was successfully written.

BlobCache addresses both problems through **Sequence IDs**—monotonically increasing 64-bit integers assigned to every write operation. The sequence counter is initialized from `time.Now().UnixNano()` at startup, ensuring that sequences are always increasing even across process restarts without requiring a scan of persistent storage.

Two protection layers leverage these sequence IDs:

The **Lifecycle Guard** tracks `maxSealedSeq`, the highest sequence ID present in the last sealed slab. Any write with a sequence ID less than or equal to this value is definitively stale—it began before the rotation but is trying to land after. Such writes are silently dropped, preventing the time travel bug.

The **Concurrency Guard** uses 256 sharded locks (indexed by key hash) to serialize same-key index updates. Under the lock, the system compares the incoming write's sequence ID against any existing entry. If the existing entry has a higher or equal sequence ID, the incoming write is dropped. This atomic check-and-update prevents the check-then-act bug.

The overhead is minimal: one atomic increment (~10ns) plus a sharded lock acquisition (~20ns uncontended), totaling less than 100ns per write. The read path remains unchanged—sequence IDs are stored but never checked during retrieval, because the write-path guards guarantee that any visible entry is definitively the latest.

Sequence IDs are also critical for WAL crash recovery (see Section 12). Sequence IDs embedded in WAL entries allow crash recovery to correctly skip replaying operations that were already persisted to segments, ensuring exactly-once semantics without complex coordination.

### 4.8 The Key Index: Ordered Iteration via Pebble

**The Problem: No User Keys in RAM**

The RAM index is a hash table keyed by 128-bit XXH3 hashes — O(1) lookup, but no key ordering. User key bytes are intentionally excluded from the RAM index: keys can be 128–512 bytes, and storing them for 10M+ items would blow up memory by gigabytes. Ordered iteration over user keys requires accessing the original key bytes, but they only exist on disk inside segment record bodies.

**Why a Global Pebble DB (Not Per-Segment SSTables)**

An earlier design used per-segment SSTables for ordered iteration. This doesn't scale: with 5,000–10,000 segments, a range scan requires a k-way merge across thousands of file handles. Each additional segment adds file descriptor pressure and merge overhead. A single global Pebble DB provides O(1) seek, O(1) next, and zero file-handle management regardless of segment count.

**Critical Invariant: Rebuildable, Not Authoritative**

The Pebble KeyIndex is explicitly a **rebuildable cache**. If corrupted or missing, it is reconstructed from `.seg` file scans — segments remain the source of truth (self-describing via `.meta` files). This follows the same design principle as the RAM index (Section 4.2): any derived data structure that can be rebuilt from segments is non-authoritative. Pebble's own WAL is disabled (`DisableWAL: true`) since there is no durability requirement.

**4-Namespace Design**

The KeyIndex uses a single Pebble DB with four namespaces distinguished by a one-byte prefix:

| Prefix | Namespace | Key → Value | Purpose |
|--------|-----------|-------------|---------|
| `0x00` | hash→key | `hash(16B)` → `userKey` | Reverse lookup during eviction (SIEVE provides only the hash) |
| `0x01` | key→hash | `userKey` → `hash(16B)` | Ordered iteration over user keys |
| `0x02` | segment membership | `segID(4B) + hash(16B)` → `""` | Enumerate all keys in a segment (drain cleanup) |
| `0x03` | sentinel | `segID(4B)` → `""` | Reconciliation: tracks which segments have been loaded |

The hash→key namespace enables a critical operation: when SIEVE evicts an item, it only knows the 128-bit hash. The reverse lookup retrieves the original user key so both the key→hash and hash→key entries can be deleted.

**Startup Reconciliation**

On startup, the KeyIndex is reconciled against the RAM index:

1. Snapshot all registered segment IDs from the RAM index
2. For each segment, check for a sentinel in Pebble (`HasSentinel`)
3. If missing: scan the `.seg` file to extract user keys, batch-insert all entries, write sentinel
4. If present: segment is already loaded, skip

This sentinel-based protocol handles three scenarios: clean restart (all sentinels present, zero rebuilds), partial Pebble corruption (missing sentinels trigger targeted rebuilds), and complete Pebble loss (full rebuild from all segments).

**Integration Points**

The KeyIndex is updated during:
- **Flush:** `AddEntries` inserts key↔hash mappings and segment membership for all records in the flushed slab
- **Delete:** `DeleteByUserKey` removes both key→hash and hash→key entries
- **Eviction:** `DeleteByHash` performs reverse lookup then removes both entries
- **Drain:** `DrainSegment` iterates segment membership prefix, removes all entries and the sentinel
- **Compaction:** `RelocateSegment` moves membership records from old to new segment ID

All KeyIndex operations are best-effort: failures are logged as warnings but do not block the critical path. The index can always be rebuilt.

---

## 5. Memory Architecture: The User-Space Page Cache

### 5.1 MmapPool: Orchestrated Backpressure
The `MmapPool` manifests physical resource limits (e.g., 8 slabs of 64MB each). It uses Go channels to hold `*MmapBuffer`. If the channel is empty (disk I/O cannot keep up with network ingestion), the `Put()` call **blocks**. This self-regulating backpressure prevents OOM crashes.

```text
INGESTION THREADS                         FLUSH WORKERS
(Network/API)                             (Disk I/O)
|                                         ^
| 1. Acquire()                            | 3. Release()
v                                         |
+----------------------------------------------+----------+
| MMAP POOL (Bounded Channel)                             |
|  [Handle] [Handle] [Handle] [Free] [Free] [Free]        |
+---------------------------------------------------------+
```

#### 5.1.1 The Librarian: A Lock-Free Read-After-Write Cache
The **Librarian** is a dedicated component that provides a multi-gigabyte L1 cache for recently written data. It maintains an immutable, atomic snapshot of `SharedSlab` pointers, enabling **wait-free** reads while the write path continues at full speed. This architecture specifically targets the high-frequency "Read-After-Write" access pattern common in blob workloads.

**Configuration:** `WithMaxCachedSlabs(n)` controls the Librarian's capacity (default: 8 slabs ≈ 512MB).

**The Slab Lifecycle:**

A `SharedSlab` transitions through a five-stage lifecycle managed by reference counting:

1. **Active:** The slab is open for concurrent `Put` operations. The MemTable holds one reference.
2. **Published:** When the slab is created, the Librarian immediately acquires its own reference and adds the slab to its immutable view.
3. **Flushing:** When full, the slab is frozen and handed to a flush worker (via `FlushTicket`), which holds its own reference during the Direct I/O write.
4. **Cached:** After flush completes, the Librarian's reference keeps the slab resident. Readers can acquire zero-copy access via `TryInc()`.
5. **Evicted:** When a new slab is published and the Librarian exceeds `MaxCachedSlabs`, the oldest slab is removed from the view and its reference is released.

```text
[ MMAP POOL ]              [ LIBRARIAN ]
      |                           |
      | 1. Acquire()              |
      v                           |
+-----------------------+         |
|  STAGE 1: ACTIVE      | ------->| 2. Publish()
|  (MemTable ref)       |         |    (Librarian acquires ref)
+-----------------------+         |
      |                           v
      | 3. Frozen             +------------------------+
      v                       | CATALOG (Atomic Slice) |
+-----------------------+     | [Slab0] -> [Slab1] ... |
|  STAGE 2: FLUSHING    |     +------------------------+
|  (Flusher ticket ref) |               |
+-----------------------+               | GET(key)
      |                                 v
      | 4. Persist Complete    +------------------------+
      v                        | Zero-Copy via TryInc() |
+-----------------------+      +------------------------+
|  STAGE 3: CACHED      |
|  (Librarian ref only) | <-- Readers can still acquire
+-----------------------+
      |
      | 5. Capacity Exceeded (Oldest evicted)
      v
[ EVICTED / RELEASED ]
```

**Lock-Free Guarantees:**

* **Readers (Acquire):** Load the atomic slice pointer, iterate, and use `TryInc()` to safely pin a slab. If `TryInc()` fails (slab was evicted mid-iteration), treat as a miss.
* **Writers (Publish):** Use Compare-And-Swap to atomically install a new slice. If CAS fails, retry. Only the successful publisher can unpin the victim.
* **No Mutexes:** The entire hot path (publish + acquire) is wait-free, avoiding lock contention under high concurrency.

This design provides a multi-hundred-megabyte L1 cache managed as `WriteBufferSize` units (default: 64MB), avoiding the overhead of managing millions of individual entries. Serving a hit from a `Cached` slab involves a simple pointer offset within the `mmap` arena, resulting in zero memory copies and minimal CPU cycles.

### 5.2 Short-Circuiting "Pathological" Blobs: Virtual Interleaving

Large blobs (XL writes) exceeding `WriteBufferSize` use a **Virtual Interleaving** strategy that avoids double-writes while maintaining strict I/O alignment:

1. **Zero-Width Reservation:** Under the MemTable lock, the system reserves a position (`wPos`) at a page-aligned boundary and increments `xlSize`, but does NOT allocate memory yet.

2. **Unlocked Allocation:** After releasing the lock, the system allocates an `XLBuf` (standalone mmap buffer with `FileHeaderSize` reserved at start for alignment).

3. **Timeline Embedding:** The XL buffer is attached to the slab's index entry as a "virtual" insertion point. The main slab buffer has a logical gap but no physical gap.

4. **Flush Merge:** During `flushViaMerge`, XL payloads are physically interleaved at their insertion points:
   ```text
   Slab Buffer:  [Header][Rec1][Rec2][----gap----][Rec3][Rec4]
   XL Buffers:                       [===XL-A===]

   On-Disk:      [Header][Rec1][Rec2][===XL-A===][Rec3][Rec4]
   ```

5. **XL Rotation Threshold:** To prevent pathological workloads (100% XL writes) from never rotating, the system triggers rotation when cumulative `xlSize` exceeds 2× `WriteBufferSize`.

**Benefits:**
- Zero double-writes (data written exactly once)
- No "dark matter" (XL data inline in segment, not separate sidecar files)
- Maintains "One Segment = One File" invariant for simple reads
- RSS returns to baseline after flush (XL buffers are unpooled mmap)

### 5.3 Reference Counting & Pinning
Memory must never be reused while I/O or a user read is in progress. Naive release leads to:
1. **The Interleaving Hazard:** Overwriting data mid-write.
2. **The Munmap Crash:** Unmapping memory while the kernel is still performing a DMA transfer to the NVMe.

BlobCache uses **Go 1.24's `runtime.AddCleanup`**. The buffer only returns to the pool once the flusher is done **and** every user handle has been released.

---

## 6. The I/O Tier: Segments and Direct I/O

### 6.1 Amortizing the Syscall Tax
In a high-traffic environment, writing 10,000 blobs as individual files requires 30,000 syscalls (`open`/`write`/`close`). This involves heavy inode allocation, kernel-level locking, and file-system journaling for every small object. By packing blobs into **Segments** (~64MB, matching `WriteBufferSize`), BlobCache converts thousands of random file-system metadata operations into a single sequential write stream. This reduces the "syscall tax" to near-zero and allows the NVMe controller to operate in its most efficient sequential mode. The MemTable-sized segments enable efficient WAL-rename (zero-copy promotion), fast crash recovery (smaller files to scan), and effective space reclamation (easier to identify sparse segments for drain).

### 6.2 Segment Footers and `.meta` Files: Defense in Depth
Per-segment **`.meta` files** are the **primary crash-recovery mechanism** (Section 4.2). They are read first during startup to populate the RAM index. The **Segment Footer** — a page-aligned (4KB) block at the absolute EOF of each `.seg` file — serves as a secondary recovery path: if the `.meta` file is corrupted or missing, the entire state can be reconstructed by scanning the trailing metadata of the `.seg` file. This two-layer approach ensures that no single-file corruption can prevent recovery.

```text
SEGMENT METADATA BLOCK (N * 4KB Aligned)
+-----------------------------------------------------------+
| Segment Header (SegmentID, CTime)               [16 bytes]|
+-----------------------------------------------------------+
| Footer Entry 0 (64 bytes each):                           |
|   - Hash         (8B)  Key hash Lo (XXH3 128-bit lower)   |
|   - HashHi       (8B)  Key hash Hi (XXH3 128-bit upper)   |
|   - Pos          (8B)  Byte offset in segment             |
|   - LogicalSize  (8B)  Uncompressed size                  |
|   - PhysicalSize (8B)  Compressed size on disk            |
|   - SeqID        (8B)  Monotonic sequence ID              |
|   - Flags        (8B)  Compression, deleted, CRC32        |
|   - KeyLen       (2B)  Original key length                |
|   - Reserved     (6B)  Alignment padding                  |
+-----------------------------------------------------------+
| Footer Entry 1 ...                              [64 bytes]|
+-----------------------------------------------------------+
| Alignment Padding (Zeros to 4KB boundary)       [Variable]|
+-----------------------------------------------------------+
| TAIL (Fixed 20 bytes at absolute EOF):                    |
|   - Record Data Length                          [8 bytes] |
|   - CRC32 Checksum                              [4 bytes] |
|   - Magic Number (0xB10BCA4EB10BCA4E)           [8 bytes] |
+-----------------------------------------------------------+
```

Each **Footer Entry** (64 bytes) provides sufficient metadata for index reconstruction without scanning record headers.

### 6.3 Direct I/O & The Latency Paradox
BlobCache utilizes `O_DIRECT`. This introduces the **Direct I/O Paradox**: By choosing the slowest physical path to the disk (bypassing the Page Cache), we achieve the highest possible application throughput.

#### The Hidden Costs of "Convenient" I/O
1. **Double-Buffer Tax:** Storing data in app RAM and Kernel RAM simultaneously.
2. **L3 Cache Pollution & The Memory Wall:** CPU-driven memory copies for large data streams evict high-frequency Index metadata.
3. **The 20-30% CPU Tax:** The overhead of kernel VFS/page cache management can consume up to 30% of total CPU cycles.
    * *Ref:* [Saeed et al., "The Case for Custom Storage Engines in the NVMe Era"](https://arxiv.org/abs/2103.14817)
4. **Bus Contention:** Buffered I/O generates $2\times$ memory bus traffic (network $\to$ app, app $\to$ kernel).

#### The Solution: User-Space Authority
The "lie" is moved to the **MemTable**. Ingestion completes at RAM speeds, while the background Flush Worker handles the blocking Direct I/O call without affecting application latency.

### 6.4 Empirical Validation: GC Invisibility Under Load

The following benchmark validates that the `MmapPool` architecture successfully hides multi-gigabyte memory allocations from Go's garbage collector, eliminating GC as a performance bottleneck.

**Test Configuration:**
- 1M write iterations (~1TB logical throughput)
- 32 vCPUs, NVMe storage
- `GODEBUG=gctrace=1` enabled

**GC Trace Analysis:**

| Metric | Observed Value | Implication |
|--------|---------------|-------------|
| **Live Heap** | 33–113 MB | >98% of memory invisible to GC |
| **RSS** | 5.79 GB | Actual memory footprint |
| **GC Overhead** | 0% | No measurable CPU cost |
| **GC Frequency** | Every 20–50 seconds | Minimal collection cycles |
| **STW Pauses** | 0.05–0.2 ms | Sub-millisecond stop-the-world |

**Sample GC Trace (steady-state):**
```
gc 19 @445.447s 0%: 0.075+7.8+0.026 ms clock, 2.2+0.23/60/159+0.85 ms cpu, 223->223->112 MB, 227 MB goal
```

**Interpretation:**
- The GC sees only ~112 MB of live heap while the application uses 5.79 GB RSS
- The ~5.5 GB difference represents `MmapPool` slabs and `Librarian` cache—completely invisible to Go's runtime
- GC cycles occur once every 20–50 seconds (vs. every few hundred milliseconds for heap-allocated buffers)
- 0% CPU overhead throughout the entire 882-second benchmark

**What occupies the ~100 MB Go heap?**
1. **Index Metadata:** Arena index shards, `Item` structs, and map buckets
2. **Bloom Filter:** Bitset for 1M+ keys
3. **Goroutine Stacks:** 32 P workers and flush goroutines
4. **Benchmark Infrastructure:** HDR histograms and counters

**The iostat Validation:**
During the benchmark, disk utilization remained at **100% continuously** with no drops to 0%. This confirms that the Direct I/O path maintained consistent pressure without any GC-induced stalls or buffer-related pauses.

**Conclusion:**
The `MmapPool` design achieves its primary goal: enabling multi-gigabyte, zero-copy buffer management without triggering garbage collection pressure. This allows BlobCache to saturate NVMe bandwidth while the Go runtime remains effectively idle.

### 6.5 Direct I/O vs Buffered I/O: Empirical Comparison

To validate the architectural choice of Direct I/O, a controlled A/B test was performed comparing `O_DIRECT` writes against standard buffered I/O (kernel page cache).

**Test Configuration:**
- 1M write iterations, Zipf(s=1.1, v=1.0) key distribution
- AWS m7gd.8xlarge: 32 vCPUs, 128GB RAM, 1.9TB NVMe
- Same codebase, toggled via `BLOBCACHE_BUFFERED_IO=1` environment variable

**Latency Comparison (nanoseconds):**

| Operation | Percentile | Direct I/O | Buffered I/O | Delta |
|-----------|------------|------------|--------------|-------|
| **GET** | p50 | 2,527 | 2,967 | +17% |
| **GET** | p99 | 2,547,711 | 287,743 | **-89%** |
| **GET** | p999 | 55,771,135 | 592,895 | **-99%** |
| **PUT** | p50 | 108,159 | 143,487 | +33% |
| **PUT** | p99 | 311,689,215 | 306,446,335 | ~Same |
| **PUT** | p999 | 449,576,959 | 454,819,839 | ~Same |

**System Behavior (vmstat):**

| Metric | Direct I/O | Buffered I/O |
|--------|------------|--------------|
| Context Switches/sec | ~7,000 | 20,000–45,000 |
| Free Memory | ~51 GB (stable) | 1–17 GB (volatile) |
| Page Cache | ~69 GB | 107–120 GB |
| Blocked Processes (`b`) | 0 | 7–22 |
| kswapd Activity | Minimal | Elevated (cliff) |

**Forensic Analysis: The "Traffic Jam" (`b=22`)**

During buffered I/O peak load, the `b` (blocked processes) column spiked to **22**, indicating a system-wide lockup when the kernel hit its `vm.dirty_ratio` limit:

1. **6 Flush Workers:** Blocked on `write()` waiting for disk sectors
2. **1 Producer:** Blocked on memory allocation (waiting for pages to free)
3. **1 Eviction Thread:** Blocked on `fallocate(PUNCH_HOLE)` requiring exclusive inode lock
4. **~14 "Innocent Bystanders":** System daemons and Go runtime threads (GC helpers) swept into the global I/O freeze

This "blocked list" demonstrates that buffered I/O creates unpredictable latency spikes—any operation becomes a game of Russian Roulette as the kernel thrashes.

**The Memory Monopoly:**

The page cache growth to **118 GB** illustrates the "write pollution" problem:
- The application wrote 1TB of data
- The kernel greedily cached it, evicting useful hot data (binaries, index files, cached reads)
- The application effectively "DDOS'd" the node's memory subsystem without exceeding its own RSS limit

**Analysis:**

1. **GET Tail Latency:** Buffered I/O exhibits dramatically better read tail latencies (p99: 8.8× better, p999: 94× better). The kernel page cache serves hot data from RAM, avoiding disk I/O for frequently accessed blobs.

2. **PUT Latency:** Direct I/O has slightly better p50 PUT latency (~108μs vs ~143μs) due to avoiding the kernel's buffer management overhead. Tail latencies are equivalent—both are dominated by NVMe write bandwidth saturation.

3. **Memory Pressure:** Buffered I/O consumed nearly all available RAM for page cache (~120GB), triggering kswapd activity and a 3× increase in context switches. This "cliff" behavior is unpredictable under varying workloads.

4. **Overall Throughput:** Identical (~881s for 1M ops). The bottleneck is NVMe bandwidth, not I/O path.

**Why BlobCache Defaults to Direct I/O:**

The data confirms that Direct I/O is not merely an optimization—it is a **structural requirement** for reliability at 1GB/s+ ingestion rates.

1. **Predictable Memory:** BlobCache maintains explicit control over its memory footprint via `MmapPool` (~3.7GB fixed). Buffered I/O surrenders memory management to the kernel, which monopolizes 118GB+ and evicts application-critical data unpredictably.

2. **System Isolation ("Good Citizenship"):** By strictly bounding memory to a fixed arena, BlobCache guarantees it will never trigger OOM kills, activate swap, or degrade neighboring workloads. The `swpd` column remained **0** throughout Direct I/O testing.

3. **Deterministic Flow Control:** Direct I/O uses Go channels for backpressure (`MaxInflightSlabs`), while buffered I/O relies on chaotic kernel blocking (`b=22`). The former is debuggable; the latter is a black box.

4. **Read-After-Write:** BlobCache's `Librarian` component provides its own "controlled page cache" for recently written data, capturing most of the read benefit without kernel overhead or "write pollution."

5. **Sustained Saturation:** Direct I/O achieved a flat-line **100% disk utilization** for the entire 15-minute test with zero drops. The software bottleneck was effectively removed.

**Configuration Option:**
For workloads with known memory headroom and read-heavy access patterns, buffered I/O can be enabled via `WithDirectIOWrite(false)`. This may improve GET tail latencies at the cost of memory predictability.

---

## 7. Compression Strategy: Distributed In-Thread Compression

### 7.1 Distributed Ingestion & The "1/8th" Heuristic
BlobCache utilizes a distributed compression model where data transformation is performed by the calling goroutine during the `Put()` operation. By offloading this burden to the ingestion threads, the system prevents background flush workers from becoming a CPU bottleneck, ensuring NVMe write saturation even under high load. This effectively increases MemTable density, as compressed payloads allow each physical slab (default: 64MB) to host a significantly larger volume of logical data before requiring a flush to disk.

To prevent wasting cycles on incompressible data, the system employs a **"1/8th Early Abort"** heuristic inspired by ZFS. The compression algorithm is provided a destination buffer exactly 12.5% smaller than the source; if the buffer is filled before the blob is fully processed, the operation is aborted and the blob is stored raw. This "savings rule" ensures CPU time is only invested in data yielding meaningful footprint reductions while signaling that the data may already be compressed or contain high entropy.

### 7.2 Per-Blob Compression and Space Reclamation
The decision to compress individual blobs rather than larger logical chunks is critical to the efficacy of the **Sieve eviction policy** and **segment drain**. Because each blob remains an independent unit of compression, the segment metadata can precisely track which items are live vs. dead, enabling accurate live-bytes tracking for pressure-driven drain candidate selection.

Alternative "chunked" designs were rejected because they introduce **reclamation friction**: segment drain requires knowing exactly which items are dead. If multiple blobs share a compressed block, the segment cannot be drained until every blob within that block has been evicted, artificially inflating the live-item count and delaying space reclamation.

### 7.3 Dual-Size Metadata & Zero-Allocation Reads
To maximize retrieval efficiency, the in-memory index and Segment Footer track both the **Logical (Uncompressed)** and **Physical (Stored)** sizes. This dual-size tracking enables a **Zero-Allocation** retrieval path: by knowing the logical size upfront, the system can pre-allocate a destination buffer of the exact required size, eliminating the CPU and GC overhead of dynamic buffer growth during a `Get()` request.

Furthermore, these metrics provide high-fidelity, real-time observability into compression ratios across the 1TB NVMe tier. This metadata allows the Retrieval Accelerator to execute a single `pread()` for the exact physical byte range, eliminating "Read Amplification" where a small request would otherwise force the disk to pull in a much larger compressed chunk.

---

## 8. Unified Bloom Filter

### 8.1 Unified vs. Distributed Rejection
Traditional LSM-trees check a separate filter for every file ($O(N)$). BlobCache uses one **Unified Bloom Filter** ($O(1)$). A miss costs $\approx 1ns$ regardless of cache size.

```text
KEY: "blob_77"  →  XXH3-128: {Hi: 0x..., Lo: 0x...}
|
| Hi → Block Selection (bits.Mul64 for uniform distribution)
|
+--[ Probe 1 ]--+
|               |    One 64-Byte "Block" (1 CPU Cache Line)
+--[ Probe 2 ]--|--> +-----------------------------------+
|               |    | ..1..0..1..1..0..0..1..1..0..1.. |
+--[ Probe 3 ]--+    +-----------------------------------+
        ^
        |
    Lo → Probe Pattern (independent entropy)
```

### 8.2 Full 128-bit Entropy: Avoiding the "32-bit Funnel"
The filter uses **full 128-bit XXH3 hashes** to prevent correlated failures at scale:
* **Block Selection (`k.Hi`):** The upper 64 bits select which cache line block to probe using `bits.Mul64(k.Hi, numBlocks)` for uniform distribution.
* **Probe Pattern (`k.Lo`):** The lower 64 bits generate the bit positions within the block.

This design prevents the "32-bit funnel" bug where truncating hashes causes correlated collisions. With 32-bit hashes, the birthday paradox reaches 50% collision probability at just 77K items. At 250M items, truncated hashes cause both block selection AND probe pattern to collide for different keys—a catastrophic false positive scenario that full 128-bit keys eliminate.

### 8.3 False Positive Decay
Standard Bloom filters cannot handle deletions. As Sieve evicts blobs, the filter decays with "ghost" entries.
1. **Proactive Tracking:** Rebuilds when ghosts exceed 10%.
2. **Reactive Monitoring:** Rebuilds if Observed FPR spikes.
   Rebuilds are non-blocking; the system snapshots the in-memory index and swaps the filter pointer atomically.

---

## 9. Resilience: Degraded Mode
When a background I/O error occurs (e.g., `Disk Full`), BlobCache enters **Degraded Mode** to maintain availability:

1.  **Worker Halt:** Background flushers stop permanently to prevent inconsistent index states.
2.  **In-Memory FIFO Eviction:** The `MmapPool` stops blocking. Instead, the `MemTable` begins dropping the oldest unflushed slabs from memory to make room for new `Put` calls.
3.  **Pragmatic Resilience:** In this mode, BlobCache functions as a high-speed, volatile cache. While durability is suspended, the system remains alive, serving hits for most-recent data and avoiding a complete service outage.

---

## 10. Eviction & Space Reclamation

BlobCache uses different space reclamation strategies depending on the operating mode:

- **Cache Mode:** SIEVE eviction + Pressure-Driven Segment Drain (zero write amplification)
- **WAL/CAS Mode:** SIEVE eviction + Segment Rewrite Compaction + Tombstone Compaction

### 10.1 SIEVE Eviction

When the cache exceeds `MaxSize`, the maintenance worker runs eviction in a loop until the cache is below the hysteresis target (93% of `MaxSize`).

The SIEVE algorithm scans across all 256 shards to identify cold items. Each `EvictBatch` call collects victims from multiple shards (up to 64 items per shard lock hold, ~13us), and victims may span multiple segments. The caller groups evicted items by segment ID to update per-segment metadata accurately.

```text
SIEVE EVICTION FLOW:

   Cache exceeds MaxSize
        |
        v
   [EvictBatch(targetBytes)]     <-- Scan shards, collect cold items
        |
        v
   [DeleteBlobs(...)]            <-- Persist tombstones to .meta files
        |
        v
   [Group by SegmentID]          <-- Victims span multiple segments
        |
        v
   [UpdateSegmentOnDelete()]     <-- Per-segment waste tracking
        |
        v
   [Repeat until below target]
```

**Why Pure SIEVE (No Spatial Expansion)?**

An earlier design co-evicted physically adjacent items ("bystanders") alongside the SIEVE-selected anchor to create contiguous dead regions. This was motivated by merge compaction using `copy_file_range`, which benefits from large contiguous holes. With merge compaction removed in favor of pressure-driven segment drain (Section 10.2), spatial expansion adds complexity without benefit: drain deletes entire segment files regardless of hole distribution.

### 10.2 Pressure-Driven Segment Drain (Cache Mode)

Segment drain is the zero-write-amplification replacement for merge compaction. When the total on-disk footprint exceeds `MaxSize`, the sparsest segments (least live data) are deleted until disk usage fits within budget.

Drain operates on a **different pressure signal** than SIEVE eviction:
- **SIEVE** manages live data size: triggers when `approxSize > MaxSize`
- **Drain** manages disk waste: triggers when `(diskBytes - liveBytes) > MaxSize / 2`

This separation prevents drain from competing with SIEVE. SIEVE runs first, selectively removing cold items. Over time, dead items accumulate as waste in segments. Drain kicks in only when that accumulated waste becomes significant (>50% of capacity), deleting the sparsest segments to reclaim disk space.

```text
PRESSURE-DRIVEN DRAIN:

   estimatedDisk = numSegments * WriteBufferSize
   waste = estimatedDisk - liveBytes
   excess = waste - MaxSize/2
        |
        v (excess > 0?)
   [Get drain candidates]          <-- All cooled segments, sorted by LiveBytes ascending
        |
        v
   [For each candidate while excess > 0:]
        |
        v
   [Exclusive segment lock]        <-- Blocks Delete() during drain
        |
        v
   [DrainSegment()]                <-- Atomic: remove each live item via deleteIfAt
        |                               (verifies segID+offset match, safe skip if relocated)
        v
   [Drop archivist FD cache]       <-- Close cached file handle
        |
        v
   [Delete .seg + .meta files]     <-- Physical space reclaimed
        |
        v
   [excess -= WriteBufferSize]     <-- Repeat until within budget
```

**Key Properties:**
- **Zero write amplification:** No data is rewritten. Entire segment files are simply deleted.
- **Pressure-driven, not threshold-driven:** Drain activates only when disk exceeds `MaxSize`. No per-segment waste threshold to tune.
- **Unbounded:** Drains as many segments as needed to meet the space budget. Under sustained write pressure, this may drain dozens of segments per maintenance pass.
- **Sparsest-first:** Segments with the least live data are drained first, minimizing the number of cache misses per GB of disk reclaimed.
- **Cooling period:** Only segments older than the Librarian cache window are eligible for drain, preventing eviction of recently-written data still serving read-after-write hits.
- **WAL mode excluded:** WAL/CAS mode cannot tolerate data loss from drain. It uses segment rewrite compaction (Section 10.3) and tombstone compaction (Section 10.6) instead.

### 10.3 Segment Rewrite Compaction (WAL/CAS Mode)

In WAL mode, segment drain is not an option — deleting entire segments would lose durable data. Instead, BlobCache uses **single-segment rewrite compaction**: sparse segments are rewritten in-place, copying only live records to a new segment file via `copy_file_range`.

**When it triggers:**
- Maintenance worker calls `maybeRewriteSegments` each cycle
- Segment must have cooled past the Librarian cache window (same cooling period as drain)
- Waste ratio must exceed `CompactionWasteThreshold` (configurable)
- Candidates returned by `GetRewriteCandidates`, sorted by segment ID ascending

**Algorithm:**

```text
SEGMENT REWRITE COMPACTION:

   [Get manifest from in-memory cache]
        |
        v
   [Classify entries using RAM index]
        |-- Live: entry exists in RAM, points to this segment, not deleted
        |-- Tombstone: entry is deleted; keep if HasOlderShadow(), dissolve otherwise
        |-- Stale: entry not in RAM or points elsewhere; skip
        |
        v
   [Build page-aligned runs]            <-- Sort live entries by offset, merge
        |                                    nearby records (gap ≤ 16KB absorbed),
        |                                    align to block boundaries
        v
   [copy_file_range per run]            <-- Source and destination both O_DIRECT
        |                                    Block-aligned offsets for reflink
        |                                    eligibility on XFS
        v
   [fdatasync + atomic rename]          <-- tmp.compact.tmp → segID.seg
        |
        v
   [Write .meta footer]                <-- New positions for live entries,
        |                                    preserved tombstones (Pos=0)
        v
   [Relocate in RAM index]             <-- RelocateBatch: old (segID, offset) →
        |                                    new (segID, offset) with CAS semantics
        v
   [Relocate in Pebble KeyIndex]       <-- RelocateSegment: move membership records
        |
        v
   [Drop old segment]                  <-- DropSegment + delete .seg/.meta files
```

**Tombstone Dissolution:**

During classification, each tombstone is checked via `HasOlderShadow(key, segID)`. This function uses `Peek()` (reads without marking visited — no SIEVE perturbation) to check whether an older version of the key exists in any segment below `segID`. If no older shadow exists, the tombstone is dissolved — it is simply omitted from the output. This is the primary mechanism for bounded tombstone accumulation.

**100% Dead Optimization:**

If a segment has zero live items and all its tombstones are dissolvable, the rewrite is skipped entirely. The segment is deleted directly (`DropSegment` + file removal), avoiding the overhead of creating a new segment file.

**No Pre-allocation:**

The output file is NOT pre-allocated with `fallocate`. On XFS, pre-allocation fills the range with zeroed extents, forcing `copy_file_range` to perform actual data copies instead of metadata-only reflinks (COW). Leaving the file sparse preserves reflink eligibility.

### 10.4 Why Not Merge Compaction? (The copy_file_range False Start)

BlobCache initially implemented merge compaction using Linux's `copy_file_range` syscall, which uses server-side copy (reflinks on supported filesystems) to merge sparse segments into dense output without reading data into userspace. The theory was compelling: zero-copy, kernel-optimized, O_DIRECT-compatible.

**What went wrong:**

1. **Fragmented output files.** `copy_file_range` with reflinks produces output files whose physical extents mirror the source files' layout. When copying live records from a sparse segment, the output file inherits the fragmented block allocation pattern — the "holes" between live records become allocated-but-discontiguous extents rather than sequential blocks. The resulting segment is logically dense but physically fragmented, defeating the purpose of compaction.

2. **Filesystem dependence.** Reflink behavior varies dramatically across filesystems (XFS, ext4, Btrfs) and even across kernel versions. Some fall back to full data copy (2x write amplification), some refuse cross-file reflinks, some silently produce fragmented output. This made the optimization unreliable in production.

3. **Complexity tax.** Merge compaction requires: contiguity validation (Leapfrog Hazard prevention), atomic `Relocate` with CAS semantics, rate limiting to avoid saturating I/O bandwidth, cooling period coordination, and tombstone GC logic. This machinery adds ~750 lines of code and several subtle concurrency protocols.

4. **Write amplification.** Even with reflinks, merge compaction has write amplification > 0: writing the output segment footer, updating the index, and the copy syscall overhead. Segment drain achieves true 0.00x write amplification — it only deletes files.

**The insight:** For cache workloads, the remaining ~10% of live items in a sparse segment are not worth preserving. They can be re-fetched from the origin on cache miss. Segment drain exploits this by simply deleting the entire segment, trading a small increase in miss rate for zero I/O cost and zero code complexity.

### 10.5 The Deletion Model: Tombstones and Consistency

BlobCache uses a **soft delete (tombstone)** model rather than immediate removal. When `Delete(key)` is called:

**Cache Mode (no WAL):**
1. **Tombstone:** Write tombstone to segment's `.meta` file (incremental log)
2. **Mark deleted in RAM:** Set `IsDeleted()` flag in the in-memory index
3. **Update segment metadata:** Track live bytes for pressure-driven drain

**CAS Mode (WAL enabled):**
1. **WAL Write:** Append delete record to WAL (crash consistency)
2. **Tombstone:** Write tombstone to segment's `.meta` file
3. **Mark deleted in RAM:** Set `IsDeleted()` flag
4. **Update segment metadata:** Track tombstone count for compaction candidate selection

```text
DELETE LIFECYCLE:

   Delete(key)
        |
        v
   [WAL: Append Delete Record]    <-- CAS mode only (crash consistency)
        |
        v
   [Tombstone in .meta]           <-- Incremental append to segment metadata
        |
        v
   [Mark Item as Deleted in RAM]  <-- Soft delete (IsDeleted flag)
        |
        v
   [Update Segment Metadata]      <-- Track waste ratio / tombstone count
        |
        v
   [Segment Drain or Tombstone    <-- Deferred cleanup by maintenance worker
    Compaction]
```

**Why Tombstones (in WAL mode)?** Tombstones prevent the "Leapfrog Hazard"—a subtle bug that can resurrect deleted keys if compaction ever merges non-contiguous segments. See Section 10.7.

### 10.6 Tombstone Compaction (WAL/CAS Mode)

In WAL mode, tombstones accumulate as incremental appendages to `.meta` files. Over time, a segment's `.meta` file grows: `[footer block] [tombstone batch 1] [batch 2] ...`. Tombstone compaction is a metadata-only operation that collapses these appendages back into the footer entries.

**When it triggers:**
- Segment crosses `TombstoneCompactionThreshold` (100 tombstones)
- Segment has cooled past `MaxCachedSlabs + CoolingPeriodMargin` segment IDs

**What it does:**
1. Read the full manifest (footer + all tombstone batches merged)
2. Rewrite the `.meta` file with tombstones baked into footer entry flags
3. Result: smaller `.meta` file, faster startup scans, bounded metadata growth

This is purely a metadata maintenance operation — no blob data is read or written.

### 10.7 The Leapfrog Hazard and Strict Contiguity

The Leapfrog Hazard is a correctness concern for any system that merges segments while tombstones exist. It is primarily relevant to WAL/CAS mode where data must not be lost:

```text
THE LEAPFROG HAZARD

Timeline:
  T1: Key K exists in Segment A (oldest)
  T2: Key K deleted -> tombstone in Segment A
  T3: Key K re-written -> new entry in Segment C (newest)

Segments: [A: tombstone(K)] [B: ...] [C: live(K)]

WRONG: Compact A + C, skipping B
  - Tombstone from A "wins" over live entry from C
  - Result: K is deleted, even though it was re-written!

CORRECT: Only compact contiguous ranges [A, B] or [B, C] or [A, B, C]
```

**Tail GC:** Tombstones can only be safely dropped when compacting the oldest (tail) segment, because by definition no older segment exists that could have a conflicting entry.

In cache mode, the Leapfrog Hazard is irrelevant: segment drain deletes entire segments (it doesn't merge them), and data loss from drain is acceptable (items become cache misses).

---

## 11. The Read Path

### 11.1 Read Path Architecture

BlobCache’s read path is a three-tier hierarchy, each tier trading latency for capacity:

```text
[ USER GET(Key) ]
        |
        v
+-------------------+       HIT        +--------------------+
| 1. Librarian      | ---------------> | Zero-Copy View     |
| (Write-path slabs)|                  | (mmap pointer)     |
| ~seconds of data  |                  | p50: ~8µs          |
+-------------------+                  +--------------------+
        |
        | MISS
        v
+-------------------+       HIT        +--------------------+
| 2. ReadCache      | ---------------> | Slab-resident copy |
| (Dedicated arenas)|                  | (mmap arena)       |
| configurable size |                  | p50: ~10µs         |
+-------------------+                  +--------------------+
        |
        | MISS
        v
+-------------------+                  +--------------------+
| 3. Archivist      | ---- pread ----> | NVMe segment file  |
| (Disk I/O)        |                  | (via IOScheduler)  |
| flight coalescing |                  | p50: ~50-100µs     |
+-------------------+                  +--------------------+
        |
        v (if ReadCache enabled)
  [Populate cache inline]
```

**Decision flow:**

1. **Librarian** (Section 5.1.1): Zero-copy access to write-path slabs still in mmap’d memory. Covers the hot write-after-read window (~seconds of writes). No additional memory cost — reuses write-path arenas.

2. **ReadCache** (Section 11.2): User-space mmap-backed read cache for disk-resident blobs accessed frequently enough to justify caching. Disabled by default. Uses its own dedicated `MmapPool` (no contention with the write path).

3. **Archivist** (Section 11.3): Single disk read with flight coalescing. If ReadCache is enabled, the disk read may be widened to a 64KB chunk and all valid records in the chunk are populated into the cache inline (temporal prefetch).

### 11.2 ReadCache: User-Space Read Acceleration

The **ReadCache** is an optional second-tier read cache for blobs that have fallen out of the Librarian window but are still accessed frequently. Typical use case: temporally distant reads-after-write, or workloads where the kernel page cache is under pressure from other processes.

**Architecture:**

ReadCache composes a `Librarian` for its sealed slab list, giving it the same lock-free `Acquire` and FIFO eviction. The only addition is an **active slab** for inserting records populated from disk reads.

```text
+-------------------+
| ReadCache         |
|                   |
|  Active Slab      | <-- Insert() copies raw records from disk
|  (mu-protected)   |     Bloom BEFORE index (no false negatives)
|                   |
|  Librarian        | <-- Sealed slabs (lock-free Acquire)
|  [Slab N-1]       |     FIFO eviction: seal → publish → acquire new
|  [Slab N-2]       |
|  [...]            |
|                   |
|  MmapPool         | <-- Dedicated arena pool (separate from write path)
+-------------------+
```

**Key Properties:**

- **Admission policy:** Items exceeding `maxItemSize` or slab size are rejected (counter: `Skipped`). Large blobs bypass the cache and go direct to disk.
- **PopulateChunk:** When the Archivist reads a 64KB chunk for a small blob, it calls `PopulateChunk` to scan the buffer for valid records (Magic + HeaderCRC verification) and insert each one. This provides **temporal prefetch** — neighboring records in the same chunk are cached for free.
- **FIFO eviction:** When the active slab fills, it is sealed, published to the Librarian (which handles FIFO eviction of the oldest slab), and a new active slab is acquired from the pool.
- **Separate MmapPool:** ReadCache has its own arena pool. No contention with write-path slab acquisition.
- **Invalidation:** `Delete()` propagates to `ReadCache.Invalidate()`, removing the key from all slabs (sealed and active) to prevent serving stale data.

**Configuration:**
```go
cache, _ := blobcache.New(path,
    blobcache.WithReadCacheSlabs(4),          // 4 read cache slabs
    blobcache.WithReadCacheMaxItemSize(1<<20), // Skip items > 1MB
)
```

### 11.3 The Archivist: Single-Read Optimization

The **Archivist** manages all disk reads. When a ReadCache is configured, the Archivist integrates with it to minimize I/O:

**Flight Coalescing (inflightGroup):**

When thousands of goroutines experience a cache miss for the same disk region simultaneously, the `inflightGroup` ensures exactly **one** goroutine performs the disk I/O. Others block on a per-shard `sync.Cond` and are woken when the fetch completes.

```text
FLIGHT COALESCING:

   Goroutine A: cache miss for chunk X
        |
        v
   [inflightGroup.DoOnce(chunkKey)]
        |
        +--> Leader: performs disk I/O, populates ReadCache
        |
   Goroutine B: cache miss for same chunk X
        |
        v
   [inflightGroup.DoOnce(chunkKey)]
        |
        +--> Waiter: blocks on sync.Cond, then serves from ReadCache
```

The flight key packs `(segmentID, alignedChunkOffset)` into a `uint64`. With 64 shards and ~50–100 active flights, each shard has ~1–2 concurrent flights, so `Broadcast` wakes at most a few goroutines per shard.

**Chunk Alignment for Small Blobs:**

For blobs ≤ 64KB (`prefetchChunkSize`), the Archivist reads a 64KB chunk aligned to chunk boundaries. This captures neighboring records for temporal prefetch via `PopulateChunk`. For large blobs (> 64KB), the read is sized to the exact blob with page alignment (no wasted bandwidth).

**Leader/Waiter Pattern:**

1. First cache miss for a chunk becomes the **leader**: performs one disk read, populates ReadCache, parses the target record
2. Concurrent misses for the same chunk become **waiters**: block until the leader finishes, then re-check ReadCache
3. If a waiter’s record wasn’t in the populated chunk (edge cases: record at chunk boundary, parse error), it falls back to a standard disk read

### 11.4 IOScheduler: Pluggable I/O Backend

All disk reads flow through the `IOScheduler` interface (`internal/iosched`), which abstracts positioned reads for pluggable backends:

```go
type IOScheduler interface {
    ReadAt(fd int, buf []byte, offset int64) (int, error)
    Stats() Stats
    Close() error
}
```

**PreadScheduler (Default):**

Synchronous `pread(2)`. Each `ReadAt` maps directly to one syscall. Zero overhead, zero complexity. Portable across Linux and Darwin.

**URingScheduler (Linux Only, Opt-In):**

Asynchronous `io_uring` with batched submission and optional `SQPOLL` kernel polling.

Architecture: a single **coordinator goroutine** exclusively owns the ring. Callers append requests to a mutex-protected queue and send a notification on a buffered(1) signal channel. The coordinator uses a **sliding-window protocol**:

```text
SLIDING-WINDOW COORDINATOR:

   [Collect: grab pending requests from queue]
        |
        v
   [Fill: prepare SQEs for free ring slots]
        |
        v
   [SubmitAndWait(1): submit to kernel, wait for ≥1 CQE]
        |
        v
   [Reap: process all ready CQEs, free slots, wake callers]
        |
        v
   [Loop: freed slots available for next batch]
```

The ring stays as full as possible, keeping the NVMe pipeline saturated. With `SQPOLL` enabled, the kernel spawns a polling thread that continuously checks for new submissions, eliminating the `io_uring_enter` syscall on the submission path (burns one CPU core).

**Latency Histograms:**

Both schedulers track per-read I/O latency via HDR histograms (1µs–10s range, 3 significant digits). The URingScheduler additionally tracks batching statistics (batch count, average batch size, max batch size).

### 11.5 Buffered vs. Direct I/O Reads

Reads default to **buffered I/O** (kernel page cache). The `WithDirectIORead` option enables Direct I/O reads with block-aligned buffers. The right choice depends heavily on the access pattern.

#### When Buffered Reads Win

For pure point-lookup workloads with strong temporal locality — `Get(key)` calls that repeatedly access the same hot subset of keys — buffered I/O is an asset. The kernel page cache learns which 4KB pages are hot, keeps them in RAM, and serves repeat accesses from DRAM without a disk round-trip. Workloads where the Librarian and ReadCache miss rates are high but the hot set fits in the kernel page cache benefit from this behavior.

#### Why BlobCache Data Does Not Benefit From Kernel Read-Ahead

BlobCache’s log-structured layout means that **blobs for adjacent keys are physically non-contiguous on disk**. Data is written in arrival order, not key order. Blob A at key `"user:1001"` may land in segment 42 at offset 0x4000, while blob B at key `"user:1002"` lands in segment 7 at offset 0x1F8000. Kernel read-ahead assumes sequential access patterns and pre-fetches pages beyond the current read position — an assumption that is fundamentally wrong for random-key blob workloads.

#### The Iterator Problem: Page Cache Thrashing

Iterator workloads expose the worst-case failure mode of buffered reads in BlobCache. An iterator must visit keys in sorted order, but the underlying blobs are stored in write-arrival order across many segments. Each `View()` call in an iterator issues a read to a **random segment, at a random offset** — there is no spatial locality between successive keys.

With buffered I/O, each random read:
1. Fetches one or more 4KB kernel pages from the disk (even if the blob is 100 bytes)
2. Populates the page cache with pages that will **never be reused** (the next key is elsewhere)
3. Evicts hot pages that were genuinely useful (recently written blobs, index metadata)

For a **wide iterator** — one that scans thousands of keys — this becomes a self-inflicted page cache DoS. Thousands of 4KB pages are loaded, consume RAM, evict hot data, and are immediately thrown away when the next key arrives at a completely different location. The page cache thrashes, and all concurrent readers suffer elevated miss rates. On a 128GB machine with a 120GB page cache, a wide iterator can corrupt the entire working set in minutes.

**Write pressure compounds the problem:** as analyzed in Section 6.5, BlobCache’s write path floods the page cache with recently-written blob data. With Direct I/O writes disabled, the page cache is already under pressure from the write side. Adding random iterator reads turns a manageable situation into a cliff.

#### Direct I/O Reads: Predictable IOPS, No Cache Pollution

`WithDirectIORead(true)` opens segment files with `O_DIRECT` (Linux) or `F_NOCACHE` (Darwin). Each read:
- Issues exactly one aligned `pread` for the blob’s physical extent
- Bypasses the page cache entirely — no pollution, no eviction side effects
- Provides predictable, hardware-bound latency (no wait for page cache to warm up)
- Allows the Librarian and ReadCache to remain effective (they serve hot data without kernel interference)

The iterator’s read-ahead prefetch (Section 4.8) works correctly with Direct I/O: it explicitly fetches contiguous records using a user-space buffer, controlled by the iterator itself rather than the kernel’s heuristics.

#### Decision Guide

| Workload | Recommended Read Mode |
|----------|----------------------|
| Pure point lookups, strong temporal locality | Buffered (default) |
| Mixed point lookups + occasional iteration | Buffered (default); monitor page cache hit rate |
| Heavy iterator workloads, wide scans | **Direct I/O** (`WithDirectIORead(true)`) |
| High write pressure + any reads | **Direct I/O** (prevents write pollution cascading into read path) |
| Write-only or Librarian-dominated reads | Either; page cache not a factor |

**Implementation:** The Archivist routes between `readBlobBuffered()` (standard aligned `pread`) and `readBlobDirect()` (4KB-aligned buffer, `O_DIRECT` file handle) based on `IO.DirectIORead`. Fadvise hints are suppressed in Direct I/O mode (pointless without page cache). The IOScheduler (Section 11.4) is shared between both paths.

See Section 6.5 for empirical comparison of buffered vs. Direct I/O under write-heavy load.

### 11.6 Blocking I/O, Go's Threading Model, and Read Path Concurrency Protection

This section explains a non-obvious hazard in any system that issues blocking file I/O from goroutines, and how BlobCache is (and is not) protected against it.

#### Go's M:N Threading Model and Blocking Syscalls

Go runs goroutines on an M:N scheduler with three entities:

- **G** — goroutine (2KB initial stack, scheduled by the runtime)
- **M** — OS thread (kernel-managed, ~1MB including kernel structures)
- **P** — logical processor (`GOMAXPROCS` of these; the "permission to run")

At most `GOMAXPROCS` goroutines execute simultaneously. Each executing goroutine occupies a G→P→M chain. The P is the scarce resource.

When a goroutine issues a **blocking syscall** (any file I/O — `pread`, `write`, `fallocate`), the Go runtime executes the following before the thread enters the kernel:

```text
goroutine calls pread()
      │
      ▼
runtime.entersyscall()
  ├── sets g.status = _Gsyscall
  └── DETACHES the P from this M     ← P is now free
            │
            ▼
      P acquired by another M        ← other goroutines keep running
            │
            ▼
OS thread blocks in kernel           (pread executing, NVMe spinning)
            │
            ▼
      NVMe responds (~50–500µs)
            │
            ▼
runtime.exitsyscall()
  ├── tries to re-acquire a P
  ├── if P available: goroutine resumes immediately on same M
  └── if no P available: goroutine queued, M parks (sleeps)
```

The P is released *before* the thread enters the kernel, so other goroutines continue unimpeded. However, the **OS thread itself remains blocked** — it cannot be reused for other goroutines during the syscall.

#### The Thread Explosion Problem

If N goroutines concurrently enter `pread`, N OS threads are simultaneously blocked in the kernel. Go creates new OS threads to ensure `GOMAXPROCS` goroutines can always run, so total thread count grows to:

```
OS threads = N (blocked in pread) + GOMAXPROCS (running other work)
```

Each additional OS thread costs roughly 1MB of kernel-side memory (task struct, kernel stack, signal tables, mm mappings) plus scheduler bookkeeping. Beyond memory, the real hazard is **thundering herd at completion**: when the NVMe services a batch of I/Os, potentially hundreds of threads all become runnable within microseconds. The Linux CFS scheduler processes all N wakeup events simultaneously — spiking CPU and degrading latency for everything else on the machine.

The hard ceiling is `runtime/debug.SetMaxThreads` (default: 10,000). Exceeding it produces:
```
runtime: program exceeds 10000-thread limit
fatal error: thread exhaustion
```

**Important: this ceiling is not hypothetical.** Any service that lets unbounded goroutine concurrency flow through to blocking file I/O syscalls can hit it under sustained load.

#### Why Throughput Doesn't Scale With Concurrency

Little's Law bounds the useful concurrency for NVMe reads:

```
Concurrency = Throughput × Latency
= 1,200 reads/sec × 0.0005 sec   (1MB blobs, 500µs NVMe latency)
≈ 0.6 concurrent I/Os
```

Even at the device's full bandwidth, fewer than one concurrent read is needed to saturate it. Modern NVMe drives internally exploit parallelism across their queues; the application does not need to provide this concurrency explicitly. Beyond ~4–8 outstanding I/Os, additional concurrent reads increase queue depth and latency with zero throughput gain. **Thread explosion costs are paid with no throughput return.**

#### The Write Path Is Naturally Protected

BlobCache's write path is bounded by design. No matter how many goroutines call `Put()` concurrently:

1. `Put()` writes to the MemTable — a lock-free memory copy, no syscall, no OS thread consumed
2. Slab rotation hands off to a flush worker — there are at most `FlushConcurrency` flush workers (default: 6)
3. Only those `FlushConcurrency` goroutines ever block in write syscalls simultaneously

`MaxInflightSlabs` provides backpressure: when the flush workers can't keep up, `Put()` callers block in Go's scheduler (no OS thread held) waiting for a slab to become available. The write path has **explicit, configurable concurrency control** at the syscall layer.

#### The Read Path Currently Has No Equivalent Protection

Any goroutine that calls `Get()` and misses both the Librarian and ReadCache proceeds directly to `pread`. There is no bound on how many goroutines can simultaneously be in `pread`. With 10,000 concurrent readers all cold-missing, 10,000 OS threads block simultaneously.

The Librarian and ReadCache **reduce the probability** that a given `Get()` reaches `pread` — they do not **bound the concurrency** when reads do reach it. Under adversarial access patterns (uniform random keys, wide iterator scans, cache cold-start) these tiers provide little protection and the full read concurrency is exposed.

#### Buffered I/O Does Not Eliminate the Risk

With buffered I/O, many `pread` calls are served from the kernel page cache (pure RAM, no blocking, no OS thread consumed beyond the cost of the syscall overhead itself). This raises the threshold at which thread explosion occurs — a workload that triggers 10,000 concurrent blocking reads with Direct I/O might only trigger 100 with buffered I/O if the hot set fits in the page cache.

However, under memory pressure (page cache eviction from iterator thrashing, competing processes, or simply insufficient RAM for the working set), buffered reads converge to the same blocking behavior. The thread explosion risk is present; the workload required to trigger it is just more extreme.

#### What Should Be Done: io_uring as the Principled Fix

The `URingScheduler` (Section 11.4) addresses this correctly. With the sliding-window coordinator pattern:

```text
pread model (N=1000 concurrent readers):
  → 1000 OS threads blocked in kernel
  → +GOMAXPROCS threads running other work
  → Thundering herd at completion

URingScheduler model (N=1000 concurrent readers):
  → 1000 goroutines parked in Go scheduler (zero OS threads consumed)
  → 1 coordinator OS thread blocked in io_uring_enter
  → Completions reaped one at a time, goroutines woken individually
```

With a proper coordinator, OS thread count is O(1) regardless of read concurrency. This is the principled solution.

#### Defensive Backstop: Read Concurrency Limiter

Even with `URingScheduler` enabled, a semaphore limiting concurrent preads (used when `URingScheduler` is not in use, or as defense-in-depth) provides the same backpressure semantics as `MaxInflightSlabs`:

```go
// Goroutines that exceed the limit block in Go's scheduler (no OS thread held),
// not in a syscall. Equivalent to how Put() blocks on MmapPool when full.
sem.Acquire(ctx, 1)
defer sem.Release(1)
pread(fd, buf, offset)
```

This makes the read path's syscall concurrency **explicit and configurable**, matching the write path's design. A reasonable default is `2 × GOMAXPROCS` — enough to keep the NVMe pipeline full without allowing unbounded thread growth. This is particularly valuable on Darwin where io_uring is unavailable and `PreadScheduler` is the only option.

#### Summary

| Path | Concurrency Control | Mechanism | Thread Explosion Risk |
|------|--------------------|-----------|-----------------------|
| Write | ✅ Explicit | `FlushConcurrency` + `MaxInflightSlabs` backpressure | None — bounded by design |
| Read (URingScheduler) | ✅ Implicit | Coordinator goroutine; readers park in scheduler | None — O(1) threads |
| Read (PreadScheduler) | ❌ None | Unbounded goroutines → unbounded OS threads | Present under high cold-read concurrency |
| Read (Buffered I/O) | ❌ None | Page cache reduces frequency; same risk under pressure | Present but requires more extreme workload |

---

## 12. Write-Ahead Log (WAL)

BlobCache supports an optional Write-Ahead Log that transforms the cache into a crash-consistent Content Addressable Storage (CAS) system. When enabled, every Put operation is durably recorded before the caller returns, guaranteeing that data survives process crashes and system restarts.

### 12.1 The "Sync Switch" Architecture

The engine operates in two modes sharing the same `sys` primitives, optimized for OS differences:

**Cache Mode (Turbo):**

| Aspect | Description |
|--------|-------------|
| Ingest | Write to MemTable only |
| Durability | Memory-only until flush |
| Linux I/O | `O_DIRECT` (Bulk Dump) |
| Darwin I/O | `F_NOCACHE` (Bulk Dump) |
| Flush Action | Direct segment write via `flushViaMerge` |

**Durable Mode (Safe):**

| Aspect | Description |
|--------|-------------|
| Ingest | Write to MemTable + WAL (Stream) |
| Durability | Synchronous |
| Linux I/O | `O_DIRECT \| O_DSYNC` (Stream) |
| Darwin I/O | `F_NOCACHE` + `F_FULLFSYNC` (Stream) |
| Flush Action | Rename `wal-active.log` → `segment-N.data` |

### 12.2 When to Use WAL

WAL is appropriate for workloads requiring durability guarantees beyond the default behavior:

- **CAS Mode**: When using BlobCache as Content Addressable Storage where data loss is unacceptable
- **Strong Durability**: When every Put must be durable before returning to the caller
- **Delete Support**: WAL enables explicit Delete operations with crash-consistent tombstones

Without WAL, BlobCache operates as a high-performance cache where data in the MemTable may be lost on crash (data already flushed to segments is always durable).

### 12.3 Group Commit Architecture

WAL uses a group commit strategy that amortizes the cost of `fsync` across multiple concurrent writers:

```text
CONCURRENT WRITERS                    LEADER ELECTION
      |                                     |
      v                                     v
+-------------+    +-------------+    +-----------+
| Writer A    |--->| pending     |--->| writerBusy|
| Writer B    |    | []*request  |    | (atomic)  |
| Writer C    |    +-------------+    +-----------+
+-------------+          |                  |
      |                  v                  |
      |         +----------------+          |
      |         | Leader flushes |<---------+
      |         | entire batch   |
      |         +----------------+
      |                  |
      v                  v
+-------------+    +-------------+
| sync.Cond   |    | writev()    |
| Wait()      |    | fdatasync() |
+-------------+    +-------------+
      |                  |
      v                  v
+----------------------------------+
|    All writers in batch return   |
+----------------------------------+
```

**Key Implementation Details:**

1. **Double-Buffered Queues (Ping-Pong)**: Two pre-allocated slices (`pending` and `flushing`) are swapped atomically, eliminating allocation during the hot path
2. **Leader Election**: A single `writerBusy` atomic bool ensures only one goroutine performs I/O at a time
3. **sync.Cond Signaling**: Waiters use `sync.Cond` instead of per-request channels, reducing allocation overhead
4. **net.Buffers (writev)**: All records are encoded into `net.Buffers` for gathered I/O, reducing syscall overhead from N writes to ~1 writev call

### 12.4 WAL File Format & Hardened Record Format (v2)

WAL entries use the **unified `record.Record` format**, enabling shared encoding/decoding code with segment files. The v2 format introduces a **Header Checksum** to prevent "allocate-on-corrupt-size" panics.

**Record Layout (42 bytes fixed header + variable payload):**

```text
[Magic:4] [HeaderCRC:4] [Flags:8] [SeqID:8] [KeyLen:2] [PhysSize:8] [LogSize:8] [Key...] [Value...]
```

| Field | Size | Description |
|-------|------|-------------|
| Magic | 4 bytes | `0xB10BCAFE` - identifies valid records |
| HeaderCRC | 4 bytes | CRC32 IEEE of the following 34 bytes (Flags..LogSize) |
| Flags | 8 bytes | Compression, status, CRC32 of payload |
| SeqID | 8 bytes | Monotonic sequence ID |
| KeyLen | 2 bytes | Key length in bytes |
| PhysSize | 8 bytes | Physical (on-disk) value size |
| LogSize | 8 bytes | Logical (uncompressed) value size |

**Safety Invariant:** Readers MUST verify HeaderCRC before trusting PhysSize to allocate memory.

**File Layout:**

```text
+-----------------------------------------------------------+
| File Header (32 bytes)                                    |
|   Magic: "BLOBWAL1" (8 bytes)                             |
|   Version: 1 (4 bytes)                                    |
|   CreatedAt: UnixNano (8 bytes)                           |
|   Flags: Reserved (4 bytes)                               |
|   HeaderCRC: CRC32 (4 bytes)                              |
|   Padding (4 bytes)                                       |
+-----------------------------------------------------------+
| Record 1 (42-byte header + key + value)                   |
+-----------------------------------------------------------+
| Record 2 ...                                              |
+-----------------------------------------------------------+
| Record N                                                  |
+-----------------------------------------------------------+
```

**File Naming:**

WAL files are named by the **first SeqID** written to them, using 20-digit zero-padding:
- `wal-00001736489723456789.log` (covers SeqID 1736489723456789 through rotation)

This naming scheme provides:
- Natural ordering for recovery (files sort by sequence)
- 1:1 pairing with MemTable slabs (each slab rotation creates a new WAL file)
- Simple cleanup (delete WAL file when corresponding slab is flushed to segment)

### 12.5 Crash-Only Initialization

BlobCache uses "crash-only" initialization for WAL recovery. Rather than maintaining complex state about partially-recovered data, the system fully recovers WAL entries, flushes them to segments, and then restarts cleanly:

```text
New() {
    for attempt := 1; attempt <= 2; attempt++ {
        cache, recovered := open()

        if !recovered {
            return cache  // Clean start
        }

        // WAL recovery happened - close and restart fresh
        cache.Close()
    }

    // If WAL still exists after 2 attempts, something is wrong
    return error("WAL files still present")
}
```

**Recovery Flow:**

1. **Scan Segments**: Compute `checkpoint = max(segment.MaxSeqID)` across all committed segments
2. **Filter WAL Files**: For each WAL file, if `firstSeqID <= checkpoint`, delete it (already flushed)
3. **Replay Records**: For uncommitted WAL files, replay each record through `ReplayRecord()`
4. **Flush After Each File**: Trigger a flush after replaying each WAL file, ensuring 1:1 slab/WAL correspondence
5. **Close and Restart**: After recovery, close the cache and restart for a clean state

This two-attempt loop guarantees:
- No "mixed mode" initialization with partially-recovered state
- Simple, predictable behavior after any crash scenario
- WAL files are deleted through the normal flush path

### 12.6 Write Path Integration: The Reserve-First Pattern

WAL writes use a **Reserve-First** pattern that prevents the "Spillover Bug" while keeping mmap syscalls outside the critical section:

```text
writeToSlab(rec) {
    // 1. LOCK: Reserve space and state
    mu.Lock()
    if seqID <= maxSealedSeq { return errSequenceTooOld }  // Lifecycle Guard
    buf, wPos = active.Alloc(rec.Size())                   // Reserve position
    if buf == nil { rotate(); retry }                      // Rotation if full
    active.pendingWrites++
    mu.Unlock()

    // 2. UNLOCKED: WAL write (I/O outside lock)
    walPos = wal.Write(rec)  // Blocks until fdatasync

    // 3. UNLOCKED: Fill reserved buffer
    rec.EncodeTo(buf)

    // 4. LOCK: Index update (sharded lock, not global)
    indexLocks[shard].Lock()
    if seqID > existing.SeqID { index.Put(key, entry) }
    indexLocks[shard].Unlock()
}
```

**Why Reserve-First (not WAL-First)?**

The naive approach (WAL write → slab allocation) causes the **Spillover Bug**:
1. Writer A writes record to WAL file N
2. Slab rotates (WAL file N is closed)
3. Writer A's slab allocation lands in the NEW slab (associated with WAL file N+1)
4. On flush, WAL file N is deleted, but Writer A's data was in WAL N
5. **Data loss**: The record exists only in WAL N, which was deleted

Reserve-First guarantees that a record's WAL file and slab are always paired:
- Position reserved under lock → rotation cannot happen mid-write
- WAL write happens to the file associated with the reserved slab
- Flush deletes WAL file only after segment contains all its records

**Critical Section Optimization:**

The mmap syscall for XL buffers happens **outside** the lock:
```text
mu.Lock()
wPos = active.AlignPosToPageBoundary()  // Reserve position only
active.xlSize += estimatedSize           // Reserve quota
mu.Unlock()

xlBuf = NewMmapBuffer(...)  // mmap syscall outside lock
buf = xlBuf.raw[FileHeaderSize:]
```

This prevents kernel VMA contention from blocking all writers.

### 12.7 Flush Strategies: Rename vs Merge

The flush path diverges based on whether WAL is enabled, reflecting two fundamentally different strategies:

**`flushViaRename` (WAL Mode):**
```text
1. Collect entries using WalPos (actual position in WAL file)
2. Sort by position
3. Rename WAL file → segment file (atomic, zero I/O)
4. Write footer to .iseg file
5. Update index
```

This is the "fast path"—the WAL file already contains all data in the correct format. A simple `rename()` syscall converts it to a segment. **Zero double-writes.**

**`flushViaMerge` (Cache Mode):**
```text
1. Collect entries using Pos (slab buffer position)
2. Collect XL buffer references
3. Sort both by (Pos, SeqID)
4. Adjust positions for XL interleaving
5. Write segment with XL payloads merged at insertion points
6. Write footer to .iseg file
7. Update index
```

This path physically constructs the segment by interleaving the main slab buffer with XL buffers at their reserved positions.

**Why Two Strategies?**

| Aspect | flushViaRename | flushViaMerge |
|--------|----------------|---------------|
| I/O | 0 (rename only) | 1× (construct file) |
| XL Handling | WAL handles internally | Merge at insertion points |
| Complexity | Simple | Position adjustment required |
| Use Case | Durable mode | Cache mode |

The split makes each path's logic explicit and avoids conditional spaghetti in a single function.

### 12.8 WAL File Lifecycle

WAL files are deleted through the normal flush path, not during recovery:

```text
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  WAL File   │     │ ActiveSlab  │     │  Segment    │
│  (SeqID X)  │     │ (slabID=X)  │     │  (flushed)  │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       │   1. Records      │                   │
       │   written to      │                   │
       │   both WAL        │                   │
       │   and slab        │                   │
       │                   │                   │
       │              2. Slab rotates          │
       │                   │                   │
       │              3. Flush worker          │
       │                   │ writes segment    │
       │                   ├──────────────────>│
       │                   │                   │
       │              4. Index updated         │
       │                   │                   │
       │<──────────────────┤                   │
       │   5. WAL file     │                   │
       │   deleted         │                   │
       │                   │                   │
```

This design ensures:
- WAL files are only deleted after data is durably in a segment
- No explicit coordination needed during recovery
- Simple crash semantics: WAL files exist = data needs recovery

### 12.9 Performance Characteristics

**Benchmark Results (1M ops, m7gd.8xlarge, 32 vCPUs, NVMe):**

| Metric | Value |
|--------|-------|
| Logical Throughput | 1.1 GB/s sustained |
| Physical Write | 3.4 GB/s |
| Write Amplification | 1.00 (pre-eviction) |
| Peak RSS | 5.76 GB (stable throughout) |
| GET p50 / p99 / p999 | 7.8µs / 127µs / 554µs |
| PUT p50 / p99 / p999 | 28ms / 54ms / 62ms |

**Performance vs. Naive Implementation:**

| Approach | Throughput | Write Amp | Notes |
|----------|------------|-----------|-------|
| Naive (WAL + separate flush) | ~0.5 GB/s | 2.0× | Double-write: WAL then segment |
| **Unified (WAL rename)** | **1.1 GB/s** | **1.0×** | Zero double-write |
| Cache mode (no WAL) | ~1.2 GB/s | 1.0× | Baseline (no durability) |

The WAL rename strategy achieves **~90% of cache-mode throughput** while providing full durability—a 10-15% overhead vs. the 50%+ overhead of naive implementations.

**Recovery Performance:**

| Scenario | Impact | Notes |
|----------|--------|-------|
| Recovery time | O(WAL size) | Linear scan, ~1 GB/s replay |
| High concurrency | ~10-15% overhead | fsync amortized via group commit |
| Single writer | ~20-30% overhead | Less batching opportunity |

**Configuration:**

```go
// Enable WAL with default settings (O_DIRECT + fdatasync after each batch)
cache, _ := blobcache.New(path, blobcache.WithWAL())

// Full durability (fsync - includes metadata)
cache, _ := blobcache.New(path, blobcache.WithWAL(), blobcache.WithWALFlags(sys.FlDirectIO|sys.SyncFull))

// Testing only (no sync, no direct I/O)
cache, _ := blobcache.New(path, blobcache.WithWAL(), blobcache.WithWALFlags(sys.SyncNone))
```

---
