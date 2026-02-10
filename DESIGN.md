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

### 2.4 Intelligent Reclamation: Spatial SIEVE & Segment Drain
Unlike simple FIFO caches that must delete entire files to reclaim space, BlobCache uses **Spatial SIEVE eviction** coupled with **Segment Drain** for zero-write-amplification space reclamation.

* **Spatial SIEVE Eviction:** When the cache hits `MaxSize`, the **SIEVE algorithm** identifies a cold "anchor" blob, then expands spatially to co-evict physically adjacent items in the same segment. This creates large contiguous dead regions instead of "Swiss cheese" fragmentation.
* **Durable Commitment:** Victims are marked as tombstones in the segment's `.meta` file and removed from the RAM index.
* **Segment Drain:** When a segment becomes sufficiently sparse (90%+ dead items), the remaining live items are force-evicted from the RAM index and the entire segment file is deleted. Zero write amplification — no data is rewritten, only deleted.

---

## 3. System Orchestration: The Hierarchy of Rejection

With the high-level flow established, the following sections detail the low-level mechanics that enable BlobCache to saturate NVMe bandwidth. The architecture is built on a hierarchy of "increasingly expensive" checks. Each layer is designed to protect the one below it from unnecessary work:

1.  **The Unified Bloom Filter** protects the CPU and Memory Bus from searching for keys that aren't there. Rejections occur in $\approx 1ns$.
2.  **The Durable Index (Skipmap)** protects the Physical Segments by providing exact coordinates for retrieval, ensuring we only hit the disk when a result is guaranteed.
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
|    Index (Skipmap)    |  <-- [FAST] O(log N) RAM lookup for coordinates
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

The Index is the "control plane" of BlobCache. It coordinates between sub-nanosecond RAM lookups and the Bitcask-powered durable metadata log. It is designed to be highly concurrent, crash-consistent, and memory-efficient.

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



### 4.2 Durable Metadata (Bitcask Persistence)
While blobs are stored in large Segment files, their metadata is stored in a **Bitcask-style log**. This allows for atomic updates and fast recovery.

* **Composite Keys:** Persistence uses a 16-byte BigEndian key: `[SegmentID (8 bytes)][Sequence (8 bytes)]`. Contiguous storage allows efficient range scans.
* **Chunked Metadata:** Batch records are chunked into 64KB entries to stay within Bitcask's `MaxValueSize` limits.
* **Atomic Transactions:** Ingestion batches and eviction sets are committed via transactions, preventing index pointers from referencing non-durable data.

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
       n3 node recycled (sync.Pool)    |
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

### 4.6 Start up and Crash Recovery
`NewIndex` performs a **Persistence Scan**:
1. It iterates through Bitcask using `scanAll`.
2. Decodes `SegmentRecord` chunks.
3. Populates the Skipmap and Sieve list in "Birth Order," ensuring the Sieve "Hand" is positioned correctly for immediate eviction logic upon startup.

```go
err := p.scanAll(func(seg metadata.SegmentRecord) bool {
for _, rec := range seg.Records {
if !rec.IsDeleted() {
idx.blobs.Store(rec.Hash, idx.evictor.Add(Entry{rec, seg.SegmentID}))
}
}
return true
})
```

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

This infrastructure also prepares BlobCache for future Write-Ahead Log (WAL) support. Sequence IDs embedded in WAL entries allow crash recovery to correctly skip replaying operations that were already persisted to segments, ensuring exactly-once semantics without complex coordination.

---

## 5. Memory Architecture: The User-Space Page Cache

### 5.1 MmapPool: Orchestrated Backpressure
The `MmapPool` manifests physical resource limits (e.g., 8 slabs of 128MB each). It uses Go channels to hold `*MmapBuffer`. If the channel is empty (disk I/O cannot keep up with network ingestion), the `Put()` call **blocks**. This self-regulating backpressure prevents OOM crashes.

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

**Configuration:** `WithMaxCachedSlabs(n)` controls the Librarian's capacity (default: 8 slabs ≈ 1GB).

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

This design provides a multi-gigabyte L1 cache managed as 128MB units, avoiding the overhead of managing millions of individual entries. Serving a hit from a `Cached` slab involves a simple pointer offset within the `mmap` arena, resulting in zero memory copies and minimal CPU cycles.

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

### 6.2 Segment Footers: Defense in Depth
The **Segment Footer** is a page-aligned (4KB) block at the absolute EOF. If the primary Bitcask index is corrupted, the entire state can be reconstructed by scanning the trailing metadata of every `.seg` file.

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
1. **Index Metadata:** Skipmap nodes and `Entry` structs
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
BlobCache utilizes a distributed compression model where data transformation is performed by the calling goroutine during the `Put()` operation. By offloading this burden to the ingestion threads, the system prevents background flush workers from becoming a CPU bottleneck, ensuring NVMe write saturation even under high load. This effectively increases MemTable density, as compressed payloads allow each 128MB physical slab to host a significantly larger volume of logical data before requiring a flush to disk.

To prevent wasting cycles on incompressible data, the system employs a **"1/8th Early Abort"** heuristic inspired by ZFS. The compression algorithm is provided a destination buffer exactly 12.5% smaller than the source; if the buffer is filled before the blob is fully processed, the operation is aborted and the blob is stored raw. This "savings rule" ensures CPU time is only invested in data yielding meaningful footprint reductions while signaling that the data may already be compressed or contain high entropy.

### 7.2 Per-Blob Compression and Space Reclamation
The decision to compress individual blobs rather than larger logical chunks is critical to the efficacy of the **Sieve eviction policy** and **segment drain**. Because each blob remains an independent unit of compression, the segment metadata can precisely track which items are live vs. dead, enabling accurate waste ratio calculation for drain candidate selection.

Alternative "chunked" designs were rejected because they introduce **reclamation friction**: segment drain requires knowing exactly which items are dead. If multiple blobs share a compressed block, the segment cannot be drained until every blob within that block has been evicted, artificially inflating the live-item count and delaying space reclamation.

### 7.3 Dual-Size Metadata & Zero-Allocation Reads
To maximize retrieval efficiency, the Skipmap and Segment Footer track both the **Logical (Uncompressed)** and **Physical (Stored)** sizes. This dual-size tracking enables a **Zero-Allocation** retrieval path: by knowing the logical size upfront, the system can pre-allocate a destination buffer of the exact required size, eliminating the CPU and GC overhead of dynamic buffer growth during a `Get()` request.

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
   Rebuilds are non-blocking; the system snapshots the Skipmap and swaps the filter pointer atomically.

---

## 9. Resilience: Degraded Mode
When a background I/O error occurs (e.g., `Disk Full`), BlobCache enters **Degraded Mode** to maintain availability:

1.  **Worker Halt:** Background flushers stop permanently to prevent inconsistent index states.
2.  **In-Memory FIFO Eviction:** The `MmapPool` stops blocking. Instead, the `MemTable` begins dropping the oldest unflushed memfiles from memory to make room for new `Put` calls.
3.  **Pragmatic Resilience:** In this mode, BlobCache functions as a high-speed, volatile cache. While durability is suspended, the system remains alive, serving hits for most-recent data and avoiding a complete service outage.

---

## 10. Eviction & Space Reclamation

BlobCache uses different space reclamation strategies depending on the operating mode:

- **Cache Mode:** Spatial SIEVE eviction + Segment Drain (zero write amplification)
- **WAL/CAS Mode:** SIEVE eviction + Tombstone Compaction (metadata-only maintenance)

### 10.1 Spatial SIEVE Eviction

When the cache exceeds `MaxSize`, the maintenance worker runs eviction in a loop until the cache is below the hysteresis target (93% of `MaxSize`).

Each eviction iteration has two phases:

1. **Anchor Selection (SIEVE):** The SIEVE algorithm picks the coldest item from the RAM index — this is the "anchor."
2. **Spatial Expansion:** The anchor's segment manifest is read from the in-memory cache (zero disk I/O in steady state). Items physically adjacent to the anchor are co-evicted by walking outward in both directions from the anchor's offset, picking the closer neighbor at each step. Each bystander is verified and removed atomically via `deleteIfAt` (no TOCTOU race with concurrent writes).

```text
SPATIAL EXPANSION (Walking outward from anchor)

Manifest items (sorted by offset):
  [A] [B] [C] [ANCHOR] [D] [E] [F]
               ^^^^^^^^^
               Cold victim (SIEVE picked)

Walk: D (closer), C, E, B, F, A  (alternating, closest first)
Budget: MaxBystanderBytes limits the blast radius

Result: Contiguous dead region instead of scattered holes
```

**Why Spatial Expansion?**

Without spatial expansion, SIEVE eviction creates scattered dead items across many segments ("Swiss cheese"). Each segment accumulates small holes that individually don't justify deletion. Spatial co-eviction concentrates dead items in fewer segments, accelerating their progression toward the drain threshold.

Bystanders may be warm or hot — they are evicted only because they are physically adjacent to the cold anchor. `MaxBystanderBytes` (default: `WriteBufferSize / 8`) bounds the "innocent bystander" damage. Set to 0 to disable spatial expansion (pure SIEVE).

### 10.2 Segment Drain (Cache Mode)

Segment drain is the zero-write-amplification replacement for merge compaction. When a segment is sufficiently sparse (90%+ dead items by default), the remaining live items are force-evicted from the RAM index and the entire segment file is deleted.

```text
SEGMENT DRAIN LIFECYCLE:

   Segment waste ratio crosses DrainWasteThreshold (0.90)
        |
        v
   [Cooling period check]         <-- Must be aged past Librarian cache
        |
        v
   [Exclusive segment lock]       <-- Blocks Delete() during drain
        |
        v
   [DrainSegment()]               <-- Atomic: remove each live item via deleteIfAt
        |                              (verifies segID+offset match, safe skip if relocated)
        v
   [Drop archivist FD cache]      <-- Close cached file handle
        |
        v
   [Delete .seg + .meta files]    <-- Physical space reclaimed
        |
        v
   [Update approxSize + bloom]    <-- Accounting
```

**Key Properties:**
- **Zero write amplification:** No data is rewritten. The entire file is simply deleted.
- **Cache misses are acceptable:** Drained live items (~10% or fewer) become cache misses. They were in nearly-dead segments anyway — the cost of re-fetching from origin is low compared to the benefit of reclaiming disk space.
- **Bounded burst:** At most `maxDrainsPerCycle` (4) segments are drained per maintenance pass, limiting the burst of cache misses.
- **WAL mode excluded:** WAL/CAS mode cannot tolerate data loss from drain. It uses tombstone compaction instead (see 10.5).

**Configuration:**
```go
// Default: drain segments that are 90%+ dead
cache, _ := blobcache.New(path, blobcache.WithDrainWasteThreshold(0.90))

// More aggressive: drain at 80% dead (more space reclamation, more cache misses)
cache, _ := blobcache.New(path, blobcache.WithDrainWasteThreshold(0.80))

// Disable segment drain entirely
cache, _ := blobcache.New(path, blobcache.WithDrainWasteThreshold(0))
```

### 10.3 Why Not Merge Compaction? (The copy_file_range False Start)

BlobCache initially implemented merge compaction using Linux's `copy_file_range` syscall, which uses server-side copy (reflinks on supported filesystems) to merge sparse segments into dense output without reading data into userspace. The theory was compelling: zero-copy, kernel-optimized, O_DIRECT-compatible.

**What went wrong:**

1. **Fragmented output files.** `copy_file_range` with reflinks produces output files whose physical extents mirror the source files' layout. When copying live records from a sparse segment, the output file inherits the fragmented block allocation pattern — the "holes" between live records become allocated-but-discontiguous extents rather than sequential blocks. The resulting segment is logically dense but physically fragmented, defeating the purpose of compaction.

2. **Filesystem dependence.** Reflink behavior varies dramatically across filesystems (XFS, ext4, Btrfs) and even across kernel versions. Some fall back to full data copy (2x write amplification), some refuse cross-file reflinks, some silently produce fragmented output. This made the optimization unreliable in production.

3. **Complexity tax.** Merge compaction requires: contiguity validation (Leapfrog Hazard prevention), atomic `Relocate` with CAS semantics, rate limiting to avoid saturating I/O bandwidth, cooling period coordination, and tombstone GC logic. This machinery adds ~750 lines of code and several subtle concurrency protocols.

4. **Write amplification.** Even with reflinks, merge compaction has write amplification > 0: writing the output segment footer, updating the index, and the copy syscall overhead. Segment drain achieves true 0.00x write amplification — it only deletes files.

**The insight:** For cache workloads, the remaining ~10% of live items in a sparse segment are not worth preserving. They can be re-fetched from the origin on cache miss. Segment drain exploits this by simply deleting the entire segment, trading a small increase in miss rate for zero I/O cost and zero code complexity.

### 10.4 The Deletion Model: Tombstones and Consistency

BlobCache uses a **soft delete (tombstone)** model rather than immediate removal. When `Delete(key)` is called:

**Cache Mode (no WAL):**
1. **Tombstone:** Write tombstone to segment's `.meta` file (incremental log)
2. **Mark deleted in RAM:** Set `IsDeleted()` flag in the in-memory index
3. **Update segment metadata:** Track waste ratio for drain candidate selection

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

**Why Tombstones (in WAL mode)?** Tombstones prevent the "Leapfrog Hazard"—a subtle bug that can resurrect deleted keys if compaction ever merges non-contiguous segments. See Section 10.6.

### 10.5 Tombstone Compaction (WAL/CAS Mode)

In WAL mode, tombstones accumulate as incremental appendages to `.meta` files. Over time, a segment's `.meta` file grows: `[footer block] [tombstone batch 1] [batch 2] ...`. Tombstone compaction is a metadata-only operation that collapses these appendages back into the footer entries.

**When it triggers:**
- Segment crosses `TombstoneCompactionThreshold` (100 tombstones)
- Segment has cooled past `MaxCachedSlabs + CoolingPeriodMargin` segment IDs

**What it does:**
1. Read the full manifest (footer + all tombstone batches merged)
2. Rewrite the `.meta` file with tombstones baked into footer entry flags
3. Result: smaller `.meta` file, faster startup scans, bounded metadata growth

This is purely a metadata maintenance operation — no blob data is read or written.

### 10.6 The Leapfrog Hazard and Strict Contiguity

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

## 11. The Read-Path Spectrum: Page Cache vs. Direct I/O

The optimization of the read path is a spectrum of strategies determined by the workload's access patterns. By default, BlobCache utilizes **Buffered I/O**, relying on the operating system’s decades of optimization.

### 11.1 The Default: Buffered I/O and the Kernel Page Cache
By default, any read that misses the Resident Segment Cache is satisfied via standard `pread`.
* **Mechanism:** The kernel intercepts the request and checks its own Page Cache (Unified Buffer Cache). If the data is missing, the kernel fetches it from NVMe, stores it in its own pages, and copies it into the application buffer.
* **Workload Implication:** This is the most efficient path for workloads with high temporal or spatial locality, as the kernel provides sophisticated read-ahead and prefetching "for free".
* **The "Double Tax" & Tail Latency:** Under heavy memory pressure, the kernel’s background page reclamation (kswapd) can introduce unpredictable stalls. Furthermore, data exists in both Kernel RAM and application RAM, reducing total caching capacity.

### 11.2 `WithDirectIORead`: Bypassing the Page Cache
Enabling `WithDirectIORead` uses `O_DIRECT` to bypass the kernel's Page Cache entirely.
* **High-Entropy Efficiency:** This is optimal for "Write-Once, Read-Once" workloads where data is unlikely to be requested again. It prevents the kernel from polluting its cache with one-time-use data that would otherwise evict critical system metadata.
* **Predictability:** Read latencies remain bound strictly to the hardware’s physical performance, avoiding the "stutter" of kernel-driven eviction.


### 11.3 `WithCacheAfterRead`: The Promotion Buffer
The `WithCacheAfterRead` option provides a mechanism to promote cold data into the user-space "Hot" zone without wasting memory.
* **The Circular Promotion Arena:** To avoid the fragmentation of 128MB slabs, promoted blobs are written into a dedicated `MmapBuffer` managed as a circular arena.
* **Granular Packing:** Unlike the "Sealed" write segments, this promotion buffer allows for granular packing of disparate blobs from different disk segments into a single contiguous memory region.
* **Indexing:** Once a blob is "promoted" into this RAM arena, the Skipmap is updated to point to these memory coordinates. This allows the system to serve future hits with zero-copy speed without needing a complex, sharded block-caching subsystem.

```text
[ USER GET(Key) ]
               |
               v
     +-------------------+       HIT        +--------------------+
     | Resident L1 Check | ---------------> | Return App Pointer |
     +-------------------+                  +--------------------+
               |
               | MISS
               v
     +-------------------+                  +-------------------------+
     | Option: DirectIO? | ---- NO -------> |   KERNEL PAGE CACHE     |
     +-------------------+ (Default Path)   | (Buffered I/O + Copy)   |
               |                            +-------------------------+
               | YES (O_DIRECT)                          |
               v                                         | UBC Miss
     +-------------------+                  +-------------------------+
     |   NVMe STORAGE    | <----------------|    PHYSICAL NVMe I/O    |
     +-------------------+                  +-------------------------+
               |
               | Data Returned
               v
     +-----------------------+       YES      +-----------------------+
     | Option: CacheOnRead?  | -------------> | CIRCULAR PROMO ARENA  |
     +-----------------------+                +-----------------------+
               |                  
               | NO
               v
       [ Return to User ]
```

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
| Flush Action | Bulk Write via `wal.WriteBulk` |

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
