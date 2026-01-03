# BlobCache Design Document

## 1. Executive Summary
BlobCache is a specialized storage engine optimized for high-throughput, append-heavy blob workloads (100KB–10MB). By bypassing the kernel’s page cache and implementing a custom user-space memory hierarchy, BlobCache provides predictable performance and maximizes NVMe bandwidth while maintaining a minimal CPU and GC footprint.

> **Note:** Performance claims such as "sub-millisecond write latencies" are architectural targets based on initial micro-benchmarks. **[TODO: Verify end-to-end write latency under sustained 1GB/s load]**.

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
* **Segments:** Large, append-only files (2GB+) that amortize the overhead of filesystem syscalls by packing thousands of blobs into single sequential write streams.
* **Durable Index:** A combination of a memory-resident **Skipmap** for O(log N) retrieval and an append-only **Bitcask-style log** for durable restartability and birth-order tracking.

### 2.4 Intelligent Reclamation: Sieve & Hole Punching
Unlike simple FIFO caches that must delete entire files to reclaim space, BlobCache uses the **Sieve Algorithm** coupled with **Physical Hole Punching**.

* **Sieve Eviction:** When the cache hits `MaxSize`, the **Sieve algorithm** identifies victim blobs in RAM. This provides a "cache-conscious" eviction policy that is significantly faster and more precise than standard LRU for high-volume blob traffic.
* **Durable Commitment:** Victims are first unlinked from the Skipmap and then committed to the durable Bitcask log. This "Commit Phase" ensures that an eviction is never lost across a system restart.
* **Hole Punching:** Once the metadata is secure, the system utilizes `fallocate(FALLOC_FL_PUNCH_HOLE)` to reclaim the physical blocks of evicted blobs directly from the middle of immutable segments.

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

### 4.1 High-Speed Lookup (Skipmap)
At the core of the Index is a **Lock-Free Skipmap** (`skipmap.Uint64Map`).
* **Why Skipmap?** Standard Go maps require a global mutex or complex sharding for high concurrency. The skipmap allows multiple goroutines to perform O(log N) lookups and insertions simultaneously without heavy lock contention.
* **The Pointer Handshake:** The skipmap stores a pointer to a `*node`. This node is shared between the lookup map and the eviction policy, ensuring that "visited" bits and metadata updates are visible to both structures instantly.



### 4.2 Durable Metadata (Bitcask Persistence)
While blobs are stored in large Segment files, their metadata is stored in a **Bitcask-style log**. This allows for atomic updates and fast recovery.

* **Composite Keys:** Persistence uses a 16-byte BigEndian key: `[SegmentID (8 bytes)][Sequence (8 bytes)]`. Contiguous storage allows efficient range scans.
* **Chunked Metadata:** Batch records are chunked into 64KB entries to stay within Bitcask's `MaxValueSize` limits.
* **Atomic Transactions:** Ingestion batches and eviction sets are committed via transactions, preventing index pointers from referencing non-durable data.

### 4.3 The Sieve Eviction Policy
BlobCache implements the **Sieve Algorithm** (a modern "Cache-Conscious" alternative to LRU) to manage the RAM footprint.

**The Sieve Data Structure**
The `sievePolicy` maintains a doubly-linked FIFO list of nodes. Unlike LRU, Sieve only sets a boolean `visited` flag via an atomic operation.

```go
type node struct {
entry   Entry
visited atomic.Bool
next    *node
prev    *node
}
```

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

### 4.4 Memory Footprint
For a system targeting **8 Million Entries** on **1TB of NVMe**, the Resident Set Size (RSS) overhead is $\approx 1.1GB$ ($1000:1$ efficiency ratio).

**1. Per-Node Memory:** ~97 bytes (Entry struct, pointers, visited flag, skipmap overhead).
**2. Total Index Overhead:** ~800MB for Skipmap + Nodes, ~9.6MB for Bloom Filter, ~256MB for Bitcask KeyDir.

### 4.5 Start up and Crash Recovery
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

### 5.2 Short-Circuiting "Pathological" Blobs
Large blobs (e.g., 20MB in a 64MB memtable) disrupt slab efficiency.
1. **Direct Allocation:** Performs a one-off `AcquireUnpooled()` mmap.
2. **Immediate Rotation:** Treated as an independent `memFile` and sent to the flusher.
3. **RSS Hygiene:** These are `munmap`'d immediately after use, ensuring RSS returns to baseline.

### 5.3 Reference Counting & Pinning
Memory must never be reused while I/O or a user read is in progress. Naive release leads to:
1. **The Interleaving Hazard:** Overwriting data mid-write.
2. **The Munmap Crash:** Unmapping memory while the kernel is still performing a DMA transfer to the NVMe.

BlobCache uses **Go 1.24's `runtime.AddCleanup`**. The buffer only returns to the pool once the flusher is done **and** every user handle has been released.

---

## 6. The I/O Tier: Segments and Direct I/O

### 6.1 Amortizing the Syscall Tax
In a high-traffic environment, writing 10,000 blobs as individual files requires 30,000 syscalls (`open`/`write`/`close`). This involves heavy inode allocation, kernel-level locking, and file-system journaling for every small object. By packing blobs into **Segments** (2GB+), BlobCache converts thousands of random file-system metadata operations into a single sequential write stream. This reduces the "syscall tax" to near-zero and allows the NVMe controller to operate in its most efficient sequential mode.

### 6.2 Segment Footers: Defense in Depth
The **Segment Footer** is a page-aligned (4KB) block at the absolute EOF. If the primary Bitcask index is corrupted, the entire state can be reconstructed by scanning the trailing metadata of every `.seg` file.

```text
SEGMENT METADATA BLOCK (N * 4KB Aligned)
+-----------------------------------------------------------+
| Segment Header (ID, CTime)                      [16 bytes]|
+-----------------------------------------------------------+
| Blob Record 0 (Hash, Pos, Size, Flags)          [32 bytes]|
+-----------------------------------------------------------+
| Alignment Padding (Zeros)                       [Variable]|
+-----------------------------------------------------------+
| FOOTER (Fixed 20 bytes at absolute EOF):                  |
|   - Record Data Length                          [8 bytes] |
|   - Magic Number (0xB10BCA4EB10BCA4E)           [8 bytes] |
+-----------------------------------------------------------+
```

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



---

## 7. Unified Bloom Filter

### 7.1 Unified vs. Distributed Rejection
Traditional LSM-trees check a separate filter for every file ($O(N)$). BlobCache uses one **Unified Bloom Filter** ($O(1)$). A miss costs $\approx 1ns$ regardless of cache size.

```text
KEY: "blob_77"
|
+--[ Hash 1 ]--+
|              |    One 64-Byte "Block" (1 CPU Cache Line)
+--[ Hash 2 ]--|--> +-----------------------------------+
|              |    | ..1..0..1..1..0..0..1..1..0..1.. |
+--[ Hash 3 ]--+    +-----------------------------------+
```

### 7.2 False Positive Decay
Standard Bloom filters cannot handle deletions. As Sieve evicts blobs, the filter decays with "ghost" entries.
1. **Proactive Tracking:** Rebuilds when ghosts exceed 10%.
2. **Reactive Monitoring:** Rebuilds if Observed FPR spikes.
   Rebuilds are non-blocking; the system snapshots the Skipmap and swaps the filter pointer atomically.

---

## 8. Resilience: Degraded Mode
When a background I/O error occurs (e.g., `Disk Full`), BlobCache enters **Degraded Mode** to maintain availability:

1.  **Worker Halt:** Background flushers stop permanently to prevent inconsistent index states.
2.  **In-Memory FIFO Eviction:** The `MmapPool` stops blocking. Instead, the `MemTable` begins dropping the oldest unflushed memfiles from memory to make room for new `Put` calls.
3.  **Pragmatic Resilience:** In this mode, BlobCache functions as a high-speed, volatile cache. While durability is suspended, the system remains alive, serving hits for most-recent data and avoiding a complete service outage.

---

## 9. Eviction & Space Reclamation

### 9.1 The Three-Phase Eviction Lifecycle
1. **Selection (Sieve):** Victims are unlinked from the Skipmap (Logical Deletion).
2. **Commit:** Deletion is persisted to the Bitcask log (Durability).
3. **Reclamation (Hole Punching):** `fallocate(FALLOC_FL_PUNCH_HOLE)` reclaims physical SSD blocks without rewriting immutable segments.

```text
SEGMENT FILE STRUCTURE (Physical View)
4KB Blocks: | B1 | B2 | B3 | B4 | B5 | B6 | B7 | B8 |

    LOGICAL BLOB:      [======= BLOB B =======]
    (Un-aligned)          ^               ^
                          |               |
                    Starts mid-B2    Ends mid-B6
    
    ACTION: fallocate(PUNCH_HOLE, B3_Start, B5_End)
    
    RESULTING DISK STATE:
    | B1 | B2 |  HOLE (B3-B5)  | B6 | B7 | B8 |
```

### 9.2 Sparse Segment Compaction
Hole punching creates "Swiss Cheese" segments—files that remain physically large on disk but contain mostly empty space.
* **The Compaction Ticker:** A background task periodically calculates the "Fullness Percentage" ($LiveBytes / TotalPhysicalSize$).
* **Migration:** Segments falling below a threshold (e.g., 20%) are marked for compaction. Remaining live blobs are read and re-inserted into the `MemTable` as new `Put()` operations.
* **Recycling:** Once live blobs are safely persisted in new, dense segments, the old sparse segment is physically deleted. This ensures long-term disk efficiency and maximizes NVMe storage utilization.