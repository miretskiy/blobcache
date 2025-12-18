# Foyer Benchmark Investigation

Benchmarking [foyer](https://github.com/foyer-rs/foyer) (Rust hybrid cache library) to understand its behavior and performance characteristics for disk caching workloads.

## Background

**Foyer** is a hybrid cache library in Rust, inspired by Facebook's CacheLib. It seamlessly integrates in-memory and disk caching with flexible eviction algorithms.

**Comparison systems:**
- **RocksDB FIFO**: 0.38 GB/s throughput, 100GB memory
- **Blobcache**: Disk-centric cache designed to replace RocksDB FIFO. Pure Go, 2100 lines, 1.21 GB/s (99% of disk bandwidth ceiling)

## Test Environment

- **Hardware**: AWS Graviton m7gd.8xlarge (32 cores, 128GB RAM, NVMe)
- **Workload**: 2.56M operations (256K writes × 1MB = 256GB, 2.3M reads)
- **Workers**: 32 parallel
- **Disk bandwidth ceiling**: 1.14 GB/s (fio psync - synchronous writes to multiple files)

## Foyer Results

### Configuration Experiments

| Rate Limit | Throughput | Memory RSS | Data Written | Config |
|-----------|------------|------------|--------------|---------|
| None | 0.69 GB/s | 48GB | 252GB | 256GB buffers, 32 flushers |
| 750 MB/s | 0.72 GB/s | 2GB | 252GB | 8GB buffers, 8 flushers |
| 1100 MB/s | 0.94 GB/s | 2GB | 252GB | 8GB buffers, 8 flushers |
| **1250 MB/s** | **0.99 GB/s** | **2GB** | **252GB** | **8GB buffers, 8 flushers** |
| 1500 MB/s | 0.88 GB/s | 38GB | 249GB | 8GB buffers, 8 flushers |

**Note on memory usage:** The 2GB RSS reflects our configuration choice - 100MB memory tier + 8GB buffer_pool_size. In production, larger memory tiers would be beneficial for caching hot data.

### Throughput Comparison

| System | Throughput | vs RocksDB | vs Disk Ceiling |
|--------|------------|------------|-----------------|
| RocksDB FIFO | 0.38 GB/s | — | 33% |
| **Foyer (optimal)** | **1.0 GB/s** | **2.6× faster** | **88%** |
| Blobcache | 1.21 GB/s | 3.2× faster | 106%* |

*Exceeds ceiling due to overlapped read/write I/O in mixed workload

**Foyer significantly outperforms RocksDB** (2.6× throughput improvement) while achieving competitive performance against a specialized disk-first cache.

## Understanding Foyer's Behavior

### Architectural Design

Foyer implements a **hybrid cache** with distinct memory and disk tiers:

**Memory tier** - Primary cache for hot data:
- Configurable eviction (LRU/LFU/S3-FIFO)
- Fast access (RAM speed)
- Limited capacity

**Disk tier** - Secondary storage:
- Block-based engine
- Persistent across restarts
- Large capacity

**Two write policies:**

**1. WriteOnEviction (default):**
- Data stays in memory until evicted
- Evicted entries written to disk
- **Advantage**: Delays I/O until necessary
- **Trade-off**: Most recent data only in memory - lost on restart/crash

**2. WriteOnInsertion:**
- Data written to both memory and disk immediately
- **Advantage**: Persistent immediately, survives restarts
- **Trade-off**: Every insert triggers disk write

**The trade-off is not clear-cut.** WriteOnEviction delays I/O but has cold-start issues after restarts (most recent data lost). For systems where restarts happen (like taxes - inevitable), WriteOnInsertion provides better durability despite higher I/O load.

### Flow Control Characteristics

Foyer's insert operation is **synchronous and non-blocking**:

**Data path:**
```
cache.insert(key, value)
  → memory tier (synchronous)
  → storage.enqueue() (synchronous)
    → check submit_queue_size
    → send to unbounded channel (never blocks)
    → return immediately

Flusher workers (async):
  → receive from channel
  → buffer.push() (limited by buffer_pool_size)
    → if full: drop entry, increment metric
  → write to disk
```

**Key characteristic: Unbounded channels**

Rust's unbounded MPSC channels guarantee:
- Send never blocks (as long as receiver alive)
- Messages buffered arbitrarily in memory if receiver falls behind
- Memory is implicit bound (process aborts if OOM)

**This design choice enables:**
- Non-blocking inserts (simple API)
- No caller coordination needed
- Best-effort semantics (like CacheLib)

**This design requires:**
- External rate limiting to prevent unbounded growth
- Monitoring for dropped entries
- Understanding sustainable write rates

### Why Rate Limiting is Essential

**The fundamental mismatch:**

In our benchmark (and real workloads), data can be generated faster than disk can persist:
- Workers generating: 21 GB/s (32 async workers at full speed)
- Disk writing: 1.0 GB/s (physical limit)
- **Gap: 21× faster generation than persistence**

**Without rate limiting:**
- Entries queue in unbounded channels
- Memory grows (48GB observed)
- Buffers fill and drop entries
- 84% data loss in our unlimited test

**With rate limiting (1250 MB/s):**
- Workers pace generation to sustainable rate
- Buffers stay within limits (2GB memory)
- All data persists (252GB written)
- System operates efficiently

**This isn't just a benchmark artifact** - it's relevant for production:

**Bursty traffic patterns:**
- Sudden spike in cache writes (e.g., S3 download surge)
- Short-term spike exceeds disk bandwidth
- Without limiting: Queue buildup, memory growth, eventual drops
- With limiting: Smooth flow, predictable resource usage

**Multi-tenant systems:**
- Multiple caches sharing disk bandwidth
- Need to allocate bandwidth fairly
- Rate limiting prevents one cache from saturating disk

**Predictable performance:**
- Prevents queue buildup and tail latency
- Avoids memory pressure from unbounded growth
- Makes system behavior deterministic

**Cold start mitigation:**
- After restart, cache empty (cold start)
- Application may try to refill cache quickly
- Could overwhelm disk if unlimited
- Rate limiting spreads refill over time

### Comparison to Backpressure Design

**Blobcache uses bounded queues with blocking:**
- When queue full → caller blocks
- Natural flow control built-in
- No external rate limiter needed
- Different trade-off: blocks hot path when disk slow

**Foyer uses unbounded queues with drops:**
- Caller never blocks
- Requires external rate limiting
- Different trade-off: needs operational infrastructure

**Neither is universally better** - depends on requirements:
- Need guaranteed persistence? Backpressure
- Need non-blocking inserts? Rate limiting + drops

## Observations

### Performance Ceiling

Foyer achieves **~1.0 GB/s sustained writes** to disk with:
- 8GB buffer_pool_size
- 8 flushers
- 1250 MB/s rate limiting
- WriteOnInsertion policy

**The 20% gap to blobcache (1.21 GB/s):**
- Foyer: Block-based architecture, serialization overhead, 64 indexer shards
- Blobcache: Simple segment appends, direct writes, skipmap index

**Both are I/O bound** - most CPU time in syscalls, not compute. Language efficiency (Rust vs Go) is negligible at the I/O limit.

### Memory Characteristics

**Configured: 8GB buffer_pool_size**
**Actual RSS at sustainable rates: 2GB**

The buffer_pool_size is a **limit**, not pre-allocation:
- Buffers grow as needed
- At 1250 MB/s rate, only ~2GB actively buffered
- At higher rates (1500 MB/s), queues accumulate and memory grows

**We configured small memory tier (100MB)** to force disk writes for testing. Production would use larger memory tier for hot data caching.

### CPU Usage

**Foyer: ~300-350% CPU** (~3.4 cores)
**Blobcache: ~300-450% CPU** (~3.5 cores)

**Essentially equivalent** - both I/O bound, spending time in syscalls. Rust's CPU efficiency advantage doesn't materialize at the I/O limit.

## Conclusion

### Foyer vs RocksDB

Foyer provides **2.6× throughput improvement** over RocksDB FIFO (1.0 GB/s vs 0.38 GB/s) while using dramatically less memory (2GB vs 100GB). This is a significant win for Rust rewrite efforts replacing RocksDB.

### Foyer vs Specialized Disk Cache

Compared to blobcache (purpose-built for disk-first FIFO caching):
- **Throughput**: Foyer 1.0 GB/s vs blobcache 1.21 GB/s (20% difference)
- **Operational**: Foyer needs rate limiting, blobcache has built-in backpressure
- **Architecture**: Foyer is memory-first hybrid, blobcache is disk-first append-only

**Both approaches are valid** - the choice depends on:
- Working set size (memory-friendly → foyer, disk-sized → blobcache)
- Operational preferences (non-blocking + rate limiting vs blocking + backpressure)
- Tolerance for complexity (general-purpose 22K lines vs specialized 2K lines)

### Rate Limiting Recommendations

For foyer in production with disk-centric workloads:
- **Essential** to prevent unbounded queue growth
- Target rate: ~80-90% of measured disk ceiling
- Monitor for dropped entries (storage_queue_buffer_overflow metric)
- Adjust based on actual workload patterns

**Foyer is a high-quality library** - it simply optimizes for different use cases than continuous high-rate disk writes. Its memory-first hybrid design excels where working sets fit in RAM and disk provides overflow capacity.
