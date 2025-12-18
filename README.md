# BlobCache

High-performance disk cache optimized for large immutable blobs, designed to replace RocksDB FIFO compaction for append-heavy workloads.

## Overview

BlobCache is a specialized disk-first caching system that eliminates RocksDB's compaction overhead for workloads where:
- Data is write-once, read-many (immutable blobs)
- FIFO eviction is sufficient (age-based, not LRU)
- Blob sizes are large (100KB-10MB)
- High miss rates make bloom filter performance critical

**Built for:** Caching large data volumes (hundreds of GB to TB) on local NVMe to avoid repeated S3 downloads.

## Performance

**Benchmark:** 2.56M operations (256K writes × 1MB = 256GB, 2.3M reads mixed) on AWS Graviton m7gd.8xlarge

| System | Throughput | Memory | Notes |
|--------|------------|--------|-------|
| **BlobCache** | **1.21 GB/s** | 10GB | At hardware ceiling |
| RocksDB FIFO | 0.38 GB/s | 100GB | 60% I/O spent on compaction |
| Foyer (Rust) | 1.0 GB/s | 2GB | Requires rate limiting |

**Key results:**
- **3.2× faster than RocksDB** (eliminates compaction overhead)
- **10× less memory** (single bloom filter vs 455 SST bloom filters)
- **99% of disk bandwidth** (1.21 GB/s vs 1.14 GB/s fio ceiling)

## Design Highlights

**Disk-First Architecture:**
- Memory used only for write buffering (memtables)
- All data persists to NVMe segments
- Built-in backpressure (blocks when buffers full)

**Key Innovations:**
- **Unified bloom filter**: Single filter for all keys (45,000× faster negative lookups than RocksDB's per-SST filters)
- **Segment-centric**: Large sequential writes (2GB segments), no per-blob file overhead
- **Hash-based keys**: uint64 throughout (skipmap, bloom, index)
- **Degraded mode**: I/O errors trigger memory-only mode (cache remains operational)

**Simple:** 2,100 lines of Go code, minimal dependencies.

## Quick Start

```go
package main

import "github.com/miretskiy/blobcache"

func main() {
    cache, _ := blobcache.New("/data/cache",
        blobcache.WithMaxSize(100<<30),      // 100GB capacity
        blobcache.WithWriteBufferSize(1<<30), // 1GB memtables
    )
    defer cache.Close()

    // Write
    cache.Put([]byte("key"), []byte("large blob data..."))
    cache.Drain()  // Wait for disk persistence

    // Read
    reader, found := cache.Get([]byte("key"))
    if found {
        // Read from io.Reader
    }
}
```

## Production Configuration

```go
cache, _ := blobcache.New("/data/cache",
    // Capacity
    blobcache.WithMaxSize(1<<40),           // 1TB

    // Write buffering (match your workload)
    blobcache.WithWriteBufferSize(1<<30),   // 1GB memtables
    blobcache.WithMaxInflightBatches(8),    // 8GB total buffer
    blobcache.WithFlushConcurrency(4),      // 4 I/O workers

    // Storage
    blobcache.WithSegmentSize(2<<30),       // 2GB segments

    // Optional: Data integrity
    blobcache.WithChecksum(),               // CRC32 checksums
)
```

## When to Use BlobCache

### ✅ Perfect For

- **Append-heavy workloads**: Logs, metrics, events, immutable artifacts
- **Large blobs**: 100KB-10MB where bloom filters provide value
- **FIFO eviction**: Age-based eviction is sufficient
- **Disk-sized datasets**: Working set exceeds memory, needs disk persistence
- **Replacing RocksDB FIFO**: Want to eliminate compaction overhead

### ❌ Not Suitable For

- **Updates/deletes**: Append-only architecture, no point updates
- **Range queries**: Hash-based index, no ordering
- **Small values** (<10KB): RocksDB's block cache helps more
- **LRU required**: Only FIFO eviction supported

## Comparison to Alternatives

**vs RocksDB FIFO:**
- BlobCache: 3.2× faster, 10× less memory
- Eliminates compaction (all I/O for new data)
- Simpler operation (no compaction tuning)
- Use when: Append-only, FIFO OK, want performance

**vs Foyer (Rust hybrid cache):**
- BlobCache: 20% faster (1.21 vs 1.0 GB/s)
- Built-in backpressure (no rate limiting needed)
- Disk-first (vs foyer's memory-first)
- Use when: Disk-sized dataset, need sustained writes

## Architecture

**Key components:**
- **Bloom Filter**: Lock-free atomic, single unified filter
- **MemTable**: Async write buffering with skipmap
- **Index**: Durable skipmap (in-memory + Bitcask backing)
- **Segments**: Append-only files with self-describing footers
- **Eviction**: Dual-mode (reactive + background)

**Error Handling:**
- Degraded mode on I/O errors (memory-only, cache still works)
- slog-based structured logging
- Close() returns errors for proper cleanup

See [DESIGN.md](DESIGN.md) for complete architectural details.

## Development

**Requirements:**
- Go 1.21+
- Linux or macOS

**Build and test:**
```bash
go test ./...
go test -bench=Benchmark_Mixed -benchtime=2560000x
```

**Benchmarks:**
- `Benchmark_Mixed`: Real workload (10% writes, 90% reads)
- Write overhead microbenchmarks
- Integration tests with degraded mode scenarios

## Documentation

- **[DESIGN.md](DESIGN.md)**: Complete design philosophy, performance analysis, and foyer comparison
- **[foyer-bench/README.md](foyer-bench/README.md)**: Foyer investigation details

## Status

**Production Ready:** No

- Comprehensive tests (including error scenarios)
- Benchmarked on production hardware
- **Not battle-tested** (new system, December 2024)

**Recommendation:** Gradual rollout with monitoring, keep RocksDB as fallback.

## License

Copyright 2024 Yevgeniy Miretskiy

## Acknowledgments

Inspired by production needs at Datadog for efficient large-blob caching to avoid S3 round trips.
