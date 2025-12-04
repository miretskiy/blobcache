# DirectIO Investigation and Debugging Summary

## Performance Findings

### M4 Max (local SSD)
- **Without DirectIO**: 2.88 GB/s initial, drops to 10-20 MB/s after SLC cache exhaustion (~40-50GB)
- **With DirectIO**: Consistent ~10 GB/s, eliminates SLC cache exhaustion cliff
- **Improvement**: 3-4x faster sustained performance

### Graviton2 (striped NVMe /instance_storage)
- **Without DirectIO**: ~0.54 GB/s
- **With DirectIO**: ~0.49 GB/s
- **Conclusion**: DirectIO provides no benefit, slightly slower

### Mixed Workload Performance (M4 Max)
- **RocksDB** (with DirectIO): 0.80 GB/s, 50% hit rate
- **blobcache** (with DirectIO): 1.7-2.3 GB/s ± 54% variance, 48-49% hit rate
- **High variance**: DirectIO or filesystem effects causing inconsistent performance

## DirectIO Alignment Optimization

### Initial Approach
- Allocated single large aligned buffer (1GB) per memtable
- Copied all values into buffer during Put()
- Wrote entire buffer with single DirectIO call

### Optimized Approach
- Check if each value is already aligned and block-sized
- Write aligned values directly (zero-copy path)
- Use small scratch buffer only for unaligned data
- **Result**: 100% of 1MB values hit fast path (aligned writes)

### Alignment Strategy
```go
// Write aligned blocks, save remainder
writeAlignedBlocks(f, buffer) -> leftover

// Final leftover padded and written
paddedSize := roundUp(len(leftover), BlockSize)
```

## Race Conditions Fixed

### Issue 1: Visibility Gap During Rotation
**Problem**: Between swapping active skipmap and adding to frozen list, data was invisible to Get()

**Fix**: Add to frozen list BEFORE swapping active (maintains visibility invariant)

### Issue 2: Multiple Concurrent Rotations
**Problem**: Multiple threads could trigger rotation simultaneously, creating duplicate frozen tables

**Fix**: Use mutex to serialize rotation, check if rotation already happened after acquiring lock

### Issue 3: Bloom Filter False Negatives
**Problem**: Atomic CAS-based bloom filter had visibility issues causing false negatives

**Fix**: Added RWMutex protection to bloom filter Add() and Test()

### Issue 4: Benchmark Counter Race
**Problem**: writeCounter incremented before Put() completed, causing reads of non-existent keys

**Fix**: Use completedWrites counter incremented AFTER Put() returns

## Design Evolution

### Original Design (Broken)
- Dual data structures: skipmap + large aligned write buffer
- activeSize tracked separately from writeBufPos (race prone)
- Multiple atomics not synchronized as unit

### Current Design (Correct)
```go
type memFile struct {
    data *skipmap.StringMap[[]byte]
    size atomic.Int64  // Single atomic unit
}

type MemTable struct {
    active atomic.Pointer[memFile]
    frozen struct {
        sync.RWMutex
        inflight []*memFile
        cond *sync.Cond
    }
}
```

**Key insight**: memFile bundles skipmap + size atomically, preventing split-brain state

## Index Schema Change

### Original
- PRIMARY KEY on key column
- Duplicate keys caused appender failures
- Fallback to individual inserts was slow

### Current
- No PRIMARY KEY constraint
- Regular index on key column
- Duplicates allowed (latest wins via ORDER BY ctime DESC LIMIT 1)
- **Result**: No appender failures, cleaner code

## Open Questions

### UnsafePut Performance Paradox
**Observation**: UnsafePut (no copy) is 60% SLOWER than Put (with copy)
- Put: 2.29 GB/s ± 54% variance
- UnsafePut: 1.40 GB/s ± 28% variance

**Theory**: Unknown - defies logic
**Status**: Unsolved mystery, needs investigation

### High Variance
Both Put and UnsafePut show high variance (28-54%) with deterministic workloads
- Suspect: DirectIO, filesystem caching, or SSD behavior
- Need investigation with regular buffered I/O for comparison

## Next Steps

1. **Re-test without DirectIO** - establish baseline variance with buffered I/O
2. **Investigate sharded directory approach** - avoid DuckDB lookups entirely
3. **Add proper metrics** - replace atomic counters with monitoring system
4. **Benchmark vs RocksDB** - full 2.56M operation comparison
5. **Solve UnsafePut mystery** - profile allocator/GC behavior

## TODO Items in Code

- `TODO: CRITICAL` - Add monitoring/alerting for segment file creation/write failures
- `TODO: Optimize alignment` - Further refinement of aligned write strategy
- `TODO: replace with proper metrics system` - metricsAlignedWrites, metricsUnalignedWrites
- `TODO: Re-add histogram tracking` - Latency measurements in benchmark
