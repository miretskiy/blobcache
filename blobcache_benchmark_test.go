package blobcache

import (
	"context"
	crand "crypto/rand" // Aliased to avoid collision with math/rand/v2
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	
	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/miretskiy/blobcache/index"
	"github.com/miretskiy/blobcache/metadata"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/process"
)

// --- WORKLOAD CONFIGURATION & BENCHMARKING STRATEGY ---
//
// Benchmark Semantics:
//
//	-benchtime=XXXx means "perform XXX writes" (each ~1MB on average)
//	Each write iteration includes interspersed reads (10% writes, 90% reads)
//
// Example workloads:
//
//	-benchtime=10000x     →  10,000 writes  ≈ 10GB
//	-benchtime=100000x    →  100,000 writes ≈ 100GB
//	-benchtime=256000x    →  256,000 writes ≈ 256GB
//	-benchtime=1000000x   →  1M writes      ≈ 1TB (tests eviction + hole punching)
//
// Write/Read Distribution:
//
//	10% Write (new data)
//	40% Hot Read (Zipfian: 10% of cache is hot)
//	25% Cold Read (sequential scan pattern)
//	25% Miss (negative lookups, tests bloom filter)
//
// -------------------------------------------------------------------------
const (
	WriteWeight    = 10
	HotReadWeight  = 40
	ColdReadWeight = 25
	
	WriteBound    = WriteWeight
	HotReadBound  = WriteBound + HotReadWeight
	ColdReadBound = HotReadBound + ColdReadWeight
	
	WarmupKeys  = 10000
	ReadMinKeys = 5000
)

func BenchmarkBlobCache(b *testing.B) {
	tmpDir := os.TempDir() + "/bench-blobcache"
	if _, err := os.Stat("/instance_storage"); err == nil {
		tmpDir = "/instance_storage/bench-blobcache"
	}
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)
	
	// lo/high markers for blob size
	const blobSizeLo = 100_000
	const blobSizeHiRng = 1_900_000
	
	// Toggle DirectIO via environment variable for A/B testing
	directIO := os.Getenv("BLOBCACHE_BUFFERED_IO") != "1"

	cache, err := New(tmpDir,
		WithMaxSize(400<<30),
		WithWriteBufferSize(128<<20),
		WithSegmentSize(2<<30),
		WithMaxInflightSlabs(32),
		WithFlushConcurrency(6),
		WithDirectIOWrite(directIO),
	)
	if err != nil {
		b.Fatal(err)
	}
	cache.Start()
	defer cache.Close()
	
	entropy := make([]byte, 32<<20)
	crand.Read(entropy)
	
	var (
		numReads, numFound, totalWriteBytes atomic.Int64
		writeHead                           atomic.Uint64
		workerID                            atomic.Int64
		
		mu sync.Mutex
		// HDR Range: 10ns to 10s. Nanoseconds are required for M4 Max resolution.
		globalPut = hdrhistogram.New(10, 10_000_000_000, 3)
		globalGet = hdrhistogram.New(10, 10_000_000_000, 3)
	)
	
	var warmupThroughput float64
	// --- BOOTSTRAP PHASE ---
	fmt.Printf(">>> Warmup: Writing %d keys to reach steady-state...\n", WarmupKeys)
	warmupStart := time.Now()
	var warmupBytes int64
	for i := 0; i < WarmupKeys; i++ {
		blobSize := 1024 * 1024
		k := fastFormatKey(make([]byte, 32), "key-", uint64(i))
		cache.Put(k, entropy[:blobSize])
		warmupBytes += int64(blobSize)
	}
	cache.Drain()
	warmupThroughput = (float64(warmupBytes) / (1024 * 1024 * 1024)) / time.Since(warmupStart).Seconds()
	writeHead.Store(WarmupKeys)
	
	// --- SYSTEM MONITOR (Background Heartbeat) ---
	ctx, cancel := context.WithCancel(context.Background())
	metricsChan := startSystemMonitor(ctx, &totalWriteBytes, tmpDir)
	
	// Reinterpret b.N: each iteration = one write
	// e.g., -benchtime=1000000x means 1M writes (~1TB at 1MB/write)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		wid := workerID.Add(1)
		rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(wid)))
		
		// ZIPFIAN CONFIGURATION:
		// s = 1.1: The "magic number" for cache workloads. Creates a curve where
		//          top ~10-15% of keys account for ~60-70% of accesses.
		// v = 1.0: Ensures the curve starts at Rank 1 (no plateau).
		// Range = 1<<25: up to 32M keys.
		zipf := rand.NewZipf(rng, 1.1, 1.0, 1<<25)
		if zipf == nil {
			b.Fatal("Zipf nil: check s > 1.0 and v >= 1.0")
		}
		
		keyBuf := make([]byte, 64)
		localPut := hdrhistogram.New(10, 10_000_000_000, 3)
		localGet := hdrhistogram.New(10, 10_000_000_000, 3)
		
		// Each pb.Next() iteration = one write (with interspersed reads)
		for pb.Next() {
			dataWritten := false
			for !dataWritten {
				op := rng.IntN(100)
				maxID := writeHead.Load()
				
				if maxID < ReadMinKeys {
					if op < 50 {
						op = 0
					} else {
						op = 99
					}
				}
				
				start := time.Now()
				
				if op < WriteBound {
					id := writeHead.Add(1)
					k := fastFormatKey(keyBuf, "key-", id)
					blobSize := blobSizeLo + rng.IntN(blobSizeHiRng)
					offset := rng.IntN(len(entropy) - blobSize)
					cache.Put(k, entropy[offset:offset+blobSize])
					totalWriteBytes.Add(int64(blobSize))
					localPut.RecordValue(time.Since(start).Nanoseconds())
					dataWritten = true // Exit inner loop, count this iteration
				} else if op < HotReadBound {
					id := zipf.Uint64() % maxID
					k := fastFormatKey(keyBuf, "key-", id)
					found := cache.View(k, func(r io.Reader) {
						io.CopyN(io.Discard, r, 64) // Force page fault
					})
					localGet.RecordValue(time.Since(start).Nanoseconds())
					numReads.Add(1)
					if found {
						numFound.Add(1)
					}
				} else if op < ColdReadBound {
					baseID := rng.Uint64() % (maxID - 4)
					for i := uint64(0); i < 4; i++ {
						k := fastFormatKey(keyBuf, "key-", baseID+i)
						cache.View(k, func(r io.Reader) {})
					}
					numReads.Add(4)
				} else {
					k := fastFormatKey(keyBuf, "miss-", rng.Uint64())
					cache.Get(k)
					numReads.Add(1)
				}
			} // End inner loop
		} // End pb.Next()
		
		mu.Lock()
		globalPut.Merge(localPut)
		globalGet.Merge(localGet)
		mu.Unlock()
	})
	
	cache.Drain()
	b.StopTimer()
	cancel()
	
	finalMetrics := <-metricsChan
	fmt.Printf("\n--- FINAL LATENCY (clat) REPORT (ns) ---\n")
	reportLatency(b, "GET", globalGet)
	reportLatency(b, "PUT", globalPut)
	
	b.ReportMetric(warmupThroughput, "warmup-GB/s")
	b.ReportMetric(finalMetrics.PeakRSS, "Peak-RSS-GB")
	b.ReportMetric(finalMetrics.AvgUtil, "Disk-Util-%")
}

func reportLatency(b *testing.B, name string, h *hdrhistogram.Histogram) {
	p50, p99, p999 := h.ValueAtQuantile(50), h.ValueAtQuantile(99), h.ValueAtQuantile(99.9)
	fmt.Printf("%s | p50: %dns | p99: %dns | p999: %dns | max: %dns\n", name, p50, p99, p999, h.Max())
	
	prefix := "clat-" + name
	b.ReportMetric(float64(p50), prefix+"-p50-ns")
	b.ReportMetric(float64(p99), prefix+"-p99-ns")
	b.ReportMetric(float64(p999), prefix+"-p999-ns")
}
func startSystemMonitor(
		ctx context.Context, logicalBytes *atomic.Int64, cachePath string,
) <-chan SystemMetrics {
	out := make(chan SystemMetrics, 1)
	go func() {
		var (
			maxRSS, totalQD float64
			samples         int
			interval        = 10 * time.Second
			ticker          = time.NewTicker(interval)
			proc, _         = process.NewProcess(int32(os.Getpid()))
			v1, _           = disk.IOCounters()
			prevLog         = logicalBytes.Load()
		)
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				res := SystemMetrics{PeakRSS: maxRSS}
				if samples > 0 {
					res.AvgQueue = totalQD / float64(samples)
				}
				out <- res
				return
			case <-ticker.C:
				// 1. Memory Usage
				mem, _ := proc.MemoryInfo()
				rss := float64(mem.RSS) / (1 << 30)
				if rss > maxRSS {
					maxRSS = rss
				}
				
				// 2. IO Pressure (Queue Depth)
				v2, _ := disk.IOCounters()
				var currentQD, physWriteBytes float64
				for name, stat2 := range v2 {
					if stat1, ok := v1[name]; ok {
						// Calculate Delta
						weightedDelta := float64(stat2.WeightedIO - stat1.WeightedIO)
						timeDelta := float64(stat2.IoTime - stat1.IoTime)
						intervalMs := float64(interval.Milliseconds())
						
						// macOS Fix: If WeightedIO is not supported by the driver,
						// fallback to IoTime/Interval to show utilization (max 1.0 per disk).
						// If WeightedIO is present, it shows true concurrency (can be > 1.0).
						if weightedDelta > 0 {
							currentQD += weightedDelta / intervalMs
						} else {
							currentQD += timeDelta / intervalMs
						}
						
						physWriteBytes += float64(stat2.WriteBytes - stat1.WriteBytes)
					}
				}
				
				// 3. Physical vs Logical (Sparse/Hole-Punching Ratio)
				var physicalSize, logicalSize int64
				_ = filepath.Walk(cachePath, func(_ string, info os.FileInfo, err error) error {
					if err == nil && !info.IsDir() {
						logicalSize += info.Size()
						if stat, ok := info.Sys().(*syscall.Stat_t); ok {
							// Blocks are 512 bytes on almost all Unix-likes (Darwin/Linux)
							physicalSize += stat.Blocks * 512
						}
					}
					return nil
				})
				
				sRatio := 0.0
				if logicalSize > 0 {
					sRatio = float64(physicalSize) / float64(logicalSize)
				}
				
				// 4. Performance Throughput
				currLog := logicalBytes.Load()
				physTP := (physWriteBytes / (1 << 30)) / interval.Seconds()
				logicalTP := (float64(currLog-prevLog) / (1 << 30)) / interval.Seconds()
				
				// 5. System Safety Check
				usage, _ := disk.Usage(cachePath)
				freeGB := float64(usage.Free) / (1 << 30)
				
				fmt.Printf("\n[HEARTBEAT %s]\n"+
						"  MEM:   RSS: %.2fGB\n"+
						"  DISK:  IO Depth: %.2f | Phys-Write: %.2f GB/s | Free: %.1fGB\n"+
						"  SIEVE: Phys: %.2fGB | Log: %.2fGB | Ratio: %.2f | Log-TP: %.2f GB/s\n",
					time.Now().Format("15:04:05"), rss, currentQD, physTP, freeGB,
					float64(physicalSize)/(1<<30), float64(logicalSize)/(1<<30),
					sRatio, logicalTP)
				
				// Update states
				totalQD += currentQD
				samples++
				v1 = v2
				prevLog = currLog
			}
		}
	}()
	return out
}

type SystemMetrics struct {
	PeakRSS, PeakVMS, AvgUtil, AvgQueue float64
}

func fastFormatKey(buf []byte, prefix string, id uint64) []byte {
	n := copy(buf, prefix)
	return strconv.AppendUint(buf[:n], id, 10)
}

// BenchmarkEviction_SieveVictimSelection measures the pure throughput of the
// Sieve eviction algorithm's victim selection, isolated from all I/O.
//
// Usage:
//
//	-benchtime=100000x    # Scan 100k entries
//	-benchtime=1000000x   # Scan 1M entries
//
// This tests ONLY the Sieve hand scan speed (CPU-bound pointer chasing).
// Does NOT include: Bitcask sync, hole punching, or size tracking.
func BenchmarkEviction_SieveVictimSelection(b *testing.B) {
	tmpDir := b.TempDir()
	defer os.RemoveAll(tmpDir)
	
	idx, err := index.NewIndex(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()
	
	// Populate index directly (no I/O)
	rng := rand.New(rand.NewPCG(42, 100))
	const segmentSize = 2 << 30 // 2GB per segment
	
	var (
		currentSegID   int64 = 1
		currentSegSize int64
		batch          []metadata.BlobRecord
	)
	
	for i := 0; i < b.N; i++ {
		// Randomized blob size: 100KB to 2MB (not page-aligned)
		blobSize := int64(100_000 + rng.IntN(1_900_000))
		
		batch = append(batch, metadata.BlobRecord{
			Hash:        uint64(i),
			Pos:         currentSegSize,
			LogicalSize: blobSize,
		})
		currentSegSize += blobSize
		
		// Flush segment when it reaches 2GB
		if currentSegSize >= segmentSize {
			if err := idx.IngestBatch(currentSegID, batch); err != nil {
				b.Fatal(err)
			}
			currentSegID++
			currentSegSize = 0
			batch = batch[:0]
		}
	}
	
	// Flush final partial segment
	if len(batch) > 0 {
		if err := idx.IngestBatch(currentSegID, batch); err != nil {
			b.Fatal(err)
		}
	}
	
	// Mark some as visited to exercise Sieve skipping logic
	for i := 0; i < b.N; i += 7 {
		idx.Get(uint64(i))
	}
	
	b.Logf("Index populated: %d entries across %d segments", b.N, currentSegID)
	
	// Measure Sieve victim identification
	b.ResetTimer()
	start := time.Now()
	
	var totalBytes int64
	for i := 0; i < b.N; i++ {
		victim, err := idx.Evict()
		if err != nil {
			b.Fatalf("Eviction failed at %d/%d: %v", i, b.N, err)
		}
		totalBytes += victim.LogicalSize
	}
	
	elapsed := time.Since(start)
	
	b.ReportMetric(elapsed.Seconds(), "scan-sec")
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "victims/sec")
	b.ReportMetric(float64(totalBytes)/(1<<30), "identified-GB")
	
	b.Logf("Identified %d victims (%.2f GB worth) in %.3f sec → %.0f victims/sec",
		b.N, float64(totalBytes)/(1<<30), elapsed.Seconds(),
		float64(b.N)/elapsed.Seconds())
}

// BenchmarkEviction_EndToEnd measures the complete eviction pipeline including
// all I/O operations: Sieve victim selection + Bitcask metadata sync + hole punching.
//
// Usage:
//
//	-benchtime=10000x     # Evict 10k blobs (~10GB)
//	-benchtime=100000x    # Evict 100k blobs (~100GB) - recommended minimum
//	-benchtime=1000000x   # Evict 1M blobs (~1TB) - stresses SLC exhaustion
//
// This reveals the TRUE eviction throughput including all I/O bottlenecks.
func BenchmarkEviction_EndToEnd(b *testing.B) {
	tmpDir := b.TempDir()
	defer os.RemoveAll(tmpDir)
	os.RemoveAll(tmpDir)
	fmt.Fprintf(os.Stderr, "Writing to %s\n", tmpDir)
	
	cache, err := New(tmpDir,
		WithMaxSize(0), // Disable reactive eviction
		WithWriteBufferSize(128<<20),
		WithSegmentSize(2<<30))
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()
	
	// Pre-populate with b.N blobs concurrently
	entropy := make([]byte, 2<<20)
	crand.Read(entropy)
	
	b.Logf("Pre-populating %d blobs concurrently...", b.N)
	popStart := time.Now()
	
	var (
		totalBytes atomic.Int64
		wg         sync.WaitGroup
	)
	workers := runtime.NumCPU()
	blobsPerWorker := max(1, b.N/workers)
	
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(workerID)))
			
			start := workerID * blobsPerWorker
			end := min(start+blobsPerWorker, b.N)
			
			for i := start; i < end; i++ {
				// Randomized: 100KB to 2MB (not page-aligned)
				blobSize := 100_000 + rng.IntN(1_900_000)
				cache.Put([]byte(fmt.Sprintf("key-%d", i)), entropy[:blobSize])
				totalBytes.Add(int64(blobSize))
			}
		}(w)
	}
	
	wg.Wait()
	cache.Drain()
	popElapsed := time.Since(popStart)
	
	b.Logf("Populated %.2f GB in %.2f sec (%.2f GB/s)",
		float64(totalBytes.Load())/(1<<30), popElapsed.Seconds(),
		float64(totalBytes.Load())/(1<<30)/popElapsed.Seconds())
	
	// Measure disk usage before eviction
	physBefore, logBefore := measureDiskUsage(tmpDir)
	b.Logf("PRE-EVICTION:  Phys: %.2fGB, Log: %.2fGB, Ratio: %.3f",
		float64(physBefore)/(1<<30), float64(logBefore)/(1<<30),
		float64(physBefore)/float64(logBefore))
	debugDiskUsage(filepath.Join(tmpDir, "segments"))
	
	// Measure end-to-end eviction (Sieve + Bitcask + Hole Punching)
	b.ResetTimer()
	start := time.Now()
	
	var evictedBytes, physicallyReclaimed int64
	evictedHashes := make(map[uint64]struct{})
	
	type segmentDeletion struct {
		size, count int64
	}
	segmentStat := func(prefix string, segments map[int64]segmentDeletion) {
		fmt.Printf("\n=== Segment Stats: %s ===\n", prefix)
		for segment := range segments {
			sf, err := cache.storage.getSegmentFile(segment)
			if err != nil {
				b.Fatal(err)
			}
			st, err := sf.file.Stat()
			if err != nil {
				b.Fatal(err)
			}
			fmt.Printf("Segment %d: sz=%d psz=%d\n",
				segment, st.Size()>>9, st.Sys().(*syscall.Stat_t).Blocks)
		}
	}
	
	evictBatch := func(victims []index.Entry) (physical int64, logical int64) {
		segments := make(map[int64]segmentDeletion)
		for _, e := range victims {
			st := segments[e.SegmentID]
			st.size += e.LogicalSize
			st.count++
			segments[e.SegmentID] = st
		}
		
		if err := cache.index.DeleteBlobs(victims...); err != nil {
			b.Fatal(err)
		}
		
		fmt.Print("\n=== Segment Deletions ===\n")
		for s, st := range segments {
			fmt.Printf("Segment %d: deleting sz=%d cnt=%d\n", s, st.size, st.count)
		}
		segmentStat("BEFORE", segments)
		
		// Track punched regions per segment to detect overlaps
		punchedRegions := make(map[int64][]struct{ start, end int64 })
		
		for _, v := range victims {
			// Check for overlaps
			victimStart := v.Pos
			victimEnd := v.Pos + v.LogicalSize
			for _, region := range punchedRegions[v.SegmentID] {
				if victimStart < region.end && victimEnd > region.start {
					fmt.Printf("OVERLAP: Seg=%d pos[%d-%d] overlaps [%d-%d]\n",
						v.SegmentID, victimStart, victimEnd, region.start, region.end)
				}
			}
			punchedRegions[v.SegmentID] = append(punchedRegions[v.SegmentID],
				struct{ start, end int64 }{victimStart, victimEnd})
			
			reclaimed, err := cache.storage.HolePunchBlob(v.SegmentID, v.Pos, v.LogicalSize)
			if err != nil {
				b.Fatal(err)
			}
			
			// Log low reclamation
			if v.LogicalSize > 500000 && reclaimed < v.LogicalSize/10 {
				fmt.Printf("LOW RECLAIM: seg=%d pos=%d size=%d reclaimed=%d (%.1f%%)\n",
					v.SegmentID, v.Pos, v.LogicalSize, reclaimed,
					100*float64(reclaimed)/float64(v.LogicalSize))
			}
			
			physical += reclaimed
			logical += v.LogicalSize
		}
		segmentStat("AFTER", segments)
		return physical, logical
	}
	
	var victims []index.Entry
	for i := 0; i < b.N; i++ {
		victim, err := cache.index.Evict()
		if err != nil {
			b.Logf("Ran out of victims at %d/%d (normal if b.N > actual entries)", i, b.N)
			break
		}
		
		// Verify no duplicates
		if _, exists := evictedHashes[victim.Hash]; exists {
			b.Fatalf("DUPLICATE EVICTION: Hash %d evicted twice!", victim.Hash)
		}
		evictedHashes[victim.Hash] = struct{}{}
		
		victims = append(victims, victim)
		
		if len(victims) == 1024 {
			phys, logical := evictBatch(victims)
			victims = victims[:0]
			physicallyReclaimed += phys
			evictedBytes += logical
			fmt.Printf("Evicted 1024 blobs: phys=%d logical=%d %.4f\n", phys, logical,
				float64(phys)/float64(logical))
			// debugDiskUsage(filepath.Join(tmpDir, "segments"))
		}
		
		// Periodic debug: show disk state
		if i > 0 && i%25000 == 0 {
			debugDiskUsage(filepath.Join(tmpDir, "segments"))
		}
	}
	
	if len(victims) > 0 {
		phys, logical := evictBatch(victims)
		physicallyReclaimed += phys
		evictedBytes += logical
		fmt.Printf("(Last batch) Evicted %d blobs: phys=%d logical=%d %.4f\n",
			len(victims), phys, logical, float64(phys)/float64(logical))
	}
	actualEvicted := len(evictedHashes)
	
	elapsed := time.Since(start)
	throughput := float64(evictedBytes) / elapsed.Seconds() / (1 << 30)
	
	// Verify hole punching actually reclaimed space
	physAfter, logAfter := measureDiskUsage(tmpDir)
	reclaimedPhysMeasured := physBefore - physAfter
	
	b.Logf("POST-EVICTION: Phys: %.2fGB, Log: %.2fGB, Ratio: %.3f",
		float64(physAfter)/(1<<30), float64(logAfter)/(1<<30),
		float64(physAfter)/float64(logAfter))
	b.Logf("Reclaimed (syscall reported): %.2f GB (%.1f%% of evicted)",
		float64(physicallyReclaimed)/(1<<30),
		100*float64(physicallyReclaimed)/float64(evictedBytes))
	b.Logf("Reclaimed (disk measured):    %.2f GB (%.1f%% of evicted)",
		float64(reclaimedPhysMeasured)/(1<<30),
		100*float64(reclaimedPhysMeasured)/float64(evictedBytes))
	
	if physicallyReclaimed < evictedBytes/2 {
		b.Logf("WARNING: Alignment rounding losing >50%% of reclamation potential")
	}
	if reclaimedPhysMeasured < physicallyReclaimed*8/10 {
		b.Logf("WARNING: Disk measurement (%.2fGB) << syscall reported (%.2fGB)",
			float64(reclaimedPhysMeasured)/(1<<30), float64(physicallyReclaimed)/(1<<30))
		b.Logf("Filesystem may be delaying deallocation or measurement is inaccurate")
	}
	
	b.ReportMetric(elapsed.Seconds(), "eviction-sec")
	b.ReportMetric(throughput, "eviction-GB/s")
	b.ReportMetric(float64(actualEvicted)/elapsed.Seconds(), "victims/sec")
	
	b.Logf("Evicted %d unique blobs (%.2f GB) in %.2f sec → %.2f GB/s, %.0f victims/sec",
		actualEvicted, float64(evictedBytes)/(1<<30), elapsed.Seconds(),
		throughput, float64(actualEvicted)/elapsed.Seconds())
}

func debugDiskUsage(segmentsPath string) {
	var physical, logical int64
	fmt.Printf("\n=== Segment Files ===\n")
	_ = filepath.Walk(segmentsPath, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			logical += info.Size()
			if stat, ok := info.Sys().(*syscall.Stat_t); ok {
				physical += stat.Blocks * 512
				fmt.Printf("  %s: Size=%dMB, Blocks=%d (%.2fMB phys)\n",
					filepath.Base(p), info.Size()/(1<<20), stat.Blocks,
					float64(stat.Blocks*512)/(1<<20))
			}
		}
		return nil
	})
	fmt.Printf("TOTAL: Phys=%.2fGB, Log=%.2fGB, Ratio=%.3f\n",
		float64(physical)/(1<<30), float64(logical)/(1<<30),
		float64(physical)/float64(logical))
}

func measureDiskUsage(basePath string) (physical, logical int64) {
	// Only measure segment files, not db/ directory
	segmentsPath := filepath.Join(basePath, "segments")
	_ = filepath.Walk(segmentsPath, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			logical += info.Size()
			if stat, ok := info.Sys().(*syscall.Stat_t); ok {
				physical += stat.Blocks * 512 // POSIX: always 512-byte units
			}
		}
		return nil
	})
	return
}
