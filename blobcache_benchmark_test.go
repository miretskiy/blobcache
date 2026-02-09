package blobcache

import (
	"context"
	crand "crypto/rand" // Aliased to avoid collision with math/rand/v2
	"fmt"
	"math/rand/v2"
	"net/http"
	_ "net/http/pprof" // Register pprof handlers
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/miretskiy/blobcache/internal/index"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/process"
)

// --- WORKLOAD CONFIGURATION & BENCHMARKING STRATEGY ---
//
// Benchmark Semantics:
//
//	-benchtime=XXXx means "perform XXX writes" (each ~1MB on average)
//	Each write iteration includes interspersed reads (40% writes, 60% reads)
//
// Example workloads:
//
//	-benchtime=10000x     →  10,000 writes  ≈ 10GB
//	-benchtime=100000x    →  100,000 writes ≈ 100GB
//	-benchtime=256000x    →  256,000 writes ≈ 256GB
//	-benchtime=1000000x   →  1M writes      ≈ 1TB (tests eviction + hole punching)
//
// Write/Read Distribution (40/40/10/10 - write-heavy to saturate NVMe):
//
//	40% Write (new data)
//	40% Hot Read (Zipfian: 10% of cache is hot)
//	10% Cold Read (sequential scan pattern)
//	10% Miss (negative lookups, tests bloom filter)
//
// -------------------------------------------------------------------------
const (
	WriteWeight    = 40
	HotReadWeight  = 40
	ColdReadWeight = 10

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
	defer func() {
		os.RemoveAll(tmpDir)
	}()

	// lo/high markers for blob size
	const blobSizeLo = 100_000
	const blobSizeHiRng = 1_900_000

	// Toggle DirectIO via environment variable for A/B testing
	directIO := os.Getenv("BLOBCACHE_BUFFERED_IO") != "1"

	cache, err := New(tmpDir,
		WithMaxSize(400<<30),
		WithWriteBufferSize(64<<20),
		WithMaxInflightSlabs(32),
		WithMaxCachedSlabs(64),
		WithFlushConcurrency(2),
		WithDirectIOWrite(directIO),
		// WithWAL(),
		WithFDataSync(true),
		WithDegradedMode(DegradedPanic), // Crash on errors during benchmarks
	)
	if err != nil {
		b.Fatal(err)
	}
	cache.Start()
	defer cache.Close()

	entropy := make([]byte, 32<<20)
	crand.Read(entropy)

	var (
		numReads, numFound, totalWriteBytes, totalReadBytes atomic.Int64
		writeHead                                           atomic.Uint64
		workerID                                            atomic.Int64

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
		if err := cache.Put(k, entropy[:blobSize]); err != nil {
			b.Fatal(err)
		}
		warmupBytes += int64(blobSize)
	}
	cache.Drain()
	warmupThroughput = (float64(warmupBytes) / (1024 * 1024 * 1024)) / time.Since(warmupStart).Seconds()
	writeHead.Store(WarmupKeys)

	// --- SYSTEM MONITOR (Background Heartbeat) ---
	ctx, cancel := context.WithCancel(context.Background())
	metricsChan := startSystemMonitor(ctx, &totalWriteBytes, &totalReadBytes, &cache.approxSize, &cache.memTable.PadBytes, tmpDir)

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

		// Hoist closure outside loop to avoid per-iteration allocation.
		// Touch memory to prove we got it, track logical read bytes.
		viewFn := func(b []byte) {
			if len(b) > 0 {
				_ = b[0]
				totalReadBytes.Add(int64(len(b)))
			}
		}

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
					if err := cache.Put(k, entropy[offset:offset+blobSize]); err != nil {
						b.Fatal(err)
					}
					totalWriteBytes.Add(int64(blobSize))
					localPut.RecordValue(time.Since(start).Nanoseconds())
					dataWritten = true // Exit inner loop, count this iteration
				} else if op < HotReadBound {
					// Target newest keys so hot reads hit the Librarian (recent slabs)
					zipfVal := zipf.Uint64() % maxID
					id := maxID - 1 - zipfVal
					k := fastFormatKey(keyBuf, "key-", id)
					found := cache.View(k, viewFn)
					localGet.RecordValue(time.Since(start).Nanoseconds())
					numReads.Add(1)
					if found {
						numFound.Add(1)
					}
				} else if op < ColdReadBound {
					baseID := rng.Uint64() % (maxID - 4)
					for i := uint64(0); i < 4; i++ {
						k := fastFormatKey(keyBuf, "key-", baseID+i)
						cache.View(k, viewFn)
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

func BenchmarkBlobCacheLookupMemory(b *testing.B) {
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
		WithWriteBufferSize(64<<20),
		WithMaxInflightSlabs(32),
		WithMaxCachedSlabs(64),
		WithFlushConcurrency(2),
		WithDirectIOWrite(directIO),
		// WithWAL(),
		WithFDataSync(true),
		WithDegradedMode(DegradedPanic), // Crash on errors during benchmarks
	)
	if err != nil {
		b.Fatal(err)
	}
	cache.Start()
	defer cache.Close()

	entropy := make([]byte, 32<<20)
	crand.Read(entropy)

	var (
		numReads, numFound atomic.Int64
		workerID           atomic.Int64

		mu sync.Mutex
		// HDR Range: 10ns to 10s. Nanoseconds are required for M4 Max resolution.
		globalGet = hdrhistogram.New(10, 10_000_000_000, 3)
	)

	// --- BOOTSTRAP PHASE ---
	const warmupKeys = 5000
	fmt.Printf(">>> Warmup: Writing %d keys to reach steady-state...\n", warmupKeys)
	var warmupBytes int64

	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(42)))
	for i := 0; i < warmupKeys; i++ {
		blobSize := blobSizeLo + rng.IntN(blobSizeHiRng)
		k := fastFormatKey(make([]byte, 32), "key-", uint64(i))
		if err := cache.Put(k, entropy[:blobSize]); err != nil {
			b.Fatal(err)
		}
		warmupBytes += int64(blobSize)
	}
	// Important: Do not drain the cache; keep things in memory.

	// Reinterpret b.N: each iteration = one write
	// e.g., -benchtime=1000000x means 1M writes (~1TB at 1MB/write)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		wid := workerID.Add(1)
		rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(wid)))

		keyBuf := make([]byte, 64)
		localGet := hdrhistogram.New(10, 10_000_000_000, 3)

		// Hoist closure outside loop to avoid per-iteration allocation.
		viewFn := func(b []byte) {
			if len(b) > 0 {
				_ = b[0]
			}
		}

		for pb.Next() {
			// Target newest keys so hot reads hit the Librarian (recent slabs)
			id := rng.IntN(warmupKeys)
			k := fastFormatKey(keyBuf, "key-", uint64(id))
			start := time.Now()
			found := cache.View(k, viewFn)
			localGet.RecordValue(time.Since(start).Nanoseconds())
			numReads.Add(1)
			if found {
				numFound.Add(1)
			}
		} // End pb.Next()

		mu.Lock()
		globalGet.Merge(localGet)
		mu.Unlock()
	})

	cache.Drain()
	b.StopTimer()
	fmt.Printf("\n--- FINAL LATENCY (clat) REPORT (ns) ---\n")
	reportLatency(b, "GET", globalGet)
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
	ctx context.Context, logicalWriteBytes, logicalReadBytes, liveSizeBytes, padBytes *atomic.Int64, cachePath string,
) <-chan SystemMetrics {
	out := make(chan SystemMetrics, 1)
	go func() {
		var (
			maxRSS, totalQD float64
			samples         int
			interval        = 30 * time.Second
			ticker          = time.NewTicker(interval)
			proc, _         = process.NewProcess(int32(os.Getpid()))
			v1, _           = disk.IOCounters()
			prevLogWrite    = logicalWriteBytes.Load()
			prevLogRead     = logicalReadBytes.Load()
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

				// 2. IO Pressure (Queue Depth) - only count physical devices (nvme*, sd*)
				v2, _ := disk.IOCounters()
				var currentQD, physWriteBytes, physReadBytes float64
				for name, stat2 := range v2 {
					// Only count physical devices, skip dm-*/loop* to avoid double-counting
					if !strings.HasPrefix(name, "nvme") && !strings.HasPrefix(name, "sd") {
						continue
					}
					if stat1, ok := v1[name]; ok {
						intervalMs := float64(interval.Milliseconds())

						// Guard against uint64 underflow: counters are monotonic,
						// but stale v1 entries or device re-enumeration can cause
						// stat2 < stat1, wrapping the unsigned subtraction.
						if stat2.WeightedIO >= stat1.WeightedIO && stat2.WeightedIO-stat1.WeightedIO > 0 {
							currentQD += float64(stat2.WeightedIO-stat1.WeightedIO) / intervalMs
						} else if stat2.IoTime >= stat1.IoTime {
							currentQD += float64(stat2.IoTime-stat1.IoTime) / intervalMs
						}

						physWriteBytes += float64(stat2.WriteBytes - stat1.WriteBytes)
						physReadBytes += float64(stat2.ReadBytes - stat1.ReadBytes)
					}
				}

				// 3. Physical disk usage (actual blocks, reflects hole punching + compaction)
				var physicalSize int64
				_ = filepath.Walk(cachePath, func(_ string, info os.FileInfo, err error) error {
					if err == nil && !info.IsDir() {
						if stat, ok := info.Sys().(*syscall.Stat_t); ok {
							physicalSize += stat.Blocks * 512
						}
					}
					return nil
				})

				// 4. Performance Throughput
				currLogWrite := logicalWriteBytes.Load()
				currLogRead := logicalReadBytes.Load()
				physWriteTP := (physWriteBytes / (1 << 30)) / interval.Seconds()
				physReadTP := (physReadBytes / (1 << 30)) / interval.Seconds()
				logWriteTP := (float64(currLogWrite-prevLogWrite) / (1 << 30)) / interval.Seconds()
				logReadTP := (float64(currLogRead-prevLogRead) / (1 << 30)) / interval.Seconds()

				// 5. System Safety Check
				usage, _ := disk.Usage(cachePath)
				freeGB := float64(usage.Free) / (1 << 30)

				// 6. Cache accounting
				writtenGB := float64(logicalWriteBytes.Load()) / (1 << 30)
				onDiskGB := float64(physicalSize) / (1 << 30)
				liveGB := float64(liveSizeBytes.Load()) / (1 << 30)
				padMB := float64(padBytes.Load()) / (1 << 20)
				diskRatio := 0.0
				if writtenGB > 0 {
					diskRatio = onDiskGB / writtenGB
				}
				padPct := 0.0
				if writtenGB > 0 {
					padPct = (padMB / 1024) / writtenGB * 100
				}

				fmt.Printf("\n[HEARTBEAT %s]\n"+
					"  MEM:   RSS: %.2fGB\n"+
					"  DISK:  IO Depth: %.2f | Phys-Read: %.2f GB/s | Phys-Write: %.2f GB/s | Free: %.1fGB\n"+
					"  CACHE: Written: %.2fGB | OnDisk: %.2fGB | Live: %.2fGB | Ratio: %.2f | Pad: %.1fMB (%.2f%%)\n"+
					"  TPUT:  Log-Write: %.2f GB/s | Log-Read: %.2f GB/s\n",
					time.Now().Format("15:04:05"), rss, currentQD, physReadTP, physWriteTP, freeGB,
					writtenGB, onDiskGB, liveGB, diskRatio, padMB, padPct,
					logWriteTP, logReadTP)

				// Update states
				totalQD += currentQD
				samples++
				v1 = v2
				prevLogWrite = currLogWrite
				prevLogRead = currLogRead
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

	idx, err := index.OpenIndex(tmpDir, 0, 1_000_000)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Close()

	// Populate index directly (no I/O)
	rng := rand.New(rand.NewPCG(42, 100))
	const segmentSize = 2 << 30 // 2GB per segment

	var (
		currentSegID   uint32 = 1
		currentSegSize uint32
		batch          []index.Item
	)

	for i := 0; i < b.N; i++ {
		// Randomized blob size: 100KB to 2MB (not page-aligned)
		blobSize := uint32(100_000 + rng.IntN(1_900_000))

		batch = append(batch, index.Item{
			Key:         index.Key{Lo: uint64(i), Hi: 0},
			Offset:      currentSegSize,
			PhysicalLen: blobSize,
		})
		currentSegSize += blobSize

		// Flush segment when it reaches 2GB
		if currentSegSize >= segmentSize {
			idx.AddSegment(0, batch)
			currentSegID++
			currentSegSize = 0
			batch = batch[:0]
		}
	}

	// Flush final partial segment
	if len(batch) > 0 {
		idx.AddSegment(0, batch)
	}

	// Mark some as visited to exercise Sieve skipping logic
	for i := 0; i < b.N; i += 7 {
		idx.Get(index.Key{Lo: uint64(i), Hi: 0})
	}

	b.Logf("Index populated: %d entries across %d segments", b.N, currentSegID)

	// Measure Sieve victim identification
	b.ResetTimer()
	start := time.Now()

	var totalBytes int64
	for i := 0; i < b.N; i++ {
		victims := idx.Evict(1, 0) // Anchor only, no bystanders — pure SIEVE speed
		if len(victims) == 0 {
			b.Fatalf("Eviction failed at %d/%d: empty", i, b.N)
		}
		totalBytes += int64(victims[0].PhysicalLen)
	}

	elapsed := time.Since(start)

	b.ReportMetric(elapsed.Seconds(), "scan-sec")
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "victims/sec")
	b.ReportMetric(float64(totalBytes)/(1<<30), "identified-GB")

	b.Logf("Identified %d victims (%.2f GB worth) in %.3f sec → %.0f victims/sec",
		b.N, float64(totalBytes)/(1<<30), elapsed.Seconds(),
		float64(b.N)/elapsed.Seconds())
}

func init() {
	go func() {
		_ = http.ListenAndServe("localhost:6060", nil)
	}()
}
