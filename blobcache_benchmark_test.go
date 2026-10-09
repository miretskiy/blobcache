package blobcache

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/process"
)

// BenchmarkBlobCache is the primary end-to-end benchmark.
//
// Each iteration (-benchtime=Nx) is one write of 100 KB–2 MB (~1 MB average),
// interleaved with reads in this mix:
//
//	30% write    — Alloc a record buffer, fill it, Put it
//	30% hot read — Zipfian (s=1.1) over the newest keys
//	30% cold read — four consecutive keys from a uniformly random position
//	10% miss     — a key that was never written
//
// Writes download into cache memory, as a caller would: Alloc, fill (the
// benchmark's one copy, standing in for the download), Put. Reads lend the
// value to a callback that only measures it. The cache never waits for
// resources: when no memory can be reclaimed, Alloc and Get return ErrBusy;
// writers here back off briefly and retry, so the benchmark keeps the device
// saturated, and busy writes and reads are reported. Every read's latency is
// recorded by where it was served: memory, disk, or a miss. A heartbeat
// prints system and cache metrics every 30 seconds.
//
//	go test -bench=BenchmarkBlobCache -benchtime=10000x   # ~10 GB
//	go test -bench=BenchmarkBlobCache -benchtime=1000000x # ~1 TB written; use BLOBCACHE_MAX_SEGMENTS
//
// Backpressured writers may wait on a previous ticket. The final drain is
// timed, so the result includes landing every write. Environment:
//
//	BLOBCACHE_BUFFERED_READS=1   read through the page cache instead of O_DIRECT
//	BLOBCACHE_PARALLELISM=p      run p workers per CPU (default 1). Get is
//	                             synchronous, so workers bound the reads in flight
//	BLOBCACHE_WRITE_PERCENT=w    percent of operations that are writes (default
//	                             30); misses stay 10%, hot and cold reads split
//	                             the rest evenly
//	BLOBCACHE_CACHE_MEMORY_MB=m  cache memory (default 1024)
//	BLOBCACHE_RINGS=n            I/O rings (default 1)
//	BLOBCACHE_WRITE_RINGS=n      dedicated write rings (default 0: shared)
//	BLOBCACHE_IO_BUDGET=d        per-class budget (default 1.5ms; off disables)
//	BLOBCACHE_MAX_SEGMENTS=n     retained segment limit (default 0: unlimited)
//	BLOBCACHE_NO_MEMORY_HITS=1   serve every read from disk, for comparing the
//	                             memory tier with a disk-only cache
func BenchmarkBlobCache(b *testing.B) {
	parent := os.TempDir()
	if _, err := os.Stat("/instance_storage"); err == nil {
		parent = "/instance_storage"
	}
	dir, err := os.MkdirTemp(parent, "bench-blobcache-")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Error(err)
		}
	}()

	const (
		missPercent   = 10
		warmupKeys    = 10000
		blobSizeLo    = 100_000
		blobSizeRange = 1_900_000
	)

	envInt := func(name string, def int) int {
		v := os.Getenv(name)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			b.Fatalf("%s: %v", name, err)
		}
		return n
	}
	parallelism := envInt("BLOBCACHE_PARALLELISM", 1)
	writeBound := envInt("BLOBCACHE_WRITE_PERCENT", 30)
	if writeBound <= 0 || writeBound >= 100-missPercent {
		b.Fatalf("BLOBCACHE_WRITE_PERCENT=%d: want 1..%d", writeBound, 100-missPercent-1)
	}
	coldReadBound := 100 - missPercent
	hotReadBound := writeBound + (coldReadBound-writeBound)/2
	opts := []Option{
		WithRings(envInt("BLOBCACHE_RINGS", 1)),
		WithDedicatedWriteRings(envInt("BLOBCACHE_WRITE_RINGS", 0)),
		WithMaxSegments(envInt("BLOBCACHE_MAX_SEGMENTS", 0)),
		WithDirectReads(os.Getenv("BLOBCACHE_BUFFERED_READS") != "1"),
		WithMemory(int64(envInt("BLOBCACHE_CACHE_MEMORY_MB", 1024)) << 20),
	}
	if os.Getenv("BLOBCACHE_NO_MEMORY_HITS") == "1" {
		opts = append(opts, withoutMemoryHits())
	}
	if budget := os.Getenv("BLOBCACHE_IO_BUDGET"); budget != "" {
		var goal time.Duration
		if budget != "off" {
			var err error
			goal, err = time.ParseDuration(budget)
			if err != nil {
				b.Fatalf("BLOBCACHE_IO_BUDGET=%q: %v", budget, err)
			}
		}
		opts = append(opts, WithIOBudget(goal))
	}
	cache, err := New(dir, opts...)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			b.Error(err)
		}
	}()

	entropy := make([]byte, 32<<20)
	if _, err := crand.Read(entropy); err != nil {
		b.Fatal(err)
	}
	var busyAlloc, busyPut, busyReads atomic.Int64
	// Download once. Busy admission retains this buffer. Wait on this worker's
	// previous write instead of copying again or serializing all workers on Drain.
	write := func(key []byte, value []byte, previous Ticket) Ticket {
		var buf []byte
		for {
			var err error
			if buf == nil {
				buf, err = cache.Alloc(cache.RecordSize(len(key), len(value)))
				if errors.Is(err, ErrBusy) {
					busyAlloc.Add(1)
				} else if err == nil {
					copy(buf, value)
				}
			}
			if err == nil {
				var ticket Ticket
				ticket, err = cache.Put(key, buf, len(value))
				if err == nil {
					return ticket
				}
				if errors.Is(err, ErrBusy) {
					busyPut.Add(1)
				}
			}
			if !errors.Is(err, ErrBusy) {
				b.Fatal(err)
			}
			if previous.t.Location().Size() != 0 {
				if err := previous.Wait(); err != nil {
					b.Fatal(err)
				}
			}
			time.Sleep(50 * time.Microsecond) // yield for the completer and segment seals
		}
	}
	// readLatency holds one worker's read latencies: every read is recorded,
	// hot, cold or miss, by where it was served.
	type readLatency struct{ memory, disk, miss *hdrhistogram.Histogram }
	newReadLatency := func() readLatency {
		return readLatency{
			memory: hdrhistogram.New(10, 10_000_000_000, 3),
			disk:   hdrhistogram.New(10, 10_000_000_000, 3),
			miss:   hdrhistogram.New(10, 10_000_000_000, 3),
		}
	}
	read := func(key []byte, readBytes *atomic.Int64, lat readLatency) bool {
		start := time.Now()
		var n int
		fromMemory, err := cache.get(key, func(v []byte) error { n = len(v); return nil })
		elapsed := time.Since(start).Nanoseconds()
		hist := lat.miss
		switch {
		case err == nil && fromMemory:
			hist = lat.memory
		case err == nil:
			hist = lat.disk
		}
		if recordErr := hist.RecordValue(elapsed); recordErr != nil {
			b.Error(recordErr)
		}
		switch {
		case err == nil:
			readBytes.Add(int64(n))
			return true
		case errors.Is(err, ErrBusy):
			busyReads.Add(1)
		case !errors.Is(err, ErrNotFound):
			b.Error(err)
		}
		return false
	}

	var (
		reads, hits, writeBytes, readBytes atomic.Int64
		writeHead                          atomic.Uint64
		workerID                           atomic.Int64
		mu                                 sync.Mutex
		putHist                            = hdrhistogram.New(10, 10_000_000_000, 3)
		getLat                             = newReadLatency()
	)

	fmt.Printf(">>> Warmup %s: writing %d 1 MB keys (N=%d)...\n", time.Now().Format(time.RFC3339Nano), warmupKeys, b.N)
	warmupStart := time.Now()
	keyBuf := make([]byte, 0, 32)
	var previous Ticket
	for i := range warmupKeys {
		previous = write(formatKey(keyBuf, "key-", uint64(i)), entropy[:1<<20], previous)
	}
	cache.Drain()
	warmup := float64(warmupKeys) / (1 << 10) / time.Since(warmupStart).Seconds()
	writeHead.Store(warmupKeys)

	monitorCtx, stopMonitor := context.WithCancel(context.Background())
	metrics := startMonitor(monitorCtx, cache, dir, &writeBytes, &readBytes, &reads, &hits)

	fmt.Printf(">>> Measured %s: %d workers, N=%d\n", time.Now().Format(time.RFC3339Nano), parallelism*runtime.GOMAXPROCS(0), b.N)
	b.SetParallelism(parallelism)
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		wid := workerID.Add(1)
		rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(wid)))
		zipf := rand.NewZipf(rng, 1.1, 1.0, 1<<25)
		keyBuf := make([]byte, 0, 64)
		var previous Ticket
		localPut := hdrhistogram.New(10, 10_000_000_000, 3)
		localGet := newReadLatency()

		for pb.Next() {
			for written := false; !written; {
				op := rng.IntN(100)
				head := writeHead.Load()
				start := time.Now()
				switch {
				case op < writeBound:
					key := formatKey(keyBuf, "key-", writeHead.Add(1))
					size := blobSizeLo + rng.IntN(blobSizeRange)
					off := rng.IntN(len(entropy) - size)
					previous = write(key, entropy[off:off+size], previous)
					writeBytes.Add(int64(size))
					if err := localPut.RecordValue(time.Since(start).Nanoseconds()); err != nil {
						b.Error(err)
					}
					written = true
				case op < hotReadBound:
					id := head - 1 - zipf.Uint64()%head
					found := read(formatKey(keyBuf, "key-", id), &readBytes, localGet)
					reads.Add(1)
					if found {
						hits.Add(1)
					}
				case op < coldReadBound:
					first := rng.Uint64() % (head - 4)
					for i := range uint64(4) {
						if read(formatKey(keyBuf, "key-", first+i), &readBytes, localGet) {
							hits.Add(1)
						}
					}
					reads.Add(4)
				default:
					read(formatKey(keyBuf, "miss-", rng.Uint64()), &readBytes, localGet)
					reads.Add(1)
				}
			}
		}
		mu.Lock()
		putHist.Merge(localPut)
		getLat.memory.Merge(localGet.memory)
		getLat.disk.Merge(localGet.disk)
		getLat.miss.Merge(localGet.miss)
		mu.Unlock()
	})
	cache.Drain()
	b.StopTimer()
	elapsed := time.Since(start).Seconds()
	stopMonitor()
	final := <-metrics

	fmt.Printf("\n--- FINAL LATENCY REPORT (ns) ---\n")
	reportLatency(b, "GET-memory", getLat.memory)
	reportLatency(b, "GET-disk", getLat.disk)
	reportLatency(b, "GET-miss", getLat.miss)
	reportLatency(b, "PUT", putHist)
	s := cache.Stats()
	fmt.Printf("\n--- CACHE ---\n  items: %d | segments: %d | evicted: %d | eviction errors: %d | retained bytes: %d | failed: %d | hits: %d | memory hits: %d | misses: %d | corrupt: %d | put errors: %d | busy alloc: %d | busy put: %d | busy reads: %d\n",
		s.Items, s.Segments, s.EvictedSegments, s.EvictionErrors, s.DiskBytes, s.FailedSegments, s.Hits, s.MemoryHits, s.Misses, s.Corrupt, s.PutErrors, busyAlloc.Load(), busyPut.Load(), busyReads.Load())
	b.ReportMetric(float64(busyAlloc.Load()), "busy-alloc")
	b.ReportMetric(float64(busyPut.Load()), "busy-put")
	b.ReportMetric(float64(s.EvictedSegments), "evicted-segments")
	b.ReportMetric(float64(s.EvictionErrors), "eviction-errors")
	b.ReportMetric(float64(s.DiskBytes)/(1<<30), "retained-GiB")
	b.ReportMetric(float64(writeBytes.Load())/(1<<30)/elapsed, "write-GB/s")
	b.ReportMetric(float64(readBytes.Load())/(1<<30)/elapsed, "read-GB/s")
	b.ReportMetric(final.physWrite, "phys-write-GB/s")
	b.ReportMetric(final.physRead, "phys-read-GB/s")
	b.ReportMetric(warmup, "warmup-GB/s")
	b.ReportMetric(final.peakRSS, "Peak-RSS-GB")
	b.ReportMetric(final.avgUtil, "Disk-Util-%")
}

// BenchmarkPutGet measures the per-operation CPU and allocation cost of the
// write and read paths on a small working set: a Put followed by a Get served
// from memory, or, with disk, from disk.
func BenchmarkPutGet(b *testing.B) {
	for _, from := range []string{"memory", "disk"} {
		for _, size := range []int{64 << 10, 1 << 20} {
			b.Run(fmt.Sprintf("%s/%dKB", from, size>>10), func(b *testing.B) {
				opts := []Option{WithSegmentSize(64 << 20), WithMemory(256 << 20)}
				if from == "disk" {
					opts = append(opts, withoutMemoryHits())
				}
				cache, err := New(b.TempDir(), opts...)
				if err != nil {
					b.Fatal(err)
				}
				defer func() {
					if err := cache.Close(); err != nil {
						b.Error(err)
					}
				}()
				keys := make([][]byte, 64)
				for i := range keys {
					keys[i] = formatKey(nil, "key-", uint64(i))
				}
				record := cache.RecordSize(len(keys[len(keys)-1]), size)
				measure := func(v []byte) error {
					if len(v) != size {
						return fmt.Errorf("read %d bytes, want %d", len(v), size)
					}
					return nil
				}
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					key := keys[i%len(keys)]
					buf, err := cache.Alloc(record)
					if err != nil {
						b.Fatal(err)
					}
					ticket, err := cache.Put(key, buf, size)
					if err != nil {
						b.Fatal(err)
					}
					if err := ticket.Wait(); err != nil {
						b.Fatal(err)
					}
					if from == "disk" {
						cache.Drain() // release the completed write pin
					}
					if err := cache.Get(key, measure); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func formatKey(buf []byte, prefix string, id uint64) []byte {
	return strconv.AppendUint(append(buf[:0], prefix...), id, 10)
}

func reportLatency(b *testing.B, name string, h *hdrhistogram.Histogram) {
	p50, p99, p999 := h.ValueAtQuantile(50), h.ValueAtQuantile(99), h.ValueAtQuantile(99.9)
	fmt.Printf("%s | n: %d | p50: %dns | p99: %dns | p999: %dns | max: %dns\n", name, h.TotalCount(), p50, p99, p999, h.Max())
	b.ReportMetric(float64(p50), "clat-"+name+"-p50-ns")
	b.ReportMetric(float64(p99), "clat-"+name+"-p99-ns")
	b.ReportMetric(float64(p999), "clat-"+name+"-p999-ns")
}

type monitorResult struct {
	peakRSS   float64
	avgUtil   float64
	physRead  float64 // average device read GB/s
	physWrite float64 // average device write GB/s
}

// startMonitor prints a heartbeat every 30 seconds and reports peak RSS and
// average disk utilization when ctx is canceled.
func startMonitor(
	ctx context.Context, cache *Cache, dir string,
	writeBytes, readBytes, reads, hits *atomic.Int64,
) <-chan monitorResult {
	out := make(chan monitorResult, 1)
	go func() {
		const interval = 30 * time.Second
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		proc, _ := process.NewProcess(int32(os.Getpid()))
		prevIO, _ := disk.IOCounters()
		prevWrite, prevRead := writeBytes.Load(), readBytes.Load()
		prevReads, prevHits := reads.Load(), hits.Load()
		var res monitorResult
		var utilSum, readSum, writeSum float64
		var samples int
		for {
			select {
			case <-ctx.Done():
				if samples > 0 {
					res.avgUtil = utilSum / float64(samples)
					res.physRead = readSum / float64(samples)
					res.physWrite = writeSum / float64(samples)
				}
				out <- res
				return
			case <-ticker.C:
			}
			var rss float64
			if proc != nil {
				if mem, err := proc.MemoryInfo(); err == nil {
					rss = float64(mem.RSS) / (1 << 30)
				}
			}
			res.peakRSS = max(res.peakRSS, rss)

			curIO, _ := disk.IOCounters()
			var util, physRead, physWrite float64
			for name, cur := range curIO {
				prev, ok := prevIO[name]
				if !ok || (!strings.HasPrefix(name, "nvme") && !strings.HasPrefix(name, "sd") && !strings.HasPrefix(name, "vd")) {
					continue
				}
				util = max(util, min(100, float64(cur.IoTime-prev.IoTime)/float64(interval.Milliseconds())*100))
				physRead += float64(cur.ReadBytes - prev.ReadBytes)
				physWrite += float64(cur.WriteBytes - prev.WriteBytes)
			}
			prevIO = curIO
			utilSum += util
			samples++

			gb := func(bytes float64) float64 { return bytes / (1 << 30) / interval.Seconds() }
			readSum += gb(physRead)
			writeSum += gb(physWrite)
			curWrite, curRead := writeBytes.Load(), readBytes.Load()
			curReads, curHits := reads.Load(), hits.Load()
			hitRate := 0.0
			if n := curReads - prevReads; n > 0 {
				hitRate = float64(curHits-prevHits) / float64(n) * 100
			}
			var freeGB float64
			if usage, err := disk.Usage(dir); err == nil {
				freeGB = float64(usage.Free) / (1 << 30)
			}
			s := cache.Stats()
			fmt.Printf("\n[HEARTBEAT %s]\n"+
				"  MEM:   RSS: %.2fGB | writes in flight: %d\n"+
				"  DISK:  Util: %.1f%% | Phys-Read: %.2f GB/s | Phys-Write: %.2f GB/s | Free: %.1fGB\n"+
				"  TPUT:  Log-Write: %.2f GB/s | Log-Read: %.2f GB/s\n"+
				"  READS: %.0f/s | HitRate: %.1f%% | hits: %d | misses: %d\n"+
				"  CACHE: items: %d | segments: %d | evicted: %d | eviction errors: %d | retained: %.2f GiB | failed: %d\n",
				time.Now().Format("15:04:05"), rss, s.WritesInFlight,
				util, gb(physRead), gb(physWrite), freeGB,
				gb(float64(curWrite-prevWrite)), gb(float64(curRead-prevRead)),
				float64(curReads-prevReads)/interval.Seconds(), hitRate, s.Hits, s.Misses,
				s.Items, s.Segments, s.EvictedSegments, s.EvictionErrors, float64(s.DiskBytes)/(1<<30), s.FailedSegments)
			prevWrite, prevRead, prevReads, prevHits = curWrite, curRead, curReads, curHits
		}
	}()
	return out
}
