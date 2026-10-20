package blobcache

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/process"
)

// BenchmarkBlobCache mixes 30% writes, 30% hot reads, 30% cold read groups
// (four reads each), and 10% intentional misses. Each benchmark iteration ends
// after one accepted write, uniformly sized from 100,000 bytes to 4 MiB. Hot reads favor recent keys.
// Readers consume the complete value and close it; writers close after Put.
// ErrBusy retries are counted and included in operation latency on both paths.
// Publication and Close drain inside the measured interval. Accepted and completed
// writes are counted separately. Logged I/O errors fail the run.
//
// Example: BLOBCACHE_DISK_GIB=100 go test -run '^$' -bench '^BenchmarkBlobCache$' -benchtime=200000x -timeout=30m
//
// Environment (defaults):
//
//	BLOBCACHE_DISK_GIB           live-payload target in GiB (required)
//	BLOBCACHE_BENCH_DIR          parent for a temporary cache (/instance_storage if present)
//	BLOBCACHE_CACHE_MEMORY_MB    prefaulted value/staging memory (16384 MiB)
//	BLOBCACHE_PARALLELISM        workers per GOMAXPROCS (1)
//	BLOBCACHE_WRITE_PERCENT      write selections (30); misses remain 10%
//	BLOBCACHE_WARMUP_KEYS        inline preload count (about 1.25x memory capacity)
//	BLOBCACHE_STAGING_KIB        streaming buffer size (256)
//	BLOBCACHE_INLINE_KIB         largest inline value (1280)
//	BLOBCACHE_CHUNK_KIB          allocator chunk size (128)
//	BLOBCACHE_MAX_BLOB_KIB       largest generated value (4096)
//	BLOBCACHE_PROFILE_DIR        optional measured-phase CPU/block/mutex profiles
//	BLOBCACHE_BUFFERED_IO=1      disable O_DIRECT
//	BLOBCACHE_INDEX_DURABLE=0    disable index group commit
//	BLOBCACHE_IO_BUDGET          scheduler budget (1.5ms; "off" disables)
//	BLOBCACHE_VERIFY_CHECKSUMS=1 write payload hashes and verify disk reads
//
// The target is not a filesystem quota: data.db keeps its allocated blocks while
// external files are evicted independently. The temporary cache is removed afterward.
func BenchmarkBlobCache(b *testing.B) {
	parent := os.Getenv("BLOBCACHE_BENCH_DIR")
	if parent == "" {
		parent = os.TempDir()
		if _, err := os.Stat("/instance_storage"); err == nil {
			parent = "/instance_storage"
		}
	}
	dir, err := os.MkdirTemp(parent, "bench-blobcache-")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)
	parallelism := benchmarkEnvInt(b, "BLOBCACHE_PARALLELISM", 1)
	writeBound := benchmarkEnvInt(b, "BLOBCACHE_WRITE_PERCENT", 30)
	if parallelism < 1 || writeBound < 1 || writeBound >= 90 {
		b.Fatal("invalid parallelism or write percentage")
	}
	hotReadBound := writeBound + (90-writeBound)/2
	diskGiB := benchmarkEnvInt(b, "BLOBCACHE_DISK_GIB", 0)
	if diskGiB <= 0 {
		b.Fatal("set BLOBCACHE_DISK_GIB (e.g. 100)")
	}
	target := int64(diskGiB) << 30
	memoryMiB := benchmarkEnvInt(b, "BLOBCACHE_CACHE_MEMORY_MB", 16384)
	chunkKiB := benchmarkEnvInt(b, "BLOBCACHE_CHUNK_KIB", 128)
	inlineMax := benchmarkEnvInt(b, "BLOBCACHE_INLINE_KIB", 1280) << 10
	blobMax := benchmarkEnvInt(b, "BLOBCACHE_MAX_BLOB_KIB", 4096) << 10
	const blobMin = 100_000
	if chunkKiB <= 0 || blobMax < blobMin || inlineMax < 0 {
		b.Fatal("invalid chunk size, blob maximum, or inline cutoff")
	}
	opts := []Option{
		storeOpt(blobstore.TestingWithDirectIO(os.Getenv("BLOBCACHE_BUFFERED_IO") != "1")),
		WithTargetSize(target), WithDurableIndex(os.Getenv("BLOBCACHE_INDEX_DURABLE") != "0"),
		WithMemory(int64(memoryMiB)<<20, alloc.WithChunkSize(int64(chunkKiB)<<10), alloc.WithWarmup()),
		WithStagingSize(int64(benchmarkEnvInt(b, "BLOBCACHE_STAGING_KIB", 256)) << 10),
		WithInlineThreshold(int64(inlineMax)),
	}
	if os.Getenv("BLOBCACHE_VERIFY_CHECKSUMS") == "1" {
		opts = append(opts, WithVerifyChecksums(true), storeOpt(blobstore.WithDataIntegrity(blobstore.DataIntegrityXXHash128)))
	}
	if budget := os.Getenv("BLOBCACHE_IO_BUDGET"); budget != "" {
		var goal time.Duration
		if budget != "off" {
			goal, err = time.ParseDuration(budget)
			if err != nil {
				b.Fatal(err)
			}
		}
		opts = append(opts, WithIOBudget(goal))
	}
	var loggedErrors atomic.Int64
	logger := slog.Default()
	slog.SetDefault(slog.New(benchmarkLogHandler{Handler: logger.Handler(), errors: &loggedErrors}))
	defer slog.SetDefault(logger)
	cache, err := New(dir, opts...)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()
	var retired atomic.Int64
	onRetire := cache.store.OnRetire
	cache.store.OnRetire = func(key blobstore.KeyHash, id uint64) { retired.Add(1); onRetire(key, id) }
	entropy := make([]byte, max(32<<20, blobMax+1))
	if _, err := crand.Read(entropy); err != nil {
		b.Fatal(err)
	}
	var busyWrites, busyReads, writes, writeBytes, readBytes atomic.Int64
	var completedWrites, completedBytes, completedExternal atomic.Int64
	var completionMu sync.Mutex
	completionLatency := hdrhistogram.New(1, int64(time.Minute), 3)
	var completionOverflow atomic.Bool
	var slowWrites atomic.Int64
	write := func(key, value []byte, stages []*hdrhistogram.Histogram) error {
		for {
			attemptStart := time.Now()
			var published func()
			if stages != nil {
				published = func() {
					elapsed := time.Since(attemptStart)
					completionMu.Lock()
					if err := completionLatency.RecordValue(elapsed.Nanoseconds()); err != nil {
						completionOverflow.Store(true)
					}
					completionMu.Unlock()
					completedWrites.Add(1)
					completedBytes.Add(int64(len(value)))
					if len(value) > inlineMax {
						completedExternal.Add(1)
					}
				}
			}
			timing, err := benchmarkPutStages(cache, key, value, published)
			if stages != nil {
				var total time.Duration
				for i, d := range timing {
					total += d
					if recordErr := stages[i].RecordValue(d.Nanoseconds()); recordErr != nil {
						return recordErr
					}
				}
				if total >= time.Second || total >= 100*time.Millisecond && slowWrites.Add(1) <= 8 {
					fmt.Printf("SLOW-WRITE %s bytes=%d new=%s stream=%s put=%s close=%s\n", time.Now().Format("15:04:05.000"), len(value), timing[0], timing[1], timing[2], timing[3])
				}
			}
			if !errors.Is(err, ErrBusy) {
				return err
			}
			busyWrites.Add(1)
			time.Sleep(50 * time.Microsecond)
		}
	}
	// Populate memory with inline-sized values; a mostly external distribution
	// would leave the cache cold even after writing many gigabytes to disk.
	warmupMax := min(blobMax, inlineMax)
	warmupMin := min(blobMin, warmupMax)
	warmupKeys := max(4, benchmarkEnvInt(b, "BLOBCACHE_WARMUP_KEYS",
		max(10000, (memoryMiB<<20)*5/4/max(1, (warmupMin+warmupMax)/2))))
	if b.N == 1 {
		warmupKeys = 4
	} // Go's calibration call does not need the full fixture.
	fmt.Printf("\n>>> cache=%s target=%dGiB workers=%d warmup=%d N=%d checksums=%t chunks=%dKiB inline=%dKiB blobs=%d..%dB warmup=inline\n", dir, diskGiB, parallelism*runtime.GOMAXPROCS(0), warmupKeys, b.N, os.Getenv("BLOBCACHE_VERIFY_CHECKSUMS") == "1", chunkKiB, inlineMax>>10, blobMin, blobMax)
	warmupStart := time.Now()
	warmupRNG := rand.New(rand.NewPCG(42, 0))
	var warmupBytes int64
	for i := range warmupKeys {
		size := warmupMin + warmupRNG.IntN(warmupMax-warmupMin+1)
		if err := write(formatKey(nil, "key-", uint64(i)), entropy[:size], nil); err != nil {
			b.Fatal(err)
		}
		warmupBytes += int64(size)
	}
	cache.store.Wait()
	warmupRate := float64(warmupBytes) / (1 << 30) / time.Since(warmupStart).Seconds()
	fmt.Printf(">>> measured: warmup=%s memory-used=%.2fGiB capacity=%.2fGiB\n", time.Since(warmupStart), float64(cache.memory.used.Load())/(1<<30), float64(cache.memory.Capacity())/(1<<30))
	busyWrites.Store(0)
	var writeHead atomic.Uint64
	writeHead.Store(uint64(warmupKeys)) // Next key to reserve; concurrent readers can legitimately miss in-flight writes.
	var workerID atomic.Uint64
	var failed atomic.Bool
	fail := func(err error) {
		if failed.CompareAndSwap(false, true) {
			b.Error(err)
		}
	}
	var counts [3]benchmarkReadCounts // hot, cold, intentional miss
	newHist := func() *hdrhistogram.Histogram { return hdrhistogram.New(1, int64(time.Minute), 3) }
	hist := [8]*hdrhistogram.Histogram{newHist(), newHist(), newHist(), newHist(), newHist(), newHist(), newHist(), newHist()}
	workerHist := make([][8]*hdrhistogram.Histogram, parallelism*runtime.GOMAXPROCS(0))
	for i := range workerHist {
		for j := range workerHist[i] {
			workerHist[i][j] = newHist()
		}
	}
	allocations := &benchmarkChunkAllocator{Allocator: cache.memory.Allocator}
	cache.memory.Allocator = allocations
	stopMonitor := monitorBenchmark(cache, dir, &writeBytes, &readBytes, &retired)
	defer stopMonitor()
	stopProfiles := benchmarkProfiles(b)
	defer stopProfiles()
	b.SetParallelism(parallelism)
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		id := workerID.Add(1)
		rng := rand.New(rand.NewPCG(42, id))
		zipf := rand.NewZipf(rng, 1.1, 1, 1<<25)
		keyBuf := make([]byte, 0, 64)
		local := workerHist[id-1]
		record := func(bucket int, start time.Time) {
			if err := local[bucket].RecordValue(time.Since(start).Nanoseconds()); err != nil {
				fail(err)
			}
		}
		read := func(key []byte, kind int) {
			start := time.Now()
			var r Reader
			var err error
			for {
				r, err = cache.Read(key)
				if !errors.Is(err, ErrBusy) {
					break
				}
				busyReads.Add(1)
				if failed.Load() {
					return
				}
				time.Sleep(50 * time.Microsecond)
			}
			if errors.Is(err, ErrNotFound) {
				counts[kind].miss.Add(1)
				record(3, start)
				return
			}
			if err != nil {
				fail(err)
				return
			}
			_, memory := r.(*memoryReader)
			n, readErr := io.Copy(io.Discard, r)
			err = errors.Join(readErr, r.Close())
			if err != nil {
				fail(err)
				return
			}
			if n != r.Size() {
				fail(fmt.Errorf("read %d bytes, want %d", n, r.Size()))
				return
			}
			if kind == 2 {
				fail(fmt.Errorf("unexpected hit for %q", key))
				return
			}
			readBytes.Add(n)
			if memory {
				counts[kind].memory.Add(1)
				counts[kind].memoryBytes.Add(n)
				record(1, start)
			} else {
				counts[kind].disk.Add(1)
				counts[kind].diskBytes.Add(n)
				record(2, start)
			}
		}
		for pb.Next() {
			for written := false; !written && !failed.Load(); {
				op, head := rng.IntN(100), writeHead.Load()
				switch {
				case op < writeBound:
					key := formatKey(keyBuf, "key-", writeHead.Add(1)-1)
					size := blobMin + rng.IntN(blobMax-blobMin+1)
					off := rng.IntN(len(entropy) - size)
					start := time.Now()
					if err := write(key, entropy[off:off+size], local[4:]); err != nil {
						fail(err)
						return
					}
					writes.Add(1)
					writeBytes.Add(int64(size))
					record(0, start)
					written = true
				case op < hotReadBound:
					read(formatKey(keyBuf, "key-", head-1-zipf.Uint64()%head), 0)
				case op < 90:
					first := rng.Uint64() % (head - 3)
					for i := range uint64(4) {
						read(formatKey(keyBuf, "key-", first+i), 1)
					}
				default:
					read(formatKey(keyBuf, "miss-", rng.Uint64()), 2)
				}
			}
			if failed.Load() {
				break
			}
		}
	})
	cache.store.Wait()       // Include completion, not just enqueue time, in throughput.
	metrics := stopMonitor() // Stop sampling Store before Close releases it.
	live, items := cache.store.Size(), cache.values.entries.Len()
	var residentPayload, residentAllocation int64
	var residentBuffers, residentChunks int64
	for i := range cache.values.entries.ShardCount() {
		shard := cache.values.entries.ShardAt(i)
		shard.RLock()
		for _, entry := range shard.Extra.list.All() {
			residentPayload += int64(shard.Items[entry.key].header.PhysicalLen)
			residentAllocation += entry.buffer.Cap()
			residentBuffers++
			residentChunks += int64(len(entry.buffer.Chunks()))
		}
		shard.RUnlock()
	}
	if err := cache.Close(); err != nil {
		b.Error(err)
	}
	elapsed := time.Since(start).Seconds()
	b.StopTimer()
	stopProfiles()
	for _, local := range workerHist {
		for i := range hist {
			hist[i].Merge(local[i])
		}
	}
	fmt.Printf("MEMORY: resident-payload=%.3fGiB resident-allocation=%.3fGiB\n", float64(residentPayload)/(1<<30), float64(residentAllocation)/(1<<30))
	b.ReportMetric(float64(residentPayload)/(1<<30), "resident-payload-GiB")
	b.ReportMetric(float64(residentAllocation)/(1<<30), "resident-allocation-GiB")
	buffers, chunks := allocations.buffers.Load(), allocations.chunks.Load()
	averageChunks := float64(chunks) / float64(max(1, buffers))
	residentAverageChunks := float64(residentChunks) / float64(max(1, residentBuffers))
	fmt.Printf("CHUNKS: allocations=%d chunks=%d avg=%.2f resident-buffers=%d resident-chunks=%d resident-avg=%.2f\n", buffers, chunks, averageChunks, residentBuffers, residentChunks, residentAverageChunks)
	b.ReportMetric(averageChunks, "allocation-chunks/buffer")
	b.ReportMetric(residentAverageChunks, "resident-chunks/buffer")
	for i, name := range []string{"PUT-accepted", "READ-memory", "READ-disk", "READ-miss", "WRITE-new", "WRITE-stream", "WRITE-put", "WRITE-close"} {
		reportLatency(b, name, hist[i])
	}
	reportLatency(b, "PUT-published", completionLatency)
	fmt.Printf("WRITES: accepted=%d completed=%d external=%d\n", writes.Load(), completedWrites.Load(), completedExternal.Load())
	if completedWrites.Load() != writes.Load() || completionOverflow.Load() {
		b.Error("incomplete publications or completion latency outside histogram range")
	}
	b.ReportMetric(float64(completedWrites.Load()), "completed-writes")
	b.ReportMetric(float64(completedExternal.Load()), "external-writes")
	readHits := newHist()
	readHits.Merge(hist[1])
	readHits.Merge(hist[2])
	reportLatency(b, "READ-hit", readHits)
	b.ReportMetric(100*float64(hist[1].TotalCount())/float64(max(1, readHits.TotalCount())), "memory-hit-percent")
	for i, name := range []string{"hot", "cold", "intentional-miss"} {
		fmt.Printf("READ %s: memory=%d disk=%d miss=%d\n", name, counts[i].memory.Load(), counts[i].disk.Load(), counts[i].miss.Load())
	}
	var memoryBytes, diskBytes int64
	for i := range counts {
		memoryBytes += counts[i].memoryBytes.Load()
		diskBytes += counts[i].diskBytes.Load()
	}
	b.ReportMetric(float64(memoryBytes)/(1<<30)/elapsed, "memory-read-GiB/s")
	b.ReportMetric(float64(diskBytes)/(1<<30)/elapsed, "disk-read-GiB/s")
	fmt.Printf("BYTES: writes=%.3fGiB memory-reads=%.3fGiB disk-reads=%.3fGiB measured=%.3fs\n", float64(writeBytes.Load())/(1<<30), float64(memoryBytes)/(1<<30), float64(diskBytes)/(1<<30), elapsed)
	dataInfo, err := os.Stat(filepath.Join(dir, "data.db"))
	if err != nil {
		b.Fatal(err)
	}
	fmt.Printf("CACHE: live=%.2fGiB data.db=%.2fGiB items=%d retired=%d logged-errors=%d\n", float64(live)/(1<<30), float64(dataInfo.Size())/(1<<30), items, retired.Load(), loggedErrors.Load())
	if loggedErrors.Load() != 0 {
		b.Errorf("cache/scheduler logged %d errors", loggedErrors.Load())
	}
	b.ReportMetric(float64(writes.Load()), "writes")
	b.ReportMetric(float64(busyWrites.Load()), "write-retries")
	b.ReportMetric(float64(busyReads.Load()), "read-retries")
	b.ReportMetric(float64(retired.Load()), "retired-records")
	b.ReportMetric(float64(completedBytes.Load())/(1<<30)/elapsed, "write-GiB/s")
	b.ReportMetric(float64(readBytes.Load())/(1<<30)/elapsed, "read-GiB/s")
	b.ReportMetric(warmupRate, "warmup-GiB/s")
	b.ReportMetric(metrics.peakRSS, "peak-RSS-GiB")
	b.ReportMetric(metrics.physRead, "host-read-GiB/s")
	b.ReportMetric(metrics.physWrite, "host-write-GiB/s")
}

type benchmarkReadCounts struct{ memory, disk, miss, memoryBytes, diskBytes atomic.Int64 }

// Installed after warmup. Includes write buffers and disk-read staging;
// resident buffers are counted separately in the final memory snapshot.
type benchmarkChunkAllocator struct {
	alloc.Allocator
	buffers, chunks atomic.Int64
}

func (a *benchmarkChunkAllocator) TryAcquire(size int64) (*alloc.Buffer, error) {
	b, err := a.Allocator.TryAcquire(size)
	if err == nil {
		a.buffers.Add(1)
		a.chunks.Add(int64(len(b.Chunks())))
	}
	return b, err
}

func benchmarkPut(cache *Cache, key, value []byte) error {
	_, err := benchmarkPutStages(cache, key, value, nil)
	return err
}

func benchmarkPutStages(cache *Cache, key, value []byte, published func()) (timing [4]time.Duration, err error) {
	start := time.Now()
	w, err := cache.NewWriter(key, int64(len(value)))
	timing[0] = time.Since(start)
	if err != nil {
		return timing, err
	}
	start = time.Now()
	_, err = w.Write(value)
	timing[1] = time.Since(start)
	start = time.Now()
	if err == nil {
		if published == nil {
			err = cache.Put(w)
		} else {
			err = cache.store.Put(w, func(h blobstore.RecordHeader, buffer *alloc.Buffer) {
				cache.publish(h, buffer)
				published()
			})
		}
	}
	timing[2] = time.Since(start)
	start = time.Now()
	err = errors.Join(err, w.Close())
	timing[3] = time.Since(start)
	return timing, err
}

// Profiles exclude fixture construction. Sampling is opt-in and applies equally
// to every case in a comparison; no production instrumentation is needed.
func benchmarkProfiles(b *testing.B) func() {
	dir := os.Getenv("BLOBCACHE_PROFILE_DIR")
	if dir == "" || b.N == 1 {
		return func() {}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		b.Fatal(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		b.Fatal(err)
	}
	runtime.SetBlockProfileRate(1_000_000)
	previous := runtime.SetMutexProfileFraction(100)
	var once sync.Once
	return func() {
		once.Do(func() {
			pprof.StopCPUProfile()
			f.Close()
			runtime.SetBlockProfileRate(0)
			runtime.SetMutexProfileFraction(previous)
			for _, name := range []string{"block", "mutex"} {
				out, err := os.Create(filepath.Join(dir, name+".pprof"))
				if err != nil {
					b.Error(err)
					continue
				}
				if err := pprof.Lookup(name).WriteTo(out, 0); err != nil {
					b.Error(err)
				}
				out.Close()
			}
		})
	}
}

func benchmarkEnvInt(b *testing.B, name string, fallback int) int {
	b.Helper()
	if s := os.Getenv(name); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			b.Fatal(err)
		}
		return n
	}
	return fallback
}

func formatKey(buf []byte, prefix string, id uint64) []byte {
	return strconv.AppendUint(append(buf[:0], prefix...), id, 10)
}

// Capture asynchronous failures without adding production counters or APIs.
type benchmarkLogHandler struct {
	slog.Handler
	errors *atomic.Int64
}

func (h benchmarkLogHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelError {
		h.errors.Add(1)
	}
	return h.Handler.Handle(ctx, r)
}
func (h benchmarkLogHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return benchmarkLogHandler{h.Handler.WithAttrs(a), h.errors}
}
func (h benchmarkLogHandler) WithGroup(g string) slog.Handler {
	return benchmarkLogHandler{h.Handler.WithGroup(g), h.errors}
}

func reportLatency(b *testing.B, name string, h *hdrhistogram.Histogram) {
	fmt.Printf("%s: n=%d p50=%s p99=%s p99.9=%s max=%s mean=%s p95=%s\n", name, h.TotalCount(), time.Duration(h.ValueAtQuantile(50)), time.Duration(h.ValueAtQuantile(99)), time.Duration(h.ValueAtQuantile(99.9)), time.Duration(h.Max()), time.Duration(h.Mean()), time.Duration(h.ValueAtQuantile(95)))
	b.ReportMetric(float64(h.ValueAtQuantile(99))/1e6, name+"-p99-ms")
}

type benchmarkMonitorResult struct{ peakRSS, physRead, physWrite float64 }

// Host physical I/O includes other processes. Sample the partial final interval
// too, so smoke runs shorter than a heartbeat still have useful measurements.
func monitorBenchmark(cache *Cache, dir string, writes, reads, retired *atomic.Int64) func() benchmarkMonitorResult {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var result benchmarkMonitorResult
	go func() {
		defer close(done)
		proc, _ := process.NewProcess(int32(os.Getpid()))
		previousIO, _ := disk.IOCounters()
		start, previous := time.Now(), time.Now()
		var totalRead, totalWrite uint64
		var previousWrite, previousRead int64
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			finished := false
			select {
			case <-ctx.Done():
				finished = true
			case <-ticker.C:
			}
			now := time.Now()
			seconds := now.Sub(previous).Seconds()
			var rss float64
			if proc != nil {
				if mem, err := proc.MemoryInfo(); err == nil {
					rss = float64(mem.RSS) / (1 << 30)
				}
			}
			result.peakRSS = max(result.peakRSS, rss)
			currentIO, _ := disk.IOCounters()
			var physicalRead, physicalWrite uint64
			for name, current := range currentIO {
				old, ok := previousIO[name]
				if ok && (strings.HasPrefix(name, "nvme") || strings.HasPrefix(name, "sd") || strings.HasPrefix(name, "vd")) {
					physicalRead += current.ReadBytes - old.ReadBytes
					physicalWrite += current.WriteBytes - old.WriteBytes
				}
			}
			totalRead += physicalRead
			totalWrite += physicalWrite
			currentWrite, currentRead := writes.Load(), reads.Load()
			usage, _ := disk.Usage(dir)
			var free float64
			if usage != nil {
				free = float64(usage.Free) / (1 << 30)
			}
			fmt.Printf("[HEARTBEAT %s] RSS=%.2fGiB live=%.2fGiB items=%d retired=%d free=%.1fGiB logical W/R=%.3f/%.3fGiB/s host W/R=%.3f/%.3fGiB/s\n", now.Format("15:04:05"), rss, float64(cache.store.Size())/(1<<30), cache.values.entries.Len(), retired.Load(), free, float64(currentWrite-previousWrite)/(1<<30)/seconds, float64(currentRead-previousRead)/(1<<30)/seconds, float64(physicalWrite)/(1<<30)/seconds, float64(physicalRead)/(1<<30)/seconds)
			if finished {
				result.physRead = float64(totalRead) / (1 << 30) / now.Sub(start).Seconds()
				result.physWrite = float64(totalWrite) / (1 << 30) / now.Sub(start).Seconds()
				return
			}
			previousIO, previous = currentIO, now
			previousWrite, previousRead = currentWrite, currentRead
		}
	}()
	return func() benchmarkMonitorResult { cancel(); <-done; return result }
}

func BenchmarkPutGet(b *testing.B) {
	for _, from := range []string{"memory", "external"} {
		for _, size := range []int{64 << 10, 1 << 20} {
			b.Run(fmt.Sprintf("%s/%dKiB", from, size>>10), func(b *testing.B) {
				opts := []Option{WithTargetSize(256 << 20), WithMemory(256 << 20)}
				if from == "external" {
					opts = append(opts, WithInlineThreshold(0))
				}
				cache, err := New(b.TempDir(), opts...)
				if err != nil {
					b.Fatal(err)
				}
				defer cache.Close()
				value, key := make([]byte, size), []byte("key")
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := benchmarkPut(cache, key, value); err != nil {
						b.Fatal(err)
					}
					cache.store.Wait()
					r, err := cache.Read(key)
					if err != nil {
						b.Fatal(err)
					}
					n, readErr := io.Copy(io.Discard, r)
					if err := errors.Join(readErr, r.Close()); err != nil {
						b.Fatal(err)
					}
					if n != int64(size) {
						b.Fatalf("read %d bytes, want %d", n, size)
					}
				}
			})
		}
	}
}

func BenchmarkMemoryHit(b *testing.B) {
	cache, err := New(b.TempDir(), WithMemory(128<<10))
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()
	key := []byte("key")
	if err := benchmarkPut(cache, key, make([]byte, 1024)); err != nil {
		b.Fatal(err)
	}
	cache.store.Wait()
	b.ReportAllocs()
	b.SetBytes(1024)
	b.ResetTimer()
	for range b.N {
		r, err := cache.Read(key)
		if err != nil {
			b.Fatal(err)
		}
		n, readErr := io.Copy(io.Discard, r)
		if err := errors.Join(readErr, r.Close()); err != nil {
			b.Fatal(err)
		}
		if n != 1024 {
			b.Fatalf("read %d bytes", n)
		}
	}
}
