package blobcache

import (
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/alloc"
)

// BenchmarkBlobCache counts individual requests: 30% writes and 70% reads of
// published keys, selected with Zipf locality toward recent writes. Values use
// the weighted 64 KiB–8 MiB distribution below. Every request finishes before
// its worker starts another. Read bodies are consumed with io.Copy(io.Discard).
//
// Warmup is mandatory, runs the same workload in parallel, and is excluded from
// all measured statistics. It writes enough inline payload to fill memory once
// with 25% headroom. Errors are collected and reported at the end of each phase.
// ns/op is elapsed time per request with workers running concurrently. Write
// latency includes NewWriter, Write, Put, and Close; Put latency times Put alone.
// Read/write slat and clat record individual I/O tickets: streaming requests
// produce multiple samples, while memory hits produce none.
// ErrBusy retries wait 50 microseconds and give up after one second per request.
//
// Environment: BLOBCACHE_DISK_GIB (required), BLOBCACHE_CACHE_MEMORY_MB (16384),
// BLOBCACHE_BENCH_DIR (/instance_storage if available, else the temporary dir),
// BLOBCACHE_FAIL_ON_ERROR (1; use 0 to report measured errors without failing).
// Warmup errors always fail. Use Go's -cpu, -benchtime, and profiling flags.
//
// Example: BLOBCACHE_DISK_GIB=100 go test -run '^$' -bench '^BenchmarkBlobCache$' -cpu=32 -benchtime=200000x -timeout=15m
func BenchmarkBlobCache(b *testing.B) {
	workload := newBenchmarkWorkload(b)
	warmup := workload.run(workload.warmupRequests())
	fmt.Printf("WARMUP: requests=%d writes=%d elapsed=%s\n", warmup.requests, warmup.writes.count(), warmup.elapsed)
	if warmup.err != nil {
		b.Fatalf("warmup: %v", warmup.err)
	}
	// run has joined all warmup workers. Reset once here, before starting any
	// measured workers. The populated cache and its keys survive the reset.
	metrics.Reset()

	b.ResetTimer()
	result := workload.run(b.N)
	b.StopTimer()
	result.report(b)
	if result.err != nil && workload.config.failOnError {
		b.Fatal(result.err)
	}
}

type benchmarkWorkload struct {
	cache  *Cache
	config benchmarkConfig
	data   []byte
	nextID atomic.Uint64
	mu     sync.RWMutex
	keys   []uint64 // Append only after Put and Close succeed.
}

// Each worker has its own random source and measurements. The phase
// barrier is also the point at which errors and histograms can be reported.
func (w *benchmarkWorkload) run(requests int) *benchmarkStats {
	workers := runtime.GOMAXPROCS(0)
	stats := make([]*benchmarkStats, workers)
	var group sync.WaitGroup
	start := time.Now()
	for id := range workers {
		worker := w.worker(id)
		stats[id] = worker.stats
		n := requests / workers
		if id < requests%workers {
			n++
		}
		group.Go(func() { worker.run(n) })
	}
	group.Wait()
	w.cache.store.Wait()
	result := newBenchmarkStats()
	for _, s := range stats {
		result.merge(s)
	}
	result.elapsed = time.Since(start)
	return result
}

type benchmarkWorker struct {
	workload *benchmarkWorkload
	rng      *rand.Rand
	zipf     *rand.Zipf
	stats    *benchmarkStats
	key      []byte
}

func (w *benchmarkWorkload) worker(id int) *benchmarkWorker {
	rng := rand.New(rand.NewPCG(42, uint64(id+1)))
	// Zipf rejection sampling must not change later request types or write sizes.
	reads := rand.New(rand.NewPCG(43, uint64(id+1)))
	return &benchmarkWorker{workload: w, rng: rng, zipf: rand.NewZipf(reads, 1.1, 1, 1<<32-1),
		stats: newBenchmarkStats(), key: make([]byte, 0, 32)}
}

func (w *benchmarkWorker) run(requests int) {
	w.stats.requests = int64(requests)
	for range requests {
		if w.rng.IntN(100) >= 30 {
			if id, ok := w.workload.readKey(w.zipf); ok {
				w.read(id)
				continue
			}
		}
		w.write() // An empty cache bootstraps with writes.
	}
}

func (w *benchmarkWorkload) readKey(zipf *rand.Zipf) (uint64, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if len(w.keys) == 0 {
		return 0, false
	}
	// Rejection, rather than modulo, preserves Zipf weights on the current set.
	rank := zipf.Uint64()
	for rank >= uint64(len(w.keys)) {
		rank = zipf.Uint64()
	}
	return w.keys[len(w.keys)-1-int(rank)], true
}

func (w *benchmarkWorker) write() {
	size := benchmarkBlobSize(w.rng)
	offset := w.rng.IntN(len(w.workload.data) - size + 1)
	value := w.workload.data[offset : offset+size]
	id := w.workload.nextID.Add(1)
	w.key = strconv.AppendUint(append(w.key[:0], "key-"...), id, 10)
	start := time.Now()
	var writer *Writer
	var err error
	for {
		writer, err = w.workload.cache.NewWriter(w.key, int64(size))
		if !errors.Is(err, ErrBusy) || time.Since(start) >= time.Second {
			break
		}
		w.stats.writeRetries++
		time.Sleep(50 * time.Microsecond)
	}
	if err != nil {
		w.stats.recordError(err)
		return
	}
	n, err := writer.Write(value)
	if err == nil && n != size {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = w.workload.cache.Put(writer)
	}
	if err = errors.Join(err, writer.Close()); err != nil {
		w.stats.recordError(err)
		return
	}
	w.workload.mu.Lock()
	w.workload.keys = append(w.workload.keys, id)
	w.workload.mu.Unlock()
	w.stats.writes.record(time.Since(start), int64(size))
	w.stats.sizes.RecordValue(int64(size))
}

func (w *benchmarkWorker) read(id uint64) {
	w.key = strconv.AppendUint(append(w.key[:0], "key-"...), id, 10)
	start := time.Now()
	var reader Reader
	var err error
	for {
		reader, err = w.workload.cache.Read(w.key)
		if !errors.Is(err, ErrBusy) || time.Since(start) >= time.Second {
			break
		}
		w.stats.readRetries++
		time.Sleep(50 * time.Microsecond)
	}
	if errors.Is(err, ErrNotFound) {
		w.stats.misses++
		return
	}
	if err != nil {
		w.stats.recordError(err)
		return
	}
	n, err := io.Copy(io.Discard, reader)
	err = errors.Join(err, reader.Close())
	if err == nil && n != reader.Size() {
		err = fmt.Errorf("read %d bytes, expected %d", n, reader.Size())
	}
	if err != nil {
		w.stats.recordError(err)
		return
	}
	if _, memory := reader.(*memoryReader); memory {
		w.stats.memoryReads.record(time.Since(start), n)
	} else {
		w.stats.diskReads.record(time.Since(start), n)
	}
}

// Uniform within each band, with half of writes between 512 KiB and 1 MiB.
var benchmarkSizeBands = [...]struct{ low, high, weight int }{
	{64 << 10, 128<<10 - 1, 1},
	{128 << 10, 256<<10 - 1, 4},
	{256 << 10, 512<<10 - 1, 15},
	{512 << 10, 1<<20 - 1, 50},
	{1 << 20, 2<<20 - 1, 20},
	{2 << 20, 4<<20 - 1, 8},
	{4 << 20, 8 << 20, 2},
}

func benchmarkBlobSize(rng *rand.Rand) int {
	choice := rng.IntN(100)
	for _, band := range benchmarkSizeBands {
		if choice < band.weight {
			return band.low + rng.IntN(band.high-band.low+1)
		}
		choice -= band.weight
	}
	panic("invalid size distribution")
}

const benchmarkInlineSize = 2 << 20

type benchmarkConfig struct {
	diskBytes, memoryBytes int64
	directory              string
	failOnError            bool
}

func readBenchmarkConfig(b *testing.B) benchmarkConfig {
	b.Helper()
	c := benchmarkConfig{diskBytes: benchmarkEnvInt(b, "BLOBCACHE_DISK_GIB", 0) << 30,
		memoryBytes: benchmarkEnvInt(b, "BLOBCACHE_CACHE_MEMORY_MB", 16384) << 20,
		directory:   os.Getenv("BLOBCACHE_BENCH_DIR"), failOnError: true}
	if c.diskBytes <= 0 {
		b.Fatal("set BLOBCACHE_DISK_GIB to the live disk target (e.g. 100)")
	}
	if c.memoryBytes < benchmarkInlineSize+(128<<10) {
		b.Fatal("memory must fit a 2 MiB inline value and its framing")
	}
	if value := os.Getenv("BLOBCACHE_FAIL_ON_ERROR"); value != "" {
		if value != "0" && value != "1" {
			b.Fatal("BLOBCACHE_FAIL_ON_ERROR must be 0 or 1")
		}
		c.failOnError = value == "1"
	}
	if c.directory == "" {
		c.directory = os.TempDir()
		if _, err := os.Stat("/instance_storage"); err == nil {
			c.directory = "/instance_storage"
		}
	}
	return c
}

func benchmarkEnvInt(b *testing.B, name string, fallback int64) int64 {
	b.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil || n < 0 {
		b.Fatalf("invalid %s=%q", name, value)
	}
	return n
}

func newBenchmarkWorkload(b *testing.B) *benchmarkWorkload {
	b.Helper()
	w := &benchmarkWorkload{config: readBenchmarkConfig(b), data: make([]byte, 32<<20)}
	dir, err := os.MkdirTemp(w.config.directory, "bench-blobcache-")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })
	w.cache, err = New(dir, WithTargetSize(w.config.diskBytes),
		WithMemory(w.config.memoryBytes, alloc.WithChunkSize(128<<10), alloc.WithWarmup()),
		WithInlineThreshold(benchmarkInlineSize), WithStagingSize(1<<20),
		WithIOBudget(1500*time.Microsecond), WithDurableIndex(true), WithVerifyChecksums(true),
		storeOpt(blobstore.WithDataIntegrity(blobstore.DataIntegrityXXHash128)))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := w.cache.Close(); err != nil {
			b.Error(err)
		}
	})
	if _, err := cryptorand.Read(w.data); err != nil {
		b.Fatal(err)
	}
	fmt.Printf("\nWORKLOAD: workers=%d requests=%d warmup-requests=%d writes=30%% reads=70%% locality=Zipf sizes=64KiB..8MiB inline=2MiB memory=%dMiB disk=%dGiB direct-io=true checksums=true durable-index=true\n",
		runtime.GOMAXPROCS(0), b.N, w.warmupRequests(), w.config.memoryBytes>>20, w.config.diskBytes>>30)
	return w
}

// Estimate requests needed to populate 125% of memory with inline write payload.
// The same request mix runs in both phases; external writes do not warm memory.
func (w *benchmarkWorkload) warmupRequests() int {
	var inlinePerHundredWrites int64
	for _, band := range benchmarkSizeBands {
		if band.high < benchmarkInlineSize {
			inlinePerHundredWrites += int64((band.low + band.high) / 2 * band.weight)
		}
	}
	bytesPerRequest := inlinePerHundredWrites * 30 / 10000
	return max(runtime.GOMAXPROCS(0), int((w.config.memoryBytes*5/4+bytesPerRequest-1)/bytesPerRequest))
}

type benchmarkMeasurements struct {
	latency *hdrhistogram.Histogram
	bytes   int64
}

func (m benchmarkMeasurements) count() int64 { return m.latency.TotalCount() }

func (m *benchmarkMeasurements) record(latency time.Duration, bytes int64) {
	m.latency.RecordValue(int64(latency))
	m.bytes += bytes
}

type benchmarkStats struct {
	requests, failures, misses     int64
	writeRetries, readRetries      int64
	elapsed                        time.Duration
	writes, memoryReads, diskReads benchmarkMeasurements
	sizes                          *hdrhistogram.Histogram
	err                            error
}

func newBenchmarkStats() *benchmarkStats {
	latency := func() benchmarkMeasurements {
		return benchmarkMeasurements{latency: hdrhistogram.New(1, math.MaxInt64, 3)}
	}
	return &benchmarkStats{writes: latency(), memoryReads: latency(), diskReads: latency(),
		sizes: hdrhistogram.New(1, 8<<20, 3)}
}

func (s *benchmarkStats) recordError(err error) {
	s.failures++
	if s.err == nil {
		s.err = err
	}
}

func (s *benchmarkStats) merge(other *benchmarkStats) {
	if s.err == nil {
		s.err = other.err
	}
	s.requests += other.requests
	s.failures += other.failures
	s.misses += other.misses
	s.writeRetries += other.writeRetries
	s.readRetries += other.readRetries
	for _, pair := range [][2]*benchmarkMeasurements{{&s.writes, &other.writes}, {&s.memoryReads, &other.memoryReads}, {&s.diskReads, &other.diskReads}} {
		pair[0].bytes += pair[1].bytes
		pair[0].latency.Merge(pair[1].latency)
	}
	s.sizes.Merge(other.sizes)
}

func (s *benchmarkStats) report(b *testing.B) {
	fmt.Printf("REQUESTS: total=%d writes=%d memory-reads=%d disk-reads=%d misses=%d errors=%d elapsed=%s\n",
		s.requests, s.writes.count(), s.memoryReads.count(), s.diskReads.count(), s.misses, s.failures, s.elapsed)
	for _, metric := range []struct {
		name      string
		histogram *hdrhistogram.Histogram
	}{
		{"write", s.writes.latency}, {"Put", metrics.PutDuration.Snapshot()},
		{"memory-read", s.memoryReads.latency}, {"disk-read", s.diskReads.latency},
		{"read-slat", metrics.ReadSlat.Snapshot()}, {"read-clat", metrics.ReadClat.Snapshot()},
		{"write-slat", metrics.WriteSlat.Snapshot()}, {"write-clat", metrics.WriteClat.Snapshot()},
	} {
		h := metric.histogram
		fmt.Printf("LATENCY %s: n=%d p50=%s p99=%s p99.9=%s max=%s mean=%s\n", metric.name, h.TotalCount(),
			time.Duration(h.ValueAtQuantile(50)), time.Duration(h.ValueAtQuantile(99)), time.Duration(h.ValueAtQuantile(99.9)), time.Duration(h.Max()), time.Duration(h.Mean()))
		b.ReportMetric(float64(h.ValueAtQuantile(99))/1e6, metric.name+"-p99-ms")
	}
	fmt.Printf("RETRIES: write=%d read=%d\n", s.writeRetries, s.readRetries)
	b.ReportMetric(float64(s.writeRetries), "write-retries")
	b.ReportMetric(float64(s.readRetries), "read-retries")
	seconds := s.elapsed.Seconds()
	fmt.Printf("PAYLOAD: written=%.3fGiB disk-read=%.3fGiB memory-read=%.3fGiB write-size-p50=%.1fKiB p99=%.1fKiB\n",
		float64(s.writes.bytes)/(1<<30), float64(s.diskReads.bytes)/(1<<30), float64(s.memoryReads.bytes)/(1<<30),
		float64(s.sizes.ValueAtQuantile(50))/1024, float64(s.sizes.ValueAtQuantile(99))/1024)
	b.ReportMetric(float64(s.writes.bytes)/(1<<30)/seconds, "write-GiB/s")
	b.ReportMetric(float64(s.diskReads.bytes)/(1<<30)/seconds, "disk-read-GiB/s")
	b.ReportMetric(float64(s.memoryReads.bytes)/(1<<30)/seconds, "memory-read-GiB/s")
	b.ReportMetric(float64(s.requests-s.failures)/seconds, "completed-requests/s")
	b.ReportMetric(float64(s.writes.count())/seconds, "completed-writes/s")
	b.ReportMetric(float64(s.failures), "request-errors")
	fmt.Printf("ERRORS: NewWriter=%d Put=%d object-close=%d unlink=%d\n",
		metrics.WriterErrors.Load(), metrics.PutErrors.Load(), metrics.CloseErrors.Load(), metrics.UnlinkErrors.Load())
	if s.err != nil {
		fmt.Printf("ERROR: %v\n", s.err)
	}
}
