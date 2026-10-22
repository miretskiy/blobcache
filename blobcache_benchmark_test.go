package blobcache

import (
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
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

// BenchmarkBlobCache offers byte-paced fills and independently scheduled reads.
// Each key gets one/two/three read attempts with probabilities 90%/9%/1%, at
// log-uniform ages from 100ms to two minutes. Failed fills remain eligible.
// b.N is the offered fill count. Reads after the observation window are counted
// as not yet due; late warmup reads carry into the measured phase.
// Warmup populates 125% of memory with inline payload (bounded by b.N for short
// calibration/smoke runs). Each reader owns a reusable 128 KiB buffer.
//
// Arrival-to-completion latency includes dispatcher and worker queueing. Write
// arrivals keep their schedule when workers block; only request indexes queue,
// not payload buffers. Pending bytes count queued and active logical payload.
// Cache ErrBusy retries retain their 50us interval and one-second limit.
//
// Environment: BLOBCACHE_DISK_GIB (required), BLOBCACHE_CACHE_MEMORY_MB (16384),
// BLOBCACHE_BENCH_DIR (persistent cache directory; a new directory if unset),
// BLOBCACHE_FAIL_ON_ERROR (1), BLOBCACHE_DURABLE_INDEX (1).
// Example: BLOBCACHE_DISK_GIB=750 BLOBCACHE_DURABLE_INDEX=0 go test -run '^$' -bench '^BenchmarkBlobCache/steady-1GBps$' -cpu=32 -benchtime=160000x -timeout=15m
func BenchmarkBlobCache(b *testing.B) {
	for _, scenario := range []benchmarkCase{
		{"steady-1GBps", 1_000_000_000, false},
		{"steady-1p2GBps", 1_200_000_000, false},
		{"steady-1p22GBps", 1_220_000_000, false},
		{"bursty-1p2GBps", 1_200_000_000, true},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			workload := newBenchmarkWorkload(b)
			warmup := workload.run(makeBenchmarkPlan(min(b.N, workload.warmupWrites()), 0, len(workload.data),
				benchmarkCase{writeRate: 1_000_000_000}), nil)
			fmt.Printf("WARMUP: requests=%d writes=%d elapsed=%s\n", warmup.requests, warmup.writes.count(), warmup.elapsed)
			if warmup.err != nil || warmup.rejectedReads != 0 {
				b.Fatalf("warmup: error=%v rejected-reads=%d", warmup.err, warmup.rejectedReads)
			}
			plan := makeBenchmarkPlan(b.N, uint64(len(workload.keys)), len(workload.data), scenario)
			fmt.Printf("SCHEDULE: case=%s fills=%d reads=%d offered-bytes=%d duration=%s average-limit=%dB/s read-rate=%.3f/s writers=%d readers=%d pending-limit=none write-queue=request-indexes\n",
				scenario.name, len(plan.writes), len(plan.reads), plan.bytes, plan.duration, scenario.writeRate,
				float64(len(plan.reads))/plan.duration.Seconds(), benchmarkWriters, benchmarkReaders)
			fmt.Printf("READ-PLAN: pattern=mostly-once age=100ms..2m new-keys-once=%d new-keys-twice=%d new-keys-thrice=%d carry-from-warmup=%d after-window=%d\n",
				plan.readCounts[0], plan.readCounts[1], plan.readCounts[2], plan.carryReads, plan.futureReads)
			for i, burst := range plan.bursts {
				if burst.start < plan.duration {
					fmt.Printf("BURST: index=%d start=%s end=%s planned-duration=%s peak=%dB/s\n", i,
						burst.start, min(burst.end, plan.duration), burst.end-burst.start, benchmarkPeakRate)
				}
			}
			metrics.Reset()
			result := workload.run(plan, b)
			result.report(b)
			if result.err != nil && workload.config.failOnError {
				b.Fatal(result.err)
			}
		})
	}
}

type benchmarkProgress struct {
	requests, writes, bytes, misses, failures                                                 atomic.Int64
	offeredWrites, offeredReads, rejectedReads                                                atomic.Int64
	pendingBytes, maxPendingBytes, activeWrites, activeReads, maxActiveWrites, maxActiveReads atomic.Int64
}

type benchmarkWorkload struct {
	cache  *Cache
	config benchmarkConfig
	data   []byte
	// Immutable length during each phase; IDs are one-based indexes. State:
	// 0 = pending, 1 = published, 2 = failed. Every offered fill stays.
	keys     []atomic.Uint32
	progress benchmarkProgress
}

func benchmarkMaximum(counter *atomic.Int64, value int64) {
	for old := counter.Load(); value > old; old = counter.Load() {
		if counter.CompareAndSwap(old, value) {
			return
		}
	}
}

func benchmarkWait(deadline time.Time) {
	if delay := time.Until(deadline); delay > 0 {
		time.Sleep(delay)
	}
}

func (w *benchmarkWorkload) run(plan benchmarkPlan, b *testing.B) *benchmarkStats {
	w.progress = benchmarkProgress{}
	// Allocate a new backing array only between phases, when no workers exist.
	// Copy atomic values explicitly: atomic.Uint32 must not be copied after use.
	keys := make([]atomic.Uint32, len(w.keys)+len(plan.writes))
	for i := range w.keys {
		keys[i].Store(w.keys[i].Load())
	}
	w.keys = keys
	// Every planned arrival fits even if all writers block. Queue indexes into
	// the existing plan, so a payload backlog does not allocate payload copies.
	writeJobs := make(chan int, len(plan.writes))
	readJobs := make(chan benchmarkJob, benchmarkReadQueue)
	stats := make([]*benchmarkStats, benchmarkWriters+benchmarkReaders)
	workers := make([]*benchmarkWorker, len(stats))
	for i := range workers {
		workers[i] = &benchmarkWorker{workload: w, stats: newBenchmarkStats(), key: make([]byte, 0, 32)}
		stats[i] = workers[i].stats
	}
	var group, dispatch, reporter sync.WaitGroup
	if b != nil {
		b.ResetTimer()
	}
	start := time.Now()
	fmt.Printf("PHASE-START: utc=%s fills=%d reads=%d scheduled-duration=%s\n", start.UTC().Format(time.RFC3339Nano), len(plan.writes), len(plan.reads), plan.duration)
	done := make(chan struct{})
	reporter.Go(func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		latencies := []struct {
			name      string
			histogram *metrics.Histogram
			previous  *hdrhistogram.Snapshot
		}{
			{"Put", metrics.PutDuration, nil},
			{"read-slat", metrics.ReadSlat, nil},
			{"read-clat", metrics.ReadClat, nil},
			{"write-slat", metrics.WriteSlat, nil},
			{"write-clat", metrics.WriteClat, nil},
		}
		for i := range latencies {
			latencies[i].previous = latencies[i].histogram.Snapshot().Export()
		}
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				live, published := w.cache.store.WriteStats()
				fmt.Printf("PROGRESS: elapsed=%s requests=%d writes=%d written=%.3fGiB offered-writes=%d offered-reads=%d pending-bytes=%d queued-writes=%d active-writes=%d active-reads=%d rejected-writes=0 rejected-reads=%d live=%.3fGiB published=%.3fGiB memory-used=%.3fGiB misses=%d errors=%d\n",
					time.Since(start).Round(time.Second), w.progress.requests.Load(), w.progress.writes.Load(), float64(w.progress.bytes.Load())/(1<<30),
					w.progress.offeredWrites.Load(), w.progress.offeredReads.Load(), w.progress.pendingBytes.Load(), len(writeJobs), w.progress.activeWrites.Load(), w.progress.activeReads.Load(),
					w.progress.rejectedReads.Load(), float64(live)/(1<<30), float64(published)/(1<<30),
					float64(w.cache.memory.used.Load())/(1<<30), w.progress.misses.Load(), w.progress.failures.Load())
				w.reportIOQueue(time.Since(start))
				for i := range latencies {
					metric := &latencies[i]
					now := metric.histogram.Snapshot().Export()
					delta := *now
					delta.Counts = slices.Clone(now.Counts)
					for j := range delta.Counts {
						delta.Counts[j] -= metric.previous.Counts[j]
					}
					h := hdrhistogram.Import(&delta)
					fmt.Printf("INTERVAL-LATENCY: elapsed=%s metric=%s n=%d p50=%s p99=%s max=%s\n",
						time.Since(start).Round(time.Second), metric.name, h.TotalCount(),
						time.Duration(h.ValueAtQuantile(50)), time.Duration(h.ValueAtQuantile(99)), time.Duration(h.Max()))
					metric.previous = now
				}
			}
		}
	})
	for i, worker := range workers {
		if i < benchmarkWriters {
			group.Go(func() {
				for index := range writeJobs {
					job := plan.writes[index]
					benchmarkMaximum(&w.progress.maxActiveWrites, w.progress.activeWrites.Add(1))
					worker.write(job, start.Add(job.at))
					w.progress.activeWrites.Add(-1)
					w.progress.pendingBytes.Add(-int64(job.size))
					w.progress.requests.Add(1)
				}
			})
		} else {
			group.Go(func() {
				for job := range readJobs {
					benchmarkMaximum(&w.progress.maxActiveReads, w.progress.activeReads.Add(1))
					worker.read(job, start.Add(job.at))
					w.progress.activeReads.Add(-1)
					w.progress.requests.Add(1)
				}
			})
		}
	}
	dispatch.Go(func() {
		defer close(writeJobs)
		for index, job := range plan.writes {
			benchmarkWait(start.Add(job.at))
			w.progress.offeredWrites.Add(1)
			pending := w.progress.pendingBytes.Add(int64(job.size))
			benchmarkMaximum(&w.progress.maxPendingBytes, pending)
			writeJobs <- index
		}
	})
	dispatch.Go(func() {
		defer close(readJobs)
		for _, job := range plan.reads {
			benchmarkWait(start.Add(job.at))
			w.progress.offeredReads.Add(1)
			select {
			case readJobs <- job:
			default:
				w.progress.rejectedReads.Add(1)
			}
		}
	})
	dispatch.Wait()
	group.Wait()
	w.cache.store.Wait()
	elapsed := time.Since(start)
	if b != nil {
		b.StopTimer()
	}
	close(done)
	reporter.Wait()
	w.reportIOQueue(elapsed)
	result := newBenchmarkStats()
	for _, s := range stats {
		result.merge(s)
	}
	result.elapsed = elapsed
	result.scheduled = plan.duration
	result.offeredWrites, result.offeredReads, result.offeredBytes = int64(len(plan.writes)), int64(len(plan.reads)), plan.bytes
	result.rejectedReads = w.progress.rejectedReads.Load()
	result.maxPendingBytes = w.progress.maxPendingBytes.Load()
	result.maxActiveWrites, result.maxActiveReads = w.progress.maxActiveWrites.Load(), w.progress.maxActiveReads.Load()
	if result.requests+result.rejectedReads != result.offeredWrites+result.offeredReads || w.progress.pendingBytes.Load() != 0 {
		result.recordError(fmt.Errorf("benchmark accounting mismatch"))
	}
	return result
}

// Peaks include warmup: sampling or printing does not reset the scheduler.
func (w *benchmarkWorkload) reportIOQueue(elapsed time.Duration) {
	if q, ok := w.cache.store.IOQueueStats(); ok {
		fmt.Printf("DIO-QUEUE: elapsed=%s scope=scheduler-lifetime staged=%d staged-peak=%d waiting=%d waiting-peak=%d ready=%d ready-peak=%d in-flight=%d in-flight-peak=%d\n",
			elapsed.Round(time.Second), q.Staged.Current, q.Staged.Peak, q.Waiting.Current, q.Waiting.Peak,
			q.Ready.Current, q.Ready.Peak, q.InFlight.Current, q.InFlight.Peak)
	}
}

type benchmarkWorker struct {
	workload   *benchmarkWorkload
	stats      *benchmarkStats
	key        []byte
	readBuffer [128 << 10]byte
}

func (w *benchmarkWorker) write(job benchmarkJob, arrival time.Time) {
	w.stats.requests++
	w.key = strconv.AppendUint(append(w.key[:0], "key-"...), job.id, 10)
	start := time.Now()
	w.stats.writeQueue.record(start.Sub(arrival), 0)
	success := false
	defer func() {
		w.stats.writeService.record(time.Since(start), 0)
		if !success {
			w.workload.keys[job.id-1].Store(2)
		}
	}()
	var writer *Writer
	var err error
	for {
		writer, err = w.workload.cache.NewWriter(w.key, int64(job.size))
		if !errors.Is(err, ErrBusy) || time.Since(start) >= time.Second {
			break
		}
		w.stats.writeRetries++
		time.Sleep(50 * time.Microsecond)
	}
	if err != nil {
		w.recordError(err)
		return
	}
	value := w.workload.data[job.offset : job.offset+job.size]
	n, err := writer.Write(value)
	if err == nil && n != job.size {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = w.workload.cache.Put(writer)
	}
	if err = errors.Join(err, writer.Close()); err != nil {
		w.recordError(err)
		return
	}
	w.workload.keys[job.id-1].Store(1)
	success = true
	w.stats.writes.record(time.Since(arrival), int64(job.size))
	w.stats.sizes.RecordValue(int64(job.size))
	w.workload.progress.writes.Add(1)
	w.workload.progress.bytes.Add(int64(job.size))
}

func (w *benchmarkWorker) recordError(err error) {
	w.stats.recordError(err)
	w.workload.progress.failures.Add(1)
}

// Explicit Read calls avoid io.Copy's WriterTo/ReaderFrom shortcuts. In
// particular, memoryReader.WriteTo(io.Discard) need not touch payload bytes.
func benchmarkConsume(reader io.Reader, buffer []byte) (int64, error) {
	var total int64
	for {
		n, err := reader.Read(buffer)
		total += int64(n)
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
}

func (w *benchmarkWorker) read(job benchmarkJob, arrival time.Time) {
	w.stats.requests++
	w.key = strconv.AppendUint(append(w.key[:0], "key-"...), job.id, 10)
	start := time.Now()
	w.stats.readQueue.record(start.Sub(arrival), 0)
	pending := w.workload.keys[job.id-1].Load() == 0
	if pending {
		w.stats.pendingReads++
	}
	defer func() { w.stats.readService.record(time.Since(start), 0) }()
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
		if pending {
			w.stats.pendingMisses++
		}
		w.stats.missLatency.record(time.Since(arrival), 0)
		w.stats.regimeMisses[job.regime]++
		w.workload.progress.misses.Add(1)
		return
	}
	if err != nil {
		w.recordError(err)
		return
	}
	expected := reader.Size()
	n, err := benchmarkConsume(reader, w.readBuffer[:])
	err = errors.Join(err, reader.Close())
	if err == nil && n != expected {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		w.recordError(err)
		return
	}
	elapsed := time.Since(arrival)
	if _, memory := reader.(*memoryReader); memory {
		w.stats.memoryReads.record(elapsed, n)
	} else {
		w.stats.diskReads.record(elapsed, n)
	}
	w.stats.regimeReads[job.regime].record(elapsed, n)
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
	durableIndex           bool
}

func readBenchmarkConfig(b *testing.B) benchmarkConfig {
	b.Helper()
	c := benchmarkConfig{diskBytes: benchmarkEnvInt(b, "BLOBCACHE_DISK_GIB", 0) << 30,
		memoryBytes: benchmarkEnvInt(b, "BLOBCACHE_CACHE_MEMORY_MB", 16384) << 20,
		directory:   os.Getenv("BLOBCACHE_BENCH_DIR"), failOnError: true, durableIndex: true}
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
	if value := os.Getenv("BLOBCACHE_DURABLE_INDEX"); value != "" {
		if value != "0" && value != "1" {
			b.Fatal("BLOBCACHE_DURABLE_INDEX must be 0 or 1")
		}
		c.durableIndex = value == "1"
	}
	if c.directory == "" {
		base := os.TempDir()
		if _, err := os.Stat("/instance_storage"); err == nil {
			base = "/instance_storage"
		}
		var err error
		c.directory, err = os.MkdirTemp(base, "bench-blobcache-")
		if err != nil {
			b.Fatal(err)
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
	dir := w.config.directory
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatal(err)
	}
	opened := time.Now()
	var err error
	w.cache, err = New(dir, WithTargetSize(w.config.diskBytes),
		WithMemory(w.config.memoryBytes, alloc.WithChunkSize(128<<10), alloc.WithWarmup()),
		WithInlineThreshold(benchmarkInlineSize), WithStagingSize(1<<20),
		WithIOBudget(1500*time.Microsecond), WithDurableIndex(w.config.durableIndex), WithVerifyChecksums(true),
		storeOpt(blobstore.WithDataIntegrity(blobstore.DataIntegrityXXHash128)))
	if err != nil {
		b.Fatal(err)
	}
	fmt.Printf("CACHE-OPEN: directory=%s elapsed=%s recovered-bytes=%d\n", dir, time.Since(opened), w.cache.store.Size())
	b.Cleanup(func() {
		if err := w.cache.Close(); err != nil {
			b.Error(err)
		}
	})
	w.verifyRecovered(b)
	if _, err := cryptorand.Read(w.data); err != nil {
		b.Fatal(err)
	}
	fmt.Printf("\nWORKLOAD: cpus=%d offered-fills=%d warmup-fills=%d locality=mostly-once sizes=64KiB..8MiB inline=2MiB memory=%dMiB disk=%dGiB direct-io=true checksums=true durable-index=%t read-buffer=128KiB\n",
		runtime.GOMAXPROCS(0), b.N, min(b.N, w.warmupWrites()), w.config.memoryBytes>>20, w.config.diskBytes>>30, w.config.durableIndex)
	return w
}

// Probe the oldest recovered records before any new writes can replace them.
func (w *benchmarkWorkload) verifyRecovered(b *testing.B) {
	b.Helper()
	var headers []blobstore.RecordHeader
	w.cache.store.Recover(func(h blobstore.RecordHeader) {
		if len(headers) < 128 {
			headers = append(headers, h)
		}
	})
	var buffer [128 << 10]byte
	var total int64
	for _, h := range headers {
		path, offset := filepath.Join(w.config.directory, "data.db"), int64(h.Offset)
		if h.Flags.IsExternal() {
			path, offset = filepath.Join(w.config.directory, "objects", strconv.FormatUint(h.ObjectID, 10)), 0
		}
		file, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		key := make([]byte, h.KeyLen)
		_, err = file.ReadAt(key, offset+blobstore.RecordHeaderSize)
		err = errors.Join(err, file.Close())
		if err != nil {
			b.Fatal(err)
		}
		reader, err := w.cache.store.NewReader(key, h)
		if err != nil {
			b.Fatalf("recovered object %d: %v", h.ObjectID, err)
		}
		n, err := benchmarkConsume(reader, buffer[:])
		err = errors.Join(err, reader.Close())
		if err != nil || n != int64(h.LogicalLen) {
			b.Fatalf("recovered object %d: read=%d expected=%d error=%v", h.ObjectID, n, h.LogicalLen, err)
		}
		total += n
	}
	fmt.Printf("RECOVERY-CHECK: records=%d bytes=%d checksums=true\n", len(headers), total)
}

// Estimate fills needed to populate 125% of memory with inline write payload.
func (w *benchmarkWorkload) warmupWrites() int {
	var inlinePerHundredWrites int64
	for _, band := range benchmarkSizeBands {
		if band.high < benchmarkInlineSize {
			inlinePerHundredWrites += int64((band.low + band.high) / 2 * band.weight)
		}
	}
	bytesPerWrite := inlinePerHundredWrites / 100
	return max(benchmarkWriters, int((w.config.memoryBytes*5/4+bytesPerWrite-1)/bytesPerWrite))
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
	requests, failures, misses                                    int64
	writeRetries, readRetries                                     int64
	elapsed, scheduled                                            time.Duration
	offeredWrites, offeredReads, offeredBytes                     int64
	rejectedReads                                                 int64
	maxPendingBytes, maxActiveWrites, maxActiveReads              int64
	pendingReads, pendingMisses                                   int64
	writes, memoryReads, diskReads                                benchmarkMeasurements
	writeQueue, writeService, readQueue, readService, missLatency benchmarkMeasurements
	regimeReads                                                   [3]benchmarkMeasurements
	regimeMisses                                                  [3]int64
	sizes                                                         *hdrhistogram.Histogram
	err                                                           error
}

func newBenchmarkStats() *benchmarkStats {
	latency := func() benchmarkMeasurements {
		return benchmarkMeasurements{latency: hdrhistogram.New(1, math.MaxInt64, 3)}
	}
	return &benchmarkStats{writes: latency(), memoryReads: latency(), diskReads: latency(),
		writeQueue: latency(), writeService: latency(), readQueue: latency(), readService: latency(), missLatency: latency(),
		regimeReads: [3]benchmarkMeasurements{latency(), latency(), latency()}, sizes: hdrhistogram.New(1, 8<<20, 3)}
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
	s.pendingReads += other.pendingReads
	s.pendingMisses += other.pendingMisses
	for _, pair := range [][2]*benchmarkMeasurements{
		{&s.writes, &other.writes}, {&s.memoryReads, &other.memoryReads}, {&s.diskReads, &other.diskReads},
		{&s.writeQueue, &other.writeQueue}, {&s.writeService, &other.writeService},
		{&s.readQueue, &other.readQueue}, {&s.readService, &other.readService}, {&s.missLatency, &other.missLatency},
	} {
		pair[0].bytes += pair[1].bytes
		pair[0].latency.Merge(pair[1].latency)
	}
	for i := range s.regimeReads {
		s.regimeReads[i].bytes += other.regimeReads[i].bytes
		s.regimeReads[i].latency.Merge(other.regimeReads[i].latency)
		s.regimeMisses[i] += other.regimeMisses[i]
	}
	s.sizes.Merge(other.sizes)
}

func (s *benchmarkStats) report(b *testing.B) {
	fmt.Printf("ARRIVALS: writes=%d reads=%d bytes=%d scheduled=%s final-drain=%s write-GB/s=%.6f read-requests/s=%.3f\n",
		s.offeredWrites, s.offeredReads, s.offeredBytes, s.scheduled, max(0, s.elapsed-s.scheduled),
		float64(s.offeredBytes)/s.scheduled.Seconds()/1e9, float64(s.offeredReads)/s.scheduled.Seconds())
	fmt.Printf("DISPATCH: rejected-writes=0 rejected-bytes=0 rejected-reads=%d peak-pending-bytes=%d peak-active-writes=%d peak-active-reads=%d\n",
		s.rejectedReads, s.maxPendingBytes, s.maxActiveWrites, s.maxActiveReads)
	fmt.Printf("REQUESTS: total=%d writes=%d memory-reads=%d disk-reads=%d misses=%d errors=%d elapsed=%s\n",
		s.requests, s.writes.count(), s.memoryReads.count(), s.diskReads.count(), s.misses, s.failures, s.elapsed)
	for _, metric := range []struct {
		name      string
		histogram *hdrhistogram.Histogram
	}{
		{"write", s.writes.latency}, {"Put", metrics.PutDuration.Snapshot()},
		{"write-queue", s.writeQueue.latency}, {"write-service", s.writeService.latency},
		{"read-queue", s.readQueue.latency}, {"read-service-including-misses", s.readService.latency}, {"read-miss", s.missLatency.latency},
		{"memory-read", s.memoryReads.latency}, {"disk-read", s.diskReads.latency},
		{"read-slat", metrics.ReadSlat.Snapshot()}, {"read-clat", metrics.ReadClat.Snapshot()},
		{"write-slat", metrics.WriteSlat.Snapshot()}, {"write-clat", metrics.WriteClat.Snapshot()},
	} {
		h := metric.histogram
		fmt.Printf("LATENCY %s: n=%d p50=%s p99=%s p99.9=%s max=%s mean=%s\n", metric.name, h.TotalCount(),
			time.Duration(h.ValueAtQuantile(50)), time.Duration(h.ValueAtQuantile(99)), time.Duration(h.ValueAtQuantile(99.9)), time.Duration(h.Max()), time.Duration(h.Mean()))
		b.ReportMetric(float64(h.ValueAtQuantile(99))/1e6, metric.name+"-p99-ms")
	}
	fmt.Printf("PENDING-READS: reads=%d misses=%d (fill-not-yet-published-at-read-start)\n", s.pendingReads, s.pendingMisses)
	for i, name := range []string{"quiet", "burst", "recovery-5s"} {
		h := s.regimeReads[i].latency
		fmt.Printf("READ-REGIME %s: hits=%d misses=%d p50=%s p99=%s p99.9=%s max=%s\n", name,
			h.TotalCount(), s.regimeMisses[i], time.Duration(h.ValueAtQuantile(50)), time.Duration(h.ValueAtQuantile(99)),
			time.Duration(h.ValueAtQuantile(99.9)), time.Duration(h.Max()))
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

const (
	benchmarkWriters   = 64
	benchmarkReaders   = 64
	benchmarkReadQueue = 4096
	benchmarkPeakRate  = 2_000_000_000
)

type benchmarkCase struct {
	name      string
	writeRate int64
	bursty    bool
}

type benchmarkJob struct {
	at           time.Duration
	id           uint64
	size, offset int
	regime       int // 0: quiet/steady, 1: burst, 2: first five seconds of recovery.
}

type benchmarkBurst struct{ start, end time.Duration }

type benchmarkPlan struct {
	writes, reads []benchmarkJob
	bursts        []benchmarkBurst
	duration      time.Duration
	bytes         int64
	readCounts    [3]int // Assigned attempts per new key, including those not yet due.
	carryReads    int
	futureReads   int
}

// Plan both streams before execution. IDs become eligible at their scheduled
// fill arrival, even when a cache rejects or delays that fill.
// No cache result or goroutine scheduling decision can change the read trace.
func makeBenchmarkPlan(n int, base uint64, dataSize int, c benchmarkCase) benchmarkPlan {
	p := makeBenchmarkWritePlan(n, base, dataSize, c)
	addReads := func(writes []benchmarkJob, shift time.Duration, carry bool) {
		for _, write := range writes {
			ages := benchmarkReadAges(write.id)
			if !carry {
				p.readCounts[len(ages)-1]++
			}
			for _, age := range ages {
				at := shift + write.at + age
				if at <= 0 { // Already due in warmup; never replay it.
					continue
				}
				if at > p.duration {
					p.futureReads++
					continue
				}
				p.reads = append(p.reads, benchmarkJob{at: at, id: write.id, regime: p.regime(at)})
				if carry {
					p.carryReads++
				}
			}
		}
	}
	if base != 0 {
		// The preceding warmup always uses 1GB/s. Reconstruct its fixed fill
		// timeline so per-key read quotas survive the phase boundary unchanged.
		warmup := makeBenchmarkWritePlan(int(base), 0, dataSize, benchmarkCase{writeRate: 1_000_000_000})
		addReads(warmup.writes, -warmup.duration, true)
	}
	addReads(p.writes, 0, false)
	slices.SortFunc(p.reads, func(a, b benchmarkJob) int {
		if a.at < b.at {
			return -1
		}
		if a.at > b.at {
			return 1
		}
		if a.id < b.id {
			return -1
		}
		if a.id > b.id {
			return 1
		}
		return 0
	})
	return p
}

// These are explicit workload assumptions, not fitted production measurements.
// Sort independently drawn ages so extra attempts follow the first attempt.
// The RNG is keyed by ID: phase boundaries and cache outcomes cannot resample it.
func benchmarkReadAges(id uint64) []time.Duration {
	rng := rand.New(rand.NewPCG(46, id))
	count := 1
	if choice := rng.IntN(100); choice >= 99 {
		count = 3
	} else if choice >= 90 {
		count = 2
	}
	ages := make([]time.Duration, count)
	for i := range ages {
		ages[i] = time.Duration(float64(100*time.Millisecond) * math.Exp(rng.Float64()*math.Log(1200)))
	}
	slices.Sort(ages)
	return ages
}

func makeBenchmarkWritePlan(n int, base uint64, dataSize int, c benchmarkCase) benchmarkPlan {
	p := benchmarkPlan{writes: make([]benchmarkJob, n)}
	sizes := rand.New(rand.NewPCG(42, base))
	for i := range p.writes {
		size := benchmarkBlobSize(sizes)
		p.writes[i] = benchmarkJob{id: base + uint64(i) + 1, size: size,
			offset: sizes.IntN(dataSize - size + 1)}
		p.bytes += int64(size)
	}
	type period struct{ end, rate float64 }
	periods := []period{{math.Inf(1), float64(c.writeRate)}}
	if c.bursty {
		periods = nil
		bursts := rand.New(rand.NewPCG(45, base))
		var end float64
		// Infrequent 8-16s peaks outlast staging that can absorb a 2s burst.
		// Each complete 60-120s cycle has the configured average byte rate;
		// the lower-rate interval allows the preceding burst's backlog to drain.
		for end*float64(c.writeRate) < float64(p.bytes) {
			window := 60 + 60*bursts.Float64()
			duration := min(16.0, 8.0/math.Pow(1-bursts.Float64(), 1.0/1.5))
			quiet := window - duration
			rate := (float64(c.writeRate)*window - benchmarkPeakRate*duration) / quiet
			periods = append(periods, period{end + quiet, rate}, period{end + window, benchmarkPeakRate})
			p.bursts = append(p.bursts, benchmarkBurst{
				time.Duration((end + quiet) * float64(time.Second)),
				time.Duration((end + window) * float64(time.Second))})
			end += window
		}
	}
	var clock float64
	periodIndex := 0
	for i := range p.writes {
		remaining := float64(p.writes[i].size)
		for {
			period := periods[periodIndex]
			available := (period.end - clock) * period.rate
			if remaining <= available {
				clock += remaining / period.rate
				break
			}
			remaining -= available
			clock = period.end
			periodIndex++
		}
		p.writes[i].at = time.Duration(clock * float64(time.Second))
		p.writes[i].regime = p.regime(p.writes[i].at)
	}
	p.duration = time.Duration(clock * float64(time.Second))
	return p
}

func (p *benchmarkPlan) regime(at time.Duration) int {
	for _, burst := range p.bursts {
		if at >= burst.start && at < burst.end {
			return 1
		}
		if at >= burst.end && at < burst.end+5*time.Second {
			return 2
		}
	}
	return 0
}
