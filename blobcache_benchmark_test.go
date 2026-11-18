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
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/nvme"
)

// BenchmarkBlobCache offers byte-paced fills and independently scheduled reads.
// The size mix, read quotas, and trace planner match ringcache's benchmark.
// Every planned fill remains eligible for reads, including rejected fills.
// Warmup uses a fixed 16 GiB reference so changing cache capacity cannot change
// the offered trace. Readers consume borrowed chunks through Next without copying.
// Each scheduled arrival starts its own goroutine, with no worker queue or
// concurrency cap. Admission failures are counted once, without sleeping or retrying.
// Pending bytes include writes waiting for their I/O ticket; preparing bytes
// cover NewWriter, Write, and Put through its return, excluding ticket waits.
// One set of request histograms spans the entire measured workload. Burst
// boundary snapshots report cumulative completions and backlog by wall time.
// Successful read latencies are also reported by full value size and serving
// path (all, memory, disk); returned bytes may be a prefix of that value.
// Burst counts/durations include only the scheduled observation window; a
// burst cut short by that window is reported as partial, and drain is separate.
//
// Environment: BLOBCACHE_NVME_WRITE=1 with BLOBCACHE_NVME_{DEVICE,OFFSET,SIZE}
// naming a dedicated range, which each sub-benchmark formats; optional
// BLOBCACHE_NVME_SERIAL and BLOBCACHE_NVME_REQUIRE_NONROOT=1.
// BLOBCACHE_CACHE_MEMORY_MB (16384), BLOBCACHE_FAIL_ON_ERROR (1),
// BLOBCACHE_READ_SCHEDULERS and BLOBCACHE_WRITE_SCHEDULERS (1 each; io_uring
// schedulers serving reads and writes),
// BLOBCACHE_RING_DEPTH (256 per ring), BLOBCACHE_IO_BUDGET_US (1500 for writes
// shared across the disk; 0 disables). Reads are always unbudgeted.
// BLOBCACHE_{WRITE,READ}_BACKLOG_MS (0: disabled) limit dio's admitted,
// unfinished work across the disk in modeled device time, and
// BLOBCACHE_WRITE_BACKLOG_RECOVERY_MS (-1: disabled) sets the timed-pause target.
// bursty-variable uses configurable background, burst and cycle ranges; see
// readBenchmarkBurstProfile. Expected dio rejections are counted, not retried.
// BLOBCACHE_SMALL_WRITE_KIB (0) admits payloads up to this size through a full backlog.
// BLOBCACHE_REGISTERED_MEMORY (0; 1 registers cache memory with the rings).
// BLOBCACHE_CPU_PROFILE records a Go CPU profile of the measured phase only.
// Example: go test -run '^$' -bench '^BenchmarkBlobCache/steady-1GBps$' -cpu=32 -benchtime=160000x -timeout=15m
func BenchmarkBlobCache(b *testing.B) {
	if os.Getenv("BLOBCACHE_NVME_WRITE") != "1" {
		b.Skip("set BLOBCACHE_NVME_WRITE=1 and BLOBCACHE_NVME_{DEVICE,OFFSET,SIZE} to a dedicated range")
	}
	parse := func(name string) uint64 {
		n, err := strconv.ParseUint(os.Getenv(name), 10, 64)
		if err != nil {
			b.Fatalf("invalid %s: %v", name, err)
		}
		return n
	}
	if os.Getenv("BLOBCACHE_NVME_REQUIRE_NONROOT") == "1" && (os.Getuid() == 0 || os.Geteuid() == 0) {
		b.Fatal("BLOBCACHE_NVME_REQUIRE_NONROOT=1 rejects a root benchmark")
	}
	device, err := nvme.Open(os.Getenv("BLOBCACHE_NVME_DEVICE"), nvme.WithRange(parse("BLOBCACHE_NVME_OFFSET"), parse("BLOBCACHE_NVME_SIZE")))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { device.Close() })
	if serial := os.Getenv("BLOBCACHE_NVME_SERIAL"); serial != "" && serial != device.Info().Serial {
		b.Fatalf("serial %s, want %s", device.Info().Serial, serial)
	}
	b.Logf("uid=%d euid=%d serial=%s range=[%d,%d)", os.Getuid(), os.Geteuid(), device.Info().Serial, device.Offset(), device.Offset()+device.Size())
	for _, scenario := range []benchmarkCase{
		{name: "steady-1GBps", writeRate: 1_000_000_000},
		{name: "steady-1p2GBps", writeRate: 1_200_000_000},
		{name: "steady-1p22GBps", writeRate: 1_220_000_000},
		{name: "bursty-1p2GBps", writeRate: 1_200_000_000, bursty: true},
		{name: "bursty-variable"},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			if scenario.name == "bursty-variable" {
				scenario.profile = readBenchmarkBurstProfile(b)
			}
			scenario.readMaxAge = time.Duration(benchmarkEnvInt(b, "BLOBCACHE_READ_MAX_AGE_S", 120)) * time.Second
			scenario.readHeadAge = time.Duration(benchmarkEnvInt(b, "BLOBCACHE_READ_HEAD_AGE_MS", 0)) * time.Millisecond
			scenario.readHeadShare = int(benchmarkEnvInt(b, "BLOBCACHE_READ_HEAD_PERCENT", 0))
			workload := newBenchmarkWorkload(b, device)
			warmup := workload.run(makeBenchmarkPlan(min(b.N, benchmarkWarmupFills), 0, len(workload.data),
				benchmarkCase{writeRate: 1_000_000_000, readMaxAge: scenario.readMaxAge,
					readHeadAge: scenario.readHeadAge, readHeadShare: scenario.readHeadShare}), nil)
			fmt.Printf("WARMUP: requests=%d writes=%d rejected-writes=%d io-rejected-reads=%d cache-busy-writes=%d cache-busy-reads=%d elapsed=%s\n", warmup.requests, warmup.writes.count(), warmup.rejectedWrites, warmup.ioRejectedReads, warmup.busyWrites, warmup.busyReads, warmup.elapsed)
			if warmup.err != nil {
				b.Fatalf("warmup: %v", warmup.err)
			}
			plan := makeBenchmarkPlan(b.N, uint64(len(workload.keys)), len(workload.data), scenario)
			fmt.Printf("SCHEDULE: case=%s fills=%d reads=%d offered-bytes=%d duration=%s offered-average=%dB/s read-rate=%.3f/s dispatch=goroutine-per-arrival worker-queues=none small-write-bytes=%d\n",
				scenario.name, len(plan.writes), len(plan.reads), plan.bytes, plan.duration, int64(float64(plan.bytes)/plan.duration.Seconds()),
				float64(len(plan.reads))/plan.duration.Seconds(), workload.config.smallWriteAdmission)
			fmt.Printf("READ-PLAN: pattern=50%%-1,20%%-2,15%%-3..5,10%%-6..10,5%%-11..44 age=100ms..%s head=%d%%..%s prefix=50..100%% ",
				scenario.maxAge(), scenario.readHeadShare, scenario.readHeadAge)
			for i, count := range plan.readCounts {
				fmt.Printf("reads-%d=%d ", i+1, count)
			}
			fmt.Printf("carry-from-warmup=%d after-window=%d\n", plan.carryReads, plan.futureReads)
			for i, burst := range plan.bursts {
				if burst.start < plan.duration {
					peak := plan.peakRate
					if i < len(plan.burstRates) {
						peak = plan.burstRates[i]
					}
					fmt.Printf("BURST: index=%d start=%s end=%s planned-duration=%s peak=%dB/s\n", i,
						burst.start, min(burst.end, plan.duration), burst.end-burst.start, peak)
				}
			}
			var profile *os.File
			if path := os.Getenv("BLOBCACHE_CPU_PROFILE"); path != "" {
				var err error
				profile, err = os.Create(path)
				if err != nil {
					b.Fatal(err)
				}
				if err := pprof.StartCPUProfile(profile); err != nil {
					profile.Close()
					b.Fatal(err)
				}
			}
			metrics.Reset()
			result := workload.run(plan, b)
			if profile != nil {
				pprof.StopCPUProfile()
				if err := profile.Close(); err != nil {
					b.Fatal(err)
				}
			}
			result.report(b)
			if result.err != nil && workload.config.failOnError {
				b.Fatal(result.err)
			}
		})
	}
}

type benchmarkProgress struct {
	requests, writes, bytes, misses, failures                                                 atomic.Int64
	offeredBytes                                                                              atomic.Int64
	preparingWrites, preparingBytes, maxPreparingWrites, maxPreparingBytes                    atomic.Int64
	memoryReadBytes, diskReadBytes                                                            atomic.Int64
	offeredWrites, offeredReads                                                               atomic.Int64
	rejectedWrites, rejectedBytes, ioRejectedReads                                            atomic.Int64
	busyWrites, busyReads                                                                     atomic.Int64
	pendingBytes, maxPendingBytes, activeWrites, activeReads, maxActiveWrites, maxActiveReads atomic.Int64
}

type benchmarkWorkload struct {
	cache  *Cache
	disk   *iosched.DiskPolicy
	config benchmarkConfig
	data   []byte
	// Immutable length during each phase; IDs are one-based indexes. State:
	// 0 = pending, 1 = published, 2 = failed, 3 = rejected. Every offered fill stays.
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

// Each arrival proceeds independently of earlier requests still doing I/O.
func benchmarkDispatch(jobs []benchmarkJob, start time.Time, group *sync.WaitGroup, run func(int, benchmarkJob)) {
	for index, job := range jobs {
		benchmarkWait(start.Add(job.at))
		group.Go(func() { run(index, job) })
	}
}

func (w *benchmarkWorkload) run(plan benchmarkPlan, b *testing.B) *benchmarkStats {
	w.progress = benchmarkProgress{}
	// Allocate a new backing array only between phases, when no requests exist.
	// Copy atomic values explicitly: atomic.Uint32 must not be copied after use.
	keys := make([]atomic.Uint32, len(w.keys)+len(plan.writes))
	for i := range w.keys {
		keys[i].Store(w.keys[i].Load())
	}
	w.keys = keys
	// Each request owns one completion slot, even when arrivals overlap.
	writeCompletedAt := make([]time.Duration, len(plan.writes))
	writeOfferedAt := make([]time.Duration, len(plan.writes))
	result := newBenchmarkStats()
	var statsMu sync.Mutex
	record := func(request *benchmarkRequest) {
		// Aggregate only after I/O and latency measurement have finished.
		statsMu.Lock()
		result.record(&request.stats)
		statsMu.Unlock()
	}
	var group, dispatch, reporter sync.WaitGroup
	if b != nil {
		b.ResetTimer()
	}
	start := time.Now()
	fmt.Printf("PHASE-START: utc=%s fills=%d reads=%d scheduled-duration=%s\n", start.UTC().Format(time.RFC3339Nano), len(plan.writes), len(plan.reads), plan.duration)
	done := make(chan struct{})
	boundary := func(at time.Duration, event string, burst int) {
		fmt.Printf("BURST-BOUNDARY: elapsed=%s scheduled=%s event=%s burst-index=%d offered-bytes=%d completed-bytes=%d completed-writes=%d completed-memory-read-bytes=%d completed-disk-read-bytes=%d pending-bytes=%d rejected-writes=%d rejected-bytes=%d\n",
			time.Since(start), at, event, burst, w.progress.offeredBytes.Load(), w.progress.bytes.Load(),
			w.progress.writes.Load(), w.progress.memoryReadBytes.Load(), w.progress.diskReadBytes.Load(),
			w.progress.pendingBytes.Load(), w.progress.rejectedWrites.Load(), w.progress.rejectedBytes.Load())
	}
	boundary(0, "start", -1)
	reporter.Go(func() {
		for _, event := range plan.burstBoundaries() {
			timer := time.NewTimer(max(0, time.Until(start.Add(event.at))))
			select {
			case <-done:
				timer.Stop()
				return
			case <-timer.C:
			}
			boundary(event.at, event.event, event.burst)
		}
	})
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
				fmt.Printf("PROGRESS: elapsed=%s requests=%d writes=%d written=%.3fGiB offered-writes=%d offered-reads=%d pending-bytes=%d active-writes=%d active-reads=%d rejected-writes=%d io-rejected-reads=%d cache-busy-writes=%d cache-busy-reads=%d live=%.3fGiB published=%.3fGiB memory-used=%.3fGiB misses=%d errors=%d reclaim-calls=%d reclaim-requested=%.3fGiB reclaim-released=%.3fGiB reclaim-visited=%d reclaim-evicted=%d reclaim-spared=%d reclaim-pending=%d reclaim-pinned=%d\n",
					time.Since(start).Round(time.Second), w.progress.requests.Load(), w.progress.writes.Load(), float64(w.progress.bytes.Load())/(1<<30),
					w.progress.offeredWrites.Load(), w.progress.offeredReads.Load(), w.progress.pendingBytes.Load(), w.progress.activeWrites.Load(), w.progress.activeReads.Load(),
					w.progress.rejectedWrites.Load(), w.progress.ioRejectedReads.Load(), w.progress.busyWrites.Load(), w.progress.busyReads.Load(), float64(live)/(1<<30), float64(published)/(1<<30),
					float64((w.cache.memory.Capacity()-w.cache.memory.Available()))/(1<<30), w.progress.misses.Load(), w.progress.failures.Load(),
					metrics.ReclaimCalls.Load(), float64(metrics.ReclaimRequestedBytes.Load())/(1<<30), float64(metrics.ReclaimReleasedBytes.Load())/(1<<30),
					metrics.ReclaimVisited.Load(), metrics.ReclaimEvicted.Load(), metrics.ReclaimSpared.Load(), metrics.ReclaimPending.Load(), metrics.ReclaimPinned.Load())
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
	dispatch.Go(func() {
		benchmarkDispatch(plan.writes, start, &group, func(index int, job benchmarkJob) {
			writeOfferedAt[index] = time.Since(start)
			w.progress.offeredWrites.Add(1)
			w.progress.offeredBytes.Add(int64(job.size))
			benchmarkMaximum(&w.progress.maxPendingBytes, w.progress.pendingBytes.Add(int64(job.size)))
			benchmarkMaximum(&w.progress.maxActiveWrites, w.progress.activeWrites.Add(1))
			request := benchmarkRequest{workload: w}
			request.write(job, start.Add(job.at))
			writeCompletedAt[index] = time.Since(start)
			w.progress.activeWrites.Add(-1)
			w.progress.pendingBytes.Add(-int64(job.size))
			w.progress.requests.Add(1)
			record(&request)
		})
	})
	dispatch.Go(func() {
		benchmarkDispatch(plan.reads, start, &group, func(_ int, job benchmarkJob) {
			w.progress.offeredReads.Add(1)
			benchmarkMaximum(&w.progress.maxActiveReads, w.progress.activeReads.Add(1))
			request := benchmarkRequest{workload: w}
			request.read(job, start.Add(job.at))
			w.progress.activeReads.Add(-1)
			w.progress.requests.Add(1)
			record(&request)
		})
	})
	dispatch.Wait()
	group.Wait()
	elapsed := time.Since(start)
	if b != nil {
		b.StopTimer()
	}
	close(done)
	reporter.Wait()
	boundary(plan.duration, "complete", -1)
	for index, burst := range plan.bursts {
		var first, last, completed time.Duration
		var bytes int64
		for i, job := range plan.writes {
			if job.at < min(burst.end, plan.duration) {
				completed = max(completed, writeCompletedAt[i])
			}
			if job.at >= burst.start && job.at < burst.end {
				if bytes == 0 {
					first = writeOfferedAt[i]
				}
				last = max(last, writeOfferedAt[i])
				bytes += int64(job.size)
			}
		}
		if bytes > 0 {
			fmt.Printf("BURST-OFFERED: index=%d first=%s last=%s bytes=%d actual-GB/s=%.6f\n", index, first, last, bytes, float64(bytes)/max(time.Nanosecond, last-first).Seconds()/1e9)
			fmt.Printf("BURST-DRAIN: index=%d scheduled-end=%s last-offer=%s last-completion=%s drain=%s partial=%t scope=all-write-outcomes-through-burst-end\n",
				index, burst.end, last, completed, max(0, completed-last), burst.end > plan.duration)
		}
	}
	w.reportIOQueue(elapsed)
	result.maxPreparingWrites, result.maxPreparingBytes = w.progress.maxPreparingWrites.Load(), w.progress.maxPreparingBytes.Load()
	for _, at := range writeOfferedAt {
		result.offeredElapsed = max(result.offeredElapsed, at)
	}
	if len(plan.reads) > 0 {
		result.offeredElapsed = max(result.offeredElapsed, plan.reads[len(plan.reads)-1].at)
	}
	result.elapsed = elapsed
	result.scheduled = plan.duration
	result.bursts, result.completeBursts, result.burstDuration = plan.burstSummary()
	result.offeredWrites, result.offeredReads, result.offeredBytes = int64(len(plan.writes)), int64(len(plan.reads)), plan.bytes
	result.maxPendingBytes = w.progress.maxPendingBytes.Load()
	result.maxActiveWrites, result.maxActiveReads = w.progress.maxActiveWrites.Load(), w.progress.maxActiveReads.Load()
	if result.requests != result.offeredWrites+result.offeredReads || w.progress.pendingBytes.Load() != 0 {
		result.recordError(fmt.Errorf("benchmark accounting mismatch"))
	}
	outcomes := result.writes.count() + result.memoryReads.count() + result.diskReads.count() +
		result.misses + result.failures + result.rejectedWrites + result.ioRejectedReads + result.busyWrites + result.busyReads
	if outcomes != result.requests {
		result.recordError(fmt.Errorf("benchmark terminal outcomes %d != requests %d", outcomes, result.requests))
	}
	return result
}

// Report admission charges; dimensions without a configured limit stay zero.
func (w *benchmarkWorkload) reportIOQueue(elapsed time.Duration) {
	d := w.cache.IODiskStats()
	fmt.Printf("DIO-DISK: elapsed=%s write-backlog=%s read-backlog=%s modeled-write-inflight=%s modeled-read-inflight=%s\n",
		elapsed.Round(time.Second), d.Writes.Backlog, d.Reads.Backlog, d.Writes.InFlight, d.Reads.InFlight)
}

type benchmarkRequest struct {
	workload *benchmarkWorkload
	stats    benchmarkRequestStats
	key      []byte
}

func (w *benchmarkRequest) write(job benchmarkJob, arrival time.Time) {
	w.stats.requests++
	w.key = strconv.AppendUint(append(w.key[:0], "key-"...), job.id, 10)
	start := time.Now()
	w.stats.writeStartDelay.record(start.Sub(arrival), 0)
	success := false
	defer func() {
		w.stats.writeService.record(time.Since(start), 0)
		if !success && w.workload.keys[job.id-1].Load() == 0 {
			w.workload.keys[job.id-1].Store(2)
		}
	}()
	ticket, err := w.prepareWrite(job)
	if err == nil {
		err = ticket.Wait()
	}
	if err != nil {
		w.writeFailure(err, job, arrival)
		return
	}
	w.workload.keys[job.id-1].Store(1)
	success = true
	elapsed := time.Since(arrival)
	w.stats.writes.record(elapsed, int64(job.size))
	w.workload.progress.writes.Add(1)
	w.workload.progress.bytes.Add(int64(job.size))
}

// Reject values above the inline threshold before allocating or copying them.
// Resident values can remain readable in RAM even when their disk writes fail
// admission.
func (w *benchmarkRequest) prepareWrite(job benchmarkJob) (Ticket, error) {
	start := time.Now()
	defer func() { w.stats.writePrepare.record(time.Since(start), 0) }()
	if w.workload.disk != nil && job.size > max(benchmarkInlineSize, int(w.workload.config.smallWriteAdmission)) {
		if err := w.workload.disk.CanAdmit(iosched.OpWrite); err != nil {
			w.stats.preflightRejected++
			w.stats.preflightRejectedBytes += int64(job.size)
			return Ticket{}, err
		}
	}
	progress := &w.workload.progress
	benchmarkMaximum(&progress.maxPreparingWrites, progress.preparingWrites.Add(1))
	benchmarkMaximum(&progress.maxPreparingBytes, progress.preparingBytes.Add(int64(job.size)))
	defer func() {
		progress.preparingWrites.Add(-1)
		progress.preparingBytes.Add(-int64(job.size))
	}()
	writer, err := w.workload.cache.NewWriter()
	if err != nil {
		return Ticket{}, err
	}
	n, err := writer.Write(w.workload.data[job.offset : job.offset+job.size])
	w.stats.copiedBytes = int64(n)
	if err == nil && n != job.size {
		err = io.ErrShortWrite
	}
	if err != nil {
		writer.Discard()
		return Ticket{}, err
	}
	return w.workload.cache.Put(w.key, writer)
}

func (w *benchmarkRequest) recordError(err error) {
	fmt.Printf("REQUEST-ERROR: utc=%s key=%s error=%v\n", time.Now().UTC().Format(time.RFC3339Nano), w.key, err)
	w.stats.failures++
	w.stats.err = err
	w.workload.progress.failures.Add(1)
}

// A joined cleanup failure must not disappear behind an expected rejection.
func benchmarkOnlyRejected(err error) bool { return benchmarkOnlyError(err, ErrRejected) }

func benchmarkOnlyError(err, target error) bool {
	if err == target {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !benchmarkOnlyError(cause, target) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return benchmarkOnlyError(wrapped.Unwrap(), target)
	}
	return false
}

func benchmarkBusy(err error) bool { return benchmarkOnlyError(err, ErrBusy) }

func (w *benchmarkRequest) writeFailure(err error, job benchmarkJob, arrival time.Time) {
	if benchmarkBusy(err) {
		w.stats.busyWrites++
		w.stats.busyBytes += int64(job.size)
		w.stats.writeBusy.record(time.Since(arrival), int64(job.size))
		w.workload.progress.busyWrites.Add(1)
		w.workload.keys[job.id-1].Store(3)
		return
	}
	if !benchmarkOnlyRejected(err) {
		w.recordError(err)
		return
	}
	w.stats.rejectedWrites++
	w.stats.rejectedBytes += int64(job.size)
	w.stats.writeRejected.record(time.Since(arrival), int64(job.size))
	w.workload.progress.rejectedWrites.Add(1)
	w.workload.progress.rejectedBytes.Add(int64(job.size))
	w.workload.keys[job.id-1].Store(3)
}

func (w *benchmarkRequest) readFailure(err error, arrival time.Time) {
	if benchmarkBusy(err) {
		w.stats.busyReads++
		w.stats.readBusy.record(time.Since(arrival), 0)
		w.workload.progress.busyReads.Add(1)
		return
	}
	if !benchmarkOnlyRejected(err) {
		w.recordError(err)
		return
	}
	w.stats.ioRejectedReads++
	w.stats.readRejected.record(time.Since(arrival), 0)
	w.workload.progress.ioRejectedReads.Add(1)
}

// Consume up to limit bytes through the cache's zero-copy API, counting only
// the requested bytes.
func benchmarkConsume(reader interface{ Next() ([]byte, error) }, limit int64) (int64, error) {
	var total int64
	for total < limit {
		data, err := reader.Next()
		total += min(int64(len(data)), limit-total)
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
		if len(data) == 0 {
			return total, io.ErrNoProgress
		}
	}
	return total, nil
}

func (w *benchmarkRequest) read(job benchmarkJob, arrival time.Time) {
	w.stats.requests++
	w.key = strconv.AppendUint(append(w.key[:0], "key-"...), job.id, 10)
	start := time.Now()
	w.stats.readStartDelay.record(start.Sub(arrival), 0)
	state := w.workload.keys[job.id-1].Load()
	pending := state == 0
	if state == 3 {
		w.stats.rejectedFillReads++
	}
	if pending {
		w.stats.pendingReads++
	}
	defer func() { w.stats.readService.record(time.Since(start), 0) }()
	// Read only the planned prefix, in per mille of the value.
	size, err := w.workload.cache.Stat(w.key)
	var reader Reader
	if err == nil {
		prefix := size
		if job.size > 0 {
			prefix = (size*int64(job.size) + 999) / 1000
		}
		reader, err = w.workload.cache.ReadRange(w.key, 0, prefix)
	}
	if errors.Is(err, ErrNotFound) {
		w.stats.misses++
		if state == 3 {
			w.stats.rejectedFillMisses++
		}
		if pending {
			w.stats.pendingMisses++
		}
		w.stats.missLatency.record(time.Since(arrival), 0)
		w.workload.progress.misses.Add(1)
		return
	}
	if err != nil {
		w.readFailure(fmt.Errorf("open reader: %w", err), arrival)
		return
	}
	expected := reader.Size()
	w.stats.readValueBytes = size
	n, err := benchmarkConsume(reader, expected)
	if err != nil {
		err = fmt.Errorf("consume reader after %d/%d bytes: %w", n, expected, err)
	}
	if closeErr := reader.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close reader: %w", closeErr))
	}
	if errors.Is(err, ErrNotFound) {
		w.stats.misses++
		if state == 3 {
			w.stats.rejectedFillMisses++
		}
		if pending {
			w.stats.pendingMisses++
		}
		w.stats.missLatency.record(time.Since(arrival), 0)
		w.workload.progress.misses.Add(1)
		return
	}
	if err == nil && n != expected {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		w.readFailure(err, arrival)
		return
	}
	elapsed := time.Since(arrival)
	if _, memory := reader.(*memoryReader); memory {
		w.stats.memoryReads.record(elapsed, n)
		w.workload.progress.memoryReadBytes.Add(n)
	} else {
		w.stats.diskReads.record(elapsed, n)
		w.workload.progress.diskReadBytes.Add(n)
	}
}

const benchmarkInlineSize = 2 << 20

type benchmarkConfig struct {
	smallWriteAdmission                             int64
	registeredMemory                                bool
	memoryBytes                                     int64
	failOnError                                     bool
	readSchedulers                                  int
	writeSchedulers                                 int
	ringDepth                                       uint32
	ioBudget                                        time.Duration
	writeBacklog, writeBacklogRecovery, readBacklog time.Duration
}

func readBenchmarkConfig(b *testing.B) benchmarkConfig {
	b.Helper()
	c := benchmarkConfig{memoryBytes: benchmarkEnvInt(b, "BLOBCACHE_CACHE_MEMORY_MB", 16384) << 20,
		smallWriteAdmission:  benchmarkEnvInt(b, "BLOBCACHE_SMALL_WRITE_KIB", 0) << 10,
		registeredMemory:     benchmarkEnvInt(b, "BLOBCACHE_REGISTERED_MEMORY", 0) != 0,
		readSchedulers:       int(benchmarkEnvInt(b, "BLOBCACHE_READ_SCHEDULERS", 1)),
		writeSchedulers:      int(benchmarkEnvInt(b, "BLOBCACHE_WRITE_SCHEDULERS", 1)),
		ringDepth:            uint32(benchmarkEnvInt(b, "BLOBCACHE_RING_DEPTH", 256)),
		ioBudget:             time.Duration(benchmarkEnvInt(b, "BLOBCACHE_IO_BUDGET_US", 1500)) * time.Microsecond,
		writeBacklog:         time.Duration(benchmarkEnvInt(b, "BLOBCACHE_WRITE_BACKLOG_MS", 0)) * time.Millisecond,
		writeBacklogRecovery: time.Duration(benchmarkEnvInt(b, "BLOBCACHE_WRITE_BACKLOG_RECOVERY_MS", -1)) * time.Millisecond,
		readBacklog:          time.Duration(benchmarkEnvInt(b, "BLOBCACHE_READ_BACKLOG_MS", 0)) * time.Millisecond,
		failOnError:          true}
	if c.ringDepth < 2 {
		b.Fatal("BLOBCACHE_RING_DEPTH must be at least 2")
	}
	if c.writeBacklog > 0 && c.writeBacklogRecovery >= c.writeBacklog {
		b.Fatal("BLOBCACHE_WRITE_BACKLOG_RECOVERY_MS must be less than BLOBCACHE_WRITE_BACKLOG_MS")
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
	return c
}

func benchmarkEnvInt(b *testing.B, name string, fallback int64) int64 {
	b.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		b.Fatalf("invalid %s=%q", name, value)
	}
	return n
}

func newBenchmarkWorkload(b *testing.B, device *nvme.Device) *benchmarkWorkload {
	b.Helper()
	w := &benchmarkWorkload{config: readBenchmarkConfig(b), data: make([]byte, 32<<20)}
	if err := Format(device); err != nil {
		b.Fatal(err)
	}
	opened := time.Now()
	w.disk = iosched.NewDiskPolicy(iosched.WithLatencyGoal(w.config.ioBudget), iosched.WithoutReadIOBudget(),
		iosched.WithWriteBacklogLimit(w.config.writeBacklog), iosched.WithWriteBacklogRecovery(w.config.writeBacklogRecovery),
		iosched.WithReadBacklogLimit(w.config.readBacklog))
	opts := []Option{
		WithMemory(w.config.memoryBytes, alloc.WithWarmup()),
		WithInlineThreshold(benchmarkInlineSize),
		WithReadSchedulers(w.config.readSchedulers), WithWriteSchedulers(w.config.writeSchedulers),
		WithRingDepth(w.config.ringDepth), WithDiskPolicy(w.disk),
		WithSmallWriteAdmission(w.config.smallWriteAdmission),
	}
	if w.config.registeredMemory {
		opts = append(opts, WithRegisteredMemory())
	}
	var err error
	w.cache, err = New(device, opts...)
	if err != nil {
		b.Fatal(err)
	}
	fmt.Printf("CACHE-OPEN: device=%s offset=%d size=%d elapsed=%s recovered-bytes=%d\n",
		device.Info().CharDevice, device.Offset(), device.Size(), time.Since(opened), w.cache.store.Size())
	b.Cleanup(func() {
		if err := w.cache.Close(); err != nil {
			b.Error(err)
		}
	})
	for i, s := range w.cache.store.IOSchedulerStats() {
		fmt.Printf("IO-SCHEDULER: id=%d role=%s ring-depth=%d read-io-budget=0s write-io-budget=%s budget-scope=disk write-backlog=%s write-backlog-recovery=%s read-backlog=%s\n",
			i, s.Role, w.config.ringDepth, s.WriteIOBudget, w.config.writeBacklog, w.config.writeBacklogRecovery, w.config.readBacklog)
	}
	if _, err := cryptorand.Read(w.data); err != nil {
		b.Fatal(err)
	}
	fmt.Printf("\nWORKLOAD: cpus=%d offered-fills=%d warmup-fills=%d locality=skewed-mean-3.6 sizes=8KiB..32MiB inline=2MiB memory=%dMiB registered-memory=%t disk=%.1fGiB read-consumption=Next\n",
		runtime.GOMAXPROCS(0), b.N, min(b.N, benchmarkWarmupFills), w.config.memoryBytes>>20, w.config.registeredMemory, float64(device.Size())/(1<<30))
	return w
}

// The fixed reference is shared with ringcache and independent of cache capacity.
var benchmarkWarmupFills = benchmarkWarmupWrites(16<<30, benchmarkInlineSize)

type benchmarkMeasurements struct {
	latency *hdrhistogram.Histogram
	bytes   int64
}

// Keep one compact sample per request. Histograms are shared and updated only
// after the request finishes, so outstanding I/O does not allocate histograms.
type benchmarkSample struct {
	latency  time.Duration
	bytes    int64
	recorded bool
}

func (s *benchmarkSample) record(latency time.Duration, bytes int64) {
	*s = benchmarkSample{latency: latency, bytes: bytes, recorded: true}
}

type benchmarkRequestStats struct {
	readValueBytes                                                          int64
	copiedBytes, preflightRejected, preflightRejectedBytes                  int64
	writePrepare                                                            benchmarkSample
	requests, failures, misses                                              int64
	pendingReads, pendingMisses                                             int64
	rejectedWrites, rejectedBytes, ioRejectedReads                          int64
	busyWrites, busyReads, busyBytes                                        int64
	rejectedFillReads, rejectedFillMisses                                   int64
	writes, memoryReads, diskReads                                          benchmarkSample
	writeRejected, readRejected, writeBusy, readBusy                        benchmarkSample
	writeStartDelay, writeService, readStartDelay, readService, missLatency benchmarkSample
	err                                                                     error
}

func (m benchmarkMeasurements) count() int64 { return m.latency.TotalCount() }

func (m *benchmarkMeasurements) record(latency time.Duration, bytes int64) {
	m.latency.RecordValue(int64(latency))
	m.bytes += bytes
}

type benchmarkStats struct {
	readsBySize                                                             [len(benchmarkReadSizeBands)]struct{ memory, disk benchmarkMeasurements }
	copiedBytes, preflightRejected, preflightRejectedBytes                  int64
	maxPreparingWrites, maxPreparingBytes                                   int64
	writePrepare                                                            benchmarkMeasurements
	offeredElapsed                                                          time.Duration
	requests, failures, misses                                              int64
	elapsed, scheduled                                                      time.Duration
	offeredWrites, offeredReads, offeredBytes                               int64
	rejectedWrites, rejectedBytes, ioRejectedReads                          int64
	busyWrites, busyReads, busyBytes                                        int64
	writeBusy, readBusy                                                     benchmarkMeasurements
	rejectedFillReads, rejectedFillMisses                                   int64
	writeRejected, readRejected                                             benchmarkMeasurements
	maxPendingBytes, maxActiveWrites, maxActiveReads                        int64
	pendingReads, pendingMisses                                             int64
	writes, memoryReads, diskReads                                          benchmarkMeasurements
	writeStartDelay, writeService, readStartDelay, readService, missLatency benchmarkMeasurements
	burstDuration                                                           time.Duration
	bursts, completeBursts                                                  int
	sizes                                                                   *hdrhistogram.Histogram
	err                                                                     error
}

func newBenchmarkStats() *benchmarkStats {
	latency := func() benchmarkMeasurements {
		return benchmarkMeasurements{latency: hdrhistogram.New(1, math.MaxInt64, 3)}
	}
	s := &benchmarkStats{writePrepare: latency(), writes: latency(), memoryReads: latency(), diskReads: latency(),
		writeBusy: latency(), readBusy: latency(), writeRejected: latency(), readRejected: latency(), writeStartDelay: latency(), writeService: latency(), readStartDelay: latency(), readService: latency(), missLatency: latency(),
		sizes: hdrhistogram.New(1, 32<<20, 3)}
	for i := range s.readsBySize {
		s.readsBySize[i].memory, s.readsBySize[i].disk = latency(), latency()
	}
	return s
}

// Bucket by the full value size, even when a read consumes only a prefix.
// Bounds are inclusive above and exclusive below the preceding bound.
var benchmarkReadSizeBands = [...]struct {
	max   int64
	label string
}{
	{256 << 10, "<=256KiB"},
	{1 << 20, "256KiB..1MiB"},
	{2 << 20, "1MiB..2MiB"},
	{4 << 20, "2MiB..4MiB"},
	{8 << 20, "4MiB..8MiB"},
	{math.MaxInt64, ">8MiB"},
}

func (s *benchmarkStats) recordError(err error) {
	s.failures++
	if s.err == nil {
		s.err = err
	}
}

func (s *benchmarkStats) record(other *benchmarkRequestStats) {
	if s.err == nil {
		s.err = other.err
	}
	s.copiedBytes += other.copiedBytes
	s.preflightRejected += other.preflightRejected
	s.preflightRejectedBytes += other.preflightRejectedBytes
	s.requests += other.requests
	s.failures += other.failures
	s.misses += other.misses
	s.pendingReads += other.pendingReads
	s.pendingMisses += other.pendingMisses
	s.rejectedWrites += other.rejectedWrites
	s.rejectedBytes += other.rejectedBytes
	s.ioRejectedReads += other.ioRejectedReads
	s.busyWrites += other.busyWrites
	s.busyReads += other.busyReads
	s.busyBytes += other.busyBytes
	s.rejectedFillReads += other.rejectedFillReads
	s.rejectedFillMisses += other.rejectedFillMisses
	for _, pair := range []struct {
		to   *benchmarkMeasurements
		from benchmarkSample
	}{
		{&s.writeBusy, other.writeBusy}, {&s.readBusy, other.readBusy},
		{&s.writeRejected, other.writeRejected}, {&s.readRejected, other.readRejected},
		{&s.writes, other.writes}, {&s.memoryReads, other.memoryReads}, {&s.diskReads, other.diskReads},
		{&s.writePrepare, other.writePrepare}, {&s.writeStartDelay, other.writeStartDelay}, {&s.writeService, other.writeService},
		{&s.readStartDelay, other.readStartDelay}, {&s.readService, other.readService}, {&s.missLatency, other.missLatency},
	} {
		if pair.from.recorded {
			pair.to.record(pair.from.latency, pair.from.bytes)
		}
	}
	if other.writes.recorded {
		s.sizes.RecordValue(other.writes.bytes)
	}
	if other.memoryReads.recorded || other.diskReads.recorded {
		for i, band := range benchmarkReadSizeBands {
			if other.readValueBytes > band.max {
				continue
			}
			if sample := other.memoryReads; sample.recorded {
				s.readsBySize[i].memory.record(sample.latency, sample.bytes)
			}
			if sample := other.diskReads; sample.recorded {
				s.readsBySize[i].disk.record(sample.latency, sample.bytes)
			}
			break
		}
	}
}

func (s *benchmarkStats) report(b *testing.B) {
	fmt.Printf("ARRIVALS: writes=%d reads=%d bytes=%d scheduled=%s offered-duration=%s final-drain=%s write-GB/s=%.6f read-requests/s=%.3f\n",
		s.offeredWrites, s.offeredReads, s.offeredBytes, s.scheduled, s.offeredElapsed, max(0, s.elapsed-s.offeredElapsed),
		float64(s.offeredBytes)/s.elapsed.Seconds()/1e9, float64(s.offeredReads)/s.elapsed.Seconds())
	fmt.Printf("DISPATCH: mode=goroutine-per-arrival peak-pending-bytes=%d peak-active-writes=%d peak-active-reads=%d\n",
		s.maxPendingBytes, s.maxActiveWrites, s.maxActiveReads)
	fmt.Printf("WRITE-PREPARATION: copied-bytes=%d rejected-before-writer=%d rejected-before-writer-bytes=%d peak-writes=%d peak-bytes=%d scope=before-ticket-wait\n", s.copiedBytes, s.preflightRejected, s.preflightRejectedBytes, s.maxPreparingWrites, s.maxPreparingBytes)
	fmt.Printf("REJECTIONS: writes=%d write-bytes=%d reads=%d scope=dio-admission no-retries=true\n", s.rejectedWrites, s.rejectedBytes, s.ioRejectedReads)
	fmt.Printf("CACHE-BUSY: writes=%d write-bytes=%d reads=%d no-retries=true\n", s.busyWrites, s.busyBytes, s.busyReads)
	fmt.Printf("REJECTED-FILL-READS: reads=%d misses=%d\n", s.rejectedFillReads, s.rejectedFillMisses)
	fmt.Printf("LATENCY-SCOPE: write/memory-read/disk-read=successful-requests start-delay/service=all-attempts rejection=separate\n")
	fmt.Printf("REQUESTS: total=%d writes=%d memory-reads=%d disk-reads=%d misses=%d errors=%d elapsed=%s\n",
		s.requests, s.writes.count(), s.memoryReads.count(), s.diskReads.count(), s.misses, s.failures, s.elapsed)
	for _, metric := range []struct {
		name      string
		histogram *hdrhistogram.Histogram
	}{
		{"write-cache-busy", s.writeBusy.latency}, {"read-cache-busy", s.readBusy.latency},
		{"write-rejected", s.writeRejected.latency}, {"read-rejected", s.readRejected.latency},
		{"write-prepare", s.writePrepare.latency}, {"write", s.writes.latency}, {"Put", metrics.PutDuration.Snapshot()},
		{"write-start-delay", s.writeStartDelay.latency}, {"write-service", s.writeService.latency},
		{"read-start-delay", s.readStartDelay.latency}, {"read-service-including-misses", s.readService.latency}, {"read-miss", s.missLatency.latency},
		{"memory-read", s.memoryReads.latency}, {"disk-read", s.diskReads.latency},
		{"read-slat", metrics.ReadSlat.Snapshot()}, {"read-clat", metrics.ReadClat.Snapshot()},
	} {
		h := metric.histogram
		fmt.Printf("LATENCY %s: n=%d p50=%s p99=%s p99.9=%s max=%s mean=%s\n", metric.name, h.TotalCount(),
			time.Duration(h.ValueAtQuantile(50)), time.Duration(h.ValueAtQuantile(99)), time.Duration(h.ValueAtQuantile(99.9)), time.Duration(h.Max()), time.Duration(h.Mean()))
		b.ReportMetric(float64(h.ValueAtQuantile(99))/1e6, metric.name+"-p99-ms")
	}
	for i, band := range benchmarkReadSizeBands {
		all := benchmarkMeasurements{
			latency: hdrhistogram.New(1, math.MaxInt64, 3),
			bytes:   s.readsBySize[i].memory.bytes + s.readsBySize[i].disk.bytes,
		}
		all.latency.Merge(s.readsBySize[i].memory.latency)
		all.latency.Merge(s.readsBySize[i].disk.latency)
		for _, path := range []struct {
			name    string
			samples benchmarkMeasurements
		}{{"all", all}, {"memory", s.readsBySize[i].memory}, {"disk", s.readsBySize[i].disk}} {
			h := path.samples.latency
			if h.TotalCount() == 0 {
				continue
			}
			fmt.Printf("READ-SIZE: value-size=%s path=%s n=%d returned-bytes=%d p50=%s p99=%s p99.9=%s mean=%s\n",
				band.label, path.name, h.TotalCount(), path.samples.bytes,
				time.Duration(h.ValueAtQuantile(50)), time.Duration(h.ValueAtQuantile(99)), time.Duration(h.ValueAtQuantile(99.9)), time.Duration(h.Mean()))
		}
	}
	fmt.Printf("PENDING-READS: reads=%d misses=%d (fill-not-yet-published-at-read-start)\n", s.pendingReads, s.pendingMisses)
	fmt.Printf("BURST-SUMMARY: count=%d complete=%d partial=%d total-duration=%s scheduled=%s\n",
		s.bursts, s.completeBursts, s.bursts-s.completeBursts, s.burstDuration, s.scheduled)
	b.ReportMetric(float64(s.bursts), "bursts")
	b.ReportMetric(s.burstDuration.Seconds(), "burst-seconds")
	seconds := s.elapsed.Seconds()
	fmt.Printf("PAYLOAD: written=%.3fGiB disk-read=%.3fGiB memory-read=%.3fGiB write-size-p50=%.1fKiB p99=%.1fKiB\n",
		float64(s.writes.bytes)/(1<<30), float64(s.diskReads.bytes)/(1<<30), float64(s.memoryReads.bytes)/(1<<30),
		float64(s.sizes.ValueAtQuantile(50))/1024, float64(s.sizes.ValueAtQuantile(99))/1024)
	b.ReportMetric(float64(s.writes.bytes)/(1<<30)/seconds, "write-GiB/s")
	b.ReportMetric(float64(s.diskReads.bytes)/(1<<30)/seconds, "disk-read-GiB/s")
	b.ReportMetric(float64(s.memoryReads.bytes)/(1<<30)/seconds, "memory-read-GiB/s")
	b.ReportMetric(float64(s.requests-s.failures-s.rejectedWrites-s.ioRejectedReads-s.busyWrites-s.busyReads)/seconds, "completed-requests/s")
	b.ReportMetric(float64(s.writes.count())/seconds, "completed-writes/s")
	b.ReportMetric(float64(s.failures), "request-errors")
	b.ReportMetric(float64(s.rejectedWrites), "rejected-writes")
	b.ReportMetric(float64(s.rejectedBytes), "rejected-write-bytes")
	b.ReportMetric(float64(s.ioRejectedReads), "io-rejected-reads")
	b.ReportMetric(float64(s.busyWrites), "cache-busy-writes")
	b.ReportMetric(float64(s.busyReads), "cache-busy-reads")
	fmt.Printf("ERRORS: NewWriter=%d Put=%d scope=api-errors-including-rejections\n",
		metrics.WriterErrors.Load(), metrics.PutErrors.Load())
	if s.err != nil {
		fmt.Printf("ERROR: %v\n", s.err)
	}
}

const benchmarkPeakRate = 2_000_000_000

// Shared size mix: 20% up to 256 KiB, 50% from 512 KiB to 1.5 MiB,
// and an expected mean of about 912 KiB. Sizes are log-uniform within each
// band; weights are per mille. Keep both cache benchmarks identical.
var benchmarkSizeBands = [...]struct{ low, high, weight int }{
	{8 << 10, 32 << 10, 50},
	{32 << 10, 256 << 10, 150},
	{256 << 10, 512 << 10, 200},
	{512 << 10, 1536 << 10, 500},
	{1536 << 10, 2 << 20, 50},
	{2 << 20, 4 << 20, 30},
	{4 << 20, 8 << 20, 15},
	{8 << 20, 32 << 20, 5},
}

func benchmarkBlobSize(rng *rand.Rand) int {
	choice := rng.IntN(1000)
	for _, band := range benchmarkSizeBands {
		if choice < band.weight {
			return int(float64(band.low) * math.Exp(rng.Float64()*math.Log(float64(band.high)/float64(band.low))))
		}
		choice -= band.weight
	}
	panic("invalid size distribution")
}

// benchmarkWarmupWrites estimates the fills that offer 125% of memoryBytes as
// inline payload: values up to inlineSize.
func benchmarkWarmupWrites(memoryBytes, inlineSize int64) int {
	var inline float64 // expected inline bytes per fill
	for _, band := range benchmarkSizeBands {
		if int64(band.high) <= inlineSize {
			mean := float64(band.high-band.low) / math.Log(float64(band.high)/float64(band.low))
			inline += mean * float64(band.weight) / 1000
		}
	}
	return int(math.Ceil(float64(memoryBytes) * 5 / 4 / inline))
}

type benchmarkCase struct {
	name       string
	writeRate  int64
	bursty     bool
	profile    *benchmarkBurstProfile
	readMaxAge time.Duration // oldest read age; zero means two minutes
	// readHeadShare percent of reads come at ages up to readHeadAge, the rest
	// from there to readMaxAge, each log-uniform. Zero: one log-uniform range.
	readHeadAge   time.Duration
	readHeadShare int
}

func (c benchmarkCase) maxAge() time.Duration {
	if c.readMaxAge > 0 {
		return c.readMaxAge
	}
	return 2 * time.Minute
}

// A variable cycle is background, then a burst, then, when recoveryRate is
// set, a recovery interval right after the burst. Rates are drawn per cycle
// from [backgroundMin, backgroundMax], [peakMin, peakRate] when peakMin is set,
// and [recoveryRate, recoveryRateMax] when recoveryRateMax is set.
type benchmarkBurstProfile struct {
	backgroundMin, backgroundMax, peakRate int64
	burstMin, burstMax, cycleMin, cycleMax time.Duration
	recoveryRate                           int64
	recoveryMin, recoveryMax               time.Duration
	peakMin, recoveryRateMax               int64
	// longBurst, when set, replaces the burst of cycle longBurstCycle.
	longBurst      time.Duration
	longBurstCycle int
}

func (p benchmarkBurstProfile) validate() error {
	if p.backgroundMin <= 0 || p.backgroundMax < p.backgroundMin || (p.peakMin == 0 && p.peakRate <= p.backgroundMax) {
		return errors.New("benchmark: require 0 < background min <= background max < burst rate")
	}
	if p.peakMin < 0 || p.peakRate < p.peakMin || (p.recoveryRateMax != 0 && p.recoveryRateMax < p.recoveryRate) {
		return errors.New("benchmark: require burst min <= burst rate and recovery min <= recovery max rate")
	}
	if p.burstMin <= 0 || p.burstMax < p.burstMin || p.cycleMin <= p.burstMax || p.cycleMax < p.cycleMin {
		return errors.New("benchmark: require 0 < burst min <= burst max < cycle min <= cycle max")
	}
	if p.recoveryRate > 0 && (max(p.recoveryRate, p.recoveryRateMax) >= p.peakRate || p.recoveryMin <= 0 || p.recoveryMax < p.recoveryMin ||
		p.burstMax+p.recoveryMax >= p.cycleMin) {
		return errors.New("benchmark: require recovery rate < burst rate and 0 < recovery min <= recovery max, burst max + recovery max < cycle min")
	}
	return nil
}

func readBenchmarkBurstProfile(b *testing.B) *benchmarkBurstProfile {
	b.Helper()
	p := &benchmarkBurstProfile{
		backgroundMin:   benchmarkEnvInt(b, "BLOBCACHE_BACKGROUND_MIN_MBPS", 750) * 1_000_000,
		backgroundMax:   benchmarkEnvInt(b, "BLOBCACHE_BACKGROUND_MAX_MBPS", 1225) * 1_000_000,
		peakRate:        benchmarkEnvInt(b, "BLOBCACHE_BURST_MBPS", 2000) * 1_000_000,
		peakMin:         benchmarkEnvInt(b, "BLOBCACHE_BURST_MIN_MBPS", 0) * 1_000_000,
		burstMin:        time.Duration(benchmarkEnvInt(b, "BLOBCACHE_BURST_MIN_MS", 2000)) * time.Millisecond,
		burstMax:        time.Duration(benchmarkEnvInt(b, "BLOBCACHE_BURST_MAX_MS", 4000)) * time.Millisecond,
		cycleMin:        time.Duration(benchmarkEnvInt(b, "BLOBCACHE_BURST_CYCLE_MIN_MS", 12000)) * time.Millisecond,
		cycleMax:        time.Duration(benchmarkEnvInt(b, "BLOBCACHE_BURST_CYCLE_MAX_MS", 18000)) * time.Millisecond,
		recoveryRate:    benchmarkEnvInt(b, "BLOBCACHE_RECOVERY_MBPS", 0) * 1_000_000,
		recoveryRateMax: benchmarkEnvInt(b, "BLOBCACHE_RECOVERY_MAX_MBPS", 0) * 1_000_000,
		recoveryMin:     time.Duration(benchmarkEnvInt(b, "BLOBCACHE_RECOVERY_MIN_MS", 3000)) * time.Millisecond,
		recoveryMax:     time.Duration(benchmarkEnvInt(b, "BLOBCACHE_RECOVERY_MAX_MS", 5000)) * time.Millisecond,
		longBurst:       time.Duration(benchmarkEnvInt(b, "BLOBCACHE_LONG_BURST_S", 0)) * time.Second,
		longBurstCycle:  int(benchmarkEnvInt(b, "BLOBCACHE_LONG_BURST_CYCLE", 0)),
	}
	if err := p.validate(); err != nil {
		b.Fatal(err)
	}
	fmt.Printf("BURST-PROFILE: background-min=%dB/s background-max=%dB/s peak-min=%dB/s peak=%dB/s duration-min=%s duration-max=%s cycle-min=%s cycle-max=%s recovery=%dB/s recovery-rate-max=%dB/s recovery-min=%s recovery-max=%s long-burst=%s long-burst-cycle=%d rates=uniform-per-cycle average=unconstrained\n",
		p.backgroundMin, p.backgroundMax, p.peakMin, p.peakRate, p.burstMin, p.burstMax, p.cycleMin, p.cycleMax, p.recoveryRate, p.recoveryRateMax, p.recoveryMin, p.recoveryMax, p.longBurst, p.longBurstCycle)
	return p
}

type benchmarkJob struct {
	at           time.Duration
	id           uint64
	size, offset int
}

type benchmarkBurst struct{ start, end time.Duration }

type benchmarkBoundary struct {
	at    time.Duration
	event string
	burst int
}

type benchmarkPlan struct {
	writes, reads []benchmarkJob
	bursts        []benchmarkBurst
	burstRates    []int64 // per burst, when the profile draws burst rates
	duration      time.Duration
	bytes         int64
	peakRate      int64
	readCounts    [benchmarkMaxReads]int // Assigned attempts per new key, including those not yet due.
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
			ages := benchmarkReadAges(write.id, c)
			// Reads take a prefix of the value: 50-100%, in per mille, as size.
			prefixes := rand.New(rand.NewPCG(47, write.id))
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
				p.reads = append(p.reads, benchmarkJob{at: at, id: write.id, size: 500 + prefixes.IntN(501)})
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

const benchmarkMaxReads = 44

// Read quotas are skewed with a mean of about 3.7 reads per key, production's
// hits per cached entry: 50% once, 20% twice, 15% 3-5, 10% 6-10 and 5% 11-44.
// Ages are log-uniform from 100ms to c's maximum, or split at its head age.
// Production measures only the mean; the skew is an assumption.
// Sort independently drawn ages so extra attempts follow the first attempt.
// The RNG is keyed by ID: phase boundaries and cache outcomes cannot resample it.
func benchmarkReadAges(id uint64, c benchmarkCase) []time.Duration {
	rng := rand.New(rand.NewPCG(46, id))
	count := 1
	switch choice := rng.IntN(100); {
	case choice >= 95:
		count = 11 + rng.IntN(34)
	case choice >= 85:
		count = 6 + rng.IntN(5)
	case choice >= 70:
		count = 3 + rng.IntN(3)
	case choice >= 50:
		count = 2
	}
	ages := make([]time.Duration, count)
	for i := range ages {
		low, high := 100*time.Millisecond, c.maxAge()
		if c.readHeadShare > 0 {
			if rng.IntN(100) < c.readHeadShare {
				high = c.readHeadAge
			} else {
				low = c.readHeadAge
			}
		}
		ages[i] = time.Duration(float64(low) * math.Exp(rng.Float64()*math.Log(float64(high)/float64(low))))
	}
	slices.Sort(ages)
	return ages
}

func makeBenchmarkWritePlan(n int, base uint64, dataSize int, c benchmarkCase) benchmarkPlan {
	p := benchmarkPlan{writes: make([]benchmarkJob, n), peakRate: benchmarkPeakRate}
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
	if c.profile != nil {
		profile := c.profile
		p.peakRate = profile.peakRate
		periods = nil
		rng := rand.New(rand.NewPCG(45, base))
		var end, bytes float64
		for bytes < float64(p.bytes) {
			cycle := profile.cycleMin.Seconds() + rng.Float64()*(profile.cycleMax-profile.cycleMin).Seconds()
			duration := profile.burstMin.Seconds() + rng.Float64()*(profile.burstMax-profile.burstMin).Seconds()
			background := float64(profile.backgroundMin) + rng.Float64()*float64(profile.backgroundMax-profile.backgroundMin)
			var recovery float64
			if profile.recoveryRate > 0 {
				recovery = profile.recoveryMin.Seconds() + rng.Float64()*(profile.recoveryMax-profile.recoveryMin).Seconds()
			}
			// Draw only for configured ranges, so fixed-rate traces stay unchanged.
			peak, recoveryRate := float64(profile.peakRate), float64(profile.recoveryRate)
			if profile.peakMin > 0 {
				peak = float64(profile.peakMin) + rng.Float64()*float64(profile.peakRate-profile.peakMin)
				p.burstRates = append(p.burstRates, int64(peak))
			}
			if profile.recoveryRateMax > 0 {
				recoveryRate += rng.Float64() * float64(profile.recoveryRateMax-profile.recoveryRate)
			}
			quiet := cycle - duration - recovery
			if profile.longBurst > 0 && len(p.bursts) == profile.longBurstCycle {
				duration = profile.longBurst.Seconds()
				cycle = quiet + duration + recovery
			}
			burstEnd := end + cycle - recovery // exactly end+cycle without recovery
			periods = append(periods, period{end + quiet, background}, period{burstEnd, peak})
			if recovery > 0 {
				periods = append(periods, period{end + cycle, recoveryRate})
			}
			p.bursts = append(p.bursts, benchmarkBurst{time.Duration((end + quiet) * float64(time.Second)), time.Duration(burstEnd * float64(time.Second))})
			bytes += quiet*background + duration*peak + recovery*recoveryRate
			end += cycle
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
	}
	p.duration = time.Duration(clock * float64(time.Second))
	return p
}

// Boundaries label backlog snapshots without assigning requests to phases.
func (p *benchmarkPlan) burstBoundaries() []benchmarkBoundary {
	var events []benchmarkBoundary
	for i, burst := range p.bursts {
		if burst.start >= p.duration {
			break
		}
		events = append(events, benchmarkBoundary{burst.start, "burst-start", i},
			benchmarkBoundary{min(burst.end, p.duration), "burst-end", i})
	}
	if len(events) == 0 || events[len(events)-1].at < p.duration {
		events = append(events, benchmarkBoundary{p.duration, "drain", -1})
	}
	return events
}

func (p *benchmarkPlan) burstSummary() (count, complete int, duration time.Duration) {
	for _, burst := range p.bursts {
		if burst.start < p.duration {
			count++
			duration += min(burst.end, p.duration) - burst.start
			if burst.end <= p.duration {
				complete++
			}
		}
	}
	return
}
