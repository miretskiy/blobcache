package blobcache

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/stretchr/testify/require"
)

func TestBenchmarkSizeHistogramIncludesLargeValues(t *testing.T) {
	s := newBenchmarkStats()
	for _, size := range []int64{16 << 20, (32 << 20) - 1} {
		s.record(&benchmarkRequestStats{writes: benchmarkSample{recorded: true, bytes: size}})
	}
	require.EqualValues(t, 2, s.sizes.TotalCount())
	require.GreaterOrEqual(t, s.sizes.ValueAtQuantile(99), int64((32<<20)-1))
}

func TestBenchmarkReadSizeStats(t *testing.T) {
	s := newBenchmarkStats()
	for _, tc := range []struct {
		size int64
		band int
	}{
		{8 << 10, 0}, {256 << 10, 0}, {256<<10 + 1, 1},
		{1 << 20, 1}, {1<<20 + 1, 2}, {2 << 20, 2},
		{2<<20 + 1, 3}, {4 << 20, 3}, {4<<20 + 1, 4},
		{8 << 20, 4}, {8<<20 + 1, 5}, {32 << 20, 5},
	} {
		memory, disk := s.readsBySize[tc.band].memory.count(), s.readsBySize[tc.band].disk.count()
		// The small consumed prefix must not determine the size bucket.
		sample := benchmarkSample{latency: time.Millisecond, bytes: 1024, recorded: true}
		s.record(&benchmarkRequestStats{readValueBytes: tc.size, memoryReads: sample})
		require.Equal(t, memory+1, s.readsBySize[tc.band].memory.count())
		require.Equal(t, disk, s.readsBySize[tc.band].disk.count())
		s.record(&benchmarkRequestStats{readValueBytes: tc.size, diskReads: sample})
		require.Equal(t, disk+1, s.readsBySize[tc.band].disk.count())
	}
	s.record(&benchmarkRequestStats{misses: 1})
	var memory, disk int64
	for _, band := range s.readsBySize {
		memory += band.memory.count()
		disk += band.disk.count()
	}
	require.Equal(t, s.memoryReads.count(), memory)
	require.Equal(t, s.diskReads.count(), disk)
	require.EqualValues(t, 12, memory)
	require.EqualValues(t, 12, disk)
	require.EqualValues(t, 1, s.misses)
}

// benchmarkPlanDigest fingerprints an offered workload: every fill's arrival,
// key, size and payload offset, every read's arrival and key, and the bursts.
// ringcache's harness pins the same digests, so both caches are offered the
// identical trace; change them together.
func benchmarkPlanDigest(p benchmarkPlan) string {
	h := sha256.New()
	put := func(values ...int64) {
		for _, v := range values {
			_ = binary.Write(h, binary.LittleEndian, v)
		}
	}
	for _, w := range p.writes {
		put(int64(w.at), int64(w.id), int64(w.size), int64(w.offset))
	}
	put(-1)
	for _, r := range p.reads {
		put(int64(r.at), int64(r.id), int64(r.size))
	}
	put(-1)
	for _, b := range p.bursts {
		put(int64(b.start), int64(b.end))
	}
	put(int64(p.duration), p.bytes, p.peakRate, int64(p.carryReads), int64(p.futureReads))
	for _, c := range p.readCounts {
		put(int64(c))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func benchmarkTraceDigests() map[string]string {
	const data = 32 << 20
	warmup := benchmarkWarmupWrites(16<<30, 2<<20)
	digests := map[string]string{
		"warmup": benchmarkPlanDigest(makeBenchmarkPlan(warmup, 0, data, benchmarkCase{writeRate: 1_000_000_000})),
	}
	for name, c := range map[string]benchmarkCase{
		"steady-1GBps":   {writeRate: 1_000_000_000},
		"bursty-1p2GBps": {writeRate: 1_200_000_000, bursty: true},
	} {
		digests[name] = benchmarkPlanDigest(makeBenchmarkPlan(700000, uint64(warmup), data, c))
	}
	// The production-shaped comparison run: background 250-750MB/s; 45-90s
	// bursts at 1.5-2GB/s (80-100% of NIC) every 3-5 minutes, the fourth one
	// six minutes long, straight back to background; 24% of reads at ages up
	// to 24s and the rest to 30 minutes; four million fills.
	production := &benchmarkBurstProfile{backgroundMin: 250_000_000, backgroundMax: 750_000_000,
		peakMin: 1_500_000_000, peakRate: 2_000_000_000, burstMin: 45 * time.Second, burstMax: 90 * time.Second,
		cycleMin: 180 * time.Second, cycleMax: 300 * time.Second, longBurst: 360 * time.Second, longBurstCycle: 3}
	digests["production"] = benchmarkPlanDigest(makeBenchmarkPlan(4000000, uint64(warmup), data,
		benchmarkCase{profile: production, readMaxAge: 30 * time.Minute, readHeadAge: 24 * time.Second, readHeadShare: 24}))
	return digests
}

func TestBenchmarkTraceDigests(t *testing.T) {
	got := benchmarkTraceDigests()
	for name, digest := range got {
		t.Logf("%s %s", name, digest)
	}
	require.Equal(t, benchmarkExpectedTraceDigests, got)
}

var benchmarkExpectedTraceDigests = map[string]string{
	"warmup":         "e35640bfb3c7472e7f6b18290bed823b23d5ddadcd74f08e845eb70d9a2bb74b",
	"bursty-1p2GBps": "66fa899d841803623a51ff76b7d31322a90a67de2f2d3a522bf44cd8365fe600",
	"steady-1GBps":   "0397f744bb63534721f9792dd017ce9fb20b422734a82113899ac521980b5337",
	"production":     "31eabc7f189adeaf1768d54fab939f2877dc3843a0cf74dfc1db183fb581b9b9",
}

func TestBenchmarkBurstPlan(t *testing.T) {
	for _, fills := range []int{600000, 1750000} {
		plan := makeBenchmarkWritePlan(fills, 27688, 32<<20, benchmarkCase{writeRate: 1_200_000_000, bursty: true})
		count, complete, total := plan.burstSummary()
		require.GreaterOrEqual(t, complete, 2, "large workload must contain sustained complete bursts")
		for _, burst := range plan.bursts {
			require.GreaterOrEqual(t, burst.end-burst.start, 8*time.Second)
			require.LessOrEqual(t, burst.end-burst.start, 16*time.Second)
			if burst.end > plan.duration {
				continue
			}
			var offered int64
			for _, job := range plan.writes {
				if job.at >= burst.start && job.at < burst.end {
					offered += int64(job.size)
				}
			}
			// Whole values straddling either boundary can differ by at most two
			// maximum-sized writes from the byte-paced interval's exact target.
			require.InDelta(t, (burst.end-burst.start).Seconds()*benchmarkPeakRate, offered, 2*(32<<20))
		}
		if fills == 1750000 {
			require.GreaterOrEqual(t, complete, 6)
		}
		t.Logf("fills=%d bytes=%d scheduled=%s burst-count=%d complete=%d total-burst-duration=%s", fills, plan.bytes, plan.duration, count, complete, total)
		for i, burst := range plan.bursts {
			t.Logf("burst=%d start=%s end=%s", i, burst.start, burst.end)
		}
	}
}

func TestBenchmarkVariableBurstPlan(t *testing.T) {
	profile := &benchmarkBurstProfile{backgroundMin: 750_000_000, backgroundMax: 1_225_000_000, peakRate: 2_000_000_000,
		burstMin: 2 * time.Second, burstMax: 4 * time.Second, cycleMin: 12 * time.Second, cycleMax: 18 * time.Second}
	require.NoError(t, profile.validate())
	c := benchmarkCase{profile: profile}
	plan := makeBenchmarkWritePlan(500000, 0, 32<<20, c)
	require.Equal(t, plan, makeBenchmarkWritePlan(500000, 0, 32<<20, c), "same seeded trace")
	t.Logf("offered-bytes=%d scheduled=%s average=%.3fGB/s", plan.bytes, plan.duration, float64(plan.bytes)/plan.duration.Seconds()/1e9)
	_, complete, _ := plan.burstSummary()
	require.Greater(t, complete, 5)
	var lastEnd time.Duration
	for _, burst := range plan.bursts {
		cycle := burst.end - lastEnd
		require.GreaterOrEqual(t, cycle, profile.cycleMin)
		require.LessOrEqual(t, cycle, profile.cycleMax)
		require.GreaterOrEqual(t, burst.end-burst.start, profile.burstMin)
		require.LessOrEqual(t, burst.end-burst.start, profile.burstMax)
		if burst.end <= plan.duration {
			var background, peak int64
			for _, job := range plan.writes {
				if job.at >= lastEnd && job.at < burst.start {
					background += int64(job.size)
				} else if job.at >= burst.start && job.at < burst.end {
					peak += int64(job.size)
				}
			}
			quiet := (burst.start - lastEnd).Seconds()
			require.GreaterOrEqual(t, float64(background), quiet*float64(profile.backgroundMin)-2*(32<<20))
			require.LessOrEqual(t, float64(background), quiet*float64(profile.backgroundMax)+2*(32<<20))
			require.InDelta(t, (burst.end-burst.start).Seconds()*float64(profile.peakRate), peak, 2*(32<<20))
		}
		lastEnd = burst.end
	}
	invalid := *profile
	invalid.cycleMin = invalid.burstMax
	require.Error(t, invalid.validate())
}

func TestBenchmarkReadQuotas(t *testing.T) {
	var counts [benchmarkMaxReads]int
	for id := uint64(1); id <= 10000; id++ {
		ages := benchmarkReadAges(id, benchmarkCase{})
		require.Equal(t, ages, benchmarkReadAges(id, benchmarkCase{}), "the quota and read ages belong to the key")
		require.GreaterOrEqual(t, len(ages), 1)
		require.LessOrEqual(t, len(ages), benchmarkMaxReads)
		counts[len(ages)-1]++
		var previous time.Duration
		for _, age := range ages {
			require.GreaterOrEqual(t, age, 100*time.Millisecond)
			require.LessOrEqual(t, age, 2*time.Minute)
			require.GreaterOrEqual(t, age, previous)
			previous = age
		}
	}
	require.InDelta(t, 5000, counts[0], 250)
	require.InDelta(t, 2000, counts[1], 200)
	for _, count := range counts[2:] {
		require.Positive(t, count, "every repeat count through forty-four must occur")
	}
	var reads int
	for i, count := range counts {
		reads += (i + 1) * count
	}
	require.InDelta(t, 3.7, float64(reads)/10000, 0.2, "production's hits per entry")
	t.Logf("read quotas 1..10: %v", counts)
}

func TestBenchmarkRejectedWriteAccounting(t *testing.T) {
	cache := newPutCache(t, testDevice(t, 64<<20), WithMemory(16<<20))
	w := benchmarkWorkload{cache: cache, keys: make([]atomic.Uint32, 2)}
	stats := newBenchmarkStats()
	for _, job := range []benchmarkJob{{id: 1, size: 16384}, {id: 2, size: 40000}} {
		request := benchmarkRequest{workload: &w}
		request.writeFailure(fmt.Errorf("write: %w", errors.Join(ErrRejected, ErrRejected)), job, time.Now())
		stats.record(&request.stats)
	}
	request := benchmarkRequest{workload: &w}
	request.read(benchmarkJob{id: 1}, time.Now())
	stats.record(&request.stats)
	require.NoError(t, stats.err)
	require.Zero(t, stats.failures)
	require.Zero(t, stats.writes.count())
	require.EqualValues(t, 2, stats.rejectedWrites)
	require.EqualValues(t, 16384+40000, stats.rejectedBytes)
	require.EqualValues(t, 2, stats.writeRejected.count())
	require.EqualValues(t, 1, stats.rejectedFillMisses)
	require.Zero(t, stats.busyWrites)
	require.EqualValues(t, 2, w.progress.rejectedWrites.Load())
	require.EqualValues(t, 16384+40000, w.progress.rejectedBytes.Load())
}

func TestBenchmarkBusyFinishesAtFirstAttempt(t *testing.T) {
	cache := newPutCache(t, testDevice(t, 64<<20))
	// Hold all allocator memory so this scheduled write encounters ErrBusy.
	held := alloc.NewAllocatingBuffer(cache.memory)
	require.NoError(t, held.Grow(int(cache.memory.Capacity())))
	defer held.Release()
	w := benchmarkWorkload{cache: cache, data: make([]byte, 512)}
	stats := w.run(benchmarkPlan{duration: time.Millisecond, bytes: 512,
		writes: []benchmarkJob{{id: 1, size: 512}}}, nil)
	require.NoError(t, stats.err)
	require.EqualValues(t, 1, stats.busyWrites)
	require.EqualValues(t, 512, stats.busyBytes)
	require.Zero(t, stats.rejectedWrites)
	require.Zero(t, stats.failures)
	require.Zero(t, stats.writes.count())
	require.Less(t, stats.elapsed, 500*time.Millisecond, "no one-second busy retry loop")
}

// Deliberately implements only Next: benchmark consumption must not use Read.
type benchmarkChunkReader struct {
	chunks [][]byte
	err    error
	calls  int
}

func (r *benchmarkChunkReader) Next() ([]byte, error) {
	r.calls++
	if len(r.chunks) > 0 {
		p := r.chunks[0]
		r.chunks = r.chunks[1:]
		return p, nil
	}
	if r.err != nil {
		return nil, r.err
	}
	return nil, io.EOF
}

func TestBenchmarkConsumeNext(t *testing.T) {
	for _, tc := range []struct {
		name     string
		limit, n int64
		calls    int
		chunks   [][]byte
		end, err error
	}{
		{name: "empty", limit: 0, n: 0, calls: 0},
		{name: "prefix", limit: 2, n: 2, calls: 1, chunks: [][]byte{[]byte("abcd"), []byte("ef")}},
		{name: "fragmented", limit: 5, n: 5, calls: 2, chunks: [][]byte{[]byte("abc"), []byte("defg")}},
		{name: "eof", limit: 10, n: 3, calls: 2, chunks: [][]byte{[]byte("abc")}},
		{name: "error", limit: 10, n: 3, calls: 2, chunks: [][]byte{[]byte("abc")}, end: ErrCorrupt, err: ErrCorrupt},
		{name: "no-progress", limit: 1, n: 0, calls: 1, chunks: [][]byte{nil}, err: io.ErrNoProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &benchmarkChunkReader{chunks: tc.chunks, err: tc.end}
			n, err := benchmarkConsume(r, tc.limit)
			require.Equal(t, tc.n, n)
			require.Equal(t, tc.calls, r.calls)
			if tc.err == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.err)
			}
		})
	}
}

func TestBenchmarkPreflightRejectsOnlyExternalValues(t *testing.T) {
	policy := iosched.NewDiskPolicy(iosched.WithWriteBandwidth(64<<10), iosched.WithoutReadIOBudget(),
		iosched.WithWriteBacklogLimit(time.Millisecond), iosched.WithWriteBacklogRecovery(0))
	c := newPutCache(t, testDevice(t, 64<<20), WithMemory(16<<20), WithDiskPolicy(policy),
		WithInlineThreshold(benchmarkInlineSize),
		WithSmallWriteAdmission(64<<10))
	putValue(t, c, "close-admission", make([]byte, benchmarkInlineSize))
	require.ErrorIs(t, policy.CanAdmit(iosched.OpWrite), ErrRejected)
	c.values.reclaimCacheMemory(c.memory.Capacity())
	held := alloc.NewAllocatingBuffer(c.memory)
	require.NoError(t, held.Grow(int(c.memory.Capacity())))
	defer held.Release()
	w := benchmarkWorkload{cache: c, disk: policy, data: make([]byte, benchmarkInlineSize+1),
		config: benchmarkConfig{smallWriteAdmission: 64 << 10}, keys: make([]atomic.Uint32, 3)}
	large := benchmarkRequest{workload: &w}
	large.write(benchmarkJob{id: 1, size: benchmarkInlineSize + 1}, time.Now())
	require.EqualValues(t, 1, large.stats.preflightRejected)
	require.EqualValues(t, 1, large.stats.rejectedWrites)
	require.Zero(t, large.stats.busyWrites, "admission is checked before the exhausted allocator")
	require.Zero(t, large.stats.copiedBytes)
	held.Release()
	inline := benchmarkRequest{workload: &w}
	inline.write(benchmarkJob{id: 2, size: benchmarkInlineSize}, time.Now())
	require.Zero(t, inline.stats.preflightRejected)
	require.EqualValues(t, benchmarkInlineSize, inline.stats.copiedBytes)
	require.EqualValues(t, 1, inline.stats.rejectedWrites, "the disk write still observes admission")
	checkRead(t, c, []byte("key-2"), w.data[:benchmarkInlineSize])

	small := benchmarkRequest{workload: &w}
	small.write(benchmarkJob{id: 3, size: 8192}, time.Now())
	require.True(t, small.stats.writes.recorded)
	require.EqualValues(t, 8192, small.stats.copiedBytes)
	require.Zero(t, small.stats.preflightRejected)
	require.ErrorIs(t, policy.CanAdmit(iosched.OpWrite), ErrRejected)
}
