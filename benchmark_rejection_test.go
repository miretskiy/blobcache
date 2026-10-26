package blobcache

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBenchmarkRejectionClassification(t *testing.T) {
	require.True(t, benchmarkOnlyRejected(fmt.Errorf("write: %w", errors.Join(ErrRejected, ErrRejected))))
	require.False(t, benchmarkOnlyRejected(errors.Join(ErrRejected, io.ErrUnexpectedEOF)))
	require.False(t, benchmarkOnlyRejected(ErrBusy))
	require.False(t, benchmarkOnlyRejected(nil))
}

func TestBenchmarkRejectedWriteAccounting(t *testing.T) {
	cache := newPutCache(t, t.TempDir(), WithMemory(16<<20))
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

func TestBenchmarkDispatchDoesNotWaitForRequests(t *testing.T) {
	const count = 256
	jobs := make([]benchmarkJob, count)
	started := make(chan int, count)
	release := make(chan struct{})
	var requests sync.WaitGroup
	dispatched := make(chan struct{})
	go func() {
		benchmarkDispatch(jobs, time.Now(), &requests, func(index int, _ benchmarkJob) {
			started <- index
			<-release
		})
		close(dispatched)
	}()
	t.Cleanup(func() { close(release); <-dispatched; requests.Wait() })
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	seen := make(map[int]bool)
	for range count {
		select {
		case index := <-started:
			require.False(t, seen[index])
			seen[index] = true
		case <-timer.C:
			t.Fatal("later arrivals waited for blocked requests")
		}
	}
}

func TestBenchmarkBusyFinishesAtFirstAttempt(t *testing.T) {
	cache := newPutCache(t, t.TempDir(), WithMemory(128<<10))
	// Hold all allocator memory so this scheduled write encounters ErrBusy.
	held, err := cache.memory.TryAcquire(cache.memory.Capacity())
	require.NoError(t, err)
	defer held.Unpin()
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

func TestBenchmarkVariableBurstPlan(t *testing.T) {
	profile := &benchmarkBurstProfile{750_000_000, 1_225_000_000, 2_000_000_000,
		2 * time.Second, 4 * time.Second, 12 * time.Second, 18 * time.Second}
	require.NoError(t, profile.validate())
	c := benchmarkCase{profile: profile}
	plan := makeBenchmarkWritePlan(200000, 0, 32<<20, c)
	require.Equal(t, plan, makeBenchmarkWritePlan(200000, 0, 32<<20, c), "same seeded trace")
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
			require.GreaterOrEqual(t, float64(background), quiet*float64(profile.backgroundMin)-2*(8<<20))
			require.LessOrEqual(t, float64(background), quiet*float64(profile.backgroundMax)+2*(8<<20))
			require.InDelta(t, (burst.end-burst.start).Seconds()*float64(profile.peakRate), peak, 2*(8<<20))
		}
		lastEnd = burst.end
	}
	invalid := *profile
	invalid.cycleMin = invalid.burstMax
	require.Error(t, invalid.validate())
}
