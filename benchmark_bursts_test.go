package blobcache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBenchmarkBurstSummary(t *testing.T) {
	for _, tc := range []struct {
		name            string
		duration        time.Duration
		bursts          []benchmarkBurst
		burstTime       time.Duration
		count, complete int
	}{
		{"steady", 100, nil, 0, 0, 0},
		{"complete", 100, []benchmarkBurst{{10, 20}}, 10, 1, 1},
		{"partial-last-burst", 100, []benchmarkBurst{{10, 20}, {90, 110}}, 20, 2, 1},
		{"burst-not-reached", 10, []benchmarkBurst{{10, 20}}, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := benchmarkPlan{duration: tc.duration, bursts: tc.bursts}
			count, complete, duration := plan.burstSummary()
			require.Equal(t, tc.count, count)
			require.Equal(t, tc.complete, complete)
			require.Equal(t, tc.burstTime, duration)
			events := plan.burstBoundaries()
			require.Equal(t, tc.duration, events[len(events)-1].at)
			var last time.Duration
			for _, event := range events {
				require.GreaterOrEqual(t, event.at, last)
				require.LessOrEqual(t, event.at, tc.duration)
				last = event.at
			}
		})
	}
}

func TestBenchmarkBurstPlan(t *testing.T) {
	for _, fills := range []int{238483, 700000} {
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
			// Whole objects straddling either boundary can differ by at most two
			// maximum-sized writes from the byte-paced interval's exact target.
			require.InDelta(t, (burst.end-burst.start).Seconds()*benchmarkPeakRate, offered, 2*(8<<20))
		}
		if fills == 700000 {
			require.GreaterOrEqual(t, complete, 6)
		}
		t.Logf("fills=%d bytes=%d scheduled=%s burst-count=%d complete=%d total-burst-duration=%s", fills, plan.bytes, plan.duration, count, complete, total)
		for i, burst := range plan.bursts {
			t.Logf("burst=%d start=%s end=%s", i, burst.start, burst.end)
		}
	}
}

func TestBenchmarkBurstDrains(t *testing.T) {
	plan := benchmarkPlan{duration: 40, bursts: []benchmarkBurst{{10, 20}, {30, 50}, {60, 70}},
		writes: []benchmarkJob{{at: 5}, {at: 15}, {at: 25}, {at: 35}}}
	// An earlier quiet write completes after the first burst's own write.
	// Work scheduled after that burst must not extend its drain measurement.
	require.Equal(t, []benchmarkBurstDrain{{20, 28, false}, {40, 60, true}},
		plan.burstDrains([]time.Duration{28, 22, 60, 45}))
}

func TestBenchmarkRunBurstAccounting(t *testing.T) {
	cache, err := New(t.TempDir(), WithMemory(16<<20), WithTargetSize(1<<20), WithDurableIndex(false))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	plan := benchmarkPlan{
		duration: 12 * time.Millisecond,
		bursts:   []benchmarkBurst{{5 * time.Millisecond, 10 * time.Millisecond}},
		bytes:    1536,
		writes: []benchmarkJob{
			{at: 0, id: 1, size: 512},
			{at: 6 * time.Millisecond, id: 2, size: 512},
			{at: 12 * time.Millisecond, id: 3, size: 512},
		},
	}
	workload := benchmarkWorkload{cache: cache, data: make([]byte, 512)}
	stats := workload.run(plan, nil)
	require.NoError(t, stats.err)
	require.EqualValues(t, 3, stats.writes.count())
	require.Equal(t, 1, stats.bursts)
	require.Equal(t, 1, stats.completeBursts)
	require.Equal(t, 5*time.Millisecond, stats.burstDuration)
	require.EqualValues(t, 1536, stats.writes.bytes)
}

func TestBenchmarkReadQuotas(t *testing.T) {
	var counts [benchmarkMaxReads]int
	for id := uint64(1); id <= 10000; id++ {
		ages := benchmarkReadAges(id)
		require.Equal(t, ages, benchmarkReadAges(id), "the quota and read ages belong to the key")
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
	require.InDelta(t, 8500, counts[0], 200)
	require.InDelta(t, 1000, counts[1], 150)
	for _, count := range counts[2:] {
		require.Positive(t, count, "every repeat count through ten must occur")
	}
	t.Logf("read quotas 1..10: %v", counts)
}
