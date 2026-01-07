package bloom

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/stretchr/testify/require"
)

func TestBloom_AddTest(t *testing.T) {
	filter := New(1000, 0.01)

	filter.Add(123)

	// Test should return true
	if !filter.Test(123) {
		t.Error("Test returned false for added key")
	}

	// Non-existent key should mostly return false
	if filter.Test(321) {
		// Could be false positive (acceptable)
		t.Log("False positive (expected occasionally)")
	}
}

func TestBloom_FalsePositiveRate(t *testing.T) {
	const (
		n      = 20000 // Sample size
		fpRate = 0.01
	)

	filter := New(uint(n), fpRate)

	// In rand/v2, if you want a local, seeded generator for reproducibility:
	// PCG is the new high-performance, statistically robust generator.
	// We'll use two fixed seeds to ensure the test is deterministic.
	pcg := rand.NewPCG(42, 100)
	rng := rand.New(pcg)

	addedKeys := make([]uint64, n)
	exists := make(map[uint64]struct{}, n)

	for i := 0; i < n; i++ {
		h := rng.Uint64()
		addedKeys[i] = h
		exists[h] = struct{}{}
		filter.Add(h)
	}

	// 1. Verify No False Negatives
	for _, h := range addedKeys {
		if !filter.Test(h) {
			t.Fatalf("CRITICAL: False Negative detected at key %d", h)
		}
	}

	// 2. Measure False Positive Rate
	falsePositives := 0
	checkSize := 100000

	for i := 0; i < checkSize; i++ {
		h := rng.Uint64()

		// Ensure this random key wasn't actually in our 'added' set
		if _, ok := exists[h]; ok {
			continue
		}

		if filter.Test(h) {
			falsePositives++
		}
	}

	actualFPRate := float64(falsePositives) / float64(checkSize)

	// Tolerance: 20% margin is standard for Bloom statistical tests
	upperBound := fpRate * 1.2
	if actualFPRate > upperBound {
		t.Errorf("FP rate too high: Got %.4f, want <= %.4f", actualFPRate, upperBound)
	}

	t.Logf("Stats: n=%d, samples=%d, FPs=%d, Actual Rate=%.4f (Target=%.4f)",
		n, checkSize, falsePositives, actualFPRate, fpRate)
}

func TestBloom_Deterministic(t *testing.T) {
	filter := New(1000, 0.01)

	// Put multiple times
	filter.Add(123)
	filter.Add(123)
	filter.Add(123)

	// Should still work
	if !filter.Test(123) {
		t.Error("Test failed after multiple Put calls")
	}
}

func TestBloom_ConcurrentAdd(t *testing.T) {
	filter := New(10000, 0.01)

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				filter.Add(uint64(1000*id + i))
			}
		}(g)
	}

	wg.Wait()

	// Verify all keys present (10,000 total)
	missing := 0
	for g := 0; g < 10; g++ {
		for i := 0; i < 1000; i++ {
			if !filter.Test(uint64(1000*g + i)) {
				missing++
			}
		}
	}

	if missing > 0 {
		t.Errorf("%d keys missing after concurrent Put", missing)
	}
}

func TestBloom_ConcurrentMixed(t *testing.T) {
	filter := New(10000, 0.01)

	// Pre-populate
	for i := 0; i < 5000; i++ {
		filter.Add(uint64(i))
	}

	// Concurrent readers and writers
	var wg sync.WaitGroup

	// 10 readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				filter.Test(uint64(j))
			}
		}()
	}

	// 10 writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				filter.Add(uint64(j))
			}
		}(i)
	}

	wg.Wait()

	// No crashes = success
	t.Log("Concurrent mixed operations completed successfully")
}

// Benchmarks

func BenchmarkAdd(b *testing.B) {
	filter := New(1000000, 0.01)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Add(uint64(i))
	}
}

func BenchmarkTestParallel(b *testing.B) {
	// Create filter sized for 1M keys, populate with 10K (1% full - realistic)
	filter := New(1000000, 0.01)
	for i := 0; i < 10000; i++ {
		filter.Add(uint64(i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Test keys not in filter (should be rejected by bloom)
			filter.Test(uint64(i + 2000000))
			i++
		}
	})
}

// BenchmarkConcurrentRealistic measures bloom filter under realistic concurrent load
// Uses b.RunParallel with batched timing to reduce measurement overhead
func BenchmarkConcurrentRealistic(b *testing.B) {
	// Production-sized filter: 10M keys, 1% FP rate
	filter := New(10000000, 0.01)

	// Pre-populate (simulates warm cache)
	for i := 0; i < 100000; i++ {
		filter.Add(uint64(i))
	}

	const batchSize = 10000 // Measure time for 10K ops to reduce overhead

	type workerHist struct {
		testHist *hdrhistogram.Histogram
		addHist  *hdrhistogram.Histogram
		mu       sync.Mutex
	}

	var histograms sync.Map // map[int]*workerHist (workerID -> hist)
	var totalTests, totalAdds atomic.Int64
	var workerID atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		myID := int(workerID.Add(1))

		hist := &workerHist{
			testHist: hdrhistogram.New(1, 60*1000*1000*1000, 3),
			addHist:  hdrhistogram.New(1, 60*1000*1000*1000, 3),
		}
		histograms.Store(myID, hist)

		keyBase := uint64(myID * 1000000)
		opsInBatch := 0
		var batchStart time.Time
		testInBatch, addInBatch := 0, 0

		for pb.Next() {
			if opsInBatch == 0 {
				batchStart = time.Now()
			}

			// 90% Test, 10% Add
			if opsInBatch%10 == 0 {
				filter.Add(keyBase + uint64(opsInBatch))
				addInBatch++
			} else {
				filter.Test(keyBase + uint64(opsInBatch))
				testInBatch++
			}

			opsInBatch++

			// Record batch when full
			if opsInBatch >= batchSize {
				elapsed := time.Since(batchStart).Nanoseconds()

				if testInBatch > 0 {
					avgNs := elapsed * int64(testInBatch) / int64(opsInBatch) / int64(testInBatch)
					hist.mu.Lock()
					hist.testHist.RecordValue(avgNs)
					hist.mu.Unlock()
					totalTests.Add(int64(testInBatch))
				}

				if addInBatch > 0 {
					avgNs := elapsed * int64(addInBatch) / int64(opsInBatch) / int64(addInBatch)
					hist.mu.Lock()
					hist.addHist.RecordValue(avgNs)
					hist.mu.Unlock()
					totalAdds.Add(int64(addInBatch))
				}

				opsInBatch, testInBatch, addInBatch = 0, 0, 0
			}
		}

		// Record final partial batch
		if opsInBatch > 0 {
			elapsed := time.Since(batchStart).Nanoseconds()
			if testInBatch > 0 {
				avgNs := elapsed * int64(testInBatch) / int64(opsInBatch) / int64(testInBatch)
				hist.mu.Lock()
				hist.testHist.RecordValue(avgNs)
				hist.mu.Unlock()
				totalTests.Add(int64(testInBatch))
			}
			if addInBatch > 0 {
				avgNs := elapsed * int64(addInBatch) / int64(opsInBatch) / int64(addInBatch)
				hist.mu.Lock()
				hist.addHist.RecordValue(avgNs)
				hist.mu.Unlock()
				totalAdds.Add(int64(addInBatch))
			}
		}
	})

	// Merge all worker histograms
	mergedTest := hdrhistogram.New(1, 60*1000*1000*1000, 3)
	mergedAdd := hdrhistogram.New(1, 60*1000*1000*1000, 3)

	histograms.Range(func(key, value interface{}) bool {
		h := value.(*workerHist)
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.testHist.TotalCount() > 0 {
			mergedTest.Merge(h.testHist)
		}
		if h.addHist.TotalCount() > 0 {
			mergedAdd.Merge(h.addHist)
		}
		return true
	})

	// Report results via ReportMetric for easy reading
	if mergedTest.TotalCount() > 0 {
		b.ReportMetric(mergedTest.Mean(), "test-avg-ns")
		b.ReportMetric(float64(mergedTest.ValueAtQuantile(0.50)), "test-p50-ns")
		b.ReportMetric(float64(mergedTest.ValueAtQuantile(0.90)), "test-p90-ns")
		b.ReportMetric(float64(mergedTest.ValueAtQuantile(0.95)), "test-p95-ns")
		b.ReportMetric(float64(mergedTest.ValueAtQuantile(0.99)), "test-p99-ns")
		b.ReportMetric(float64(mergedTest.Max()), "test-max-ns")
	}

	if mergedAdd.TotalCount() > 0 {
		b.ReportMetric(mergedAdd.Mean(), "add-avg-ns")
		b.ReportMetric(float64(mergedAdd.ValueAtQuantile(0.50)), "add-p50-ns")
		b.ReportMetric(float64(mergedAdd.ValueAtQuantile(0.90)), "add-p90-ns")
		b.ReportMetric(float64(mergedAdd.ValueAtQuantile(0.95)), "add-p95-ns")
		b.ReportMetric(float64(mergedAdd.ValueAtQuantile(0.99)), "add-p99-ns")
		b.ReportMetric(float64(mergedAdd.Max()), "add-max-ns")
	}

	b.ReportMetric(float64(totalTests.Load()), "total-tests")
	b.ReportMetric(float64(totalAdds.Load()), "total-adds")
}

func TestRecordAdditions_StopPreventsRecording(t *testing.T) {
	filter := New(1000, 0.01)

	stop, consume := filter.RecordAdditions()

	// Add while recording
	filter.Add(123)

	// Stop
	stop()

	filter.Add(321)

	// Replay
	newFilter := New(1000, 0.01)
	consume(newFilter.AddHash)

	// Only first key should be in new filter
	require.True(t, newFilter.Test(123))
	require.False(t, newFilter.Test(321))
}

func TestFilterBasic(t *testing.T) {
	f := New(100, 0.01)

	// Add some hashes
	testHashes := []uint64{12345, 67890, 111222333}
	for _, h := range testHashes {
		f.AddHash(h)
	}

	// Test all should return true
	for _, h := range testHashes {
		if !f.Test(h) {
			t.Errorf("Test(%d) returned false after AddHash", h)
		}
	}

	// Test a hash we didn't add - might return true (false positive) or false
	notAdded := uint64(999999)
	f.Test(notAdded) // Result doesn't matter for this test
}
