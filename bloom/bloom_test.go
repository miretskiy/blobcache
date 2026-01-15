package bloom

import (
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// testKey creates a simple deterministic Key from an integer.
func testKey(n int) Key {
	return Key{Lo: uint64(n), Hi: 0}
}

// testKeyU64 creates a Key from a uint64 (used for random/mixed hashes).
func testKeyU64(h uint64) Key {
	// Use the same mixing pattern: Lo gets the hash, Hi gets a rotated/mixed version
	return Key{Lo: h, Hi: h ^ (h >> 33)}
}

func TestBloom_AddTest(t *testing.T) {
	filter := New(1000, 0.01)

	filter.Add(testKey(123))

	// Test should return true
	if !filter.Test(testKey(123)) {
		t.Error("Test returned false for added key")
	}

	// Non-existent key should mostly return false
	if filter.Test(testKey(321)) {
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

	addedKeys := make([]Key, n)
	exists := make(map[Key]struct{}, n)

	for i := 0; i < n; i++ {
		k := testKeyU64(rng.Uint64())
		addedKeys[i] = k
		exists[k] = struct{}{}
		filter.Add(k)
	}

	// 1. Verify No False Negatives
	for _, k := range addedKeys {
		if !filter.Test(k) {
			t.Fatalf("CRITICAL: False Negative detected at key %v", k)
		}
	}

	// 2. Measure False Positive Rate
	falsePositives := 0
	checkSize := 100000

	for i := 0; i < checkSize; i++ {
		k := testKeyU64(rng.Uint64())

		// Ensure this random key wasn't actually in our 'added' set
		if _, ok := exists[k]; ok {
			continue
		}

		if filter.Test(k) {
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
	filter.Add(testKey(123))
	filter.Add(testKey(123))
	filter.Add(testKey(123))

	// Should still work
	if !filter.Test(testKey(123)) {
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
				filter.Add(testKey(1000*id + i))
			}
		}(g)
	}

	wg.Wait()

	// Verify all keys present (10,000 total)
	missing := 0
	for g := 0; g < 10; g++ {
		for i := 0; i < 1000; i++ {
			if !filter.Test(testKey(1000*g + i)) {
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
		filter.Add(testKey(i))
	}

	// Concurrent readers and writers
	var wg sync.WaitGroup

	// 10 readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				filter.Test(testKey(j))
			}
		}()
	}

	// 10 writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				filter.Add(testKey(j))
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
		filter.Add(testKey(i))
	}
}

func BenchmarkTestParallel(b *testing.B) {
	// Create filter sized for 1M keys, populate with 10K (1% full - realistic)
	filter := New(1000000, 0.01)
	for i := 0; i < 10000; i++ {
		filter.Add(testKey(i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// Test keys not in filter (should be rejected by bloom)
			filter.Test(testKey(i + 2000000))
			i++
		}
	})
}

func TestRecordAdditions_StopPreventsRecording(t *testing.T) {
	filter := New(1000, 0.01)

	stop, consume := filter.RecordAdditions()

	// Add while recording
	filter.Add(testKey(123))

	// Stop
	stop()

	filter.Add(testKey(321))

	// Replay
	newFilter := New(1000, 0.01)
	consume(newFilter.AddHash)

	// Only first key should be in new filter
	require.True(t, newFilter.Test(testKey(123)))
	require.False(t, newFilter.Test(testKey(321)))
}
