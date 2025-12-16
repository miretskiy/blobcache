package bloom

import (
	"sync"
	"testing"

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
	n := uint64(10000)
	fpRate := 0.01

	filter := New(uint(n), fpRate)

	// Put n keys
	for i := uint64(0); i < n; i++ {
		filter.Add(i)
	}

	// Test n non-existent keys
	falsePositives := 0
	for i := uint64(0); i < n; i++ {
		if filter.Test(i + n) {
			falsePositives++
		}
	}

	actualFPRate := float64(falsePositives) / float64(n)

	// Should be close to target (within 50% tolerance)
	if actualFPRate < fpRate*0.5 || actualFPRate > fpRate*1.5 {
		t.Errorf("FP rate = %.4f, want ~%.4f (±50%%)", actualFPRate, fpRate)
	}

	t.Logf("False positive rate: %.4f (target: %.4f)", actualFPRate, fpRate)
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
