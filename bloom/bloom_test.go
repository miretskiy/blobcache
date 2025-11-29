package bloom

import (
	"fmt"
	"sync"
	"testing"
)

func TestBloom_AddTest(t *testing.T) {
	filter := New(1000, 0.01)

	// Add a key
	key := []byte("test-key")
	filter.Add(key)

	// Test should return true
	if !filter.Test(key) {
		t.Error("Test returned false for added key")
	}

	// Non-existent key should mostly return false
	if filter.Test([]byte("nonexistent")) {
		// Could be false positive (acceptable)
		t.Log("False positive (expected occasionally)")
	}
}

func TestBloom_FalsePositiveRate(t *testing.T) {
	n := uint(10000)
	fpRate := 0.01

	filter := New(n, fpRate)

	// Add n keys
	for i := uint(0); i < n; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		filter.Add(key)
	}

	// Test n non-existent keys
	falsePositives := 0
	for i := uint(0); i < n; i++ {
		key := []byte(fmt.Sprintf("nonexistent-%d", i))
		if filter.Test(key) {
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

	key := []byte("deterministic-key")

	// Add multiple times
	filter.Add(key)
	filter.Add(key)
	filter.Add(key)

	// Should still work
	if !filter.Test(key) {
		t.Error("Test failed after multiple Add calls")
	}
}

func TestBloom_Serialize(t *testing.T) {
	filter1 := New(1000, 0.01)

	// Add keys
	keys := []string{"key1", "key2", "key3"}
	for _, k := range keys {
		filter1.Add([]byte(k))
	}

	// Serialize
	data, err := filter1.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize
	filter2, err := Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Verify all keys present
	for _, k := range keys {
		if !filter2.Test([]byte(k)) {
			t.Errorf("After deserialize, key %s not found", k)
		}
	}

	// Verify parameters match
	if filter2.m != filter1.m {
		t.Errorf("m = %d, want %d", filter2.m, filter1.m)
	}

	if filter2.k != filter1.k {
		t.Errorf("k = %d, want %d", filter2.k, filter1.k)
	}
}

func TestBloom_ConcurrentAdd(t *testing.T) {
	filter := New(10000, 0.01)

	// 100 goroutines add different keys
	var wg sync.WaitGroup
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("g%d-key%d", id, i))
				filter.Add(key)
			}
		}(g)
	}

	wg.Wait()

	// Verify all keys present (10,000 total)
	missing := 0
	for g := 0; g < 100; g++ {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("g%d-key%d", g, i))
			if !filter.Test(key) {
				missing++
			}
		}
	}

	if missing > 0 {
		t.Errorf("%d keys missing after concurrent Add", missing)
	}
}

func TestBloom_ConcurrentMixed(t *testing.T) {
	filter := New(10000, 0.01)

	// Pre-populate
	for i := 0; i < 5000; i++ {
		filter.Add([]byte(fmt.Sprintf("key-%d", i)))
	}

	// Concurrent readers and writers
	var wg sync.WaitGroup

	// 1000 readers
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				filter.Test([]byte(fmt.Sprintf("key-%d", j%10000)))
			}
		}()
	}

	// 100 writers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				filter.Add([]byte(fmt.Sprintf("new-key-%d-%d", id, j)))
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
	key := []byte("benchmark-key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Add(key)
	}
}

func BenchmarkTest(b *testing.B) {
	filter := New(1000000, 0.01)
	key := []byte("benchmark-key")
	filter.Add(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Test(key)
	}
}

func BenchmarkConcurrentTest(b *testing.B) {
	filter := New(1000000, 0.01)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		filter.Add([]byte(fmt.Sprintf("key-%d", i)))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := []byte("key-5000")
		for pb.Next() {
			filter.Test(key)
		}
	})
}

func BenchmarkConcurrentAdd(b *testing.B) {
	filter := New(1000000, 0.01)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("key-%d", i))
			filter.Add(key)
			i++
		}
	})
}
