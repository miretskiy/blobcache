package bloom

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBloom_AddTest(t *testing.T) {
	filter := New(1000, 0.01)

	// Put a key
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

	// Put n keys
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

	// Put multiple times
	filter.Add(key)
	filter.Add(key)
	filter.Add(key)

	// Should still work
	if !filter.Test(key) {
		t.Error("Test failed after multiple Put calls")
	}
}

func TestBloom_Serialize(t *testing.T) {
	filter1 := New(1000, 0.01)

	// Put keys
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
		t.Errorf("%d keys missing after concurrent Put", missing)
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

func TestRecordAdditions_Basic(t *testing.T) {
	filter := New(1000, 0.01)

	// Start recording
	stop, consume := filter.RecordAdditions()

	// Add some keys while recording
	filter.Add([]byte("key1"))
	filter.Add([]byte("key2"))
	filter.Add([]byte("key3"))

	// Stop recording
	stop()

	// Create new filter and replay
	newFilter := New(1000, 0.01)
	consume(newFilter.AddHash)

	// Verify all recorded keys are in new filter
	require.True(t, newFilter.Test([]byte("key1")))
	require.True(t, newFilter.Test([]byte("key2")))
	require.True(t, newFilter.Test([]byte("key3")))
}

func TestRecordAdditions_ConcurrentAdds(t *testing.T) {
	filter := New(10000, 0.01)

	// Start recording
	stop, consume := filter.RecordAdditions()

	// Add keys concurrently
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := []byte{byte(id), byte(j)}
				filter.Add(key)
			}
		}(i)
	}
	wg.Wait()

	// Stop and consume
	stop()

	newFilter := New(10000, 0.01)
	consume(newFilter.AddHash)

	// Verify all 1000 keys present
	for i := 0; i < 10; i++ {
		for j := 0; j < 100; j++ {
			key := []byte{byte(i), byte(j)}
			require.True(t, newFilter.Test(key), "key %v should be present", key)
		}
	}
}

func TestRebuildBloom_ConcurrentWrites(t *testing.T) {
	// Simulate rebuild scenario: existing keys + concurrent additions

	// Initial filter with some keys
	oldFilter := New(10000, 0.01)
	for i := 0; i < 1000; i++ {
		oldFilter.Add([]byte{byte(i >> 8), byte(i)})
	}

	// Start recording
	stop, consume := oldFilter.RecordAdditions()

	// Simulate concurrent writes during rebuild
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1000; i < 1100; i++ {
			oldFilter.Add([]byte{byte(i >> 8), byte(i)})
		}
	}()

	// Build new filter from "index" (simulate first 1000 keys)
	newFilter := New(10000, 0.01)
	for i := 0; i < 1000; i++ {
		newFilter.Add([]byte{byte(i >> 8), byte(i)})
	}

	// Wait for concurrent adds
	wg.Wait()
	stop()

	// Replay recorded additions
	consume(newFilter.AddHash)

	// Verify all 1100 keys present (1000 from index + 100 concurrent)
	for i := 0; i < 1100; i++ {
		key := []byte{byte(i >> 8), byte(i)}
		require.True(t, newFilter.Test(key), "key %d should be present", i)
	}
}

func TestRecordAdditions_StopPreventsRecording(t *testing.T) {
	filter := New(1000, 0.01)

	stop, consume := filter.RecordAdditions()

	// Add while recording
	filter.Add([]byte("recorded"))

	// Stop
	stop()

	// Add after stop (should NOT be recorded)
	filter.Add([]byte("not-recorded"))

	// Replay
	newFilter := New(1000, 0.01)
	consume(newFilter.AddHash)

	// Only first key should be in new filter
	require.True(t, newFilter.Test([]byte("recorded")))
	require.False(t, newFilter.Test([]byte("not-recorded")))
}

func TestRecordAdditions_MultipleConsumes(t *testing.T) {
	filter := New(1000, 0.01)

	stop, consume := filter.RecordAdditions()

	filter.Add([]byte("key1"))
	filter.Add([]byte("key2"))

	// First consume
	newFilter := New(1000, 0.01)
	consume(newFilter.AddHash)

	// Add more (still recording)
	filter.Add([]byte("key3"))

	stop()

	// Second consume (should get key3)
	consume(newFilter.AddHash)

	require.True(t, newFilter.Test([]byte("key1")))
	require.True(t, newFilter.Test([]byte("key2")))
	require.True(t, newFilter.Test([]byte("key3")))
}

func TestBloom_ChecksumValidation(t *testing.T) {
	filter := New(1000, 0.01)
	filter.Add([]byte("test"))

	data, err := filter.Serialize()
	require.NoError(t, err)

	// Corrupt checksum (last 12 bytes are footer)
	footerStart := len(data) - 12
	data[footerStart] = 0xFF
	data[footerStart+1] = 0xFF

	// Should fail to deserialize
	_, err = Deserialize(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

func TestBloom_MagicValidation(t *testing.T) {
	filter := New(1000, 0.01)
	filter.Add([]byte("test"))

	data, err := filter.Serialize()
	require.NoError(t, err)

	// Corrupt magic (last 8 bytes of footer)
	footerStart := len(data) - 12
	binary.LittleEndian.PutUint64(data[footerStart+4:], 0xDEADBEEF)

	// Should fail to deserialize
	_, err = Deserialize(data)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid bloom magic")
}
