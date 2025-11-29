package bloom

import (
	"testing"

	bitsbloom "github.com/bits-and-blooms/bloom/v3"
)

// Single-threaded
func BenchmarkOurs_Test_SingleThread(b *testing.B) {
	filter := New(1000000, 0.01)
	key := []byte("benchmark-key-1234567890")
	filter.Add(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Test(key)
	}
}

func BenchmarkBitsAndBlooms_Test_SingleThread(b *testing.B) {
	filter := bitsbloom.NewWithEstimates(1000000, 0.01)
	key := []byte("benchmark-key-1234567890")
	filter.Add(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filter.Test(key)
	}
}

// Concurrent
func BenchmarkOurs_Test_Concurrent(b *testing.B) {
	filter := New(1000000, 0.01)
	key := []byte("benchmark-key-1234567890")
	filter.Add(key)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			filter.Test(key)
		}
	})
}

func BenchmarkBitsAndBlooms_Test_Concurrent(b *testing.B) {
	filter := bitsbloom.NewWithEstimates(1000000, 0.01)
	key := []byte("benchmark-key-1234567890")
	filter.Add(key)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			filter.Test(key)
		}
	})
}

// Mixed workload (90% read, 10% write)
func BenchmarkOurs_Mixed_90Read(b *testing.B) {
	filter := New(1000000, 0.01)

	// Pre-generate keys to avoid sprintf in benchmark
	testKey := []byte("test-key-1234567890")
	writeKeys := make([][]byte, 1000)
	for i := range writeKeys {
		writeKeys[i] = []byte{byte(i), byte(i >> 8), byte(i >> 16)}
	}

	filter.Add(testKey)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				filter.Add(writeKeys[i%len(writeKeys)])
			} else {
				filter.Test(testKey)
			}
			i++
		}
	})
}

func BenchmarkBitsAndBlooms_Mixed_90Read(b *testing.B) {
	filter := bitsbloom.NewWithEstimates(1000000, 0.01)

	testKey := []byte("test-key-1234567890")
	writeKeys := make([][]byte, 1000)
	for i := range writeKeys {
		writeKeys[i] = []byte{byte(i), byte(i >> 8), byte(i >> 16)}
	}

	filter.Add(testKey)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				filter.Add(writeKeys[i%len(writeKeys)])
			} else {
				filter.Test(testKey)
			}
			i++
		}
	})
}
