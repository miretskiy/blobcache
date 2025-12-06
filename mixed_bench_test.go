package blobcache

import (
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/base"
)

// Benchmark_Mixed runs a mixed workload: 10% write, 45% hit-read, 45% miss-read
// All operations use random keys in range [0, b.N)
// No pre-population - cache starts empty, hit rate ramps up naturally
//
// Example: -benchtime=256000x
//   - 256K operations: ~25.6K writes, ~115K reads (hits+misses)
//   - Total data: ~25.6 GB written
//   - Cache limit: 256 GB (Mac) / production ~1TB
func Benchmark_Mixed(b *testing.B) {
	tmpDir := "/tmp/bench-blobcache-mixed"
	os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir,
		WithMaxSize(256<<30), // 256GB for Mac (production ~1TB)
		WithWriteBufferSize(1<<27),
		WithBitcaskIndex(),
		// WithDirectIOWrites(),
	)
	if err != nil {
		b.Fatal(err)
	}

	var numReads, numFound atomic.Int64
	var numWrites, completedWrites atomic.Int64 // Pre-allocates key IDs
	var workerID atomic.Int64
	start := time.Now()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerID := workerID.Add(1)
		rng := rand.New(rand.NewSource(42 + workerID))
		myKeys := make([]int64, 0, 1024) // Track this worker's completed writes

		for pb.Next() {
			op := rng.Intn(100)
			if op < 10 {
				// 10% writes
				value := make([]byte, 1024*1024) // 1MB per worker
				for i := 0; i < len(value); i += 128 * 1024 {
					value[i] = byte(rng.Intn(255))
				}

				keyID := numWrites.Add(1) - 1
				key := []byte(fmt.Sprintf("w-%d-key-%d", workerID, keyID))
				cache.UnsafePut(key, value)
				myKeys = append(myKeys, keyID)
				completedWrites.Add(1)
			} else if op < 55 {
				// 45% reads from this worker's completed writes
				if len(myKeys) > 0 {
					// Pick random key from myKeys
					idx := rng.Intn(len(myKeys))
					keyID := myKeys[idx]
					key := []byte(fmt.Sprintf("w-%d-key-%d", workerID, keyID))
					_, found := cache.Get(key)
					numReads.Add(1)
					if found {
						numFound.Add(1)
					} else {
						k := base.NewKey(key, cache.cfg.Shards)
						b.Logf("MUST_GET FAIL [worker=%d]: key=%s id=%d\n", workerID, string(key),
							k.FileID())
					}
				}
			} else {
				// 45% reads with miss prefix (tests bloom filter)
				keyID := rng.Int63n(int64(b.N))
				key := []byte(fmt.Sprintf("miss-%d", keyID))
				_, _ = cache.Get(key)
			}
		}
	})

	cache.Drain() // include drain time to flush the rest of the data.
	duration := time.Since(start)
	b.StopTimer()

	writeThroughput := float64(numWrites.Load()) / duration.Seconds() / 1024 // GB/s
	b.ReportMetric(writeThroughput, "write-GB/s")
}
