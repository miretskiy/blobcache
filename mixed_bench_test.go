package blobcache

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"
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
		WithMaxSize(256*1024*1024*1024),     // 256GB for Mac (production ~1TB)
		WithWriteBufferSize(1024*1024*1024)) // 1GB like production
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		cache.Drain()
		cache.Close()
	}()

	ctx := context.Background()

	var numWrites, numHits, numMisses, numBloomFP atomic.Int64
	var writeCounter atomic.Int64    // Pre-allocates key IDs
	var completedWrites atomic.Int64 // Tracks completed Put() calls
	var workerID atomic.Int64
	start := time.Now()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42 + workerID.Add(1)))
		for pb.Next() {
			op := rng.Intn(100)
			if op < 10 {
				// 10% writes
				value := make([]byte, 1024*1024) // 1MB per worker
				for i := 0; i < len(value); i += 128 * 1024 {
					value[i] = byte(rng.Intn(255))
				}

				keyID := writeCounter.Add(1) - 1
				key := []byte(fmt.Sprintf("key-%d", keyID))
				err := cache.Put(ctx, key, value)
				if err == nil {
					numWrites.Add(1)
					completedWrites.Add(1)
				}
			} else if op < 55 {
				// 45% reads from completed writes only
				maxKey := completedWrites.Load()
				if maxKey > 0 {
					keyID := rng.Int63n(maxKey)
					key := []byte(fmt.Sprintf("key-%d", keyID))
					_, err := cache.Get(ctx, key)
					if err == nil {
						numHits.Add(1)
					} else if err == ErrNotFound {
						numMisses.Add(1)
					}
				}
			} else {
				// 45% reads with miss prefix (tests bloom filter)
				keyID := rng.Int63n(int64(b.N))
				key := []byte(fmt.Sprintf("miss-%d", keyID))
				_, err := cache.Get(ctx, key)
				if err == nil {
					numBloomFP.Add(1)
				} else if err == ErrNotFound {
					numMisses.Add(1)
				}
			}
		}
	})

	duration := time.Since(start)
	b.StopTimer()
	cache.Drain()

	// Calculate metrics
	writes := numWrites.Load()
	hits := numHits.Load()
	misses := numMisses.Load()
	bloomFP := numBloomFP.Load()
	totalReads := hits + misses

	writeThroughput := float64(writes) / duration.Seconds() / 1024 // GB/s
	bloomFPRate := float64(bloomFP) / float64(misses) * 100        // %
	hitRate := float64(hits) / float64(totalReads) * 100           // %

	// Report metrics
	b.ReportMetric(float64(writes), "writes")
	b.ReportMetric(float64(hits), "hits")
	b.ReportMetric(float64(misses), "misses")
	b.ReportMetric(writeThroughput, "write-GB/s")
	b.ReportMetric(hitRate, "hit-%")
	b.ReportMetric(bloomFPRate, "bloom-FP-%")
	b.ReportMetric(float64(metricsAlignedWrites.Load()), "aligned-writes")
	b.ReportMetric(float64(metricsUnalignedWrites.Load()), "unaligned-writes")

	b.Logf("Operations: %d writes, %d reads (%d hits, %d misses, %d bloom-FP) in %v",
		writes, totalReads, hits, misses, bloomFP, duration)
	b.Logf("Write: %.2f GB/s, Hit rate: %.1f%%, Bloom FP: %.4f%%",
		writeThroughput, hitRate, bloomFPRate)
	// TODO: Re-add histogram tracking for latency measurements
	// var buf bytes.Buffer
	// hist.PercentilesPrint(&buf, 1, 1)
	// b.Log(buf.String())
	// b.Logf("hist: min=%d mean=%f 95=%d 99=%d max=%d",
	// 	hist.Min(), hist.Mean(), hist.ValueAtPercentile(95), hist.ValueAtPercentile(99), hist.Max())
}
