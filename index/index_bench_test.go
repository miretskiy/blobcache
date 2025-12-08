package index

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/base"
)

// Benchmark_IndexLookup compares index Get() performance with 1M keys
// Run with: go test -bench=Benchmark_IndexLookup -benchtime=1000000x -count=10
func Benchmark_IndexLookup(b *testing.B) {
	const numKeys = 1_000_000
	const numShards = 256

	// Generate keys upfront
	keys := make([]base.Key, numKeys)
	for i := 0; i < numKeys; i++ {
		keyBytes := []byte(fmt.Sprintf("key-%d", i))
		keys[i] = base.NewKey(keyBytes, numShards)
	}

	ctx := context.Background()
	now := int64(1700000000000000000)

	// Pre-create records
	records := make([]Record, numKeys)
	for i := 0; i < numKeys; i++ {
		records[i] = Record{
			Key:       keys[i].Raw(),
			SegmentID: int64(i / 1000),
			Pos:       int64((i % 1000) * 1024),
			Size:      1024,
			CTime:     now + int64(i),
		}
	}

	b.Run("Bitcask", func(b *testing.B) {
		b.StopTimer()
		tmpDir, _ := os.MkdirTemp("", "bench-bitcask-*")

		idx, _ := NewBitcaskIndex(tmpDir)

		// Populate
		populateStart := time.Now()
		if err := idx.PutBatch(ctx, records); err != nil {
			b.Fatal(err)
		}
		populateTime := time.Since(populateStart)
		b.ReportMetric(populateTime.Seconds(), "populate-sec")
		b.StartTimer()

		var entry Record
		for i := 0; i < b.N; i++ {
			err := idx.Get(ctx, keys[i%numKeys], &entry)
			if err != nil {
				b.Fatalf("Get failed: %v", err)
			}
			if entry.Size != 1024 {
				b.Fatalf("Size mismatch")
			}
			i++
		}
		b.StopTimer()
		idx.Close()
		os.RemoveAll(tmpDir)
	})

	b.Run("Skipmap", func(b *testing.B) {
		b.StopTimer()
		idx := NewSkipmapIndex()
		// Populate
		populateStart := time.Now()
		if err := idx.PutBatch(ctx, records); err != nil {
			b.Fatal(err)
		}
		populateTime := time.Since(populateStart)
		b.ReportMetric(populateTime.Seconds(), "populate-sec")

		b.StartTimer()
		var entry Record
		for i := 0; i < b.N; i++ {
			err := idx.Get(ctx, keys[i%numKeys], &entry)
			if err != nil {
				b.Fatalf("Get failed: %v", err)
			}
			if entry.Size != 1024 {
				b.Fatalf("Size mismatch")
			}
			i++
		}
		b.StopTimer()
		idx.Close()
	})

	b.Run("DurableSkipmap", func(b *testing.B) {
		b.StopTimer()
		tmpDir, _ := os.MkdirTemp("", "bench-durable-skipmap-*")

		idx, _ := NewDurableSkipmapIndex(tmpDir)

		// Populate
		populateStart := time.Now()
		if err := idx.PutBatch(ctx, records); err != nil {
			b.Fatal(err)
		}
		populateTime := time.Since(populateStart)
		b.ReportMetric(populateTime.Seconds(), "populate-sec")
		b.StartTimer()

		var entry Record
		for i := 0; i < b.N; i++ {
			err := idx.Get(ctx, keys[i%numKeys], &entry)
			if err != nil {
				b.Fatalf("Get failed: %v", err)
			}
			if entry.Size != 1024 {
				b.Fatalf("Size mismatch")
			}
			i++
		}
		b.StopTimer()
		idx.Close()
		os.RemoveAll(tmpDir)
	})
}
