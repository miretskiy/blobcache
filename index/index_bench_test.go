package index

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// Benchmark_IndexLookup compares index Get() performance with 1M keys
// Run with: go test -bench=Benchmark_IndexLookup -benchtime=1000000x -count=10
func Benchmark_IndexLookup(b *testing.B) {
	const numKeys = 1 << 16

	// Generate keys upfront
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}

	ctx := context.Background()
	now := int64(1700000000000000000)

	// Pre-create records
	records := make([]KeyValue, numKeys)
	for i := 0; i < numKeys; i++ {
		records[i] = KeyValue{
			Key: keys[i],
			Val: Value{
				SegmentID: int64(i / 1000),
				Pos:       int64((i % 1000) * 1024),
				Size:      1024,
				CTime:     now + int64(i),
			},
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

		var entry Value
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
		var entry Value
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

		var entry Value
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
