package index

import (
	"os"
	"testing"
	"time"
)

// Benchmark_IndexLookup compares index Get() performance with 1M keys
// Run with: go test -bench=Benchmark_IndexLookup -benchtime=1000000x -count=10
func Benchmark_IndexLookup(b *testing.B) {
	const numKeys = 1 << 16

	// Pre-create records
	records := make([]KeyValue, numKeys)
	for i := 0; i < numKeys; i++ {
		records[i] = KeyValue{
			Key: Key(i),
			Val: &Value{
				Pos:  int64((i % 1000) * 1024),
				Size: 1024,
			},
		}
	}

	b.StopTimer()
	tmpDir, _ := os.MkdirTemp("", "bench-segments-skipmap-*")

	idx, _ := NewIndex(tmpDir)

	// Populate
	populateStart := time.Now()
	if err := idx.PutBatch(records); err != nil {
		b.Fatal(err)
	}
	populateTime := time.Since(populateStart)
	b.ReportMetric(populateTime.Seconds(), "populate-sec")
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		entry, err := idx.Get(Key(i % numKeys))
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
}
