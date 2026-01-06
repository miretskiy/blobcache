package index

import (
	"fmt"
	"os"
	"testing"
	"time"
	
	"github.com/miretskiy/blobcache/metadata"
)

// Benchmark_IndexLookup compares index Get() performance
// Run with: go test -bench=Benchmark_IndexLookup -benchtime=5s
func Benchmark_IndexLookup(b *testing.B) {
	// Testing various scales: 64K, 1M, 4M keys
	sizes := []int{1 << 16, 1 << 20, 4 << 20}
	
	for _, numKeys := range sizes {
		b.Run(fmt.Sprintf("Keys-%dK", numKeys>>10), func(b *testing.B) {
			b.StopTimer()
			tmpDir, _ := os.MkdirTemp("", "bench-index-lookup-*")
			defer os.RemoveAll(tmpDir)
			
			idx, _ := NewIndex(tmpDir)
			defer idx.Close()
			
			// Prepare Batch
			// Note: We use metadata.BlobRecord directly to feed IngestBatch
			records := make([]metadata.BlobRecord, numKeys)
			for i := 0; i < numKeys; i++ {
				// Mix the SegmentID to simulate xxHash entropy
				h := uint64(i) * 0x9e3779b97f4a7c15
				records[i] = metadata.BlobRecord{
					Hash:        h,
					Pos:         int64(i * 1024),
					LogicalSize: 1024,
				}
			}
			
			// Measure Population
			start := time.Now()
			// Ingest in 10k chunks to simulate real MemTable flushes
			const batchSize = 10000
			for i := 0; i < numKeys; i += batchSize {
				end := i + batchSize
				if end > numKeys {
					end = numKeys
				}
				if err := idx.IngestBatch(int64(i/batchSize), records[i:end]); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(numKeys)/time.Since(start).Seconds(), "ingest-keys/sec")
			
			b.StartTimer()
			// Benchmark Lookups
			for i := 0; i < b.N; i++ {
				// Re-mix to find the actual key
				h := uint64(i%numKeys) * 0x9e3779b97f4a7c15
				val, ok := idx.Get(h)
				if !ok {
					b.Fatalf("Lookup failed for hash %d", h)
				}
				// Tiny check to ensure the compiler doesn't elide the loop
				if val.LogicalSize != 1024 {
					b.Fatal("Data corruption detected")
				}
			}
			b.StopTimer()
		})
	}
}
