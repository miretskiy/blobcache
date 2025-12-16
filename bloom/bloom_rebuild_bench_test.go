package bloom

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/index"
	"github.com/miretskiy/blobcache/metadata"
)

// Benchmark_BloomRebuild measures time to rebuild bloom filter from index
// Run with: go test -bench=Benchmark_BloomRebuild -benchtime=10x
func Benchmark_BloomRebuild(b *testing.B) {
	sizes := []int{128 << 10, 1 << 20, 1 << 22}
	const keysPerSegment = 1024

	for _, numKeys := range sizes {
		b.Run(fmt.Sprintf("%dK", numKeys>>10), func(b *testing.B) {
			b.StopTimer()

			// Setup: Create index with N keys (not timed)
			tmpDir, _ := os.MkdirTemp("", "bloom-rebuild-bench-*")
			defer os.RemoveAll(tmpDir)

			idx, _ := index.NewIndex(tmpDir)
			defer idx.Close()

			// Populate index (not timed)
			for i := 0; i < numKeys; i += keysPerSegment {
				entries := make([]index.KeyValue, keysPerSegment)
				now := time.Now()
				for k := 0; k < keysPerSegment; k++ {
					val := index.Value{
						SegmentID: int64(i >> 10),
						Pos:       int64(i % 1000),
						Size:      1024,
					}
					val.TestingSetCTime(now)
					entries[k] = index.KeyValue{
						Key: index.Key(i),
						Val: val,
					}
				}
				if err := idx.PutBatch(entries); err != nil {
					b.Fatal(err)
				}
				// fmt.Fprintf(os.Stderr, "Populated %d\n", i+keysPerSegment)
			}
			b.StartTimer()

			// Benchmark: Rebuild bloom filter
			for i := 0; i < b.N; i++ {
				filter := New(uint(4<<20), 0.01)

				if err := idx.ForEachSegment(func(segment metadata.SegmentRecord) bool {
					for _, rec := range segment.Records {
						filter.Add(rec.Hash)
					}
					return true
				}); err != nil {
					b.Fatal(err)
				}

				// Verify it works
				if !filter.Test(0) {
					b.Fatal("bloom filter broken")
				}
			}
		})
	}
}
