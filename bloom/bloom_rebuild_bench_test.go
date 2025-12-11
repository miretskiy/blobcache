package bloom

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/miretskiy/blobcache/index"
)

// Benchmark_BloomRebuild measures time to rebuild bloom filter from index
// Run with: go test -bench=Benchmark_BloomRebuild -benchtime=10x
func Benchmark_BloomRebuild(b *testing.B) {
	sizes := []int{100_000, 1_000_000, 4_000_000}

	for _, numKeys := range sizes {
		b.Run(fmt.Sprintf("%dkeys", numKeys), func(b *testing.B) {
			b.StopTimer()

			// Setup: Create index with N keys (not timed)
			tmpDir, _ := os.MkdirTemp("", "bloom-rebuild-bench-*")
			defer os.RemoveAll(tmpDir)

			idx, _ := index.NewDurableSkipmapIndex(tmpDir)
			defer idx.Close()

			// Populate index (not timed)
			entries := make([]index.KeyValue, numKeys)
			for i := 0; i < numKeys; i++ {
				key := []byte(fmt.Sprintf("key-%d", i))
				entries[i] = index.KeyValue{
					Key: key,
					Val: index.Value{
						SegmentID: int64(i / 1000),
						Pos:       int64(i % 1000),
						Size:      1024,
						CTime:     int64(i),
					},
				}
			}
			idx.PutBatch(context.Background(), entries)

			b.StartTimer()

			// Benchmark: Rebuild bloom filter
			for i := 0; i < b.N; i++ {
				filter := New(uint(numKeys), 0.01)

				// Rebuild from index
				idx.Range(context.Background(), func(kv index.KeyValue) bool {
					filter.Add(kv.Key)
					return true
				})

				// Verify it works
				if !filter.Test([]byte("key-0")) {
					b.Fatal("bloom filter broken")
				}
			}
		})
	}
}
