package bloom

import (
	"fmt"
	"os"
	"testing"

	"github.com/miretskiy/blobcache/internal/index"
)

// Benchmark_BloomRebuild measures time to rebuild bloom filter from index
func Benchmark_BloomRebuild(b *testing.B) {
	// 128k, 1M, 4M keys
	sizes := []int{128 << 10, 1 << 20, 4 << 20}

	for _, numKeys := range sizes {
		b.Run(fmt.Sprintf("%dK-Keys", numKeys>>10), func(b *testing.B) {
			// Setup: Create index
			tmpDir, err := os.MkdirTemp("", "bloom-rebuild-*")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tmpDir)

			idx, err := index.OpenIndex(tmpDir, numKeys)
			if err != nil {
				b.Fatal(err)
			}
			defer idx.Close()

			// Populate with "Mixed" hashes to simulate real entropy
			const batchSize = 1024
			for i := 0; i < numKeys; i += batchSize {
				items := make([]index.Item, batchSize)
				for k := 0; k < batchSize; k++ {
					id := uint64(i + k)
					// Use a simple Knuth mixer to prevent "Silly Hash" linearity
					mixedHash := id * 0x9e3779b97f4a7c15

					items[k] = index.Item{
						Key:         index.Key{Lo: mixedHash, Hi: 0},
						Offset:      uint32(id % 1000),
						PhysicalLen: 1024,
					}
				}
				if err := idx.IngestBatch(uint32(i/batchSize), items, 0); err != nil {
					b.Fatal(err)
				}
			}

			// Pre-calculate filter specs
			estimatedKeys := uint(numKeys)
			fpRate := 0.01

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// We allocate inside the loop to simulate a real "rebuild from scratch"
				filter := New(estimatedKeys, fpRate)

				// REBUILD PATH: Use the Skipmap Range.
				// It's all in RAM and pointer-stable, so it's the fastest way
				// to populate the filter.
				idx.ForEachBlob(func(v index.Item) bool {
					filter.AddHash(v.Key) // Full 128-bit key
					return true
				})

				// Tiny sanity check (not enough to skew bench)
				if !filter.Test(Key{Lo: 0, Hi: 0}) {
					b.Fatal("Bloom lookup failed")
				}
			}
		})
	}
}
