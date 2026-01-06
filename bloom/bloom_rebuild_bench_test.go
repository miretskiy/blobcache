package bloom

import (
	"fmt"
	"os"
	"testing"
	
	"github.com/miretskiy/blobcache/index"
	"github.com/miretskiy/blobcache/metadata"
)

// Benchmark_BloomRebuild measures time to rebuild bloom filter from index
func Benchmark_BloomRebuild(b *testing.B) {
	// 128k, 1M, 4M keys
	sizes := []int{128 << 10, 1 << 20, 4 << 20}
	
	for _, numKeys := range sizes {
		b.Run(fmt.Sprintf("%dK-Keys", numKeys>>10), func(b *testing.B) {
			// Setup: Create index
			tmpDir, _ := os.MkdirTemp("", "bloom-rebuild-*")
			defer os.RemoveAll(tmpDir)
			
			idx, _ := index.NewIndex(tmpDir)
			defer idx.Close()
			
			// Populate with "Mixed" hashes to simulate real entropy
			const batchSize = 1024
			for i := 0; i < numKeys; i += batchSize {
				entries := make([]metadata.BlobRecord, batchSize)
				for k := 0; k < batchSize; k++ {
					id := uint64(i + k)
					// Use a simple Knuth mixer to prevent "Silly Hash" linearity
					mixedHash := id * 0x9e3779b97f4a7c15
					
					entries[k] = metadata.BlobRecord{
						Hash:        mixedHash,
						Pos:         int64(id % 1000),
						LogicalSize: 1024,
					}
				}
				_ = idx.IngestBatch(int64(i/batchSize), entries)
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
				idx.ForEachBlob(func(v index.Entry) bool {
					filter.AddHash(v.Hash) // Use AddHash to skip internal mixer if already mixed
					return true
				})
				
				// Tiny sanity check (not enough to skew bench)
				if !filter.Test(uint64(0) * 0x9e3779b97f4a7c15) {
					b.Fatal("Bloom lookup failed")
				}
			}
		})
	}
}
