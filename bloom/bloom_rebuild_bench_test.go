package bloom

import (
	"fmt"
	"testing"
)

// Benchmark_BloomRebuild measures time to rebuild bloom filter from a set of keys.
// Simulates rebuilding from an index scan by iterating over generated keys.
func Benchmark_BloomRebuild(b *testing.B) {
	// 128k, 1M, 4M keys
	sizes := []int{128 << 10, 1 << 20, 4 << 20}

	for _, numKeys := range sizes {
		b.Run(fmt.Sprintf("%dK-Keys", numKeys>>10), func(b *testing.B) {
			// Pre-generate keys with Knuth mixer to simulate real entropy
			keys := make([]Key, numKeys)
			for i := range keys {
				mixedHash := uint64(i) * 0x9e3779b97f4a7c15
				keys[i] = Key{Lo: mixedHash, Hi: 0}
			}

			// Pre-calculate filter specs
			estimatedKeys := uint(numKeys)
			fpRate := 0.01

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Allocate inside the loop to simulate a real "rebuild from scratch"
				filter := New(estimatedKeys, fpRate)

				// REBUILD PATH: Iterate over all keys
				for _, k := range keys {
					filter.AddHash(k)
				}

				// Tiny sanity check (not enough to skew bench)
				if !filter.Test(Key{Lo: 0, Hi: 0}) {
					b.Fatal("Bloom lookup failed")
				}
			}
		})
	}
}
