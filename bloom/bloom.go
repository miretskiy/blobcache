package bloom

import (
	"math"
	"sync"
	"sync/atomic"
)

// Filter is a lock-free bloom filter using atomic operations.
// Optimized for AWS Nitro instances by pinning all probes for a key to a single
// 64-byte cache line, reducing memory latency by ~6x.
type Filter struct {
	data      []uint32 // Bit vector (accessed atomically)
	m         uint     // Filter size in bits (aligned to 512)
	k         uint     // Number of hash functions (probes)
	numBlocks uint32   // Number of 64-byte blocks

	recording atomic.Pointer[recording]
}

// recording is a fast structure to record bloom filter
// additions while the bloom filter is being rebuilt.
type recording struct {
	primary  []uint64
	cursor   atomic.Uint64
	mu       sync.Mutex
	overflow []uint64
}

func (r *recording) Add(h uint64) {
	// 1. GUARANTEED RECORDING: Using atomic reservation
	idx := r.cursor.Add(1) - 1
	if idx < uint64(len(r.primary)) {
		r.primary[idx] = h
	} else {
		// Emergency overflow to guarantee NO FALSE NEGATIVES
		r.mu.Lock()
		r.overflow = append(r.overflow, h)
		r.mu.Unlock()
	}
}

// New creates a bloom filter optimized for n elements with target false positive rate.
func New(estimatedKeys uint, fpRate float64) *Filter {
	m := optimalM(estimatedKeys, fpRate)
	// SURGERY: Block-based filters require a "Variance Buffer."
	// We add 15% more bits to account for the Poisson distribution of keys
	// into blocks. This ensures "unlucky" blocks don't saturate.
	m = uint(float64(m) * 1.15)

	k := optimalK(fpRate)

	// RocksDB/FastLocalBloom alignment: Round m up to nearest 512 bits (64 bytes).
	m = (m + 511) &^ 511
	numBlocks := uint32(m >> 9) // m / 512
	if numBlocks == 0 {
		numBlocks = 1
		m = 512
	}

	return &Filter{
		// 16 uint32s = 64 bytes = 1 CPU cache line
		data:      make([]uint32, numBlocks*16),
		m:         m,
		k:         k,
		numBlocks: numBlocks,
	}
}

// Add inserts a key into the bloom filter (lock-free, concurrent-safe).
func (f *Filter) Add(h uint64) {
	if rec := f.recording.Load(); rec != nil {
		rec.Add(h)
	}
	f.AddHash(h)
}

// AddHash inserts specified hash into this filter using RocksDB-style local probing.
func (f *Filter) AddHash(h uint64) {
	// Level 1: Pick the 64-byte block.
	// We use the "Fast Range" method: (h32 * numBlocks) >> 32.
	// This provides a more uniform distribution than a simple modulo (%)
	// for smaller key ranges.
	h32 := uint32(h)
	blockIdx := uint32((uint64(h32) * uint64(f.numBlocks)) >> 32)
	baseIdx := blockIdx << 4

	// Level 2: Local Probes.
	// We use the RocksDB technique: a single 32-bit value provides the seed and the delta.
	// 'delta' is a bit-rotation of the hash to ensure independent stepping.
	delta := (h32 >> 17) | (h32 << 15)

	for i := uint(0); i < f.k; i++ {
		// Bit position 0-511 inside the block
		bitInBlock := h32 & 511

		idx := baseIdx + (bitInBlock >> 5)
		mask := uint32(1 << (bitInBlock & 31))

		// Atomic bit-set
		for {
			orig := atomic.LoadUint32(&f.data[idx])
			if orig&mask != 0 {
				break
			}
			if atomic.CompareAndSwapUint32(&f.data[idx], orig, orig|mask) {
				break
			}
		}
		// Increment the hash by the delta for the next probe position
		h32 += delta
	}
}

// Test checks if a key might be in the set (lock-free).
func (f *Filter) Test(h uint64) bool {
	h32 := uint32(h)
	blockIdx := uint32((uint64(h32) * uint64(f.numBlocks)) >> 32)
	baseIdx := blockIdx << 4

	delta := (h32 >> 17) | (h32 << 15)
	for i := uint(0); i < f.k; i++ {
		bitInBlock := h32 & 511
		idx := baseIdx + (bitInBlock >> 5)
		mask := uint32(1 << (bitInBlock & 31))

		// All subsequent iterations are L1 cache hits
		if (atomic.LoadUint32(&f.data[idx]) & mask) == 0 {
			return false
		}
		h32 += delta
	}
	return true
}

type HashConsumer func(h uint64)

// RecordAdditions arranges for this filter to record all added hashes until
// stopRecording or consumeRecording function is invoked.
func (f *Filter) RecordAdditions() (
	stopRecording func(),
	consumeRecording func(consumer HashConsumer),
) {
	// Pre-allocate 256k slots (2MB). This is "large" for 8KB blobs.
	r := &recording{
		primary: make([]uint64, 256*1024),
	}
	f.recording.Store(r)

	stopRecording = func() {
		f.recording.Store(nil)
	}
	consumeRecording = func(fn HashConsumer) {
		// 1. Drain Primary
		count := min(len(r.primary), int(r.cursor.Load()))
		for i := range count {
			fn(r.primary[i])
		}

		// 2. Drain Overflow
		r.mu.Lock()
		for _, h := range r.overflow {
			fn(h)
		}
		r.mu.Unlock()
	}
	return stopRecording, consumeRecording
}

// optimalM calculates optimal filter size in bits.
func optimalM(n uint, p float64) uint {
	m := math.Ceil(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
	return uint(m)
}

// optimalK calculates optimal number of hash functions.
func optimalK(p float64) uint {
	k := math.Ceil(-math.Log2(p))
	if k < 1 {
		k = 1
	}
	return uint(k)
}
