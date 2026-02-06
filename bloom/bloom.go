package bloom

import (
	"math"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/miretskiy/blobcache/internal/xmap"
)

// Key is a 128-bit XXH3 hash.
// Alias to xmap.Key for consistency across the codebase.
type Key = xmap.Key

// Filter is a lock-free bloom filter using atomic operations.
// Optimized for AWS Nitro instances by pinning all probes for a key to a single
// 64-byte cache line, reducing memory latency by ~6x.
//
// Key design: Uses full 128-bit XXH3 hashes to avoid the "32-bit funnel" bug:
//   - k.Hi selects the 64-byte block (via bits.Mul64 for uniform distribution)
//   - k.Lo generates the probe pattern (independent entropy source)
//
// This prevents correlated failures where both block selection AND probe pattern
// collide for different keys, which happens at scale with truncated hashes.
//
// Lifecycle: Filter starts unfrozen (concurrent Add/Test safe via atomics).
// Call Freeze() when no more Add() calls will occur to enable faster Test() path.
type Filter struct {
	data      []uint32 // Bit vector (accessed atomically when unfrozen)
	m         uint     // Filter size in bits (aligned to 512)
	k         uint     // Number of hash functions (probes)
	numBlocks uint32   // Number of 64-byte blocks

	frozen    atomic.Bool // When true, Test uses direct reads (no atomics)
	recording atomic.Pointer[recording]
}

// recording is a fast structure to record bloom filter
// additions while the bloom filter is being rebuilt.
type recording struct {
	primary  []Key
	cursor   atomic.Uint64
	mu       sync.Mutex
	overflow []Key
}

func (r *recording) Add(k Key) {
	// 1. GUARANTEED RECORDING: Using atomic reservation
	idx := r.cursor.Add(1) - 1
	if idx < uint64(len(r.primary)) {
		r.primary[idx] = k
	} else {
		// Emergency overflow to guarantee NO FALSE NEGATIVES
		r.mu.Lock()
		r.overflow = append(r.overflow, k)
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
func (f *Filter) Add(k Key) {
	if rec := f.recording.Load(); rec != nil {
		rec.Add(k)
	}
	f.AddHash(k)
}

// AddHash inserts specified hash into this filter using RocksDB-style local probing.
// Uses full 128-bit entropy: Hi for block selection, Lo for probe pattern.
func (f *Filter) AddHash(k Key) {
	// Level 1: Pick the 64-byte block using Hi bits.
	// bits.Mul64(x, y) returns (hi, lo) where hi*2^64 + lo = x*y.
	// We want floor(k.Hi * numBlocks / 2^64), which is the hi result.
	// This gives uniform distribution across [0, numBlocks) range.
	blockIdx, _ := bits.Mul64(k.Hi, uint64(f.numBlocks))
	baseIdx := uint32(blockIdx) << 4

	// Level 2: Local Probes using Lo bits.
	// We use the RocksDB technique: Lo provides the seed, delta provides stepping.
	// 'delta' is a bit-rotation to ensure independent stepping per probe.
	h32 := uint32(k.Lo)
	delta := uint32(k.Lo>>17) | uint32(k.Lo<<15)

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
// Uses atomic loads when unfrozen (safe for concurrent Add).
// Uses direct reads when frozen (faster, no atomic overhead).
func (f *Filter) Test(k Key) bool {
	// Block selection using Hi (same as AddHash)
	blockIdx, _ := bits.Mul64(k.Hi, uint64(f.numBlocks))
	baseIdx := uint32(blockIdx) << 4

	// Probe pattern using Lo (same as AddHash)
	h32 := uint32(k.Lo)
	delta := uint32(k.Lo>>17) | uint32(k.Lo<<15)

	// Bounds check hint (helps compiler remove checks inside loop)
	// f.data is numBlocks*16. idx max is (numBlocks-1)*16 + 15.
	// If we prove len(data) is large enough, we save checks.
	if len(f.data) < int(f.numBlocks)<<4 {
		return false // Should never happen
	}

	if f.frozen.Load() {
		// Fast path: direct reads (no atomic overhead)
		for i := uint(0); i < f.k; i++ {
			bitInBlock := h32 & 511
			idx := baseIdx + (bitInBlock >> 5)
			mask := uint32(1 << (bitInBlock & 31))
			if (f.data[idx] & mask) == 0 {
				return false
			}
			h32 += delta
		}
		return true
	}

	// Slow path: atomic loads (safe for concurrent Add)
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

type KeyConsumer func(k Key)

// RecordAdditions arranges for this filter to record all added keys until
// stopRecording or consumeRecording function is invoked.
func (f *Filter) RecordAdditions() (
	stopRecording func(),
	consumeRecording func(consumer KeyConsumer),
) {
	// Pre-allocate 256k slots (4MB for 128-bit keys). This is "large" for 8KB blobs.
	r := &recording{
		primary: make([]Key, 256*1024),
	}
	f.recording.Store(r)

	stopRecording = func() {
		f.recording.Store(nil)
	}
	consumeRecording = func(fn KeyConsumer) {
		// 1. Drain Primary
		count := min(len(r.primary), int(r.cursor.Load()))
		for i := range count {
			fn(r.primary[i])
		}

		// 2. Drain Overflow
		r.mu.Lock()
		for _, k := range r.overflow {
			fn(k)
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

// Freeze transitions the filter to read-only mode.
// After Freeze(), Test() uses direct reads without atomic overhead.
// Add() should not be called after Freeze() - behavior is undefined.
// This is a one-way transition; there is no Unfreeze().
func (f *Filter) Freeze() {
	f.frozen.Store(true)
}
