package bloom

import (
	"errors"
	"math"
	"sync/atomic"

	"github.com/zhangyunhao116/skipset"
)

// Filter is a lock-free bloom filter using atomic operations
// Combines fastbloom's atomic CAS approach with RocksDB's FastLocalBloom algorithm
type Filter struct {
	data      []uint32 // Bit vector (accessed atomically)
	m         uint     // Filter size in bits
	k         uint     // Number of hash functions (probes)
	recording atomic.Pointer[skipset.Int64Set]
}

// New creates a bloom filter optimized for n elements with target false positive rate
func New(estimatedKeys uint, fpRate float64) *Filter {
	m := optimalM(estimatedKeys, fpRate)
	k := optimalK(fpRate)

	return &Filter{
		data: make([]uint32, m/32+1),
		m:    m,
		k:    k,
	}
}

// Add inserts a key into the bloom filter (lock-free, concurrent-safe)
func (f *Filter) Add(h uint64) {
	if rec := f.recording.Load(); rec != nil {
		rec.Add(int64(h))
	}
	f.AddHash(h)
}

// AddHash inserts specified hash into this filter.
func (f *Filter) AddHash(h uint64) {
	for i := uint(0); i < f.k; i++ {
		// RocksDB FastLocalBloom: use high bits for bit position
		bit := uint(h>>23) % f.m

		// Atomic bit set (fastbloom approach)
		idx := bit / 32
		mask := uint32(1 << (bit % 32))

		// CAS loop to set bit atomically
		for {
			orig := atomic.LoadUint32(&f.data[idx])
			if orig&mask != 0 {
				break // Bit already set
			}
			if atomic.CompareAndSwapUint32(&f.data[idx], orig, orig|mask) {
				break
			}
			// CAS failed (another writer won), retry
		}

		// RocksDB golden ratio multiply for next probe
		h = h * 0x9e3779b9
	}
}

// Test checks if a key might be in the set (lock-free)
func (f *Filter) Test(h uint64) bool {
	for i := uint(0); i < f.k; i++ {
		bit := uint(h>>23) % f.m
		idx := bit / 32
		mask := uint32(1 << (bit % 32))

		// Atomic load (lock-free read!)
		word := atomic.LoadUint32(&f.data[idx])
		if word&mask == 0 {
			return false
		}

		h = h * 0x9e3779b9
	}

	return true
}

type HashConsumer func(h uint64)

// RecordAdditions arranges for this filter to record all added hashes either
// stopRecording or consumeRecording function is invoked.
func (f *Filter) RecordAdditions() (
	stopRecording func(),
	consumeRecording func(consumer HashConsumer),
) {
	set := skipset.NewInt64()
	f.recording.Store(set)
	stopRecording = func() {
		f.recording.Store(nil)
	}
	consumeRecording = func(consumer HashConsumer) {
		set.Range(func(k int64) bool {
			consumer(uint64(k))
			return true
		})
	}
	return stopRecording, consumeRecording
}

const (
	bloomMagic = 0xB100F1173DAA // Magic number for bloom file validation
)

// optimalM calculates optimal filter size in bits
// Formula: m = -n * ln(p) / (ln(2)^2)
func optimalM(n uint, p float64) uint {
	m := math.Ceil(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
	return uint(m)
}

// optimalK calculates optimal number of hash functions
// Formula: k = -log2(p)
func optimalK(p float64) uint {
	k := math.Ceil(-math.Log2(p))
	if k < 1 {
		k = 1
	}
	return uint(k)
}

var ErrInvalidFormat = errors.New("invalid bloom filter format")
