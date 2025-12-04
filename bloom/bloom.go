package bloom

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// Filter is a bloom filter with mutex protection for correctness
type Filter struct {
	mu   sync.RWMutex
	data []uint32 // Bit vector
	m    uint     // Filter size in bits
	k    uint     // Number of hash functions (probes)
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

// Add inserts a key into the bloom filter
func (f *Filter) Add(key []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	h := xxhash.Sum64(key)

	for i := uint(0); i < f.k; i++ {
		bit := uint(h>>23) % f.m
		index := bit / 32
		mask := uint32(1 << (bit % 32))
		f.data[index] |= mask
		h = h * 0x9e3779b9
	}
}

// Test checks if a key might be in the set
func (f *Filter) Test(key []byte) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	h := xxhash.Sum64(key)

	for i := uint(0); i < f.k; i++ {
		bit := uint(h>>23) % f.m
		index := bit / 32
		mask := uint32(1 << (bit % 32))

		if f.data[index]&mask == 0 {
			return false
		}

		h = h * 0x9e3779b9
	}

	return true
}

// Serialize returns the bloom filter as bytes
func (f *Filter) Serialize() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	buf := make([]byte, 12+len(f.data)*4)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(f.m))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(f.k))

	for i, word := range f.data {
		binary.LittleEndian.PutUint32(buf[12+i*4:], word)
	}

	return buf, nil
}

// Deserialize creates a bloom filter from bytes
func Deserialize(data []byte) (*Filter, error) {
	if len(data) < 12 {
		return nil, ErrInvalidFormat
	}

	m := uint(binary.LittleEndian.Uint64(data[0:8]))
	k := uint(binary.LittleEndian.Uint32(data[8:12]))

	words := make([]uint32, (len(data)-12)/4)
	for i := range words {
		words[i] = binary.LittleEndian.Uint32(data[12+i*4:])
	}

	return &Filter{
		data: words,
		m:    m,
		k:    k,
	}, nil
}

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
