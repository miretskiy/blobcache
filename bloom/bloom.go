package bloom

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
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
func (f *Filter) Add(key []byte) {
	h := xxhash.Sum64(key)
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
		index := bit / 32
		mask := uint32(1 << (bit % 32))

		// CAS loop to set bit atomically
		for {
			orig := atomic.LoadUint32(&f.data[index])
			if orig&mask != 0 {
				break // Bit already set
			}
			if atomic.CompareAndSwapUint32(&f.data[index], orig, orig|mask) {
				break
			}
			// CAS failed (another writer won), retry
		}

		// RocksDB golden ratio multiply for next probe
		h = h * 0x9e3779b9
	}
}

// Test checks if a key might be in the set (lock-free)
func (f *Filter) Test(key []byte) bool {
	h := xxhash.Sum64(key)

	for i := uint(0); i < f.k; i++ {
		bit := uint(h>>23) % f.m
		index := bit / 32
		mask := uint32(1 << (bit % 32))

		// Atomic load (lock-free read!)
		word := atomic.LoadUint32(&f.data[index])
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

// Serialize returns the bloom filter as bytes with checksum footer
// Format: [m:8][k:4][data...][checksum(4)][magic(8)]
func (f *Filter) Serialize() ([]byte, error) {
	// Snapshot data atomically
	snapshot := make([]uint32, len(f.data))
	for i := range f.data {
		snapshot[i] = atomic.LoadUint32(&f.data[i])
	}

	// Encode bloom data: [m:8][k:8][data...]
	// Both m and k are uint64 for consistency (k could be uint8 since it's <20, but alignment)
	bloomData := make([]byte, 16+len(snapshot)*4)
	binary.LittleEndian.PutUint64(bloomData[0:8], uint64(f.m))
	binary.LittleEndian.PutUint64(bloomData[8:16], uint64(f.k))

	for i, word := range snapshot {
		binary.LittleEndian.PutUint32(bloomData[16+i*4:], word)
	}

	// Append checksum footer
	checksum := crc32.ChecksumIEEE(bloomData)
	footer := make([]byte, 12)
	binary.LittleEndian.PutUint32(footer[0:4], checksum)
	binary.LittleEndian.PutUint64(footer[4:12], bloomMagic)

	return append(bloomData, footer...), nil
}

// Deserialize creates a bloom filter from bytes with validation
// Format: [m:8][k:4][data...][checksum(4)][magic(8)]
// Validates magic and checksum, returns error if corrupted
func Deserialize(data []byte) (*Filter, error) {
	if len(data) < 24 { // Minimum: m(8) + k(4) + checksum(4) + magic(8)
		return nil, ErrInvalidFormat
	}

	// Extract and validate footer
	footerStart := len(data) - 12
	bloomData := data[:footerStart]
	checksum := binary.LittleEndian.Uint32(data[footerStart : footerStart+4])
	magic := binary.LittleEndian.Uint64(data[footerStart+4 : footerStart+12])

	// Validate magic
	if magic != bloomMagic {
		return nil, fmt.Errorf("invalid bloom magic: %x (expected %x)", magic, bloomMagic)
	}

	// Validate checksum
	computed := crc32.ChecksumIEEE(bloomData)
	if computed != checksum {
		return nil, fmt.Errorf("bloom checksum mismatch: computed %x != stored %x", computed, checksum)
	}

	// Decode bloom data: [m:8][k:8][data...]
	m := uint(binary.LittleEndian.Uint64(bloomData[0:8]))
	k := uint(binary.LittleEndian.Uint64(bloomData[8:16]))

	words := make([]uint32, (len(bloomData)-16)/4)
	for i := range words {
		words[i] = binary.LittleEndian.Uint32(bloomData[16+i*4:])
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
