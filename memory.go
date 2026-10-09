package blobcache

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/mempool"
)

// memory owns lazily mapped chunks, divided into reusable blocks. Records are
// bumped into an active block and never moved. Full blocks stay cached until
// pressure reclaims them. The allocator lock protects reservations and lists;
// readers only pin their block. Eviction stops new pins immediately;
// existing borrowers delay storage reuse, not logical eviction. Index cleanup,
// mmap growth, and dedicated buffer unmapping run outside it.
type memory struct {
	mu                   sync.Mutex
	limit, mapped        int64
	blockSize, chunkSize int
	pool                 *mempool.MmapPool
	chunks               []*memoryChunk
	blocks               []*memoryBlock // allocation order; includes the active block
	retired              int            // evicted blocks still held by borrowers or I/O
	active               *memoryBlock
	borrowed             map[*byte]memoryValue // only allocations still owned by callers
	onEvict              func([]*memoryBlock)
}

type memoryChunk struct {
	buffer *mempool.MmapBuffer
	slab   *mempool.SlabPool
	used   int // blocks handed out, including victims awaiting index cleanup
}

// A fresh descriptor is created every time storage is reused. refs includes
// one cache reference. A negative count marks eviction and refuses new pins.
// The last existing pin releases storage, after index cleanup drops the cache
// reference. Old descriptors can never pin a replacement block.
const retiredRefs int64 = -1 << 63

type memoryBlock struct {
	owner  *memory
	refs   atomic.Int64
	data   []byte
	next   int
	keys   []Key
	chunk  *memoryChunk // nil for a dedicated oversized allocation
	slot   mempool.Slot
	buffer *mempool.MmapBuffer // dedicated allocation only
}

type memoryValue struct {
	block *memoryBlock
	data  []byte
}

func (v memoryValue) pin() bool {
	if v.block == nil {
		return false
	}
	for {
		n := v.block.refs.Load()
		if n <= 0 {
			return false
		}
		if v.block.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

func (v memoryValue) unpin() {
	b := v.block
	if b.refs.Add(-1) == retiredRefs {
		m := b.owner
		m.mu.Lock()
		m.release(b)
		m.retired--
		m.mu.Unlock()
	}
}

// ErrForeignMemory means a buffer is not currently held from Alloc.
var ErrForeignMemory = errors.New("blobcache: memory not held from Alloc")

func newMemory(size int64) (*memory, error) {
	if size < 128<<10 || size%align.BlockSize != 0 {
		return nil, fmt.Errorf("blobcache: memory %d must be a page multiple of at least 128 KiB", size)
	}
	block := min(4<<20, int(size/4)/align.BlockSize*align.BlockSize)
	chunk := min(16<<20, int(size/2)/block*block)
	return &memory{
		limit: size, blockSize: block, chunkSize: chunk,
		pool:     mempool.NewLazyMmapPool("blobcache", int64(chunk), int(size/int64(chunk))),
		borrowed: make(map[*byte]memoryValue),
	}, nil
}

func (m *memory) maxAlloc() int { return int(m.limit) }

// alloc reserves a contiguous, page-aligned record and one borrower pin.
// Oversized records get a dedicated mapping charged to the same byte budget.
func (m *memory) alloc(size int, owned bool, key *Key) (memoryValue, []byte, error) {
	if size <= 0 || size > m.maxAlloc() {
		return memoryValue{}, nil, fmt.Errorf("%w: %d bytes of memory (at most %d)", ErrValueTooLarge, size, m.maxAlloc())
	}
	size = int(align.PageAlign(int64(size)))
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		b := m.active
		if size > m.blockSize || b == nil || len(b.data)-b.next < size {
			// Closing a block to reservations does not release its borrowers.
			m.active = nil
			var err error
			b, err = m.newBlock(size)
			if err != nil {
				return memoryValue{}, nil, err
			}
			if b == nil {
				victims := m.retire(max(size, 2*m.blockSize))
				if len(victims) == 0 {
					return memoryValue{}, nil, ErrBusy
				}
				m.mu.Unlock()
				if m.onEvict != nil {
					m.onEvict(victims)
				}
				for _, victim := range victims {
					(memoryValue{block: victim}).unpin() // release the cache reference
				}
				m.mu.Lock()
				continue
			}
			if size <= m.blockSize {
				m.active = b
			}
		}
		end := b.next + size
		v := memoryValue{block: b, data: b.data[b.next:end:end]}
		b.next = end
		b.refs.Add(1)
		if key != nil {
			b.keys = append(b.keys, *key)
		}
		if owned {
			m.borrowed[unsafe.SliceData(v.data)] = v
		}
		return v, v.data, nil
	}
}

// newBlock is called with mu held. Growth reserves its budget before dropping
// the lock to map/prefault memory. Retiring blocks remain charged until release.
func (m *memory) newBlock(size int) (*memoryBlock, error) {
	if size <= m.blockSize {
		for _, ch := range m.chunks {
			if ch.used == ch.slab.NumSlots() {
				continue
			}
			slot, err := ch.slab.Acquire()
			if err != nil {
				return nil, err
			}
			ch.used++
			return m.addBlock(&memoryBlock{data: slot.Data, chunk: ch, slot: slot}), nil
		}
	}
	allocation := size
	pooled := size <= m.blockSize
	if pooled {
		allocation = m.chunkSize
	}
	if m.mapped+int64(allocation) > m.limit {
		// An empty chunk can make room for a differently sized allocation.
		for i := 0; i < len(m.chunks); {
			ch := m.chunks[i]
			if ch.used != 0 {
				i++
				continue
			}
			ch.slab.Close()
			ch.buffer.Unpin()
			m.chunks = slices.Delete(m.chunks, i, i+1)
			m.mapped -= int64(m.chunkSize)
		}
		m.pool.Trim()
	}
	// Use a single dedicated block if the remaining budget cannot fit an
	// entire backing chunk. This also uses non-chunk-sized budget tails.
	if pooled && m.mapped+int64(allocation) > m.limit && m.mapped+int64(m.blockSize) <= m.limit {
		allocation, pooled = m.blockSize, false
	}
	if m.mapped+int64(allocation) > m.limit {
		return nil, nil
	}
	m.mapped += int64(allocation)
	m.mu.Unlock()
	buffer, err := m.mapBuffer(allocation, pooled)
	m.mu.Lock()
	if err != nil {
		m.mapped -= int64(allocation)
		return nil, err
	}
	if !pooled {
		return m.addBlock(&memoryBlock{data: buffer.Bytes(), buffer: buffer}), nil
	}
	slab, err := mempool.NewSlabPoolFrom(buffer.Bytes(), m.blockSize)
	if err != nil {
		buffer.Unpin()
		m.pool.Trim()
		m.mapped -= int64(allocation)
		return nil, err
	}
	ch := &memoryChunk{buffer: buffer, slab: slab, used: 1}
	m.chunks = append(m.chunks, ch)
	slot, _ := slab.Acquire() // a new nonempty slab
	return m.addBlock(&memoryBlock{data: slot.Data, chunk: ch, slot: slot}), nil
}

func (m *memory) mapBuffer(size int, pooled bool) (buffer *mempool.MmapBuffer, err error) {
	// dio's aligned allocator reports mmap failures by panic. Cache allocation
	// reports them to its caller, after returning its budget reservation.
	defer func() {
		if failure := recover(); failure != nil {
			err = fmt.Errorf("blobcache: mmap: %v", failure)
		}
	}()
	if !pooled {
		return mempool.NewMmapBuffer(int64(size)), nil
	}
	buffer, ok := m.pool.TryAcquire()
	if !ok {
		return nil, ErrBusy
	}
	return buffer, nil
}

func (m *memory) addBlock(b *memoryBlock) *memoryBlock {
	b.owner = m
	b.refs.Store(1)
	m.blocks = append(m.blocks, b)
	return b
}

// retire evicts an oldest prefix, regardless of outstanding pins. Marking a
// block retired makes its key list immutable and prevents new memory readers.
// Borrowers keep storage alive until they finish; no scan for unpinned victims.
func (m *memory) retire(target int) []*memoryBlock {
	var victims []*memoryBlock
	for _, b := range m.blocks {
		if target <= 0 || b == m.active {
			break
		}
		b.refs.Add(retiredRefs)
		victims = append(victims, b)
		target -= len(b.data)
	}
	m.blocks = slices.Delete(m.blocks, 0, len(victims))
	m.retired += len(victims)
	return victims
}

// release is called with mu held, after the index has dropped the victim's
// slices. A dedicated mapping stays charged while munmap runs outside mu.
func (m *memory) release(b *memoryBlock) {
	if b.chunk != nil {
		b.slot.Release()
		b.chunk.used--
	} else {
		m.mu.Unlock()
		b.buffer.Unpin()
		m.mu.Lock()
		m.mapped -= int64(len(b.data))
	}
}

// take transfers a caller's existing pin, without changing the block count.
func (m *memory) take(buf []byte, key *Key) (memoryValue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ptr := unsafe.SliceData(buf)
	v, ok := m.borrowed[ptr]
	if !ok || len(buf) != len(v.data) {
		return memoryValue{}, ErrForeignMemory
	}
	delete(m.borrowed, ptr)
	if key != nil && v.block.refs.Load() > 0 {
		v.block.keys = append(v.block.keys, *key)
	}
	return v, nil
}

// restore returns a rejected write's existing pin to its caller. The caller
// retains ownership on ErrBusy, so a retry needs no new allocation or copy.
func (m *memory) restore(buf []byte, v memoryValue) {
	m.mu.Lock()
	m.borrowed[unsafe.SliceData(buf)] = v
	m.mu.Unlock()
}

// used includes free slots in mapped backing chunks and dedicated allocations.
func (m *memory) used() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mapped
}

func (m *memory) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.retired != 0 {
		return fmt.Errorf("blobcache: evicted buffers are still held; their memory stays mapped")
	}
	for _, b := range m.blocks {
		if b.refs.Load() != 1 {
			return fmt.Errorf("blobcache: buffers are still held; their memory stays mapped")
		}
	}
	for _, b := range m.blocks {
		b.refs.Store(0)
		m.release(b)
	}
	for _, ch := range m.chunks {
		ch.slab.Close()
		ch.buffer.Unpin()
	}
	m.pool.Close()
	m.blocks, m.chunks, m.active = nil, nil, nil
	m.mapped = 0
	return nil
}
