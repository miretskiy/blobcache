package blobcache

import (
	"time"

	"github.com/miretskiy/dio/mempool"
)

// MmapBuffer and MmapPool are type aliases for the dio/mempool implementations.
// All reference counting, ABA safety, and backpressure logic lives there.
type (
	MmapBuffer = mempool.MmapBuffer
	MmapPool   = mempool.MmapPool
)

// NewMmapBuffer allocates a standalone unpooled page-aligned buffer of at
// least size bytes. It is unmapped when Unpin reduces the reference count to zero.
func NewMmapBuffer(size int64) *MmapBuffer { return mempool.NewMmapBuffer(size) }

// NewMmapPool creates a fixed-capacity pool of page-aligned mmap buffers and
// configures a 10-second slow-acquire warning to aid debugging undersized pools.
func NewMmapPool(name string, bufferSize int64, capacity int) *MmapPool {
	p := mempool.NewMmapPool(name, bufferSize, capacity)
	p.SetSlowAcquireWarning(10 * time.Second)
	return p
}
