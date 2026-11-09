package blobcache

import (
	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/alloc"
	"testing"
)

type testPool struct{ *alloc.Allocator }

func newTestPool(t testing.TB, size int64) *testPool {
	a := alloc.NewAllocator(size)
	t.Cleanup(a.Close)
	return &testPool{a}
}
func (p *testPool) TryAcquire(n int64) (blobstore.Memory, error) {
	m, err := p.Allocator.TryAcquire(n)
	if err != nil {
		return nil, err
	}
	return blobstore.Memory{m}, nil
}
func newTestMemory(size int64) blobstore.Memory { return blobstore.Memory{alloc.NewMemory(size)} }
func singleMemoryChunk(m blobstore.Memory) []byte {
	if len(m) != 1 {
		panic("test requires contiguous memory")
	}
	return m[0].Bytes()
}
