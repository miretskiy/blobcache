package blobcache

import (
	"testing"

	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
)

type testPool struct{ *alloc.Allocator }

func newTestPool(t testing.TB, size int64) *testPool {
	a := alloc.NewAllocator(size)
	t.Cleanup(a.Close)
	return &testPool{a}
}

func (p *testPool) TryAcquire(n int64) (*alloc.ReadOnlyBuffer, error) {
	b := alloc.NewAllocatingBuffer(p.Allocator)
	if err := b.Grow(int(n)); err != nil {
		b.Release()
		return nil, err
	}
	return b.Freeze(), nil
}

func newTestMemory(t *testing.T, size int64) *alloc.ReadOnlyBuffer {
	b, err := newTestPool(t, size).TryAcquire(size)
	require.NoError(t, err)
	return b
}

func singleMemoryChunk(b *alloc.ReadOnlyBuffer) []byte {
	if len(b.First()) != b.Cap() {
		panic("test requires contiguous memory")
	}
	return b.First()
}

func memorySize(b *alloc.ReadOnlyBuffer) int64 { return int64(b.Cap()) }
func unpinMemory(b *alloc.ReadOnlyBuffer) bool { return b.Release() == int64(b.Cap()) }
