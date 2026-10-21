package blobcache

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
)

// cacheAllocator sees the actual framed or staging allocation requested by
// blobstore. All acquisitions from its allocator must go through this wrapper so
// release callbacks can account for every live buffer, including pinned victims.
// capacity is the allocator's total usable bytes, supplied at construction.
// The callback owns reclamation and its synchronization. bestEffort requests
// must not block; required requests wait for a reclamation pass to finish.
type cacheAllocator struct {
	alloc.Allocator
	capacity int64
	reclaim  func(size int64, bestEffort bool)
	used     atomic.Int64
}

// Capacity returns the configured total capacity of the wrapped allocator.
func (a *cacheAllocator) Capacity() int64 { return a.capacity }

// Available reads the live-buffer counter without consulting the allocator.
// Acquisition/release and accounting are separate steps, so this snapshot does
// not reserve capacity; TryAcquire remains authoritative.
func (a *cacheAllocator) Available() int64 {
	return a.Capacity() - a.used.Load()
}

func (a *cacheAllocator) wrapBuffer(b *alloc.Buffer) *alloc.Buffer {
	allocated := b.Cap()
	a.used.Add(allocated)
	b.WrapRelease(func(release func()) func() {
		return func() {
			a.used.Add(-allocated)
			release()
		}
	})
	return b
}

func (a *cacheAllocator) tryAcquire(size int64) (*alloc.Buffer, error) {
	b, err := a.Allocator.TryAcquire(size)
	if err != nil {
		return nil, err
	}
	return a.wrapBuffer(b), nil
}

func (a *cacheAllocator) TryAcquire(size int64) (*alloc.Buffer, error) {
	needCapacity := a.Available() < size
	if needCapacity {
		a.reclaim(align.PageAlign(size), false)
	}
	b, err := a.tryAcquire(size)
	if errors.Is(err, alloc.ErrExhausted) && !needCapacity {
		// Another allocation may have consumed the capacity in our snapshot.
		a.reclaim(align.PageAlign(size), false)
		b, err = a.tryAcquire(size)
	}
	if err == nil && a.used.Load() >= a.capacity-a.capacity/5 {
		// At 80% occupancy, ask for headroom without delaying this allocation.
		a.reclaim(b.Cap(), true)
	}
	return b, err
}

// Acquire waits for the underlying allocator to supply storage or for the context
// to be cancelled. Blobstore uses TryAcquire, which requests cache reclamation
// before returning allocation exhaustion to the caller.
func (a *cacheAllocator) Acquire(ctx context.Context, size int64) (*alloc.Buffer, error) {
	b, err := a.Allocator.Acquire(ctx, size)
	if err != nil {
		return nil, err
	}
	return a.wrapBuffer(b), nil
}
