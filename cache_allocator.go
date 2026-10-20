package blobcache

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

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

func (a *cacheAllocator) tryAcquire(size int64) (*alloc.Buffer, error) {
	b, err := a.Allocator.TryAcquire(size)
	if err != nil {
		return nil, err
	}
	allocated := b.Cap()
	a.used.Add(allocated)
	b.WrapRelease(func(release func()) func() {
		return func() {
			// Subtract before returning chunks so their next lease cannot be
			// counted alongside the old one if another goroutine reuses them.
			a.used.Add(-allocated)
			release()
		}
	})
	return b, nil
}

func (a *cacheAllocator) TryAcquire(size int64) (*alloc.Buffer, error) {
	reclaimed := a.Available() < size
	if reclaimed {
		a.reclaim(align.PageAlign(size), false)
	}
	b, err := a.tryAcquire(size)
	if errors.Is(err, alloc.ErrExhausted) && !reclaimed {
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

// Acquire retries reclamation and acquisition until storage is available or the
// context is cancelled. Blobstore uses TryAcquire, which waits for a required
// reclamation pass but does not wait for borrowers to release pinned buffers.
func (a *cacheAllocator) Acquire(ctx context.Context, size int64) (*alloc.Buffer, error) {
	b, err := a.TryAcquire(size)
	if !errors.Is(err, alloc.ErrExhausted) {
		return b, err
	}
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			if b, err := a.TryAcquire(size); !errors.Is(err, alloc.ErrExhausted) {
				return b, err
			}
		}
	}
}
