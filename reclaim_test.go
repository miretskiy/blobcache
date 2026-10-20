package blobcache

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestCacheAllocatorReclaimModes(t *testing.T) {
	const page = int64(align.BlockSize)
	pool, err := alloc.NewSlabAllocator(10 * page)
	require.NoError(t, err)
	defer pool.Close()
	var held []*alloc.Buffer
	defer func() {
		for _, b := range held {
			if b != nil {
				b.Unpin()
			}
		}
	}()
	var modes []bool
	a := &cacheAllocator{Allocator: pool, capacity: 10 * page, reclaim: func(size int64, bestEffort bool) {
		require.Equal(t, page, size)
		modes = append(modes, bestEffort)
		if !bestEffort {
			require.True(t, held[0].Unpin())
			held[0] = nil
		}
	}}
	for i := range 10 {
		b, err := a.TryAcquire(1)
		require.NoError(t, err)
		held = append(held, b)
		if i < 7 {
			require.Empty(t, modes, "no proactive signal below 80%%")
		}
	}
	require.Equal(t, []bool{true, true, true}, modes)
	modes = nil
	b, err := a.TryAcquire(1)
	require.NoError(t, err, "required reclamation releases capacity before acquisition")
	held = append(held, b)
	require.Equal(t, []bool{false, true}, modes)
}

func TestStartCacheReclaimDelivery(t *testing.T) {
	c := &Cache{reclaimRequests: make(chan reclaimRequest, 1)}
	queued := make(chan struct{})
	go func() {
		for range 100 {
			c.startCacheReclaim(4096, true)
		}
		close(queued)
	}()
	select {
	case <-queued:
	case <-time.After(5 * time.Second):
		t.Fatal("best-effort signals blocked on a full queue")
	}
	finished := make(chan struct{})
	go func() {
		c.startCacheReclaim(8192, false)
		close(finished)
	}()
	first := <-c.reclaimRequests
	require.Nil(t, first.done)
	var required reclaimRequest
	select {
	case required = <-c.reclaimRequests:
	case <-time.After(5 * time.Second):
		t.Fatal("required reclamation was dropped")
	}
	require.EqualValues(t, 8192, required.size)
	require.NotNil(t, required.done)
	select {
	case <-finished:
		t.Fatal("required reclamation returned before its pass completed")
	default:
	}
	close(required.done)
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("completed reclamation did not wake the caller")
	}
}

func TestCacheProactiveReclamation(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	for i := range 26 { // 26 of 32 pages crosses 80% usage.
		putValue(t, c, fmt.Sprint(i), []byte("v"))
	}
	require.Eventually(t, func() bool {
		return c.memory.Available() >= c.memory.Capacity()/4
	}, 5*time.Second, time.Millisecond, "reclamation starts without a failed allocation")
	require.Equal(t, 26, c.values.entries.Len(), "all disk metadata survives")
}

func TestCacheCloseJoinsReclaimer(t *testing.T) {
	c := newPutCache(t, t.TempDir())
	started, resume := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	})
	b, err := c.memory.tryAcquire(align.BlockSize)
	require.NoError(t, err)
	b.WrapRelease(func(release func()) func() {
		return func() {
			close(started)
			<-resume
			release()
		}
	})
	c.values.replace(xmap.Key{}, b, blobstore.RecordHeader{})
	c.startCacheReclaim(c.memory.Capacity(), true)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("reclamation worker did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned during reclamation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(resume)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join the reclamation worker")
	}
	require.Zero(t, c.memory.used.Load())
	require.Zero(t, c.values.entries.Len())
}

func TestPutConcurrentMemoryPressure(t *testing.T) {
	dir := t.TempDir()
	c := newPutCache(t, dir)
	const workers, iterations = 4, 40
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)
	for worker := range workers {
		group.Go(func() error {
			for i := range iterations {
				key := []byte(fmt.Sprintf("%d-%d", worker, i))
				var w *Writer
				var err error
				for {
					if err := ctx.Err(); err != nil {
						return err
					}
					w, err = c.NewWriter(key, 1)
					if !errors.Is(err, ErrBusy) {
						break
					}
					runtime.Gosched()
				}
				if err != nil {
					return err
				}
				_, err = w.Write([]byte("v"))
				if err == nil {
					err = c.Put(w)
				}
				if err = errors.Join(err, w.Close()); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, group.Wait())
	c.store.Wait()
	require.Equal(t, workers*iterations, c.values.entries.Len())
	require.NoError(t, c.Close())
	require.Len(t, diskRecords(t, dir), workers*iterations)
}
