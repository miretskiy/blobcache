package blobcache

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestMmapPool_FrozenHandover validates the state machine:
// Buffers should NOT return to the pool until they are both Unpinned AND Frozen.
// This is critical for the MemTable rotation logic.
func TestMmapPool_FrozenHandover(t *testing.T) {
	pool := NewMmapPool(1, 1024, 0)
	buf := pool.Acquire() // refCount = 1, isFrozen = false

	// 1. Simulate owner finishing work, but NOT freezing yet.
	// This happens when a write completes but the MemTable isn't full.
	buf.Unpin() // refCount = 0

	select {
	case <-pool.buffers:
		t.Fatal("Buffer returned to pool before being marked as Frozen!")
	default:
		// Success: refCount is 0, but it's waiting for the "Freeze" signal.
	}

	// 2. Simulate MemTable rotation (The Freeze signal).
	// In production, isFrozen is set to true before the final flusher Unpins.
	buf.isFrozen.Store(true)

	// We need to trigger the logic that checks (refCount == 0 && isFrozen).
	// Since refCount is already 0, we simulate a final activity or just
	// call the internal reset logic.
	buf.refCount.Store(1)
	buf.Unpin() // This call should now trigger resetAndRelease()

	select {
	case <-pool.buffers:
		// Success: now it's back in the pool for reuse.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Buffer failed to return to pool after being Frozen and Unpinned")
	}
}

// TestMmapPool_AcquireAligned validates the "Dual Path" (Pooled vs Unpooled).
// This ensures the SegmentWriter can handle standard footers and massive ones.
func TestMmapPool_AcquireAligned(t *testing.T) {
	capacity := 1
	standardSize := int64(1024)
	pool := NewMmapPool(capacity, standardSize, 0)

	// 1. Test Pooled Path: Fits within standard poolSize.
	buf1 := pool.AcquireAligned(512)
	if buf1.pool != pool || int64(len(buf1.raw)) < standardSize {
		t.Error("Should have acquired from pool for small request")
	}
	buf1.isFrozen.Store(true)
	buf1.Unpin()

	// 2. Test Pathological Path: Request exceeds standard poolSize.
	giantSize := int64(1024 * 1024)
	buf2 := pool.AcquireAligned(giantSize)
	if buf2.pool != nil {
		t.Error("Large request should have returned unpooled buffer (nil pool)")
	}
	if int64(len(buf2.raw)) < giantSize {
		t.Errorf("Expected at least %d bytes, got %d", giantSize, len(buf2.raw))
	}
	// Unpooled buffers are unmapped via GC/Cleanup or manual Munmap.
	buf2.Unpin()
}

// TestMmapBuffer_ReaderRefCounting validates that concurrent readers
// properly hold the "pin" even if the primary owner is done.
func TestMmapBuffer_ReaderRefCounting(t *testing.T) {
	pool := NewMmapPool(1, 1024, 0)
	buf := pool.Acquire()
	buf.isFrozen.Store(true)

	// Create two concurrent readers.
	r1 := buf.NewSectionReader(0, 10)
	r2 := buf.NewSectionReader(10, 10) // refCount is now 3 (1 Owner + 2 Readers)

	// Primary owner (MemTable) finishes and unpins.
	buf.Unpin() // refCount = 2

	select {
	case <-pool.buffers:
		t.Fatal("Buffer returned to pool while readers were still active")
	default:
		// Correct.
	}

	// Close first reader.
	r1.Close() // refCount = 1
	select {
	case <-pool.buffers:
		t.Fatal("Buffer returned to pool while one reader was still active")
	default:
	}

	// Close final reader.
	r2.Close() // refCount = 0 -> Release()
	select {
	case <-pool.buffers:
		// Success: Finally returned.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Buffer stuck; failed to return after all readers closed")
	}
}

// TestMmapPool_SafetyNet validates the Go 1.24+ runtime.AddCleanup fallback.
// This ensures that even "leaked" handles eventually return memory to the pool.
func TestMmapPool_SafetyNet(t *testing.T) {
	pool := NewMmapPool(1, 1024, 0)
	buf := pool.Acquire()
	buf.isFrozen.Store(true)

	// Create a reader in a scope and let it "leak" (not calling Close).
	func() {
		_ = buf.NewSectionReader(0, 5)
	}()

	buf.Unpin() // refCount remains 1 due to the leaked reader.

	select {
	case <-pool.buffers:
		t.Fatal("Buffer returned to pool before GC reaped the leaked handle")
	default:
	}

	// Trigger GC to fire AddCleanup for the MmapHandle.
	for i := 0; i < 3; i++ {
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}

	select {
	case <-pool.buffers:
		// Success: Safety net reclaimed the leaked handle.
	case <-time.After(1 * time.Second):
		t.Fatal("Safety net failed to reclaim leaked handle after GC")
	}
}

// TestMmapPool_Stress validates concurrent atomic access and data integrity.
func TestMmapPool_Stress(t *testing.T) {
	const (
		concurrency = 32
		iters       = 500
		size        = 4096
	)
	pool := NewMmapPool(10, size, 0)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				b := pool.Acquire()
				b.isFrozen.Store(true)

				payload := []byte(fmt.Sprintf("worker-%d-iter-%d", workerID, j))
				_, _ = b.Append(payload)

				r := b.NewSectionReader(0, int64(len(payload)))
				data, _ := io.ReadAll(r)

				if string(data) != string(payload) {
					t.Errorf("Data corruption: expected %s, got %s", payload, data)
				}

				_ = r.Close()
				b.Unpin()
			}
		}(i)
	}
	wg.Wait()
}

func TestMmap_AllocatePreWarm(t *testing.T) {
	size := 8192
	buf := allocate(int64(size))

	// Verify pre-warming: all bytes should be zeroed out.
	for i, b := range buf.raw {
		if b != 0 {
			t.Fatalf("Byte at index %d was not zeroed during allocation", i)
		}
	}
	// Manual cleanup for this one-off allocation.
	_ = unix.Munmap(buf.raw)
}
