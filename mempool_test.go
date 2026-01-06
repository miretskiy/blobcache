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

// TestMmapPool_SealHandover validates the state machine:
// Buffers should NOT return to the pool until they are both Unpinned AND Sealed.
// This uses the int64 sentinel (-1 = active, >= 0 = sealed).
func TestMmapPool_SealHandover(t *testing.T) {
	pool := NewMmapPool(1, 1024, 0)
	buf := pool.Acquire() // refCount = 1, off = -1 (Active)
	
	// 1. Simulate owner finishing work, but NOT sealing yet.
	// This happens during active writes before a MemTable rotation.
	buf.Unpin() // refCount = 0
	
	select {
	case <-pool.buffers:
		t.Fatal("Buffer returned to pool before being Sealed (off was still -1)!")
	default:
		// Success: refCount is 0, but it's waiting for the "Seal" signal.
	}
	
	// 2. Simulate MemTable rotation (The Seal signal).
	// Calling Seal(finalOffset) sets off >= 0, marking it ready for release.
	buf.Seal(100) // off = 100
	
	// Trigger the logic that checks (refCount == 0 && off >= 0).
	// We increment/decrement to trigger the Unpin logic.
	buf.refCount.Store(1)
	buf.Unpin() // This call should now trigger resetAndRelease()
	
	select {
	case <-pool.buffers:
		// Success: now it's back in the pool for reuse.
		if buf.off.Load() != -1 {
			t.Errorf("Expected offset to be reset to -1, got %d", buf.off.Load())
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Buffer failed to return to pool after being Sealed and Unpinned")
	}
}

// TestMmapPool_AcquireUnpooled validates that AcquireUnpooled creates
// one-off allocations that do not return to a pool.
func TestMmapPool_AcquireUnpooled(t *testing.T) {
	pool := NewMmapPool(1, 1024, 0)
	
	giantSize := int64(1024 * 1024)
	buf := pool.AcquireUnpooled(giantSize)
	
	if buf.pool != nil {
		t.Error("AcquireUnpooled should return buffer with nil pool")
	}
	if int64(len(buf.raw)) < giantSize {
		t.Errorf("Expected at least %d bytes, got %d", giantSize, len(buf.raw))
	}
	
	buf.Seal(giantSize)
	buf.Unpin() // Should Munmap via GC/Cleanup instead of returning to channel.
}

// TestMmapBuffer_ReaderRefCounting validates that concurrent readers
// properly hold the "pin" even after the MemTable is done with the slab.
func TestMmapBuffer_ReaderRefCounting(t *testing.T) {
	pool := NewMmapPool(1, 1024, 0)
	buf := pool.Acquire()
	buf.Seal(1024) // Mark as ready for release once readers finish
	
	// Create concurrent readers.
	r1 := buf.NewSectionReader(0, 10)  // refCount = 2
	r2 := buf.NewSectionReader(10, 10) // refCount = 3
	
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
		t.Fatal("Buffer returned to pool while r2 was still active")
	default:
	}
	
	// Close final reader.
	r2.Close() // refCount = 0 -> resetAndRelease()
	select {
	case <-pool.buffers:
		// Success: Finally returned.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Buffer stuck; failed to return after all readers closed")
	}
}

// TestMmapPool_SafetyNet validates the Go 1.24+ runtime.AddCleanup fallback.
func TestMmapPool_SafetyNet(t *testing.T) {
	pool := NewMmapPool(1, 1024, 0)
	buf := pool.Acquire()
	buf.Seal(100)
	
	// Create a reader and leak it (don't call Close).
	func() {
		_ = buf.NewSectionReader(0, 5)
	}() // Reader falls out of scope
	
	buf.Unpin() // refCount remains 1 due to leaked reader
	
	select {
	case <-pool.buffers:
		t.Fatal("Buffer returned to pool before GC reaped handle")
	default:
	}
	
	// Trigger GC cleanup
	for i := 0; i < 3; i++ {
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}
	
	select {
	case <-pool.buffers:
		// Success.
	case <-time.After(1 * time.Second):
		t.Fatal("Safety net failed to reclaim leaked handle after GC")
	}
}

// TestMmapBuffer_WriteAt_Stress validates concurrent writes via the
// unified reservation pattern (simulating multiple PutActive threads).
func TestMmapBuffer_WriteAt_Stress(t *testing.T) {
	const (
		concurrency = 32
		iters       = 100
		entrySize   = 64
	)
	pool := NewMmapPool(1, concurrency*iters*entrySize, 0)
	buf := pool.Acquire()
	
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				offset := int64((workerID*iters + j) * entrySize)
				payload := []byte(fmt.Sprintf("w-%02d-i-%03d", workerID, j))
				
				buf.WriteAt(payload, offset)
			}
		}(i)
	}
	wg.Wait()
	
	// Seal and verify a random entry
	buf.Seal(concurrency * iters * entrySize)
	checkReader := buf.NewSectionReader(entrySize*5, entrySize)
	data := make([]byte, 11)
	io.ReadFull(checkReader, data)
	if string(data) != "w-00-i-005" {
		t.Errorf("Data mismatch, got %s", data)
	}
	checkReader.Close()
	buf.Unpin()
}

func TestMmap_AllocatePreWarm(t *testing.T) {
	size := 8192
	buf := allocate(int64(size))
	
	// Verify pre-warming: all bytes should be zeroed out
	for i, b := range buf.raw {
		if b != 0 {
			t.Fatalf("Byte at index %d was not zeroed during allocation", i)
		}
	}
	// Manual Munmap for the raw slice.
	_ = unix.Munmap(buf.raw)
}
