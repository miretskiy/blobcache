package blobcache

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

// TestMmapPool_AcquireUnpooled validates that AcquireUnpooled creates
// one-off allocations that do not return to a pool.
func TestMmapPool_AcquireUnpooled(t *testing.T) {
	giantSize := int64(1024 * 1024)
	buf := NewMmapBuffer(giantSize)
	
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
	pool := NewMmapPool("", 1024, 0, 1)
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
	pool := NewMmapPool("", 1024, 0, 1)
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
	pool := NewMmapPool("", concurrency*iters*entrySize, 0, 1)
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
	data := make([]byte, len("w-00-i-005"))
	n, err := io.ReadFull(checkReader, data)
	if err != nil {
		t.Fatalf("Failed to read: %v (read %d bytes)", err, n)
	}
	expected := "w-00-i-005"
	if string(data) != expected {
		t.Errorf("Data mismatch, got %q expected %q", string(data), expected)
	}
	checkReader.Close()
	buf.Unpin()
}
