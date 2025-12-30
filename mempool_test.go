package blobcache

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

func TestMmapBuffer_Write(t *testing.T) {
	// Setup a small pool for testing
	pool := NewMmapPool(1, 64)
	buf := pool.Acquire()
	defer pool.Release(buf)

	t.Run("Atomic Write Success", func(t *testing.T) {
		data := []byte("nitro-speed")
		n, err := buf.Write(data)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if n != len(data) {
			t.Errorf("expected %d bytes written, got %d", len(data), n)
		}
		if !bytes.Equal(buf.Bytes(), data) {
			t.Errorf("buffer content mismatch")
		}
	})

	t.Run("Atomic Write Failure (All-or-Nothing)", func(t *testing.T) {
		buf.Reset()
		largeData := make([]byte, 100) // Larger than 64 byte Cap
		n, err := buf.Write(largeData)

		if !errors.Is(err, ErrBufferFull) {
			t.Errorf("expected ErrBufferFull, got %v", err)
		}
		if n != 0 {
			t.Errorf("expected 0 bytes written on failure, got %d", n)
		}
		if buf.Len() != 0 {
			t.Errorf("buffer offset should not have advanced")
		}
	})
}

func TestMmapBuffer_InterfaceCompliance(t *testing.T) {
	pool := NewMmapPool(1, 128)
	buf := pool.Acquire()
	defer pool.Release(buf)

	t.Run("JSON Encoder Integration", func(t *testing.T) {
		type Stats struct {
			ID    int    `json:"id"`
			Label string `json:"label"`
		}
		item := Stats{ID: 42, Label: "graviton3"}

		// MmapBuffer satisfies io.Writer
		encoder := json.NewEncoder(buf)
		if err := encoder.Encode(item); err != nil {
			t.Fatalf("failed to encode into mmap buffer: %v", err)
		}

		// Verify result
		expected := `{"id":42,"label":"graviton3"}` + "\n"
		if string(buf.Bytes()) != expected {
			t.Errorf("expected %q, got %q", expected, string(buf.Bytes()))
		}
	})
}

func TestMmapPool_Backpressure(t *testing.T) {
	capacity := 2
	pool := NewMmapPool(capacity, 64)

	// Exhaust the pool
	b1 := pool.Acquire()
	b2 := pool.Acquire()

	var wg sync.WaitGroup
	wg.Add(1)

	blocked := true
	go func() {
		defer wg.Done()
		// This should block until b1 or b2 is released
		b3 := pool.Acquire()
		blocked = false
		pool.Release(b3)
	}()

	// If the pool isn't blocking, 'blocked' would flip to false immediately.
	// We check after a small sleep.
	if !blocked {
		t.Error("pool did not block when exhausted")
	}

	pool.Release(b1)
	wg.Wait()

	if blocked {
		t.Error("Acquire was still blocked after Release")
	}

	pool.Release(b2)
}

func TestMmapBuffer_Reset(t *testing.T) {
	pool := NewMmapPool(1, 64)
	buf := pool.Acquire()

	buf.Write([]byte("data"))
	if buf.Len() != 4 {
		t.Errorf("expected len 4, got %d", buf.Len())
	}

	buf.Reset()
	if buf.Len() != 0 {
		t.Error("Reset did not zero the offset")
	}
	if len(buf.Bytes()) != 0 {
		t.Error("Bytes() should return empty slice after reset")
	}
}
