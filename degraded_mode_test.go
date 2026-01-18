package blobcache

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCache_DegradedMode_FlushWriteError(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "degraded-test-*")
	defer os.RemoveAll(tmpDir)

	errInjected := errors.New("disk full")
	callCount := atomic.Int32{}

	cache, err := New(tmpDir,
		WithWriteBufferSize(2<<10), // 2KB buffer
		WithTestingKnobs(&TestingKnobs{
			InjectWriteErr: func() error {
				if callCount.Add(1) == 1 {
					return errInjected // Fail first write
				}
				return nil
			},
		}),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Write enough to trigger flush
	value := make([]byte, 1024)
	for i := 0; i < 3; i++ {
		require.NoError(t, cache.Put(fmt.Appendf(nil, "key-%d", i), value))
	}

	// Drain to ensure flush completes (and fails)
	cache.Drain()

	// Verify degraded mode
	require.NotNil(t, cache.BGError(), cache.BGError())
	require.ErrorIs(t, cache.BGError(), errInjected)

	// Subsequent Puts should still work (memory-only)
	for i := 10; i < 20; i++ {
		require.NoError(t, cache.Put(fmt.Appendf(nil, "key-%d", i), value))
	}

	// Verify data still accessible from memtable
	data, found := readAll(t, cache, []byte("key-15"))
	require.True(t, found)
	require.Equal(t, value, data)
}

func TestCache_DegradedMode_IndexError(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "degraded-test-*")
	defer os.RemoveAll(tmpDir)

	errInjected := errors.New("index corruption")
	callCount := atomic.Int32{}

	cache, err := New(tmpDir,
		WithWriteBufferSize(2<<10),
		WithTestingKnobs(&TestingKnobs{
			InjectIndexErr: func() error {
				if callCount.Add(1) == 1 {
					return errInjected
				}
				return nil
			},
		}),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Trigger flush
	value := make([]byte, 1024)
	for i := 0; i < 3; i++ {
		require.NoError(t, cache.Put(fmt.Appendf(nil, "key-%d", i), value))
	}
	cache.Drain()

	// Verify degraded
	require.NotNil(t, cache.BGError())
	require.ErrorIs(t, cache.BGError(), errInjected)

	// Cache still works in memory
	require.NoError(t, cache.Put([]byte("new-key"), value))
	data, found := readAll(t, cache, []byte("new-key"))
	require.True(t, found)
	require.Equal(t, value, data)
}

func TestCache_DegradedMode_MemtableEviction(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "degraded-test-*")
	defer os.RemoveAll(tmpDir)

	errInjected := errors.New("disk error")

	cache, err := New(tmpDir,
		WithWriteBufferSize(1<<10), // 1KB buffer (small for fast rotation)
		// Default MaxInflightSlabs is 6
		WithTestingKnobs(&TestingKnobs{
			InjectWriteErr: func() error {
				return errInjected // Always fail
			},
		}),
	)
	require.NoError(t, err)
	defer cache.Close()

	value := make([]byte, 512) // 512 bytes

	// Fill and rotate 6 memtables (to fill files list - default MaxInflightSlabs=6)
	for i := 0; i < 12; i++ {
		require.NoError(t, cache.Put(fmt.Appendf(nil, "key-%d", i), value))
		if (i+1)%2 == 0 {
			// Every 2 Puts triggers rotation (512*2 > 1KB buffer)
			require.NoError(t, cache.Put(fmt.Appendf(nil, "trigger-%d", i), make([]byte, 100)))
		}
	}

	// Drain to ensure flush fails and degraded mode set
	cache.Drain()
	require.NotNil(t, cache.BGError())

	// Now files list should be at capacity (6 memtables)
	// next rotation should drop oldest files memtable

	// Trigger more rotations (should drop oldest)
	for i := 20; i < 30; i++ {
		require.NoError(t, cache.Put(fmt.Appendf(nil, "new-key-%d", i), value))
		if (i+1)%2 == 0 {
			require.NoError(t, cache.Put(fmt.Appendf(nil, "new-trigger-%d", i), make([]byte, 100)))
		}
	}

	// Recent keys should still be in files memtables
	data, found := readAll(t, cache, []byte("new-key-25"))
	require.True(t, found)
	require.Equal(t, value, data)
}

func TestCache_DegradedMode_EvictionError(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "degraded-test-*")
	defer os.RemoveAll(tmpDir)

	errInjected := errors.New("eviction failed")
	callCount := atomic.Int32{}

	cache, err := New(tmpDir,
		WithMaxSize(5<<10), // 5KB limit
		WithTestingKnobs(&TestingKnobs{
			InjectEvictErr: func() error {
				if callCount.Add(1) == 1 {
					return errInjected
				}
				return nil
			},
		}),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Start background worker
	cache.Start()

	value := make([]byte, 1024)

	// Fill to trigger eviction
	for i := 0; i < 7; i++ {
		require.NoError(t, cache.Put(fmt.Appendf(nil, "key-%d", i), value))
	}
	cache.Drain()

	// Wait for async eviction to run and set degraded mode
	require.Eventually(t, func() bool {
		return cache.BGError() != nil
	}, time.Second, 10*time.Millisecond, "should enter degraded mode from eviction error")

	require.ErrorIs(t, cache.BGError(), errInjected)

	// Cache still works
	require.NoError(t, cache.Put([]byte("after-error"), value))
	data, found := readAll(t, cache, []byte("after-error"))
	require.True(t, found)
	require.Equal(t, value, data)
}

func TestCache_DegradedMode_DrainDuringDegraded(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "degraded-test-*")
	defer os.RemoveAll(tmpDir)

	errInjected := errors.New("flush error")

	cache, err := New(tmpDir,
		WithWriteBufferSize(1<<10), // 1KB buffer
		WithTestingKnobs(&TestingKnobs{
			InjectWriteErr: func() error {
				return errInjected // Always fail
			},
		}),
	)
	require.NoError(t, err)
	defer cache.Close()

	// Write enough to trigger flush
	value := make([]byte, 1024)
	require.NoError(t, cache.Put([]byte("key-1"), value))
	require.NoError(t, cache.Put([]byte("key-2"), value)) // Triggers rotation

	// Drain will fail and set degraded mode
	cache.Drain()
	require.NotNil(t, cache.BGError())

	// Add more data while degraded
	require.NoError(t, cache.Put([]byte("key-3"), value))
	require.NoError(t, cache.Put([]byte("key-4"), value))

	// Calling Drain() again while degraded should not block or panic
	cache.Drain() // Should return immediately (no-op)

	// Data should still be accessible
	data, found := readAll(t, cache, []byte("key-4"))
	require.True(t, found)
	require.Equal(t, value, data)
}

func TestCache_Close_WithoutDrain(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "degraded-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir,
		WithWriteBufferSize(1<<10),
	)
	require.NoError(t, err)

	// Write data but DON'T drain
	value := make([]byte, 512)
	for i := 0; i < 10; i++ {
		require.NoError(t, cache.Put(fmt.Appendf(nil, "key-%d", i), value))
	}

	// Close without drain should succeed (data may be lost)
	err = cache.Close()
	require.NoError(t, err)

	// Calling close again should be safe
	err = cache.Close()
	require.NoError(t, err)
}

func TestCache_Close_ReturnsErrors(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "degraded-test-*")
	defer os.RemoveAll(tmpDir)

	cache, err := New(tmpDir)
	require.NoError(t, err)

	// Normal close should not error
	err = cache.Close()
	require.NoError(t, err)

	// Calling close again should be safe
	err = cache.Close()
	require.NoError(t, err)
}

// Note: DegradedPanic mode is not tested here because it panics in a goroutine
// (the flush worker), which can't be caught by require.Panics. The panic is
// intentional - it's a debug feature for catching issues during benchmarking.
// Manual verification: run the test with DegradedPanic and observe the stack trace.
