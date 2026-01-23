package blobcache

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/xmap"
	"github.com/stretchr/testify/require"
)

// TrackedPool wraps the production MmapPool for deterministic cleanup.
type TrackedPool struct {
	*MmapPool
	mu           sync.Mutex
	extraRegions [][]byte
}

func NewTrackedPool(capacity int, slabSize int64) *TrackedPool {
	return &TrackedPool{
		MmapPool: NewMmapPool("", slabSize, capacity),
	}
}

func (tp *TrackedPool) AcquireAligned(size int64) *MmapBuffer {
	buf := tp.MmapPool.AcquireAligned(size)
	// If buf.pool is nil, it's a one-off unpooled allocation that needs tracking.
	if buf.pool == nil {
		tp.mu.Lock()
		tp.extraRegions = append(tp.extraRegions, buf.Bytes())
		tp.mu.Unlock()
	}
	return buf
}

func (tp *TrackedPool) Teardown() {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	// Clean up unpooled regions.
	for _, r := range tp.extraRegions {
		_ = unix.Munmap(r)
	}
	tp.extraRegions = nil
}

// MockBatcher implements the Batcher interface for testing.
type MockBatcher struct {
	mu      sync.Mutex
	Batches map[uint32][]index.Item
	Count   int
}

func (m *MockBatcher) PutBatch(segID uint32, items []index.Item, _ uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Batches == nil {
		m.Batches = make(map[uint32][]index.Item)
	}
	// Copy slice to simulate persistent storage.
	m.Batches[segID] = append(m.Batches[segID], items...)
	m.Count += len(items)
	return nil
}

// MockHealthReporter tracks desyncs or disk failures.
type MockHealthReporter struct {
	mu           sync.Mutex
	ReportedErr  error
	DegradedFlag bool
}

type mockLibrarian struct{}

func (m mockLibrarian) Publish(slab *SharedSlab) {
}

var _ Publisher = mockLibrarian{}

func (m *MockHealthReporter) ReportError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReportedErr = err
	m.DegradedFlag = true
}

func (m *MockHealthReporter) IsDegraded() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.DegradedFlag
}

func (m *MockHealthReporter) ReportBlobError(key Key, errno base.BlobErrno) {
	// No-op for tests
}

func TestMemTable_Integration_Rotation(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := config{
		Path:             tmpDir,
		WriteBufferSize:  512 * 1024,
		MaxInflightSlabs: 4,
		FlushConcurrency: 2,
		Shards:           1,
	}

	// Create segment directory structure (normally done by checkOrInitialize)
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "segments", "0000"), 0o755))

	mb := &MockBatcher{}
	mh := &MockHealthReporter{}

	mt := NewMemTable(cfg, mb, mh, &mockLibrarian{}, nil, newSegmentIDProvider(cfg.Path, cfg.Shards))
	defer mt.Close()

	// Ingest blobs to force rotation across multiple 1MB segments.
	blobCount := 20
	blobSize := 100 * 1024
	data := make([]byte, blobSize)
	for i := 0; i < blobSize; i++ {
		data[i] = byte(i % 256)
	}

	for i := 0; i < blobCount; i++ {
		key := xmap.Key{Lo: uint64(i), Hi: 0}
		keyBytes := []byte("test-key")
		require.NoError(t, mt.Put(uint64(i+1), key, keyBytes, data))
	}

	mt.Drain()

	// Check if any errors were reported during flush
	if mh.DegradedFlag {
		t.Fatalf("MemTable entered degraded mode: %v", mh.ReportedErr)
	}

	// Verify all records hit the Batcher.
	require.Equal(t, blobCount, mb.Count)

	// 2. Verify rotation occurred.
	require.GreaterOrEqual(t, len(mb.Batches), 2, "Should have flushed multiple segments")

	// 3. Verify physical segments exist on disk.
	segCount := 0
	err := filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".seg" {
			segCount++
		}
		return nil
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, segCount, 2)
}
