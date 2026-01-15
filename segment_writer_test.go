package blobcache

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/blobcache/base"
	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
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
		MmapPool: NewMmapPool("", slabSize, 0, capacity),
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

// MockBatcher implements: PutBatch(segID uint32, items []index.Item) error
type MockBatcher struct {
	mu      sync.Mutex
	Batches map[uint32][]index.Item
	Count   int
}

func (m *MockBatcher) PutBatch(segID uint32, items []index.Item) error {
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

// Helpers
func testRoundToPage(size int64) int64 {
	const pageSize = 4096
	return (size + pageSize - 1) & ^(pageSize - 1)
}

func TestSegmentWriter_FullCycle(t *testing.T) {
	tmpDir := t.TempDir()
	const slabSize = 1024 * 1024
	const segSize = 4 * 1024 * 1024
	const segID = uint32(777)

	pool := NewTrackedPool(4, slabSize)
	defer pool.Teardown()

	path := filepath.Join(tmpDir, "777.seg")

	t.Run("AlignedPhysicalWrites", func(t *testing.T) {
		sw, err := NewSegmentWriter(segID, path, segSize, pool, false, true)
		require.NoError(t, err)

		// Slab 1
		slab1 := pool.Acquire()
		data1 := []byte("direct-io-block-1")
		slab1.WriteAt(data1, 0)
		slab1Len := int64(len(data1))

		entries1 := []record.FooterEntry{{
			Hash:        101,
			Pos:         0,
			LogicalSize: slab1Len,
		}}
		_, err = sw.WriteSlab(slab1.AlignedBytes(slab1Len), entries1)
		require.NoError(t, err)
		slab1.Unpin()

		// Slab 2
		slab2 := pool.Acquire()
		data2 := []byte("direct-io-block-2")
		slab2.WriteAt(data2, 0)
		slab2Len := int64(len(data2))

		entries2 := []record.FooterEntry{{
			Hash:        202,
			Pos:         0,
			LogicalSize: slab2Len,
		}}
		_, err = sw.WriteSlab(slab2.AlignedBytes(slab2Len), entries2)
		require.NoError(t, err)
		slab2.Unpin()

		require.NoError(t, sw.Close())

		// Verify Footer Recovery
		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()

		info, _ := f.Stat()
		footer, _, err := record.ReadSegmentFooterFromFile(f, info.Size(), int64(segID))
		require.NoError(t, err)

		// Verify Pos rounding (critical for Direct I/O reads)
		// Slab 2 should be positioned at the next 4KB boundary.
		expectedOffset := testRoundToPage(int64(len(data1)))
		require.Equal(t, expectedOffset, footer.Entries[1].Pos)
	})
}

func TestMemTable_Integration_Rotation(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := config{
		Path:             tmpDir,
		WriteBufferSize:  512 * 1024,
		SegmentSize:      1024 * 1024,
		MaxInflightSlabs: 4,
		FlushConcurrency: 2,
		Shards:           1,
	}

	mb := &MockBatcher{}
	mh := &MockHealthReporter{}

	mt := NewMemTable(cfg, mb, mh, &mockLibrarian{})
	defer mt.Close()

	// Ingest blobs to force rotation across multiple 1MB segments.
	blobCount := 20
	blobSize := 100 * 1024
	data := make([]byte, blobSize)
	for i := 0; i < blobSize; i++ {
		data[i] = byte(i % 256)
	}

	for i := 0; i < blobCount; i++ {
		key := Key(i)
		require.NoError(t, mt.Put(uint64(i+1), key, data))
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

	// 3. Verify physical mu exist on disk.
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
