package blobcache

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/miretskiy/blobcache/metadata"
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
		MmapPool: NewMmapPool(capacity, slabSize, 0),
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

// MockBatcher implements: PutBatch(segID int64, records []metadata.BlobRecord) error
type MockBatcher struct {
	mu      sync.Mutex
	Batches map[int64][]metadata.BlobRecord
	Count   int
}

func (m *MockBatcher) PutBatch(segID int64, records []metadata.BlobRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Batches == nil {
		m.Batches = make(map[int64][]metadata.BlobRecord)
	}
	// Copy slice to simulate persistent storage.
	m.Batches[segID] = append(m.Batches[segID], records...)
	m.Count += len(records)
	return nil
}

// MockHealthReporter tracks desyncs or disk failures.
type MockHealthReporter struct {
	mu           sync.Mutex
	ReportedErr  error
	DegradedFlag bool
}

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

// Helpers
func testRoundToPage(size int64) int64 {
	const pageSize = 4096
	return (size + pageSize - 1) & ^(pageSize - 1)
}

func TestSegmentWriter_FullCycle(t *testing.T) {
	tmpDir := t.TempDir()
	const slabSize = 1024 * 1024
	const segSize = 4 * 1024 * 1024
	const segID = int64(777)

	pool := NewTrackedPool(4, slabSize)
	defer pool.Teardown()

	path := filepath.Join(tmpDir, "777.seg")

	t.Run("AlignedPhysicalWrites", func(t *testing.T) {
		sw, err := NewSegmentWriter(segID, path, segSize, pool)
		require.NoError(t, err)

		// Slab 1
		slab1 := pool.Acquire()
		data1 := []byte("direct-io-block-1")
		// Use WriteAt instead of Append.
		slab1.WriteAt(data1, 0)
		slab1.Seal(int64(len(data1))) // Mark as ready for O_DIRECT.

		recs1 := []metadata.BlobRecord{{
			Hash:        101,
			Pos:         0,
			LogicalSize: int64(len(data1)), // Renamed field.
		}}
		require.NoError(t, sw.WriteSlab(slab1.AlignedBytes(), recs1))
		slab1.Unpin()

		// Slab 2
		slab2 := pool.Acquire()
		data2 := []byte("direct-io-block-2")
		slab2.WriteAt(data2, 0)
		slab2.Seal(int64(len(data2)))

		recs2 := []metadata.BlobRecord{{
			Hash:        202,
			Pos:         0,
			LogicalSize: int64(len(data2)),
		}}
		require.NoError(t, sw.WriteSlab(slab2.AlignedBytes(), recs2))
		slab2.Unpin()

		require.NoError(t, sw.Close())

		// Verify Footer Recovery
		f, err := os.Open(path)
		require.NoError(t, err)
		defer f.Close()

		info, _ := f.Stat()
		footer, _, err := metadata.ReadSegmentFooterFromFile(f, info.Size(), segID)
		require.NoError(t, err)

		// Verify Pos rounding (critical for Direct I/O reads)
		// Slab 2 should be positioned at the next 4KB boundary.
		expectedOffset := testRoundToPage(int64(len(data1)))
		require.Equal(t, expectedOffset, footer.Records[1].Pos)
	})
}

func TestMemTable_Integration_Rotation(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := config{
		Path:               tmpDir,
		WriteBufferSize:    512 * 1024,
		SegmentSize:        1024 * 1024,
		MaxInflightBatches: 4,
		FlushConcurrency:   2,
		Shards:             1,
	}

	mb := &MockBatcher{}
	mh := &MockHealthReporter{}

	mt := NewMemTable(cfg, mb, mh)
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
		mt.Put(key, data)
		if i == 0 {
			// After first put, check if it's in memtable
			if _, found := mt.Get(key, nil); !found {
				t.Fatalf("First Put failed: key %d not found in memtable", i)
			}
			t.Logf("First key successfully written to memtable")
		}
	}

	mt.Drain()

	// Check if any errors were reported during flush
	if mh.DegradedFlag {
		t.Fatalf("MemTable entered degraded mode: %v", mh.ReportedErr)
	}

	// Debug: Check what actually happened
	t.Logf("Batcher received %d blobs (expected %d)", mb.Count, blobCount)
	t.Logf("Batcher has %d segment batches", len(mb.Batches))
	for segID, recs := range mb.Batches {
		t.Logf("  Segment %d: %d records", segID, len(recs))
	}

	// 1. Verify all records hit the Batcher.
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
