package index

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/miretskiy/blobcache/base"
	"github.com/stretchr/testify/require"
)

func TestIndex_PutGet(t *testing.T) {
	// Create temp directory for test
	tmpDir, err := os.MkdirTemp("", "blobcache-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Open index
	idx, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	defer idx.Close()

	ctx := context.Background()

	// Put an entry
	key := base.Key([]byte("test-key"))
	now := time.Now().UnixNano()
	segmentID := fmt.Sprintf("%d-00", now)
	pos := int64(1024)

	err = idx.Put(ctx, key, segmentID, pos, 1024, now)
	require.NoError(t, err)

	// Get it back
	var entry Entry
	err = idx.Get(ctx, key, &entry)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Verify
	if entry.SegmentID != segmentID {
		t.Errorf("SegmentID = %s, want %s", entry.SegmentID, segmentID)
	}

	if entry.Pos != pos {
		t.Errorf("Pos = %d, want %d", entry.Pos, pos)
	}

	if entry.Size != 1024 {
		t.Errorf("Size = %d, want 1024", entry.Size)
	}

	if entry.CTime != now {
		t.Errorf("CTime = %d, want %d", entry.CTime, now)
	}
}

func TestIndex_GetNotFound(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	idx, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}
	defer idx.Close()

	key := base.Key([]byte("nonexistent"))
	var entry Entry
	err = idx.Get(context.Background(), key, &entry)

	if err != ErrNotFound {
		t.Errorf("Get() error = %v, want ErrNotFound", err)
	}
}

// TestIndex_UpdateSameKey removed - with memtable design, duplicate keys are handled
// by skipmap overwriting in memory, not at index level. Index sees unique keys only.

func TestIndex_Delete(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	idx, _ := New(tmpDir)
	defer idx.Close()

	ctx := context.Background()
	key := base.Key([]byte("delete-me"))

	// Insert
	now := time.Now().UnixNano()
	segmentID := fmt.Sprintf("%d-00", now)
	idx.Put(ctx, key, segmentID, 0, 500, now)

	// Delete
	err := idx.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify gone
	var entry Entry
	err = idx.Get(ctx, key, &entry)
	if err != ErrNotFound {
		t.Errorf("After delete, Get() = %v, want ErrNotFound", err)
	}

	// Delete again should return ErrNotFound
	err = idx.Delete(ctx, key)
	if err != ErrNotFound {
		t.Errorf("Delete nonexistent = %v, want ErrNotFound", err)
	}
}

func TestIndex_TotalSizeOnDisk(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	idx, _ := New(tmpDir)
	defer idx.Close()

	ctx := context.Background()

	// Empty database
	total, err := idx.TotalSizeOnDisk(ctx)
	if err != nil {
		t.Fatalf("TotalSizeOnDisk failed: %v", err)
	}

	if total != 0 {
		t.Errorf("Empty total = %d, want 0", total)
	}

	// Put entries
	now := time.Now().UnixNano()
	segmentID := fmt.Sprintf("%d-00", now)
	idx.Put(ctx, base.Key([]byte("key1")), segmentID, 0, 100, now)
	idx.Put(ctx, base.Key([]byte("key2")), segmentID, 100, 200, now)
	idx.Put(ctx, base.Key([]byte("key3")), segmentID, 300, 300, now)

	total, err = idx.TotalSizeOnDisk(ctx)
	if err != nil {
		t.Fatalf("TotalSizeOnDisk failed: %v", err)
	}

	if total != 600 {
		t.Errorf("Total = %d, want 600", total)
	}

	// Delete one
	idx.Delete(ctx, base.Key([]byte("key2")))

	total, _ = idx.TotalSizeOnDisk(ctx)
	if total != 400 {
		t.Errorf("After delete, total = %d, want 400", total)
	}
}

func TestIndex_Persistence(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	key := base.Key([]byte("persist-key"))
	now := time.Now().UnixNano()
	segmentID := fmt.Sprintf("%d-00", now)

	// Create index and insert
	idx1, _ := New(tmpDir)
	idx1.Put(ctx, key, segmentID, 0, 999, now)
	idx1.Close()

	// Reopen and verify data persisted
	idx2, err := New(tmpDir)
	if err != nil {
		t.Fatalf("Failed to reopen index: %v", err)
	}
	defer idx2.Close()

	var entry Entry
	err = idx2.Get(ctx, key, &entry)
	if err != nil {
		t.Fatalf("After reopen, Get failed: %v", err)
	}

	if entry.Size != 999 {
		t.Errorf("After reopen, size = %d, want 999", entry.Size)
	}
}
