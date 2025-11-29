package index

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/miretskiy/blobcache"
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
	key := blobcache.NewKey([]byte("test-key"), 256)
	now := time.Now().UnixNano()

	err = idx.Put(ctx, key, 1024, now, now)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get it back
	var entry Entry
	err = idx.Get(ctx, key, &entry)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	// Verify
	if entry.ShardID != key.ShardID() {
		t.Errorf("ShardID = %d, want %d", entry.ShardID, key.ShardID())
	}

	if entry.FileID != int64(key.FileID()) {
		t.Errorf("FileID = %d, want %d", entry.FileID, key.FileID())
	}

	if entry.Size != 1024 {
		t.Errorf("Size = %d, want 1024", entry.Size)
	}

	if entry.CTime != now {
		t.Errorf("CTime = %d, want %d", entry.CTime, now)
	}

	if entry.MTime != now {
		t.Errorf("MTime = %d, want %d", entry.MTime, now)
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

	key := blobcache.NewKey([]byte("nonexistent"), 256)
	var entry Entry
	err = idx.Get(context.Background(), key, &entry)

	if err != blobcache.ErrNotFound {
		t.Errorf("Get() error = %v, want ErrNotFound", err)
	}
}

func TestIndex_DuplicateKey(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	idx, _ := New(tmpDir)
	defer idx.Close()

	ctx := context.Background()
	key := blobcache.NewKey([]byte("dup-key"), 256)
	now := time.Now().UnixNano()

	// Insert
	err := idx.Put(ctx, key, 100, now, now)
	if err != nil {
		t.Fatalf("First Put failed: %v", err)
	}

	// Insert same key again - should fail (no ON CONFLICT)
	err = idx.Put(ctx, key, 200, now, now)
	if err == nil {
		t.Error("Second Put should fail with duplicate key, but succeeded")
	}
}

func TestIndex_Delete(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	idx, _ := New(tmpDir)
	defer idx.Close()

	ctx := context.Background()
	key := blobcache.NewKey([]byte("delete-me"), 256)

	// Insert
	now := time.Now().UnixNano()
	idx.Put(ctx, key, 500, now, now)

	// Delete
	err := idx.Delete(ctx, key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify gone
	var entry Entry
	err = idx.Get(ctx, key, &entry)
	if err != blobcache.ErrNotFound {
		t.Errorf("After delete, Get() = %v, want ErrNotFound", err)
	}

	// Delete again should return ErrNotFound
	err = idx.Delete(ctx, key)
	if err != blobcache.ErrNotFound {
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

	// Add entries
	now := time.Now().UnixNano()
	idx.Put(ctx, blobcache.NewKey([]byte("key1"), 256), 100, now, now)
	idx.Put(ctx, blobcache.NewKey([]byte("key2"), 256), 200, now, now)
	idx.Put(ctx, blobcache.NewKey([]byte("key3"), 256), 300, now, now)

	total, err = idx.TotalSizeOnDisk(ctx)
	if err != nil {
		t.Fatalf("TotalSizeOnDisk failed: %v", err)
	}

	if total != 600 {
		t.Errorf("Total = %d, want 600", total)
	}

	// Delete one
	idx.Delete(ctx, blobcache.NewKey([]byte("key2"), 256))

	total, _ = idx.TotalSizeOnDisk(ctx)
	if total != 400 {
		t.Errorf("After delete, total = %d, want 400", total)
	}
}

func TestIndex_Persistence(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "blobcache-test-*")
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	key := blobcache.NewKey([]byte("persist-key"), 256)
	now := time.Now().UnixNano()

	// Create index and insert
	idx1, _ := New(tmpDir)
	idx1.Put(ctx, key, 999, now, now)
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
