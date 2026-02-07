package wal

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/miretskiy/blobcache/internal/sys"
	"github.com/stretchr/testify/require"
)

// writeAndVerify writes a record to the WAL and verifies the WriteResult.
// expectedOffset is the expected absolute file offset of the record.
// Returns the WriteResult for chaining.
func writeAndVerify(t *testing.T, w *WAL, rec record.Record, expectedOffset int64) WriteResult {
	t.Helper()
	result, err := w.Write(rec)
	require.NoError(t, err)
	require.Equal(t, expectedOffset, result.Offset, "offset mismatch for SeqID %d", rec.SeqID)
	require.Equal(t, int64(rec.EncodedSize()), result.BytesWritten, "bytes written mismatch")
	require.GreaterOrEqual(t, result.BytesAligned, result.BytesWritten, "aligned size should be >= written")
	require.Zero(t, result.BytesAligned%4096, "aligned size should be 4KB-aligned")
	return result
}

func TestFileHeader_EncodeDecode(t *testing.T) {
	hdr := FileHeader{
		Magic:     FileMagic,
		Version:   FileVersion,
		Flags:     0,
		CreatedAt: 1234567890,
		Reserved:  0,
	}

	encoded := hdr.Encode()
	require.Len(t, encoded, FileHeaderSize)

	decoded, err := DecodeFileHeader(encoded)
	require.NoError(t, err)
	require.Equal(t, hdr, decoded)
}

func TestFileHeader_InvalidMagic(t *testing.T) {
	buf := make([]byte, FileHeaderSize)
	buf[0] = 0xFF // Invalid magic

	_, err := DecodeFileHeader(buf)
	require.ErrorIs(t, err, ErrInvalidMagic)
}

func TestFileHeader_TooSmall(t *testing.T) {
	_, err := DecodeFileHeader(make([]byte, 10))
	require.ErrorIs(t, err, ErrHeaderTooSmall)
}

func TestWAL_OpenClose(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO // Faster tests

	w, err := Open(cfg)
	require.NoError(t, err)
	require.NotNil(t, w)

	err = w.Close()
	require.NoError(t, err)

	// Double close should be safe
	err = w.Close()
	require.NoError(t, err)
}

// testReplayer implements Replayer for testing
type testReplayer struct {
	records []record.Record
}

func (r *testReplayer) ReplayRecord(rec record.Record) error {
	r.records = append(r.records, rec)
	return nil
}

func (r *testReplayer) Flush() {}
func (r *testReplayer) Drain() {}

func TestWAL_WriteAndRecover(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	// Write some records
	w, err := Open(cfg)
	require.NoError(t, err)

	records := []record.Record{
		record.NewRecord(1, []byte("key1"), []byte("value1"), 6),
		record.NewRecord(2, []byte("key2"), []byte("value2"), 6),
		record.NewRecord(3, []byte("key3"), []byte("value3"), 6),
	}

	// First record starts after file header; sequential writes each become their own batch
	expectedOffset := sys.PageAlign(int64(FileHeaderSize))
	for i, rec := range records {
		result := writeAndVerify(t, w, rec, expectedOffset)
		if i == 0 {
			// First batch includes header, next starts at BytesAligned
			expectedOffset = result.BytesAligned
		} else {
			// Subsequent batches: next offset is current + BytesAligned
			expectedOffset += result.BytesAligned
		}
	}

	require.NoError(t, w.Close())

	// Reopen and recover
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	// Recover (replay all - no files are committed)
	replayer := &testReplayer{}
	recovered, err := w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.True(t, recovered)

	// Verify recovered records
	require.Len(t, replayer.records, 3)
	for i, rec := range replayer.records {
		require.Equal(t, records[i].SeqID, rec.SeqID)
		require.Equal(t, records[i].Key, rec.Key)
		require.Equal(t, records[i].Value, rec.Value)
	}
}

func TestWAL_RecoverSkipsCommittedFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	// Write records with SeqID 100 to first file
	w, err := Open(cfg)
	require.NoError(t, err)

	rec := record.NewRecord(100, []byte("key1"), []byte("value1"), 6)
	writeAndVerify(t, w, rec, sys.PageAlign(int64(FileHeaderSize)))
	_, err = w.EnqueueRotation()
	require.NoError(t, err)

	// Write records with SeqID 200 to second file (new file, so offset resets to FileHeaderSize)
	rec = record.NewRecord(200, []byte("key2"), []byte("value2"), 6)
	writeAndVerify(t, w, rec, sys.PageAlign(int64(FileHeaderSize)))
	require.NoError(t, w.Close())

	// Reopen and recover - mark first file as committed
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	isCommitted := func(firstSeqID uint64) bool {
		return firstSeqID == 100 // First file is committed
	}
	recovered, err := w2.Recover(replayer, isCommitted)
	require.NoError(t, err)
	require.True(t, recovered)

	// Only second file should be replayed
	require.Len(t, replayer.records, 1)
	require.Equal(t, uint64(200), replayer.records[0].SeqID)
}

func TestWAL_Rotate(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)
	defer w.Close()

	// Write record with SeqID 100 (becomes first WAL file name)
	rec1 := record.NewRecord(100, []byte("key1"), []byte("value1"), 6)
	writeAndVerify(t, w, rec1, sys.PageAlign(int64(FileHeaderSize)))
	require.Equal(t, uint64(100), w.CurrentFirstID())

	// Rotate (closes current file)
	_, err = w.EnqueueRotation()
	require.NoError(t, err)
	require.Equal(t, uint64(0), w.CurrentFirstID()) // Reset until next write

	// Write to new slab with SeqID 200 (new file, offset resets)
	rec2 := record.NewRecord(200, []byte("key2"), []byte("value2"), 6)
	writeAndVerify(t, w, rec2, sys.PageAlign(int64(FileHeaderSize)))
	require.Equal(t, uint64(200), w.CurrentFirstID())

	// Rotate again
	_, err = w.EnqueueRotation()
	require.NoError(t, err)

	// Write to new slab with SeqID 300 (new file, offset resets)
	rec3 := record.NewRecord(300, []byte("key3"), []byte("value3"), 6)
	writeAndVerify(t, w, rec3, sys.PageAlign(int64(FileHeaderSize)))

	// Check multiple files were created
	files, err := w.listWALFiles()
	require.NoError(t, err)
	require.Equal(t, 3, len(files), "expected 3 WAL files for 3 slabs")
}

func TestWAL_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)
	defer w.Close()

	const numWriters = 10
	const writesPerWriter = 100

	var wg sync.WaitGroup
	var seqCounter atomic.Uint64

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < writesPerWriter; j++ {
				seq := seqCounter.Add(1)
				rec := record.NewRecord(seq, []byte("key"), []byte("value"), 5)
				result, err := w.Write(rec)
				require.NoError(t, err)
				// Can't verify exact offset in concurrent test, but verify invariants
				require.Greater(t, result.Offset, int64(0))
				require.Greater(t, result.BytesWritten, int64(0))
				require.Zero(t, result.BytesAligned%4096)
			}
		}(i)
	}

	wg.Wait()

	// Verify metrics
	require.Equal(t, int64(numWriters*writesPerWriter), w.WrittenRecs.Load())

	// Group commits should be less than total writes (batching occurred)
	require.Less(t, w.GroupCommits.Load(), int64(numWriters*writesPerWriter))
}

func TestWAL_DeleteFile(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)

	// Write with SeqID 100 (file named wal-0000000000000100.log)
	rec := record.NewRecord(100, []byte("key"), []byte("value"), 5)
	writeAndVerify(t, w, rec, sys.PageAlign(int64(FileHeaderSize)))

	// Rotate
	_, err = w.EnqueueRotation()
	require.NoError(t, err)

	// Write with SeqID 200 (file named wal-0000000000000200.log)
	rec2 := record.NewRecord(200, []byte("key2"), []byte("value2"), 6)
	writeAndVerify(t, w, rec2, sys.PageAlign(int64(FileHeaderSize)))

	require.NoError(t, w.Close())

	// Verify 2 files exist
	files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	require.Len(t, files, 2)

	// Reopen and delete first WAL file (simulating flush completion)
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	require.NoError(t, w2.DeleteFile(100)) // Delete by firstSeqID

	// Verify only 1 file remains
	files, _ = filepath.Glob(filepath.Join(dir, "wal-*.log"))
	require.Len(t, files, 1)

	// Deleting non-existent file should not error
	require.NoError(t, w2.DeleteFile(999))
}

func TestWAL_DeleteRecord(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)

	// Create a tombstone record (FlagDeleted set)
	rec := record.Record{
		Header: record.Header{
			Magic:        record.RecordMagic,
			Flags:        record.FlagDeleted,
			SeqID:        1,
			KeyLen:       3,
			PhysicalSize: 0,
			LogicalSize:  0,
		},
		Key:   []byte("key"),
		Value: nil,
	}
	rec.SetCRC(record.ComputeCRC(rec.Key, nil))

	writeAndVerify(t, w, rec, sys.PageAlign(int64(FileHeaderSize)))
	require.NoError(t, w.Close())

	// Recover and verify tombstone
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	_, err = w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.Len(t, replayer.records, 1)
	require.True(t, replayer.records[0].IsDeleted())
	require.Equal(t, []byte("key"), replayer.records[0].Key)
}

func TestWAL_RecoverCorruptedRecord(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	// Write valid records
	w, err := Open(cfg)
	require.NoError(t, err)

	rec1 := record.NewRecord(1, []byte("key1"), []byte("value1"), 6)
	rec2 := record.NewRecord(2, []byte("key2"), []byte("value2"), 6)
	rec3 := record.NewRecord(3, []byte("key3"), []byte("value3"), 6)

	// Sequential writes: each becomes its own batch
	r1 := writeAndVerify(t, w, rec1, sys.PageAlign(int64(FileHeaderSize)))
	r2 := writeAndVerify(t, w, rec2, r1.BytesAligned)
	writeAndVerify(t, w, rec3, r1.BytesAligned+r2.BytesAligned)
	require.NoError(t, w.Close())

	// Corrupt the middle record by modifying its CRC
	files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	require.Len(t, files, 1)

	data, err := os.ReadFile(files[0])
	require.NoError(t, err)

	// Corrupt somewhere in rec2's data area (10 bytes into rec2)
	corruptOffset := int(r2.Offset) + 10
	data[corruptOffset] ^= 0xFF // Flip bits

	require.NoError(t, os.WriteFile(files[0], data, 0644))

	// Recovery should still work, potentially skipping the corrupted record
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	_, err = w2.Recover(replayer, nil)
	require.NoError(t, err)

	// At least record 1 should be recovered (before corruption)
	require.GreaterOrEqual(t, len(replayer.records), 1)
	require.Equal(t, uint64(1), replayer.records[0].SeqID)
}

func TestWAL_RecoverTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	// Write records
	w, err := Open(cfg)
	require.NoError(t, err)

	rec1 := record.NewRecord(1, []byte("key1"), []byte("value1"), 6)
	rec2 := record.NewRecord(2, []byte("key2"), []byte("value2"), 6)

	// Sequential writes: each becomes its own batch
	r1 := writeAndVerify(t, w, rec1, sys.PageAlign(int64(FileHeaderSize)))
	writeAndVerify(t, w, rec2, r1.BytesAligned)
	require.NoError(t, w.Close())

	// Truncate file to simulate crash mid-write
	files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	require.Len(t, files, 1)

	fi, err := os.Stat(files[0])
	require.NoError(t, err)

	// Truncate to 5 bytes into the second write batch (partial record 2)
	truncateAt := r1.BytesAligned + 5
	require.Less(t, truncateAt, fi.Size())
	require.NoError(t, os.Truncate(files[0], truncateAt))

	// Recovery should succeed with first record
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	_, err = w2.Recover(replayer, nil)
	require.NoError(t, err)

	// First record should be recovered
	require.Len(t, replayer.records, 1)
	require.Equal(t, uint64(1), replayer.records[0].SeqID)
}

func TestWAL_RecoverEmptyFile(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	// Create WAL and write one record to create file, then close
	w, err := Open(cfg)
	require.NoError(t, err)

	rec := record.NewRecord(1, []byte("key"), []byte("value"), 5)
	writeAndVerify(t, w, rec, sys.PageAlign(int64(FileHeaderSize)))
	require.NoError(t, w.Close())

	// Truncate to just header (no records)
	files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	require.Len(t, files, 1)
	require.NoError(t, os.Truncate(files[0], FileHeaderSize))

	// Recovery should succeed with no records
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	_, err = w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.Empty(t, replayer.records)
}

func TestWAL_RecoverMultipleFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	// Write records to two files
	w, err := Open(cfg)
	require.NoError(t, err)

	rec1 := record.NewRecord(100, []byte("key1"), []byte("value1"), 6)
	writeAndVerify(t, w, rec1, sys.PageAlign(int64(FileHeaderSize)))
	_, err = w.EnqueueRotation()
	require.NoError(t, err)

	rec2 := record.NewRecord(200, []byte("key2"), []byte("value2"), 6)
	writeAndVerify(t, w, rec2, sys.PageAlign(int64(FileHeaderSize))) // New file, offset resets
	require.NoError(t, w.Close())

	// Verify 2 files exist
	files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	require.Len(t, files, 2)

	// Reopen and recover
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	recovered, err := w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.True(t, recovered)

	// Both records should be recovered
	require.Len(t, replayer.records, 2)
	require.Equal(t, uint64(100), replayer.records[0].SeqID)
	require.Equal(t, uint64(200), replayer.records[1].SeqID)

	// Note: WAL file deletion is handled by the flush path (memtable layer),
	// not by WAL.Recover itself. See integration tests for full recovery flow.
}

func TestComputeRecoveryCheckpoint(t *testing.T) {
	tests := []struct {
		name     string
		segments []record.SegmentFooter
		want     uint64
	}{
		{
			name:     "empty",
			segments: nil,
			want:     0,
		},
		{
			name: "single segment",
			segments: []record.SegmentFooter{
				{MaxSeqID: 100},
			},
			want: 100,
		},
		{
			name: "multiple segments",
			segments: []record.SegmentFooter{
				{MaxSeqID: 100},
				{MaxSeqID: 500},
				{MaxSeqID: 250},
			},
			want: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeRecoveryCheckpoint(tt.segments)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestScanMaxSeqID(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)

	// Write records with various SeqIDs - sequential writes, each is its own batch
	expectedOffset := sys.PageAlign(int64(FileHeaderSize))
	seqIDs := []uint64{10, 50, 30, 100, 75}
	for i, seq := range seqIDs {
		rec := record.NewRecord(seq, []byte("key"), []byte("value"), 5)
		result := writeAndVerify(t, w, rec, expectedOffset)
		if i == 0 {
			expectedOffset = result.BytesAligned
		} else {
			expectedOffset += result.BytesAligned
		}
	}
	require.NoError(t, w.Close())

	// Scan the WAL file
	files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	require.Len(t, files, 1)

	maxSeq, err := ScanMaxSeqID(files[0])
	require.NoError(t, err)
	require.Equal(t, uint64(100), maxSeq)
}

func TestWAL_WriteAfterClose(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Write after close should fail
	rec := record.NewRecord(1, []byte("key"), []byte("value"), 5)
	_, err = w.Write(rec)
	require.Error(t, err)
	require.Contains(t, err.Error(), "closed")
}

func TestIsWALFile(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"wal-00000000000000000001.log", true},   // 20 digits
		{"wal-00000000000012345678.log", true},   // 20 digits
		{"wal-00000000000000000000.log", true},   // 20 digits (zero)
		{"wal-1.log", false},                     // Too short
		{"wal-00000001.log", false},              // Old format (8 digits)
		{"wal-0000000000000001.log", false},      // Old format (16 digits)
		{"wal-000000000000000000001.log", false}, // Too long (21 digits)
		{"log-00000000000000000001.log", false},  // Wrong prefix
		{"wal-00000000000000000001.txt", false},  // Wrong extension
		{"WAL-00000000000000000001.log", false},  // Case sensitive
		{"wal-0000000000000000000a.log", true},   // Hex chars (passes pattern, fails parse)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isWALFile(tt.name)
			require.Equal(t, tt.want, got, "isWALFile(%q)", tt.name)
		})
	}
}

func TestParseWALFileName(t *testing.T) {
	seq, err := parseWALFileName("wal-00000000000000000001.log")
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)

	seq, err = parseWALFileName("wal-00000000000012345678.log")
	require.NoError(t, err)
	require.Equal(t, uint64(12345678), seq)

	_, err = parseWALFileName("invalid.log")
	require.Error(t, err)
}

func TestWAL_ListWALFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)

	// Write and rotate to create multiple files with different SeqIDs
	seqIDs := []uint64{100, 200, 300, 400}
	for i, seqID := range seqIDs {
		rec := record.NewRecord(seqID, []byte("key"), []byte("value"), 5)
		writeAndVerify(t, w, rec, sys.PageAlign(int64(FileHeaderSize))) // Each file starts fresh
		if i < len(seqIDs)-1 {
			_, err = w.EnqueueRotation()
			require.NoError(t, err)
		}
	}
	require.NoError(t, w.Close())

	// List WAL files
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	firstIDs, err := w2.ListWALFiles()
	require.NoError(t, err)
	require.Equal(t, seqIDs, firstIDs)
}

// TestWAL_BatchSplitting verifies that large batches are correctly split into
// multiple chunks when they exceed the staging buffer capacity.
func TestWAL_BatchSplitting(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO
	cfg.MaxBatchSize = 8192 // Small buffer (8KB) to force splitting

	w, err := Open(cfg)
	require.NoError(t, err)

	// Create many records that will exceed the 8KB buffer
	// Each record: 35 byte header + key + value
	// With 100-byte values, each record is ~140 bytes
	// 100 records = ~14KB, should require multiple chunks
	numRecords := 100
	records := make([]record.Record, numRecords)
	for i := 0; i < numRecords; i++ {
		key := []byte("key")
		value := make([]byte, 100)
		for j := range value {
			value[j] = byte(i)
		}
		records[i] = record.NewRecord(uint64(i+1), key, value, 6)
	}

	// Write all records (batch splitting doesn't allow precise offset tracking)
	offset := sys.PageAlign(int64(FileHeaderSize))
	for _, rec := range records {
		result, err := w.Write(rec)
		require.NoError(t, err)
		require.Greater(t, result.Offset, int64(0))
		require.Zero(t, result.BytesAligned%4096)
		offset += result.BytesWritten
	}
	require.NoError(t, w.Close())

	// Recover and verify all records
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	recovered, err := w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.True(t, recovered)
	require.Len(t, replayer.records, numRecords)

	// Verify each record's content
	for i, rec := range replayer.records {
		require.Equal(t, records[i].SeqID, rec.SeqID, "SeqID mismatch at %d", i)
		require.Equal(t, records[i].Key, rec.Key, "Key mismatch at %d", i)
		require.Equal(t, records[i].Value, rec.Value, "Value mismatch at %d", i)
	}
}

// TestWAL_OversizedRecord verifies that a single record larger than the
// staging buffer is handled via the slow path (temporary allocation).
func TestWAL_OversizedRecord(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO
	cfg.MaxBatchSize = 4096 // 4KB buffer

	w, err := Open(cfg)
	require.NoError(t, err)

	// Create a record larger than the buffer (8KB value)
	largeValue := make([]byte, 8192)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	largeRec := record.NewRecord(1, []byte("bigkey"), largeValue, 6)

	// Write should succeed via slow path
	writeAndVerify(t, w, largeRec, sys.PageAlign(int64(FileHeaderSize)))
	require.NoError(t, w.Close())

	// Recover and verify
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	recovered, err := w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.True(t, recovered)
	require.Len(t, replayer.records, 1)
	require.Equal(t, largeRec.SeqID, replayer.records[0].SeqID)
	require.Equal(t, largeRec.Key, replayer.records[0].Key)
	require.Equal(t, largeRec.Value, replayer.records[0].Value)
}

// TestWAL_MixedRecordSizes verifies that a mix of small and oversized records
// in the same batch are all correctly written and recovered.
func TestWAL_MixedRecordSizes(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO
	cfg.MaxBatchSize = 4096 // 4KB buffer

	w, err := Open(cfg)
	require.NoError(t, err)

	// Create large values with identifiable patterns BEFORE creating records
	// (so CRC is computed correctly)
	largeValue1 := make([]byte, 8192)
	for i := range largeValue1 {
		largeValue1[i] = byte(0xAA)
	}
	largeValue2 := make([]byte, 6000)
	for i := range largeValue2 {
		largeValue2[i] = byte(0xBB)
	}

	// Mix of small and large records
	records := []record.Record{
		record.NewRecord(1, []byte("small1"), []byte("value1"), 6),
		record.NewRecord(2, []byte("bigkey"), largeValue1, 6), // Oversized
		record.NewRecord(3, []byte("small2"), []byte("value2"), 6),
		record.NewRecord(4, []byte("bigkey2"), largeValue2, 6), // Oversized
		record.NewRecord(5, []byte("small3"), []byte("value3"), 6),
	}

	// Write all records (mixed sizes don't allow precise offset tracking)
	for _, rec := range records {
		result, err := w.Write(rec)
		require.NoError(t, err)
		require.Greater(t, result.Offset, int64(0))
		require.Zero(t, result.BytesAligned%4096)
	}
	require.NoError(t, w.Close())

	// Recover and verify
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	recovered, err := w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.True(t, recovered)
	require.Len(t, replayer.records, len(records))

	for i, rec := range replayer.records {
		require.Equal(t, records[i].SeqID, rec.SeqID, "SeqID mismatch at %d", i)
		require.Equal(t, records[i].Key, rec.Key, "Key mismatch at %d", i)
		require.Equal(t, records[i].Value, rec.Value, "Value mismatch at %d", i)
	}
}

// TestWAL_BatchSplittingWithRotation verifies batch splitting works correctly
// when rotation commands are interspersed with data.
func TestWAL_BatchSplittingWithRotation(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO
	cfg.MaxBatchSize = 4096 // Small buffer

	w, err := Open(cfg)
	require.NoError(t, err)

	// Write records, rotate, write more
	for i := 1; i <= 50; i++ {
		value := make([]byte, 50)
		for j := range value {
			value[j] = byte(i)
		}
		rec := record.NewRecord(uint64(i), []byte("key"), value, 6)
		_, err := w.Write(rec)
		require.NoError(t, err)

		// Rotate after every 20 records
		if i%20 == 0 && i < 50 {
			_, err = w.EnqueueRotation()
			require.NoError(t, err)
		}
	}
	require.NoError(t, w.Close())

	// Count WAL files
	files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	require.Len(t, files, 3) // Should have 3 files (20, 20, 10 records)

	// Recover and verify all 50 records
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	recovered, err := w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.True(t, recovered)
	require.Len(t, replayer.records, 50)

	for i, rec := range replayer.records {
		require.Equal(t, uint64(i+1), rec.SeqID)
	}
}

// TestWAL_Guard_PreventsTimeTravel verifies that the sealed guard prevents
// writes with sequence IDs that belong to a previous (rotated) file.
// This protects against "Stale Leader" bugs where an old MemTable tries
// to write to a newer WAL file.
func TestWAL_Guard_PreventsTimeTravel(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)

	// 1. Write SeqID 100
	rec100 := record.NewRecord(100, []byte("k"), []byte("v"), 0)
	writeAndVerify(t, w, rec100, sys.PageAlign(int64(FileHeaderSize)))

	// 2. Rotate. This sets w.lastRotatedSeq = 100
	_, err = w.EnqueueRotation()
	require.NoError(t, err)

	// 3. Write SeqID 200 (Valid: > 100) - new file, offset resets
	rec200 := record.NewRecord(200, []byte("k"), []byte("v"), 0)
	writeAndVerify(t, w, rec200, sys.PageAlign(int64(FileHeaderSize)))

	// 4. ATTACK: Attempt to write SeqID 99 (Time Travel)
	// This should be rejected because 99 <= lastRotatedSeq (100)
	recOld := record.NewRecord(99, []byte("k"), []byte("v"), 0)
	_, err = w.Write(recOld) // Error expected, result not meaningful

	require.Error(t, err)
	require.ErrorIs(t, err, ErrSequenceRegression)

	require.NoError(t, w.Close())
}

// TestWAL_Write_LargeAlignedValue verifies that Write handles large values
// that exceed the staging buffer, using the slow path.
func TestWAL_Write_LargeAlignedValue(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)

	// Allocate aligned buffer (must be page-sized)
	valueSize := 4096 * 2 // 8KB - two pages
	value := sys.AllocAligned(valueSize)
	defer sys.FreeAligned(value)

	// Fill with pattern
	for i := range value {
		value[i] = byte(i % 256)
	}

	rec := record.NewRecord(1, []byte("big-key"), value, 6)
	result := writeAndVerify(t, w, rec, sys.PageAlign(int64(FileHeaderSize)))

	// Write another normal record - next offset is at BytesAligned (not BytesWritten)
	rec2 := record.NewRecord(2, []byte("small-key"), []byte("small-value"), 6)
	writeAndVerify(t, w, rec2, result.BytesAligned)

	require.NoError(t, w.Close())

	// Recover and verify
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	recovered, err := w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.True(t, recovered)
	require.Len(t, replayer.records, 2)

	// Verify the large record
	require.Equal(t, uint64(1), replayer.records[0].SeqID)
	require.Equal(t, []byte("big-key"), replayer.records[0].Key)
	require.Equal(t, valueSize, len(replayer.records[0].Value))
	for i, b := range replayer.records[0].Value {
		require.Equal(t, byte(i%256), b, "Value mismatch at offset %d", i)
	}

	// Verify the small record
	require.Equal(t, uint64(2), replayer.records[1].SeqID)
	require.Equal(t, []byte("small-key"), replayer.records[1].Key)
}

// TestWAL_Write_1MB_Record verifies recovery of large records (1MB+).
func TestWAL_Write_1MB_Record(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)

	// Allocate a 1MB aligned buffer (256 pages)
	valueSize := 1 << 20 // 1MB
	value := sys.AllocAligned(valueSize)
	defer sys.FreeAligned(value)

	// Fill with deterministic pattern for verification
	for i := range value {
		value[i] = byte((i * 7) % 256)
	}

	rec := record.NewRecord(1, []byte("megabyte-key"), value, 6)
	writeAndVerify(t, w, rec, sys.PageAlign(int64(FileHeaderSize)))

	require.NoError(t, w.Close())

	// Recover and verify the large record
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	recovered, err := w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.True(t, recovered)
	require.Len(t, replayer.records, 1)

	// Verify full content
	require.Equal(t, uint64(1), replayer.records[0].SeqID)
	require.Equal(t, []byte("megabyte-key"), replayer.records[0].Key)
	require.Equal(t, valueSize, len(replayer.records[0].Value))

	// Spot-check several positions in the recovered value
	for _, i := range []int{0, 1000, 100000, 500000, valueSize - 1} {
		expected := byte((i * 7) % 256)
		require.Equal(t, expected, replayer.records[0].Value[i],
			"Value mismatch at offset %d: expected %d, got %d", i, expected, replayer.records[0].Value[i])
	}
}

// TestWAL_RecordBlockAlignment verifies that every record's Offset is block-aligned
// (a multiple of 4096), which is the prerequisite for XFS reflinks during compaction.
func TestWAL_RecordBlockAlignment(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)

	// Write records of various sizes — all offsets must be block-aligned
	sizes := []int{1, 42, 100, 4096, 4097, 8000, 100000}
	for i, size := range sizes {
		value := make([]byte, size)
		rec := record.NewRecord(uint64(i+1), []byte("key"), value, 3)
		result, err := w.Write(rec)
		require.NoError(t, err)
		require.Zero(t, result.Offset%sys.BlockSize,
			"record %d (size=%d) has unaligned offset %d", i, size, result.Offset)
	}

	require.NoError(t, w.Close())

	// Verify recovery finds all records
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	recovered, err := w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.True(t, recovered)
	require.Len(t, replayer.records, len(sizes))
}

// TestWAL_Write_MixedSizes verifies that Write handles records of mixed sizes
// correctly, with some exceeding the staging buffer (slow path) and some not.
func TestWAL_Write_MixedSizes(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.Flags = sys.FlDirectIO

	w, err := Open(cfg)
	require.NoError(t, err)

	// Write pattern: small, large, small, large, small
	// Sequential writes: each becomes its own batch; track using BytesAligned

	// Normal write 1
	rec1 := record.NewRecord(1, []byte("k1"), []byte("normal1"), 6)
	r1 := writeAndVerify(t, w, rec1, sys.PageAlign(int64(FileHeaderSize)))
	nextOffset := r1.BytesAligned // First batch includes header

	// Direct write 1
	val2 := sys.AllocAligned(4096)
	defer sys.FreeAligned(val2)
	for i := range val2 {
		val2[i] = 0xAA
	}
	rec2 := record.NewRecord(2, []byte("k2"), val2, 6)
	r2 := writeAndVerify(t, w, rec2, nextOffset)
	nextOffset += r2.BytesAligned

	// Normal write 2
	rec3 := record.NewRecord(3, []byte("k3"), []byte("normal2"), 6)
	r3 := writeAndVerify(t, w, rec3, nextOffset)
	nextOffset += r3.BytesAligned

	// Direct write 2
	val4 := sys.AllocAligned(8192)
	defer sys.FreeAligned(val4)
	for i := range val4 {
		val4[i] = 0xBB
	}
	rec4 := record.NewRecord(4, []byte("k4"), val4, 6)
	r4 := writeAndVerify(t, w, rec4, nextOffset)
	nextOffset += r4.BytesAligned

	// Normal write 3
	rec5 := record.NewRecord(5, []byte("k5"), []byte("normal3"), 6)
	writeAndVerify(t, w, rec5, nextOffset)

	require.NoError(t, w.Close())

	// Recover and verify all 5 records
	w2, err := Open(cfg)
	require.NoError(t, err)
	defer w2.Close()

	replayer := &testReplayer{}
	recovered, err := w2.Recover(replayer, nil)
	require.NoError(t, err)
	require.True(t, recovered)
	require.Len(t, replayer.records, 5)

	// Verify records in order
	require.Equal(t, uint64(1), replayer.records[0].SeqID)
	require.Equal(t, []byte("normal1"), replayer.records[0].Value)

	require.Equal(t, uint64(2), replayer.records[1].SeqID)
	require.Equal(t, 4096, len(replayer.records[1].Value))
	require.Equal(t, byte(0xAA), replayer.records[1].Value[0])

	require.Equal(t, uint64(3), replayer.records[2].SeqID)
	require.Equal(t, []byte("normal2"), replayer.records[2].Value)

	require.Equal(t, uint64(4), replayer.records[3].SeqID)
	require.Equal(t, 8192, len(replayer.records[3].Value))
	require.Equal(t, byte(0xBB), replayer.records[3].Value[0])

	require.Equal(t, uint64(5), replayer.records[4].SeqID)
	require.Equal(t, []byte("normal3"), replayer.records[4].Value)
}
