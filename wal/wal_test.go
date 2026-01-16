package wal

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/miretskiy/blobcache/internal/record"
	"github.com/stretchr/testify/require"
)

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
	cfg.SyncMode = SyncNone // Faster tests

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
	cfg.SyncMode = SyncNone

	// Write some records
	w, err := Open(cfg)
	require.NoError(t, err)

	records := []record.Record{
		record.NewRecord(1, []byte("key1"), []byte("value1"), 6),
		record.NewRecord(2, []byte("key2"), []byte("value2"), 6),
		record.NewRecord(3, []byte("key3"), []byte("value3"), 6),
	}

	for _, rec := range records {
		err := w.Write(rec)
		require.NoError(t, err)
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
	cfg.SyncMode = SyncNone

	// Write records with SeqID 100 to first file
	w, err := Open(cfg)
	require.NoError(t, err)

	rec := record.NewRecord(100, []byte("key1"), []byte("value1"), 6)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Rotate())

	// Write records with SeqID 200 to second file
	rec = record.NewRecord(200, []byte("key2"), []byte("value2"), 6)
	require.NoError(t, w.Write(rec))
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
	cfg.SyncMode = SyncNone

	w, err := Open(cfg)
	require.NoError(t, err)
	defer w.Close()

	// Write record with SeqID 100 (becomes first WAL file name)
	rec1 := record.NewRecord(100, []byte("key1"), []byte("value1"), 6)
	require.NoError(t, w.Write(rec1))
	require.Equal(t, uint64(100), w.CurrentFirstID())

	// Rotate (closes current file)
	require.NoError(t, w.Rotate())
	require.Equal(t, uint64(0), w.CurrentFirstID()) // Reset until next write

	// Write to new slab with SeqID 200
	rec2 := record.NewRecord(200, []byte("key2"), []byte("value2"), 6)
	require.NoError(t, w.Write(rec2))
	require.Equal(t, uint64(200), w.CurrentFirstID())

	// Rotate again
	require.NoError(t, w.Rotate())

	// Write to new slab with SeqID 300
	rec3 := record.NewRecord(300, []byte("key3"), []byte("value3"), 6)
	require.NoError(t, w.Write(rec3))

	// Check multiple files were created
	files, err := w.listWALFiles()
	require.NoError(t, err)
	require.Equal(t, 3, len(files), "expected 3 WAL files for 3 slabs")
}

func TestWAL_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.SyncMode = SyncNone

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
				err := w.Write(rec)
				require.NoError(t, err)
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
	cfg.SyncMode = SyncNone

	w, err := Open(cfg)
	require.NoError(t, err)

	// Write with SeqID 100 (file named wal-0000000000000100.log)
	rec := record.NewRecord(100, []byte("key"), []byte("value"), 5)
	require.NoError(t, w.Write(rec))

	// Rotate
	require.NoError(t, w.Rotate())

	// Write with SeqID 200 (file named wal-0000000000000200.log)
	rec2 := record.NewRecord(200, []byte("key2"), []byte("value2"), 6)
	require.NoError(t, w.Write(rec2))

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
	cfg.SyncMode = SyncNone

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

	require.NoError(t, w.Write(rec))
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
	cfg.SyncMode = SyncNone

	// Write valid records
	w, err := Open(cfg)
	require.NoError(t, err)

	rec1 := record.NewRecord(1, []byte("key1"), []byte("value1"), 6)
	rec2 := record.NewRecord(2, []byte("key2"), []byte("value2"), 6)
	rec3 := record.NewRecord(3, []byte("key3"), []byte("value3"), 6)

	require.NoError(t, w.Write(rec1))
	require.NoError(t, w.Write(rec2))
	require.NoError(t, w.Write(rec3))
	require.NoError(t, w.Close())

	// Corrupt the middle record by modifying its CRC
	files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	require.Len(t, files, 1)

	data, err := os.ReadFile(files[0])
	require.NoError(t, err)

	// Corrupt somewhere in the middle of the file (after first record)
	// The file structure is: header + rec1 + rec2 + rec3
	// We corrupt a byte in rec2's area
	corruptOffset := FileHeaderSize + rec1.EncodedSize() + 10 // Middle of rec2
	data[corruptOffset] ^= 0xFF                               // Flip bits

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
	cfg.SyncMode = SyncNone

	// Write records
	w, err := Open(cfg)
	require.NoError(t, err)

	rec1 := record.NewRecord(1, []byte("key1"), []byte("value1"), 6)
	rec2 := record.NewRecord(2, []byte("key2"), []byte("value2"), 6)

	require.NoError(t, w.Write(rec1))
	require.NoError(t, w.Write(rec2))
	require.NoError(t, w.Close())

	// Truncate file to simulate crash mid-write
	files, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	require.Len(t, files, 1)

	fi, err := os.Stat(files[0])
	require.NoError(t, err)

	// Truncate to just after the first record (cutting off record 2)
	truncateAt := int64(FileHeaderSize + rec1.EncodedSize() + 5) // Partial record 2
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
	cfg.SyncMode = SyncNone

	// Create WAL and write one record to create file, then close
	w, err := Open(cfg)
	require.NoError(t, err)

	rec := record.NewRecord(1, []byte("key"), []byte("value"), 5)
	require.NoError(t, w.Write(rec))
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
	cfg.SyncMode = SyncNone

	// Write records to two files
	w, err := Open(cfg)
	require.NoError(t, err)

	rec1 := record.NewRecord(100, []byte("key1"), []byte("value1"), 6)
	require.NoError(t, w.Write(rec1))
	require.NoError(t, w.Rotate())

	rec2 := record.NewRecord(200, []byte("key2"), []byte("value2"), 6)
	require.NoError(t, w.Write(rec2))
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
		segments []record.SegmentEnvelope
		want     uint64
	}{
		{
			name:     "empty",
			segments: nil,
			want:     0,
		},
		{
			name: "single segment",
			segments: []record.SegmentEnvelope{
				{MaxSeqID: 100},
			},
			want: 100,
		},
		{
			name: "multiple segments",
			segments: []record.SegmentEnvelope{
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
	cfg.SyncMode = SyncNone

	w, err := Open(cfg)
	require.NoError(t, err)

	// Write records with various SeqIDs
	for _, seq := range []uint64{10, 50, 30, 100, 75} {
		rec := record.NewRecord(seq, []byte("key"), []byte("value"), 5)
		require.NoError(t, w.Write(rec))
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
	cfg.SyncMode = SyncNone

	w, err := Open(cfg)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Write after close should fail
	rec := record.NewRecord(1, []byte("key"), []byte("value"), 5)
	err = w.Write(rec)
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
	cfg.SyncMode = SyncNone

	w, err := Open(cfg)
	require.NoError(t, err)

	// Write and rotate to create multiple files with different SeqIDs
	seqIDs := []uint64{100, 200, 300, 400}
	for i, seqID := range seqIDs {
		rec := record.NewRecord(seqID, []byte("key"), []byte("value"), 5)
		require.NoError(t, w.Write(rec))
		if i < len(seqIDs)-1 {
			require.NoError(t, w.Rotate())
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
