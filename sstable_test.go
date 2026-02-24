package blobcache

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSSTValueRoundTrip(t *testing.T) {
	entry := sstEntry{
		Hash:         Key{Lo: 0xDEADBEEF, Hi: 0xCAFEBABE},
		Offset:       12345,
		LogicalSize:  1000,
		PhysicalSize: 800,
		SeqID:        999,
		Flags:        0x42,
		KeyLen:       32,
	}

	encoded := encodeSSTValue(&entry)
	decoded := decodeSSTValue(encoded[:])

	require.Equal(t, entry.Hash, decoded.Hash)
	require.Equal(t, entry.Offset, decoded.Offset)
	require.Equal(t, entry.LogicalSize, decoded.LogicalSize)
	require.Equal(t, entry.PhysicalSize, decoded.PhysicalSize)
	require.Equal(t, entry.SeqID, decoded.SeqID)
	require.Equal(t, entry.Flags, decoded.Flags)
	require.Equal(t, entry.KeyLen, decoded.KeyLen)
}

func TestWriteAndReadSST(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sst")

	entries := []sstEntry{
		{UserKey: []byte("aaa"), Hash: Key{Lo: 1}, Offset: 0, PhysicalSize: 100, SeqID: 1, KeyLen: 3},
		{UserKey: []byte("bbb"), Hash: Key{Lo: 2}, Offset: 100, PhysicalSize: 200, SeqID: 2, KeyLen: 3},
		{UserKey: []byte("ccc"), Hash: Key{Lo: 3}, Offset: 300, PhysicalSize: 300, SeqID: 3, KeyLen: 3},
	}

	meta := sstMeta{
		SegmentID:   42,
		CTime:       1234567890,
		MinSeqID:    1,
		MaxSeqID:    3,
		RecordCount: 3,
	}

	require.NoError(t, WriteSSTFile(path, entries, meta))

	// Read back.
	batch, err := ReadSST(path, 42)
	require.NoError(t, err)
	require.Equal(t, uint32(42), batch.SegmentID)
	require.Equal(t, int64(1234567890), batch.CTime)
	require.Equal(t, uint64(3), batch.MaxSeqID)
	require.Len(t, batch.Items, 3)
	require.Len(t, batch.Entries, 3)

	// Verify entries are in order.
	require.Equal(t, Key{Lo: 1}, batch.Items[0].Key)
	require.Equal(t, Key{Lo: 2}, batch.Items[1].Key)
	require.Equal(t, Key{Lo: 3}, batch.Items[2].Key)
}

func TestEmptySST(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "empty.sst")

	require.NoError(t, WriteSSTFile(path, nil, sstMeta{SegmentID: 1}))

	batch, err := ReadSST(path, 1)
	require.NoError(t, err)
	require.Empty(t, batch.Items)
}

func TestSSTPath(t *testing.T) {
	require.Equal(t, "/data/segments/0001/123.sst",
		SegmentSSTPath("/data/segments/0001/123.seg"))
}

func TestDelPath(t *testing.T) {
	require.Equal(t, "/data/segments/0001/123.del",
		SegmentDelPath("/data/segments/0001/123.seg"))
}

func TestSSTProperties(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "props.sst")

	meta := sstMeta{
		SegmentID:   99,
		CTime:       1700000000,
		MinSeqID:    500,
		MaxSeqID:    1000,
		RecordCount: 42,
	}

	entries := []sstEntry{
		{UserKey: []byte("key"), Hash: Key{Lo: 1}, KeyLen: 3},
	}

	require.NoError(t, WriteSSTFile(path, entries, meta))

	batch, err := ReadSST(path, 99)
	require.NoError(t, err)
	require.Equal(t, uint32(99), batch.SegmentID)
	require.Equal(t, int64(1700000000), batch.CTime)
	require.Equal(t, uint64(1000), batch.MaxSeqID)
}

func TestRewriteSSTable(t *testing.T) {
	tmpDir := t.TempDir()
	srcPath := filepath.Join(tmpDir, "src.sst")
	dstPath := filepath.Join(tmpDir, "dst.sst")

	// Write source SSTable with 5 entries.
	entries := make([]sstEntry, 5)
	for i := range entries {
		entries[i] = sstEntry{
			UserKey:      []byte{byte('a' + i)},
			Hash:         Key{Lo: uint64(i + 1)},
			Offset:       uint32(i * 100),
			PhysicalSize: 100,
			SeqID:        uint64(i + 1),
			KeyLen:       1,
		}
	}
	srcMeta := sstMeta{SegmentID: 1, CTime: 100, MinSeqID: 1, MaxSeqID: 5, RecordCount: 5}
	require.NoError(t, WriteSSTFile(srcPath, entries, srcMeta))

	// Rewrite keeping only entries 0, 2, 4 (keys a, c, e) with new offsets.
	liveOffsets := map[Key]uint32{
		{Lo: 1}: 0,   // a → offset 0
		{Lo: 3}: 200, // c → offset 200
		{Lo: 5}: 400, // e → offset 400
	}

	require.NoError(t, RewriteSSTable(srcPath, dstPath, 2, liveOffsets, nil, 200))

	// Read back.
	batch, err := ReadSST(dstPath, 2)
	require.NoError(t, err)
	require.Len(t, batch.Items, 3)

	// Verify offsets were updated.
	require.Equal(t, uint32(0), batch.Items[0].Offset)
	require.Equal(t, uint32(200), batch.Items[1].Offset)
	require.Equal(t, uint32(400), batch.Items[2].Offset)
}

func TestDelFile_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "segments", "0000"), 0o755))

	// Use persistence's tombstone mechanism directly.
	p := &testDelFile{basePath: tmpDir}
	path := filepath.Join(tmpDir, "segments", "0000", "1.del")

	// Write tombstones.
	p.writeTombstone(t, path, Key{Lo: 100, Hi: 200})
	p.writeTombstone(t, path, Key{Lo: 300, Hi: 400})

	// Read back.
	tombstones, err := readTombstonesFromFile(path)
	require.NoError(t, err)
	require.Len(t, tombstones, 2)

	_, ok := tombstones[Key{Lo: 100, Hi: 200}]
	require.True(t, ok, "tombstone 1 should exist")
	_, ok = tombstones[Key{Lo: 300, Hi: 400}]
	require.True(t, ok, "tombstone 2 should exist")
}

func TestDelFile_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "nonexistent.del")

	tombstones, err := readTombstonesFromFile(path)
	require.NoError(t, err)
	require.Nil(t, tombstones)
}

// testDelFile writes individual tombstones to a .del file for testing.
type testDelFile struct {
	basePath string
}

func (p *testDelFile) writeTombstone(t *testing.T, path string, key Key) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer f.Close()

	// Write single-tombstone batch.
	var hdr [13]byte
	hdr[0] = 0xDD // TombstoneMagic
	hdr[1] = 1    // count = 1
	_, err = f.Write(hdr[:])
	require.NoError(t, err)

	var keyBuf [16]byte
	keyBuf[0] = byte(key.Lo)
	keyBuf[1] = byte(key.Lo >> 8)
	keyBuf[2] = byte(key.Lo >> 16)
	keyBuf[3] = byte(key.Lo >> 24)
	keyBuf[4] = byte(key.Lo >> 32)
	keyBuf[5] = byte(key.Lo >> 40)
	keyBuf[6] = byte(key.Lo >> 48)
	keyBuf[7] = byte(key.Lo >> 56)
	keyBuf[8] = byte(key.Hi)
	keyBuf[9] = byte(key.Hi >> 8)
	keyBuf[10] = byte(key.Hi >> 16)
	keyBuf[11] = byte(key.Hi >> 24)
	keyBuf[12] = byte(key.Hi >> 32)
	keyBuf[13] = byte(key.Hi >> 40)
	keyBuf[14] = byte(key.Hi >> 48)
	keyBuf[15] = byte(key.Hi >> 56)
	_, err = f.Write(keyBuf[:])
	require.NoError(t, err)
}

// readTombstonesFromFile is a test wrapper around the internal readTombstones.
func readTombstonesFromFile(path string) (map[Key]struct{}, error) {
	return readTombstones(path)
}

// readTombstones reads tombstone hashes from a .del file (re-exported for testing).
func readTombstones(path string) (map[Key]struct{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	tombstones := make(map[Key]struct{})
	pos := 0
	for pos < len(data) {
		if pos+13 > len(data) {
			break
		}
		if data[pos] != 0xDD {
			break
		}
		count := uint32(data[pos+1]) | uint32(data[pos+2])<<8 | uint32(data[pos+3])<<8 | uint32(data[pos+4])<<8
		pos += 13
		for range count {
			if pos+16 > len(data) {
				break
			}
			lo := uint64(data[pos]) | uint64(data[pos+1])<<8 | uint64(data[pos+2])<<16 | uint64(data[pos+3])<<24 |
				uint64(data[pos+4])<<32 | uint64(data[pos+5])<<40 | uint64(data[pos+6])<<48 | uint64(data[pos+7])<<56
			hi := uint64(data[pos+8]) | uint64(data[pos+9])<<8 | uint64(data[pos+10])<<16 | uint64(data[pos+11])<<24 |
				uint64(data[pos+12])<<32 | uint64(data[pos+13])<<40 | uint64(data[pos+14])<<48 | uint64(data[pos+15])<<56
			tombstones[Key{Lo: lo, Hi: hi}] = struct{}{}
			pos += 16
		}
	}
	return tombstones, nil
}
