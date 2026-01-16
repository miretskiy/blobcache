package record

import (
	"bytes"
	"testing"

	"github.com/miretskiy/blobcache/compression"
	"github.com/stretchr/testify/require"
)

func TestHeaderEncodeDecode(t *testing.T) {
	h := Header{
		Magic:        RecordMagic,
		Flags:        0x1234567890ABCDEF,
		SeqID:        42,
		KeyLen:       100,
		PhysicalSize: 1000,
		LogicalSize:  2000,
	}

	// Encode
	buf := make([]byte, HeaderSize)
	n, err := h.Encode(buf)
	require.NoError(t, err)
	require.Equal(t, HeaderSize, n)

	// Decode
	decoded, err := DecodeHeader(buf)
	require.NoError(t, err)
	require.Equal(t, h, decoded)
}

func TestAppendHeader(t *testing.T) {
	h := Header{
		Magic:        RecordMagic,
		Flags:        0xDEADBEEF,
		SeqID:        123,
		KeyLen:       50,
		PhysicalSize: 500,
		LogicalSize:  500,
	}

	buf := AppendHeader(nil, h)
	require.Len(t, buf, HeaderSize)

	decoded, err := DecodeHeader(buf)
	require.NoError(t, err)
	require.Equal(t, h, decoded)
}

func TestHeaderBufferTooSmall(t *testing.T) {
	h := Header{Magic: RecordMagic}

	// Encode with small buffer
	buf := make([]byte, HeaderSize-1)
	_, err := h.Encode(buf)
	require.ErrorIs(t, err, ErrBufferTooSmall)

	// Decode with small buffer
	_, err = DecodeHeader(buf)
	require.ErrorIs(t, err, ErrBufferTooSmall)
}

func TestHeaderMagic(t *testing.T) {
	valid := Header{Magic: RecordMagic}
	require.True(t, valid.IsValid())
	require.False(t, valid.IsHole())

	hole := Header{Magic: HoleMagic}
	require.False(t, hole.IsValid())
	require.True(t, hole.IsHole())
}

func TestHeaderDeleted(t *testing.T) {
	h := Header{}
	require.False(t, h.IsDeleted())

	h.SetDeleted()
	require.True(t, h.IsDeleted())
	require.Equal(t, FlagDeleted, h.Flags&FlagDeleted)
}

func TestHeaderCRC(t *testing.T) {
	h := Header{Flags: FlagInvalidCRC}
	require.False(t, h.HasValidCRC())
	require.Equal(t, uint32(0), h.CRC())

	h.SetCRC(0xDEADBEEF)
	require.True(t, h.HasValidCRC())
	require.Equal(t, uint32(0xDEADBEEF), h.CRC())

	// SetCRC should clear InvalidCRC flag
	require.Zero(t, h.Flags&FlagInvalidCRC)
}

func TestHeaderCompression(t *testing.T) {
	h := Header{}
	require.Equal(t, compression.CodexNone, h.Compression())
	require.False(t, h.IsCompressed())

	h.SetCompression(compression.CodexZstd)
	require.Equal(t, compression.CodexZstd, h.Compression())
	require.True(t, h.IsCompressed())

	h.SetCompression(compression.CodexLZ4)
	require.Equal(t, compression.CodexLZ4, h.Compression())
}

func TestHeaderSizes(t *testing.T) {
	h := Header{
		KeyLen:       100,
		PhysicalSize: 1000,
	}
	require.Equal(t, 1100, h.PayloadSize())
	require.Equal(t, HeaderSize+1100, h.TotalSize())
}

func TestComputeCRC(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")

	crc1 := ComputeCRC(key, value)
	crc2 := ComputeCRC(key, value)
	require.Equal(t, crc1, crc2, "CRC should be deterministic")

	// Different data should produce different CRC
	crc3 := ComputeCRC(key, []byte("different"))
	require.NotEqual(t, crc1, crc3)
}

func TestVerifyCRC(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")
	crc := ComputeCRC(key, value)

	require.NoError(t, VerifyCRC(key, value, crc))
	require.ErrorIs(t, VerifyCRC(key, value, crc+1), ErrCRCMismatch)
}

func TestBlockHeader(t *testing.T) {
	// Verify constant matches expected encoding
	require.Len(t, BlockHeaderBytes, BlockHeaderSize)

	err := ValidateBlockHeader(BlockHeaderBytes[:])
	require.NoError(t, err)

	// Invalid magic
	badMagic := make([]byte, BlockHeaderSize)
	copy(badMagic, BlockHeaderBytes[:])
	badMagic[0] = 0xFF
	require.ErrorIs(t, ValidateBlockHeader(badMagic), ErrInvalidBlockMagic)

	// Invalid version
	badVersion := make([]byte, BlockHeaderSize)
	copy(badVersion, BlockHeaderBytes[:])
	badVersion[4] = 0xFF
	require.ErrorIs(t, ValidateBlockHeader(badVersion), ErrInvalidVersion)

	// Too small
	require.ErrorIs(t, ValidateBlockHeader([]byte{1, 2, 3}), ErrBufferTooSmall)
}

func TestRoundtripRecord(t *testing.T) {
	// Simulate writing and reading a complete record
	key := []byte("my-key")
	value := []byte("my-value-data")

	// Create header
	h := Header{
		Magic:        RecordMagic,
		SeqID:        1000,
		KeyLen:       uint16(len(key)),
		PhysicalSize: int64(len(value)),
		LogicalSize:  int64(len(value)), // uncompressed
	}
	h.SetCRC(ComputeCRC(key, value))

	// Write: header + key + value
	var buf bytes.Buffer
	headerBytes := AppendHeader(nil, h)
	buf.Write(headerBytes)
	buf.Write(key)
	buf.Write(value)

	// Read back
	data := buf.Bytes()

	// Decode header
	decoded, err := DecodeHeader(data)
	require.NoError(t, err)
	require.True(t, decoded.IsValid())
	require.Equal(t, h.SeqID, decoded.SeqID)

	// Extract key and value
	keyStart := HeaderSize
	keyEnd := keyStart + int(decoded.KeyLen)
	valEnd := keyEnd + int(decoded.PhysicalSize)

	readKey := data[keyStart:keyEnd]
	readValue := data[keyEnd:valEnd]

	require.Equal(t, key, readKey)
	require.Equal(t, value, readValue)

	// Verify CRC
	require.NoError(t, VerifyCRC(readKey, readValue, decoded.CRC()))
}

// =============================================================================
// Record Struct Tests
// =============================================================================

func TestNewRecord(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value-data")
	seqID := uint64(12345)

	rec := NewRecord(seqID, key, value, int64(len(value)))

	require.Equal(t, RecordMagic, rec.Magic)
	require.Equal(t, seqID, rec.SeqID)
	require.Equal(t, uint16(len(key)), rec.KeyLen)
	require.Equal(t, int64(len(value)), rec.PhysicalSize)
	require.Equal(t, int64(len(value)), rec.LogicalSize)
	require.True(t, rec.HasValidCRC())
	require.Equal(t, ComputeCRC(key, value), rec.CRC())
	require.Equal(t, key, rec.Key)
	require.Equal(t, value, rec.Value)
}

func TestNewRecord_Compressed(t *testing.T) {
	key := []byte("test-key")
	original := []byte("this is uncompressed data that would normally be larger")
	compressed := []byte("compressed")
	seqID := uint64(999)

	// logicalSize is the original size, value is compressed
	rec := NewRecord(seqID, key, compressed, int64(len(original)))

	require.Equal(t, int64(len(compressed)), rec.PhysicalSize)
	require.Equal(t, int64(len(original)), rec.LogicalSize)
	// CRC is computed over key + compressed value (what's on disk)
	require.Equal(t, ComputeCRC(key, compressed), rec.CRC())
}

func TestRecord_EncodedSize(t *testing.T) {
	rec := Record{
		Header: Header{KeyLen: 10, PhysicalSize: 100},
		Key:    make([]byte, 10),
		Value:  make([]byte, 100),
	}
	require.Equal(t, HeaderSize+10+100, rec.EncodedSize())
}

func TestAppendRecord_DecodeRecord_RoundTrip(t *testing.T) {
	key := []byte("round-trip-key")
	value := []byte("round-trip-value-data-here")
	seqID := uint64(42)

	// Create record
	rec := NewRecord(seqID, key, value, int64(len(value)))

	// Serialize
	buf := AppendRecord(nil, rec)
	require.Equal(t, rec.EncodedSize(), len(buf))

	// Deserialize with CRC verification
	decoded, err := DecodeRecord(buf, true)
	require.NoError(t, err)

	require.Equal(t, rec.Magic, decoded.Magic)
	require.Equal(t, rec.SeqID, decoded.SeqID)
	require.Equal(t, rec.KeyLen, decoded.KeyLen)
	require.Equal(t, rec.PhysicalSize, decoded.PhysicalSize)
	require.Equal(t, rec.LogicalSize, decoded.LogicalSize)
	require.Equal(t, rec.CRC(), decoded.CRC())
	require.Equal(t, key, decoded.Key)
	require.Equal(t, value, decoded.Value)
}

func TestRecord_EncodeTo(t *testing.T) {
	key := []byte("encode-to-key")
	value := []byte("encode-to-value-data")
	seqID := uint64(999)

	rec := NewRecord(seqID, key, value, int64(len(value)))

	// Pre-allocate buffer of exact size
	buf := make([]byte, rec.EncodedSize())
	rec.EncodeTo(buf)

	// Verify it matches AppendRecord output
	appendBuf := AppendRecord(nil, rec)
	require.Equal(t, appendBuf, buf)

	// Verify round-trip
	decoded, err := DecodeRecord(buf, true)
	require.NoError(t, err)
	require.Equal(t, key, decoded.Key)
	require.Equal(t, value, decoded.Value)
}

func TestRecord_EncodeTo_BufferTooSmall(t *testing.T) {
	rec := NewRecord(1, []byte("key"), []byte("value"), 5)

	// Buffer smaller than needed - should panic
	buf := make([]byte, rec.EncodedSize()-1)
	require.Panics(t, func() { rec.EncodeTo(buf) })
}

func TestDecodeRecord_CRCMismatch(t *testing.T) {
	key := []byte("key")
	value := []byte("value")
	rec := NewRecord(1, key, value, int64(len(value)))

	buf := AppendRecord(nil, rec)

	// Corrupt a byte in the value
	buf[len(buf)-1] ^= 0xFF

	// Should fail CRC check
	_, err := DecodeRecord(buf, true)
	require.ErrorIs(t, err, ErrCRCMismatch)

	// Should succeed without CRC check (returns corrupted data)
	decoded, err := DecodeRecord(buf, false)
	require.NoError(t, err)
	require.NotEqual(t, value, decoded.Value)
}

func TestDecodeRecord_InvalidMagic(t *testing.T) {
	buf := make([]byte, HeaderSize+10)
	buf[0] = 0x42 // Invalid magic

	_, err := DecodeRecord(buf, false)
	require.ErrorIs(t, err, ErrInvalidMagic)
}

func TestDecodeRecord_Hole(t *testing.T) {
	buf := make([]byte, HeaderSize+10)
	buf[0] = HoleMagic

	_, err := DecodeRecord(buf, false)
	require.ErrorIs(t, err, ErrHole)
}

func TestDecodeRecord_BufferTooSmall(t *testing.T) {
	// Buffer smaller than header
	_, err := DecodeRecord(make([]byte, HeaderSize-1), false)
	require.ErrorIs(t, err, ErrBufferTooSmall)

	// Buffer smaller than header + payload
	rec := NewRecord(1, []byte("key"), []byte("value"), 5)
	buf := AppendRecord(nil, rec)

	// Truncate buffer
	_, err = DecodeRecord(buf[:len(buf)-3], false)
	require.ErrorIs(t, err, ErrBufferTooSmall)
}

func TestDecodeRecord_NoCRCSet(t *testing.T) {
	// Create record without CRC
	rec := Record{
		Header: Header{
			Magic:        RecordMagic,
			Flags:        FlagInvalidCRC, // CRC not set
			SeqID:        1,
			KeyLen:       3,
			PhysicalSize: 5,
			LogicalSize:  5,
		},
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	buf := AppendRecord(nil, rec)

	// Should succeed even with verifyCRC=true because HasValidCRC() is false
	decoded, err := DecodeRecord(buf, true)
	require.NoError(t, err)
	require.False(t, decoded.HasValidCRC())
}
