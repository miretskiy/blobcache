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

func TestTrailerEncodeDecode(t *testing.T) {
	tr := Trailer{
		Magic:          uint64(FileMagic),
		EnvelopeOffset: 4096,
		EnvelopeSize:   1024,
		SealMagic:      SealMagic,
		Reserved:       0,
	}

	// Encode
	buf := make([]byte, TrailerSize)
	n, err := tr.Encode(buf)
	require.NoError(t, err)
	require.Equal(t, TrailerSize, n)

	// Decode
	decoded, err := DecodeTrailer(buf)
	require.NoError(t, err)
	require.Equal(t, tr, decoded)
}

func TestTrailerSealed(t *testing.T) {
	sealed := Trailer{SealMagic: SealMagic}
	require.True(t, sealed.IsSealed())

	unsealed := Trailer{SealMagic: 0}
	require.False(t, unsealed.IsSealed())
}

func TestTrailerHasEnvelope(t *testing.T) {
	withEnv := Trailer{EnvelopeOffset: 100, EnvelopeSize: 50}
	require.True(t, withEnv.HasEnvelope())

	noEnv := Trailer{EnvelopeOffset: 0, EnvelopeSize: 0}
	require.False(t, noEnv.HasEnvelope())

	partialEnv := Trailer{EnvelopeOffset: 100, EnvelopeSize: 0}
	require.False(t, partialEnv.HasEnvelope())
}

func TestTrailerValid(t *testing.T) {
	valid := Trailer{Magic: uint64(FileMagic)}
	require.NoError(t, valid.Valid())

	invalid := Trailer{Magic: 0xBADBAD}
	require.ErrorIs(t, invalid.Valid(), ErrInvalidTrailer)
}

func TestFileHeader(t *testing.T) {
	// Verify constant matches expected encoding
	require.Len(t, FileHeaderBytes, FileHeaderSize)

	err := ValidFileHeader(FileHeaderBytes[:])
	require.NoError(t, err)

	// Invalid magic
	badMagic := make([]byte, FileHeaderSize)
	copy(badMagic, FileHeaderBytes[:])
	badMagic[0] = 0xFF
	require.ErrorIs(t, ValidFileHeader(badMagic), ErrInvalidFileMagic)

	// Invalid version
	badVersion := make([]byte, FileHeaderSize)
	copy(badVersion, FileHeaderBytes[:])
	badVersion[4] = 0xFF
	require.ErrorIs(t, ValidFileHeader(badVersion), ErrInvalidVersion)

	// Too small
	require.ErrorIs(t, ValidFileHeader([]byte{1, 2, 3}), ErrBufferTooSmall)
}

func TestAppendTrailer(t *testing.T) {
	tr := Trailer{
		Magic:          uint64(FileMagic),
		EnvelopeOffset: 8192,
		EnvelopeSize:   2048,
		SealMagic:      SealMagic,
	}

	buf := AppendTrailer(nil, tr)
	require.Len(t, buf, TrailerSize)

	decoded, err := DecodeTrailer(buf)
	require.NoError(t, err)
	require.Equal(t, tr, decoded)
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
