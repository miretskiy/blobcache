package blobcache

import (
	"bytes"
	"hash/crc32"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChecksumVerifyingReader_ValidChecksum(t *testing.T) {
	data := []byte("hello world")
	expected := crc32.ChecksumIEEE(data)

	reader := newChecksumVerifyingReader(
		bytes.NewReader(data),
		crc32.NewIEEE,
		expected,
	)

	// Read all data
	result, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, data, result)
}

func TestChecksumVerifyingReader_InvalidChecksum(t *testing.T) {
	data := []byte("hello world")
	wrongChecksum := uint32(12345) // Intentionally wrong

	reader := newChecksumVerifyingReader(
		bytes.NewReader(data),
		crc32.NewIEEE,
		wrongChecksum,
	)

	// Read all data - should fail on EOF with checksum error
	result, err := io.ReadAll(reader)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
	// Should still get the data despite checksum error
	require.Equal(t, data, result)
}

func TestChecksumVerifyingReader_SubsequentReadsAfterError(t *testing.T) {
	data := []byte("test")
	wrongChecksum := uint32(99999)

	reader := newChecksumVerifyingReader(
		bytes.NewReader(data),
		crc32.NewIEEE,
		wrongChecksum,
	)

	// Read until we hit the checksum error
	// bytes.Reader may return (n, nil) then (0, EOF), or (n, EOF) in one call
	buf := make([]byte, 100)
	var totalRead int
	var checksumErr error

	for {
		n, err := reader.Read(buf[totalRead:])
		totalRead += n

		if err != nil {
			checksumErr = err
			break
		}
	}

	// Should have checksum mismatch error
	require.Error(t, checksumErr)
	require.Contains(t, checksumErr.Error(), "checksum mismatch")
	require.Equal(t, 4, totalRead) // Should have read all data

	// Subsequent reads should return cached error
	n, err := reader.Read(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
	require.Equal(t, 0, n)
}

func TestChecksumVerifyingReader_OneByteAtATime(t *testing.T) {
	data := []byte("incremental checksum test")
	expected := crc32.ChecksumIEEE(data)

	reader := newChecksumVerifyingReader(
		bytes.NewReader(data),
		crc32.NewIEEE,
		expected,
	)

	// Read one byte at a time
	var result []byte
	buf := make([]byte, 1)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err == io.EOF {
			// Valid checksum should still return io.EOF (not a checksum error)
			break
		}
		if err != nil {
			t.Fatalf("unexpected error (should only get EOF): %v", err)
		}
	}

	require.Equal(t, data, result)
}

func TestChecksumVerifyingReader_OneByteAtATime_InvalidChecksum(t *testing.T) {
	data := []byte("this will fail")
	wrongChecksum := uint32(11111)

	reader := newChecksumVerifyingReader(
		bytes.NewReader(data),
		crc32.NewIEEE,
		wrongChecksum,
	)

	// Read one byte at a time until error
	var result []byte
	buf := make([]byte, 1)
	var finalErr error
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err != nil {
			finalErr = err
			break
		}
	}

	// Should get checksum mismatch on the final read
	require.Error(t, finalErr)
	require.Contains(t, finalErr.Error(), "checksum mismatch")
	require.Equal(t, data, result)

	// Subsequent read should return cached error
	n, err := reader.Read(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
	require.Equal(t, 0, n)
}

func TestChecksumVerifyingReader_PartialReads(t *testing.T) {
	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i % 256)
	}
	expected := crc32.ChecksumIEEE(data)

	reader := newChecksumVerifyingReader(
		bytes.NewReader(data),
		crc32.NewIEEE,
		expected,
	)

	// Read in varying chunk sizes
	var result []byte
	chunkSizes := []int{17, 31, 100, 200, 50, 602} // Odd sizes, sum=1000

	for _, size := range chunkSizes {
		buf := make([]byte, size)
		n, err := reader.Read(buf)
		result = append(result, buf[:n]...)
		if err == io.EOF {
			require.NoError(t, err, "valid checksum should not error")
			break
		}
		require.NoError(t, err)
	}

	require.Equal(t, data, result)
}

func TestChecksumVerifyingReader_EmptyData(t *testing.T) {
	var data []byte
	expected := crc32.ChecksumIEEE(data) // Checksum of empty data

	reader := newChecksumVerifyingReader(
		bytes.NewReader(data),
		crc32.NewIEEE,
		expected,
	)

	// Read should immediately return EOF with valid checksum
	buf := make([]byte, 10)
	n, err := reader.Read(buf)
	require.Equal(t, 0, n)
	require.Equal(t, io.EOF, err) // Not an error - EOF is expected
}

func TestChecksumVerifyingReader_EmptyData_WrongChecksum(t *testing.T) {
	var data []byte
	wrongChecksum := uint32(999)

	reader := newChecksumVerifyingReader(
		bytes.NewReader(data),
		crc32.NewIEEE,
		wrongChecksum,
	)

	// Read should return checksum error on EOF
	buf := make([]byte, 10)
	n, err := reader.Read(buf)
	require.Equal(t, 0, n)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum mismatch")
}

func TestChecksumVerifyingReader_LargeData(t *testing.T) {
	// Test with 10MB data
	data := make([]byte, 10*1024*1024)
	for i := range data {
		data[i] = byte(i)
	}
	expected := crc32.ChecksumIEEE(data)

	reader := newChecksumVerifyingReader(
		bytes.NewReader(data),
		crc32.NewIEEE,
		expected,
	)

	// Read in 64KB chunks
	result, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, len(data), len(result))
	require.Equal(t, data, result)
}
