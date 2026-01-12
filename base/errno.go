package base

//go:generate stringer -type=BlobErrno -trimprefix=Err

// BlobErrno represents an error condition for a blob record.
// Stored in bits 34-38 of the BlobRecord Flags field (5-bit, 32 values).
type BlobErrno uint8

const (
	ErrNone BlobErrno = iota
	ErrDecompression
	ErrChecksumMismatch
	ErrIORead
	ErrIOWrite
	ErrTruncated
	ErrInvalidFormat
	ErrUnknown

	maxErrno // Sentinel for max valid errno
)
