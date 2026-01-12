package base

import (
	"errors"
	"fmt"
	"io"
	"syscall"
)

// BlobError is the interface for blob-specific errors that can be converted to errno.
type BlobError interface {
	error
	ToErrno() BlobErrno
}

// CompressionError represents a compression/decompression failure.
type CompressionError struct {
	Codec string
	Err   error
}

func (e *CompressionError) Error() string {
	return fmt.Sprintf("compression error (codec=%s): %v", e.Codec, e.Err)
}

func (e *CompressionError) Unwrap() error { return e.Err }

func (e *CompressionError) ToErrno() BlobErrno {
	return ErrDecompression
}

// ChecksumError represents a checksum mismatch.
type ChecksumError struct {
	Expected uint32
	Got      uint32
}

func (e *ChecksumError) Error() string {
	return fmt.Sprintf("checksum mismatch: expected %08x, got %08x", e.Expected, e.Got)
}

func (e *ChecksumError) ToErrno() BlobErrno {
	return ErrChecksumMismatch
}

// TruncatedError represents truncated or incomplete data.
type TruncatedError struct {
	Expected int64
	Got      int64
}

func (e *TruncatedError) Error() string {
	return fmt.Sprintf("truncated data: expected %d bytes, got %d", e.Expected, e.Got)
}

func (e *TruncatedError) ToErrno() BlobErrno {
	return ErrTruncated
}

// InvalidFormatError represents invalid data format.
type InvalidFormatError struct {
	Msg string
}

func (e *InvalidFormatError) Error() string {
	return fmt.Sprintf("invalid format: %s", e.Msg)
}

func (e *InvalidFormatError) ToErrno() BlobErrno {
	return ErrInvalidFormat
}

// ToErrno converts any error to a BlobErrno.
// It recognizes typed errors and falls back to heuristics for standard errors.
func ToErrno(err error) BlobErrno {
	if err == nil {
		return ErrNone
	}

	// Check if it's a BlobError
	var blobErr BlobError
	if errors.As(err, &blobErr) {
		return blobErr.ToErrno()
	}

	// Check for EOF (truncation)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrTruncated
	}

	// Check for syscall errors
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return ErrIORead
	}

	// Default to unknown
	return ErrUnknown
}

// IsCompressionError returns true if err is a CompressionError.
func IsCompressionError(err error) bool {
	var ce *CompressionError
	return errors.As(err, &ce)
}

// IsChecksumError returns true if err is a ChecksumError.
func IsChecksumError(err error) bool {
	var ce *ChecksumError
	return errors.As(err, &ce)
}

// IsTruncatedError returns true if err is a TruncatedError.
func IsTruncatedError(err error) bool {
	var te *TruncatedError
	return errors.As(err, &te)
}

// IsInvalidFormatError returns true if err is an InvalidFormatError.
func IsInvalidFormatError(err error) bool {
	var ife *InvalidFormatError
	return errors.As(err, &ife)
}
