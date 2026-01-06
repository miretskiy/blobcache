package compression

import (
	"errors"
)

type Codex uint8
type Level uint8

const (
	CodexNone Codex = iota
	CodexZstd
	CodexLZ4
	CodexS2
)

const (
	CompressionDefault Level = iota
	CompressionSpeed
	CompressionBest
)

// ErrBufferTooSmall is returned when the provided destination buffer
// is insufficient to hold the output. The caller (Engine) uses this
// to trigger the "Early Abort" heuristic.
var ErrBufferTooSmall = errors.New("destination buffer too small")

// IsBufferTooSmall is a helper to detect heuristic/capacity failures.
func IsBufferTooSmall(err error) bool {
	return errors.Is(err, ErrBufferTooSmall)
}

// Compress attempts to compress src into dst using the specified codec and level.
// It follows "into" semantics: if dst is too small, it returns ErrBufferTooSmall.
func Compress(codec Codex, level Level, dst, src []byte) ([]byte, error) {
	switch codec {
	case CodexZstd:
		return compressZstd(dst, src, level)
	case CodexLZ4:
		return compressLZ4(dst, src, level)
	case CodexS2:
		return compressS2(dst, src, level)
	default:
		return nil, errors.New("unsupported codec")
	}
}

// Decompress restores data into the provided dst buffer.
// This is a zero-allocation operation if dst is sized correctly.
func Decompress(codec Codex, dst, src []byte) error {
	switch codec {
	case CodexZstd:
		return decompressZstd(dst, src)
	case CodexLZ4:
		return decompressLZ4(dst, src)
	case CodexS2:
		return decompressS2(dst, src)
	default:
		return nil
	}
}
