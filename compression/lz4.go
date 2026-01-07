package compression

import (
	"github.com/pierrec/lz4/v4"
)

// lzLevel maps our normalized levels to LZ4 specific levels.
// LZ4 levels 0-2 use the fast algorithm, while 3-12 use High Compression (HC).
func lzLevel(l Level) lz4.CompressionLevel {
	switch l {
	case CompressionSpeed:
		return lz4.Fast // Standard fast compression
	case CompressionBest:
		return lz4.Level9 // High compression (HC) - good balance for 'Best'
	default:
		return lz4.Level5
	}
}

func compressLZ4(dst, src []byte, level Level) ([]byte, error) {
	var c lz4.CompressorHC
	if level == CompressionBest {
		c.Level = lzLevel(level)
	}

	// CompressBlock naturally errors or returns 0 if dst is too small.
	n, err := c.CompressBlock(src, dst)
	if err != nil || n == 0 {
		return nil, ErrBufferTooSmall
	}
	return dst[:n], nil
}

func decompressLZ4(dst, src []byte) error {
	// UncompressBlock is inherently an "Into" operation.
	_, err := lz4.UncompressBlock(src, dst)
	return err
}
