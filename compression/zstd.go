package compression

import (
	"errors"
	
	"github.com/DataDog/zstd"
)

func zLevel(l Level) int {
	switch l {
	case CompressionSpeed:
		return zstd.BestSpeed
	case CompressionBest:
		return zstd.BestCompression
	default:
		return zstd.DefaultCompression
	}
}

func compressZstd(dst, src []byte, level Level) ([]byte, error) {
	// zstd.CompressLevel will use dst if it has enough capacity.
	res, err := zstd.CompressLevel(dst, src, zLevel(level))
	if err != nil {
		return nil, err
	}
	
	// If the library returned a different slice, it allocated a new one
	// because the provided dst was too small.
	if len(res) > 0 && &res[0] != &dst[0] {
		return nil, ErrBufferTooSmall
	}
	return res, nil
}

func decompressZstd(dst, src []byte) error {
	// We use Decompress and verify the pointer to ensure the
	// LogicalSize hint from metadata was respected.
	res, err := zstd.Decompress(dst, src)
	if err != nil {
		return err
	}
	if len(res) > 0 && &res[0] != &dst[0] {
		return errors.New("zstd decompression: buffer address mismatch")
	}
	return nil
}
