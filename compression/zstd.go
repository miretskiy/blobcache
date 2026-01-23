package compression

import (
	"errors"
	"fmt"
	"sync"

	"github.com/DataDog/zstd"
)

// ctxPool holds reusable zstd.Ctx instances to amortize allocation costs.
// Each Ctx maintains internal compression/decompression state that can be
// reused across operations, avoiding repeated memory allocations.
var ctxPool = sync.Pool{
	New: func() any { return zstd.NewCtx() },
}

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
	ctx := ctxPool.Get().(zstd.Ctx)
	defer ctxPool.Put(ctx)

	// ctx.CompressLevel reuses internal buffers for better performance.
	res, err := ctx.CompressLevel(dst, src, zLevel(level))
	if err != nil {
		return nil, err
	}

	// If the library returned a different slice, it allocated a new one
	// because the provided dst was too small.
	// We check capacity to detect reallocation, handling the case where dst has length 0.
	if cap(res) > cap(dst) {
		return nil, ErrBufferTooSmall
	}
	return res, nil
}

func decompressZstd(dst, src []byte) error {
	// DataDog/zstd's DecompressInto panics when dst is empty.
	// Handle this edge case by using the allocating Decompress instead.
	if len(dst) == 0 {
		result, err := zstd.Decompress(nil, src)
		if err != nil {
			return err
		}
		if len(result) != 0 {
			return errors.New("zstd decompression: expected empty output")
		}
		return nil
	}

	ctx := ctxPool.Get().(zstd.Ctx)
	defer ctxPool.Put(ctx)

	// ctx.DecompressInto reuses internal buffers and requires dst to be
	// pre-sized to hold the decompressed output.
	n, err := ctx.DecompressInto(dst, src)
	if err != nil {
		return err
	}
	if n != len(dst) {
		return fmt.Errorf("zstd decompression: size mismatch (n=%d, expected=%d)", n, len(dst))
	}
	return nil
}
