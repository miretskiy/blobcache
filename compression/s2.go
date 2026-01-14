package compression

import (
	"github.com/klauspost/compress/s2"
)

func compressS2(dst, src []byte, level Level) ([]byte, error) {
	var res []byte
	if level == CompressionBest {
		res = s2.EncodeBetter(dst, src)
	} else {
		res = s2.Encode(dst, src)
	}

	// S2 uses append logic; if it grows beyond dst capacity, it reallocates.
	// Check capacity to detect reallocation, handling case where dst has length 0.
	if cap(res) > cap(dst) {
		return nil, ErrBufferTooSmall
	}
	return res, nil
}

func decompressS2(dst, src []byte) error {
	res, err := s2.Decode(dst, src)
	if err != nil {
		return err
	}

	// S2 uses append semantics - it will reallocate if dst is too small.
	// Detect this by comparing capacities.
	if cap(res) > cap(dst) {
		return ErrBufferTooSmall
	}
	return nil
}
