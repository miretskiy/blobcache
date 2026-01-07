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
	if len(res) > 0 && &res[0] != &dst[0] {
		return nil, ErrBufferTooSmall
	}
	return res, nil
}

func decompressS2(dst, src []byte) error {
	_, err := s2.Decode(dst, src)
	return err
}
