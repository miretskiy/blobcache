package blobcache

import (
	"errors"
	"hash"
	"io"

	"github.com/miretskiy/blobcache/base"
)

type Hasher func() hash.Hash32

// verifyChecksum computes the checksum of data and compares it to expected.
func verifyChecksum(data []byte, hasher Hasher, expected uint32) error {
	h := hasher()
	_, _ = h.Write(data) // hash.Hash implementations never return an error
	computed := h.Sum32()
	if computed != expected {
		return &base.ChecksumError{
			Expected: expected,
			Got:      computed,
		}
	}
	return nil
}

// checksumVerifyingReader wraps a reader and verifies checksum on final read
type checksumVerifyingReader struct {
	r        io.Reader
	hash     hash.Hash32
	expected uint32
	err      error // Cached error from checksum mismatch
}

// newChecksumVerifyingReader creates a reader that verifies checksum on EOF
func newChecksumVerifyingReader(r io.Reader, hasher Hasher, expected uint32) io.Reader {
	return &checksumVerifyingReader{
		r:        r,
		hash:     hasher(),
		expected: expected,
	}
}

func (c *checksumVerifyingReader) Read(p []byte) (n int, err error) {
	if c.err != nil {
		return 0, c.err
	}

	n, err = c.r.Read(p)

	if n > 0 {
		// Standard hash.Hash implementations (crc32, etc) never return an error on Write.
		_, _ = c.hash.Write(p[:n])
	}

	if errors.Is(err, io.EOF) {
		computed := c.hash.Sum32()
		if computed != c.expected {
			c.err = &base.ChecksumError{
				Expected: c.expected,
				Got:      computed,
			}
			return n, c.err
		}
	}

	return n, err
}
