package blobcache

import (
	"fmt"
	"hash"
	"io"
)

type Hasher func() hash.Hash32

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

// Read implements io.Reader, computing checksum incrementally
// Returns checksum mismatch error on the Read() that hits EOF
func (c *checksumVerifyingReader) Read(p []byte) (n int, err error) {
	// If we already hit a checksum error, keep returning it
	if c.err != nil {
		return 0, c.err
	}

	// Read from underlying reader
	n, err = c.r.Read(p)

	// Update hash with data read
	if n > 0 {
		c.hash.Write(p[:n])
	}

	// Check if we hit EOF
	if err == io.EOF {
		// Verify checksum
		computed := c.hash.Sum32()
		if computed != c.expected {
			c.err = fmt.Errorf("checksum mismatch: expected %d, got %d", c.expected, computed)
			return n, c.err
		}
	}

	return n, err
}
