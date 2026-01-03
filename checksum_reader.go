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

func (c *checksumVerifyingReader) Read(p []byte) (n int, err error) {
	if c.err != nil {
		return 0, c.err
	}

	n, err = c.r.Read(p)

	if n > 0 {
		// Standard hash.Hash implementations (crc32, etc) never return an error on Write.
		_, _ = c.hash.Write(p[:n])
	}

	if err == io.EOF {
		computed := c.hash.Sum32()
		if computed != c.expected {
			// Suggestion: Use hex (%08x) for checksums. It makes comparing
			// logs with hex-dumped data much easier for humans.
			c.err = fmt.Errorf("checksum mismatch: expected %08x, got %08x", c.expected, computed)
			return n, c.err
		}
	}

	return n, err
}
