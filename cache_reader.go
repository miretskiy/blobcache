package blobcache

import (
	"bytes"
	"io"

	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/dio/v2/alloc"
)

// memoryReader owns a pin on an immutable resident value. Its cursor borrows
// allocation chunks; replacement and reclamation cannot release this pin.
type memoryReader struct {
	memory    *alloc.Buffer
	chunks    [][]byte
	offset    int
	remaining int64
	size      int64
}

// newMemoryReader takes ownership of memory, including on error. Resident
// values are trusted; only disk reads verify checksums.
func newMemoryReader(key []byte, h blobstore.RecordHeader, memory *alloc.Buffer) (_ Reader, err error) {
	r := &memoryReader{
		memory: memory, chunks: memory.Chunks(), offset: blobstore.RecordHeaderSize,
		remaining: int64(h.KeyLen) + int64(h.PhysicalLen), size: int64(h.LogicalLen),
	}
	defer func() {
		if err != nil {
			r.Close()
		}
	}()
	if len(key) != int(h.KeyLen) {
		return nil, ErrNotFound
	}
	// Compare the full key because the metadata lookup uses its hash.
	for rest := key; len(rest) != 0; {
		data := r.readable()
		n := min(len(rest), len(data))
		if !bytes.Equal(rest[:n], data[:n]) {
			return nil, ErrNotFound
		}
		r.advance(n)
		rest = rest[n:]
	}
	return r, nil
}

func (r *memoryReader) Size() int64 { return r.size }

func (r *memoryReader) readable() []byte {
	data := r.chunks[0][r.offset:]
	return data[:min(int64(len(data)), r.remaining)]
}

func (r *memoryReader) advance(n int) {
	r.remaining -= int64(n)
	r.offset += n
	if r.offset == len(r.chunks[0]) {
		r.chunks = r.chunks[1:]
		r.offset = 0
	}
}

func (r *memoryReader) Read(p []byte) (int, error) {
	if len(p) != 0 && r.remaining == 0 {
		return 0, io.EOF
	}
	var read int
	for len(p) != 0 && r.remaining != 0 {
		n := copy(p, r.readable())
		r.advance(n)
		read += n
		p = p[n:]
	}
	return read, nil
}

func (r *memoryReader) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for r.remaining != 0 {
		data := r.readable()
		n, err := w.Write(data)
		r.advance(n)
		total += int64(n)
		if err != nil {
			return total, err
		}
		if n != len(data) {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

func (r *memoryReader) Close() error {
	if r.memory != nil {
		r.memory.Unpin()
		r.memory = nil
	}
	r.chunks = nil
	return nil
}
