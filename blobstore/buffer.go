package blobstore

import (
	"errors"
	"io"

	"github.com/miretskiy/dio/v2/align"
)

var ErrBufferFull = errors.New("blobstore: buffer full")

// Buffer is a fixed-capacity byte buffer over allocator chunks. It borrows the
// allocation: the caller holds a reference while using it, including pending I/O.
// Reads consume bytes but do not free space for writes; Reset permits reuse once
// the caller has finished with those bytes. Buffer never grows or moves data.
type Buffer struct {
	chunks     [][]byte
	capacity   int64
	rPos, wPos bufferPosition
}

type bufferPosition struct {
	chunk  int
	offset int
	pos    int64
}

// NewBuffer starts with no readable bytes and uses the supplied chunks' full
// capacity. Each chunk must be nonempty. The caller keeps the memory alive.
func NewBuffer(chunks [][]byte) *Buffer {
	b := &Buffer{chunks: chunks}
	for _, chunk := range b.chunks {
		b.capacity += int64(len(chunk))
	}
	return b
}

func (b *Buffer) Cap() int64       { return b.capacity }
func (b *Buffer) Len() int64       { return b.wPos.pos - b.rPos.pos }
func (b *Buffer) Available() int64 { return b.capacity - b.wPos.pos }

func (b *Buffer) Reset() { b.rPos, b.wPos = bufferPosition{}, bufferPosition{} }

// resetAt preserves a prefix while making the rest of the allocation reusable.
func (b *Buffer) resetAt(offset int64) {
	b.rPos = b.position(offset)
	b.wPos = b.rPos
}

func (b *Buffer) position(offset int64) bufferPosition {
	p := bufferPosition{pos: offset}
	for p.chunk < len(b.chunks) && offset >= int64(len(b.chunks[p.chunk])) {
		offset -= int64(len(b.chunks[p.chunk]))
		p.chunk++
	}
	p.offset = int(offset)
	return p
}

// advance stays within the current chunk, moving to the next at its boundary.
func (p *bufferPosition) advance(chunks [][]byte, n int) {
	p.pos += int64(n)
	p.offset += n
	if p.offset == len(chunks[p.chunk]) {
		p.chunk++
		p.offset = 0
	}
}

func (b *Buffer) writable() []byte { return b.chunks[b.wPos.chunk][b.wPos.offset:] }

func (b *Buffer) readable() []byte {
	data := b.chunks[b.rPos.chunk][b.rPos.offset:]
	return data[:min(int64(len(data)), b.Len())]
}

func (b *Buffer) Write(p []byte) (int, error) {
	var written int
	for len(p) != 0 {
		if b.Available() == 0 {
			return written, ErrBufferFull
		}
		n := copy(b.writable(), p)
		b.wPos.advance(b.chunks, n)
		written += n
		p = p[n:]
	}
	return written, nil
}

// ReadFrom stops at EOF, a source error, or capacity. ErrBufferFull means the
// caller must consume the data and Reset before continuing with the source.
func (b *Buffer) ReadFrom(r io.Reader) (int64, error) {
	start := b.wPos.pos
	for b.Available() != 0 {
		n, err := r.Read(b.writable())
		b.wPos.advance(b.chunks, n)
		if err == io.EOF {
			return b.wPos.pos - start, nil
		}
		if err != nil {
			return b.wPos.pos - start, err
		}
	}
	return b.wPos.pos - start, ErrBufferFull
}

func (b *Buffer) Read(p []byte) (int, error) {
	if len(p) != 0 && b.Len() == 0 {
		return 0, io.EOF
	}
	var read int
	for len(p) != 0 && b.Len() != 0 {
		n := copy(p, b.readable())
		b.rPos.advance(b.chunks, n)
		read += n
		p = p[n:]
	}
	return read, nil
}

func (b *Buffer) WriteTo(w io.Writer) (int64, error) {
	var written int64
	for b.Len() != 0 {
		data := b.readable()
		n, err := w.Write(data)
		b.rPos.advance(b.chunks, n)
		written += int64(n)
		if err != nil {
			return written, err
		}
		if n != len(data) {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

// padded returns the written range extended to a page boundary. Only the slice
// descriptors are copied; the allocator still owns the underlying bytes.
func (b *Buffer) padded(start int64) [][]byte {
	end := align.PageAlign(b.wPos.pos)
	p := b.position(start)
	data := make([][]byte, 0, len(b.chunks)-p.chunk)
	for start < end {
		chunk := b.chunks[p.chunk][p.offset:]
		chunk = chunk[:min(int64(len(chunk)), end-start)]
		if start+int64(len(chunk)) > b.wPos.pos {
			clear(chunk[max(0, b.wPos.pos-start):])
		}
		data = append(data, chunk)
		start += int64(len(chunk))
		p.chunk++
		p.offset = 0
	}
	return data
}
