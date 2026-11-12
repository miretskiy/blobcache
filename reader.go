package blobcache

import (
	"bytes"
	"github.com/miretskiy/blobcache/blobstore"
	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/alloc"
)

// memoryReader owns one buffer reference; its embedded cursor borrows it.
type memoryReader struct {
	alloc.BufferView
	memory *alloc.ReadOnlyBuffer
	size   int64
}

// newMemoryReader consumes buffer, including on error. Resident values are
// trusted; the full header and key must be cached.
func newMemoryReader(key []byte, h blobstore.RecordHeader, buffer *alloc.ReadOnlyBuffer) (Reader, error) {
	if len(key) != int(h.KeyLen) {
		buffer.Release()
		return nil, ErrNotFound
	}
	if int64(buffer.Cap()) < h.FrameLen() {
		buffer.Release()
		return nil, ErrCorrupt
	}
	if !bytes.Equal(key, buffer.First()[blobstore.RecordHeaderSize:blobstore.RecordHeaderSize+len(key)]) {
		buffer.Release()
		return nil, ErrNotFound
	}
	metrics.ResidentReadBytes.Add(int64(buffer.Cap()))
	metrics.ResidentReaders.Add(1)
	return &memoryReader{BufferView: buffer.View(int(h.ValueOffset()), int(h.PhysicalLen)), memory: buffer, size: int64(h.LogicalLen)}, nil
}

func (r *memoryReader) Size() int64 { return r.size }

func (r *memoryReader) Close() error {
	if r.memory != nil {
		metrics.ResidentReadBytes.Add(-int64(r.memory.Cap()))
		metrics.ResidentReaders.Add(-1)
		r.memory.Release()
		r.memory = nil
		r.BufferView = alloc.BufferView{}
	}
	return nil
}
