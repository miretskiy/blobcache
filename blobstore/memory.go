package blobstore

import (
	"iter"

	"github.com/miretskiy/dio/v2/alloc"
)

// Memory owns one reference to each allocation in an immutable cached record.
type Memory []alloc.Memory

func (m Memory) Size() (size int64) {
	for _, part := range m {
		size += int64(len(part.Bytes()))
	}
	return size
}

func (m Memory) Chunks() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for _, part := range m {
			if !yield(part.Bytes()) {
				return
			}
		}
	}
}

func (m Memory) TryInc() bool {
	if len(m) == 0 {
		return false
	}
	for i, part := range m {
		if !part.TryInc() {
			m[:i].Release()
			return false
		}
	}
	return true
}

func (m Memory) Release() (freed int64) {
	for _, part := range m {
		if part.Unpin() {
			freed += int64(len(part.Bytes()))
		}
	}
	return freed
}

func (m Memory) Unpin() bool { return len(m) != 0 && m.Release() == m.Size() }
