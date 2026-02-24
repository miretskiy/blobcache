package blobcache

import "container/heap"

// mergingIter implements k-way merge over multiple SSTable iterators.
// Yields entries in user key order (ascending for forward, descending for reverse).
// When multiple sources have the same key, highest segID (newest) wins.
type mergingIter struct {
	sources []mergeSource
	h       mergeHeap
	cmp     func(a, b []byte) int
	prevKey []byte // last yielded key (for dedup)
	err     error
}

// mergeSource represents one SSTable iterator in the merge.
type mergeSource struct {
	segID uint32
	iter  sstableIter
}

// sstableIter abstracts SSTable iteration for testing.
type sstableIter interface {
	First() (key []byte, val []byte, ok bool)
	Next() (key []byte, val []byte, ok bool)
	SeekGE(target []byte) (key []byte, val []byte, ok bool)
	Close() error
	Error() error
}

// mergeEntry is one element in the min-heap.
type mergeEntry struct {
	userKey []byte
	value   sstValue
	srcIdx  int
	segID   uint32
}

// mergeHeap implements heap.Interface for the k-way merge.
type mergeHeap struct {
	entries []mergeEntry
	cmp     func(a, b []byte) int
}

func (h mergeHeap) Len() int { return len(h.entries) }

func (h mergeHeap) Less(i, j int) bool {
	c := h.cmp(h.entries[i].userKey, h.entries[j].userKey)
	if c != 0 {
		return c < 0
	}
	// Same key: highest segID first (newest wins).
	return h.entries[i].segID > h.entries[j].segID
}

func (h mergeHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
}

func (h *mergeHeap) Push(x any) {
	h.entries = append(h.entries, x.(mergeEntry))
}

func (h *mergeHeap) Pop() any {
	old := h.entries
	n := len(old)
	x := old[n-1]
	h.entries = old[:n-1]
	return x
}

func newMergingIter(sources []mergeSource, cmp func(a, b []byte) int) *mergingIter {
	return &mergingIter{
		sources: sources,
		h:       mergeHeap{cmp: cmp},
		cmp:     cmp,
	}
}

// First positions all source iterators at their first key and returns the
// smallest unique key across all sources.
func (m *mergingIter) First() ([]byte, sstValue, bool) {
	m.h.entries = m.h.entries[:0]
	m.prevKey = nil

	for i := range m.sources {
		src := &m.sources[i]
		key, val, ok := src.iter.First()
		if !ok {
			if err := src.iter.Error(); err != nil {
				m.err = err
				return nil, sstValue{}, false
			}
			continue
		}
		if len(val) < sstValueSize {
			continue
		}
		heap.Push(&m.h, mergeEntry{
			userKey: append([]byte(nil), key...),
			value:   decodeSSTValue(val),
			srcIdx:  i,
			segID:   src.segID,
		})
	}
	heap.Init(&m.h)
	return m.next()
}

// SeekGE positions all sources at the first key >= target.
func (m *mergingIter) SeekGE(target []byte) ([]byte, sstValue, bool) {
	m.h.entries = m.h.entries[:0]
	m.prevKey = nil

	for i := range m.sources {
		src := &m.sources[i]
		key, val, ok := src.iter.SeekGE(target)
		if !ok {
			if err := src.iter.Error(); err != nil {
				m.err = err
				return nil, sstValue{}, false
			}
			continue
		}
		if len(val) < sstValueSize {
			continue
		}
		heap.Push(&m.h, mergeEntry{
			userKey: append([]byte(nil), key...),
			value:   decodeSSTValue(val),
			srcIdx:  i,
			segID:   src.segID,
		})
	}
	heap.Init(&m.h)
	return m.next()
}

// Next returns the next unique key in sorted order.
func (m *mergingIter) Next() ([]byte, sstValue, bool) {
	return m.next()
}

// next pops from the heap, deduplicates, and returns the next unique key.
func (m *mergingIter) next() ([]byte, sstValue, bool) {
	for m.h.Len() > 0 {
		top := heap.Pop(&m.h).(mergeEntry)

		// Advance the source that produced this entry.
		src := &m.sources[top.srcIdx]
		key, val, ok := src.iter.Next()
		if ok && len(val) >= sstValueSize {
			heap.Push(&m.h, mergeEntry{
				userKey: append([]byte(nil), key...),
				value:   decodeSSTValue(val),
				srcIdx:  top.srcIdx,
				segID:   src.segID,
			})
		} else if err := src.iter.Error(); err != nil {
			m.err = err
			return nil, sstValue{}, false
		}

		// Dedup: skip if same key as previously yielded.
		if m.prevKey != nil && m.cmp(top.userKey, m.prevKey) == 0 {
			continue
		}

		m.prevKey = top.userKey
		return top.userKey, top.value, true
	}

	return nil, sstValue{}, false
}

// Error returns any error encountered during iteration.
func (m *mergingIter) Error() error {
	return m.err
}

// Close closes all source iterators.
func (m *mergingIter) Close() error {
	var firstErr error
	for i := range m.sources {
		if err := m.sources[i].iter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
