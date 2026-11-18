package blobstore

import (
	"io"

	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
)

// Reader streams a range of a record's value. It reads the pages covering the
// range one read window at a time, with the next window in flight while the
// current one is consumed. Log positions are never reused, so a record still
// live when its read completes is the record that was read; one the log has
// overwritten is ErrNotFound.
//
// Next returns immutable chunks borrowed from the current window; they stay
// valid until the next call. Close never waits for I/O. Methods on one Reader
// must not race each other or Store.Close.
type Reader struct {
	st        *Store
	header    RecordHeader
	lo, hi    int64 // frame bytes of the range
	pos, end  int64 // frame bytes not yet submitted
	cur, next window
	value     alloc.BufferView // unread range bytes in cur
	err       error            // first failure, or io.EOF once the range is consumed
}

// window is one read of frame bytes [lo, hi).
type window struct {
	buffer *alloc.ReadOnlyBuffer
	ticket iosched.Ticket
	lo, hi int64
}

// ValueRange clamps n bytes at off to the value and returns the clamped length.
func (h RecordHeader) ValueRange(off, n int64) (int64, error) {
	size := int64(h.LogicalLen)
	if off < 0 || n < 0 || off > size {
		return 0, ErrInvalidRange
	}
	return min(n, size-off), nil
}

// NewReader reads the whole value; see NewRangeReader.
func (st *Store) NewReader(h RecordHeader) (*Reader, error) {
	return st.NewRangeReader(h, 0, int64(h.LogicalLen))
}

// NewRangeReader reads n bytes of the value from off, clamped to its end, and
// submits the first window. h is the record's index header. Close every
// returned Reader before closing Store or its allocator.
func (st *Store) NewRangeReader(h RecordHeader, off, n int64) (*Reader, error) {
	n, err := h.ValueRange(off, n)
	if err != nil {
		return nil, err
	}
	if !st.Live(h) {
		return nil, st.miss(h)
	}
	lo := h.ValueOffset() + off
	r := &Reader{st: st, header: h, lo: lo, hi: lo + n}
	r.pos, r.end = lo&^(align.BlockSize-1), align.PageAlign(lo+n)
	if n == 0 {
		r.pos = r.end
	}
	if r.next, err = r.submit(); err != nil {
		return nil, err
	}
	metrics.DiskReaders.Add(1)
	return r, nil
}

// miss reports a record the log no longer holds.
func (st *Store) miss(h RecordHeader) error {
	if st.cfg.onMiss != nil {
		st.cfg.onMiss(h)
	}
	return ErrNotFound
}

// submit reads the next window of [pos, end) into a new buffer; it returns an
// empty window once everything is submitted.
func (r *Reader) submit() (window, error) {
	if r.pos == r.end {
		return window{}, nil
	}
	w := window{lo: r.pos, hi: min(r.end, r.pos+r.st.cfg.readWindow)}
	r.pos = w.hi
	buffer := alloc.NewAllocatingBuffer(r.st.memory)
	if err := buffer.Grow(int(w.hi - w.lo)); err != nil {
		buffer.Release()
		return window{}, err
	}
	w.buffer = buffer.Freeze()
	metrics.DiskReadBytes.Add(int64(w.buffer.Cap()))
	// The I/O holds its own reference so Close can return while it is pending.
	pinned := w.buffer.Retain()
	op := iosched.NVMeReadBufferOp(r.st.device, buffer, int(w.hi-w.lo), r.st.geo.offset(r.header.Position)+w.lo)
	var err error
	w.ticket, err = iosched.SubmitNotify(r.st.readScheduler(), op, func(int, error) { pinned.Release() })
	if err != nil {
		pinned.Release()
		w.release()
		return window{}, err
	}
	return w, nil
}

func (w *window) release() {
	if w.buffer != nil {
		metrics.DiskReadBytes.Add(-int64(w.buffer.Cap()))
		w.buffer.Release()
	}
	*w = window{}
}

// advance moves to the next window holding range bytes: it waits for the read,
// checks that the log still holds the record, and puts the following window in
// flight. It reports io.EOF after the last window.
func (r *Reader) advance() error {
	for r.err == nil && r.value.Len() == 0 {
		if r.next.buffer == nil {
			r.err = io.EOF
			break
		}
		r.cur.release()
		r.cur, r.next = r.next, window{}
		if _, err := r.cur.ticket.Wait(); err != nil {
			r.err = err
		} else if !r.st.Live(r.header) {
			r.err = r.st.miss(r.header)
		} else if r.next, r.err = r.submit(); r.err == nil {
			lo, hi := max(r.cur.lo, r.lo), min(r.cur.hi, r.hi)
			r.value = r.cur.buffer.View(int(lo-r.cur.lo), int(hi-lo))
		}
	}
	if r.value.Len() != 0 {
		return nil
	}
	return r.err
}

// Size returns the length of the reader's range.
func (r *Reader) Size() int64 { return r.hi - r.lo }

func (r *Reader) Next() ([]byte, error) {
	if err := r.advance(); err != nil {
		return nil, err
	}
	return r.value.Next()
}

func (r *Reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.advance(); err != nil {
		return 0, err
	}
	return r.value.Read(p)
}

func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for {
		if err := r.advance(); err == io.EOF {
			return total, nil
		} else if err != nil {
			return total, err
		}
		n, err := r.value.WriteTo(w)
		total += n
		if err != nil {
			return total, err
		}
	}
}

func (r *Reader) Close() error {
	if r.st != nil {
		metrics.DiskReaders.Add(-1)
		r.st = nil
	}
	r.cur.release()
	r.next.release()
	r.value = alloc.BufferView{}
	return nil
}
