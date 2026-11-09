package blobstore

import (
	"errors"
	"hash/crc32"
	"io"
	"os"
	"slices"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/collections"
	"github.com/miretskiy/dio/v2/iosched"
)

// Writer buffers writes in fixed allocator memory and flushes to its sink.
// Close discards buffered bytes; Store.Put flushes and publishes the value.
// Calls must be sequential.
type Writer struct {
	err error
	buf *alloc.Buffer
	wr  interface {
		io.WriteCloser
		Flush() error
	}
}

func (b *Writer) Write(p []byte) (nn int, err error) {
	for len(p) != 0 && b.err == nil {
		available := b.buf.Cap() - b.buf.Len()
		if available == 0 {
			b.flush()
			continue
		}
		n, err := b.buf.Write(p[:min(len(p), available)])
		b.err = err
		nn += n
		p = p[n:]
	}
	return nn, b.err
}

func (b *Writer) ReadFrom(r io.Reader) (n int64, err error) {
	if b.err != nil {
		return 0, b.err
	}
	n, b.err = io.Copy(struct{ io.Writer }{b}, r)
	return n, b.err
}

// flush is called on a full buffer, or once at Put before closing the writer.
// This keeps intermediate direct-I/O writes page-aligned.
func (b *Writer) flush() error {
	if b.err != nil {
		return b.err
	}
	_, err := b.buf.WriteTo(b.wr)
	// Drain even after a submission fails, before reusing or releasing memory.
	b.err = errors.Join(err, b.wr.Flush())
	return b.err
}

func (b *Writer) Close() error {
	if b.buf == nil {
		return nil
	}
	err := b.wr.Close()
	b.buf.Release()
	b.buf = nil
	b.err = io.ErrClosedPipe
	return err
}

// bufferSink owns record placement and I/O. Write borrows its input until
// Flush completes; Writer drains the sink before reusing its buffer.
type bufferSink struct {
	store       *Store
	scheduler   iosched.Scheduler
	header      RecordHeader
	reservation collections.Handle
	offset      int64
	checksum    uint32
	vfd         int
	pending     []iosched.Ticket
}

func (st *Store) NewWriter(key []byte) (*Writer, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	if len(key) > MaxKeyLen {
		return nil, ErrKeyTooLarge
	}
	// The fixed buffer holds any inline value, including its framing.
	size := align.PageAlign(max(st.cfg.stagingSize, RecordHeaderSize+int64(len(key))+st.cfg.inlineThreshold))
	b := alloc.NewAllocatingBuffer(st.memory)
	if err := b.Grow(int(size)); err != nil {
		b.Release()
		return nil, err
	}
	b.Write(make([]byte, RecordHeaderSize))
	b.Write(key)
	return &Writer{buf: b, wr: &bufferSink{
		store: st, scheduler: st.writeScheduler(), vfd: -1,
		// Until Put supplies final metadata, a flush starts a streamed record.
		header: RecordHeader{Flags: st.cfg.flags | FlagKeyHashXXH3 | FlagFooter, KeyLen: uint16(len(key)), Hash: HashKey(key)},
	}}, nil
}

// Put consumes w. publish receives ownership of inline memory before disk
// submission, or nil memory after an external value has been written. complete
// follows publish exactly once, including submission failures, and reports disk
// persistence separately. Both callbacks must be brief; complete may run on the
// scheduler. Inline Put returns after submission; external writes remain paced.
func (st *Store) Put(w *Writer, publish func(RecordHeader, Memory), complete func(RecordHeader, error)) (err error) {
	if w.buf == nil {
		return io.ErrClosedPipe
	}
	defer func() { err = errors.Join(err, w.Close()) }()
	if w.err != nil {
		return w.err
	}
	s := w.wr.(*bufferSink)
	h := s.header
	h.PhysicalLen = uint64(s.offset + int64(w.buf.Len()) - RecordHeaderSize - int64(h.KeyLen))
	h.LogicalLen = h.PhysicalLen
	if s.offset == 0 {
		h.Flags &^= FlagFooter
	}
	if h.PhysicalLen > uint64(st.cfg.inlineThreshold) {
		h.Flags |= FlagExternal
	}
	s.header = h
	if h.Flags&FlagFooter == 0 && h.Flags.DataIntegrity() == DataIntegrityCRC32C {
		// An unflushed record can carry its complete checksum in the header.
		remaining := w.buf.Len()
		for i, m := range w.buf.RawAllocations() {
			p := m.Bytes()[:min(len(m.Bytes()), remaining)]
			remaining -= len(p)
			if i == 0 {
				p = p[RecordHeaderSize:]
			}
			s.checksum = crc32.Update(s.checksum, crcTable, p)
			if remaining == 0 {
				break
			}
		}
	}
	if !h.Flags.IsExternal() {
		w.buf.Trim()
		if err := s.reserve(w.buf.RawAllocations()[0].Bytes()); err != nil {
			return err
		}
		h = s.header
		memory := Memory(slices.Clone(w.buf.RawAllocations()))
		memory.TryInc() // Transferred to publish.
		memory.TryInc() // Held by the completion until disk I/O is finished.
		publish(h, memory)
		chunks := slices.Collect(memory.Chunks())
		last := chunks[len(chunks)-1]
		clear(last[len(last)-(w.buf.Cap()-w.buf.Len()):])
		size := w.buf.Cap()
		reservation := s.reservation
		s.reservation = 0 // The completion now owns the reservation.
		finish := func(n int, writeErr error) {
			if writeErr == nil && n != size {
				writeErr = io.ErrShortWrite
			}
			st.mu.Lock()
			if writeErr == nil {
				st.mu.index.publish(reservation, h)
			} else {
				st.mu.index.remove(reservation)
			}
			st.mu.Unlock()
			memory.Release()
			complete(h, writeErr)
		}
		_, err := iosched.SubmitNotify(s.scheduler, iosched.WritevOp(st.data, chunks, h.fileOffset()), finish)
		if err != nil {
			finish(0, err)
		}
		return nil // The inline value was accepted in memory.
	}
	err = w.flush()
	h = s.header
	if err != nil {
		return err
	}
	if h.Flags&FlagFooter != 0 {
		// All body I/O has completed, so reuse the same allocation for the footer.
		page := w.buf.RawAllocations()[0].Bytes()[:align.BlockSize]
		clear(page)
		footer := h
		footer.Hash = uint64(s.checksum)
		footer.Encode((*RawHeader)(page))
		err = errors.Join(s.write(page, align.PageAlign(s.offset)), s.Flush())
		if err != nil {
			return err
		}
	}
	st.mu.Lock()
	st.mu.index.publish(s.reservation, h)
	st.mu.Unlock()
	s.reservation = 0
	publish(h, nil)
	complete(h, nil)
	return nil
}

func (s *bufferSink) Write(p []byte) (int, error) {
	n := len(p)
	padded := p[:align.PageAlign(int64(n))]
	clear(padded[n:])
	if err := s.write(padded, s.offset); err != nil {
		return 0, err
	}
	if s.header.Flags&FlagFooter != 0 && s.header.Flags.DataIntegrity() == DataIntegrityCRC32C {
		if s.offset == 0 {
			p = p[RecordHeaderSize:]
		}
		s.checksum = crc32.Update(s.checksum, crcTable, p)
	}
	s.offset += int64(n)
	return n, nil
}

func (s *bufferSink) write(p []byte, offset int64) error {
	if len(s.pending) == 2 {
		if err := s.Flush(); err != nil {
			return err
		}
	}
	// Reservation and an external open are both part of the first write.
	first := s.reservation == 0
	if first {
		if err := s.reserve(p); err != nil {
			return err
		}
	}
	op := iosched.WriteOp(s.store.data, p, s.header.fileOffset()+offset)
	opening := first && s.header.Flags.IsExternal()
	if s.header.Flags.IsExternal() {
		op = iosched.VWriteOp(uint32(s.vfd), p, offset)
		if opening {
			op = s.store.openObject(s.header.ObjectID, os.O_CREATE|os.O_TRUNC|os.O_RDWR).Link(
				iosched.VWriteOp(iosched.AllocateVFD, p, offset))
		}
	}
	ticket, err := s.scheduler.Submit(op)
	if err != nil {
		return err
	}
	if !opening {
		s.pending = append(s.pending, ticket)
		return nil
	}
	// Resolve the external descriptor before submitting independent writes.
	_, err = ticket.Wait()
	results := ticket.AllResults()
	if results[0].Err == nil {
		s.vfd = results[0].N
	}
	n := results[len(results)-1].N
	if err != nil {
		return openError(err)
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}

// reserve assigns placement and encodes the first page before it is shared.
func (s *bufferSink) reserve(p []byte) error {
	var inlineSize uint64
	if s.header.Flags&FlagFooter != 0 {
		s.header.Flags |= FlagExternal
	} else if !s.header.Flags.IsExternal() {
		inlineSize = uint64(align.PageAlign(RecordHeaderSize + int64(s.header.KeyLen) + int64(s.header.PhysicalLen)))
	}
	var err error
	s.reservation, err = s.store.reserve(&s.header, inlineSize)
	if err != nil {
		return err
	}
	prefix := s.header
	prefix.Hash = uint64(s.checksum)
	prefix.Encode((*RawHeader)(p))
	return nil
}

func (s *bufferSink) Flush() (err error) {
	for _, ticket := range s.pending {
		_, writeErr := ticket.Wait()
		err = errors.Join(err, openError(writeErr))
	}
	clear(s.pending)
	s.pending = s.pending[:0]
	return err
}

func (s *bufferSink) Close() (err error) {
	err = s.Flush()
	if s.vfd >= 0 {
		ticket, closeErr := s.scheduler.Submit(iosched.VCloseOp(uint32(s.vfd)))
		if closeErr == nil {
			_, closeErr = ticket.Wait()
		}
		err = errors.Join(err, closeErr)
		s.vfd = -1
	}
	if s.reservation != 0 {
		s.store.mu.Lock()
		s.store.mu.index.remove(s.reservation)
		s.store.mu.Unlock()
		s.reservation = 0
		if s.header.Flags.IsExternal() {
			s.store.unlinkObject(s.scheduler, s.header.ObjectID)
		}
	}
	return err
}
