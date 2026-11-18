package blobstore

import (
	"errors"
	"io"

	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
)

var (
	ErrSealed   = errors.New("blobstore: writer is sealed")
	ErrUnsealed = errors.New("blobstore: writer is not sealed")
)

// Writer accumulates a value in allocator memory without requiring its size or
// key up front. Seal reserves storage and encodes the final header and key.
// Store.Put borrows it for I/O; the caller must
// Discard it, including on failure. Methods on one Writer must be sequential.
// Values larger than available allocator memory fail with alloc.ErrExhausted.
type Writer struct {
	store   *Store
	segment *segment // reservation from Seal until Put or Discard reports it
	slot    int
	err     error
	buf     *alloc.Buffer
	frame   *alloc.ReadOnlyBuffer
	header  RecordHeader
	sealed  bool
}

func (w *Writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.sealed {
		return 0, ErrSealed
	}
	n, err := w.buf.Write(p)
	w.err = err
	return n, err
}

// Seal assigns the record's log position and fills the header/key prefix
// without moving value bytes. Sealing or writing again returns ErrSealed.
func (w *Writer) Seal(key []byte) error {
	if w.err != nil {
		return w.err
	}
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > MaxKeyLen {
		return ErrKeyTooLarge
	}
	if w.sealed {
		return ErrSealed
	}
	size := uint64(w.buf.Len() - recordPrefixSize)
	h := RecordHeader{Flags: FlagHeaderIntegrity, PhysicalLen: size,
		LogicalLen: size, KeyLen: uint16(len(key)), Hash: HashKey(key)}
	seg, slot, err := w.store.reserve(&h)
	if err != nil {
		return err
	}
	w.segment, w.slot, w.header = seg, slot, h
	prefix := w.buf.RawAllocations()[0].Bytes()
	h.Encode((*RawHeader)(prefix))
	copy(prefix[RecordHeaderSize:], key)
	w.buf.Trim()
	w.frame = w.buf.Freeze()
	w.sealed = true
	return nil
}

// Header returns the final sealed record metadata.
func (w *Writer) Header() RecordHeader { return w.header }

// Buffer borrows the sealed frame. Store.Put borrows it through completion.
func (w *Writer) Buffer() *alloc.ReadOnlyBuffer { return w.frame }

// TakeBuffer transfers the sealed frame's ownership without retaining memory.
// Call only after Store.Put completes.
func (w *Writer) TakeBuffer() *alloc.ReadOnlyBuffer {
	frame := w.frame
	w.frame, w.buf = nil, nil
	return frame
}

func (w *Writer) Discard() {
	if w.segment != nil {
		w.store.finish(w.segment, w.slot, ErrUnsealed)
		w.segment = nil
	}
	if w.frame != nil {
		w.frame.Release()
		w.frame, w.buf = nil, nil
	} else if w.buf != nil {
		w.buf.Release()
		w.buf = nil
	}
	w.err = io.ErrClosedPipe
}

var emptyRecordPrefix [recordPrefixSize]byte

func (st *Store) NewWriter() (*Writer, error) {
	b := alloc.NewAllocatingBuffer(st.memory)
	if _, err := b.Write(emptyRecordPrefix[:]); err != nil {
		b.Release()
		return nil, err
	}
	return &Writer{store: st, buf: b}, nil
}

// Ticket waits for the write and the caller's completion updates.
type Ticket struct{ io iosched.Ticket }

var ErrInvalidTicket = errors.New("blobstore: invalid ticket")

func (t Ticket) Wait() error {
	if t.io == (iosched.Ticket{}) {
		return ErrInvalidTicket
	}
	_, err := t.io.Wait()
	return err
}

// Put borrows a sealed writer until notification. It owns only the storage
// reservation, which it reports after the caller's cache updates. The caller
// owns the buffer throughout; notify may release it or transfer it to the cache.
// The whole frame is one submission with one admission decision.
func (st *Store) Put(w *Writer, notify func(error)) (Ticket, error) {
	if w.sealed && w.segment == nil {
		return Ticket{}, ErrSealed // Already put or discarded; the first Put's notify owns cleanup.
	}
	if !w.sealed {
		if notify != nil {
			notify(ErrUnsealed)
		}
		return Ticket{}, ErrUnsealed
	}
	h, seg, slot := w.header, w.segment, w.slot
	w.segment = nil
	op := iosched.NVMeWriteBufferOp(st.device, w.buf, int(h.frameSize()), st.geo.offset(h.Position))
	if st.cfg.smallWriteAdmission > 0 && h.PhysicalLen <= uint64(st.cfg.smallWriteAdmission) {
		op = op.AllowBacklog()
	}
	ticket, err := iosched.SubmitNotify(st.writeScheduler(), op, func(_ int, err error) {
		if notify != nil {
			notify(err)
		}
		st.finish(seg, slot, err)
	})
	if err != nil {
		if notify != nil {
			notify(err)
		}
		st.finish(seg, slot, err)
	}
	return Ticket{io: ticket}, err
}
