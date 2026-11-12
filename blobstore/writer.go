package blobstore

import (
	"errors"
	"hash/crc32"
	"io"
	"os"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/collections"
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
	store       *Store
	reservation collections.Handle
	err         error
	buf         *alloc.Buffer
	frame       *alloc.ReadOnlyBuffer
	header      RecordHeader
	sealed      bool
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

// WriteTo drains a sealed frame to dest, excluding I/O padding.
func (w *Writer) WriteTo(dest io.Writer) (int64, error) {
	if w.err != nil {
		return 0, w.err
	}
	if !w.sealed {
		return 0, ErrUnsealed
	}
	return w.buf.WriteTo(dest)
}

// Seal assigns record placement and fills the header/key prefix without moving value bytes.
// Sealing or writing again returns ErrSealed.
// The data checksum covers key followed by value, excluding page padding.
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
	reservation, h, err := w.store.reserveRecord(key, int64(size))
	if err != nil {
		return err
	}
	w.reservation, w.header = reservation, h
	parts := w.buf.RawAllocations()
	copy(parts[0].Bytes()[RecordHeaderSize:], key)
	var checksum uint32
	if w.header.Flags.DataIntegrity() == DataIntegrityCRC32C {
		checksum = crc32.Checksum(key, crcTable)
		left := int(size)
		for i, memory := range parts {
			p := memory.Bytes()
			if i == 0 {
				p = p[recordPrefixSize:]
			}
			p = p[:min(len(p), left)]
			checksum = crc32.Update(checksum, crcTable, p)
			left -= len(p)
			if left == 0 {
				break
			}
		}
	}
	h.Hash = uint64(checksum)
	h.Encode((*RawHeader)(parts[0].Bytes()))
	w.buf.Trim()
	w.frame = w.buf.Freeze()
	w.sealed = true
	return nil
}

// Size reports accumulated value bytes and any write error before reservation.
func (w *Writer) Size() (int64, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.sealed {
		return 0, ErrSealed
	}
	return int64(w.buf.Len() - recordPrefixSize), nil
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
	if w.reservation != 0 {
		w.store.complete(w.reservation, w.header, ErrUnsealed)
		w.reservation = 0
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
	return openError(err)
}

// Put borrows a sealed writer until notification. It owns only the storage
// reservation, which it completes after the caller's cache updates. The caller
// owns the buffer throughout; notify may release it or transfer it to the cache.
// The whole frame uses one linked submission and one admission decision.
func (st *Store) Put(w *Writer, notify func(error)) (Ticket, error) {
	if !w.sealed {
		if notify != nil {
			notify(ErrUnsealed)
		}
		return Ticket{}, ErrUnsealed
	}
	h, reservation := w.header, w.reservation
	w.reservation = 0
	vfd := uint32(0)
	if h.Flags.IsExternal() {
		vfd = iosched.AllocateVFD
	}
	op := iosched.VWriteBufferOp(vfd, w.buf, int(align.PageAlign(h.FrameLen())), h.fileOffset())
	if h.Flags.IsExternal() {
		op = st.openObject(h.ObjectID, os.O_CREATE|os.O_TRUNC|os.O_RDWR).Link(
			op.HardLink(iosched.VCloseOp(vfd)))
	}
	if st.cfg.smallWriteAdmission > 0 && h.PhysicalLen <= uint64(st.cfg.smallWriteAdmission) {
		op = op.AllowBacklog()
	}
	ticket, err := iosched.SubmitNotify(st.writeScheduler(), op, func(_ int, err error) {
		h := w.Header()
		err = openError(err)
		if notify != nil {
			notify(err)
		}
		st.complete(reservation, h, err)
		if err != nil && h.Flags.IsExternal() {
			st.unlinkObject(st.writeScheduler(), h.ObjectID)
		}
	})
	if err != nil {
		if notify != nil {
			notify(openError(err))
		}
		st.complete(reservation, h, err)
	}
	return Ticket{io: ticket}, err
}
