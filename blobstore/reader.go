package blobstore

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"os"

	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
)

// Reader owns a whole record buffer. NewReader submits I/O immediately; first
// access waits and verifies the record before exposing any value bytes. Next
// returns immutable borrowed chunks valid until Close. Close never waits for I/O.
// Methods on one Reader must not race each other or Store.Close.
type Reader struct {
	frame  *alloc.ReadOnlyBuffer
	value  alloc.BufferView
	ticket iosched.Ticket
	header RecordHeader
	key    []byte
	verify bool
	onMiss func(RecordHeader)
	onRead func(RecordHeader, *alloc.ReadOnlyBuffer)
	ready  bool
	err    error
}

// NewReader submits one whole-frame read. External open, read and close share
// one linked submission. Allocation and admission errors return immediately;
// missing, stale, corrupt and failed disk records are reported on first access.
// Close every returned Reader before closing Store or its allocator.
// TODO: coordinate inline reads with data.db reuse during reclamation.
func (st *Store) NewReader(key []byte, h RecordHeader) (*Reader, error) {
	if len(key) > MaxKeyLen {
		return nil, ErrKeyTooLarge
	}
	if len(key) != int(h.KeyLen) {
		return nil, ErrNotFound
	}
	if err := st.disk.CanAdmit(iosched.OpRead); err != nil {
		return nil, err
	}
	buffer := alloc.NewAllocatingBuffer(st.memory)
	length := int(align.PageAlign(h.FrameLen()))
	if err := buffer.Grow(length); err != nil {
		buffer.Release()
		return nil, err
	}
	vfd := uint32(dataReadVFD)
	if h.Flags.IsExternal() {
		vfd = iosched.AllocateVFD
	}
	op := iosched.VReadBufferOp(vfd, buffer, length, h.fileOffset())
	if h.Flags.IsExternal() {
		op = st.openObject(h.ObjectID, os.O_RDONLY).Link(op.HardLink(iosched.VCloseOp(vfd)))
	}
	r := &Reader{frame: buffer.Freeze(),
		header: h, key: bytes.Clone(key), verify: st.cfg.verifyChecksums, onMiss: st.cfg.onRetire, onRead: st.cfg.onRead}
	metrics.DiskReadBytes.Add(int64(r.frame.Cap()))
	metrics.DiskReaders.Add(1)
	// I/O holds its own reference so Close can return while it is pending.
	frame := r.frame.Retain()
	ticket, err := iosched.SubmitNotify(st.readScheduler(), op, func(_ int, _ error) { frame.Release() })
	if err != nil {
		frame.Release()
		r.Close()
		return nil, openError(err)
	}
	r.ticket = ticket
	return r, nil
}

func (r *Reader) Size() int64 { return int64(r.header.LogicalLen) }

func (r *Reader) load() error {
	if r.ready {
		return r.err
	}
	r.ready = true
	r.err = r.wait()
	if r.err == nil {
		r.err = r.validate()
	}
	if r.err == nil && r.onRead != nil {
		r.onRead(r.header, r.frame)
	}
	if errors.Is(r.err, ErrNotFound) && r.onMiss != nil {
		missing := r.header
		missing.Hash = HashKey(r.key)
		r.onMiss(missing)
	}
	r.key, r.ticket, r.onMiss, r.onRead = nil, iosched.Ticket{}, nil, nil
	return r.err
}

func (r *Reader) wait() error {
	_, err := r.ticket.Wait()
	if r.ticket.Slat > 0 {
		metrics.ReadSlat.RecordValue(int64(r.ticket.Slat))
		metrics.ReadClat.RecordValue(int64(r.ticket.Clat))
	}
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	if err != nil {
		return openError(err)
	}
	results := r.ticket.AllResults()
	if r.header.Flags.IsExternal() {
		results = results[1 : len(results)-1]
	}
	var read int64
	for _, result := range results {
		read += int64(result.N)
	}
	if read < r.header.FrameLen() {
		return errors.Join(ErrCorrupt, io.ErrUnexpectedEOF)
	}
	return nil
}

func (r *Reader) validate() error {
	prefix := r.frame.First()
	var actual RecordHeader
	if err := (*RawHeader)(prefix).Decode(&actual, r.verify); err != nil {
		return errors.Join(ErrCorrupt, err)
	}
	expected := r.header
	expected.Hash = actual.Hash // Index stores the key hash; disk stores the payload checksum.
	if actual != expected || !bytes.Equal(r.key, prefix[RecordHeaderSize:RecordHeaderSize+len(r.key)]) {
		return ErrNotFound
	}
	r.value = r.frame.View(int(expected.ValueOffset()), int(expected.PhysicalLen))
	if r.verify && actual.Flags.DataIntegrity() == DataIntegrityCRC32C {
		checksum := crc32.Checksum(r.key, crcTable)
		view := r.value
		for chunk, err := view.Next(); err == nil; chunk, err = view.Next() {
			checksum = crc32.Update(checksum, crcTable, chunk)
		}
		if uint64(checksum) != actual.Hash {
			return ErrCorrupt
		}
	}
	return nil
}

func (r *Reader) Next() ([]byte, error) {
	if err := r.load(); err != nil {
		return nil, err
	}
	return r.value.Next()
}

func (r *Reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.load(); err != nil {
		return 0, err
	}
	return r.value.Read(p)
}

func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	if err := r.load(); err != nil {
		return 0, err
	}
	return r.value.WriteTo(w)
}

func (r *Reader) Close() error {
	if r.frame != nil {
		metrics.DiskReadBytes.Add(-int64(r.frame.Cap()))
		metrics.DiskReaders.Add(-1)
		r.frame.Release()
		r.frame = nil
		r.value, r.key, r.ticket, r.onMiss, r.onRead = alloc.BufferView{}, nil, iosched.Ticket{}, nil, nil
	}
	return nil
}
