package blobstore

import (
	"errors"
	"hash/crc32"
	"io"
	"os"

	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/collections"
	"github.com/miretskiy/dio/v2/iosched"
)

// SizeUnknown selects a dedicated object file with no promised value size.
const SizeUnknown int64 = -1

var ErrWriterFull = errors.New("blobstore: value exceeds writer reservation")

// Writer owns the allocation until Discard. Obtain it from Store.NewWriter;
// its zero value is not usable. Inline values retain all bytes;
// external values reuse staging space after waiting for its writes. The first
// page stays intact so finish can replace its header without rereading it.
// Calls are sequential. Write, ReadFrom, and Flush require an open, unpublished
// writer. Put may be called once; afterward only metadata access, RetainBuffer,
// Close, and Discard are allowed. The reservation remains protected until Close.
type Writer struct {
	store       *Store
	key         KeyHash
	file        *os.File
	vfd         int // -1 for inline records or an unopened/closed external descriptor
	header      RecordHeader
	reservation collections.Handle // protected until Close
	committed   bool               // Close releases a committed record or discards an unfinished one
	scheduler   iosched.Scheduler
	memory      *alloc.Buffer // owns the allocation referenced by buffer
	buffer      *Buffer
	limit       int64 // maximum value size, or SizeUnknown; actual size is header.PhysicalLen
	checksum    uint32
	err         error
}

// NewWriter reserves memory, a data location, and an index slot. The key is
// copied before return. size is the expected value size, or SizeUnknown.
// Inline writes are bounded by their reservation. Keys must fit MaxKeyLen.
// Allocation exhaustion is returned to the caller. ErrBusy means reuse would
// overlap an open writer. Reusing index slots waits if their wipe is in flight.
// External files open with the first write; descriptor exhaustion returns ErrNoDescriptors.
// The caller must Discard the writer when finished, including after Put.
func (st *Store) NewWriter(key []byte, size int64) (*Writer, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	if len(key) > MaxKeyLen {
		return nil, ErrKeyTooLarge
	}
	if size < SizeUnknown {
		return nil, errors.New("blobstore: invalid value size")
	}
	allocator := st.memory
	var bufferSize int64
	var storeFile *os.File
	external := size == SizeUnknown || size > st.cfg.inlineThreshold

	if external {
		allocator = st.staging
		bufferSize = align.BlockSize + align.PageAlign(st.cfg.stagingSize)
		size = SizeUnknown
	} else {
		bufferSize = align.PageAlign(RecordHeaderSize + int64(len(key)) + size)
		storeFile = st.data
	}

	memory, err := allocator.TryAcquire(bufferSize)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		store: st,
		key:   HashKey(key),
		file:  storeFile,
		vfd:   -1,
		header: RecordHeader{
			Flags: st.cfg.flags | FlagKeyHashXXH3,
		},
		scheduler: st.writeScheduler(),
		memory:    memory,
		buffer:    NewBuffer(memory.Chunks()),
		limit:     size,
	}
	metrics.WriterBytes.Add(memory.Cap())
	metrics.ActiveWriters.Add(1)
	// Reserve space for the header that finish writes before publication.
	w.buffer.wPos = bufferPosition{offset: RecordHeaderSize, pos: RecordHeaderSize}
	if external {
		w.header.Flags |= FlagExternal
	}
	var inlineSize uint64
	if !external {
		inlineSize = uint64(bufferSize)
	}
	w.reservation, err = st.reserve(&w.header, inlineSize)
	if err != nil {
		w.Discard()
		return nil, err
	}

	if err := w.writeKey(key); err != nil {
		w.Discard()
		return nil, err
	}
	return w, nil
}

func (w *Writer) Size() int64 { return int64(w.header.PhysicalLen) }

// KeyHash identifies the key copied by NewWriter.
func (w *Writer) KeyHash() KeyHash { return w.key }

// IsExternal reports whether the value is stored in a dedicated object file.
func (w *Writer) IsExternal() bool { return w.header.Flags.IsExternal() }

// Header returns the record's index metadata. The payload size is final after Put.
func (w *Writer) Header() RecordHeader {
	h := w.header
	h.Hash = w.key
	return h
}

// RetainBuffer pins the complete inline record, including header and key.
// The caller owns the reference and must Unpin it. External records return nil.
// Call after successful Put and before closing the writer.
func (w *Writer) RetainBuffer() *alloc.Buffer {
	if w.IsExternal() {
		return nil
	}
	w.memory.TryInc() // The writer still owns a reference; Close cannot race this call.
	return w.memory
}

func (w *Writer) endOffset() int64 {
	return w.header.fileOffset() + RecordHeaderSize + int64(w.header.KeyLen) + w.Size()
}

func (w *Writer) writeKey(key []byte) error {
	for len(key) != 0 {
		data, err := w.writable()
		if err != nil {
			return err
		}
		n := copy(data, key)
		key = key[n:]
		w.header.KeyLen += uint16(n)
		w.advance(n)
	}
	return nil
}

func (w *Writer) Write(p []byte) (written int, err error) {
	if w.err != nil {
		return 0, w.err
	}
	for len(p) != 0 {
		if w.limit != SizeUnknown && w.Size() == w.limit {
			w.err = ErrWriterFull
			return written, w.err
		}
		data, err := w.writable()
		if err != nil {
			return written, err
		}
		if w.limit != SizeUnknown {
			data = data[:min(int64(len(data)), w.limit-w.Size())]
		}
		n := copy(data, p)
		written += n
		p = p[n:]
		w.header.PhysicalLen += uint64(n)
		w.advance(n)
	}
	return written, w.err
}

// ReadFrom reads directly into allocator memory until EOF, an error, or the
// reserved size. Source errors remain on the writer for Put to report.
func (w *Writer) ReadFrom(r io.Reader) (int64, error) {
	if w.err != nil {
		return 0, w.err
	}
	start := w.Size()
	for w.limit == SizeUnknown || w.Size() < w.limit {
		data, err := w.writable()
		if err != nil {
			return w.Size() - start, err
		}
		if w.limit != SizeUnknown {
			data = data[:min(int64(len(data)), w.limit-w.Size())]
		}
		n, readErr := r.Read(data)
		w.header.PhysicalLen += uint64(n)
		w.advance(n)
		if readErr == io.EOF {
			return w.Size() - start, nil
		}
		if readErr != nil {
			w.err = readErr
			return w.Size() - start, w.err
		}
	}
	return w.Size() - start, w.err
}

func (w *Writer) writable() ([]byte, error) {
	if w.buffer.Available() == 0 {
		// External writers reuse staging only after its previous contents reach disk.
		if err := w.Flush(); err != nil {
			return nil, err
		}
		w.buffer.resetAt(align.BlockSize)
	}
	return w.buffer.writable(), nil
}

func (w *Writer) advance(n int) {
	if w.header.Flags.DataIntegrity() == DataIntegrityCRC32C {
		w.checksum = crc32.Update(w.checksum, crcTable, w.buffer.writable()[:n])
	}
	w.buffer.wPos.advance(w.buffer.chunks, n)
}

func (w *Writer) writeOp(data [][]byte, offset int64) iosched.Op {
	if w.IsExternal() {
		return iosched.VWritevOp(uint32(w.vfd), data, offset)
	}
	return iosched.WritevOp(w.file, data, offset)
}

// Flush writes buffered bytes, padding the final page, and waits before staging
// can be reused. The partial page remains available for further writes. No fsync.
// Put writes the final header and waits for both data and index completion.
func (w *Writer) Flush() error {
	if w.err != nil || w.buffer.Len() == 0 {
		return w.err
	}
	b := w.buffer
	err := w.submit(w.writeOp(b.padded(b.rPos.pos), w.endOffset()-b.Len()), false)
	b.rPos = b.position(b.wPos.pos &^ align.BlockMask)
	return err
}

// submit links the first I/O to its open and records descriptor ownership from
// individual results. A final chain ends with close -> index write.
func (w *Writer) submit(op iosched.Op, final bool) error {
	opening := w.IsExternal() && w.vfd < 0
	if opening {
		op = w.store.openObject(w.header.ObjectID, os.O_CREATE|os.O_TRUNC|os.O_RDWR).Link(op)
	}
	ticket, err := w.scheduler.Submit(op)
	if err != nil {
		w.err = err
		return err
	}
	_, err = ticket.Wait()
	if ticket.Slat > 0 {
		metrics.WriteSlat.RecordValue(int64(ticket.Slat))
		metrics.WriteClat.RecordValue(int64(ticket.Clat))
	}
	if opening || (final && w.IsExternal()) {
		results := ticket.AllResults()
		if opening && results[0].Err == nil {
			w.vfd = results[0].N
		}
		if final && results[len(results)-2].Err == nil {
			w.vfd = -1 // Close succeeded even if the following index write failed.
		}
	}
	w.err = openError(err)
	return w.err
}

// finish encodes the final header before submitting the last data batch. Earlier
// staging batches, if any, completed before their memory was reused.
func (w *Writer) finish() (iosched.Op, error) {
	if w.err != nil {
		return iosched.Op{}, w.err
	}
	h := &w.header
	h.LogicalLen = h.PhysicalLen
	if h.Flags.DataIntegrity() == DataIntegrityCRC32C {
		h.Hash = uint64(w.checksum)
	}
	h.Encode((*RawHeader)(w.memory.Chunks()[0]))
	b := w.buffer
	if !h.Flags.IsExternal() || w.endOffset() == b.wPos.pos {
		return w.writeOp(b.padded(0), h.fileOffset()), nil
	}
	// The first page was preserved while external staging cycled. Its final
	// header and the remaining tail occupy different file offsets.
	header := w.writeOp([][]byte{b.chunks[0][:align.BlockSize]}, 0)
	if b.Len() == 0 {
		return header, nil
	}
	return w.writeOp(b.padded(b.rPos.pos), w.endOffset()-b.Len()).Link(header), nil
}

// Discard releases the caller's ownership and reclaims excess indexed payload.
// Put has already completed data and index I/O; cleanup may continue afterward.
func (w *Writer) Discard() {
	if w.memory == nil {
		return
	}
	if w.reservation != 0 {
		w.store.mu.Lock()
		if w.committed {
			w.store.mu.index.records.Value(w.reservation).pending = false
		} else {
			w.store.mu.index.remove(w.reservation)
		}
		w.store.mu.Unlock()
		w.reservation = 0
	}
	if w.vfd >= 0 {
		w.store.closeObject(w.scheduler, w.vfd, w.header.ObjectID, !w.committed)
		w.vfd = -1
	} else if w.IsExternal() && !w.committed {
		// A final chain can close the file successfully before its index fails.
		w.store.unlinkObject(w.header.ObjectID)
	}
	w.file = nil
	allocated := w.memory.Cap()
	w.memory.Unpin()
	metrics.WriterBytes.Add(-allocated)
	metrics.ActiveWriters.Add(-1)
	w.memory = nil
	w.buffer = nil
	w.store.reclaimExcess()
}

// Close releases the writer. It is safe to defer immediately after NewWriter.
func (w *Writer) Close() error {
	w.Discard()
	return w.err
}
