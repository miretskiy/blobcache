package blobstore

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/sys"
	"github.com/zeebo/xxh3"
)

// Reader owns a buffer reference and, for external records, an open file.
// Close is required even after EOF. Closing early releases resources without
// reading or verifying the rest of the value. Methods on one Reader must not
// race each other or Store.Close. Read and WriteTo require an open Reader.
//
// The header and full key are matched before NewReader returns. WithVerifyChecksums
// checks any stored payload hash as the last bytes are consumed; earlier streaming
// bytes may already have reached the caller when an error is reported.
type Reader struct {
	scheduler  iosched.Scheduler
	file       *os.File
	memory     *alloc.Buffer
	buffer     *Buffer
	header     RecordHeader
	offset     int64 // next disk read, initially the record header's offset
	remaining  int64 // framed bytes not yet loaded from disk
	hash       *xxh3.Hasher
	verifyData bool // Construction-time decision: verification enabled and a supported hash stored.
	err        error
}

// NewReader opens the disk record described by h and verifies that it belongs to
// key. It acquires reusable staging memory and reads through the scheduler. The
// caller must Close a successful Reader before closing Store or its allocators.
//
// A missing object or a header/key mismatch at a reused data location is a miss.
// TODO: coordinate ongoing inline reads with data.db reuse during reclamation.
func (st *Store) NewReader(key []byte, h RecordHeader) (_ *Reader, err error) {
	verifyChecksums := st.cfg.verifyChecksums
	r := &Reader{
		scheduler:  st.scheduler,
		header:     h,
		verifyData: verifyChecksums && h.Flags.DataIntegrity() == DataIntegrityXXHash128,
	}
	defer func() {
		if err != nil {
			_ = r.Close()
		}
	}()
	if len(key) != int(h.KeyLen) {
		return nil, ErrNotFound
	}
	total := RecordHeaderSize + int64(h.KeyLen) + int64(h.PhysicalLen)
	r.memory, err = st.staging.TryAcquire(min(align.PageAlign(total), align.PageAlign(st.cfg.stagingSize)))
	if err != nil {
		return nil, err
	}
	metrics.DiskReadBytes.Add(r.memory.Cap())
	metrics.DiskReaders.Add(1)
	r.buffer = NewBuffer(r.memory.Chunks())
	// The final read may shorten a chunk. Keep the allocator's slice intact.
	r.buffer.chunks = append([][]byte(nil), r.buffer.chunks...)
	r.file = st.data
	if h.Flags.IsExternal() {
		r.file, err = sys.OpenDirect(filepath.Join(st.base, "objects", strconv.FormatUint(h.ObjectID, 10)), st.fileFlags())
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, err
		}
	}
	r.offset, r.remaining = h.fileOffset(), total

	if err := r.next(); err != nil {
		return nil, err
	}
	var actual RecordHeader
	// The header fits in the first page of every allocator chunk.
	if err := (*RawHeader)(r.buffer.readable()).Decode(&actual, verifyChecksums); err != nil {
		return nil, errors.Join(ErrCorrupt, err)
	}
	// Hash identifies the key in index metadata and checksums the data here.
	h.Hash = actual.Hash
	if actual != h {
		return nil, ErrNotFound
	}
	r.header = actual
	r.buffer.rPos.advance(r.buffer.chunks, RecordHeaderSize)
	// Keys can exceed the staging size and straddle allocator chunks.
	for rest := key; len(rest) != 0; {
		if err := r.next(); err != nil {
			return nil, err
		}
		data := r.buffer.readable()
		n := min(len(rest), len(data))
		if !bytes.Equal(rest[:n], data[:n]) {
			return nil, ErrNotFound
		}
		r.buffer.rPos.advance(r.buffer.chunks, n)
		rest = rest[n:]
	}
	if r.verifyData {
		r.hash = xxh3.New()
		r.hash.Write(key)
	}
	if err := r.verifyEnd(); err != nil { // Includes empty values.
		return nil, err
	}
	return r, nil
}

func (r *Reader) Size() int64 { return int64(r.header.LogicalLen) }

// next makes bytes available without consuming them. Each refill ends at a
// page boundary, except the logical end of the record; padding is never exposed.
func (r *Reader) next() error {
	if r.err != nil {
		return r.err
	}
	if r.buffer.Len() != 0 {
		return nil
	}
	if r.remaining == 0 {
		return io.EOF
	}
	n := min(r.remaining, r.buffer.Cap())
	end := r.buffer.position(align.PageAlign(n))
	chunks := r.buffer.chunks
	if end.offset != 0 {
		chunks[end.chunk] = chunks[end.chunk][:end.offset]
		chunks = chunks[:end.chunk+1]
	} else {
		chunks = chunks[:end.chunk]
	}
	ticket, err := r.scheduler.Submit(iosched.ReadvOp(r.file, chunks, r.offset))
	if err == nil {
		var read int
		read, err = ticket.Wait()
		if ticket.Slat > 0 {
			metrics.ReadSlat.RecordValue(int64(ticket.Slat))
			metrics.ReadClat.RecordValue(int64(ticket.Clat))
		}
		if err == nil && int64(read) < n {
			err = errors.Join(ErrCorrupt, io.ErrUnexpectedEOF)
		}
	}
	if err != nil {
		r.err = err
		return err
	}
	r.buffer.Reset()
	r.buffer.wPos = r.buffer.position(n)
	r.offset += n
	r.remaining -= n
	return nil
}

func (r *Reader) verifyEnd() error {
	if !r.verifyData {
		return nil
	}
	if r.remaining == 0 && r.buffer.Len() == 0 && r.hash.Sum128() != r.header.Hash {
		r.err = ErrCorrupt
	}
	return r.err
}

func (r *Reader) consume(n int) error {
	if r.verifyData {
		r.hash.Write(r.buffer.readable()[:n])
	}
	r.buffer.rPos.advance(r.buffer.chunks, n)
	return r.verifyEnd()
}

func (r *Reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.next(); err != nil {
		return 0, err
	}
	n := copy(p, r.buffer.readable())
	return n, r.consume(n)
}

// WriteTo passes existing buffer slices directly to w, avoiding io.Copy's
// intermediate buffer. It does not close the Reader.
func (r *Reader) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for {
		if err := r.next(); err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
		data := r.buffer.readable()
		n, err := w.Write(data)
		total += int64(n)
		readErr := r.consume(n)
		if err != nil {
			return total, err
		}
		if readErr != nil {
			return total, readErr
		}
		if n != len(data) {
			return total, io.ErrShortWrite
		}
	}
}

func (r *Reader) Close() error {
	if r.memory != nil {
		allocated := r.memory.Cap()
		r.memory.Unpin()
		metrics.DiskReadBytes.Add(-allocated)
		metrics.DiskReaders.Add(-1)
		r.memory = nil
	}
	r.buffer = nil
	r.scheduler = nil
	if r.file != nil && r.header.Flags.IsExternal() {
		err := r.file.Close()
		r.file = nil
		return err
	}
	return nil
}
