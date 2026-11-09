package blobstore

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
)

// readRetries bounds resubmissions of a refill that completed with EAGAIN.
// Backoff doubles from readRetryBackoff, sleeping about 5 ms in total.
const (
	readRetries      = 8
	readRetryBackoff = 20 * time.Microsecond
)

// Reader owns a buffer reference and, for an external record, the object's
// descriptor from its first refill until Close, so eviction cannot remove the
// file from under a reader that has started.
// Close is required even after EOF. Closing early releases resources without
// reading or verifying the rest of the value. Methods on one Reader must not
// race each other or Store.Close. Read and WriteTo require an open Reader.
//
// The header and full key are matched before NewReader returns. WithVerifyChecksums
// checks any stored payload hash as the last bytes are consumed; earlier streaming
// bytes may already have reached the caller when an error is reported.
type Reader struct {
	store      *Store
	scheduler  iosched.Scheduler
	vfd        int // an external record's open descriptor, or -1
	memory     *alloc.Buffer
	chunks     [][]byte
	chunk      int
	header     RecordHeader
	offset     int64 // next disk read, initially the record header's offset
	remaining  int64 // framed bytes not yet loaded from disk
	checksum   uint32
	verifyData bool // Construction-time decision: verification enabled and a supported checksum stored.
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
		store:      st,
		scheduler:  st.readScheduler(),
		vfd:        -1,
		header:     h,
		verifyData: verifyChecksums && (h.Flags.DataIntegrity() == DataIntegrityCRC32C || h.Flags&FlagFooter != 0),
	}
	defer func() {
		if err != nil {
			_ = r.Close()
		}
	}()
	if len(key) != int(h.KeyLen) {
		return nil, ErrNotFound
	}
	if err := st.disk.CanAdmit(iosched.OpRead); err != nil {
		return nil, err
	}
	total := RecordHeaderSize + int64(h.KeyLen) + int64(h.PhysicalLen)
	r.memory = alloc.NewAllocatingBuffer(st.memory)
	err = r.memory.Grow(int(min(align.PageAlign(total), align.PageAlign(st.cfg.stagingSize))))
	if err != nil {
		r.memory.Release()
		r.memory = nil
		return nil, err
	}
	metrics.DiskReadBytes.Add(int64(r.memory.Cap()))
	metrics.DiskReaders.Add(1)
	r.offset, r.remaining = h.fileOffset(), total

	if err := r.next(); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var actual RecordHeader
	// The header fits in the first page of every allocator chunk.
	if err := (*RawHeader)(r.readable()).Decode(&actual, verifyChecksums); err != nil {
		return nil, errors.Join(ErrCorrupt, err)
	}
	// Hash identifies the key in index metadata and checksums the data here.
	expected := h
	if h.Flags&FlagFooter != 0 {
		expected.PhysicalLen, expected.LogicalLen = 0, 0
	}
	expected.Hash = actual.Hash
	if actual != expected {
		return nil, ErrNotFound
	}
	r.header.Hash = actual.Hash
	r.advance(RecordHeaderSize)
	// Keys can exceed the staging size and straddle allocator chunks.
	for rest := key; len(rest) != 0; {
		if err := r.next(); err != nil {
			return nil, err
		}
		data := r.readable()
		n := min(len(rest), len(data))
		if !bytes.Equal(rest[:n], data[:n]) {
			return nil, ErrNotFound
		}
		r.advance(n)
		rest = rest[n:]
	}
	if r.verifyData && r.header.Flags.DataIntegrity() == DataIntegrityCRC32C {
		r.checksum = crc32.Update(0, crcTable, key)
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
	if r.chunk < len(r.chunks) {
		return nil
	}
	if r.remaining == 0 {
		return io.EOF
	}
	n := min(r.remaining, int64(r.memory.Cap()))
	r.chunks = r.chunks[:0]
	for left, i := align.PageAlign(n), 0; left > 0; i++ {
		p := r.memory.RawAllocations()[i].Bytes()
		p = p[:min(int64(len(p)), left)]
		r.chunks = append(r.chunks, p)
		left -= int64(len(p))
	}
	err := r.load(r.chunks, n)
	if err != nil {
		r.err = err
		return err
	}
	last := len(r.chunks) - 1
	r.chunks[last] = r.chunks[last][:int64(len(r.chunks[last]))-(align.PageAlign(n)-n)]
	r.chunk = 0
	r.offset += n
	r.remaining -= n
	return nil
}

// load fills aligned memory and retries transient direct-I/O read failures.
func (r *Reader) load(data [][]byte, n int64) error {
	for attempt := 0; ; attempt++ {
		var err error
		if r.header.Flags.IsExternal() {
			err = r.loadExternal(data, n)
		} else {
			var ticket iosched.Ticket
			ticket, err = r.scheduler.Submit(iosched.ReadvOp(r.store.data, data, r.offset))
			if err == nil {
				var read int
				read, err = r.wait(ticket)
				if err == nil {
					err = checkRead(read, n)
				}
			}
		}
		if !errors.Is(err, syscall.EAGAIN) || attempt == readRetries {
			return err
		}
		metrics.ReadRetries.Add(1)
		time.Sleep(readRetryBackoff << attempt)
	}
}

// loadExternal opens the object file with the first refill and reads later
// refills through the same descriptor, which Close releases.
func (r *Reader) loadExternal(data [][]byte, n int64) error {
	opening := r.vfd < 0
	op := iosched.VReadvOp(uint32(r.vfd), data, r.offset)
	if opening {
		op = r.store.openObject(r.header.ObjectID, os.O_RDONLY).Link(
			iosched.VReadvOp(iosched.AllocateVFD, data, r.offset))
	}
	ticket, err := r.scheduler.Submit(op)
	if err != nil {
		return err
	}
	_, err = r.wait(ticket)
	results := ticket.AllResults()
	if opening && results[0].Err == nil {
		r.vfd = results[0].N
	}
	if err != nil {
		return openError(err)
	}
	return checkRead(results[len(results)-1].N, n)
}

// wait returns the root operation's count and the first error in the chain.
func (r *Reader) wait(ticket iosched.Ticket) (int, error) {
	n, err := ticket.Wait()
	if ticket.Slat > 0 {
		metrics.ReadSlat.RecordValue(int64(ticket.Slat))
		metrics.ReadClat.RecordValue(int64(ticket.Clat))
	}
	return n, err
}

func checkRead(read int, n int64) error {
	if int64(read) < n {
		return errors.Join(ErrCorrupt, io.ErrUnexpectedEOF)
	}
	return nil
}

func (r *Reader) readable() []byte {
	return r.chunks[r.chunk]
}

func (r *Reader) advance(n int) {
	r.chunks[r.chunk] = r.chunks[r.chunk][n:]
	if len(r.chunks[r.chunk]) == 0 {
		r.chunk++
	}
}

func (r *Reader) verifyEnd() error {
	if !r.verifyData || r.remaining != 0 || r.chunk < len(r.chunks) {
		return nil
	}
	if r.header.Flags&FlagFooter != 0 {
		r.offset = align.PageAlign(RecordHeaderSize + int64(r.header.KeyLen) + int64(r.header.PhysicalLen))
		page := r.memory.RawAllocations()[0].Bytes()[:align.BlockSize]
		if err := r.load([][]byte{page}, RecordHeaderSize); err != nil {
			r.err = err
			return err
		}
		var footer RecordHeader
		if err := (*RawHeader)(page).Decode(&footer, true); err != nil {
			r.err = errors.Join(ErrCorrupt, err)
			return r.err
		}
		expected := r.header
		expected.Hash = footer.Hash
		if expected != footer {
			r.err = ErrCorrupt
			return r.err
		}
		r.header.Hash = footer.Hash
	}
	if r.header.Flags.DataIntegrity() == DataIntegrityCRC32C && uint64(r.checksum) != r.header.Hash {
		r.err = ErrCorrupt
	}
	r.verifyData = false
	return r.err
}

func (r *Reader) consume(n int) error {
	if r.verifyData && r.header.Flags.DataIntegrity() == DataIntegrityCRC32C {
		r.checksum = crc32.Update(r.checksum, crcTable, r.readable()[:n])
	}
	r.advance(n)
	return r.verifyEnd()
}

func (r *Reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.next(); err != nil {
		return 0, err
	}
	n := copy(p, r.readable())
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
		data := r.readable()
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
	if r.vfd >= 0 {
		r.store.closeObject(r.scheduler, r.vfd, r.header.ObjectID)
		r.vfd = -1
	}
	if r.memory != nil {
		allocated := int64(r.memory.Cap())
		r.memory.Release()
		metrics.DiskReadBytes.Add(-allocated)
		metrics.DiskReaders.Add(-1)
		r.memory = nil
	}
	r.chunks = nil
	r.chunk = 0
	r.store = nil
	return nil
}
