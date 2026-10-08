// Package blobstore is the I/O layer of a disk cache for large blobs. It
// appends records to preallocated segment files through an io_uring
// scheduler, seals each full segment with a footer that lists its records,
// and reads a record back by its Location.
//
// A Store keeps no index and owns no memory for values: the caller supplies
// the memory for every write and read, and keeps its own index of where each
// key lives, which ReadIndex fills from the segments on disk.
//
// On Linux the store requires io_uring; Open fails without it. There is no
// fallback to synchronous I/O, which would put disk waits inside Write.
// Elsewhere it uses dio's POSIX scheduler, for development.
//
// WithMaxSize enables whole-segment FIFO eviction. Notifications describe a batch of retired segments, with no knowledge of the caller's index organization.
package blobstore

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/sys"
	"github.com/zeebo/xxh3"
)

var (
	// ErrClosed is returned by operations on a closed store.
	ErrClosed = errors.New("blobstore: closed")
	// ErrEmptyKey is returned for an empty key.
	ErrEmptyKey = errors.New("blobstore: empty key")
	// ErrKeyTooLarge is returned for a key longer than MaxKeyLen.
	ErrKeyTooLarge = fmt.Errorf("blobstore: key longer than %d bytes", MaxKeyLen)
	// ErrValueTooLarge is returned for a record that cannot fit in a segment.
	ErrValueTooLarge = errors.New("blobstore: value does not fit in a segment")
	// ErrBusy is returned by Write when the record needs a new segment and
	// every write slot is held by a segment still being sealed, and by Read
	// when its segment is not open and every open one is about to be read.
	// The caller decides whether to skip the operation or retry.
	ErrBusy = errors.New("blobstore: busy")
	// ErrNotFound is returned by Read for a Location whose segment file is
	// gone.
	ErrNotFound = errors.New("blobstore: no such record")
	// ErrCorrupt is returned by Read for a record that fails verification: its
	// trailer, its key, or its value, in which case the
	// error is also a *base.ChecksumError.
	ErrCorrupt = errors.New("blobstore: corrupt record")
)

// KeyHash is the 128-bit XXH3 hash of a key, as recorded in segment footers.
// The key itself lives only on disk, next to its value, where every read
// verifies it.
type KeyHash = xxh3.Uint128

// HashKey returns key's KeyHash.
func HashKey(key []byte) KeyHash { return xxh3.Hash128(key) }

// Location is where a record lives on disk. Its zero value names no record.
type Location struct {
	segment uint64 // segment id; ids only grow
	offset  uint32 // byte offset of the record within the segment
	size    uint32 // record size in bytes
}

// Size returns the record's size: the memory Read needs.
func (l Location) Size() int { return int(l.size) }

// Segment returns the segment ID, for batch index maintenance.
func (l Location) Segment() uint64 { return l.segment }

// Store is a set of segment files. All methods are safe for concurrent use,
// except that Close must not race other calls.
//
// Segment files are opened only in io_uring virtual descriptor slots (the
// ring's registered-file table), so creating, writing, sealing and opening
// them for reads are asynchronous. Write does not wait for disk I/O;
// capacity eviction submits unlinks through dio outside the write lock. The first writeSlots slots hold the segments being written
// (the active one and those being sealed); the rest cache segments open for
// reads (see readSlots).
//
//   - A segment's first write is chained after the open of its file into a
//     free write slot and its preallocation. The scheduler holds writes to
//     the slot accepted after that chain until it completes. Preallocation
//     keeps concurrent O_DIRECT writes from extending the file, which would
//     serialize them in the filesystem.
//   - The record that leaves no room for another is the segment's last: its
//     write is chained with the seal — footer, fdatasync, close of the slot,
//     fsync of the segment's directory — and may run past the segment size,
//     extending the file. The scheduler holds a submission that closes a
//     slot, whole, until every operation accepted earlier on it has completed.
//     The footer follows the data and fdatasync covers both. Failed records
//     may appear in a valid footer and are rejected by read verification.
//     The seal's completion returns the slot to the pool.
//     Writes to the next segment never wait for the seal. Every submission of
//     a segment is made with the active segment's lock held, which orders
//     them: the open first, the seal last.
//
// Footers list reservations, not successful writes. A damaged or unwritten
// record is a miss after checksum verification, without poisoning its peers.
type Store struct {
	cfg        config
	root       string
	dirs       [shardCount]*os.File // segment directories, held open: segments are opened relative to them
	sched      iosched.Scheduler
	writeSlots chan uint32    // free write slots
	reads      *readSlots     // read slots
	open       sync.WaitGroup // segments holding write slots; Close waits for them
	known      uint64         // segments with ids below it were on disk at Open

	// activeSegment is the segment being written. Its lock is held to reserve
	// room in it, rotate it, and submit its operations (see append).
	activeSegment struct {
		sync.Mutex
		seg      *segment // nil before the first write, after a rotation that found no free slot, and after Close
		nextID   uint64
		closed   bool
		segments []*segment // FIFO lifecycle metadata; no record index
	}

	stats struct {
		segments        atomic.Int64
		failedSegments  atomic.Uint64
		diskBytes       atomic.Int64
		evictedSegments atomic.Uint64
		evictionErrors  atomic.Uint64
	}
}

// writeSlots is the number of write slots, and so of segments being written
// at once: the active one and those whose seal chain has not completed. A
// sealed segment holds its slot only until its writes land and the chain
// runs, milliseconds against the hundreds a segment takes to fill at device
// speed, so a few suffice; with none free, Write returns ErrBusy.
const writeSlots = 16

// Stats is a point-in-time snapshot of a Store's counters.
type Stats struct {
	Segments        int    // segment files, including those being written
	FailedSegments  uint64 // seal submissions that reported errors
	DiskBytes       int64  // retained segments plus reservations; excludes pending/failed unlinks
	EvictedSegments uint64 // logically retired segments
	EvictionErrors  uint64
}

// markerName is the file Open writes once it has created the segment
// directories. With it present, Open creates nothing.
const markerName = "BLOBSTORE"

var markerContents = fmt.Sprintf("blobstore format %d\n", formatVersion)

// Open opens the store in the directory path, which must exist. A store
// opened for the first time creates its segment directories, then a marker
// file; once the marker is there, Open only reads: it lists the segments on
// disk, so that new ones are numbered after them, and starts the I/O
// scheduler. It reads no segment: ReadIndex does.
func Open(path string, opts ...Option) (*Store, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if info, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("blobstore: %w", err)
	} else if !info.IsDir() {
		return nil, fmt.Errorf("blobstore: %s is not a directory", path)
	}
	if err := initialize(path); err != nil {
		return nil, err
	}
	st := &Store{
		cfg:        cfg,
		root:       path,
		writeSlots: make(chan uint32, writeSlots),
		reads:      newReadSlots(cfg.readHandles),
	}
	for shard := range shardCount {
		d, err := os.Open(filepath.Join(path, shardName(shard)))
		if err != nil {
			return nil, errors.Join(fmt.Errorf("blobstore: %w", err), st.closeDirs())
		}
		st.dirs[shard] = d
	}
	ids, err := st.listSegments()
	if err != nil {
		return nil, errors.Join(err, st.closeDirs())
	}
	if len(ids) > 0 {
		if ids[len(ids)-1] == math.MaxUint64 {
			return nil, errors.Join(errors.New("blobstore: segment IDs exhausted"), st.closeDirs())
		}
		st.known = ids[len(ids)-1] + 1
	}
	for _, id := range ids {
		info, err := os.Stat(segmentPath(path, id))
		if err != nil {
			return nil, errors.Join(err, st.closeDirs())
		}
		seg := &segment{id: id, size: info.Size()}
		seg.done.Store(true)
		st.activeSegment.segments = append(st.activeSegment.segments, seg)
		st.stats.diskBytes.Add(info.Size())
	}
	st.activeSegment.nextID = st.known
	st.stats.segments.Store(int64(len(ids)))
	if st.sched, err = newScheduler(cfg, writeSlots+cfg.readHandles); err != nil {
		return nil, errors.Join(fmt.Errorf("blobstore: %w", err), st.closeDirs())
	}
	for slot := range uint32(writeSlots) {
		st.writeSlots <- slot
	}
	return st, nil
}

// initialize creates the segment directories of a store opened for the first
// time, durably, then the marker. A store whose marker is present is left
// alone; one with a marker of another format is refused.
func initialize(path string) error {
	marker := filepath.Join(path, markerName)
	got, err := os.ReadFile(marker)
	switch {
	case err == nil && string(got) == markerContents:
		return nil
	case err == nil:
		return fmt.Errorf("blobstore: %s: unsupported store %q", marker, strings.TrimSpace(string(got)))
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("blobstore: %w", err)
	}
	for shard := range shardCount {
		if err := os.Mkdir(filepath.Join(path, shardName(shard)), 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("blobstore: %w", err)
		}
	}
	// The directories are durable before the marker that vouches for them.
	tmp := marker + ".tmp"
	if err := errors.Join(syncDir(path), sys.WriteFile(tmp, []byte(markerContents), 0)); err != nil {
		return fmt.Errorf("blobstore: initialize: %w", err)
	}
	if err := errors.Join(os.Rename(tmp, marker), syncDir(path)); err != nil {
		return fmt.Errorf("blobstore: initialize: %w", err)
	}
	return nil
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func (st *Store) closeDirs() error {
	var errs []error
	for _, d := range st.dirs {
		if d != nil {
			errs = append(errs, d.Close())
		}
	}
	return errors.Join(errs...)
}

func (st *Store) dirFD(id uint64) int { return int(st.dirs[id%shardCount].Fd()) }

// listSegments returns the ids of the segment files, in ascending order.
func (st *Store) listSegments() ([]uint64, error) {
	var ids []uint64
	for shard := range shardCount {
		dirents, err := os.ReadDir(filepath.Join(st.root, shardName(shard)))
		if err != nil {
			return nil, fmt.Errorf("blobstore: read segment directory: %w", err)
		}
		for _, de := range dirents {
			name, ok := strings.CutSuffix(de.Name(), extSegment)
			if !ok || de.IsDir() || len(name) != 16 {
				continue
			}
			id, err := strconv.ParseUint(name, 16, 64)
			if err != nil || id%shardCount != uint64(shard) {
				continue
			}
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}

// ReadIndex reads the footer of every segment that was on disk at Open and
// reports each record to fn in write order (segment id, then offset, so a
// later record of a key comes after an earlier one): it is how the caller
// builds its index. Record data is never read. A segment without a valid
// footer was being written when the process stopped, or failed; it is
// deleted, and losing its records costs misses, which a cache tolerates. fn is
// called with none of the store's locks held.
//
// Segments written since Open are not reported, so a caller building an index
// calls ReadIndex before writing, once. ReadIndex takes a function rather
// than returning an iterator because it must run to the end, deleting what it
// cannot index, and its errors would need a side channel.
func (st *Store) ReadIndex(fn func(KeyHash, Location)) error {
	ids, err := st.listSegments()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if id >= st.known {
			break
		}
		entries, err := st.readFooter(id)
		if errors.Is(err, fs.ErrNotExist) {
			st.forgetSegment(id)
			continue
		}
		if err != nil {
			var pathErr *os.PathError
			if errors.As(err, &pathErr) {
				return err
			}
			log().Info("deleting segment without a valid footer", "segment", id, "error", err)
			if err := os.Remove(segmentPath(st.root, id)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			st.forgetSegment(id)
			continue
		}
		for _, e := range entries {
			fn(e.hash, Location{segment: id, offset: e.off, size: e.size})
		}
	}
	st.activeSegment.Lock()
	evicted := st.evictLocked(0)
	st.activeSegment.Unlock()
	st.evict(evicted)
	return nil
}

// readFooter reads the footer of segment id through a file of its own, not a
// read slot, so that reading the index neither fills the cache nor holds every
// segment open.
func (st *Store) readFooter(id uint64) ([]footerEntry, error) {
	f, err := os.OpenFile(segmentPath(st.root, id), st.readFlags(), 0)
	if err != nil {
		return nil, err
	}
	entries, err := readSegmentFooter(f, id)
	return entries, errors.Join(err, f.Close())
}

// Close seals the active segment, waits for every seal chain to complete, and
// releases all resources; closing the scheduler closes every file in its
// slots. Close must not race other calls.
func (st *Store) Close() error {
	a := &st.activeSegment
	a.Lock()
	if a.closed {
		a.Unlock()
		return nil
	}
	a.closed = true
	if a.seg != nil {
		st.sealLocked(a.seg)
		a.seg = nil
	}
	a.Unlock()
	st.open.Wait()
	st.reads.mu.Lock()
	st.reads.closed = true
	st.reads.mu.Unlock()
	// Closing the scheduler waits for every accepted ticket.
	return errors.Join(st.sched.Close(), st.closeDirs())
}

// Stats returns a snapshot of the store's counters.
func (st *Store) Stats() Stats {
	return Stats{
		Segments:        int(st.stats.segments.Load()),
		FailedSegments:  st.stats.failedSegments.Load(),
		DiskBytes:       st.stats.diskBytes.Load(),
		EvictedSegments: st.stats.evictedSegments.Load(),
		EvictionErrors:  st.stats.evictionErrors.Load(),
	}
}

// submit passes op through the test hook, if any, and submits it, with
// whenDone run on completion.
func (st *Store) submit(op iosched.Op, whenDone func(int, error)) (iosched.Ticket, error) {
	if st.cfg.preSubmit != nil {
		op = st.cfg.preSubmit(op)
	}
	return iosched.SubmitNotify(st.sched, op, whenDone)
}

// --- Writes ---

// RecordSize returns the size of the record for a value of valueLen bytes
// under a key of keyLen bytes: the memory Write's buffer should have, and the
// size of the record's Location. With direct writes (the default) it is
// padded to a page multiple.
func (st *Store) RecordSize(keyLen, valueLen int) int {
	size := valueLen + keyLen + trailerSize
	if st.cfg.directWrites {
		return int(align.PageAlign(int64(size)))
	}
	return size
}

// Write appends buf[:n] as a record of key and returns the write's ticket. It
// does not wait for disk I/O. At capacity, it submits segment unlinks through
// dio and invokes the eviction callback outside store locks.
//
// buf is the caller's memory, lent whole until the ticket completes: Write
// frames the record in it — padding, the key and a trailer after the value —
// and writes buf[:RecordSize(len(key), n)] with one write. Like append, Write
// uses buf when it can and otherwise allocates: memory too short for the
// record, or not page-aligned for a direct write, is copied into memory Write
// allocates, at the cost of the copy. Sizing and aligning it is the caller's
// business. The caller must not modify buf before the ticket completes;
// afterwards buf[:n] still holds the value.
//
// The record's Location comes from the ticket once the write has landed; the
// record can be read there from then on. If the process stops before the
// record's segment is sealed, the record does not survive the restart.
func (st *Store) Write(key, buf []byte, n int) (Ticket, error) {
	switch {
	case len(key) == 0:
		return Ticket{}, ErrEmptyKey
	case len(key) > MaxKeyLen:
		return Ticket{}, ErrKeyTooLarge
	case n < 0 || n > len(buf):
		return Ticket{}, fmt.Errorf("blobstore: value length %d outside a %d-byte buffer", n, len(buf))
	}
	size := st.RecordSize(len(key), n)
	if int64(size)+footerSize(1) > st.cfg.segmentSize {
		return Ticket{}, ErrValueTooLarge
	}
	if len(buf) < size || (st.cfg.directWrites && !align.IsAligned(buf)) {
		rec := allocRecord(size)
		copy(rec, buf[:n])
		buf = rec
	}
	rec := buf[:size]
	tr := recordTrailer{valueLen: uint32(n), keyLen: uint16(len(key))}
	frameRecord(rec, key, tr)
	return st.append(rec, HashKey(key))
}

// append writes rec, a framed record of key hash h, at the end of the active
// segment, page-aligned for a direct write, and submits the write, all with
// the active segment's lock held: the lock orders a segment's submissions, so
// its open is accepted before its writes and its seal after them. Submit is a
// non-blocking handoff, so the section stays short.
//
// The record that leaves no room for another starts a segment's seal: it is
// written even if it runs past the segment size, and its write is chained
// with the seal (see sealOps), so the file ends past the preallocated size
// with the footer. The next record starts a new segment, whose first write is
// chained after the open of its file.
func (st *Store) append(rec []byte, h KeyHash) (Ticket, error) {
	a := &st.activeSegment
	a.Lock()
	var evicted Eviction
	defer func() {
		a.Unlock()
		st.evict(evicted)
	}()
	if a.closed {
		return Ticket{}, ErrClosed
	}
	size := int64(len(rec))
	s, off := a.seg, int64(0)
	if s == nil {
		evicted = st.evictLocked(st.cfg.segmentSize)
		var err error
		if s, err = st.startLocked(); err != nil {
			return Ticket{}, err
		}
		a.seg = s
	} else if off = s.pos; st.cfg.directWrites {
		off = align.PageAlign(off)
	}
	end := off + size
	count := len(s.entries) + 1
	last := end+footerSize(count+1) > st.cfg.segmentSize
	fileSize := s.size
	if last {
		fileSize = align.PageAlign(end) + footerSize(count)
	}
	if extra := fileSize - s.size; extra > 0 {
		evicted = append(evicted, st.evictLocked(extra)...)
		if st.cfg.maxSize != 0 && st.stats.diskBytes.Load()+extra > st.cfg.maxSize {
			return Ticket{}, ErrBusy
		}
		st.stats.diskBytes.Add(extra)
		s.size = fileSize
	}
	s.pos = end
	s.entries = append(s.entries, footerEntry{hash: h, off: uint32(off), size: uint32(size)})
	op := iosched.VWriteOp(s.slot, rec, off)
	if off == 0 {
		op = iosched.VOpenatOp(st.dirFD(s.id), segmentName(s.id), st.createFlags(), 0o644, s.slot).
			Link(iosched.VFallocateOp(s.slot, st.cfg.segmentSize), op)
	}
	var whenDone func(int, error)
	if last {
		// Every seal step runs even after an I/O error. The footer is a list of
		// candidates; read verification decides which records survived.
		op = op.HardLink(st.sealOps(s, align.PageAlign(end)))
		whenDone = func(_ int, err error) { st.sealed(s, err) }
		a.seg = nil
	}
	ticket, err := st.submit(op, whenDone)
	if err != nil {
		if last {
			st.closeRejectedSeal(s, err)
		}
		return Ticket{}, err
	}
	return Ticket{ticket: ticket, loc: Location{segment: s.id, offset: uint32(off), size: uint32(size)}}, nil
}

// startLocked starts a new segment in a free write slot; its first write
// creates the file. ErrBusy if every write slot is held by a segment still
// being sealed. Called with the active segment's lock held.
func (st *Store) startLocked() (*segment, error) {
	if st.activeSegment.nextID == math.MaxUint64 {
		return nil, errors.New("blobstore: segment IDs exhausted")
	}
	if st.cfg.maxSize != 0 && st.stats.diskBytes.Load()+st.cfg.segmentSize > st.cfg.maxSize {
		return nil, ErrBusy
	}
	var slot uint32
	select {
	case slot = <-st.writeSlots:
	default:
		return nil, ErrBusy
	}
	a := &st.activeSegment
	s := &segment{id: a.nextID, slot: slot, size: st.cfg.segmentSize}
	a.nextID++
	a.segments = append(a.segments, s)
	st.stats.diskBytes.Add(s.size)
	st.stats.segments.Add(1)
	st.open.Add(1)
	return s, nil
}

// createFlags are the open flags of a new segment file. O_EXCL because ids
// are never reused: a file already there is not ours to overwrite.
func (st *Store) createFlags() int {
	flags := os.O_CREATE | os.O_EXCL | os.O_WRONLY
	if st.cfg.directWrites {
		flags |= sys.FlDirectIO.OpenFlags()
	}
	return flags
}

// sealOps builds an immutable footer with the final record's submission.
// Close drains earlier operations; hard links ensure cleanup is attempted
// even when data, footer, or sync fails. No callback modifies the footer.
func (st *Store) sealOps(s *segment, off int64) iosched.Op {
	footer := allocRecord(int(footerSize(len(s.entries))))
	encodeSegmentFooter(footer, s.id, s.entries)
	s.entries = nil
	return iosched.VWriteOp(s.slot, footer, off).HardLink(
		iosched.VFdatasyncOp(s.slot), iosched.VCloseOp(s.slot), iosched.FsyncOp(st.dirs[s.id%shardCount]))
}

// sealLocked seals a partial segment during Close. Rotation attaches these
// operations to the final record instead, under this same write lock.
func (st *Store) sealLocked(s *segment) {
	op := st.sealOps(s, s.size-footerSize(len(s.entries)))
	done := func(_ int, err error) { st.sealed(s, err) }
	if _, err := st.submit(op, done); err != nil {
		st.closeRejectedSeal(s, err)
	}
}

// A rejected seal never reached dio, so its close cannot drain earlier writes.
// Submit a real close before releasing the slot or exposing it to eviction.
func (st *Store) closeRejectedSeal(s *segment, cause error) {
	done := func(_ int, err error) { st.sealed(s, errors.Join(cause, err)) }
	if _, err := iosched.SubmitNotify(st.sched, iosched.VCloseOp(s.slot), done); err != nil {
		// The scheduler cannot accept cleanup. Close owns the remaining handles;
		// leave this segment ineligible for eviction while releasing the waiter.
		st.stats.failedSegments.Add(1)
		st.writeSlots <- s.slot
		st.open.Done()
	}
}

// sealed only publishes completion and releases the slot. Eviction decisions
// belong to rotation, never to the scheduler completion callback.
func (st *Store) sealed(s *segment, err error) {
	if err != nil {
		st.stats.failedSegments.Add(1)
	}
	s.done.Store(true)
	st.writeSlots <- s.slot
	st.open.Done()
}

// Ticket is the completion ticket of a Write. The record's Location is not
// valid until the write has landed, so only Wait reveals it.
type Ticket struct {
	ticket iosched.Ticket
	loc    Location
}

// Wait waits for the write and returns where the record landed. On error the
// record is not readable and there is no Location. (The scheduler reports a
// short write as io.ErrShortWrite.) The ticket of a segment's last write
// completes with the segment's seal.
func (t Ticket) Wait() (Location, error) {
	if _, err := t.ticket.Wait(); err != nil {
		return Location{}, err
	}
	return t.loc, nil
}

// allocRecord returns size bytes of page-aligned, garbage-collected memory.
func allocRecord(size int) []byte {
	b := make([]byte, size+int(align.BlockSize))
	skip := int(align.PageAlign(int64(uintptr(unsafe.Pointer(unsafe.SliceData(b))))) - int64(uintptr(unsafe.Pointer(unsafe.SliceData(b)))))
	return b[skip : skip+size : skip+size]
}
