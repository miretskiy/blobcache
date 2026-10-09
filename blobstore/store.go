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
// WithMaxSize or WithMaxSegments enables whole-segment FIFO eviction.
// Notifications list retired IDs, without knowing the caller's index organization.
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
	// error is also a *ChecksumError.
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
// capacity eviction submits unlinks through dio outside store locks. Each
// queue owns one scheduler and local descriptor table. Write-capable queues
// have an append stream; read-capable queues have a read-handle cache.
// Keys select writers, while segment IDs select readers.
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
	cfg     config
	root    string
	dirs    [shardCount]*os.File // held until every scheduler closes
	queues  []*ioQueue           // one scheduler and local virtual table each
	writers []*ioQueue           // key hash selects an append stream
	readers []*ioQueue           // segment ID selects a read-handle cache
	open    sync.WaitGroup       // write segments whose seal has not completed
	known   uint64               // segments below this ID existed at Open

	// The common registry receives segment creation, growth and completion
	// from all producers. Ordinary record appends do not acquire its mutex.
	segments struct {
		sync.Mutex
		nextID uint64
		closed bool
		fifo   []*segment
	}

	stats struct {
		segments        atomic.Int64
		failedSegments  atomic.Uint64
		diskBytes       atomic.Int64
		evictedSegments atomic.Uint64
		evictionErrors  atomic.Uint64
	}
}

// writeSlots bounds segment files being written or sealed by each producer.
// A slot returns after its seal chain completes; with none free, Write returns
// ErrBusy. Queued writes retain their slots until they actually reach the disk.
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
	cpus, err := queueCPUs(cfg)
	if err != nil {
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
	st := &Store{cfg: cfg, root: path}
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
		st.segments.fifo = append(st.segments.fifo, seg)
		st.stats.diskBytes.Add(info.Size())
	}
	st.segments.nextID = st.known
	st.stats.segments.Store(int64(len(ids)))
	if err := st.startQueues(cpus); err != nil {
		return nil, errors.Join(fmt.Errorf("blobstore: %w", err), st.closeQueues(), st.closeDirs())
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
// reports records to fn in segment/offset order (for any particular key, a
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
	st.segments.Lock()
	evicted := st.evictLocked(0, 0)
	st.segments.Unlock()
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

// Close seals every active segment, waits for all seals, then closes all
// schedulers and directory descriptors. Close must not race other calls.
func (st *Store) Close() error {
	st.segments.Lock()
	if st.segments.closed {
		st.segments.Unlock()
		return nil
	}
	st.segments.closed = true
	st.segments.Unlock()
	for _, q := range st.writers {
		q.active.Lock()
		q.active.closed = true
		if s := q.active.seg; s != nil {
			q.sealLocked(s)
			q.active.seg = nil
		}
		q.active.Unlock()
	}
	st.open.Wait()
	for _, q := range st.readers {
		q.reads.mu.Lock()
		q.reads.closed = true
		q.reads.mu.Unlock()
	}
	return errors.Join(st.closeQueues(), st.closeDirs())
}

func (st *Store) closeQueues() error {
	var err error
	for _, q := range st.queues {
		err = errors.Join(err, q.sched.Close())
	}
	return err
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
// The ticket exposes the reserved Location immediately. Wait confirms that
// the write landed; until then a verified read there may miss. If the process
// stops before the record's segment is sealed, it does not survive the restart.
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
	h := HashKey(key)
	return st.writers[h.Lo%uint64(len(st.writers))].append(rec, h)
}

// Ticket is the completion ticket of a Write.
type Ticket struct {
	ticket iosched.Ticket
	loc    Location
}

// Location returns the reserved location, even before I/O completes. Reads
// there may miss until the record lands; callers must verify the record and
// retain the write buffer until Wait returns.
func (t Ticket) Location() Location { return t.loc }

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
