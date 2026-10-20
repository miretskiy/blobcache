// Package blobstore stores framed records in data.db and dedicated object files,
// with their headers recorded in index.db.
package blobstore

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/miretskiy/blobcache/internal/intrusive"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/sys"
	"github.com/zeebo/xxh3"
)

type KeyHash = xxh3.Uint128

func HashKey(key []byte) KeyHash { return xxh3.Hash128(key) }

var (
	ErrEmptyKey    = errors.New("blobstore: empty key")
	ErrKeyTooLarge = errors.New("blobstore: key too large")
	ErrNotFound    = errors.New("blobstore: key not found")
	ErrCorrupt     = errors.New("blobstore: corrupt record")
	ErrBusy        = errors.New("blobstore: reuse overlaps an unfinished write")
)

// Store owns its files and scheduler, and borrows memory and staging allocators.
// A cache directory must have one owner. Close all readers and writers before
// closing Store; release retained buffers before closing the allocators.
// Methods other than Close require an open Store.
type Store struct {
	base            string
	cfg             config
	data            *os.File
	scheduler       iosched.Scheduler
	memory, staging alloc.Allocator
	pending         sync.WaitGroup
	vfilesAvailable *sync.Cond

	// OnRetire reports a disk record leaving the index. Set before using Store.
	// Calls occur outside the store lock and may run concurrently. Resident data
	// can remain usable; ObjectID distinguishes replacements for the same key.
	OnRetire func(KeyHash, uint64)

	// Reservations change data placement and index membership together.
	mu struct {
		sync.Mutex
		dataAppendOffset uint64
		index            *diskIndex
		vfiles           [16]uint64 // 1024 occupied virtual descriptor slots
	}
}

// Open creates or opens data.db, index.db, and objects/ under path.
// memory retains inline values; staging supplies reusable object-file buffers.
// Both allocators must be initialized and outlive Store and its readers/writers;
// Store does not close them.
func Open(path string, memory, staging alloc.Allocator, opts ...Option) (_ *Store, err error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	st := &Store{base: path, cfg: cfg, memory: memory, staging: staging}
	st.vfilesAvailable = sync.NewCond(&st.mu)
	defer func() {
		if err != nil {
			_ = st.Close()
		}
	}()
	if err := os.MkdirAll(filepath.Join(path, "objects"), 0o755); err != nil {
		return nil, err
	}
	st.data, err = os.OpenFile(filepath.Join(path, "data.db"),
		os.O_CREATE|os.O_RDWR|st.fileFlags().OpenFlags(), 0o644)
	if err != nil {
		return nil, err
	}
	if cfg.directIO && runtime.GOOS == "darwin" {
		// Darwin applies cache bypass to the descriptor rather than open flags.
		if err := sys.Fadvise(st.data, 0, 0, sys.FadvDontNeed); err != nil {
			return nil, err
		}
	}
	st.mu.index, err = openDiskIndex(filepath.Join(path, "index.db"), cfg.verifyChecksums)
	if err != nil {
		return nil, err
	}
	optsIO := []iosched.Option{iosched.WithRingDepth(cfg.ringDepth), iosched.WithVFiles(1024)}
	if cfg.ioBudget == 0 {
		optsIO = append(optsIO, iosched.WithoutIOBudget())
	} else {
		optsIO = append(optsIO, iosched.WithLatencyGoal(cfg.ioBudget))
	}
	st.scheduler = iosched.NewDefaultScheduler(optsIO...)
	st.recoverIndex()
	return st, nil
}

func (st *Store) fileFlags() sys.OpenFlag {
	if st.cfg.directIO {
		return sys.FlDirectIO
	}
	return 0
}

// Close drains the scheduler before closing the files. It must not race Store
// or reader/writer methods. The caller remains responsible for the allocators.
func (st *Store) Close() (err error) {
	st.Wait()
	if st.scheduler != nil {
		err = st.scheduler.Close()
		st.scheduler = nil
	}
	if st.data != nil {
		err = errors.Join(err, st.data.Close())
		st.data = nil
	}
	if st.mu.index != nil {
		err = errors.Join(err, st.mu.index.close())
		st.mu.index = nil
	}
	return err
}

// Wait drains accepted puts and object cleanup, including publication callbacks.
// Stop submitting new work before using it as a shutdown barrier.
func (st *Store) Wait() { st.pending.Wait() }

// Put submits the final data and publishes only after its index write completes.
// publish, if non-nil, takes ownership of the inline buffer reference; external
// records pass nil. It runs on I/O completion and must not block. Late errors are
// logged and leave the record unpublished. w must belong to this Store. Call Put
// once after filling w, then Close w; neither call waits for Linux I/O completion.
func (st *Store) Put(w *Writer, publish func(RecordHeader, *alloc.Buffer)) error {
	op, err := w.finish()
	if err != nil {
		return err
	}
	h, memory, reservation := w.header, w.memory, w.reservation
	vfd := w.vfd
	h.Hash = w.key  // Index headers carry the key hash, data headers the checksum.
	memory.TryInc() // I/O owns this pin independently of the caller's writer.
	w.reservation, w.file, w.vfd = 0, nil, -1
	st.pending.Add(1)
	complete := func(err error) {
		if err != nil {
			slog.Error("put record", "object", h.ObjectID, "error", err)
			st.discard(reservation, h.Flags)
		} else {
			if publish != nil {
				var resident *alloc.Buffer
				if !h.Flags.IsExternal() {
					resident = memory // Transfer the I/O reference to the cache.
				}
				publish(h, resident)
			}
			st.release(reservation, h.Flags)
		}
		if err != nil || publish == nil || h.Flags.IsExternal() {
			memory.Unpin()
		}
		if h.Flags.IsExternal() {
			st.closeObject(vfd, h.ObjectID, err != nil)
		}
		st.pending.Done()
	}
	_, err = iosched.SubmitNotify(st.scheduler, op, func(_ int, err error) {
		if err != nil {
			complete(err)
			return
		}
		st.mu.Lock()
		op := st.mu.index.publish(reservation, h)
		st.mu.Unlock()
		if st.cfg.indexDurable {
			op = op.Durable()
		}
		_, err = iosched.SubmitNotify(st.scheduler, op, func(_ int, err error) { complete(err) })
		if err != nil {
			complete(err)
		}
	})
	if err != nil {
		complete(err)
		w.err = err
	}
	return err
}

func (st *Store) release(handle intrusive.Handle, flags RecordFlags) {
	st.mu.Lock()
	st.mu.index.records(flags).Value(handle).pending = false
	st.mu.Unlock()
}

func (st *Store) discard(handle intrusive.Handle, flags RecordFlags) {
	st.mu.Lock()
	st.mu.index.remove(handle, flags)
	st.mu.Unlock()
}

// Recover visits the index metadata in ObjectID order without reading data files.
// Call after Open, before using the Store. Missing or reused records are handled
// when read; no key or payload scan is needed to rebuild the lookup.
func (st *Store) Recover(visit func(RecordHeader)) {
	for h := range st.mu.index.headers() {
		visit(h)
	}
}
