// Package blobstore stores framed records in data.db and dedicated object files,
// with their headers recorded in index.db.
package blobstore

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/miretskiy/blobcache/metrics"
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
	ErrBusy        = errors.New("blobstore: reuse overlaps an open writer")
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
	cleanup         sync.WaitGroup
	vfilesAvailable *sync.Cond
	indexAvailable  *sync.Cond

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
	st.indexAvailable = sync.NewCond(&st.mu)
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
	st.logIndexState("open")
	st.reclaimExcess()
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
		st.logIndexState("close")
		err = errors.Join(err, st.mu.index.wipeErr, st.mu.index.close())
		st.mu.index = nil
	}
	return err
}

// Called only while quiescent, so the fingerprint describes a complete index.
func (st *Store) logIndexState(event string) {
	idx := st.mu.index
	digest := sha256.New()
	for h := range idx.headers() {
		var raw RawHeader
		h.Encode(&raw)
		digest.Write(raw[:])
	}
	slog.Info("blobstore index", "event", event, "path", st.base,
		"head", idx.head, "reuse_head", idx.reuseHead, "tail", idx.tail, "slots", idx.slots,
		"data_offset", st.mu.dataAppendOffset, "data_wrap_id", idx.dataWrapID,
		"next_object_id", idx.nextObjectID, "records", idx.records.Len(), "live_bytes", idx.size(),
		"headers_sha256", fmt.Sprintf("%x", digest.Sum(nil)))
}

// Wait drains asynchronous index wipes and object cleanup.
// Stop submitting new work before using it as a shutdown barrier.
func (st *Store) Wait() { st.cleanup.Wait() }

// IOQueueStats reports the asynchronous scheduler's submission depths and
// lifetime high-water marks. Synchronous backends return false.
func (st *Store) IOQueueStats() (iosched.QueueStats, bool) {
	scheduler, ok := st.scheduler.(interface{ QueueStats() iosched.QueueStats })
	if !ok {
		return iosched.QueueStats{}, false
	}
	return scheduler.QueueStats(), true
}

// Put soft-links this writer's final data and index writes, waits for their
// shared ticket, then updates the disk index. A data failure cancels the index write.
// WithDurableIndex also waits for the index sync. Submission and completion
// errors are returned to the caller. Concurrent puts proceed independently.
// The record remains protected from reclamation until the writer is closed,
// allowing the caller to publish its cache mapping after Put returns.
// w must belong to this Store.
// Call Put once after filling w, then Close w on either success or failure.
func (st *Store) Put(w *Writer) error {
	op, err := w.finish()
	if err != nil {
		return err
	}
	h := w.Header()
	st.mu.Lock()
	indexOp := st.mu.index.writeOp(w.reservation, h)
	st.mu.Unlock()
	if st.cfg.indexDurable {
		indexOp = indexOp.Durable()
	}
	ticket, err := st.scheduler.Submit(op.Link(indexOp))
	if err != nil {
		w.err = err
		return err
	}
	_, err = ticket.Wait()
	if ticket.Slat > 0 { // Admission failures never reached the ring.
		metrics.WriteSlat.RecordValue(int64(ticket.Slat))
		metrics.WriteClat.RecordValue(int64(ticket.Clat))
	}
	if err != nil {
		w.err = err
		return err
	}
	st.mu.Lock()
	st.mu.index.publish(w.reservation, h)
	st.mu.Unlock()
	w.committed = true
	return nil
}

// Recover visits the index metadata in ObjectID order without reading data files.
// Call after Open, before using the Store. Missing or reused records are handled
// when read; no key or payload scan is needed to rebuild the lookup.
func (st *Store) Recover(visit func(RecordHeader)) {
	for h := range st.mu.index.headers() {
		visit(h)
	}
}
