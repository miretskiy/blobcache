// Package blobstore stores framed records in data.db and dedicated object files,
// with their headers recorded in index.db.
package blobstore

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/sys"
	"github.com/zeebo/xxh3"
)

// KeyHash identifies a lookup candidate; readers always compare the full key.
type KeyHash = uint64

// HashKey returns the XXH3-64 hash of key, stable across restarts for
// persisted index lookups. Records carry FlagKeyHashXXH3.
func HashKey(key []byte) KeyHash { return xxh3.Hash(key) }

var (
	// ErrRejected is returned when a scheduler declines I/O at a configured queue limit.
	ErrRejected    = iosched.ErrRejected
	ErrEmptyKey    = errors.New("blobstore: empty key")
	ErrKeyTooLarge = errors.New("blobstore: key too large")
	ErrNotFound    = errors.New("blobstore: key not found")
	ErrCorrupt     = errors.New("blobstore: corrupt record")
	ErrBusy        = errors.New("blobstore: reuse overlaps an open writer")
	// ErrNoDescriptors means a scheduler's virtual descriptor table is full:
	// open external readers and writers each hold a slot. It wraps ENFILE.
	ErrNoDescriptors = errors.New("blobstore: virtual file descriptors exhausted")
)

// Store owns its files and schedulers, and borrows a dio allocator.
// A cache directory must have one owner. Close all readers and writers before
// closing Store; release retained buffers before closing the allocators.
type Store struct {
	base       string
	cfg        config
	data       *os.File
	schedulers []iosched.Scheduler
	disk       *iosched.DiskPolicy
	nextRead   atomic.Uint64
	nextWrite  atomic.Uint64
	memory     *alloc.Allocator

	// Reservations change data placement and index membership together.
	mu struct {
		sync.Mutex
		dataAppendOffset uint64
		index            *diskIndex
	}
}

// Open creates or opens data.db, index.db, and objects/ under path.
// memory supplies writer buffers and disk-read staging. It must outlive Store
// and its readers/writers; Store does not close it.
func Open(path string, memory *alloc.Allocator, opts ...Option) (_ *Store, err error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	st := &Store{base: path, cfg: cfg, memory: memory}
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
	st.mu.index, err = loadDiskIndex(filepath.Join(path, "index.db"), cfg.verifyChecksums)
	if err != nil {
		return nil, err
	}
	if err := st.removeOrphans(); err != nil {
		return nil, err
	}
	if err := st.openSchedulers(); err != nil {
		return nil, err
	}
	st.recoverIndex()
	st.logIndexState("open")
	return st, nil
}

func (st *Store) fileFlags() sys.OpenFlag {
	if st.cfg.directIO {
		return sys.FlDirectIO
	}
	return 0
}

// Close drains every scheduler before
// saving the index. It must not race other Store, reader, or writer calls.
func (st *Store) Close() (err error) {
	for _, scheduler := range st.schedulers {
		err = errors.Join(err, scheduler.Close())
	}
	st.schedulers = nil
	if st.data != nil {
		err = errors.Join(err, st.data.Close())
		st.data = nil
	}
	if st.mu.index != nil {
		st.logIndexState("close")
		err = errors.Join(err, st.mu.index.save(filepath.Join(st.base, "index.db")))
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
		"data_offset", st.mu.dataAppendOffset, "data_wrap_id", idx.dataWrapID,
		"next_object_id", idx.nextObjectID, "records", idx.records.Len(), "live_bytes", idx.size(), "published_bytes", idx.publishedBytes,
		"headers_sha256", fmt.Sprintf("%x", digest.Sum(nil)))
}

// Records yields published records in reservation order. It must not race
// writers.
func (st *Store) Records() iter.Seq[RecordHeader] { return st.mu.index.headers() }
