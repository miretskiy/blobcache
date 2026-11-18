// Package blobstore stores framed records in a log on a bounded NVMe range.
package blobstore

import (
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/nvme"
)

var (
	// ErrRejected is returned when a scheduler declines I/O at a configured queue limit.
	ErrRejected       = iosched.ErrRejected
	ErrEmptyKey       = errors.New("blobstore: empty key")
	ErrKeyTooLarge    = errors.New("blobstore: key too large")
	ErrNotFound       = errors.New("blobstore: key not found")
	ErrInvalidRange   = errors.New("blobstore: range outside the value")
	ErrCorrupt        = errors.New("blobstore: corrupt record")
	ErrRecordTooLarge = errors.New("blobstore: record exceeds half the log")
	// ErrUnformatted means the range has no superblock for its size; Format it.
	ErrUnformatted = errors.New("blobstore: device range is not formatted for its size")
)

// Store owns a log on one NVMe range, its schedulers, and the recovered index,
// and borrows a dio allocator. A range must have one owner. Close readers and
// discard abandoned writers before closing Store; release retained buffers
// before closing the allocator.
type Store struct {
	cfg          config
	device       *nvme.Device
	geo          geometry
	segmentBytes uint64
	schedulers   []iosched.Scheduler
	disk         *iosched.DiskPolicy
	nextRead     atomic.Uint64
	nextWrite    atomic.Uint64
	memory       *alloc.Allocator

	head      atomic.Uint64 // next free log position; read without mu
	published atomic.Uint64 // payload bytes written since Open
	recovered []RecordHeader
	stopSeal  chan struct{}
	sealDone  chan struct{}

	mu struct {
		sync.Mutex
		idle        sync.Cond // outstanding reached zero
		active      *segment
		outstanding int // sealed segments and directory writes not yet finished
		directory   []uint64
		sectorBusy  []bool
		sectorDirty []bool
	}
}

// Open opens the store that Format initialized on device's range. Store opens
// its own handle to the same range; the caller retains device. memory supplies
// writer and read buffers; it must outlive Store and its readers and writers.
func Open(device *nvme.Device, memory *alloc.Allocator, opts ...Option) (_ *Store, err error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	st := &Store{cfg: cfg, memory: memory}
	st.mu.idle.L = &st.mu.Mutex
	defer func() {
		if err != nil {
			_ = st.Close()
		}
	}()
	if st.device, err = reopen(device); err != nil {
		return nil, err
	}
	if err := st.openSchedulers(); err != nil {
		return nil, err
	}
	if err := st.recover(); err != nil {
		return nil, err
	}
	st.segmentBytes = st.geo.segmentBytes()
	if cfg.sealInterval > 0 {
		st.stopSeal, st.sealDone = make(chan struct{}), make(chan struct{})
		go st.sealLoop(cfg.sealInterval)
	}
	st.logState("open")
	return st, nil
}

// reopen opens the same range through the caller's handle, even when its path
// was a symlink.
func reopen(device *nvme.Device) (*nvme.Device, error) {
	if device == nil || device.File() == nil {
		return nil, errors.New("blobstore: Open requires an open device")
	}
	path := fmt.Sprintf("/proc/self/fd/%d", device.File().Fd())
	return nvme.Open(path, nvme.WithRange(device.Offset(), device.Size()))
}

// Close seals the active segment, waits for its footer and directory writes,
// and drains every scheduler. It must not race other Store, reader, or writer
// calls.
func (st *Store) Close() (err error) {
	if st.schedulers != nil && st.geo.capacity != 0 {
		st.drainLog()
		st.logState("close")
	}
	for _, scheduler := range st.schedulers {
		err = errors.Join(err, scheduler.Close())
	}
	st.schedulers = nil
	if st.device != nil {
		err = errors.Join(err, st.device.Close())
		st.device = nil
	}
	return err
}

func (st *Store) logState(event string) {
	slog.Info("blobstore log", "event", event, "device", st.device.Info().CharDevice,
		"offset", st.device.Offset(), "capacity", st.geo.capacity, "unit", st.geo.unit,
		"head", st.head.Load(), "tail", st.geo.tail(st.head.Load()), "recovered", len(st.recovered))
}

// Records yields the records recovered at Open, oldest first.
func (st *Store) Records() iter.Seq[RecordHeader] { return slices.Values(st.recovered) }
