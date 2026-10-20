package blobstore

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/miretskiy/blobcache/internal/intrusive"
	"github.com/miretskiy/dio/v2/iosched"
	"golang.org/x/sys/unix"
)

// Size reports indexed payload bytes. It excludes framing, padding, index
// storage, unfinished writers, and objects left behind by failed unlinks.
func (st *Store) Size() uint64 { return st.mu.index.size() }

// WriteStats snapshots live indexed payload and cumulative payload published
// since Open. Reclamation reduces liveBytes but never publishedBytes.
func (st *Store) WriteStats() (liveBytes, publishedBytes uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.mu.index.size(), st.mu.index.publishedBytes
}

// TargetSize is the live-payload target for reclamation policy. Writes may
// exceed it until reclamation runs; data.db retains allocated blocks on wrap.
func (st *Store) TargetSize() uint64 { return uint64(st.cfg.targetSize) }

// Reclaim evicts external objects first, then inline records, toward bytes of
// indexed payload. Age and popularity do not protect external objects. Whole
// records may overshoot the request. An unfinished reservation ends the attempt.
// Eviction leaves both append positions unchanged. Inline eviction does not
// release filesystem blocks; later writes reuse them when data.db wraps.
// Retirements are reported and external unlinks submitted after unlocking.
// Unoverwritten index records can return after reopening.
func (st *Store) Reclaim(bytes uint64) (reclaimed uint64) {
	var retired []RecordHeader
	st.mu.Lock()
retire:
	for _, flags := range []RecordFlags{FlagExternal, 0} {
		records := st.mu.index.records(flags)
		for first, ok := records.Front(); ok && reclaimed < bytes; first, ok = records.Front() {
			if records.Value(first).pending {
				break retire
			}
			h := st.mu.index.remove(first, flags)
			reclaimed += h.PhysicalLen
			retired = append(retired, h)
		}
	}
	st.mu.Unlock()
	st.notifyRetired(retired)
	return reclaimed
}

// reserve assigns an index slot and data extent. Only inline allocation rotates
// data.db; eviction and external writes never move its append offset. The target
// bounds a lap, except that a single larger inline record gets the entire file.
// Overlapping records are retired here even if background eviction has not run.
// TODO: coordinate overlapping readers when the ring catches them.
func (st *Store) reserve(h *RecordHeader, inlineSize uint64) (intrusive.Handle, error) {
	var retired []RecordHeader
	st.mu.Lock()
	defer func() {
		st.mu.Unlock()
		st.notifyRetired(retired)
	}()
	start := st.mu.dataAppendOffset
	if inlineSize != 0 {
		if start+inlineSize > max(st.TargetSize(), inlineSize) {
			start = 0
		}
		end := start + inlineSize
		idx := st.mu.index
		for first, covered := idx.coveredInline(start, end, st.mu.dataAppendOffset); covered; first, covered = idx.coveredInline(start, end, st.mu.dataAppendOffset) {
			if idx.inline.Value(first).pending {
				return 0, ErrBusy
			}
			retired = append(retired, idx.remove(first, 0))
		}
		st.mu.dataAppendOffset = end
	}
	h.Offset = start
	handle := st.mu.index.reserve(start, h.Flags)
	h.ObjectID = st.mu.index.records(h.Flags).Value(handle).header.ObjectID
	return handle, nil
}

// notifyRetired runs without Store.mu. Unlink completion only logs failures;
// neither foreground reservations nor the evictor wait for filesystem cleanup.
func (st *Store) notifyRetired(records []RecordHeader) {
	for _, h := range records {
		if st.OnRetire != nil {
			st.OnRetire(h.Hash, h.ObjectID)
		}
		if h.Flags.IsExternal() {
			path := filepath.Join(st.base, "objects", strconv.FormatUint(h.ObjectID, 10))
			_, err := iosched.SubmitNotify(st.scheduler, iosched.UnlinkatOp(unix.AT_FDCWD, path),
				func(_ int, err error) { logUnlinkError(h.ObjectID, err) })
			logUnlinkError(h.ObjectID, err)
		}
	}
}

func logUnlinkError(id uint64, err error) {
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Error("unlink object", "object", id, "error", err)
	}
}

// recoverIndex restores data placement from the newest retained reservation.
func (st *Store) recoverIndex() {
	var newestID uint64
	for _, flags := range []RecordFlags{FlagExternal, 0} {
		records := st.mu.index.records(flags)
		if handle, ok := records.Back(); ok {
			h := records.Value(handle).header
			if h.ObjectID > newestID {
				newestID = h.ObjectID
				st.mu.dataAppendOffset = h.dataEnd()
			}
		}
	}
}
