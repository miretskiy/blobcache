package blobstore

import (
	"errors"
	"log/slog"
	"os"

	"github.com/miretskiy/blobcache/metrics"
	"github.com/miretskiy/dio/v2/collections"
	"github.com/miretskiy/dio/v2/iosched"
	"golang.org/x/sys/unix"
)

// Size reports indexed payload bytes. It excludes framing, padding, index
// storage, uncommitted writes, and objects left behind by failed unlinks.
func (st *Store) Size() uint64 { return st.mu.index.size() }

// WriteStats snapshots live indexed payload and cumulative payload published
// since Open. Reclamation reduces liveBytes but never publishedBytes.
func (st *Store) WriteStats() (liveBytes, publishedBytes uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.mu.index.size(), st.mu.index.publishedBytes
}

// TargetSize is the live-payload target. Open writers can temporarily exceed
// it; closing them reclaims the excess. data.db retains allocated blocks on wrap.
func (st *Store) TargetSize() uint64 { return uint64(st.cfg.targetSize) }

func (st *Store) excess() uint64 {
	if size, target := st.Size(), st.TargetSize(); size > target {
		return size - target
	}
	return 0
}

// Closing a writer releases its pending reservation, which may have blocked
// the FIFO. Drain excess payload then, including final sizes of streamed values.
// Each pass submits its I/O to dio; further passes wait for the one active wipe.
func (st *Store) reclaimExcess() {
	st.mu.Lock()
	defer st.mu.Unlock()
	idx := st.mu.index
	for {
		bytes := st.excess()
		if bytes == 0 || idx.wipeErr != nil {
			return
		}
		if idx.head != idx.reuseHead {
			st.indexAvailable.Wait()
			continue // Another writer may already have removed the excess.
		}
		batch := st.reclaimLocked(bytes, 0, 0)
		if batch.end == 0 {
			return
		}
		st.mu.Unlock()
		st.submitReclaim(batch)
		st.mu.Lock()
	}
}

// Bound both the mutex hold and the immutable wipe buffer.
const reclaimSlots = 4096

// indexReclaim is one reclamation request, prepared under Store.mu and
// submitted after releasing it.
type indexReclaim struct {
	// op wipes retired index slots using its own immutable header buffer.
	op iosched.Op
	// end is the exclusive logical slot boundary released by a successful wipe;
	// zero means this request is empty.
	end uint64
	// bytes counts retired payload from both inline and external records.
	bytes uint64
	// records holds copies of retired headers for callbacks and external-file
	// unlink requests. The original index entries have already been removed.
	records []RecordHeader
}

// Reclaim evicts an oldest prefix, regardless of storage kind. Each pass is
// bounded by reclaimSlots; whole records may overshoot the byte request.
// An unfinished writer ends the pass. Concurrent evictors wait for the current
// index wipe; ordinary reservations wait only when they need protected slots.
func (st *Store) Reclaim(bytes uint64) uint64 {
	if bytes == 0 {
		return 0
	}
	for {
		st.mu.Lock()
		idx := st.mu.index
		for idx.head != idx.reuseHead && idx.wipeErr == nil {
			st.indexAvailable.Wait()
		}
		if idx.wipeErr != nil {
			st.mu.Unlock()
			return 0
		}
		batch := st.reclaimLocked(bytes, 0, 0)
		st.mu.Unlock()
		if batch.end == 0 {
			return 0
		}
		st.submitReclaim(batch)
		if batch.bytes != 0 {
			return batch.bytes
		}
		// A batch of abandoned slots freed no payload. Continue to the live prefix.
	}
}

// reclaimLocked sweeps one FIFO prefix for payload, index slots, and inline
// placement together. A pending writer stops the sweep; head != reuseHead
// excludes another batch. External records in the prefix are retired too.
func (st *Store) reclaimLocked(bytes, until, inlineSize uint64) indexReclaim {
	idx := st.mu.index
	start := idx.head
	limit := min(idx.tail, start+reclaimSlots)
	dataStart := st.dataStart(inlineSize)
	var batch indexReclaim
	for idx.head < limit {
		if batch.bytes >= bytes && idx.head >= until &&
			(inlineSize == 0 || !idx.covered(dataStart+inlineSize, dataStart < st.mu.dataAppendOffset)) {
			break
		}
		first, ok := idx.records.Front()
		if !ok {
			idx.head = limit
			break
		}
		entry := idx.records.Value(first)
		position := idx.position(entry.slot)
		if position > idx.head {
			idx.head = min(position, limit)
			continue
		}
		if entry.pending {
			break
		}
		idx.head++
		h := idx.remove(first)
		batch.bytes += h.PhysicalLen
		batch.records = append(batch.records, h)
	}
	if idx.head != start {
		batch.end = idx.head
		batch.op = idx.wipeOp(start, batch.end)
		st.cleanup.Add(1)
	}
	return batch
}

// submitReclaim runs outside mu. Only the completed wipe releases index slots;
// external unlinks address unique ObjectIDs and do not gate their reuse.
func (st *Store) submitReclaim(batch indexReclaim) {
	st.notifyRetired(batch.records)
	complete := func(_ int, err error) {
		st.mu.Lock()
		idx := st.mu.index
		if err == nil {
			idx.reuseHead = batch.end
		} else {
			idx.wipeErr = err
		}
		st.indexAvailable.Broadcast()
		st.mu.Unlock()
		if err != nil {
			slog.Error("wipe index", "error", err)
		}
		st.cleanup.Done()
	}
	// Retired slots cannot be reused until this wipe completes. Its admission
	// bypasses admission limits and accounting so overload cannot prevent reclamation.
	if _, err := iosched.SubmitNotify(st.writeScheduler(), batch.op.MustAdmit(), complete); err != nil {
		complete(0, err)
	}
}

// reserve assigns an index slot and inline placement together. A data overlap
// evicts the whole FIFO prefix through that inline record. Active readers do
// not pin disk locations; unfinished writers still prevent their reuse.
func (st *Store) reserve(h *RecordHeader, inlineSize uint64) (collections.Handle, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	idx := st.mu.index
	for {
		if idx.wipeErr != nil {
			return 0, idx.wipeErr
		}
		start := st.dataStart(inlineSize)
		var until uint64
		if idx.full() && !idx.growable() {
			until = idx.head + 1
		}
		needsSpace := until != 0 || inlineSize != 0 && idx.covered(start+inlineSize, start < st.mu.dataAppendOffset)
		bytes := st.excess()
		if needsSpace || bytes != 0 && idx.head == idx.reuseHead {
			if idx.head != idx.reuseHead {
				st.indexAvailable.Wait()
				continue
			}
			batch := st.reclaimLocked(bytes, until, inlineSize)
			if batch.end == 0 && needsSpace {
				return 0, ErrBusy
			}
			if batch.end != 0 {
				st.mu.Unlock()
				st.submitReclaim(batch)
				st.mu.Lock()
				// Other reservations may have advanced placement while unlocked.
				continue
			}
			// Pending writers may defer the byte target, but never permit overlap.
		}
		if inlineSize != 0 {
			if start < st.mu.dataAppendOffset {
				idx.dataWrapID = idx.nextObjectID
			}
			st.mu.dataAppendOffset = start + inlineSize
		}
		h.Offset = start
		handle := idx.reserve(start, h.Flags)
		h.ObjectID = idx.records.Value(handle).header.ObjectID
		return handle, nil
	}
}

func (st *Store) dataStart(size uint64) uint64 {
	start := st.mu.dataAppendOffset
	if size != 0 && start+size > max(st.TargetSize(), size) {
		return 0
	}
	return start
}

// notifyRetired runs without Store.mu. Resident copies may survive disk
// eviction; callbacks compare ObjectIDs so replacements remain untouched.
func (st *Store) notifyRetired(records []RecordHeader) {
	for _, h := range records {
		if st.OnRetire != nil {
			st.OnRetire(h.KeyHash(), h.ObjectID)
		}
		if h.Flags.IsExternal() {
			st.cleanup.Add(1)
			_, err := iosched.SubmitNotify(st.writeScheduler(), iosched.UnlinkatOp(unix.AT_FDCWD, st.objectPath(h.ObjectID)),
				func(_ int, err error) {
					logUnlinkError(h.ObjectID, err)
					st.cleanup.Done()
				})
			if err != nil {
				logUnlinkError(h.ObjectID, err)
				st.cleanup.Done()
			}
		}
	}
}

func logUnlinkError(id uint64, err error) {
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		metrics.UnlinkErrors.Add(1)
		slog.Error("unlink object", "object", id, "error", err)
	}
}

// recoverIndex restores data placement from the newest retained reservation.
func (st *Store) recoverIndex() {
	idx := st.mu.index
	if newest, ok := idx.records.Back(); ok {
		st.mu.dataAppendOffset = idx.records.Value(newest).header.dataEnd()
	}
}
