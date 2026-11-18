package blobstore

import (
	"log/slog"
	"time"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/alloc"
	"github.com/miretskiy/dio/v2/iosched"
)

// The log is one ring of frames addressed by a monotonic position. Head is the
// next free position and tail = head - capacity; a frame is live while its
// position is at least tail, so eviction is just writing. Consecutive frames
// form loose segments. Sealing a segment reserves its footer right after them:
//
//	... record record record [footer k] record record ... [footer k+1] ...
//	    └──────── segment k ───────┘         └── segment k+1 ──┘
//
// A footer is written once every record write in its segment has finished, so
// it lists only complete records; failed or abandoned ones carry FlagDeleted.
// After the footer lands, its directory slot is set, and open finds it there.

// segment collects consecutive records until it seals.
type segment struct {
	start   uint64
	headers []RecordHeader
	pending int    // record writes not yet finished
	footer  uint64 // reserved footer position; zero while active
}

// Live reports whether h's frame is still in the log.
func (st *Store) Live(h RecordHeader) bool { return h.Position >= st.geo.tail(st.head.Load()) }

// Size reports the bytes in the live window, including padding and footers.
func (st *Store) Size() uint64 { return min(st.head.Load(), st.geo.capacity) }

// WriteStats snapshots the live window and the payload published since Open.
func (st *Store) WriteStats() (liveBytes, publishedBytes uint64) {
	return st.Size(), st.published.Load()
}

// advance reserves size bytes at head. Frames never wrap: one that does not fit
// before the end of the lap starts the next lap. Callers hold st.mu.
func (st *Store) advance(size uint64) uint64 {
	position := st.head.Load()
	if rest := st.geo.capacity - position%st.geo.capacity; size > rest {
		position += rest
	}
	st.head.Store(position + size)
	return position
}

// reserve assigns h its position in the active segment. Writer.Seal calls it;
// Put or Discard reports the write's outcome through finish.
func (st *Store) reserve(h *RecordHeader) (*segment, int, error) {
	if h.frameSize() > st.geo.capacity/2 {
		return nil, 0, ErrRecordTooLarge
	}
	st.mu.Lock()
	h.Position = st.advance(h.frameSize())
	seg := st.mu.active
	if seg == nil {
		seg = &segment{start: h.Position}
		st.mu.active = seg
	}
	slot := len(seg.headers)
	seg.headers = append(seg.headers, *h)
	seg.pending++
	var sealed *segment
	if st.head.Load()-seg.start >= st.segmentBytes {
		sealed = st.sealLocked()
	}
	st.mu.Unlock()
	st.writeFooter(sealed)
	return seg, slot, nil
}

// finish records the outcome of one reserved record's write.
func (st *Store) finish(seg *segment, i int, err error) {
	st.mu.Lock()
	if err != nil {
		seg.headers[i].Flags |= FlagDeleted
	} else {
		st.published.Add(seg.headers[i].PhysicalLen)
	}
	seg.pending--
	var ready *segment
	if seg.footer != 0 && seg.pending == 0 {
		ready = seg
	}
	st.mu.Unlock()
	st.writeFooter(ready)
}

// sealLocked closes the active segment and reserves its footer. It returns the
// segment when no record write is pending; the caller then writes the footer
// after releasing st.mu. Otherwise the last finish writes it.
func (st *Store) sealLocked() *segment {
	seg := st.mu.active
	st.mu.active = nil
	seg.footer = st.advance(footerSize(len(seg.headers)))
	st.mu.outstanding++
	if seg.pending != 0 {
		return nil
	}
	return seg
}

// sealActive seals the active segment once it spans a unit, bounding what a
// crash forgets. Closing seals any remainder, first padding head to the next
// unit so the footer cannot share a unit with the previous footer.
func (st *Store) sealActive(closing bool) {
	st.mu.Lock()
	seg := st.mu.active
	var sealed *segment
	if seg != nil {
		short := st.head.Load()-seg.start < st.geo.unit
		if closing && short {
			offset := st.head.Load() % st.geo.capacity
			st.advance(min((offset/st.geo.unit+1)*st.geo.unit, st.geo.capacity) - offset)
		}
		if closing || !short {
			sealed = st.sealLocked()
		}
	}
	st.mu.Unlock()
	st.writeFooter(sealed)
}

func (st *Store) sealLoop(interval time.Duration) {
	defer close(st.sealDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-st.stopSeal:
			return
		case <-ticker.C:
			st.sealActive(false)
		}
	}
}

// writeFooter writes a sealed segment's footer.
func (st *Store) writeFooter(seg *segment) {
	if seg == nil {
		return
	}
	size := footerSize(len(seg.headers))
	memory := alloc.NewMemory(int64(size))
	page := memory.Bytes()[:size]
	encodeFooter(page, seg.footer, seg.start, seg.headers)
	op := iosched.NVMeWriteOp(st.device, page, st.geo.offset(seg.footer)).MustAdmit()
	_, err := iosched.SubmitNotify(st.writeScheduler(), op, func(_ int, err error) {
		memory.Unpin()
		if err == nil {
			st.setSlot(seg.footer)
		} else {
			slog.Warn("blobstore: footer write failed", "position", seg.footer, "error", err)
		}
		st.done()
	})
	if err != nil {
		memory.Unpin()
		slog.Warn("blobstore: footer write failed", "position", seg.footer, "error", err)
		st.done()
	}
}

// setSlot points a footer's directory slot at it. Each directory sector has at
// most one write in flight; an update during that write marks the sector dirty
// and the completion writes it again, so the newest values always land last.
func (st *Store) setSlot(position uint64) {
	i := st.geo.slot(position)
	sector := i / slotsPerSector
	st.mu.Lock()
	st.mu.directory[i] = position
	if st.mu.sectorBusy[sector] {
		st.mu.sectorDirty[sector] = true
		st.mu.Unlock()
		return
	}
	st.mu.sectorBusy[sector] = true
	st.mu.outstanding++
	st.mu.Unlock()
	st.writeSector(sector)
}

func (st *Store) writeSector(sector int) {
	memory := alloc.NewMemory(align.BlockSize)
	page := memory.Bytes()[:align.BlockSize]
	st.mu.Lock()
	encodeDirectory(page, st.mu.directory[sector*slotsPerSector:][:slotsPerSector])
	st.mu.Unlock()
	written := func(err error) {
		memory.Unpin()
		if err != nil {
			slog.Warn("blobstore: directory write failed", "sector", sector, "error", err)
		}
		st.mu.Lock()
		again := err == nil && st.mu.sectorDirty[sector]
		st.mu.sectorDirty[sector] = false
		st.mu.sectorBusy[sector] = again
		st.mu.Unlock()
		if again {
			st.writeSector(sector)
			return
		}
		st.done()
	}
	op := iosched.NVMeWriteOp(st.device, page, int64(superblockSize)+int64(sector)*align.BlockSize).MustAdmit()
	if _, err := iosched.SubmitNotify(st.writeScheduler(), op, func(_ int, err error) { written(err) }); err != nil {
		written(err)
	}
}

// done retires one sealed segment or directory write.
func (st *Store) done() {
	st.mu.Lock()
	st.mu.outstanding--
	if st.mu.outstanding == 0 {
		st.mu.idle.Broadcast()
	}
	st.mu.Unlock()
}

// drainLog seals the remainder and waits for every footer and directory write.
// Writers must have been put or discarded.
func (st *Store) drainLog() {
	if st.stopSeal != nil {
		close(st.stopSeal)
		<-st.sealDone
		st.stopSeal = nil
	}
	st.sealActive(true)
	st.mu.Lock()
	for st.mu.outstanding != 0 {
		st.mu.idle.Wait()
	}
	st.mu.Unlock()
}
