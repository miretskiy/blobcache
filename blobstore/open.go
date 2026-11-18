package blobstore

import (
	"encoding/binary"
	"errors"

	"github.com/miretskiy/dio/v2/align"
	"github.com/miretskiy/dio/v2/iosched"
	"github.com/miretskiy/dio/v2/nvme"
)

// Format initializes a store on device's range: a superblock for the range's
// size and an empty directory. Previous frames become unreachable without
// being cleared. Close every user of the range first.
func Format(device *nvme.Device) (err error) {
	geo := fitGeometry(device.Size())
	if geo.capacity < 16*align.BlockSize {
		return errors.New("blobstore: device range is too small")
	}
	scheduler, err := iosched.NewURingScheduler(iosched.WithNVMeCommands(device), iosched.WithRingDepth(32),
		iosched.WithDiskPolicy(iosched.NewDiskPolicy(iosched.WithoutIOBudget())))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, scheduler.Close()) }()
	zero := align.AllocAligned(1 << 20)
	defer align.FreeAligned(zero)
	var ops []iosched.Op
	for offset := uint64(0); offset < geo.dirBytes; offset += uint64(len(zero)) {
		n := min(uint64(len(zero)), geo.dirBytes-offset)
		ops = append(ops, iosched.NVMeWriteOp(device, zero[:n], int64(superblockSize+offset)))
	}
	if err := waitAll(scheduler, ops); err != nil {
		return err
	}
	// The superblock lands after the empty directory it describes.
	page := align.AllocAligned(superblockSize)
	defer align.FreeAligned(page)
	geo.encode(page)
	return waitAll(scheduler, []iosched.Op{iosched.NVMeWriteOp(device, page, 0)})
}

// waitAll submits ops together, then waits for all of them.
func waitAll(scheduler iosched.Scheduler, ops []iosched.Op) error {
	tickets := make([]iosched.Ticket, 0, len(ops))
	var err error
	for _, op := range ops {
		ticket, submitErr := scheduler.Submit(op)
		if submitErr != nil {
			err = errors.Join(err, submitErr)
			continue
		}
		tickets = append(tickets, ticket)
	}
	for _, ticket := range tickets {
		_, waitErr := ticket.Wait()
		err = errors.Join(err, waitErr)
	}
	return err
}

// recover checks the superblock, then rebuilds head and the index from the
// footers that directory slots name.
func (st *Store) recover() error {
	page := align.AllocAligned(superblockSize)
	defer align.FreeAligned(page)
	if err := waitAll(st.readScheduler(), []iosched.Op{iosched.NVMeReadOp(st.device, page, 0)}); err != nil {
		return err
	}
	geo, ok := decodeGeometry(page)
	if !ok || geo != fitGeometry(st.device.Size()) {
		return ErrUnformatted
	}
	st.geo = geo
	slots, err := st.readDirectory()
	if err != nil {
		return err
	}
	footers, err := st.readFooters(slots)
	if err != nil {
		return err
	}
	head, records := rebuild(geo, slots, footers)
	st.head.Store(head)
	st.recovered = records
	st.mu.directory = slots
	st.mu.sectorBusy = make([]bool, len(slots)/slotsPerSector)
	st.mu.sectorDirty = make([]bool, len(slots)/slotsPerSector)
	return nil
}

func (st *Store) readDirectory() ([]uint64, error) {
	dir := align.AllocAligned(int64(st.geo.dirBytes))
	defer align.FreeAligned(dir)
	var ops []iosched.Op
	for offset := 0; offset < len(dir); offset += 1 << 20 {
		chunk := dir[offset:min(len(dir), offset+1<<20)]
		ops = append(ops, iosched.NVMeReadOp(st.device, chunk, int64(superblockSize+offset)))
	}
	if err := waitAll(st.readScheduler(), ops); err != nil {
		return nil, err
	}
	slots := make([]uint64, st.geo.slots())
	for i := range slots {
		slots[i] = binary.LittleEndian.Uint64(dir[i*8:])
	}
	return slots, nil
}

// readFooters reads the footer each nonzero slot names, a batch at a time:
// the batch's first pages together, then together the rest of every footer
// larger than a page.
func (st *Store) readFooters(slots []uint64) ([]footer, error) {
	const batch = 256
	var named []int
	for i, position := range slots {
		if position != 0 {
			named = append(named, i)
		}
	}
	pages := align.AllocAligned(batch * align.BlockSize)
	defer align.FreeAligned(pages)
	frames := make([][]byte, batch)
	var footers []footer
	for len(named) != 0 {
		n := min(batch, len(named))
		ops := make([]iosched.Op, 0, n)
		for j, i := range named[:n] {
			frames[j] = pages[j*align.BlockSize:][:align.BlockSize]
			ops = append(ops, iosched.NVMeReadOp(st.device, frames[j], st.geo.offset(slots[i])))
		}
		if err := waitAll(st.readScheduler(), ops); err != nil {
			return nil, err
		}
		// A footer claiming more than one page, within its lap, needs the rest.
		ops = ops[:0]
		for j, i := range named[:n] {
			size := footerFrameSize(frames[j])
			if size <= align.BlockSize || slots[i]%st.geo.capacity+size > st.geo.capacity {
				continue
			}
			frame := align.AllocAligned(int64(size))
			copy(frame, frames[j])
			frames[j] = frame
			ops = append(ops, iosched.NVMeReadOp(st.device, frame[align.BlockSize:], st.geo.offset(slots[i])+align.BlockSize))
		}
		err := waitAll(st.readScheduler(), ops)
		for j, i := range named[:n] {
			if err == nil {
				if f, ok := parseFooter(st.geo, i, slots[i], frames[j]); ok {
					footers = append(footers, f)
				}
			}
			if len(frames[j]) > align.BlockSize {
				align.FreeAligned(frames[j])
			}
		}
		if err != nil {
			return nil, err
		}
		named = named[n:]
	}
	return footers, nil
}
