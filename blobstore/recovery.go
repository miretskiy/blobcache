package blobstore

import (
	"cmp"
	"encoding/binary"
	"hash/crc32"
	"slices"

	"github.com/miretskiy/dio/v2/align"
)

// footer is a validated footer: where it is, its footprint, and its records.
type footer struct {
	position uint64
	size     uint64
	records  []RecordHeader
}

// encodeFooter fills a footer frame of footerSize(len(headers)) bytes: its own
// header, then one header per segment record in the records' index view.
// Position is the footer's; LogicalLen is the segment start; Hash is the
// CRC32C of the record headers.
func encodeFooter(page []byte, position, start uint64, headers []RecordHeader) {
	clear(page)
	for i := range headers {
		headers[i].Encode((*RawHeader)(page[(i+1)*RecordHeaderSize:]))
	}
	entries := page[RecordHeaderSize : (len(headers)+1)*RecordHeaderSize]
	footer := RecordHeader{Flags: FlagFooter | FlagHeaderIntegrity, Position: position,
		PhysicalLen: uint64(len(entries)), LogicalLen: start, Hash: uint64(crc32.Checksum(entries, crcTable))}
	footer.Encode((*RawHeader)(page))
}

// footerFrameSize reads the footprint a footer's first page claims, before
// validation; zero when the page holds no footer header.
func footerFrameSize(first []byte) uint64 {
	var h RecordHeader
	if (*RawHeader)(first).Decode(&h, false) != nil || !h.Flags.IsFooter() || h.PhysicalLen%RecordHeaderSize != 0 {
		return 0
	}
	return footerSize(int(h.PhysicalLen / RecordHeaderSize))
}

// parseFooter validates the frame that directory slot names at position: the
// header CRC and footer flag, the footer's own position and unit, a footprint
// within one lap, and the CRC over its record headers. Records keep their
// index view; failed writes and records outside the segment are dropped.
func parseFooter(g geometry, slot int, position uint64, frame []byte) (footer, bool) {
	var h RecordHeader
	if len(frame) < RecordHeaderSize || (*RawHeader)(frame).Decode(&h, true) != nil {
		return footer{}, false
	}
	if !h.Flags.IsFooter() || !h.Flags.HasHeaderIntegrity() || h.Position != position ||
		g.slot(position) != slot || h.PhysicalLen%RecordHeaderSize != 0 {
		return footer{}, false
	}
	n := int(h.PhysicalLen / RecordHeaderSize)
	size := footerSize(n)
	if position%g.capacity+size > g.capacity || uint64(len(frame)) < size {
		return footer{}, false
	}
	entries := frame[RecordHeaderSize : RecordHeaderSize+h.PhysicalLen]
	if uint64(crc32.Checksum(entries, crcTable)) != h.Hash {
		return footer{}, false
	}
	f := footer{position: position, size: size, records: make([]RecordHeader, 0, n)}
	for i := range n {
		var r RecordHeader
		if (*RawHeader)(entries[i*RecordHeaderSize:]).Decode(&r, false) != nil {
			return footer{}, false
		}
		if r.Flags.IsDeleted() || r.Flags.IsFooter() || r.KeyLen == 0 ||
			r.Position < h.LogicalLen || r.Position+r.frameSize() > position {
			continue
		}
		f.records = append(f.records, r)
	}
	return f, true
}

// rebuild derives head from the slots and valid footers, then keeps the
// records at or after tail, oldest first. Every nonzero slot names a footer
// position that was once written, so head never falls below one even when
// that footer no longer validates: positions are never reused for new frames.
func rebuild(g geometry, slots []uint64, footers []footer) (uint64, []RecordHeader) {
	var head uint64
	for _, position := range slots {
		if position != 0 {
			head = max(head, position+align.BlockSize)
		}
	}
	for _, f := range footers {
		head = max(head, f.position+f.size)
	}
	tail := g.tail(head)
	var records []RecordHeader
	for _, f := range footers {
		for _, r := range f.records {
			if r.Position >= tail {
				records = append(records, r)
			}
		}
	}
	slices.SortFunc(records, func(a, b RecordHeader) int { return cmp.Compare(a.Position, b.Position) })
	return head, records
}

// encodeDirectory writes slots into a directory image, little endian.
func encodeDirectory(dir []byte, slots []uint64) {
	for i, position := range slots {
		binary.LittleEndian.PutUint64(dir[i*8:], position)
	}
}
