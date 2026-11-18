package blobstore

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/miretskiy/dio/v2/align"
)

// The store occupies one bounded NVMe range:
//
//	0          4 KiB              dataStart                       dataStart+capacity
//	[superblock][directory .......][log: frames at position mod capacity .........]
//
// The superblock page holds static geometry, written once by format. The
// directory has one 8-byte slot per unit of the log. A slot holds the position
// of the newest footer that starts in its unit; zero means none. Head and tail
// are never stored: open derives them from the footers the slots name.
//
// Superblock page layout, little endian:
//
//	[ 0: 8) magic "BLOBLOG1"
//	[ 8:16) log capacity in bytes
//	[16:24) unit in bytes
//	[24:32) directory bytes
//	[32:40) data start
//	[40:44) CRC32C of [0:40)

const (
	superblockSize = align.BlockSize
	slotsPerSector = align.BlockSize / 8
	maxUnit        = 4 << 20
	maxSegment     = 64 << 20
)

var superblockMagic = [8]byte{'B', 'L', 'O', 'B', 'L', 'O', 'G', '1'}

// geometry places the directory and the log on a device.
type geometry struct {
	capacity  uint64 // log bytes; positions wrap at this size
	unit      uint64 // directory granularity
	dirBytes  uint64
	dataStart uint64
}

// newGeometry lays out a log of capacity bytes, rounded down to a page. Small
// logs halve the unit until the directory covers at least 16 units.
func newGeometry(capacity uint64) geometry {
	capacity &^= align.BlockSize - 1
	unit := uint64(maxUnit)
	for unit > align.BlockSize && capacity < 16*unit {
		unit /= 2
	}
	slots := (capacity + unit - 1) / unit
	dirBytes := uint64(align.PageAlign(int64(slots * 8)))
	return geometry{capacity: capacity, unit: unit, dirBytes: dirBytes, dataStart: superblockSize + dirBytes}
}

// fitGeometry lays out the largest log that fits in total device bytes. A
// smaller log never needs a larger directory, so the second layout fits.
func fitGeometry(total uint64) geometry {
	return newGeometry(total - newGeometry(total).dataStart)
}

// segmentBytes is the seal threshold: up to 64 MiB, and at least one unit so
// consecutive footers start in different units.
func (g geometry) segmentBytes() uint64 {
	return max(g.unit, min(maxSegment, g.capacity/8)&^(align.BlockSize-1))
}

func (g geometry) offset(position uint64) int64 { return int64(g.dataStart + position%g.capacity) }

func (g geometry) slot(position uint64) int { return int((position % g.capacity) / g.unit) }

func (g geometry) slots() int { return int(g.dirBytes / 8) }

// tail is the oldest live position once head has filled a lap.
func (g geometry) tail(head uint64) uint64 { return head - min(head, g.capacity) }

func (g geometry) encode(page []byte) {
	clear(page[:superblockSize])
	copy(page, superblockMagic[:])
	binary.LittleEndian.PutUint64(page[8:], g.capacity)
	binary.LittleEndian.PutUint64(page[16:], g.unit)
	binary.LittleEndian.PutUint64(page[24:], g.dirBytes)
	binary.LittleEndian.PutUint64(page[32:], g.dataStart)
	binary.LittleEndian.PutUint32(page[40:], crc32.Checksum(page[:40], crcTable))
}

func decodeGeometry(page []byte) (geometry, bool) {
	if [8]byte(page[:8]) != superblockMagic || binary.LittleEndian.Uint32(page[40:]) != crc32.Checksum(page[:40], crcTable) {
		return geometry{}, false
	}
	return geometry{
		capacity:  binary.LittleEndian.Uint64(page[8:]),
		unit:      binary.LittleEndian.Uint64(page[16:]),
		dirBytes:  binary.LittleEndian.Uint64(page[24:]),
		dataStart: binary.LittleEndian.Uint64(page[32:]),
	}, true
}

// footerSize is a footer frame's footprint: its header followed by one header
// per segment record, padded to a page. One page lists up to 63 records.
func footerSize(records int) uint64 {
	return uint64(align.PageAlign(int64((records + 1) * RecordHeaderSize)))
}
