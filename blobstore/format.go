package blobstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"

	"github.com/miretskiy/dio/v2/align"
)

// MaxKeyLen is the longest key Write accepts: the record trailer stores the
// key length in 16 bits.
const MaxKeyLen = math.MaxUint16

// ChecksumError reports a value CRC mismatch. Read also returns ErrCorrupt
// through the same error chain.
type ChecksumError struct {
	Expected uint32
	Got      uint32
}

func (e *ChecksumError) Error() string {
	return fmt.Sprintf("checksum mismatch: expected %08x, got %08x", e.Expected, e.Got)
}

// --- Record ---
//
// A segment holds records back to back:
//
//	[ value | zero padding | key | trailer ]
//
// A record written with direct I/O starts on a page boundary and is a page
// multiple, PageAlign(value+key+trailer), so that it can be written in place
// from the caller's page-aligned memory with one O_DIRECT write and read back
// with one O_DIRECT read. A buffered record (WithDirectWrites(false)) has no
// padding and starts wherever the previous record ended.
//
// The value comes first so that it is page-aligned both on disk and in the
// caller's memory. The trailer ends exactly at the record's end, so a reader
// holding the record's (offset, size) finds it without other metadata.
//
// Record trailer (20 bytes, little endian):
//
//	[ 0:4 ] value length
//	[ 4:8 ] CRC32C of the value (always present)
//	[ 8:10] key length
//	[10]    flags
//	[11]    format version
//	[12:16] CRC32C of key || trailer[0:12]
//	[16:20] magic
//
// The trailer CRC protects framing and key metadata; the value CRC detects
// torn or damaged payloads even when the key and trailer survived. Footers
// list reserved locations, so every disk read must verify both checksums.
const (
	trailerSize          = 20
	trailerMagic  uint32 = 0xB10BCAC4
	formatVersion        = 1

	flagValueCRC = 1 << 0
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type recordTrailer struct {
	valueLen uint32
	keyLen   uint16
}

// frameRecord fills rec past its value, rec[:t.valueLen], with zero padding,
// the key, and the trailer ending at len(rec).
func frameRecord(rec, key []byte, t recordTrailer) {
	size := len(rec)
	keyOff := size - trailerSize - len(key)
	clear(rec[t.valueLen:keyOff])
	copy(rec[keyOff:], key)
	raw := rec[size-trailerSize:]
	binary.LittleEndian.PutUint32(raw[0:], t.valueLen)
	binary.LittleEndian.PutUint32(raw[4:], valueCRC(rec[:t.valueLen]))
	binary.LittleEndian.PutUint16(raw[8:], t.keyLen)
	raw[10] = flagValueCRC
	raw[11] = formatVersion
	binary.LittleEndian.PutUint32(raw[12:], crc32.Checksum(rec[keyOff:size-8], castagnoli))
	binary.LittleEndian.PutUint32(raw[16:], trailerMagic)
}

// verifyRecord checks that rec, one whole record, is intact and belongs to
// key, and returns its value, a prefix of rec. A record that fails any check
// is ErrCorrupt; a value CRC mismatch is also a *ChecksumError.
func verifyRecord(rec, key []byte) ([]byte, error) {
	size := len(rec)
	if size < trailerSize {
		return nil, ErrCorrupt
	}
	raw := rec[size-trailerSize:]
	if binary.LittleEndian.Uint32(raw[16:]) != trailerMagic || raw[11] != formatVersion {
		return nil, ErrCorrupt
	}
	keyOff := size - trailerSize - int(binary.LittleEndian.Uint16(raw[8:]))
	valueLen := int64(binary.LittleEndian.Uint32(raw[0:]))
	if keyOff < 0 || valueLen > int64(keyOff) {
		return nil, ErrCorrupt
	}
	if crc32.Checksum(rec[keyOff:size-8], castagnoli) != binary.LittleEndian.Uint32(raw[12:]) {
		return nil, ErrCorrupt
	}
	if string(rec[keyOff:size-trailerSize]) != string(key) {
		return nil, ErrCorrupt
	}
	value := rec[:valueLen]
	if raw[10] != flagValueCRC {
		return nil, ErrCorrupt
	}
	want := binary.LittleEndian.Uint32(raw[4:])
	if got := valueCRC(value); got != want {
		return nil, errors.Join(ErrCorrupt, &ChecksumError{Expected: want, Got: got})
	}
	return value, nil
}

// valueCRC is the CRC32C stored for every value.
func valueCRC(value []byte) uint32 { return crc32.Checksum(value, castagnoli) }

// --- Segment footer ---
//
// A segment is preallocated to its full size. Records grow from the front;
// the footer, written once when the segment is sealed, occupies the last
// page-aligned region of the file:
//
//	[ records → | free | entries (24 bytes each) | zero padding | tail (32) ]
//
//	entry: key hash (Lo, Hi) | offset | size
//	tail:  count (4) | segment id (8) | version (4) | CRC32C (4) | reserved (4) | magic (8)
//
// The CRC covers the entries and tail[0:16]. Because the tail ends exactly at
// the end of the file, reading a footer takes one read of the file's last
// footerReadSize bytes, and a second only for an unusually large footer.
// Entries are in offset order, which is write order.
const (
	footerEntrySize        = 24
	footerTailSize         = 32
	footerMagic     uint64 = 0xB10BCAC4F0072E00
	footerReadSize         = 64 << 10
)

type footerEntry struct {
	hash KeyHash
	off  uint32
	size uint32
}

// footerSize returns the size of a segment footer with count entries.
func footerSize(count int) int64 {
	return align.PageAlign(int64(count*footerEntrySize + footerTailSize))
}

// encodeSegmentFooter writes the footer for entries into dst, which must be
// exactly footerSize(len(entries)) bytes.
func encodeSegmentFooter(dst []byte, id uint64, entries []footerEntry) {
	for i, e := range entries {
		p := dst[i*footerEntrySize:]
		binary.LittleEndian.PutUint64(p[0:], e.hash.Lo)
		binary.LittleEndian.PutUint64(p[8:], e.hash.Hi)
		binary.LittleEndian.PutUint32(p[16:], e.off)
		binary.LittleEndian.PutUint32(p[20:], e.size)
	}
	clear(dst[len(entries)*footerEntrySize : len(dst)-footerTailSize])
	tail := dst[len(dst)-footerTailSize:]
	binary.LittleEndian.PutUint32(tail[0:], uint32(len(entries)))
	binary.LittleEndian.PutUint64(tail[4:], id)
	binary.LittleEndian.PutUint32(tail[12:], formatVersion)
	crc := crc32.Update(crc32.Checksum(dst[:len(entries)*footerEntrySize], castagnoli), castagnoli, tail[:16])
	binary.LittleEndian.PutUint32(tail[16:], crc)
	clear(tail[20:24])
	binary.LittleEndian.PutUint64(tail[24:], footerMagic)
}

// footerEntryCount parses the footer tail at the end of src and returns the
// entry count.
func footerEntryCount(id uint64, src []byte) (int, error) {
	if len(src) < footerTailSize {
		return 0, fmt.Errorf("segment %016x: no footer", id)
	}
	tail := src[len(src)-footerTailSize:]
	if binary.LittleEndian.Uint64(tail[24:]) != footerMagic {
		return 0, fmt.Errorf("segment %016x: no footer", id)
	}
	if v := binary.LittleEndian.Uint32(tail[12:]); v != formatVersion {
		return 0, fmt.Errorf("segment %016x footer: unsupported version %d", id, v)
	}
	if got := binary.LittleEndian.Uint64(tail[4:]); got != id {
		return 0, fmt.Errorf("segment %016x footer: names segment %016x", id, got)
	}
	return int(binary.LittleEndian.Uint32(tail[0:])), nil
}

// decodeSegmentFooter parses a whole footer, which ends at the end of the
// file.
func decodeSegmentFooter(id uint64, footer []byte) ([]footerEntry, error) {
	count, err := footerEntryCount(id, footer)
	if err != nil {
		return nil, err
	}
	if int64(len(footer)) != footerSize(count) {
		return nil, fmt.Errorf("segment %016x footer: %d bytes for %d entries", id, len(footer), count)
	}
	tail := footer[len(footer)-footerTailSize:]
	body := footer[:count*footerEntrySize]
	if crc32.Update(crc32.Checksum(body, castagnoli), castagnoli, tail[:16]) != binary.LittleEndian.Uint32(tail[16:]) {
		return nil, fmt.Errorf("segment %016x footer: checksum mismatch", id)
	}
	entries := make([]footerEntry, count)
	for i := range entries {
		p := body[i*footerEntrySize:]
		entries[i] = footerEntry{
			hash: KeyHash{Lo: binary.LittleEndian.Uint64(p[0:]), Hi: binary.LittleEndian.Uint64(p[8:])},
			off:  binary.LittleEndian.Uint32(p[16:]),
			size: binary.LittleEndian.Uint32(p[20:]),
		}
	}
	return entries, nil
}

// readSegmentFooter reads and verifies the footer of segment id from f. It
// reads page-aligned memory at page-aligned offsets, so f may be a direct
// handle.
func readSegmentFooter(f *os.File, id uint64) ([]footerEntry, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()
	if fileSize%align.BlockSize != 0 {
		return nil, fmt.Errorf("segment %016x: size %d is not a page multiple", id, fileSize)
	}
	buf := allocRecord(int(min(footerReadSize, fileSize)))
	if _, err := f.ReadAt(buf, fileSize-int64(len(buf))); err != nil {
		return nil, err
	}
	count, err := footerEntryCount(id, buf)
	if err != nil {
		return nil, err
	}
	size := footerSize(count)
	if size > fileSize {
		return nil, fmt.Errorf("segment %016x footer: %d entries exceed the file", id, count)
	}
	if size > int64(len(buf)) {
		buf = allocRecord(int(size))
		if _, err := f.ReadAt(buf, fileSize-size); err != nil {
			return nil, err
		}
	}
	entries, err := decodeSegmentFooter(id, buf[int64(len(buf))-size:])
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.size < trailerSize || int64(e.off)+int64(e.size) > fileSize-size {
			return nil, fmt.Errorf("segment %016x footer: entry [%d, +%d) outside the record area", id, e.off, e.size)
		}
	}
	return entries, nil
}
