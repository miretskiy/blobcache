package blobstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/miretskiy/dio/v2/align"
	"github.com/zeebo/xxh3"
)

// KeyHash identifies a key in the index. Resident reads compare the full key;
// disk reads trust the 64-bit hash and key length.
type KeyHash = uint64

// HashKey returns the XXH3-64 hash of key, stable across restarts for
// recovered index lookups.
func HashKey(key []byte) KeyHash { return xxh3.Hash(key) }

// Records are frames in the log: [header][key][page padding][value][page
// padding]. The header and key occupy one 4 KiB page; the value starts on the
// next page. Writers reserve that page before receiving bytes and fill it at
// Seal. Footers list the headers of their segment's records, from which open
// rebuilds the index. All integers are little endian.
//
// [ 0: 4) magic (0xB10BCAC7)
// [ 4: 6) flags (RecordFlags, uint16)
// [ 6: 8) reserved
// [ 8:16) log position of the frame
// [16:24) reserved
// [24:32) stored value size, excluding header, key, and padding
// [32:40) value size readers see; equal to the stored size. Footers keep their
//         segment's start here.
// [40:42) key length
// [42:50) Hash: XXH3-64 of the key
// [50:60) reserved; pads headers to 64 bytes
// [60:64) CRC32C of header[4:60) when FlagHeaderIntegrity is set; zero otherwise

const RecordHeaderSize = 64

const recordPrefixSize = align.BlockSize

// MaxKeyLen leaves room for the header in the record's first page.
const MaxKeyLen = recordPrefixSize - RecordHeaderSize

const recordMagic uint32 = 0xB10BCAC7

var (
	ErrBadMagic = errors.New("invalid header magic")
	ErrBadCRC   = errors.New("header crc mismatch")

	// Hardware accelerated on x86/ARM
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// RawHeader represents the exact 64-byte on-disk layout.
type RawHeader [RecordHeaderSize]byte

// RecordHeader is the safe, logical in-memory representation.
// Magic and the optional header CRC are handled automatically during Encode/Decode.
// The value starts immediately after the header/key page.
type RecordHeader struct {
	Flags       RecordFlags
	Position    uint64 // log position of the frame; its identity
	PhysicalLen uint64 // stored value bytes; excludes header, key, and padding
	LogicalLen  uint64 // value bytes readers see
	KeyLen      uint16
	Hash        uint64 // XXH3-64 of the key
}

// KeyHash returns the lookup identity from an index header.
func (h RecordHeader) KeyHash() KeyHash { return h.Hash }

type RecordFlags uint16

const (
	// FlagDeleted marks a record without a usable disk copy. Footers use it
	// for failed writes; the cache uses it for retired resident values.
	FlagDeleted RecordFlags = 0b0000_0000_0000_0001
	// FlagHeaderIntegrity enables writing and verifying the header CRC32C.
	FlagHeaderIntegrity RecordFlags = 0b0000_0000_0000_0010
	// FlagFooter marks a segment footer: its value is the segment's headers.
	FlagFooter RecordFlags = 0b0000_0000_0000_0100
)

// Encode serializes the logical header into the fixed-size raw array, clearing
// reserved bytes and writing a CRC32C only when FlagHeaderIntegrity is set.
func (h *RecordHeader) Encode(raw *RawHeader) {
	*raw = RawHeader{}

	binary.LittleEndian.PutUint32(raw[0:4], recordMagic)

	binary.LittleEndian.PutUint16(raw[4:6], uint16(h.Flags))

	binary.LittleEndian.PutUint64(raw[8:16], h.Position)
	binary.LittleEndian.PutUint64(raw[24:32], h.PhysicalLen)
	binary.LittleEndian.PutUint64(raw[32:40], h.LogicalLen)
	binary.LittleEndian.PutUint16(raw[40:42], h.KeyLen)

	binary.LittleEndian.PutUint64(raw[42:50], h.Hash)

	if h.Flags.HasHeaderIntegrity() {
		crc := crc32.Checksum(raw[4:60], crcTable)
		binary.LittleEndian.PutUint32(raw[60:64], crc)
	}
}

// Decode extracts the fields into h after checking the magic marker. Header CRC
// verification requires both verifyChecksums and the stored FlagHeaderIntegrity.
// Records without that flag are decoded without a checksum. On error, h is unchanged.
func (raw *RawHeader) Decode(h *RecordHeader, verifyChecksums bool) error {
	if binary.LittleEndian.Uint32(raw[0:4]) != recordMagic {
		return ErrBadMagic
	}

	flags := RecordFlags(binary.LittleEndian.Uint16(raw[4:6]))
	if verifyChecksums && flags.HasHeaderIntegrity() {
		if binary.LittleEndian.Uint32(raw[60:64]) != crc32.Checksum(raw[4:60], crcTable) {
			return ErrBadCRC
		}
	}

	h.Flags = flags
	h.Position = binary.LittleEndian.Uint64(raw[8:16])
	h.PhysicalLen = binary.LittleEndian.Uint64(raw[24:32])
	h.LogicalLen = binary.LittleEndian.Uint64(raw[32:40])
	h.KeyLen = binary.LittleEndian.Uint16(raw[40:42])
	h.Hash = binary.LittleEndian.Uint64(raw[42:50])

	return nil
}

// ValueOffset returns the value's byte offset within its data record.
func (h RecordHeader) ValueOffset() int64 {
	return recordPrefixSize
}

// KeyOffset returns the key's byte offset within its data record.
func (h RecordHeader) KeyOffset() int64 {
	return RecordHeaderSize
}

// FrameLen includes the header/key prefix and value, excluding final I/O padding.
func (h RecordHeader) FrameLen() int64 { return h.ValueOffset() + int64(h.PhysicalLen) }

// frameSize is the frame's log footprint, padded to a page.
func (h RecordHeader) frameSize() uint64 { return uint64(align.PageAlign(h.FrameLen())) }

func (f RecordFlags) IsFooter() bool { return f&FlagFooter != 0 }

func (f RecordFlags) IsDeleted() bool {
	return f&FlagDeleted != 0
}

func (f RecordFlags) HasHeaderIntegrity() bool {
	return f&FlagHeaderIntegrity != 0
}
