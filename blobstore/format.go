package blobstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"

	"github.com/miretskiy/dio/v2/align"
	"github.com/zeebo/xxh3"
)

// Records in index.db, data.db, and objects/ share this packed header.
// index.db reuses retired header slots; ObjectID restores reservation order.
// ObjectIDs come from a counter seeded with the startup timestamp in nanoseconds.
// FlagExternal distinguishes object files (named by decimal ObjectID) from data.db.
// Offset records the data.db cursor at reservation; external file I/O starts at zero.
// All integers are little endian.
//
// [ 0: 4) magic (0xB10BCAC5)
// [ 4: 6) flags (RecordFlags, uint16)
// [ 6: 8) reserved
// [ 8:16) ObjectID (uint64)
// [16:24) data.db offset at reservation
// [24:32) stored value size, excluding header, key, and padding
// [32:40) original value size before compression
// [40:42) key length
// [42:58) Hash (Lo then Hi): key hash in index.db; key/value checksum in data files
// [58:60) reserved
// [60:64) CRC32C of header[4:60) when FlagHeaderIntegrity is set; zero otherwise

const RecordHeaderSize = 64
const MaxKeyLen = math.MaxUint16

const recordMagic uint32 = 0xB10BCAC5

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
// A record occupies RecordHeaderSize + KeyLen + PhysicalLen bytes before I/O padding.
type RecordHeader struct {
	Flags       RecordFlags
	ObjectID    uint64
	Offset      uint64 // data.db cursor at reservation; external file I/O starts at zero
	PhysicalLen uint64 // stored value bytes; excludes header, key, and padding
	LogicalLen  uint64 // original value bytes before compression
	KeyLen      uint16
	Hash        xxh3.Uint128 // key hash in index.db; key/value checksum in data files
}

type RecordFlags uint16

const (
	FlagDeleted RecordFlags = 0b0000_0000_0000_0001
	// FlagHeaderIntegrity enables writing and verifying the header CRC32C.
	FlagHeaderIntegrity RecordFlags = 0b0000_0000_0000_0010
	// FlagExternal identifies records stored in a dedicated object file.
	FlagExternal RecordFlags = 0b0000_0000_0000_0100

	maskDataIntegrity  RecordFlags = 0b0000_0001_1000_0000
	shiftDataIntegrity             = 7

	maskCompression  RecordFlags = 0b0000_1110_0000_0000
	shiftCompression             = 9

	maskVersion  RecordFlags = 0b1111_0000_0000_0000
	shiftVersion             = 12
)

type CompressionCodec uint8

const (
	CompressionNone CompressionCodec = iota
	CompressionZstd
	CompressionLZ4

	numCompressionCodec = 8
)

type DataIntegrity uint8

const (
	DataIntegrityNone DataIntegrity = iota
	DataIntegrityXXHash128

	numDataIntegrity = 4
)

// Encode serializes the logical header into the fixed-size raw array, clearing
// reserved bytes and writing a CRC32C only when FlagHeaderIntegrity is set.
func (h *RecordHeader) Encode(raw *RawHeader) {
	*raw = RawHeader{}

	binary.LittleEndian.PutUint32(raw[0:4], recordMagic)

	binary.LittleEndian.PutUint16(raw[4:6], uint16(h.Flags))

	binary.LittleEndian.PutUint64(raw[8:16], h.ObjectID)

	binary.LittleEndian.PutUint64(raw[16:24], h.Offset)
	binary.LittleEndian.PutUint64(raw[24:32], h.PhysicalLen)
	binary.LittleEndian.PutUint64(raw[32:40], h.LogicalLen)
	binary.LittleEndian.PutUint16(raw[40:42], h.KeyLen)

	binary.LittleEndian.PutUint64(raw[42:50], h.Hash.Lo)
	binary.LittleEndian.PutUint64(raw[50:58], h.Hash.Hi)

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
	h.ObjectID = binary.LittleEndian.Uint64(raw[8:16])
	h.Offset = binary.LittleEndian.Uint64(raw[16:24])
	h.PhysicalLen = binary.LittleEndian.Uint64(raw[24:32])
	h.LogicalLen = binary.LittleEndian.Uint64(raw[32:40])
	h.KeyLen = binary.LittleEndian.Uint16(raw[40:42])
	h.Hash.Lo = binary.LittleEndian.Uint64(raw[42:50])
	h.Hash.Hi = binary.LittleEndian.Uint64(raw[50:58])

	return nil
}

// dataEnd is the data.db append position recorded by this completed record.
func (h RecordHeader) dataEnd() uint64 {
	if h.Flags.IsExternal() {
		return h.Offset
	}
	return h.Offset + uint64(align.PageAlign(RecordHeaderSize+int64(h.KeyLen)+int64(h.PhysicalLen)))
}

func (h RecordHeader) fileOffset() int64 {
	if h.Flags.IsExternal() {
		return 0
	}
	return int64(h.Offset)
}

func (f RecordFlags) IsExternal() bool {
	return f&FlagExternal != 0
}

func (f RecordFlags) IsDeleted() bool {
	return f&FlagDeleted != 0
}

func (f RecordFlags) HasHeaderIntegrity() bool {
	return f&FlagHeaderIntegrity != 0
}

func (f RecordFlags) DataIntegrity() DataIntegrity {
	return DataIntegrity((f & maskDataIntegrity) >> shiftDataIntegrity)
}

func (f RecordFlags) Compression() CompressionCodec {
	return CompressionCodec((f & maskCompression) >> shiftCompression)
}

func (f RecordFlags) Version() uint8 {
	return uint8((f & maskVersion) >> shiftVersion)
}

func (f *RecordFlags) SetDeleted(deleted bool) {
	if deleted {
		*f |= FlagDeleted
	} else {
		*f &^= FlagDeleted
	}
}

func (f *RecordFlags) SetHeaderIntegrity(enabled bool) {
	if enabled {
		*f |= FlagHeaderIntegrity
	} else {
		*f &^= FlagHeaderIntegrity
	}
}

func (f *RecordFlags) SetDataIntegrity(algo DataIntegrity) {
	// Shift the enum up, and explicitly mask it to prevent invalid
	// enum values (>3) from spilling into the Compression bits.
	shifted := (RecordFlags(algo) << shiftDataIntegrity) & maskDataIntegrity
	*f = (*f &^ maskDataIntegrity) | shifted
}

func (f *RecordFlags) SetCompression(codec CompressionCodec) {
	shifted := (RecordFlags(codec) << shiftCompression) & maskCompression
	*f = (*f &^ maskCompression) | shifted
}

func (f *RecordFlags) SetVersion(version uint8) {
	shifted := (RecordFlags(version) << shiftVersion) & maskVersion
	*f = (*f &^ maskVersion) | shifted
}
