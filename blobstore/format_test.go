package blobstore

import (
	"encoding/hex"
	"math"
	"testing"

	"github.com/zeebo/xxh3"
)

func goldenRecordHeader() RecordHeader {
	return RecordHeader{
		Flags:       0xa487, // version 10, LZ4, XXHash128, header CRC, external, deleted
		ObjectID:    0x0706050403020100,
		Offset:      0x3837363534333231,
		PhysicalLen: 0x0807060504030201,
		LogicalLen:  0x1817161514131211,
		KeyLen:      0x201f,
		Hash:        xxh3.Uint128{Lo: 0x2827262524232221, Hi: 0x302f2e2d2c2b2a29},
	}
}

func goldenRawHeader(t *testing.T) RawHeader {
	t.Helper()
	// Fixed wire fixture: little-endian fields, numeric object ID and both hash halves,
	// zero reserved bytes, and CRC32C 0xd917d235 over bytes [4:60).
	const wire = "c5ca0bb187a40000" +
		"0001020304050607" +
		"3132333435363738" +
		"0102030405060708" +
		"1112131415161718" +
		"1f20" +
		"2122232425262728292a2b2c2d2e2f30" +
		"000035d217d9"
	b, err := hex.DecodeString(wire)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 64 || RecordHeaderSize != 64 {
		t.Fatalf("header size: fixture=%d, RecordHeaderSize=%d; want 64", len(b), RecordHeaderSize)
	}
	return RawHeader(b)
}

func TestRecordHeaderWireFormat(t *testing.T) {
	wantHeader := goldenRecordHeader()
	wantRaw := goldenRawHeader(t)

	t.Run("encode", func(t *testing.T) {
		var got RawHeader
		wantHeader.Encode(&got)
		if got != wantRaw {
			t.Fatalf("encoded header:\n got %x\nwant %x", got, wantRaw)
		}
	})
	t.Run("decode", func(t *testing.T) {
		var got RecordHeader
		if err := wantRaw.Decode(&got, true); err != nil {
			t.Fatal(err)
		}
		if got != wantHeader {
			t.Fatalf("decoded header:\n got %+v\nwant %+v", got, wantHeader)
		}
	})
}

func TestRecordHeaderRoundTrip(t *testing.T) {
	if MaxKeyLen != math.MaxUint16 {
		t.Fatalf("MaxKeyLen = %d; want %d", MaxKeyLen, math.MaxUint16)
	}
	for _, tc := range []struct {
		name   string
		header RecordHeader
	}{
		{name: "zero"},
		{name: "empty_checked", header: RecordHeader{Flags: FlagHeaderIntegrity}},
		{name: "inline", header: RecordHeader{Flags: FlagHeaderIntegrity, Offset: 1<<40 + 4096, PhysicalLen: 4096, LogicalLen: 4096, KeyLen: 1}},
		{name: "external", header: RecordHeader{Flags: FlagExternal, ObjectID: uint64(math.MaxUint64), PhysicalLen: 1 << 33, LogicalLen: 1 << 33, KeyLen: 1}},
		{name: "external_compressed", header: goldenRecordHeader()},
		{name: "maximum", header: RecordHeader{
			Flags: math.MaxUint16, ObjectID: uint64(math.MaxUint64),
			Offset: math.MaxUint64, PhysicalLen: math.MaxUint64, LogicalLen: math.MaxUint64, KeyLen: math.MaxUint16,
			Hash: xxh3.Uint128{Lo: math.MaxUint64, Hi: math.MaxUint64},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var raw RawHeader
			tc.header.Encode(&raw)
			// Decode must replace every field, including zero offset/ID/hash.
			got := goldenRecordHeader()
			if err := raw.Decode(&got, true); err != nil {
				t.Fatal(err)
			}
			if got != tc.header {
				t.Fatalf("round trip:\n got %+v\nwant %+v", got, tc.header)
			}
		})
	}
}

func TestRecordHeaderEncodeReusedBuffer(t *testing.T) {
	want := goldenRawHeader(t)
	header := goldenRecordHeader()
	var raw RawHeader
	for i := range raw {
		raw[i] = 0xff
	}
	header.Encode(&raw)
	if raw != want {
		t.Fatalf("encoding into dirty memory:\n got %x\nwant %x", raw, want)
	}

	// Reusing a checksummed header for an empty unchecked record must erase
	// its old fields and CRC as well as the reserved bytes.
	var empty RecordHeader
	empty.Encode(&raw)
	want = RawHeader{0xc5, 0xca, 0x0b, 0xb1}
	if raw != want {
		t.Fatalf("encoding empty header into reused memory:\n got %x\nwant %x", raw, want)
	}
}

func TestRecordHeaderDecodeRejectsCorruption(t *testing.T) {
	valid := goldenRawHeader(t)
	unchanged := RecordHeader{Flags: 0x5555, ObjectID: uint64(math.MaxUint64), Offset: 5, PhysicalLen: 1, LogicalLen: 2, KeyLen: 3, Hash: xxh3.Uint128{Lo: 4}}
	for offset := range valid {
		for bit := range 8 {
			if offset == 4 && bit == 1 {
				continue // Clearing the presence flag disables CRC verification.
			}
			raw := valid
			raw[offset] ^= 1 << bit
			wantErr := ErrBadCRC
			if offset < 4 {
				wantErr = ErrBadMagic
			}
			got := unchanged
			if err := raw.Decode(&got, true); err != wantErr {
				t.Fatalf("byte %d bit %d: error = %v; want %v", offset, bit, err, wantErr)
			}
			if got != unchanged {
				t.Fatalf("byte %d bit %d: failed decode changed destination to %+v", offset, bit, got)
			}
		}
	}

	// Magic is checked before the CRC, including for entirely empty storage.
	for _, raw := range []RawHeader{{}, {4: 2}} {
		got := unchanged
		if err := raw.Decode(&got, true); err != ErrBadMagic {
			t.Fatalf("invalid magic and CRC: error = %v; want %v", err, ErrBadMagic)
		}
		if got != unchanged {
			t.Fatal("invalid magic changed destination")
		}
	}
}

func TestRecordHeaderOptionalCRC(t *testing.T) {
	for _, present := range []bool{false, true} {
		want := goldenRecordHeader()
		want.Flags.SetHeaderIntegrity(present)
		var raw RawHeader
		want.Encode(&raw)
		raw[60] ^= 1 // A bad CRC matters only when verification and its flag are set.
		for _, verify := range []bool{false, true} {
			var got RecordHeader
			err := raw.Decode(&got, verify)
			if present && verify {
				if err != ErrBadCRC || got != (RecordHeader{}) {
					t.Fatalf("CRC failure: header=%+v, error=%v", got, err)
				}
			} else if err != nil || got != want {
				t.Fatalf("present=%t verify=%t: header=%+v, error=%v", present, verify, got, err)
			}
		}
	}
	// The format marker still identifies records when checksum verification is off.
	var raw RawHeader
	var h RecordHeader
	if err := raw.Decode(&h, false); err != ErrBadMagic {
		t.Fatalf("bad magic: got %v, want %v", err, ErrBadMagic)
	}
}

func TestRecordFlagsGetters(t *testing.T) {
	// Check every bit pattern against the published wire positions, without
	// using the implementation's masks or setters to construct expectations.
	for bits := 0; bits <= math.MaxUint16; bits++ {
		flags := RecordFlags(bits)
		if flags.IsDeleted() != (bits&1 != 0) ||
			flags.HasHeaderIntegrity() != (bits&2 != 0) ||
			flags.IsExternal() != (bits&4 != 0) ||
			flags.DataIntegrity() != DataIntegrity((bits>>7)&3) ||
			flags.Compression() != CompressionCodec((bits>>9)&7) ||
			flags.Version() != uint8(bits>>12) {
			t.Fatalf("flags %04x: deleted=%t, header integrity=%t, data integrity=%d, compression=%d, version=%d",
				bits, flags.IsDeleted(), flags.HasHeaderIntegrity(), flags.DataIntegrity(), flags.Compression(), flags.Version())
		}
	}
}

func TestRecordFlagsBooleanSetters(t *testing.T) {
	for _, tc := range []struct {
		name string
		mask RecordFlags
		set  func(*RecordFlags, bool)
	}{
		{"deleted", 0x0001, (*RecordFlags).SetDeleted},
		{"header_integrity", 0x0002, (*RecordFlags).SetHeaderIntegrity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for bits := 0; bits <= math.MaxUint16; bits++ {
				for _, enabled := range []bool{false, true} {
					want := RecordFlags(bits) &^ tc.mask
					if enabled {
						want |= tc.mask
					}
					got := RecordFlags(bits)
					tc.set(&got, enabled)
					if got != want {
						t.Fatalf("flags %04x, enabled=%t: got %04x; want %04x", bits, enabled, got, want)
					}
				}
			}
		})
	}
}

func TestRecordFlagsFieldSetters(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mask  RecordFlags
		shift uint
		set   func(*RecordFlags, uint8)
	}{
		{"data_integrity", 0x0180, 7, func(f *RecordFlags, v uint8) { f.SetDataIntegrity(DataIntegrity(v)) }},
		{"compression", 0x0e00, 9, func(f *RecordFlags, v uint8) { f.SetCompression(CompressionCodec(v)) }},
		{"version", 0xf000, 12, (*RecordFlags).SetVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, initial := range []RecordFlags{0, 0xffff, 0xaaaa, 0x5555} {
				// Include values wider than the field: high bits must be masked
				// without changing neighboring fields or reserved boolean bits.
				for value := 0; value <= math.MaxUint8; value++ {
					want := (initial &^ tc.mask) | (RecordFlags(value)<<tc.shift)&tc.mask
					got := initial
					tc.set(&got, uint8(value))
					if got != want {
						t.Fatalf("flags %04x, value=%d: got %04x; want %04x", initial, value, got, want)
					}
					tc.set(&got, 0)
					if got != initial&^tc.mask {
						t.Fatalf("clearing field in %04x changed unrelated bits: got %04x", initial, got)
					}
				}
			}
		})
	}
}
