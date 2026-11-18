package blobstore

import (
	"bytes"
	"testing"

	"github.com/miretskiy/dio/v2/align"
	"github.com/stretchr/testify/require"
)

func testRecord(position uint64, key string, size uint64) RecordHeader {
	return RecordHeader{Flags: FlagHeaderIntegrity, Position: position,
		PhysicalLen: size, LogicalLen: size, KeyLen: uint16(len(key)), Hash: HashKey([]byte(key))}
}

// testSegment lays out records of size bytes from start; it returns them and
// the position its footer takes.
func testSegment(start uint64, keys []string, size uint64) ([]RecordHeader, uint64) {
	var records []RecordHeader
	position := start
	for _, key := range keys {
		r := testRecord(position, key, size)
		records = append(records, r)
		position += r.frameSize()
	}
	return records, position
}

func footerFrame(position, start uint64, records []RecordHeader) []byte {
	frame := make([]byte, footerSize(len(records)))
	encodeFooter(frame, position, start, records)
	return frame
}

func TestParseFooter(t *testing.T) {
	g := newGeometry(16 << 20)
	records, position := testSegment(3<<20, []string{"a", "b", "c"}, 8000)
	frame := footerFrame(position, 3<<20, records)
	f, ok := parseFooter(g, g.slot(position), position, frame)
	require.True(t, ok)
	require.Equal(t, footer{position: position, size: align.BlockSize, records: records}, f)
	require.Equal(t, uint64(align.BlockSize), footerFrameSize(frame))

	// A failed write stays listed, flagged, and is not recovered.
	failed := append([]RecordHeader(nil), records...)
	failed[1].Flags |= FlagDeleted
	f, ok = parseFooter(g, g.slot(position), position, footerFrame(position, 3<<20, failed))
	require.True(t, ok)
	require.Equal(t, []RecordHeader{records[0], records[2]}, f.records)

	for name, corrupt := range map[string]func() ([]byte, int, uint64){
		"header CRC": func() ([]byte, int, uint64) {
			b := bytes.Clone(frame)
			b[30] ^= 1
			return b, g.slot(position), position
		},
		"entries CRC": func() ([]byte, int, uint64) {
			b := bytes.Clone(frame)
			b[100] ^= 1
			return b, g.slot(position), position
		},
		"other footer": func() ([]byte, int, uint64) { return frame, g.slot(position + 4096), position + 4096 },
		"wrong slot":   func() ([]byte, int, uint64) { return frame, g.slot(position) + 1, position },
		"record frame": func() ([]byte, int, uint64) {
			b := bytes.Clone(frame)
			clear(b[:8])
			return b, g.slot(position), position
		},
		"truncated":    func() ([]byte, int, uint64) { return frame[:100], g.slot(position), position },
		"not a footer": func() ([]byte, int, uint64) { return encodedRecord(records[0]), g.slot(position), position },
	} {
		b, slot, at := corrupt()
		_, ok := parseFooter(g, slot, at, b)
		require.False(t, ok, name)
	}
}

func TestParseFooterSpanningPages(t *testing.T) {
	g := newGeometry(64 << 20)
	keys := make([]string, 200)
	for i := range keys {
		keys[i] = string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	records, position := testSegment(0, keys, 100)
	frame := footerFrame(position, 0, records)
	require.Equal(t, uint64(4*align.BlockSize), footerFrameSize(frame[:align.BlockSize]))
	f, ok := parseFooter(g, g.slot(position), position, frame)
	require.True(t, ok)
	require.Len(t, f.records, 200)
	_, ok = parseFooter(g, g.slot(position), position, frame[:align.BlockSize])
	require.False(t, ok, "only the first page was read")
}

func TestRebuild(t *testing.T) {
	g := newGeometry(16 << 20)
	slots := make([]uint64, g.slots())
	var footers []footer
	var all []RecordHeader
	start := uint64(0)
	// Four 6 MiB segments: the log wraps once and the oldest records die.
	for i := range 4 {
		records, position := testSegment(start, []string{string(rune('a' + 2*i)), string(rune('b' + 2*i))}, 3<<20-recordPrefixSize-1)
		if rest := g.capacity - position%g.capacity; footerSize(2) > rest {
			position += rest
		}
		frame := footerFrame(position, start, records)
		f, ok := parseFooter(g, g.slot(position), position, frame)
		require.True(t, ok)
		footers = append(footers, f)
		slots[g.slot(position)] = position
		all = append(all, records...)
		start = position + f.size
	}
	head, records := rebuild(g, slots, footers)
	require.Equal(t, start, head)
	tail := g.tail(head)
	var live []RecordHeader
	for _, r := range all {
		if r.Position >= tail {
			live = append(live, r)
		}
	}
	require.Equal(t, live, records)
	require.Less(t, len(records), len(all), "the first lap's oldest records are gone")

	// A slot whose footer no longer validates still keeps head from moving
	// back over positions it once used.
	stale := slots
	stale[g.slot(head+(4<<20))] = head + 4<<20
	raised, _ := rebuild(g, stale, footers)
	require.Equal(t, head+4<<20+align.BlockSize, raised)

	head, records = rebuild(g, make([]uint64, g.slots()), nil)
	require.Zero(t, head)
	require.Empty(t, records)
}

func encodedRecord(h RecordHeader) []byte {
	frame := make([]byte, align.BlockSize)
	h.Encode((*RawHeader)(frame))
	return frame
}
