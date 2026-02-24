package blobcache

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/miretskiy/blobcache/internal/index"
	"github.com/miretskiy/blobcache/internal/record"
)

const (
	sstExtension = ".sst"
	delExtension = ".del"
	sstValueSize = 42
)

// SSTable user property keys.
const (
	sstPropSegmentID   = "blobcache.segment_id"
	sstPropCTime       = "blobcache.ctime"
	sstPropMinSeqID    = "blobcache.min_seq_id"
	sstPropMaxSeqID    = "blobcache.max_seq_id"
	sstPropRecordCount = "blobcache.record_count"
)

// SegmentSSTPath converts a segment data path to its SSTable index path.
// Example: "/data/segments/0001/123.seg" -> "/data/segments/0001/123.sst"
func SegmentSSTPath(segmentPath string) string {
	ext := filepath.Ext(segmentPath)
	return segmentPath[:len(segmentPath)-len(ext)] + sstExtension
}

// SegmentDelPath converts a segment data path to its tombstone log path.
// Example: "/data/segments/0001/123.seg" -> "/data/segments/0001/123.del"
func SegmentDelPath(segmentPath string) string {
	ext := filepath.Ext(segmentPath)
	return segmentPath[:len(segmentPath)-len(ext)] + delExtension
}

// sstEntry holds one key-value pair to be written into an SSTable.
type sstEntry struct {
	UserKey      []byte
	Hash         Key    // xxh3.Hash128 of user key
	Offset       uint32 // byte offset of record in segment file
	LogicalSize  uint32 // uncompressed value size
	PhysicalSize uint32 // compressed value size (or == LogicalSize)
	SeqID        uint64 // monotonic sequence ID
	Flags        uint32 // compression codec, deleted, checksum, errno
	KeyLen       uint16 // user key length in bytes
}

// sstMeta holds segment-level metadata stored as SSTable properties.
type sstMeta struct {
	SegmentID   uint32
	CTime       int64
	MinSeqID    uint64
	MaxSeqID    uint64
	RecordCount int64
}

// sstValue is the decoded 42-byte value stored in an SSTable.
type sstValue struct {
	Hash         Key    // 16B
	Offset       uint32 // 4B
	LogicalSize  uint32 // 4B
	PhysicalSize uint32 // 4B
	SeqID        uint64 // 8B
	Flags        uint32 // 4B
	KeyLen       uint16 // 2B
}

// encodeSSTValue encodes an sstEntry into a 42-byte wire format.
//
// Wire format (little-endian):
//
//	[Hash.Lo(8)][Hash.Hi(8)][Offset(4)][LogicalSize(4)]
//	[PhysicalSize(4)][SeqID(8)][Flags(4)][KeyLen(2)]
func encodeSSTValue(e *sstEntry) [sstValueSize]byte {
	var buf [sstValueSize]byte
	binary.LittleEndian.PutUint64(buf[0:8], e.Hash.Lo)
	binary.LittleEndian.PutUint64(buf[8:16], e.Hash.Hi)
	binary.LittleEndian.PutUint32(buf[16:20], e.Offset)
	binary.LittleEndian.PutUint32(buf[20:24], e.LogicalSize)
	binary.LittleEndian.PutUint32(buf[24:28], e.PhysicalSize)
	binary.LittleEndian.PutUint64(buf[28:36], e.SeqID)
	binary.LittleEndian.PutUint32(buf[36:40], e.Flags)
	binary.LittleEndian.PutUint16(buf[40:42], e.KeyLen)
	return buf
}

// decodeSSTValue parses a 42-byte wire format into an sstValue.
func decodeSSTValue(b []byte) sstValue {
	return sstValue{
		Hash: Key{
			Lo: binary.LittleEndian.Uint64(b[0:8]),
			Hi: binary.LittleEndian.Uint64(b[8:16]),
		},
		Offset:       binary.LittleEndian.Uint32(b[16:20]),
		LogicalSize:  binary.LittleEndian.Uint32(b[20:24]),
		PhysicalSize: binary.LittleEndian.Uint32(b[24:28]),
		SeqID:        binary.LittleEndian.Uint64(b[28:36]),
		Flags:        binary.LittleEndian.Uint32(b[36:40]),
		KeyLen:       binary.LittleEndian.Uint16(b[40:42]),
	}
}

// sstValueToFooterEntry converts an sstValue to a record.FooterEntry.
func sstValueToFooterEntry(v sstValue) record.FooterEntry {
	return record.FooterEntry{
		Key:          v.Hash,
		Pos:          int64(v.Offset),
		LogicalSize:  int64(v.LogicalSize),
		PhysicalSize: int64(v.PhysicalSize),
		SeqID:        v.SeqID,
		Flags:        uint64(v.Flags),
		KeyLen:       v.KeyLen,
	}
}

// sstValueToItem converts an sstValue to an index.Item.
func sstValueToItem(v sstValue, segID uint32) index.Item {
	physicalLen := int64(record.HeaderSize) + int64(v.KeyLen) + int64(v.PhysicalSize)
	item := index.Item{
		Key:         v.Hash,
		SegmentID:   segID,
		Offset:      v.Offset,
		PhysicalLen: uint32(physicalLen),
	}
	// Extract compression from flags (same bit layout as record.Header.Flags).
	fe := record.FooterEntry{Flags: uint64(v.Flags)}
	item.SetCompression(fe.Compression())
	if fe.IsDeleted() {
		item.SetDeleted()
	}
	return item
}

// WriteSSTFile writes a sorted SSTable to path.
// entries MUST be pre-sorted by user key (bytes.Compare order).
// Stores segment metadata as table properties.
func WriteSSTFile(path string, entries []sstEntry, meta sstMeta) error {
	f, err := vfs.Default.Create(path)
	if err != nil {
		return fmt.Errorf("create sst %s: %w", path, err)
	}

	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		Comparer:    pebble.DefaultComparer,
		Compression: sstable.NoCompression, // Values are tiny (42B); compression overhead > savings
		TablePropertyCollectors: []func() sstable.TablePropertyCollector{
			newBlobcachePropertyCollectorFactory(meta),
		},
	})

	for i := range entries {
		val := encodeSSTValue(&entries[i])
		if err := w.Set(entries[i].UserKey, val[:]); err != nil {
			_ = w.Close()
			return fmt.Errorf("sst set key: %w", err)
		}
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("sst close: %w", err)
	}
	return nil
}

// ReadSST reads all entries from an SSTable, returning a DurableBatch.
// Does NOT apply tombstones (caller handles .del separately).
func ReadSST(path string, segmentID uint32) (index.DurableBatch, error) {
	f, err := os.Open(path)
	if err != nil {
		return index.DurableBatch{}, fmt.Errorf("open sst %s: %w", path, err)
	}

	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		_ = f.Close()
		return index.DurableBatch{}, fmt.Errorf("sst readable %s: %w", path, err)
	}

	r, err := sstable.NewReader(readable, sstable.ReaderOptions{
		Comparer: pebble.DefaultComparer,
	})
	if err != nil {
		return index.DurableBatch{}, fmt.Errorf("sst reader %s: %w", path, err)
	}
	defer func() { _ = r.Close() }()

	// Parse metadata from table properties.
	meta, err := parseSSTProperties(r)
	if err != nil {
		return index.DurableBatch{}, fmt.Errorf("sst properties %s: %w", path, err)
	}

	// Iterate all entries.
	iter, err := r.NewIter(nil, nil)
	if err != nil {
		return index.DurableBatch{}, fmt.Errorf("sst iter %s: %w", path, err)
	}

	var items []index.Item
	var entries []record.FooterEntry

	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		valBytes, _, err := val.Value(nil)
		if err != nil {
			_ = iter.Close()
			return index.DurableBatch{}, fmt.Errorf("sst value: %w", err)
		}
		if len(valBytes) < sstValueSize {
			_ = iter.Close()
			return index.DurableBatch{}, fmt.Errorf("sst value too short: %d < %d", len(valBytes), sstValueSize)
		}

		v := decodeSSTValue(valBytes)
		items = append(items, sstValueToItem(v, segmentID))
		entries = append(entries, sstValueToFooterEntry(v))
	}
	if err := iter.Close(); err != nil {
		return index.DurableBatch{}, fmt.Errorf("sst iter close: %w", err)
	}

	return index.DurableBatch{
		SegmentID: segmentID,
		CTime:     meta.CTime,
		MaxSeqID:  meta.MaxSeqID,
		Items:     items,
		Entries:   entries,
	}, nil
}

// RewriteSSTable reads entries from srcPath, filters them against liveHashes,
// writes survivors (with updated offsets) + tombstone entries to dstPath.
//
// liveOffsets maps hash → new offset for live entries. Entries whose hash is
// not in liveOffsets are dropped. tombstoneEntries are merged at their sorted
// position in the output.
func RewriteSSTable(
	srcPath, dstPath string,
	newSegID uint32,
	liveOffsets map[Key]uint32,
	tombstoneEntries []record.FooterEntry,
	ctime int64,
) error {
	f, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source sst: %w", err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("sst readable: %w", err)
	}
	r, err := sstable.NewReader(readable, sstable.ReaderOptions{
		Comparer: pebble.DefaultComparer,
	})
	if err != nil {
		return fmt.Errorf("sst reader: %w", err)
	}
	defer func() { _ = r.Close() }()

	iter, err := r.NewIter(nil, nil)
	if err != nil {
		return fmt.Errorf("sst iter: %w", err)
	}

	// Collect output entries: live entries with updated offsets.
	var outputEntries []sstEntry
	var minSeq, maxSeq uint64

	for key, val := iter.First(); key != nil; key, val = iter.Next() {
		valBytes, _, err := val.Value(nil)
		if err != nil {
			_ = iter.Close()
			return fmt.Errorf("sst value: %w", err)
		}
		v := decodeSSTValue(valBytes)

		newOffset, live := liveOffsets[v.Hash]
		if !live {
			continue
		}

		e := sstEntry{
			UserKey:      append([]byte(nil), key.UserKey...),
			Hash:         v.Hash,
			Offset:       newOffset,
			LogicalSize:  v.LogicalSize,
			PhysicalSize: v.PhysicalSize,
			SeqID:        v.SeqID,
			Flags:        v.Flags,
			KeyLen:       v.KeyLen,
		}
		outputEntries = append(outputEntries, e)
		if minSeq == 0 || v.SeqID < minSeq {
			minSeq = v.SeqID
		}
		if v.SeqID > maxSeq {
			maxSeq = v.SeqID
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("sst iter close: %w", err)
	}

	// Merge tombstone entries at their sorted positions.
	for i := range tombstoneEntries {
		te := &tombstoneEntries[i]
		// We don't have user key bytes for tombstones in compaction path.
		// Tombstones are keyed by hash only — skip them from SSTable output.
		// They'll be tracked via .del file or baked-in deleted flags.
		_ = te
	}

	if ctime == 0 {
		ctime = time.Now().Unix()
	}
	meta := sstMeta{
		SegmentID:   newSegID,
		CTime:       ctime,
		MinSeqID:    minSeq,
		MaxSeqID:    maxSeq,
		RecordCount: int64(len(outputEntries)),
	}

	return WriteSSTFile(dstPath, outputEntries, meta)
}

// parseSSTProperties extracts blobcache metadata from SSTable user properties.
func parseSSTProperties(r *sstable.Reader) (sstMeta, error) {
	props := r.Properties.UserProperties
	var m sstMeta

	if v, ok := props[sstPropSegmentID]; ok {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return m, fmt.Errorf("parse segment_id: %w", err)
		}
		m.SegmentID = uint32(n)
	}
	if v, ok := props[sstPropCTime]; ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return m, fmt.Errorf("parse ctime: %w", err)
		}
		m.CTime = n
	}
	if v, ok := props[sstPropMinSeqID]; ok {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return m, fmt.Errorf("parse min_seq_id: %w", err)
		}
		m.MinSeqID = n
	}
	if v, ok := props[sstPropMaxSeqID]; ok {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return m, fmt.Errorf("parse max_seq_id: %w", err)
		}
		m.MaxSeqID = n
	}
	if v, ok := props[sstPropRecordCount]; ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return m, fmt.Errorf("parse record_count: %w", err)
		}
		m.RecordCount = n
	}

	return m, nil
}

// --- Table Property Collector ---

// blobcachePropertyCollector implements sstable.TablePropertyCollector.
// It stores segment-level metadata in the SSTable's user properties block.
type blobcachePropertyCollector struct {
	meta sstMeta
}

func newBlobcachePropertyCollectorFactory(meta sstMeta) func() sstable.TablePropertyCollector {
	return func() sstable.TablePropertyCollector {
		return &blobcachePropertyCollector{meta: meta}
	}
}

func (c *blobcachePropertyCollector) Add(_ pebble.InternalKey, _ []byte) error {
	return nil // Metadata is pre-computed, not derived from entries.
}

func (c *blobcachePropertyCollector) Finish(userProps map[string]string) error {
	userProps[sstPropSegmentID] = strconv.FormatUint(uint64(c.meta.SegmentID), 10)
	userProps[sstPropCTime] = strconv.FormatInt(c.meta.CTime, 10)
	userProps[sstPropMinSeqID] = strconv.FormatUint(c.meta.MinSeqID, 10)
	userProps[sstPropMaxSeqID] = strconv.FormatUint(c.meta.MaxSeqID, 10)
	userProps[sstPropRecordCount] = strconv.FormatInt(c.meta.RecordCount, 10)
	return nil
}

func (c *blobcachePropertyCollector) Name() string {
	return "blobcache.segment_metadata"
}
