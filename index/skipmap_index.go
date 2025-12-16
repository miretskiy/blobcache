package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"go.mills.io/bitcask/v2"

	"github.com/miretskiy/blobcache/metadata"
	"github.com/zhangyunhao116/skipmap"
)

// Index has a fast in-memory store backed by skipmap, along
// with segments index.
type Index struct {
	// In-memory skipmap for fast lookups for blob metadata.
	blobs *skipmap.Uint64Map[Value]

	// Durable index, backed by disk (with write ahead log).
	// Stores metadata.SegmentRecords.
	segments *bitcask.Bitcask

	// segment sequence number.
	seq atomic.Int64
}

// NewIndex creates a pure in-memory skipmap index (no persistence)
func NewIndex(basePath string) (*Index, error) {
	db, err := openOrCreateBitcask(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open bitcask: %w", err)
	}

	blobs := skipmap.NewUint64[Value]()

	// Load all entries from Bitcask segments into in-memory skipmap.
	if err := db.ForEach(
		func(key bitcask.Key) error {
			v, err := db.Get(key)
			if err != nil {
				return err
			}
			segment, err := metadata.DecodeSegmentRecord(v)
			if err != nil {
				return err
			}
			for _, rec := range segment.Records {
				v := Value{
					Pos:       rec.Pos,
					Size:      rec.Size,
					Checksum:  rec.Checksum,
					SegmentID: segment.SegmentID,
					CTime:     segment.CTime,
				}
				blobs.Store(rec.Hash, v)
			}
			return nil
		}); err != nil {
		return nil, errors.Join(err, db.Close())
	}

	idx := Index{
		blobs:    blobs,
		segments: db,
	}
	idx.seq.Store(time.Now().UnixNano())
	return &idx, nil
}

// Get retrieves blob entry.
func (idx *Index) Get(key Key, val *Value) error {
	stored, ok := idx.blobs.Load(uint64(key))
	if !ok {
		return ErrNotFound
	}

	// Copy blobs to caller's entry
	*val = stored
	return nil
}

// PutBlob -- intended for tests -- puts a single blob entry into the index.
// Equivalent to creating a single element batch.
func (idx *Index) PutBlob(key Key, val Value) error {
	return idx.PutBatch([]KeyValue{{Key: key, Val: val}})
}

// DeleteBlob -- intended for tests -- deletes a single blob entry from the index.
// Segment containing the blob not updated.
func (idx *Index) DeleteBlob(key Key) {
	idx.blobs.Delete(uint64(key))
}

// DeleteSegment removes all records associated with the specified segment record.
func (idx *Index) DeleteSegment(segment metadata.SegmentRecord) error {
	for _, rec := range segment.Records {
		idx.blobs.Delete(rec.Hash)
	}
	return idx.segments.Delete(segment.IndexKey)
}

// Close closes the Bitcask backing store if present
func (idx *Index) Close() {
	if err := idx.segments.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close segments: %v\n", err)
	}
}

// ForEachSegment iterates over all segment records in the index
func (idx *Index) ForEachSegment(fn ScanSegmentFn) error {
	return idx.segments.ForEach(
		func(key bitcask.Key) error {
			v, err := idx.segments.Get(key)
			if err != nil {
				return err
			}
			rec, err := metadata.DecodeSegmentRecord(v)
			if err != nil {
				return err
			}
			rec.IndexKey = key
			fn(rec)
			return nil
		})
}

// ForEachBlob iterates over all blob records.
func (idx *Index) ForEachBlob(fn KeyValueFn) {
	idx.blobs.Range(func(key uint64, record Value) bool {
		//key := record.
		return fn(KeyValue{Key: Key(key), Val: record})
	})
}

func (idx *Index) makeSegmentKey() []byte {
	return binary.LittleEndian.AppendUint64(nil, uint64(idx.seq.Add(1)))
}

func (idx *Index) getSegmentKey(s int64) []byte {
	return binary.LittleEndian.AppendUint64(nil, uint64(s))
}

// PutBatch inserts multiple records.
func (idx *Index) PutBatch(records []KeyValue) error {
	if len(records) == 0 {
		return nil
	}

	// Bin the records by segmentID
	segments := make(map[int64][]KeyValue)
	for _, record := range records {
		segments[record.Val.SegmentID] = append(segments[record.Val.SegmentID], record)
	}
	for _, segment := range segments {
		if err := idx.putSegment(segment); err != nil {
			return err
		}
	}
	return nil
}

func (idx *Index) putSegment(records []KeyValue) error {
	seg := metadata.SegmentRecord{
		Records: nil,
		CTime:   time.Now(),
	}

	txn := idx.segments.Transaction()
	defer txn.Discard()

	flushSegment := func() error {
		if len(seg.Records) == 0 {
			return nil
		}
		data := metadata.AppendSegmentRecord(nil, seg)
		seg.Records = seg.Records[:0]
		return txn.Put(idx.makeSegmentKey(), data)
	}

	for _, rec := range records {
		idx.blobs.Store(uint64(rec.Key), rec.Val)

		if len(seg.Records) == maxBlobsPerSegment {
			if err := flushSegment(); err != nil {
				return err
			}
		}

		if len(seg.Records) == 0 {
			// We may be appending to this segment -- load it.
			if v, err := txn.Get(idx.getSegmentKey(rec.Val.SegmentID)); err == nil {
				segment, err := metadata.DecodeSegmentRecord(v)
				if err != nil {
					return err
				}
				seg = segment
			} else {
				seg.CTime = rec.Val.CTime
				seg.SegmentID = rec.Val.SegmentID
			}
		} else {
			if seg.CTime != rec.Val.CTime || seg.SegmentID != rec.Val.SegmentID {
				return fmt.Errorf("inconsistent segment record: %s != %s or %d != %d", seg.CTime,
					rec.Val.CTime, seg.SegmentID, rec.Val.SegmentID)
			}
		}

		seg.Records = append(seg.Records, metadata.BlobRecord{
			Hash:     uint64(rec.Key),
			Pos:      rec.Val.Pos,
			Size:     rec.Val.Size,
			Checksum: rec.Val.Checksum,
		})
	}

	if err := flushSegment(); err != nil {
		return err
	}

	return txn.Commit()
}
