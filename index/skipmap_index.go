package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
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
	// Stores pointers to enable Sieve linked list (next/prev pointers in Value).
	blobs *skipmap.Uint64Map[*Value]

	// Durable index, backed by disk (with write ahead log).
	// Stores metadata.SegmentRecords.
	segments *bitcask.Bitcask

	// segment sequence number.
	seq atomic.Int64

	// FIFO queue maintaining insertion order
	fifo struct {
		sync.Mutex
		head *Value // Newest entry (insertion point)
		tail *Value // Oldest entry
		hand *Value // Sieve eviction scan position
	}
}

// NewIndex creates a pure in-memory skipmap index (no persistence)
func NewIndex(basePath string) (*Index, error) {
	db, err := openOrCreateBitcask(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open bitcask: %w", err)
	}

	blobs := skipmap.NewUint64[*Value]()

	idx := Index{
		blobs:    blobs,
		segments: db,
	}
	idx.seq.Store(time.Now().UnixNano())

	// Load all entries from Bitcask segments into in-memory skipmap and FIFO queue
	// Bitcask iterates in key order (segment keys are sequential timestamps)
	// so this builds the FIFO queue in insertion order (oldest to newest)
	if err := db.ForEach(func(key bitcask.Key) error {
		v, err := db.Get(key)
		if err != nil {
			return err
		}
		segment, err := metadata.DecodeSegmentRecord(v)
		if err != nil {
			return err
		}

		// Create Values and link into FIFO queue in segment age order
		// Skip deleted blobs (evicted blobs with Deleted flag set)
		for _, rec := range segment.Records {
			if rec.IsDeleted() {
				continue // Skip deleted blobs
			}
			val := NewValueFrom(rec.Pos, rec.Size, rec.Flags, segment.SegmentID)
			blobs.Store(rec.Hash, val)
			idx.insertIntoQueue(val)
		}
		return nil
	}); err != nil {
		return nil, errors.Join(err, db.Close())
	}

	return &idx, nil
}

// Get retrieves blob entry pointer from skipmap.
// Returns pointer-stable Value for Sieve queue operations.
func (idx *Index) Get(key Key) (*Value, error) {
	stored, ok := idx.blobs.Load(uint64(key))
	if !ok {
		return nil, ErrNotFound
	}
	return stored, nil
}

// PutBlob -- intended for tests -- puts a single blob entry into the index.
// Equivalent to creating a single element batch.
func (idx *Index) PutBlob(key Key, val *Value) error {
	return idx.PutBatch([]KeyValue{{Key: key, Val: val}})
}

// DeleteBlob -- intended for tests -- deletes a single blob entry from the index.
// Returns the deleted Value for recycling, or nil if not found.
func (idx *Index) DeleteBlob(key Key) *Value {
	deleted, _ := idx.blobs.LoadAndDelete(uint64(key))
	if deleted != nil {
		idx.fifo.Lock()
		idx.removeFromQueueLocked(deleted)
		idx.fifo.Unlock()
	}
	return deleted
}

// DeleteSegment removes all records associated with the specified segment record.
// Recycles deleted Values back to pool.
func (idx *Index) DeleteSegment(segment metadata.SegmentRecord) error {
	idx.fifo.Lock()
	defer idx.fifo.Unlock()

	for _, rec := range segment.Records {
		if val, _ := idx.blobs.LoadAndDelete(rec.Hash); val != nil {
			idx.removeFromQueueLocked(val)
			recycleValue(val)
		}
	}
	return idx.segments.Delete(segment.IndexKey)
}

// Close closes the Bitcask backing store
func (idx *Index) Close() error {
	return idx.segments.Close()
}

// GetSegmentRecord retrieves a specific segment record by ID
func (idx *Index) GetSegmentRecord(segmentID int64) (*metadata.SegmentRecord, error) {
	key := idx.getSegmentKey(segmentID)
	v, err := idx.segments.Get(key)
	if err != nil {
		return nil, err
	}
	segment, err := metadata.DecodeSegmentRecord(v)
	if err != nil {
		return nil, err
	}
	segment.IndexKey = key
	return &segment, nil
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
	idx.blobs.Range(func(key uint64, record *Value) bool {
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
	}

	txn := idx.segments.Transaction()
	defer txn.Discard()

	// Lock for FIFO queue updates (held for entire batch)
	idx.fifo.Lock()
	defer idx.fifo.Unlock()

	flushSegment := func() error {
		if len(seg.Records) == 0 {
			return nil
		}
		data := metadata.AppendSegmentRecord(nil, seg)
		key := idx.getSegmentKey(seg.SegmentID)
		seg.Records = seg.Records[:0]
		return txn.Put(key, data)
	}

	for _, rec := range records {
		idx.blobs.Store(uint64(rec.Key), rec.Val)
		idx.insertIntoQueue(rec.Val) // Add to FIFO queue (lock held at function level)

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
				// New segment, use current time
				seg.CTime = time.Now()
				seg.SegmentID = rec.Val.SegmentID
			}
		} else {
			// Validate consistency
			if seg.SegmentID != rec.Val.SegmentID {
				return fmt.Errorf("inconsistent segment ID: %d != %d", seg.SegmentID, rec.Val.SegmentID)
			}
		}

		seg.Records = append(seg.Records, metadata.BlobRecord{
			Hash:  uint64(rec.Key),
			Pos:   rec.Val.Pos,
			Size:  rec.Val.Size,
			Flags: rec.Val.Checksum,
		})
	}

	if err := flushSegment(); err != nil {
		return err
	}

	return txn.Commit()
}

// valuePool manages Value allocations to reduce GC pressure
var valuePool = sync.Pool{
	New: func() interface{} {
		return &Value{}
	},
}

func newValue() *Value {
	return valuePool.Get().(*Value)
}

func recycleValue(v *Value) {
	*v = Value{} // Zero out
	valuePool.Put(v)
}

// NewValueFrom creates a new Value with the specified fields
func NewValueFrom(pos, size int64, checksum uint64, segmentID int64) *Value {
	v := newValue()
	v.Pos = pos
	v.Size = size
	v.Checksum = checksum
	v.SegmentID = segmentID
	v.visited.Store(false) // Initialize as unvisited
	v.next = nil
	v.prev = nil
	return v
}

// FIFO queue operations

// insertIntoQueue adds a Value to the head (newest position)
// Must hold idx.fifo.Mutex or be called during initialization
func (idx *Index) insertIntoQueue(val *Value) {
	val.next = nil
	val.prev = idx.fifo.head

	if idx.fifo.head != nil {
		idx.fifo.head.next = val
	}
	idx.fifo.head = val

	if idx.fifo.tail == nil {
		idx.fifo.tail = val
	}
}

// removeFromQueueLocked removes a Value from the queue
// Must hold idx.fifo.Mutex
func (idx *Index) removeFromQueueLocked(val *Value) {
	if val.prev != nil {
		val.prev.next = val.next
	} else {
		idx.fifo.tail = val.next // Was tail
	}

	if val.next != nil {
		val.next.prev = val.prev
	} else {
		idx.fifo.head = val.prev // Was head
	}

	val.next = nil
	val.prev = nil
}

// OldestBlob returns the oldest blob in insertion order (tail of queue)
func (idx *Index) OldestBlob() *Value {
	idx.fifo.Lock()
	defer idx.fifo.Unlock()
	return idx.fifo.tail
}

// SieveScan performs one Sieve eviction scan to find a victim
// Updates internal hand position, returns (key, value, nil) on success
// Returns error if queue is empty
func (idx *Index) SieveScan() (Key, *Value, error) {
	idx.fifo.Lock()
	defer idx.fifo.Unlock()

	// Start at hand position (or tail if nil)
	if idx.fifo.hand == nil {
		idx.fifo.hand = idx.fifo.tail
		if idx.fifo.hand == nil {
			return 0, nil, fmt.Errorf("empty queue")
		}
	}

	// Scan from hand toward head (oldest → newest)
	for obj := idx.fifo.hand; obj != nil; obj = obj.next {
		if obj.visited.Load() {
			// Reset visited, advance hand
			obj.visited.Store(false)
			idx.fifo.hand = obj.next
		} else {
			// Found victim!
			idx.fifo.hand = obj.next

			// Reverse lookup to find key
			var victimKey Key
			idx.blobs.Range(func(key uint64, val *Value) bool {
				if val == obj {
					victimKey = Key(key)
					return false
				}
				return true
			})

			return victimKey, obj, nil
		}
	}

	// Reached end, wrap around to tail
	idx.fifo.hand = nil
	return idx.SieveScan() // Recursive
}

// ForEachBlobSorted iterates over blobs in the specified order
// If start is nil, begins at tail (oldest) or head (newest) based on order
func (idx *Index) ForEachBlobSorted(order SortOrder, start *Value, fn KeyValueFn) {
	idx.fifo.Lock()
	defer idx.fifo.Unlock()

	var advance func(*Value) *Value

	if order == SortByInsertionOrder {
		// Oldest to newest
		if start == nil {
			start = idx.fifo.tail
		}
		advance = func(v *Value) *Value { return v.next }
	} else {
		// Newest to oldest
		if start == nil {
			start = idx.fifo.head
		}
		advance = func(v *Value) *Value { return v.prev }
	}

	for v := start; v != nil; v = advance(v) {
		// Need to find the key for this Value
		// This requires reverse lookup from skipmap
		var foundKey Key
		idx.blobs.Range(func(key uint64, val *Value) bool {
			if val == v {
				foundKey = Key(key)
				return false // Stop
			}
			return true
		})

		if !fn(KeyValue{Key: foundKey, Val: v}) {
			break
		}
	}
}
