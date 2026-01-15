package index

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"time"

	"go.mills.io/bitcask/v2"

	"github.com/miretskiy/blobcache/internal/record"
)

const (
	// maxValueSize defines the maximum size for a Bitcask value (64KB).
	maxValueSize = 64 << 10
)

// maxBlobsPerSegment is the number of footer entries we can pack into one Bitcask entry.
var maxBlobsPerSegment = (maxValueSize - record.SegmentFooterHeaderSize) / record.FooterEntrySize

func testingSetMaxBlobsPerSegment(n int) func() {
	old := maxBlobsPerSegment
	maxBlobsPerSegment = n
	return func() {
		maxBlobsPerSegment = old
	}
}

type persistence struct {
	db *bitcask.Bitcask
}

func newPersistence(basePath string) (*persistence, error) {
	dbPath := filepath.Join(basePath, "db")
	// Bitcask's MaxValueSize ensures individual records stay within 64KB
	db, err := bitcask.Open(dbPath, bitcask.WithMaxValueSize(maxValueSize))
	if err != nil {
		return nil, fmt.Errorf("failed to open bitcask: %w", err)
	}

	return &persistence{db: db}, nil
}

func (p *persistence) DeleteRecordsFromSegment(segID int64, hashes map[uint64]struct{}) error {
	var liveEntries []record.FooterEntry
	var originalKeys [][]byte
	var cTime int64

	// 1. Flatten and Filter: Collect only what is still alive
	err := p.scanSegment(segID, func(seg record.SegmentFooter) bool {
		originalKeys = append(originalKeys, seg.IndexKey)
		if cTime == 0 {
			cTime = seg.CTime
		}

		for _, entry := range seg.Entries {
			// If it's NOT in our deletion set AND wasn't already deleted, keep it
			if _, deleted := hashes[entry.Hash]; !deleted && !entry.IsDeleted() {
				liveEntries = append(liveEntries, entry)
			}
		}
		return true
	})

	if err != nil || len(originalKeys) == 0 {
		return err
	}

	txn := p.db.Transaction()
	defer txn.Discard()

	// 2. Wipe: Clear the old positional keys
	for _, k := range originalKeys {
		_ = txn.Delete(k)
	}

	// 3. Re-Pack: Write back only the live entries into the minimum number of chunks
	currentFooter := record.SegmentFooter{
		SegmentID: segID,
		CTime:     cTime,
	}

	// Handle the 'empty segment' case: write one empty tombstone at index 0
	if len(liveEntries) == 0 {
		data := record.AppendSegmentFooter(nil, currentFooter)
		return txn.Put(p.makeKey(segID, 0), data)
	}

	var chunkIdx int64
	for i := 0; i < len(liveEntries); i++ {
		currentFooter.Entries = append(currentFooter.Entries, liveEntries[i])

		if len(currentFooter.Entries) >= maxBlobsPerSegment || i == len(liveEntries)-1 {
			data := record.AppendSegmentFooter(nil, currentFooter)
			if err := txn.Put(p.makeKey(segID, chunkIdx), data); err != nil {
				return err
			}
			chunkIdx++
			currentFooter.Entries = currentFooter.Entries[:0] // Use [:0] to keep capacity
		}
	}

	return txn.Commit()
}

// makeKey creates a 16-byte BigEndian composite key: [SegmentID (8)][Sequence (8)].
func (p *persistence) makeKey(segID int64, chunkIdx int64) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[0:8], uint64(segID))
	binary.BigEndian.PutUint64(key[8:16], uint64(chunkIdx))
	return key
}

func (p *persistence) writeBatch(segID int64, batch []record.FooterEntry) error {
	if len(batch) == 0 {
		return nil
	}

	txn := p.db.Transaction()
	defer txn.Discard()

	currentFooter := record.SegmentFooter{
		SegmentID: segID,
		CTime:     time.Now().Unix(),
	}

	// Track which chunk we are currently writing for this specific segment.
	// This ensures the keys are [SegID][0], [SegID][1], etc.
	var chunkIdx int64

	flush := func() error {
		if len(currentFooter.Entries) == 0 {
			return nil
		}
		data := record.AppendSegmentFooter(nil, currentFooter)
		if err := txn.Put(p.makeKey(segID, chunkIdx), data); err != nil {
			return err
		}
		currentFooter.Entries = currentFooter.Entries[:0]
		chunkIdx++
		return nil
	}

	for _, entry := range batch {
		currentFooter.Entries = append(currentFooter.Entries, entry)
		if len(currentFooter.Entries) >= maxBlobsPerSegment {
			if err := flush(); err != nil {
				return err
			}
			currentFooter.CTime = time.Now().Unix()
		}
	}

	if err := flush(); err != nil {
		return err
	}
	return txn.Commit()
}

func (p *persistence) scanAll(fn ScanSegmentFn) error {
	return p.db.ForEach(func(key bitcask.Key) error {
		return p.loadAndInvoke(key, fn)
	})
}

func (p *persistence) scanSegment(segID int64, fn ScanSegmentFn) error {
	// Start key: Segment SegmentID with chunk 0
	start := p.makeKey(segID, 0)

	// We use the same SegmentID, but the LARGEST possible chunk index.
	// In BigEndian, this key is lexicographically larger than any
	// real chunk in this segment, but still smaller than the next segment.
	end := p.makeKey(segID, 0x7FFFFFFFFFFFFFFF)

	return p.db.Range(start, end, func(key bitcask.Key) error {
		return p.loadAndInvoke(key, fn)
	})
}

func (p *persistence) loadAndInvoke(key bitcask.Key, fn ScanSegmentFn) error {
	buf, err := p.db.Get(key)
	if err != nil {
		return fmt.Errorf("failed to get chunk %v: %w", key, err)
	}

	footer, err := record.DecodeSegmentFooter(buf)
	if err != nil {
		return fmt.Errorf("failed to decode chunk %v: %w", key, err)
	}

	footer.IndexKey = key
	if !fn(footer) {
		// Stop requested by caller
		return nil
	}
	return nil
}

func (p *persistence) delete(keys ...[]byte) error {
	txn := p.db.Transaction()
	defer txn.Discard()
	for _, key := range keys {
		if err := txn.Delete(key); err != nil {
			return err
		}
	}
	return txn.Commit()
}

func (p *persistence) close() error {
	return p.db.Close()
}
