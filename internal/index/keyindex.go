package index

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// Namespace prefixes for the KeyIndex Pebble DB.
// Each namespace stores a different mapping to support ordered iteration,
// reverse lookup (hash → user key), segment membership, and reconciliation.
const (
	nsHashToKey   byte = 0x00 // hash(16) → userKey    (reverse lookup for eviction)
	nsKeyToHash   byte = 0x01 // userKey → hash(16)    (ordered iteration)
	nsSegMember   byte = 0x02 // segID(4BE)+hash(16) → ""  (segment membership)
	nsSegSentinel byte = 0x03 // segID(4BE) → ""       (reconciliation sentinel)
)

const hashSize = 16 // Lo(8) + Hi(8)

// KeyIndex is a rebuildable Pebble-backed index that maps user keys to their
// 128-bit hashes and vice versa. It enables ordered iteration over cache entries
// without keeping user key bytes in RAM.
//
// The KeyIndex is NOT the source of truth — segments with .meta files are.
// If the KeyIndex is missing or corrupt, it is rebuilt from segment files on startup.
type KeyIndex struct {
	db   *pebble.DB
	path string
}

// KeyIndexEntry pairs a user key with its 128-bit hash for bulk insertion.
type KeyIndexEntry struct {
	UserKey []byte
	Hash    Key
}

// OpenKeyIndex opens or creates a Pebble DB for the key index at the given path.
// The DB is configured with conservative settings — it's a rebuildable cache,
// not a durability layer, so WAL is disabled and compaction is tuned for reads.
func OpenKeyIndex(path string) (*KeyIndex, error) {
	dir := filepath.Join(path, "keyindex")
	opts := &pebble.Options{
		MaxOpenFiles:             500,
		MemTableSize:             16 << 20, // 16MB — not write-heavy
		L0CompactionThreshold:    4,
		LBaseMaxBytes:            64 << 20, // 64MB
		DisableWAL:               true,     // Rebuildable — no WAL needed.
		FS:                       vfs.Default,
		MaxConcurrentCompactions: func() int { return 2 },
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("open keyindex: %w", err)
	}
	return &KeyIndex{db: db, path: dir}, nil
}

// Close closes the Pebble DB.
func (ki *KeyIndex) Close() error {
	if ki == nil || ki.db == nil {
		return nil
	}
	return ki.db.Close()
}

// AddEntries inserts user key ↔ hash mappings and segment membership records
// for a batch of entries. Also writes a sentinel for the segment ID.
// All writes are in a single Pebble batch (atomic).
func (ki *KeyIndex) AddEntries(segID uint32, entries []KeyIndexEntry) error {
	b := ki.db.NewBatch()
	defer b.Close()

	for i := range entries {
		e := &entries[i]
		hashKey := encodeHashLookupKey(e.Hash)
		userKeyKey := encodeUserKeyLookupKey(e.UserKey)
		memberKey := encodeSegMemberKey(segID, e.Hash)
		hashVal := encodeHashValue(e.Hash)

		if err := b.Set(hashKey, e.UserKey, pebble.NoSync); err != nil {
			return fmt.Errorf("set hash→key: %w", err)
		}
		if err := b.Set(userKeyKey, hashVal, pebble.NoSync); err != nil {
			return fmt.Errorf("set key→hash: %w", err)
		}
		if err := b.Set(memberKey, nil, pebble.NoSync); err != nil {
			return fmt.Errorf("set seg member: %w", err)
		}
	}

	// Write sentinel.
	sentinelKey := encodeSegSentinelKey(segID)
	if err := b.Set(sentinelKey, nil, pebble.NoSync); err != nil {
		return fmt.Errorf("set sentinel: %w", err)
	}

	return b.Commit(pebble.NoSync)
}

// DeleteByHash performs a reverse lookup (hash → user key) and then deletes
// the hash→key and key→hash entries. Used during eviction where only the
// 128-bit hash is available.
//
// Does NOT delete segment membership (0x02) records — those are cleaned up
// during drain when the entire segment is removed.
func (ki *KeyIndex) DeleteByHash(h Key) error {
	hashKey := encodeHashLookupKey(h)

	// Reverse lookup: hash → user key.
	userKey, closer, err := ki.db.Get(hashKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil // Already deleted or never existed.
		}
		return fmt.Errorf("get hash→key: %w", err)
	}
	// Copy userKey before closing since closer invalidates the slice.
	userKeyCopy := append([]byte(nil), userKey...)
	if err := closer.Close(); err != nil {
		return fmt.Errorf("close get: %w", err)
	}

	b := ki.db.NewBatch()
	defer b.Close()

	if err := b.Delete(hashKey, pebble.NoSync); err != nil {
		return fmt.Errorf("delete hash→key: %w", err)
	}
	userKeyKey := encodeUserKeyLookupKey(userKeyCopy)
	if err := b.Delete(userKeyKey, pebble.NoSync); err != nil {
		return fmt.Errorf("delete key→hash: %w", err)
	}

	return b.Commit(pebble.NoSync)
}

// DeleteByUserKey deletes the hash→key and key→hash entries when both the
// user key and hash are known. Used during explicit Delete() calls.
func (ki *KeyIndex) DeleteByUserKey(userKey []byte, h Key) error {
	b := ki.db.NewBatch()
	defer b.Close()

	if err := b.Delete(encodeHashLookupKey(h), pebble.NoSync); err != nil {
		return fmt.Errorf("delete hash→key: %w", err)
	}
	if err := b.Delete(encodeUserKeyLookupKey(userKey), pebble.NoSync); err != nil {
		return fmt.Errorf("delete key→hash: %w", err)
	}

	return b.Commit(pebble.NoSync)
}

// DrainSegment removes all entries associated with a segment ID.
// Iterates 0x02+segID prefix to find all hashes, then removes hash→key,
// key→hash, and segment membership records. Deletes the sentinel.
// All deletions are in a single batch.
func (ki *KeyIndex) DrainSegment(segID uint32) error {
	// Build prefix for segment membership keys: 0x02 + segID(4BE)
	prefix := make([]byte, 5)
	prefix[0] = nsSegMember
	binary.BigEndian.PutUint32(prefix[1:5], segID)

	// Upper bound for prefix iteration.
	upperBound := prefixUpperBound(prefix)

	iter, err := ki.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return fmt.Errorf("drain iter: %w", err)
	}

	// Collect all hashes in this segment.
	type hashAndUserKey struct {
		hash    Key
		userKey []byte
	}
	var entries []hashAndUserKey

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) < 5+hashSize {
			continue
		}
		h := decodeHash(k[5:])

		// Reverse lookup to get user key.
		hashKey := encodeHashLookupKey(h)
		userKey, closer, lookupErr := ki.db.Get(hashKey)
		if lookupErr == nil {
			entries = append(entries, hashAndUserKey{
				hash:    h,
				userKey: append([]byte(nil), userKey...),
			})
			_ = closer.Close()
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("drain iter close: %w", err)
	}

	b := ki.db.NewBatch()
	defer b.Close()

	for _, e := range entries {
		_ = b.Delete(encodeHashLookupKey(e.hash), pebble.NoSync)
		_ = b.Delete(encodeUserKeyLookupKey(e.userKey), pebble.NoSync)
		_ = b.Delete(encodeSegMemberKey(segID, e.hash), pebble.NoSync)
	}

	// Delete sentinel.
	_ = b.Delete(encodeSegSentinelKey(segID), pebble.NoSync)

	return b.Commit(pebble.NoSync)
}

// RelocateSegment updates segment membership records when a segment is rewritten
// during compaction. Moves hashes from oldSegID to newSegID.
func (ki *KeyIndex) RelocateSegment(oldSegID, newSegID uint32, hashes []Key) error {
	b := ki.db.NewBatch()
	defer b.Close()

	for _, h := range hashes {
		_ = b.Delete(encodeSegMemberKey(oldSegID, h), pebble.NoSync)
		_ = b.Set(encodeSegMemberKey(newSegID, h), nil, pebble.NoSync)
	}

	// Move sentinel.
	_ = b.Delete(encodeSegSentinelKey(oldSegID), pebble.NoSync)
	_ = b.Set(encodeSegSentinelKey(newSegID), nil, pebble.NoSync)

	return b.Commit(pebble.NoSync)
}

// HasSentinel checks if a segment has been loaded into the key index.
func (ki *KeyIndex) HasSentinel(segID uint32) (bool, error) {
	key := encodeSegSentinelKey(segID)
	_, closer, err := ki.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	_ = closer.Close()
	return true, nil
}

// SetSentinel marks a segment as loaded in the key index.
func (ki *KeyIndex) SetSentinel(segID uint32) error {
	return ki.db.Set(encodeSegSentinelKey(segID), nil, pebble.NoSync)
}

// NewSnapshotIter creates a Pebble snapshot and an iterator scoped to the
// userKey→hash (0x01) namespace, with bounds derived from user-space [lower, upper).
// The caller must close both the iterator and the snapshot when done.
// On error, both are nil and all cleanup has already been performed.
func (ki *KeyIndex) NewSnapshotIter(lower, upper []byte) (*pebble.Snapshot, *pebble.Iterator, error) {
	snap := ki.db.NewSnapshot()
	var lowerBound, upperBound []byte
	if lower != nil {
		lowerBound = make([]byte, 1+len(lower))
		lowerBound[0] = nsKeyToHash
		copy(lowerBound[1:], lower)
	} else {
		lowerBound = []byte{nsKeyToHash}
	}
	if upper != nil {
		upperBound = make([]byte, 1+len(upper))
		upperBound[0] = nsKeyToHash
		copy(upperBound[1:], upper)
	} else {
		// Stop before the next namespace (0x02).
		upperBound = []byte{nsKeyToHash + 1}
	}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		_ = snap.Close()
		return nil, nil, err
	}
	return snap, iter, nil
}

// EncodeSeekKey encodes a user key for use with pebble.Iterator.SeekGE.
func (ki *KeyIndex) EncodeSeekKey(userKey []byte) []byte {
	buf := make([]byte, 1+len(userKey))
	buf[0] = nsKeyToHash
	copy(buf[1:], userKey)
	return buf
}

// DecodeEntry decodes a (pebbleKey, pebbleValue) pair from the 0x01 namespace
// into a user key (namespace prefix stripped) and 128-bit hash.
// Returns ok=false if the value is too short (corrupt entry).
// The returned userKey is a fresh copy — safe to retain after iterator movement.
func (ki *KeyIndex) DecodeEntry(pebbleKey, pebbleValue []byte) (userKey []byte, hash Key, ok bool) {
	if len(pebbleValue) < hashSize {
		return nil, Key{}, false
	}
	userKey = make([]byte, len(pebbleKey)-1)
	copy(userKey, pebbleKey[1:])
	return userKey, decodeHash(pebbleValue), true
}

// --- Key encoding helpers ---

func encodeHashLookupKey(h Key) []byte {
	buf := make([]byte, 1+hashSize)
	buf[0] = nsHashToKey
	binary.LittleEndian.PutUint64(buf[1:9], h.Lo)
	binary.LittleEndian.PutUint64(buf[9:17], h.Hi)
	return buf
}

func encodeUserKeyLookupKey(userKey []byte) []byte {
	buf := make([]byte, 1+len(userKey))
	buf[0] = nsKeyToHash
	copy(buf[1:], userKey)
	return buf
}

func encodeSegMemberKey(segID uint32, h Key) []byte {
	buf := make([]byte, 1+4+hashSize)
	buf[0] = nsSegMember
	binary.BigEndian.PutUint32(buf[1:5], segID)
	binary.LittleEndian.PutUint64(buf[5:13], h.Lo)
	binary.LittleEndian.PutUint64(buf[13:21], h.Hi)
	return buf
}

func encodeSegSentinelKey(segID uint32) []byte {
	buf := make([]byte, 5)
	buf[0] = nsSegSentinel
	binary.BigEndian.PutUint32(buf[1:5], segID)
	return buf
}

func encodeHashValue(h Key) []byte {
	buf := make([]byte, hashSize)
	binary.LittleEndian.PutUint64(buf[0:8], h.Lo)
	binary.LittleEndian.PutUint64(buf[8:16], h.Hi)
	return buf
}

func decodeHash(b []byte) Key {
	return Key{
		Lo: binary.LittleEndian.Uint64(b[0:8]),
		Hi: binary.LittleEndian.Uint64(b[8:16]),
	}
}

// prefixUpperBound returns the immediate successor of prefix for Pebble iteration.
// e.g., []byte{0x02, 0x00, 0x01} → []byte{0x02, 0x00, 0x02}
func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 {
			return upper
		}
	}
	return nil // All 0xFF — no upper bound needed.
}
