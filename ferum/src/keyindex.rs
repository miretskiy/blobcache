//! redb-backed global key index for ordered iteration.
//!
//! The KeyIndex maps between user-supplied byte keys and the 128-bit hash keys
//! used internally. It is separate from the durable index (which is hash-keyed)
//! and is only needed when the caller wants ordered iteration or key-by-name
//! lookup.
//!
//! # Tables
//!
//! - `key_to_hash`:  user_key (&[u8]) → hash_bytes ([u8; 16])
//! - `hash_to_key`:  hash_bytes ([u8; 16]) → user_key (&[u8])
//! - `seg_members`:  (seg_id[4BE] ++ hash[16]) → ()  (for bulk drain by segment)
//! - `seg_sentinel`: seg_id[4BE] → ()  (marks that a segment is fully indexed)

use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadTransaction, ReadableTable, TableDefinition};

use crate::error::{Error, Result};
use crate::key::Key;

// =============================================================================
// Table definitions
// =============================================================================

const KEY_TO_HASH: TableDefinition<&[u8], [u8; 16]> = TableDefinition::new("key_to_hash");
const HASH_TO_KEY: TableDefinition<[u8; 16], &[u8]> = TableDefinition::new("hash_to_key");
/// Composite key: seg_id (4 bytes BE) ++ hash (16 bytes) → ()
const SEG_MEMBERS: TableDefinition<[u8; 20], ()> = TableDefinition::new("seg_members");
/// Sentinel: seg_id (4 bytes BE) → ()
const SEG_SENTINEL: TableDefinition<[u8; 4], ()> = TableDefinition::new("seg_sentinel");

fn seg_id_bytes(seg_id: u32) -> [u8; 4] {
    seg_id.to_be_bytes()
}

fn member_key(seg_id: u32, hash: Key) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[..4].copy_from_slice(&seg_id.to_be_bytes());
    k[4..].copy_from_slice(&hash.to_bytes());
    k
}

fn redb_err_str(op: &str, e: impl std::fmt::Display) -> Error {
    Error::InvalidConfig {
        message: format!("{}: {}", op, e),
    }
}

macro_rules! re {
    ($op:expr, $expr:expr) => {
        $expr.map_err(|e| redb_err_str($op, e))
    };
}

// =============================================================================
// KeyIndexEntry
// =============================================================================

/// An entry to be inserted into the KeyIndex.
#[derive(Debug, Clone)]
pub struct KeyIndexEntry {
    /// The 128-bit hash (index key).
    pub hash: Key,
    /// The original user-supplied key bytes.
    pub user_key: Vec<u8>,
}

// =============================================================================
// KeyIndex
// =============================================================================

/// redb-backed global key index for ordered iteration.
pub struct KeyIndex {
    db: Arc<Database>,
}

impl KeyIndex {
    /// Opens or creates the KeyIndex database at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        let db = re!("open keyindex", Database::create(path))?;

        // Ensure all tables exist
        let txn = re!("begin_write", db.begin_write())?;
        re!("create key_to_hash", txn.open_table(KEY_TO_HASH))?;
        re!("create hash_to_key", txn.open_table(HASH_TO_KEY))?;
        re!("create seg_members", txn.open_table(SEG_MEMBERS))?;
        re!("create seg_sentinel", txn.open_table(SEG_SENTINEL))?;
        re!("init commit", txn.commit())?;

        Ok(KeyIndex { db: Arc::new(db) })
    }

    /// Inserts a batch of entries for a segment atomically.
    ///
    /// Also writes the sentinel to mark the segment as fully indexed.
    pub fn add_entries(&self, seg_id: u32, entries: &[KeyIndexEntry]) -> Result<()> {
        let txn = re!("begin_write", self.db.begin_write())?;

        {
            let mut k2h = re!("open key_to_hash", txn.open_table(KEY_TO_HASH))?;
            let mut h2k = re!("open hash_to_key", txn.open_table(HASH_TO_KEY))?;
            let mut members = re!("open seg_members", txn.open_table(SEG_MEMBERS))?;
            let mut sentinel = re!("open seg_sentinel", txn.open_table(SEG_SENTINEL))?;

            for entry in entries {
                let hash_bytes = entry.hash.to_bytes();
                re!("insert key_to_hash", k2h.insert(entry.user_key.as_slice(), hash_bytes))?;
                re!("insert hash_to_key", h2k.insert(hash_bytes, entry.user_key.as_slice()))?;
                re!("insert seg_members", members.insert(member_key(seg_id, entry.hash), ()))?;
            }

            re!("insert seg_sentinel", sentinel.insert(seg_id_bytes(seg_id), ()))?;
        }

        re!("add_entries commit", txn.commit())?;
        Ok(())
    }

    /// Deletes a single entry by hash (used during eviction).
    pub fn delete_by_hash(&self, h: Key) -> Result<()> {
        let txn = re!("begin_write", self.db.begin_write())?;

        {
            let mut k2h = re!("open key_to_hash", txn.open_table(KEY_TO_HASH))?;
            let mut h2k = re!("open hash_to_key", txn.open_table(HASH_TO_KEY))?;

            let hash_bytes = h.to_bytes();

            // Look up user_key via hash, then delete both directions
            if let Some(v) = re!("get hash_to_key", h2k.get(hash_bytes))? {
                let user_key = v.value().to_vec();
                drop(v);
                re!("remove key_to_hash", k2h.remove(user_key.as_slice()))?;
            }
            re!("remove hash_to_key", h2k.remove(hash_bytes))?;
        }

        re!("delete_by_hash commit", txn.commit())?;
        Ok(())
    }

    /// Drains all entries for a segment (used during segment drain/compaction).
    pub fn drain_segment(&self, seg_id: u32) -> Result<()> {
        let txn = re!("begin_write", self.db.begin_write())?;

        {
            let mut k2h = re!("open key_to_hash", txn.open_table(KEY_TO_HASH))?;
            let mut h2k = re!("open hash_to_key", txn.open_table(HASH_TO_KEY))?;
            let mut members = re!("open seg_members", txn.open_table(SEG_MEMBERS))?;
            let mut sentinel = re!("open seg_sentinel", txn.open_table(SEG_SENTINEL))?;

            // Range scan SEG_MEMBERS for this segment's prefix
            let prefix_start = member_key(seg_id, Key::zero());
            let prefix_end = member_key(seg_id.saturating_add(1), Key::zero());

            // Collect member keys to avoid borrow issues
            let to_remove: Vec<[u8; 20]> = re!("range seg_members", members.range(prefix_start..prefix_end))?
                .map(|r| r.map(|(k, _)| k.value()).map_err(|e| redb_err_str("iter seg_members", e)))
                .collect::<Result<Vec<_>>>()?;

            for member_k in to_remove {
                let hash_bytes: [u8; 16] = member_k[4..].try_into().unwrap();

                // Delete from hash_to_key and key_to_hash
                if let Some(v) = re!("get h2k", h2k.get(hash_bytes))? {
                    let user_key = v.value().to_vec();
                    drop(v);
                    re!("remove k2h", k2h.remove(user_key.as_slice()))?;
                }
                re!("remove h2k", h2k.remove(hash_bytes))?;
                re!("remove member", members.remove(member_k))?;
            }

            re!("remove sentinel", sentinel.remove(seg_id_bytes(seg_id)))?;
        }

        re!("drain_segment commit", txn.commit())?;
        Ok(())
    }

    /// Returns true if the segment has been fully indexed (sentinel exists).
    pub fn has_sentinel(&self, seg_id: u32) -> bool {
        let txn = match self.db.begin_read() {
            Ok(t) => t,
            Err(_) => return false,
        };
        let table = match txn.open_table(SEG_SENTINEL) {
            Ok(t) => t,
            Err(_) => return false,
        };
        table.get(seg_id_bytes(seg_id))
            .map(|v| v.is_some())
            .unwrap_or(false)
    }

    /// Starts a read transaction for snapshot-isolated iteration.
    pub fn new_read_txn(&self) -> Result<ReadTransaction> {
        re!("begin_read", self.db.begin_read())
    }

    /// Looks up the hash for a user key.
    pub fn get_hash(&self, user_key: &[u8]) -> Result<Option<Key>> {
        let txn = re!("begin_read", self.db.begin_read())?;
        let table = re!("open key_to_hash", txn.open_table(KEY_TO_HASH))?;
        let result = re!("get key_to_hash", table.get(user_key))?;
        Ok(result.map(|v| Key::decode(&v.value())))
    }

    /// Looks up the user key for a hash.
    pub fn get_user_key(&self, h: Key) -> Result<Option<Vec<u8>>> {
        let txn = re!("begin_read", self.db.begin_read())?;
        let table = re!("open hash_to_key", txn.open_table(HASH_TO_KEY))?;
        let result = re!("get hash_to_key", table.get(h.to_bytes()))?;
        Ok(result.map(|v| v.value().to_vec()))
    }

    /// Scans key_to_hash in lexicographic order.
    ///
    /// The callback receives `(user_key, hash)` for each live entry.
    /// Returns early if the callback returns false.
    pub fn scan<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], Key) -> bool,
    {
        let txn = re!("begin_read", self.db.begin_read())?;
        let table = re!("open key_to_hash", txn.open_table(KEY_TO_HASH))?;
        for entry in re!("iter key_to_hash", table.iter())? {
            let (k, v) = re!("next key_to_hash", entry)?;
            if !f(k.value(), Key::decode(&v.value())) {
                break;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_key(s: &str) -> Key {
        Key::from_bytes(s.as_bytes())
    }

    #[test]
    fn test_keyindex_basic() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("keyindex.redb");
        let ki = KeyIndex::open(&db_path).unwrap();

        let entries = vec![
            KeyIndexEntry { hash: test_key("key1"), user_key: b"key1".to_vec() },
            KeyIndexEntry { hash: test_key("key2"), user_key: b"key2".to_vec() },
        ];

        ki.add_entries(1, &entries).unwrap();

        assert!(ki.has_sentinel(1));
        assert!(!ki.has_sentinel(2));

        let h = ki.get_hash(b"key1").unwrap().unwrap();
        assert_eq!(h, test_key("key1"));

        let uk = ki.get_user_key(test_key("key2")).unwrap().unwrap();
        assert_eq!(uk, b"key2");
    }

    #[test]
    fn test_keyindex_drain_segment() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("keyindex.redb");
        let ki = KeyIndex::open(&db_path).unwrap();

        let entries = vec![
            KeyIndexEntry { hash: test_key("a"), user_key: b"a".to_vec() },
            KeyIndexEntry { hash: test_key("b"), user_key: b"b".to_vec() },
        ];
        ki.add_entries(5, &entries).unwrap();
        assert!(ki.has_sentinel(5));

        ki.drain_segment(5).unwrap();
        assert!(!ki.has_sentinel(5));
        assert!(ki.get_hash(b"a").unwrap().is_none());
        assert!(ki.get_hash(b"b").unwrap().is_none());
    }

    #[test]
    fn test_keyindex_scan_order() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("keyindex.redb");
        let ki = KeyIndex::open(&db_path).unwrap();

        // Insert in non-lexicographic order
        let entries = vec![
            KeyIndexEntry { hash: test_key("banana"), user_key: b"banana".to_vec() },
            KeyIndexEntry { hash: test_key("apple"),  user_key: b"apple".to_vec() },
            KeyIndexEntry { hash: test_key("cherry"), user_key: b"cherry".to_vec() },
        ];
        ki.add_entries(1, &entries).unwrap();

        let mut keys: Vec<Vec<u8>> = Vec::new();
        ki.scan(|k, _| { keys.push(k.to_vec()); true }).unwrap();

        let expected: Vec<Vec<u8>> = vec![b"apple".to_vec(), b"banana".to_vec(), b"cherry".to_vec()];
        assert_eq!(keys, expected);
    }

    #[test]
    fn test_keyindex_delete_by_hash() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("keyindex.redb");
        let ki = KeyIndex::open(&db_path).unwrap();

        let entries = vec![
            KeyIndexEntry { hash: test_key("mykey"), user_key: b"mykey".to_vec() },
        ];
        ki.add_entries(1, &entries).unwrap();

        ki.delete_by_hash(test_key("mykey")).unwrap();
        assert!(ki.get_hash(b"mykey").unwrap().is_none());
        assert!(ki.get_user_key(test_key("mykey")).unwrap().is_none());
    }
}
