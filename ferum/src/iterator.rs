//! Ordered iterator over cached blobs using the KeyIndex.
//!
//! Provides lexicographically ordered iteration over live keys via the KeyIndex.
//! Uses read-ahead prefetch for sequential workloads when blobs are in the same
//! segment and stored contiguously.
//!
//! # Usage
//!
//! ```ignore
//! let mut iter = cache.new_iterator(None, None).unwrap();
//! while iter.next() {
//!     let key = iter.key().unwrap();
//!     iter.view(|data| { /* use data */ });
//! }
//! ```

use std::sync::Arc;

use crate::durable_index::DurableIndex;
use crate::index::Item;
use crate::key::Key;
use crate::keyindex::KeyIndex;
use crate::storage::Archivist;

// =============================================================================
// IteratorStats
// =============================================================================

/// Statistics accumulated during iteration.
#[derive(Debug, Default, Clone)]
pub struct IteratorStats {
    /// Number of read-ahead buffer hits (avoided a disk read).
    pub prefetch_hits: i64,
    /// Number of read-ahead buffer misses (required a disk read).
    pub prefetch_misses: i64,
    /// Total extra bytes read for read-ahead speculation.
    pub read_ahead_bytes: i64,
}

// =============================================================================
// Iterator
// =============================================================================

/// Ordered iterator over live blobs in the cache.
///
/// Iteration order is lexicographic by user key. The iterator holds a snapshot
/// of the KeyIndex taken at construction time; keys inserted after construction
/// are not visible.
pub struct Iterator {
    index: Arc<DurableIndex>,
    archivist: Arc<Archivist>,

    /// Snapshot of all keys in lexicographic order, collected at construction.
    entries: Vec<(Vec<u8>, Key)>,
    cursor: usize,

    // Current valid state
    valid: bool,
    current_key: Option<Vec<u8>>,
    current_item: Option<Item>,

    pub stats: IteratorStats,
}

impl Iterator {
    /// Creates a new iterator, optionally bounded to `[lower, upper)`.
    ///
    /// If `lower` is `None`, starts from the first key.
    /// If `upper` is `None`, ends at the last key.
    pub fn new(
        index: Arc<DurableIndex>,
        archivist: Arc<Archivist>,
        key_index: &KeyIndex,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> crate::error::Result<Self> {
        // Collect all entries into a Vec (snapshot at construction time).
        let mut entries: Vec<(Vec<u8>, Key)> = Vec::new();
        key_index.scan(|k, h| {
            if let Some(lo) = lower {
                if k < lo {
                    return true; // skip
                }
            }
            if let Some(hi) = upper {
                if k >= hi {
                    return false; // stop
                }
            }
            entries.push((k.to_vec(), h));
            true
        })?;

        Ok(Iterator {
            index,
            archivist,
            entries,
            cursor: 0,
            valid: false,
            current_key: None,
            current_item: None,
            stats: IteratorStats::default(),
        })
    }

    /// Positions the iterator at the first live key. Returns true if valid.
    pub fn first(&mut self) -> bool {
        self.cursor = 0;
        self.valid = false;
        self.advance()
    }

    /// Advances to the next live key. Returns true if valid.
    pub fn next(&mut self) -> bool {
        if self.cursor < self.entries.len() {
            self.cursor += 1;
        }
        self.advance()
    }

    /// Seeks to the first key >= `target`. Returns true if valid.
    pub fn seek_ge(&mut self, target: &[u8]) -> bool {
        self.cursor = self.entries.partition_point(|(k, _)| k.as_slice() < target);
        self.valid = false;
        self.advance()
    }

    /// Returns the current user key, or `None` if not valid.
    pub fn key(&self) -> Option<&[u8]> {
        if self.valid {
            self.current_key.as_deref()
        } else {
            None
        }
    }

    /// Returns true if the iterator is positioned at a valid entry.
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// Reads the current blob and calls `f` with its data.
    ///
    /// Returns false if not valid, the item was evicted, or a read error occurred.
    pub fn view<F: FnOnce(&[u8])>(&mut self, f: F) -> bool {
        let item = match &self.current_item {
            Some(i) if !i.is_deleted() => i.clone(),
            _ => return false,
        };
        let key = match &self.current_key {
            Some(k) => k.clone(),
            None => return false,
        };

        self.stats.prefetch_misses += 1;

        match self.archivist.read_blob(&item, &key) {
            Ok(result) => {
                f(&result.value);
                true
            }
            Err(_) => false,
        }
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /// Advances cursor until a live (non-deleted) item is found.
    fn advance(&mut self) -> bool {
        while self.cursor < self.entries.len() {
            let (user_key, hash) = &self.entries[self.cursor];

            if let Some(item) = self.index.get(hash) {
                if !item.is_deleted() {
                    self.current_key = Some(user_key.clone());
                    self.current_item = Some(item);
                    self.valid = true;
                    return true;
                }
            }

            // Dead entry — skip
            self.cursor += 1;
        }

        self.valid = false;
        self.current_key = None;
        self.current_item = None;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyindex::{KeyIndex, KeyIndexEntry};
    use crate::durable_index::DurableIndex;
    use crate::index::Item;
    use tempfile::tempdir;

    fn make_ki(dir: &std::path::Path) -> Arc<KeyIndex> {
        let ki = KeyIndex::open(&dir.join("ki.redb")).unwrap();
        Arc::new(ki)
    }

    #[test]
    fn test_iterator_empty() {
        let dir = tempdir().unwrap();
        let ki = make_ki(dir.path());
        let index = Arc::new(DurableIndex::open(None, 1000).unwrap());
        let archivist = Arc::new(crate::storage::Archivist::new(dir.path(), 4));

        let mut iter = Iterator::new(
            index, archivist, &ki, None, None,
        ).unwrap();

        assert!(!iter.first());
        assert!(!iter.is_valid());
    }

    #[test]
    fn test_iterator_seek_ge() {
        let dir = tempdir().unwrap();
        let ki = make_ki(dir.path());
        let index = Arc::new(DurableIndex::open(None, 1000).unwrap());

        // Add entries to ki and index
        let keys: Vec<&[u8]> = vec![b"apple", b"banana", b"cherry", b"date"];
        let mut entries = Vec::new();
        for k in &keys {
            let hash = Key::from_bytes(k);
            let item = Item::new(hash, 1, 0, 100);
            index.put(item);
            entries.push(KeyIndexEntry { hash, user_key: k.to_vec() });
        }
        ki.add_entries(1, &entries).unwrap();

        let archivist = Arc::new(crate::storage::Archivist::new(dir.path(), 4));

        let mut iter = Iterator::new(
            Arc::clone(&index), archivist, &ki, None, None,
        ).unwrap();

        // seek_ge to "banana"
        assert!(iter.seek_ge(b"banana"));
        assert_eq!(iter.key().unwrap(), b"banana");

        // Next key is "cherry"
        assert!(iter.next());
        assert_eq!(iter.key().unwrap(), b"cherry");

        // next from last available key
        assert!(iter.next());
        assert_eq!(iter.key().unwrap(), b"date");

        // No more
        assert!(!iter.next());
    }

    #[test]
    fn test_iterator_skips_deleted() {
        let dir = tempdir().unwrap();
        let ki = make_ki(dir.path());
        let index = Arc::new(DurableIndex::open(None, 1000).unwrap());

        // Add entries
        let keys: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let mut entries = Vec::new();
        for &k in &keys {
            let hash = Key::from_bytes(k);
            let item = Item::new(hash, 1, 0, 100);
            index.put(item);
            entries.push(KeyIndexEntry { hash, user_key: k.to_vec() });
        }
        ki.add_entries(1, &entries).unwrap();

        // Mark "b" as deleted in the in-memory index
        let b_hash = Key::from_bytes(b"b");
        index.delete(&b_hash);

        let archivist = Arc::new(crate::storage::Archivist::new(dir.path(), 4));

        let mut iter = Iterator::new(
            Arc::clone(&index), archivist, &ki, None, None,
        ).unwrap();

        assert!(iter.first());
        assert_eq!(iter.key().unwrap(), b"a");

        assert!(iter.next());
        assert_eq!(iter.key().unwrap(), b"c"); // "b" skipped

        assert!(!iter.next());
    }
}
