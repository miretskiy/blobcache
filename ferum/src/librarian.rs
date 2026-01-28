//! Lock-free read-after-write cache (the "Visible History" / "Catalog").
//!
//! The Librarian maintains a list of recently-written SharedSlabs for fast
//! read-after-write access. Uses atomic pointer swapping and reference counting
//! for wait-free reads.
//!
//! # Design
//!
//! - **Lock-free**: Uses `ArcSwap` for atomic pointer swapping, no mutexes
//! - **Wait-free reads**: Readers just load the atomic pointer and iterate
//! - **Safe eviction**: Reference counting via `Arc` prevents use-after-free
//! - **Bounded memory**: Configurable max cached slabs with automatic eviction
//! - **Zero-copy**: Direct access to mmap'd slab buffers

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::error::BlobErrno;
use crate::key::Key;
use crate::slab::SharedSlab;

// =============================================================================
// Publisher trait
// =============================================================================

/// Trait for publishing slabs to a cache.
pub trait Publisher {
    /// Publishes a slab to the cache.
    fn publish(&self, slab: SharedSlab);
}

// =============================================================================
// AcquireResult
// =============================================================================

/// Result of acquiring data from the Librarian.
pub struct AcquireResult {
    /// The value data (possibly decompressed).
    pub value: Vec<u8>,
    /// The stored key bytes for collision detection.
    pub stored_key: Vec<u8>,
    /// The slab containing the data (keeps buffer pinned).
    slab: SharedSlab,
}

impl AcquireResult {
    /// Releases the acquired data.
    pub fn release(self) {
        // Slab is dropped here, which will call unpin on the buffer
        drop(self.slab);
    }
}

// =============================================================================
// Librarian
// =============================================================================

/// Manages the "Visible History" (The Catalog) of Slabs.
///
/// LOCK-FREE: Uses atomic pointer swapping and safe reference counting.
/// Readers load an immutable snapshot and iterate - completely wait-free.
/// Writers use CAS loop to atomically install new snapshots.
pub struct Librarian {
    /// The current slab list - atomic pointer to immutable Arc<Vec>.
    view: ArcSwap<Vec<SharedSlab>>,
    /// Whether the librarian has been closed.
    closed: AtomicBool,
    /// Maximum number of slabs to cache.
    max_cached: usize,
}

impl Librarian {
    /// Creates a new Librarian with the specified cache size.
    ///
    /// Setting `max_cached` to 0 disables caching.
    pub fn new(max_cached: usize) -> Self {
        Librarian {
            view: ArcSwap::from_pointee(Vec::new()),
            closed: AtomicBool::new(false),
            max_cached,
        }
    }

    /// Returns true if the librarian is disabled (max_cached = 0).
    pub fn is_disabled(&self) -> bool {
        self.max_cached == 0
    }

    /// Publishes a slab to the cache.
    ///
    /// The slab is prepended to the list (newest first). If the list exceeds
    /// `max_cached`, the oldest slab is evicted.
    ///
    /// Uses a CAS loop to ensure linearizable updates even under concurrent publish.
    pub fn publish(&self, slab: SharedSlab) {
        // Disabled: do nothing
        if self.max_cached == 0 {
            return;
        }

        // SharedSlab contains Arc<MmapBuffer> - Arc::clone is safe and increments ref count.
        // No try_inc needed.

        // Optimistic Update Loop (Compare-And-Swap)
        // Ensures linear history even if Publish is called concurrently
        // (though usually MemTable is the single producer).
        loop {
            let old_guard = self.view.load();
            let old_list: &Vec<SharedSlab> = &old_guard;

            // Build new list with new slab prepended (newest first)
            let mut new_list = Vec::with_capacity(old_list.len() + 1);
            new_list.push(slab.clone());
            new_list.extend(old_list.iter().cloned());

            // Evict oldest if over limit
            let victim = if new_list.len() > self.max_cached {
                new_list.pop()
            } else {
                None
            };

            // Attempt atomic swap
            let new_arc = Arc::new(new_list);
            let result = self
                .view
                .compare_and_swap(&old_guard, Arc::clone(&new_arc));

            if Arc::ptr_eq(&result, &old_guard) {
                // Success! We installed the new view.
                // Victim (if any) is dropped automatically, releasing the Arc reference.
                drop(victim);
                return;
            }
            // CAS failed, retry
        }
    }

    /// Searches the catalog for the key.
    ///
    /// WAIT-FREE: No mutexes, just an atomic pointer load.
    ///
    /// Returns the value and stored key if found. The stored key should be
    /// verified against the expected key to detect 128-bit hash collisions.
    pub fn acquire(&self, hash_key: Key) -> Result<Option<AcquireResult>, BlobErrno> {
        // 1. Load the immutable snapshot (wait-free)
        let list = self.view.load();

        // 2. Iterate through slabs
        for slab in list.iter() {
            // 3. Attempt to acquire from this slab.
            // Arc-based reference counting ensures the slab buffer remains valid
            // even if 'Publish' evicts this slab while we are iterating.
            match slab.acquire(&hash_key) {
                Ok(Some(result)) => {
                    return Ok(Some(AcquireResult {
                        value: result.value,
                        stored_key: result.stored_key,
                        slab: slab.clone(),
                    }));
                }
                Ok(None) => continue, // Not found in this slab
                Err(errno) => return Err(errno), // Error (e.g., decompression failure)
            }
        }

        Ok(None) // Not found in any slab
    }

    /// Protected view that handles lifecycle automatically via a closure.
    ///
    /// The callback receives (stored_key, value). Data is valid only for the
    /// duration of the function.
    pub fn protected_view<F>(&self, hash_key: Key, mut callback: F) -> Result<bool, BlobErrno>
    where
        F: FnMut(&[u8], &[u8]),
    {
        match self.acquire(hash_key)? {
            Some(result) => {
                callback(&result.stored_key, &result.value);
                result.release();
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Invalidates a key from all slabs in the Librarian.
    ///
    /// Used by Delete to prevent serving stale data from cache after deletion.
    pub fn invalidate(&self, key: &Key) {
        let list = self.view.load();
        for slab in list.iter() {
            slab.invalidate(key);
        }
    }

    /// Returns the number of currently cached slabs.
    pub fn len(&self) -> usize {
        self.view.load().len()
    }

    /// Returns true if no slabs are cached.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Closes the librarian, releasing all cached slabs.
    pub fn close(&self) {
        if self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // Replace with empty list - old list is dropped, releasing all Arc references.
            self.view.swap(Arc::new(Vec::new()));
        }
    }
}

impl Publisher for Librarian {
    fn publish(&self, slab: SharedSlab) {
        Librarian::publish(self, slab);
    }
}

impl Drop for Librarian {
    fn drop(&mut self) {
        // Ensure close is called
        self.close();
    }
}

// =============================================================================
// DisabledLibrarian
// =============================================================================

/// A no-op publisher that discards all published slabs.
pub struct DisabledLibrarian;

impl Publisher for DisabledLibrarian {
    fn publish(&self, _slab: SharedSlab) {
        // Do nothing
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mempool::MmapBuffer;
    use crate::record::Header;
    use crate::slab::{SlabEntry, SlabIndex};

    fn create_test_slab(key: &[u8], value: &[u8]) -> SharedSlab {
        use crate::record::HEADER_SIZE;

        // Create a buffer with a record
        let record_size = HEADER_SIZE + key.len() + value.len();
        let buf = MmapBuffer::new(record_size);

        // Write header
        let header = Header::new(1, key.len() as u16, value.len() as i64, value.len() as i64);
        let mut header_bytes = [0u8; HEADER_SIZE];
        header.encode(&mut header_bytes).unwrap();
        buf.write_at(&header_bytes, 0);

        // Write key
        buf.write_at(key, HEADER_SIZE);

        // Write value
        buf.write_at(value, HEADER_SIZE + key.len());

        // Create index entry
        let index = Arc::new(SlabIndex::new());
        let entry = SlabEntry {
            flags: 0,
            seq_id: 1,
            key_len: key.len() as u16,
            physical_size: value.len() as i64,
            logical_size: value.len() as i64,
            pos: 0,
            wal_pos: 0,
            xl_buf: None,
        };
        index.insert(Key::from_bytes(key), entry);

        SharedSlab::new(buf, index)
    }

    #[test]
    fn test_librarian_basic() {
        let librarian = Librarian::new(4);
        assert!(librarian.is_empty());

        let slab = create_test_slab(b"key1", b"value1");
        librarian.publish(slab);

        assert_eq!(librarian.len(), 1);
        assert!(!librarian.is_empty());
    }

    #[test]
    fn test_librarian_disabled() {
        let librarian = Librarian::new(0);
        assert!(librarian.is_disabled());

        let slab = create_test_slab(b"key1", b"value1");
        librarian.publish(slab);

        // Should still be empty since disabled
        assert!(librarian.is_empty());
    }

    #[test]
    fn test_librarian_eviction() {
        let librarian = Librarian::new(2);

        // Publish 3 slabs
        for i in 0..3 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let slab = create_test_slab(key.as_bytes(), value.as_bytes());
            librarian.publish(slab);
        }

        // Should only keep 2
        assert_eq!(librarian.len(), 2);
    }

    #[test]
    fn test_librarian_acquire() {
        let librarian = Librarian::new(4);

        let slab = create_test_slab(b"testkey", b"testvalue");
        librarian.publish(slab);

        // Should find the key
        let key = Key::from_bytes(b"testkey");
        let result = librarian.acquire(key).unwrap();
        assert!(result.is_some());

        let data = result.unwrap();
        assert_eq!(data.stored_key, b"testkey");
        assert_eq!(data.value, b"testvalue");
        data.release();
    }

    #[test]
    fn test_librarian_acquire_not_found() {
        let librarian = Librarian::new(4);

        let slab = create_test_slab(b"key1", b"value1");
        librarian.publish(slab);

        // Should not find different key
        let key = Key::from_bytes(b"nonexistent");
        let result = librarian.acquire(key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_librarian_invalidate() {
        let librarian = Librarian::new(4);

        let slab = create_test_slab(b"key1", b"value1");
        librarian.publish(slab);

        // Should find before invalidate
        let key = Key::from_bytes(b"key1");
        let result = librarian.acquire(key).unwrap();
        assert!(result.is_some());
        result.unwrap().release();

        // Invalidate
        librarian.invalidate(&key);

        // Should not find after invalidate
        let result = librarian.acquire(key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_librarian_close() {
        let librarian = Librarian::new(4);

        let slab = create_test_slab(b"key1", b"value1");
        librarian.publish(slab);
        assert_eq!(librarian.len(), 1);

        librarian.close();
        assert!(librarian.is_empty());
    }

    #[test]
    fn test_librarian_protected_view() {
        let librarian = Librarian::new(4);

        let slab = create_test_slab(b"mykey", b"myvalue");
        librarian.publish(slab);

        let mut captured_key = Vec::new();
        let mut captured_value = Vec::new();

        let found = librarian
            .protected_view(Key::from_bytes(b"mykey"), |k, v| {
                captured_key = k.to_vec();
                captured_value = v.to_vec();
            })
            .unwrap();

        assert!(found);
        assert_eq!(captured_key, b"mykey");
        assert_eq!(captured_value, b"myvalue");
    }

    #[test]
    fn test_librarian_concurrent_publish() {
        use std::thread;

        let librarian = Arc::new(Librarian::new(100));
        let mut handles = vec![];

        for i in 0..10 {
            let lib = Arc::clone(&librarian);
            handles.push(thread::spawn(move || {
                for j in 0..10 {
                    let key = format!("key-{}-{}", i, j);
                    let value = format!("value-{}-{}", i, j);
                    let slab = create_test_slab(key.as_bytes(), value.as_bytes());
                    lib.publish(slab);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Should have at most max_cached slabs
        assert!(librarian.len() <= 100);
    }
}
