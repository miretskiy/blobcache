//! Durable index combining in-memory SIEVE index with persistent storage.
//!
//! This module provides crash-consistent index storage:
//! - `BlobIndex`: In-memory sharded hash table with SIEVE eviction
//! - `Persistence`: redb-based durable storage
//! - `SegmentMetadataMap`: Per-segment tracking for compaction heuristics and coordination
//!
//! On startup, items are loaded from persistence into memory.
//! On writes, items are written to both memory and persistence.
//!
//! # Segment Coordination
//!
//! The `SegmentMetadataMap` provides segment-level locking:
//! - Delete: exclusive lock (blocks compaction of that segment)
//! - Compaction: shared lock (multiple compactions allowed)
//!
//! This prevents races between delete operations and compaction.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::error::Result;
use crate::index::{BlobIndex, IndexStats, Item, RelocationRequest};
use crate::key::Key;
use crate::persistence::{
    Persistence, SegmentExclusiveGuard, SegmentMetadataMap, SegmentMetadataSnapshot,
    SegmentSharedGuard, TombstoneFn,
};

/// Durable index combining in-memory and persistent storage.
pub struct DurableIndex {
    /// In-memory SIEVE index for fast lookups.
    blobs: Arc<BlobIndex>,
    /// Optional persistence layer (None = cache mode, no durability).
    persistence: Option<Persistence>,
    /// Per-segment metadata for compaction heuristics and coordination.
    segment_meta: SegmentMetadataMap,
}

impl DurableIndex {
    /// Opens a durable index, loading items from persistence if available.
    ///
    /// # Arguments
    /// * `path` - Directory for the persistence database (if None, runs without persistence)
    /// * `capacity` - Maximum number of items in the in-memory index
    pub fn open(path: Option<&Path>, capacity: usize) -> Result<Self> {
        let blobs = Arc::new(BlobIndex::new(capacity));
        let segment_meta = SegmentMetadataMap::new();

        let persistence = if let Some(p) = path {
            let db_path = p.join("index.redb");
            let pers = Persistence::open(&db_path)?;

            // Load all items into memory and track segment metadata
            pers.scan_all(|batch| {
                let mut live_count = 0i32;
                let mut physical_bytes = 0i64;

                for item in &batch.items {
                    if !item.is_deleted() {
                        blobs.put(*item);
                        live_count += 1;
                        physical_bytes += item.physical_len as i64;
                    }
                }

                // Initialize segment metadata
                if live_count > 0 || !batch.items.is_empty() {
                    segment_meta.update(batch.segment_id, |meta| {
                        meta.live_item_count
                            .fetch_add(live_count, std::sync::atomic::Ordering::Relaxed);
                        meta.physical_bytes
                            .fetch_add(physical_bytes, std::sync::atomic::Ordering::Relaxed);
                        meta.logical_bytes
                            .fetch_add(physical_bytes, std::sync::atomic::Ordering::Relaxed);
                    });
                }
                true
            })?;

            Some(pers)
        } else {
            None
        };

        Ok(DurableIndex {
            blobs,
            persistence,
            segment_meta,
        })
    }

    /// Gets an item by key from the in-memory index.
    #[inline]
    pub fn get(&self, key: &Key) -> Option<Item> {
        self.blobs.get(key)
    }

    /// Puts an item into the in-memory index only.
    ///
    /// Use `ingest_batch` for durable writes.
    #[inline]
    pub fn put(&self, item: Item) {
        self.blobs.put(item);
    }

    /// Puts a batch of items into memory only.
    ///
    /// Use `ingest_batch` for durable writes.
    #[inline]
    pub fn put_batch(&self, items: &[Item], max_seq_id: u64) {
        self.blobs.put_batch(items, max_seq_id);
    }

    /// Ingests a batch of items, writing to both persistence and memory.
    ///
    /// This is the durable write path used after flushing a slab.
    /// Also initializes segment metadata for compaction tracking.
    pub fn ingest_batch(&self, seg_id: u32, items: &[Item], max_seq_id: u64) -> Result<()> {
        // Write to persistence first (for durability)
        if let Some(ref pers) = self.persistence {
            pers.write_batch(seg_id, items, max_seq_id)?;
        }

        // Then update in-memory index (use batch operation for efficiency)
        self.blobs.put_batch(items, max_seq_id);

        // Update segment metadata
        let live_count = items.iter().filter(|i| !i.is_deleted()).count() as i32;
        let physical_bytes: i64 = items
            .iter()
            .filter(|i| !i.is_deleted())
            .map(|i| i.physical_len as i64)
            .sum();

        self.segment_meta.update(seg_id, |meta| {
            meta.live_item_count
                .fetch_add(live_count, std::sync::atomic::Ordering::Relaxed);
            meta.physical_bytes
                .fetch_add(physical_bytes, std::sync::atomic::Ordering::Relaxed);
            meta.logical_bytes
                .fetch_add(physical_bytes, std::sync::atomic::Ordering::Relaxed);
        });

        Ok(())
    }

    /// Marks an item as deleted in both persistence and memory.
    ///
    /// This uses the incremental tombstone log for O(1) writes instead of
    /// rewriting the entire segment manifest. Tombstones are merged during
    /// reads and can be compacted with `compact_tombstones()`.
    ///
    /// Acquires exclusive lock on the segment to coordinate with compaction.
    pub fn tombstone(&self, key: Key, seg_id: u32) -> Result<()> {
        // Acquire exclusive lock on segment (blocks compaction)
        let mut guard = self.segment_meta.lock_exclusive(seg_id);

        // Get physical_len for metadata update
        let physical_len = self
            .blobs
            .get(&key)
            .map(|item| item.physical_len as i64)
            .unwrap_or(0);

        // Update in-memory index
        self.blobs.mark_deleted(key);

        // Write tombstone to incremental log (O(1) operation)
        if let Some(ref pers) = self.persistence {
            pers.tombstone(seg_id, key)?;
        }

        // Update segment metadata (using guard we already hold)
        guard.record_delete(physical_len);

        Ok(())
    }

    /// Marks an item as deleted with an existing exclusive lock.
    ///
    /// Used when caller already holds the segment lock.
    pub fn tombstone_with_lock(
        &self,
        key: Key,
        seg_id: u32,
        guard: &mut SegmentExclusiveGuard<'_>,
    ) -> Result<()> {
        // Get physical_len for metadata update
        let physical_len = self
            .blobs
            .get(&key)
            .map(|item| item.physical_len as i64)
            .unwrap_or(0);

        // Update in-memory index
        self.blobs.mark_deleted(key);

        // Write tombstone to incremental log
        if let Some(ref pers) = self.persistence {
            pers.tombstone(seg_id, key)?;
        }

        // Update segment metadata using the guard we already hold
        guard.record_delete(physical_len);

        Ok(())
    }

    /// Acquires exclusive lock on a segment (used by Delete).
    pub fn lock_segment_exclusive(&self, seg_id: u32) -> SegmentExclusiveGuard<'_> {
        self.segment_meta.lock_exclusive(seg_id)
    }

    /// Acquires shared locks on segments (used by Compaction).
    ///
    /// Multiple compactions can hold shared locks simultaneously.
    /// Delete operations will block until compaction releases.
    pub fn lock_segments_shared(&self, seg_ids: &[u32]) -> Vec<SegmentSharedGuard<'_>> {
        self.segment_meta.lock_shared(seg_ids)
    }

    /// Drops a segment from the persistence store and metadata.
    ///
    /// Called during compaction when a segment is merged/deleted.
    pub fn drop_segment(&self, seg_id: u32) -> Result<()> {
        if let Some(ref pers) = self.persistence {
            pers.drop_segment(seg_id)?;
        }
        // Clean up segment metadata
        self.segment_meta.remove(seg_id);
        Ok(())
    }

    /// Drains all items belonging to `seg_id` from the in-memory index and
    /// persistence, then removes segment metadata.
    ///
    /// Returns `(total_physical_bytes_freed, item_count_removed)`.
    ///
    /// Used by cache-mode drain: after calling this, delete the segment files.
    pub fn drain_segment(&self, seg_id: u32) -> (u64, i64) {
        let mut total_bytes = 0u64;
        let mut count = 0i64;

        // Collect all items belonging to this segment
        let mut to_delete = Vec::new();
        self.blobs.for_each(|item| {
            if item.segment_id == seg_id {
                to_delete.push(*item);
            }
        });

        // Remove each item from the in-memory index
        for item in &to_delete {
            if self.blobs.delete(&item.key) {
                total_bytes += item.physical_len as u64;
                count += 1;
            }
        }

        // Drop from persistence (removes all items + tombstones for this segment)
        if let Some(ref pers) = self.persistence {
            let _ = pers.drop_segment(seg_id);
        }

        // Remove segment metadata
        self.segment_meta.remove(seg_id);

        (total_bytes, count)
    }

    /// Returns the maximum sequence ID from persistence.
    pub fn max_seq_id(&self) -> Result<u64> {
        if let Some(ref pers) = self.persistence {
            pers.max_seq_id()
        } else {
            Ok(0)
        }
    }

    /// Returns the maximum sequence ID from the in-memory index.
    ///
    /// This is used during recovery to determine which WAL files are already committed.
    pub fn memory_max_seq_id(&self) -> u64 {
        self.blobs.max_seq_id()
    }

    /// Returns the number of items in the in-memory index.
    pub fn len(&self) -> usize {
        self.blobs.len()
    }

    /// Returns true if the in-memory index is empty.
    pub fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }

    /// Returns a reference to the underlying in-memory index.
    pub fn blobs(&self) -> &Arc<BlobIndex> {
        &self.blobs
    }

    /// Deletes an item from the in-memory index.
    ///
    /// Note: For CAS mode, use `tombstone()` instead to persist the deletion.
    pub fn delete(&self, key: &Key) -> bool {
        self.blobs.delete(key)
    }

    /// Evicts items to free up target_bytes of storage.
    ///
    /// Uses hybrid strategy from BlobIndex:
    /// - Small targets (<64KB): Random Greedy
    /// - Large targets (>=64KB): Proportional Fair
    pub fn evict_batch(&self, target_bytes: i64) -> Vec<Item> {
        self.blobs.evict_batch(target_bytes)
    }

    /// Returns statistics about the in-memory index.
    pub fn stats(&self) -> IndexStats {
        self.blobs.stats()
    }

    /// Compacts the persistence database.
    pub fn compact(&mut self) -> Result<()> {
        if let Some(ref mut pers) = self.persistence {
            pers.compact()?;
        }
        Ok(())
    }

    /// Returns the number of tombstones for a segment.
    ///
    /// Used by compaction to decide when to merge tombstones into the manifest.
    pub fn tombstone_count(&self, seg_id: u32) -> Result<usize> {
        if let Some(ref pers) = self.persistence {
            pers.tombstone_count(seg_id)
        } else {
            Ok(0)
        }
    }

    /// Compacts tombstones for a segment, merging them into the manifest.
    ///
    /// The optional callback is invoked for each tombstone before it's merged,
    /// allowing the caller to perform side effects like hole punching.
    /// The callback receives `TombstoneRecord` with `key_hash` and `item`
    /// (containing offset/physical_len for hole punch calculations).
    ///
    /// Returns the number of tombstones that were compacted.
    pub fn compact_tombstones(
        &self,
        seg_id: u32,
        on_tombstone: Option<&TombstoneFn>,
    ) -> Result<usize> {
        if let Some(ref pers) = self.persistence {
            pers.compact_tombstones(seg_id, on_tombstone)
        } else {
            Ok(0)
        }
    }

    /// Returns items for a segment from the persistence store.
    ///
    /// This is used by compaction to get items without reading iseg files.
    /// Returns (items, max_seq_id) if segment exists, None otherwise.
    ///
    /// Tombstones are merged on-the-fly, so deleted items have is_deleted() = true.
    pub fn get_segment_items(&self, seg_id: u32) -> Result<Option<(Vec<Item>, u64)>> {
        if let Some(ref pers) = self.persistence {
            let mut result: Option<(Vec<Item>, u64)> = None;
            pers.scan_segment(seg_id, |batch| {
                result = Some((batch.items, batch.max_seq_id));
                true
            })?;
            Ok(result)
        } else {
            // Memory-only mode: collect items from in-memory index that belong to this segment
            let mut items = Vec::new();
            self.blobs.for_each(|item| {
                if item.segment_id == seg_id {
                    items.push(*item);
                }
            });
            if items.is_empty() {
                Ok(None)
            } else {
                Ok(Some((items, 0)))
            }
        }
    }

    /// Applies multiple relocations with minimized lock contention.
    ///
    /// Updates are grouped by shard so each shard lock is acquired exactly once,
    /// eliminating per-item lock thrashing during large compactions.
    ///
    /// Returns the number of successful relocations.
    pub fn relocate_batch(&self, requests: &[RelocationRequest]) -> usize {
        self.blobs.relocate_batch(requests)
    }

    /// Returns a snapshot of all segment metadata for compaction selection.
    ///
    /// Use this to identify sparse segments for compaction without holding locks.
    pub fn segment_metadata_snapshot(&self) -> HashMap<u32, SegmentMetadataSnapshot> {
        self.segment_meta.snapshot()
    }

    /// Updates segment metadata after eviction.
    ///
    /// Called when items are evicted to update tombstone/live counts.
    pub fn record_eviction(&self, seg_id: u32, physical_len: i64) {
        self.segment_meta.update(seg_id, |meta| {
            meta.record_evict(physical_len);
        });
    }

    /// Returns segment IDs that have tombstones (for tombstone compaction).
    pub fn segments_with_tombstones(&self) -> Vec<u32> {
        self.segment_meta
            .snapshot()
            .into_iter()
            .filter(|(_, meta)| meta.tombstone_count > 0)
            .map(|(seg_id, _)| seg_id)
            .collect()
    }

    /// Returns the oldest (minimum) live segment ID.
    ///
    /// Used to determine if tombstones can be dropped during compaction.
    pub fn oldest_live_segment_id(&self) -> u32 {
        self.segment_meta
            .snapshot()
            .keys()
            .copied()
            .min()
            .unwrap_or(0)
    }

    /// Returns segment statistics for compaction selection.
    ///
    /// Converts SegmentMetadataSnapshot to SegmentStats for compatibility with
    /// compaction selection functions.
    pub fn segment_stats(&self) -> std::collections::HashMap<u32, crate::compaction::SegmentStats> {
        self.segment_meta
            .snapshot()
            .into_iter()
            .map(|(seg_id, meta)| {
                (
                    seg_id,
                    crate::compaction::SegmentStats {
                        segment_id: seg_id,
                        tombstone_count: meta.tombstone_count as usize,
                        live_item_count: meta.live_item_count as usize,
                        physical_bytes: meta.physical_bytes as u64,
                        logical_bytes: meta.logical_bytes as u64,
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_durable_index_memory_only() {
        // No path = memory-only mode
        let index = DurableIndex::open(None, 1000).unwrap();

        let key = Key::from_bytes(b"testkey");
        let item = Item::new(key, 1, 0, 100);

        index.put(item.clone());

        let found = index.get(&key).unwrap();
        assert_eq!(found.segment_id, 1);
        assert_eq!(found.offset, 0);
        assert_eq!(found.physical_len, 100);
    }

    #[test]
    fn test_durable_index_with_persistence() {
        let dir = tempdir().unwrap();

        // Create index and write some items
        {
            let index = DurableIndex::open(Some(dir.path()), 1000).unwrap();

            let key1 = Key::from_bytes(b"key1");
            let key2 = Key::from_bytes(b"key2");

            let items = vec![
                Item::new(key1, 1, 0, 100),
                Item::new(key2, 1, 100, 200),
            ];

            index.ingest_batch(1, &items, 42).unwrap();

            // Verify items are in memory
            assert!(index.get(&key1).is_some());
            assert!(index.get(&key2).is_some());
        }

        // Reopen and verify items are recovered
        {
            let index = DurableIndex::open(Some(dir.path()), 1000).unwrap();

            let key1 = Key::from_bytes(b"key1");
            let key2 = Key::from_bytes(b"key2");

            let item1 = index.get(&key1).unwrap();
            assert_eq!(item1.segment_id, 1);
            assert_eq!(item1.offset, 0);

            let item2 = index.get(&key2).unwrap();
            assert_eq!(item2.segment_id, 1);
            assert_eq!(item2.offset, 100);

            assert_eq!(index.max_seq_id().unwrap(), 42);
        }
    }

    #[test]
    fn test_durable_index_tombstone() {
        let dir = tempdir().unwrap();

        // Create index with an item
        {
            let index = DurableIndex::open(Some(dir.path()), 1000).unwrap();

            let key = Key::from_bytes(b"testkey");
            let item = Item::new(key, 1, 0, 100);

            index.ingest_batch(1, &[item], 10).unwrap();
            assert!(index.get(&key).is_some());

            // Delete it
            index.tombstone(key, 1).unwrap();
        }

        // Reopen - item should still exist but be marked deleted
        {
            let index = DurableIndex::open(Some(dir.path()), 1000).unwrap();

            let key = Key::from_bytes(b"testkey");
            // Deleted items are not loaded during recovery
            // (the scan_all skips is_deleted() items)
            assert!(index.get(&key).is_none());
        }
    }

    #[test]
    fn test_durable_index_drop_segment() {
        let dir = tempdir().unwrap();

        // Create index with items in multiple segments
        {
            let index = DurableIndex::open(Some(dir.path()), 1000).unwrap();

            let key1 = Key::from_bytes(b"key1");
            let key2 = Key::from_bytes(b"key2");

            index
                .ingest_batch(1, &[Item::new(key1, 1, 0, 100)], 10)
                .unwrap();
            index
                .ingest_batch(2, &[Item::new(key2, 2, 0, 100)], 20)
                .unwrap();

            // Drop segment 1
            index.drop_segment(1).unwrap();
        }

        // Reopen - only segment 2 items should be recovered
        {
            let index = DurableIndex::open(Some(dir.path()), 1000).unwrap();

            let key1 = Key::from_bytes(b"key1");
            let key2 = Key::from_bytes(b"key2");

            // key1 was in dropped segment
            assert!(index.get(&key1).is_none());
            // key2 should still exist
            assert!(index.get(&key2).is_some());
        }
    }

    #[test]
    fn test_durable_index_tombstone_count_and_compact() {
        use crate::persistence::TombstoneFn;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let dir = tempdir().unwrap();

        let index = DurableIndex::open(Some(dir.path()), 1000).unwrap();

        // Add items
        let key1 = Key::from_bytes(b"key1");
        let key2 = Key::from_bytes(b"key2");
        let key3 = Key::from_bytes(b"key3");

        index
            .ingest_batch(
                1,
                &[
                    Item::new(key1, 1, 0, 100),
                    Item::new(key2, 1, 100, 200),
                    Item::new(key3, 1, 300, 150),
                ],
                10,
            )
            .unwrap();

        // Initially no tombstones
        assert_eq!(index.tombstone_count(1).unwrap(), 0);

        // Delete two items (creates tombstones in incremental log)
        index.tombstone(key1, 1).unwrap();
        index.tombstone(key2, 1).unwrap();

        // Should have 2 tombstones now
        assert_eq!(index.tombstone_count(1).unwrap(), 2);

        // Items are still in index but marked deleted
        // (caller is expected to check is_deleted())
        let item1 = index.get(&key1).unwrap();
        assert!(item1.is_deleted());
        let item2 = index.get(&key2).unwrap();
        assert!(item2.is_deleted());
        // key3 should be alive
        let item3 = index.get(&key3).unwrap();
        assert!(!item3.is_deleted());

        // Compact tombstones with callback
        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = Arc::clone(&callback_count);
        let callback: TombstoneFn = Box::new(move |_record| {
            callback_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        let compacted = index.compact_tombstones(1, Some(&callback)).unwrap();
        assert_eq!(compacted, 2);
        assert_eq!(callback_count.load(Ordering::SeqCst), 2);

        // After compaction, tombstone count should be 0
        assert_eq!(index.tombstone_count(1).unwrap(), 0);
    }
}
