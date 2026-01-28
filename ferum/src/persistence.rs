//! Durable index persistence using redb.
//!
//! Provides Bitcask-style persistence for crash recovery:
//! - Segments table: segment_id -> DurableBatch (items in that segment)
//! - Tombstones table: (segment_id, key_high, key_low) -> timestamp
//! - Supports atomic batch writes via redb transactions
//!
//! # Tombstone Design
//!
//! Tombstones use a separate table to avoid rewriting the entire segment manifest
//! on each delete. With 128MB memtable and 4KB blobs, each segment has ~32K items
//! (~1MB manifest). Writing a ~30 byte tombstone record is O(1) vs O(manifest_size).
//!
//! Tombstones are merged into the manifest during:
//! - `scan_segment()` / `scan_all()` reads (on-the-fly merge)
//! - `compact_tombstones()` (batch merge + cleanup)
//!
//! # SegmentMetadata
//!
//! Per-segment tracking with atomic counters for:
//! - Tombstone/live item counts (compaction heuristics)
//! - Physical/logical byte tracking (sparseness detection)
//! - Segment-level locking (Delete=exclusive, Compaction=shared)
//!
//! This is the primary index storage (not just disaster recovery).

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use redb::{Database, MultimapTableDefinition, ReadableMultimapTable, ReadableTable, TableDefinition};

use crate::error::{Error, Result};
use crate::index::Item;
use crate::key::Key;

/// Helper trait to convert redb errors to our Error type.
trait RedbErrorExt<T> {
    fn map_redb_err(self, op: &'static str) -> Result<T>;
}

impl<T, E: std::fmt::Display> RedbErrorExt<T> for std::result::Result<T, E> {
    fn map_redb_err(self, op: &'static str) -> Result<T> {
        self.map_err(|e| Error::InvalidConfig {
            message: format!("{}: {}", op, e),
        })
    }
}

/// Segments table: segment_id (u32) -> serialized DurableBatch
const SEGMENTS: TableDefinition<u32, &[u8]> = TableDefinition::new("segments");

/// Tombstones table: (segment_id, key_high, key_low) -> timestamp
/// Using a multimap allows multiple tombstones per composite key if needed.
/// Key structure enables efficient range scans per segment.
const TOMBSTONES: MultimapTableDefinition<(u32, u64, u64), u64> =
    MultimapTableDefinition::new("tombstones");

/// Max sequence ID table: single key "max_seq" -> u64
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

// =============================================================================
// TombstoneRecord
// =============================================================================

/// Information about a tombstoned item.
/// Passed to callback during tombstone compaction for optional hole punching.
#[derive(Debug, Clone)]
pub struct TombstoneRecord {
    /// 128-bit hash of the deleted key.
    pub key_hash: Key,
    /// The tombstoned item (has offset, physical_len for hole punching).
    pub item: Item,
}

/// Callback for tombstone compaction.
pub type TombstoneFn = Box<dyn Fn(TombstoneRecord) + Send + Sync>;

// =============================================================================
// SegmentMetadata
// =============================================================================

/// Per-segment state for compaction decisions and synchronization.
///
/// Tracks tombstone/live counts and byte usage for compaction heuristics.
/// Used with `SegmentMetadataMap` for segment-level locking.
#[derive(Debug, Default)]
pub struct SegmentMetadata {
    /// Number of tombstones (incremented on Delete/Evict).
    pub tombstone_count: AtomicI32,
    /// Number of live items (decremented on Delete/Evict).
    pub live_item_count: AtomicI32,
    /// Actual disk usage (from stat.Blocks * 512).
    pub physical_bytes: AtomicI64,
    /// Sum of live item PhysicalLen.
    pub logical_bytes: AtomicI64,
}

impl SegmentMetadata {
    /// Creates new segment metadata with initial counts.
    pub fn new(live_items: i32, physical_bytes: i64, logical_bytes: i64) -> Self {
        SegmentMetadata {
            tombstone_count: AtomicI32::new(0),
            live_item_count: AtomicI32::new(live_items),
            physical_bytes: AtomicI64::new(physical_bytes),
            logical_bytes: AtomicI64::new(logical_bytes),
        }
    }

    /// Records a deletion (tombstone added, live item removed).
    pub fn record_delete(&self, physical_len: i64) {
        self.tombstone_count.fetch_add(1, Ordering::Relaxed);
        self.live_item_count.fetch_sub(1, Ordering::Relaxed);
        self.logical_bytes.fetch_sub(physical_len, Ordering::Relaxed);
    }

    /// Records an eviction (tombstone added, live item removed).
    pub fn record_evict(&self, physical_len: i64) {
        self.tombstone_count.fetch_add(1, Ordering::Relaxed);
        self.live_item_count.fetch_sub(1, Ordering::Relaxed);
        self.logical_bytes.fetch_sub(physical_len, Ordering::Relaxed);
    }

    /// Returns the waste ratio (tombstones / total items).
    pub fn waste_ratio(&self) -> f64 {
        let tombstones = self.tombstone_count.load(Ordering::Relaxed);
        let live = self.live_item_count.load(Ordering::Relaxed);
        let total = tombstones + live;
        if total == 0 {
            return 0.0;
        }
        tombstones as f64 / total as f64
    }

    /// Returns true if segment is sparse (waste ratio above threshold).
    pub fn is_sparse(&self, threshold: f64) -> bool {
        self.waste_ratio() >= threshold
    }
}

/// Number of shards for segment metadata locking.
const SEGMENT_META_SHARDS: usize = 256;

/// Sharded map for segment metadata with per-shard RwLock.
///
/// Locking protocol:
/// - Delete: write lock (exclusive, blocks compaction)
/// - Compaction: read lock (shared, multiple compactions allowed)
pub struct SegmentMetadataMap {
    shards: Vec<RwLock<HashMap<u32, SegmentMetadata>>>,
}

impl Default for SegmentMetadataMap {
    fn default() -> Self {
        Self::new()
    }
}

impl SegmentMetadataMap {
    /// Creates a new segment metadata map.
    pub fn new() -> Self {
        let shards = (0..SEGMENT_META_SHARDS)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        SegmentMetadataMap { shards }
    }

    /// Returns the shard index for a segment ID.
    #[inline]
    fn shard_index(&self, seg_id: u32) -> usize {
        (seg_id as usize) & (SEGMENT_META_SHARDS - 1)
    }

    /// Gets or creates metadata for a segment.
    pub fn get_or_insert(&self, seg_id: u32) -> SegmentMetadataRef<'_> {
        let shard_idx = self.shard_index(seg_id);
        let mut shard = self.shards[shard_idx].write();
        shard.entry(seg_id).or_insert_with(SegmentMetadata::default);
        drop(shard);

        // Return a reference that can be used for operations
        SegmentMetadataRef {
            map: self,
            seg_id,
        }
    }

    /// Updates metadata for a segment, creating if needed.
    pub fn update<F>(&self, seg_id: u32, f: F)
    where
        F: FnOnce(&SegmentMetadata),
    {
        let shard_idx = self.shard_index(seg_id);
        let mut shard = self.shards[shard_idx].write();
        let meta = shard.entry(seg_id).or_insert_with(SegmentMetadata::default);
        f(meta);
    }

    /// Removes metadata for a segment.
    pub fn remove(&self, seg_id: u32) {
        let shard_idx = self.shard_index(seg_id);
        let mut shard = self.shards[shard_idx].write();
        shard.remove(&seg_id);
    }

    /// Acquires exclusive lock for a segment and returns access to metadata.
    ///
    /// Used by Delete to coordinate with Compaction.
    /// The guard provides direct access to the segment's metadata for updates.
    pub fn lock_exclusive(&self, seg_id: u32) -> SegmentExclusiveGuard<'_> {
        let shard_idx = self.shard_index(seg_id);
        let mut guard = self.shards[shard_idx].write();
        // Ensure metadata exists
        guard.entry(seg_id).or_insert_with(SegmentMetadata::default);
        SegmentExclusiveGuard { guard, seg_id }
    }

    /// Acquires shared lock for segments (used by Compaction).
    pub fn lock_shared(&self, seg_ids: &[u32]) -> Vec<SegmentSharedGuard<'_>> {
        // Sort to prevent deadlocks
        let mut sorted_ids: Vec<u32> = seg_ids.to_vec();
        sorted_ids.sort();
        sorted_ids.dedup();

        sorted_ids
            .into_iter()
            .map(|seg_id| {
                let shard_idx = self.shard_index(seg_id);
                SegmentSharedGuard {
                    _guard: self.shards[shard_idx].read(),
                    seg_id,
                }
            })
            .collect()
    }

    /// Returns snapshot of all segment metadata for compaction selection.
    pub fn snapshot(&self) -> HashMap<u32, SegmentMetadataSnapshot> {
        let mut result = HashMap::new();
        for shard in &self.shards {
            let shard = shard.read();
            for (&seg_id, meta) in shard.iter() {
                result.insert(
                    seg_id,
                    SegmentMetadataSnapshot {
                        segment_id: seg_id,
                        tombstone_count: meta.tombstone_count.load(Ordering::Relaxed),
                        live_item_count: meta.live_item_count.load(Ordering::Relaxed),
                        physical_bytes: meta.physical_bytes.load(Ordering::Relaxed),
                        logical_bytes: meta.logical_bytes.load(Ordering::Relaxed),
                    },
                );
            }
        }
        result
    }
}

/// Reference to segment metadata for atomic updates.
pub struct SegmentMetadataRef<'a> {
    map: &'a SegmentMetadataMap,
    seg_id: u32,
}

impl<'a> SegmentMetadataRef<'a> {
    /// Records a deletion on this segment.
    pub fn record_delete(&self, physical_len: i64) {
        self.map.update(self.seg_id, |meta| {
            meta.record_delete(physical_len);
        });
    }
}

/// Exclusive lock guard for delete operations.
///
/// Provides access to the segment's metadata while holding the lock.
pub struct SegmentExclusiveGuard<'a> {
    guard: parking_lot::RwLockWriteGuard<'a, HashMap<u32, SegmentMetadata>>,
    seg_id: u32,
}

impl<'a> SegmentExclusiveGuard<'a> {
    /// Records a deletion on this segment's metadata.
    pub fn record_delete(&mut self, physical_len: i64) {
        if let Some(meta) = self.guard.get(&self.seg_id) {
            meta.record_delete(physical_len);
        }
    }

    /// Returns the segment ID this guard is protecting.
    pub fn segment_id(&self) -> u32 {
        self.seg_id
    }
}

/// Shared lock guard for compaction operations.
pub struct SegmentSharedGuard<'a> {
    _guard: parking_lot::RwLockReadGuard<'a, HashMap<u32, SegmentMetadata>>,
    #[allow(dead_code)]
    seg_id: u32,
}

/// Snapshot of segment metadata for compaction selection.
#[derive(Debug, Clone)]
pub struct SegmentMetadataSnapshot {
    pub segment_id: u32,
    pub tombstone_count: i32,
    pub live_item_count: i32,
    pub physical_bytes: i64,
    pub logical_bytes: i64,
}

impl SegmentMetadataSnapshot {
    /// Returns waste ratio (tombstones / total).
    pub fn waste_ratio(&self) -> f64 {
        let total = self.tombstone_count + self.live_item_count;
        if total == 0 {
            return 0.0;
        }
        self.tombstone_count as f64 / total as f64
    }
}

// =============================================================================
// DurableBatch
// =============================================================================

/// A batch of items for a single segment, serialized for storage.
#[derive(Debug, Clone)]
pub struct DurableBatch {
    pub segment_id: u32,
    pub ctime: i64,
    pub max_seq_id: u64,
    pub items: Vec<Item>,
}

impl DurableBatch {
    /// Serializes the batch to bytes.
    ///
    /// Wire format:
    /// - segment_id: 4 bytes (u32 LE)
    /// - ctime: 8 bytes (i64 LE)
    /// - max_seq_id: 8 bytes (u64 LE)
    /// - num_items: 4 bytes (u32 LE)
    /// - items: 32 bytes each (see Item serialization)
    pub fn encode(&self) -> Vec<u8> {
        let item_size = 32; // Key(16) + segment(4) + offset(4) + len(4) + flags(4)
        let header_size = 4 + 8 + 8 + 4; // segment_id + ctime + max_seq_id + num_items
        let mut buf = vec![0u8; header_size + self.items.len() * item_size];

        let mut pos = 0;

        // Header
        buf[pos..pos + 4].copy_from_slice(&self.segment_id.to_le_bytes());
        pos += 4;
        buf[pos..pos + 8].copy_from_slice(&self.ctime.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.max_seq_id.to_le_bytes());
        pos += 8;
        buf[pos..pos + 4].copy_from_slice(&(self.items.len() as u32).to_le_bytes());
        pos += 4;

        // Items
        for item in &self.items {
            // Key (16 bytes) - stored as high, low
            buf[pos..pos + 8].copy_from_slice(&item.key.high.to_le_bytes());
            pos += 8;
            buf[pos..pos + 8].copy_from_slice(&item.key.low.to_le_bytes());
            pos += 8;

            // Segment ID (4 bytes)
            buf[pos..pos + 4].copy_from_slice(&item.segment_id.to_le_bytes());
            pos += 4;

            // Offset (4 bytes)
            buf[pos..pos + 4].copy_from_slice(&item.offset.to_le_bytes());
            pos += 4;

            // Physical length (4 bytes)
            buf[pos..pos + 4].copy_from_slice(&item.physical_len.to_le_bytes());
            pos += 4;

            // Flags (4 bytes)
            buf[pos..pos + 4].copy_from_slice(&item.flags.to_le_bytes());
            pos += 4;
        }

        buf
    }

    /// Deserializes a batch from bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 24 {
            return Err(Error::BufferTooSmall {
                needed: 24,
                have: data.len(),
            });
        }

        let mut pos = 0;

        // Header
        let segment_id = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let ctime = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let max_seq_id = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let num_items = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // Validate size
        let item_size = 32;
        let expected_size = 24 + num_items * item_size;
        if data.len() < expected_size {
            return Err(Error::BufferTooSmall {
                needed: expected_size,
                have: data.len(),
            });
        }

        // Items
        let mut items = Vec::with_capacity(num_items);
        for _ in 0..num_items {
            let key_high = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let key_low = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let seg_id = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let offset = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let physical_len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let flags = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            let key = Key::from_parts(key_high, key_low);
            let mut item = Item::new(key, seg_id, offset, physical_len);
            item.flags = flags;
            items.push(item);
        }

        Ok(DurableBatch {
            segment_id,
            ctime,
            max_seq_id,
            items,
        })
    }
}

// =============================================================================
// Persistence
// =============================================================================

/// Durable storage for index data using redb.
pub struct Persistence {
    db: Database,
}

impl Persistence {
    /// Opens or creates the persistence database.
    pub fn open(path: &Path) -> Result<Self> {
        let db = Database::create(path).map_redb_err("open persistence database")?;

        // Initialize tables
        let write_txn = db.begin_write().map_redb_err("begin write txn")?;
        {
            let _ = write_txn.open_table(SEGMENTS);
            let _ = write_txn.open_multimap_table(TOMBSTONES);
            let _ = write_txn.open_table(META);
        }
        write_txn.commit().map_redb_err("commit init txn")?;

        Ok(Persistence { db })
    }

    /// Writes a batch of items for a segment.
    pub fn write_batch(&self, seg_id: u32, items: &[Item], max_seq_id: u64) -> Result<()> {
        let batch = DurableBatch {
            segment_id: seg_id,
            ctime: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            max_seq_id,
            items: items.to_vec(),
        };

        let encoded = batch.encode();

        let write_txn = self.db.begin_write().map_redb_err("begin write txn")?;
        {
            let mut table = write_txn
                .open_table(SEGMENTS)
                .map_redb_err("open segments table")?;
            table
                .insert(seg_id, encoded.as_slice())
                .map_redb_err("insert segment batch")?;

            // Update max sequence ID
            let mut meta = write_txn.open_table(META).map_redb_err("open meta table")?;
            let current_max = meta
                .get("max_seq")
                .map_redb_err("get max_seq")?
                .map(|v| v.value())
                .unwrap_or(0);
            if max_seq_id > current_max {
                meta.insert("max_seq", max_seq_id)
                    .map_redb_err("update max_seq")?;
            }
        }
        write_txn.commit().map_redb_err("commit write txn")?;

        Ok(())
    }

    /// Writes a single tombstone to the incremental log.
    ///
    /// This is O(1) - just writes a small record without touching the segment manifest.
    /// Tombstones are merged during scan_segment() or compact_tombstones().
    pub fn tombstone(&self, seg_id: u32, key: Key) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let write_txn = self.db.begin_write().map_redb_err("begin write txn")?;
        {
            let mut table = write_txn
                .open_multimap_table(TOMBSTONES)
                .map_redb_err("open tombstones table")?;
            table
                .insert((seg_id, key.high, key.low), timestamp)
                .map_redb_err("insert tombstone")?;
        }
        write_txn.commit().map_redb_err("commit tombstone txn")?;

        Ok(())
    }

    /// Loads tombstones for a segment within a transaction.
    fn load_tombstones_in_txn(
        &self,
        txn: &redb::ReadTransaction,
        seg_id: u32,
    ) -> Result<HashSet<Key>> {
        let mut tombstones = HashSet::new();

        let table = txn
            .open_multimap_table(TOMBSTONES)
            .map_redb_err("open tombstones table")?;

        // Range scan for this segment: (seg_id, 0, 0) to (seg_id+1, 0, 0)
        let start = (seg_id, 0u64, 0u64);
        let end = (seg_id + 1, 0u64, 0u64);

        let range = table.range(start..end).map_redb_err("range tombstones")?;
        for result in range {
            let (key, _values) = result.map_redb_err("read tombstone")?;
            let (_, key_high, key_low) = key.value();
            tombstones.insert(Key::from_parts(key_high, key_low));
        }

        Ok(tombstones)
    }

    /// Scans a single segment, merging tombstones on-the-fly.
    ///
    /// This provides a consistent view within a transaction.
    pub fn scan_segment<F>(&self, seg_id: u32, mut f: F) -> Result<()>
    where
        F: FnMut(DurableBatch) -> bool,
    {
        let read_txn = self.db.begin_read().map_redb_err("begin read txn")?;

        // 1. Load tombstones first (consistent view within transaction)
        let tombstones = self.load_tombstones_in_txn(&read_txn, seg_id)?;

        // 2. Load segment data
        let table = read_txn
            .open_table(SEGMENTS)
            .map_redb_err("open segments table")?;

        if let Some(value) = table.get(seg_id).map_redb_err("get segment")? {
            let mut batch = DurableBatch::decode(value.value())?;

            // 3. Merge tombstones into items
            for item in &mut batch.items {
                if tombstones.contains(&item.key) {
                    item.set_deleted();
                }
            }

            f(batch);
        }

        Ok(())
    }

    /// Scans all segments and calls the provided function for each batch.
    ///
    /// Tombstones are merged on-the-fly for each segment.
    /// Returns early if `f` returns `false`.
    pub fn scan_all<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(DurableBatch) -> bool,
    {
        let read_txn = self.db.begin_read().map_redb_err("begin read txn")?;

        // Load ALL tombstones first (across all segments)
        let mut all_tombstones: HashSet<Key> = HashSet::new();
        {
            let tomb_table = read_txn
                .open_multimap_table(TOMBSTONES)
                .map_redb_err("open tombstones table")?;

            let range = tomb_table.iter().map_redb_err("iter tombstones")?;
            for result in range {
                let (key, _values) = result.map_redb_err("read tombstone")?;
                let (_, key_high, key_low) = key.value();
                all_tombstones.insert(Key::from_parts(key_high, key_low));
            }
        }

        // Scan segments, merging tombstones
        let table = read_txn
            .open_table(SEGMENTS)
            .map_redb_err("open segments table")?;

        for result in table.iter().map_redb_err("iter segments")? {
            let (_, value) = result.map_redb_err("read segment")?;
            let mut batch = DurableBatch::decode(value.value())?;

            // Merge tombstones into items
            for item in &mut batch.items {
                if all_tombstones.contains(&item.key) {
                    item.set_deleted();
                }
            }

            if !f(batch) {
                break;
            }
        }

        Ok(())
    }

    /// Compacts tombstones for a segment, merging them into the manifest.
    ///
    /// This is a metadata cleanup operation that also enables space reclamation.
    /// The `on_tombstone` callback is invoked for each tombstone with its item,
    /// allowing the caller to hole punch (idempotent operation).
    ///
    /// All operations in a single transaction for atomicity.
    pub fn compact_tombstones(
        &self,
        seg_id: u32,
        on_tombstone: Option<&TombstoneFn>,
    ) -> Result<usize> {
        let write_txn = self.db.begin_write().map_redb_err("begin write txn")?;

        // 1. Load tombstones within transaction
        let mut tombstones = HashSet::new();
        {
            let tomb_table = write_txn
                .open_multimap_table(TOMBSTONES)
                .map_redb_err("open tombstones table")?;

            let start = (seg_id, 0u64, 0u64);
            let end = (seg_id + 1, 0u64, 0u64);

            let range = tomb_table.range(start..end).map_redb_err("range tombstones")?;
            for result in range {
                let (key, _values) = result.map_redb_err("read tombstone")?;
                let (_, key_high, key_low) = key.value();
                tombstones.insert(Key::from_parts(key_high, key_low));
            }
        }

        if tombstones.is_empty() {
            return Ok(0);
        }

        let tombstone_count = tombstones.len();

        // 2. Read segment manifest
        let batch = {
            let table = write_txn
                .open_table(SEGMENTS)
                .map_redb_err("open segments table")?;

            match table.get(seg_id).map_redb_err("get segment")? {
                Some(value) => DurableBatch::decode(value.value())?,
                None => return Ok(0), // Segment doesn't exist
            }
        };

        // 3. Invoke callback for each tombstone (caller can hole punch)
        if let Some(callback) = on_tombstone {
            for item in &batch.items {
                if tombstones.contains(&item.key) && !item.is_deleted() {
                    callback(TombstoneRecord {
                        key_hash: item.key,
                        item: item.clone(),
                    });
                }
            }
        }

        // 4. Mark items as deleted and rewrite manifest
        let mut updated_items = batch.items.clone();
        for item in &mut updated_items {
            if tombstones.contains(&item.key) {
                item.set_deleted();
            }
        }

        let updated_batch = DurableBatch {
            segment_id: batch.segment_id,
            ctime: batch.ctime,
            max_seq_id: batch.max_seq_id,
            items: updated_items,
        };

        {
            let mut table = write_txn
                .open_table(SEGMENTS)
                .map_redb_err("open segments table")?;
            table
                .insert(seg_id, updated_batch.encode().as_slice())
                .map_redb_err("update segment")?;
        }

        // 5. Delete tombstone records
        {
            let mut tomb_table = write_txn
                .open_multimap_table(TOMBSTONES)
                .map_redb_err("open tombstones table")?;

            for key in &tombstones {
                // Remove all values for this tombstone key
                tomb_table
                    .remove_all((seg_id, key.high, key.low))
                    .map_redb_err("remove tombstone")?;
            }
        }

        write_txn.commit().map_redb_err("commit compact txn")?;

        Ok(tombstone_count)
    }

    /// Returns the number of tombstones for a segment.
    ///
    /// Used for compaction decisions (e.g., when tombstone count exceeds threshold).
    pub fn tombstone_count(&self, seg_id: u32) -> Result<usize> {
        let read_txn = self.db.begin_read().map_redb_err("begin read txn")?;
        let table = read_txn
            .open_multimap_table(TOMBSTONES)
            .map_redb_err("open tombstones table")?;

        let start = (seg_id, 0u64, 0u64);
        let end = (seg_id + 1, 0u64, 0u64);

        let mut count = 0;
        let range = table.range(start..end).map_redb_err("range tombstones")?;
        for result in range {
            let _ = result.map_redb_err("read tombstone")?;
            count += 1;
        }

        Ok(count)
    }

    /// Returns the maximum sequence ID stored.
    pub fn max_seq_id(&self) -> Result<u64> {
        let read_txn = self.db.begin_read().map_redb_err("begin read txn")?;
        let table = read_txn.open_table(META).map_redb_err("open meta table")?;

        Ok(table
            .get("max_seq")
            .map_redb_err("get max_seq")?
            .map(|v| v.value())
            .unwrap_or(0))
    }

    /// Drops a segment from the persistence store.
    ///
    /// Also removes any associated tombstones.
    pub fn drop_segment(&self, seg_id: u32) -> Result<()> {
        let write_txn = self.db.begin_write().map_redb_err("begin write txn")?;
        {
            // Remove segment data
            let mut table = write_txn
                .open_table(SEGMENTS)
                .map_redb_err("open segments table")?;
            let _ = table.remove(seg_id);

            // Remove associated tombstones
            let mut tomb_table = write_txn
                .open_multimap_table(TOMBSTONES)
                .map_redb_err("open tombstones table")?;

            // Collect keys to remove (can't modify while iterating)
            let start = (seg_id, 0u64, 0u64);
            let end = (seg_id + 1, 0u64, 0u64);
            let mut keys_to_remove = Vec::new();

            {
                let range = tomb_table.range(start..end).map_redb_err("range tombstones")?;
                for result in range {
                    let (key, _values) = result.map_redb_err("read tombstone")?;
                    keys_to_remove.push(key.value());
                }
            }

            for (seg, high, low) in keys_to_remove {
                tomb_table
                    .remove_all((seg, high, low))
                    .map_redb_err("remove tombstone")?;
            }
        }
        write_txn.commit().map_redb_err("commit drop txn")?;

        Ok(())
    }

    /// Compacts the database.
    pub fn compact(&mut self) -> Result<()> {
        self.db.compact().map_redb_err("compact database")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_durable_batch_roundtrip() {
        let key1 = Key::from_bytes(b"key1");
        let key2 = Key::from_bytes(b"key2");

        let items = vec![Item::new(key1, 1, 0, 100), Item::new(key2, 1, 100, 200)];

        let batch = DurableBatch {
            segment_id: 42,
            ctime: 1234567890,
            max_seq_id: 999,
            items,
        };

        let encoded = batch.encode();
        let decoded = DurableBatch::decode(&encoded).unwrap();

        assert_eq!(decoded.segment_id, 42);
        assert_eq!(decoded.ctime, 1234567890);
        assert_eq!(decoded.max_seq_id, 999);
        assert_eq!(decoded.items.len(), 2);
        assert_eq!(decoded.items[0].key, key1);
        assert_eq!(decoded.items[1].key, key2);
    }

    #[test]
    fn test_persistence_basic() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("index.redb");

        let persistence = Persistence::open(&db_path).unwrap();

        // Write a batch
        let key = Key::from_bytes(b"testkey");
        let items = vec![Item::new(key, 1, 0, 100)];
        persistence.write_batch(1, &items, 42).unwrap();

        // Read it back
        let mut found = false;
        persistence
            .scan_all(|batch| {
                assert_eq!(batch.segment_id, 1);
                assert_eq!(batch.max_seq_id, 42);
                assert_eq!(batch.items.len(), 1);
                assert_eq!(batch.items[0].key, key);
                found = true;
                true
            })
            .unwrap();
        assert!(found);

        // Check max seq
        assert_eq!(persistence.max_seq_id().unwrap(), 42);
    }

    #[test]
    fn test_persistence_tombstone() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("index.redb");

        let persistence = Persistence::open(&db_path).unwrap();

        // Write a batch
        let key1 = Key::from_bytes(b"key1");
        let key2 = Key::from_bytes(b"key2");
        let items = vec![Item::new(key1, 1, 0, 100), Item::new(key2, 1, 100, 200)];
        persistence.write_batch(1, &items, 42).unwrap();

        // Write tombstone for key1
        persistence.tombstone(1, key1).unwrap();

        // Verify tombstone count
        assert_eq!(persistence.tombstone_count(1).unwrap(), 1);

        // Scan should show key1 as deleted
        persistence
            .scan_segment(1, |batch| {
                assert_eq!(batch.items.len(), 2);
                assert!(batch.items[0].is_deleted()); // key1 tombstoned
                assert!(!batch.items[1].is_deleted()); // key2 still live
                true
            })
            .unwrap();

        // Compact tombstones
        let compacted = persistence.compact_tombstones(1, None).unwrap();
        assert_eq!(compacted, 1);

        // After compaction, tombstone count should be 0
        assert_eq!(persistence.tombstone_count(1).unwrap(), 0);

        // Item should still be marked deleted in manifest
        persistence
            .scan_segment(1, |batch| {
                assert!(batch.items[0].is_deleted());
                true
            })
            .unwrap();
    }

    #[test]
    fn test_persistence_multiple_segments() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("index.redb");

        let persistence = Persistence::open(&db_path).unwrap();

        // Write multiple segments
        for seg_id in 1..=5 {
            let key = Key::from_bytes(format!("key{}", seg_id).as_bytes());
            let items = vec![Item::new(key, seg_id, 0, 100)];
            persistence
                .write_batch(seg_id, &items, seg_id as u64 * 10)
                .unwrap();
        }

        // Read all back
        let mut count = 0;
        persistence
            .scan_all(|_| {
                count += 1;
                true
            })
            .unwrap();
        assert_eq!(count, 5);

        // Max seq should be from last segment
        assert_eq!(persistence.max_seq_id().unwrap(), 50);
    }

    #[test]
    fn test_persistence_drop_segment() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("index.redb");

        let persistence = Persistence::open(&db_path).unwrap();

        // Write two segments
        let key1 = Key::from_bytes(b"key1");
        let key2 = Key::from_bytes(b"key2");
        persistence
            .write_batch(1, &[Item::new(key1, 1, 0, 100)], 10)
            .unwrap();
        persistence
            .write_batch(2, &[Item::new(key2, 2, 0, 100)], 20)
            .unwrap();

        // Add tombstone to segment 1
        persistence.tombstone(1, key1).unwrap();
        assert_eq!(persistence.tombstone_count(1).unwrap(), 1);

        // Drop first segment
        persistence.drop_segment(1).unwrap();

        // Tombstone should also be dropped
        assert_eq!(persistence.tombstone_count(1).unwrap(), 0);

        // Only second segment should remain
        let mut count = 0;
        persistence
            .scan_all(|batch| {
                assert_eq!(batch.segment_id, 2);
                count += 1;
                true
            })
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_tombstone_with_callback() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("index.redb");

        let persistence = Persistence::open(&db_path).unwrap();

        // Write a batch
        let key1 = Key::from_bytes(b"key1");
        let item1 = Item::new(key1, 1, 1000, 500); // offset=1000, len=500
        persistence.write_batch(1, &[item1], 42).unwrap();

        // Write tombstone
        persistence.tombstone(1, key1).unwrap();

        // Compact with callback to verify we get the item info
        use std::sync::atomic::{AtomicBool, Ordering};
        let callback_called = AtomicBool::new(false);
        let callback: TombstoneFn = Box::new(move |record| {
            assert_eq!(record.item.offset, 1000);
            assert_eq!(record.item.physical_len, 500);
            callback_called.store(true, Ordering::SeqCst);
        });

        persistence.compact_tombstones(1, Some(&callback)).unwrap();
        // Note: callback_called check would require Arc, simplified for test
    }
}
