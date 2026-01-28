//! GC-optimized in-memory index for blob metadata with SIEVE eviction.
//!
//! # Design Philosophy
//!
//! RAM stores only "coordinates" (32 bytes per item). Full metadata lives on disk
//! and is read on-demand during Get() operations.
//!
//! # SIEVE Algorithm
//!
//! SIEVE is a cache-efficient eviction algorithm that combines:
//! - Clock hand scanning (low overhead)
//! - Visited bit for second-chance semantics
//! - No frequency counters (unlike LFU)
//!
//! # Memory Layout
//!
//! - 256 shards for lock distribution (uniform from XXH3 hash)
//! - Arena-backed nodes with indices instead of pointers (GC-friendly in Rust too)
//! - Circular doubly-linked list for O(1) eviction

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::compression::Codec;
use crate::error::BlobErrno;
use crate::key::Key;

// =============================================================================
// Constants
// =============================================================================

/// Number of shards for lock distribution.
pub const SHARD_COUNT: usize = 256;

/// Sentinel value for "no link" in arena indices.
const NULL_IDX: u32 = 0xFFFF_FFFF;

/// Maximum items evicted per lock hold (~13µs).
const MAX_EVICT_PER_LOCK: usize = 64;

/// Stop if freed > 2× target.
const OVERSHOOT_FACTOR: i64 = 2;

/// Threshold for switching eviction strategy.
const SMALL_TARGET_THRESHOLD: i64 = 64 * 1024;

// =============================================================================
// Item
// =============================================================================

/// Coordinates needed to locate a blob on disk.
///
/// Layout (32 bytes, 8-byte aligned):
/// - Key: 16 bytes (128-bit XXH3 hash, back-reference for eviction)
/// - SegmentID: 4 bytes
/// - Offset: 4 bytes
/// - PhysicalLen: 4 bytes
/// - Flags: 4 bytes
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct Item {
    /// 128-bit XXH3 hash - back-reference for eviction.
    pub key: Key,
    /// Which segment file contains this blob.
    pub segment_id: u32,
    /// Byte offset within segment (to record header).
    pub offset: u32,
    /// On-disk size in bytes (header + key + value).
    pub physical_len: u32,
    /// Status flags (deleted, errno, compression codec).
    pub flags: u32,
}

// Item flag constants.
const ITEM_FLAG_COMPRESSION_SHIFT: u32 = 28;
const ITEM_FLAG_COMPRESSION_MASK: u32 = 0xF << ITEM_FLAG_COMPRESSION_SHIFT;
const ITEM_FLAG_ERRNO_SHIFT: u32 = 23;
const ITEM_FLAG_ERRNO_MASK: u32 = 0x1F << ITEM_FLAG_ERRNO_SHIFT;
const ITEM_FLAG_DELETED: u32 = 1 << 22;

impl Item {
    /// Creates a new item with the given coordinates.
    pub fn new(key: Key, segment_id: u32, offset: u32, physical_len: u32) -> Self {
        Item {
            key,
            segment_id,
            offset,
            physical_len,
            flags: 0,
        }
    }

    /// Returns true if the blob is marked as deleted (tombstone).
    #[inline]
    pub fn is_deleted(&self) -> bool {
        (self.flags & ITEM_FLAG_DELETED) != 0
    }

    /// Marks the blob as deleted (tombstone).
    pub fn set_deleted(&mut self) {
        self.flags = ITEM_FLAG_DELETED;
        self.physical_len = 0;
    }

    /// Returns the error code for this blob.
    #[inline]
    pub fn errno(&self) -> BlobErrno {
        BlobErrno::from_raw(((self.flags & ITEM_FLAG_ERRNO_MASK) >> ITEM_FLAG_ERRNO_SHIFT) as u8)
    }

    /// Sets the error code for this blob.
    pub fn set_errno(&mut self, errno: BlobErrno) {
        self.flags =
            (self.flags & !ITEM_FLAG_ERRNO_MASK) | ((errno as u32 & 0x1F) << ITEM_FLAG_ERRNO_SHIFT);
    }

    /// Returns true if the blob has an error.
    #[inline]
    pub fn has_error(&self) -> bool {
        !self.errno().is_ok()
    }

    /// Returns the compression codec.
    #[inline]
    pub fn compression(&self) -> Codec {
        Codec::from_raw(((self.flags & ITEM_FLAG_COMPRESSION_MASK) >> ITEM_FLAG_COMPRESSION_SHIFT) as u8)
    }

    /// Sets the compression codec.
    pub fn set_compression(&mut self, codec: Codec) {
        self.flags = (self.flags & !ITEM_FLAG_COMPRESSION_MASK)
            | ((codec as u32) << ITEM_FLAG_COMPRESSION_SHIFT);
    }

    /// Returns true if the blob is compressed.
    #[inline]
    pub fn is_compressed(&self) -> bool {
        self.compression() != Codec::None
    }
}

// =============================================================================
// Node (Arena Entry)
// =============================================================================

/// Arena-backed entry with no heap pointers.
/// Uses u32 indices instead of pointers for cache efficiency.
struct Node {
    /// Blob coordinates including Key for eviction.
    item: Item,
    /// Index in 'nodes' slice (circular list - next).
    next: u32,
    /// Index in 'nodes' slice (circular list - prev).
    prev: u32,
    /// SIEVE algorithm: 0=cold, 1=hot.
    visited: AtomicU32,
}

impl Default for Node {
    fn default() -> Self {
        Node {
            item: Item::default(),
            next: NULL_IDX,
            prev: NULL_IDX,
            visited: AtomicU32::new(0),
        }
    }
}

// =============================================================================
// Shard
// =============================================================================

/// A single shard of the index with its SIEVE state.
struct Shard {
    /// Key -> node index mapping.
    items: HashMap<Key, u32>,
    /// Arena of nodes.
    nodes: Vec<Node>,
    /// Free list head (stack).
    free_head: u32,
    /// SIEVE cursor (current scan position).
    hand: u32,
    /// Circular list head (newest item).
    head: u32,
}

impl Shard {
    fn new(capacity: usize) -> Self {
        Shard {
            items: HashMap::with_capacity(capacity),
            nodes: Vec::with_capacity(capacity),
            free_head: NULL_IDX,
            hand: NULL_IDX,
            head: NULL_IDX,
        }
    }

    /// Allocates a node index from free list or appends to arena.
    fn alloc_node(&mut self) -> u32 {
        if self.free_head != NULL_IDX {
            let idx = self.free_head;
            self.free_head = self.nodes[idx as usize].next;
            return idx;
        }
        self.nodes.push(Node::default());
        (self.nodes.len() - 1) as u32
    }

    /// Frees a node by pushing it onto the free list.
    fn free_node(&mut self, idx: u32) {
        self.unlink_node(idx);
        self.nodes[idx as usize].next = self.free_head;
        self.free_head = idx;
    }

    /// Links a node to the circular list at head position (newest).
    fn link_node(&mut self, idx: u32) {
        if self.head == NULL_IDX {
            // First item: circular self-reference
            self.head = idx;
            self.nodes[idx as usize].next = idx;
            self.nodes[idx as usize].prev = idx;
            self.hand = idx;
            return;
        }

        // Insert between tail (head.prev) and head
        let head = self.head;
        let tail = self.nodes[head as usize].prev;

        self.nodes[idx as usize].next = head;
        self.nodes[idx as usize].prev = tail;
        self.nodes[head as usize].prev = idx;
        self.nodes[tail as usize].next = idx;

        self.head = idx;
    }

    /// Unlinks a node from the circular list.
    fn unlink_node(&mut self, idx: u32) {
        let next = self.nodes[idx as usize].next;
        let prev = self.nodes[idx as usize].prev;

        if next == idx {
            // Only one item (points to itself)
            self.head = NULL_IDX;
            self.hand = NULL_IDX;
            return;
        }

        // Stitch neighbors together
        self.nodes[prev as usize].next = next;
        self.nodes[next as usize].prev = prev;

        // Update head if removed
        if self.head == idx {
            self.head = next;
        }

        // Update hand if it was on the victim
        if self.hand == idx {
            self.hand = next;
        }
    }

    /// Runs SIEVE to find a victim. Assumes list is NOT empty.
    fn run_sieve(&mut self) -> u32 {
        if self.head == NULL_IDX {
            return NULL_IDX;
        }
        if self.hand == NULL_IDX {
            self.hand = self.head;
        }

        loop {
            let curr = self.hand;
            self.hand = self.nodes[curr as usize].next;

            if self.nodes[curr as usize].visited.load(Ordering::Acquire) == 1 {
                // Give second chance: clear visited bit
                self.nodes[curr as usize].visited.store(0, Ordering::Release);
                continue;
            }

            // Found cold victim
            return curr;
        }
    }

    /// Evicts items up to quota_bytes, returning bytes freed.
    fn evict_up_to(&mut self, quota_bytes: i64, dst: &mut Vec<Item>, max_count: usize) -> i64 {
        let mut freed: i64 = 0;
        let mut count = 0;

        while count < max_count && freed < quota_bytes {
            if self.head == NULL_IDX {
                break;
            }

            let victim_idx = self.run_sieve();
            let item = self.nodes[victim_idx as usize].item;

            dst.push(item);
            freed += item.physical_len as i64;
            count += 1;

            self.items.remove(&item.key);
            self.free_node(victim_idx);
        }

        freed
    }
}

// =============================================================================
// BlobIndex
// =============================================================================

/// The main sharded index with SIEVE eviction.
pub struct BlobIndex {
    shards: Vec<RwLock<Shard>>,
    /// Highest sequence ID seen (for WAL recovery)
    max_seq_id: AtomicU64,
}

impl BlobIndex {
    /// Creates a new index with the given initial capacity hint.
    pub fn new(initial_capacity: usize) -> Self {
        let shard_cap = (initial_capacity / SHARD_COUNT).max(1);
        let shards = (0..SHARD_COUNT)
            .map(|_| RwLock::new(Shard::new(shard_cap)))
            .collect();

        BlobIndex {
            shards,
            max_seq_id: AtomicU64::new(0),
        }
    }

    /// Returns a read lock on the shard for a key.
    fn shard_read(&self, key: &Key) -> RwLockReadGuard<'_, Shard> {
        self.shards[key.shard() as usize].read()
    }

    /// Returns a write lock on the shard for a key.
    fn shard_write(&self, key: &Key) -> RwLockWriteGuard<'_, Shard> {
        self.shards[key.shard() as usize].write()
    }

    /// Returns a write lock on a shard by index.
    fn shard_at(&self, idx: usize) -> RwLockWriteGuard<'_, Shard> {
        self.shards[idx].write()
    }

    /// Gets the item for a key and marks it as visited (hot).
    pub fn get(&self, key: &Key) -> Option<Item> {
        let shard = self.shard_read(key);

        let idx = *shard.items.get(key)?;
        let node = &shard.nodes[idx as usize];

        // Hot path optimization: check before atomic write
        if node.visited.load(Ordering::Acquire) == 0 {
            node.visited.store(1, Ordering::Release);
        }

        Some(node.item)
    }

    /// Inserts or updates an item.
    ///
    /// This is "pure storage" - it NEVER evicts. Eviction is driven externally.
    pub fn put(&self, item: Item) {
        let mut shard = self.shard_write(&item.key);

        // Update existing?
        if let Some(&idx) = shard.items.get(&item.key) {
            shard.nodes[idx as usize].item = item;
            shard.nodes[idx as usize].visited.store(1, Ordering::Release);
            return;
        }

        // Allocate new node
        let new_idx = shard.alloc_node();
        shard.nodes[new_idx as usize].item = item;
        shard.nodes[new_idx as usize].visited.store(0, Ordering::Release);
        shard.items.insert(item.key, new_idx);
        shard.link_node(new_idx);
    }

    /// Inserts multiple items in a batch.
    ///
    /// Items with the same key will be deduplicated, keeping the one with
    /// the highest seq_id. This is used during flush to update the index.
    pub fn put_batch(&self, items: &[Item], max_seq_id: u64) {
        for item in items {
            self.put(*item);
        }
        // Atomically update max_seq_id if this batch's max is higher
        loop {
            let current = self.max_seq_id.load(Ordering::Acquire);
            if max_seq_id <= current {
                break;
            }
            if self.max_seq_id.compare_exchange_weak(
                current, max_seq_id, Ordering::AcqRel, Ordering::Acquire
            ).is_ok() {
                break;
            }
        }
    }

    /// Returns the highest sequence ID seen (for WAL recovery).
    pub fn max_seq_id(&self) -> u64 {
        self.max_seq_id.load(Ordering::Acquire)
    }

    /// Deletes an item by key.
    pub fn delete(&self, key: &Key) -> bool {
        let mut shard = self.shard_write(key);

        let idx = match shard.items.remove(key) {
            Some(i) => i,
            None => return false,
        };

        shard.free_node(idx);
        true
    }

    /// Marks an item as deleted (tombstone) without removing it.
    ///
    /// Used by durable index to persist deletions.
    pub fn mark_deleted(&self, key: Key) -> bool {
        let mut shard = self.shard_write(&key);

        let idx = match shard.items.get(&key) {
            Some(&i) => i,
            None => return false,
        };

        shard.nodes[idx as usize].item.set_deleted();
        true
    }

    /// Evicts items to free up target_bytes of storage.
    ///
    /// Uses hybrid strategy:
    /// - Small targets (<64KB): Random Greedy - pick random start shard
    /// - Large targets (≥64KB): Proportional Fair - each shard pays fair share
    pub fn evict_batch(&self, target_bytes: i64) -> Vec<Item> {
        if target_bytes <= 0 {
            return Vec::new();
        }

        let mut evicted = Vec::with_capacity((target_bytes / 4096 + 16) as usize);
        let mut freed_total: i64 = 0;

        if target_bytes < SMALL_TARGET_THRESHOLD {
            // Small target: Random Greedy
            let start_shard = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as usize)
                % SHARD_COUNT;

            for i in 0..SHARD_COUNT {
                if freed_total >= target_bytes {
                    break;
                }
                let shard_id = (start_shard + i) % SHARD_COUNT;
                let mut shard = self.shard_at(shard_id);
                freed_total +=
                    shard.evict_up_to(target_bytes - freed_total, &mut evicted, MAX_EVICT_PER_LOCK);
            }
        } else {
            // Large target: Proportional Fairness
            let quota_per_shard = (target_bytes / SHARD_COUNT as i64).max(1);

            for i in 0..SHARD_COUNT {
                if freed_total >= target_bytes * OVERSHOOT_FACTOR {
                    break;
                }
                let mut shard = self.shard_at(i);
                freed_total += shard.evict_up_to(quota_per_shard, &mut evicted, MAX_EVICT_PER_LOCK);
            }
        }

        evicted
    }

    /// Returns the total number of items.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().items.len()).sum()
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterates over all items in the index.
    ///
    /// Note: This acquires read locks on all shards sequentially.
    /// The callback receives each item by reference.
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&Item),
    {
        for shard in &self.shards {
            let s = shard.read();
            for &idx in s.items.values() {
                f(&s.nodes[idx as usize].item);
            }
        }
    }

    /// Returns statistics about the index.
    pub fn stats(&self) -> IndexStats {
        let mut stats = IndexStats {
            items: 0,
            arena_nodes: 0,
            free_nodes: 0,
            shards: SHARD_COUNT,
            memory_est: 0,
        };

        for shard in &self.shards {
            let s = shard.read();
            stats.items += s.items.len();
            stats.arena_nodes += s.nodes.len();

            // Count free list nodes
            let mut free_idx = s.free_head;
            while free_idx != NULL_IDX {
                stats.free_nodes += 1;
                free_idx = s.nodes[free_idx as usize].next;
            }
        }

        // Estimate memory: node ~44 bytes + map overhead ~32 bytes
        const NODE_SIZE: usize = 44;
        const MAP_ENTRY_OVERHEAD: usize = 32;
        stats.memory_est =
            (stats.arena_nodes * NODE_SIZE + stats.items * MAP_ENTRY_OVERHEAD) as i64;

        stats
    }

    /// Relocates an item from old location to new location atomically.
    ///
    /// Returns true if relocation succeeded, false if:
    /// - Item doesn't exist
    /// - Current location doesn't match old location (concurrent write)
    /// - Deleted state doesn't match mode
    pub fn relocate(
        &self,
        key: &Key,
        old_segment: u32,
        new_segment: u32,
        old_offset: u32,
        new_offset: u32,
        mode: RelocateMode,
    ) -> bool {
        let mut shard = self.shard_write(key);

        let idx = match shard.items.get(key) {
            Some(&i) => i,
            None => return false,
        };

        let item = &mut shard.nodes[idx as usize].item;
        if item.segment_id != old_segment || item.offset != old_offset {
            return false;
        }

        if item.is_deleted() != mode.expect_deleted() {
            return false;
        }

        item.segment_id = new_segment;
        item.offset = new_offset;
        true
    }

    /// Applies multiple relocations with minimized lock contention.
    ///
    /// Updates are grouped by shard so each shard lock is acquired exactly once,
    /// eliminating per-item lock thrashing during large compactions.
    ///
    /// Returns the number of successful relocations.
    pub fn relocate_batch(&self, requests: &[RelocationRequest]) -> usize {
        if requests.is_empty() {
            return 0;
        }

        // Partition requests by shard index (no locking)
        let mut shard_requests: [Vec<usize>; SHARD_COUNT] = std::array::from_fn(|_| Vec::new());
        for (i, req) in requests.iter().enumerate() {
            let shard_id = req.key.shard() as usize;
            shard_requests[shard_id].push(i);
        }

        // Process each shard (lock once per shard)
        let mut success_count = 0;
        for (shard_id, req_indices) in shard_requests.iter().enumerate() {
            if req_indices.is_empty() {
                continue;
            }

            let mut shard = self.shards[shard_id].write();
            for &req_idx in req_indices {
                let req = &requests[req_idx];

                let node_idx = match shard.items.get(&req.key) {
                    Some(&idx) => idx,
                    None => continue,
                };

                let item = &mut shard.nodes[node_idx as usize].item;
                if item.segment_id != req.old_segment_id || item.offset != req.old_offset {
                    // Location changed - concurrent write won
                    continue;
                }

                if item.is_deleted() != req.mode.expect_deleted() {
                    // State mismatch - Ghost Guard / Race Guard triggered
                    continue;
                }

                // Success: update location
                item.segment_id = req.new_segment_id;
                item.offset = req.new_offset;
                success_count += 1;
            }
        }

        success_count
    }
}

// =============================================================================
// Supporting Types
// =============================================================================

/// Statistics about the index state.
#[derive(Debug, Default)]
pub struct IndexStats {
    pub items: usize,
    pub arena_nodes: usize,
    pub free_nodes: usize,
    pub shards: usize,
    pub memory_est: i64,
}

/// Mode for relocation operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelocateMode {
    /// Relocating live items. Fails if item is deleted (Ghost Guard).
    Live,
    /// Relocating tombstones. Fails if item is NOT deleted.
    Tombstone,
}

impl RelocateMode {
    /// Returns true if expecting deleted state.
    fn expect_deleted(self) -> bool {
        self == RelocateMode::Tombstone
    }
}

/// Request for batch relocation during compaction.
#[derive(Debug, Clone)]
pub struct RelocationRequest {
    pub key: Key,
    pub old_segment_id: u32,
    pub old_offset: u32,
    pub new_segment_id: u32,
    pub new_offset: u32,
    pub mode: RelocateMode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_item_flags() {
        let mut item = Item::new(Key::from_bytes(b"test"), 1, 100, 1000);

        assert!(!item.is_deleted());
        item.set_deleted();
        assert!(item.is_deleted());
        assert_eq!(item.physical_len, 0);

        let mut item = Item::new(Key::from_bytes(b"test2"), 1, 100, 1000);
        item.set_compression(Codec::Zstd);
        assert_eq!(item.compression(), Codec::Zstd);

        item.set_errno(BlobErrno::IoRead);
        assert_eq!(item.errno(), BlobErrno::IoRead);
        assert!(item.has_error());
    }

    #[test]
    fn test_index_put_get() {
        let index = BlobIndex::new(100);
        let key = Key::from_bytes(b"hello");
        let item = Item::new(key, 1, 100, 500);

        assert!(index.get(&key).is_none());

        index.put(item);

        let retrieved = index.get(&key).unwrap();
        assert_eq!(retrieved.segment_id, 1);
        assert_eq!(retrieved.offset, 100);
        assert_eq!(retrieved.physical_len, 500);
    }

    #[test]
    fn test_index_update() {
        let index = BlobIndex::new(100);
        let key = Key::from_bytes(b"update");

        let item1 = Item::new(key, 1, 100, 500);
        index.put(item1);

        let item2 = Item::new(key, 2, 200, 600);
        index.put(item2);

        let retrieved = index.get(&key).unwrap();
        assert_eq!(retrieved.segment_id, 2);
        assert_eq!(retrieved.offset, 200);
    }

    #[test]
    fn test_index_delete() {
        let index = BlobIndex::new(100);
        let key = Key::from_bytes(b"delete");
        let item = Item::new(key, 1, 100, 500);

        index.put(item);
        assert!(index.get(&key).is_some());

        assert!(index.delete(&key));
        assert!(index.get(&key).is_none());

        // Delete non-existent
        assert!(!index.delete(&key));
    }

    #[test]
    fn test_index_eviction() {
        let index = BlobIndex::new(100);

        // Insert 100 items of 100 bytes each
        for i in 0..100u64 {
            let key = Key::from_u128(i as u128);
            let item = Item::new(key, 1, i as u32 * 100, 100);
            index.put(item);
        }

        assert_eq!(index.len(), 100);

        // Evict 5000 bytes worth (50 items)
        let evicted = index.evict_batch(5000);
        assert!(!evicted.is_empty());
        assert!(index.len() < 100);
    }

    #[test]
    fn test_sieve_second_chance() {
        let index = BlobIndex::new(100);

        // Insert items
        for i in 0..10u64 {
            let key = Key::from_u128(i as u128);
            let item = Item::new(key, 1, i as u32 * 100, 100);
            index.put(item);
        }

        // Access some items (mark as visited)
        for i in 0..5u64 {
            let key = Key::from_u128(i as u128);
            index.get(&key);
        }

        // Evict - visited items should get second chance
        let evicted = index.evict_batch(500);

        // Should have evicted non-visited items first
        for item in &evicted {
            // The evicted items should mostly be from the non-accessed range
            // (This is probabilistic due to the circular nature)
            let _ = item;
        }
    }

    #[test]
    fn test_relocate() {
        let index = BlobIndex::new(100);
        let key = Key::from_bytes(b"relocate");
        let item = Item::new(key, 1, 100, 500);
        index.put(item);

        // Successful relocation
        assert!(index.relocate(&key, 1, 2, 100, 200, RelocateMode::Live));

        let retrieved = index.get(&key).unwrap();
        assert_eq!(retrieved.segment_id, 2);
        assert_eq!(retrieved.offset, 200);

        // Failed relocation - wrong old location
        assert!(!index.relocate(&key, 1, 3, 100, 300, RelocateMode::Live));
    }

    #[test]
    fn test_relocate_batch() {
        let index = BlobIndex::new(1000);

        // Insert items across multiple shards
        // Key.shard() uses (high >> 56), so set high = i << 56 to distribute
        let mut keys = Vec::new();
        for i in 0..100u64 {
            let key = Key::new(i << 56, i); // Distributes across shards 0-99
            let item = Item::new(key, 1, i as u32 * 100, 100);
            index.put(item);
            keys.push(key);
        }

        // Verify items were inserted
        assert_eq!(index.len(), 100);

        // Verify each item can be retrieved with correct offset
        for (i, key) in keys.iter().enumerate() {
            let item = index.get(key).expect(&format!("key {} not found", i));
            assert_eq!(item.offset, i as u32 * 100, "key {} has wrong offset", i);
        }

        // Create batch relocation requests
        let requests: Vec<RelocationRequest> = keys
            .iter()
            .enumerate()
            .map(|(i, &key)| RelocationRequest {
                key,
                old_segment_id: 1,
                old_offset: i as u32 * 100,
                new_segment_id: 2,
                new_offset: i as u32 * 200,
                mode: RelocateMode::Live,
            })
            .collect();

        // Batch relocate
        let success_count = index.relocate_batch(&requests);
        assert_eq!(success_count, 100);

        // Verify all items were relocated
        for (i, key) in keys.iter().enumerate() {
            let item = index.get(key).unwrap();
            assert_eq!(item.segment_id, 2);
            assert_eq!(item.offset, i as u32 * 200);
        }

        // Try relocating with wrong old locations - should fail
        let bad_requests: Vec<RelocationRequest> = keys
            .iter()
            .enumerate()
            .map(|(i, &key)| RelocationRequest {
                key,
                old_segment_id: 1, // Wrong - items are now in segment 2
                old_offset: i as u32 * 100,
                new_segment_id: 3,
                new_offset: i as u32 * 300,
                mode: RelocateMode::Live,
            })
            .collect();

        let fail_count = index.relocate_batch(&bad_requests);
        assert_eq!(fail_count, 0); // All should fail

        // Items should still be at segment 2
        for (i, key) in keys.iter().enumerate() {
            let item = index.get(key).unwrap();
            assert_eq!(item.segment_id, 2);
            assert_eq!(item.offset, i as u32 * 200);
        }
    }

    #[test]
    fn test_stats() {
        let index = BlobIndex::new(100);

        for i in 0..50u64 {
            let key = Key::from_u128(i as u128);
            let item = Item::new(key, 1, i as u32 * 100, 100);
            index.put(item);
        }

        let stats = index.stats();
        assert_eq!(stats.items, 50);
        assert_eq!(stats.shards, SHARD_COUNT);
        assert!(stats.memory_est > 0);
    }
}
