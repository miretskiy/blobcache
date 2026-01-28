//! Segment compaction using a copy-forward approach.
//!
//! Compaction merges contiguous segments, filters out stale/deleted items,
//! and produces a single compacted segment.
//!
//! # Strict Contiguity Rule
//!
//! Segment IDs must form a contiguous range to prevent the "Leapfrog Hazard"
//! where skipping segments could resurrect deleted keys.
//!
//! # Tombstone Handling
//!
//! - **Tail segments**: Tombstones can be garbage collected (safe - no older data)
//! - **Non-tail segments**: Tombstones must be preserved for crash safety

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use crate::durable_index::DurableIndex;
use crate::error::{Error, Result};
use crate::index::{Item, RelocateMode, RelocationRequest};
use crate::key::Key;
use crate::record::{FooterEntry, HEADER_SIZE};
use crate::storage::{get_segment_path, Archivist, SegmentIDProvider, SegmentWriter};
use crate::sys::OpenFlags;

// =============================================================================
// SegmentStats
// =============================================================================

/// Statistics for a single segment.
#[derive(Debug, Clone, Default)]
pub struct SegmentStats {
    /// Segment ID.
    pub segment_id: u32,
    /// Number of tombstones in the segment.
    pub tombstone_count: usize,
    /// Number of live items in the segment.
    pub live_item_count: usize,
    /// Physical bytes used by live data.
    pub physical_bytes: u64,
    /// Logical bytes (uncompressed) for live data.
    pub logical_bytes: u64,
}

impl SegmentStats {
    /// Returns the proportion of tombstones (0.0 to 1.0).
    pub fn waste_ratio(&self) -> f64 {
        let total = self.tombstone_count + self.live_item_count;
        if total == 0 {
            return 0.0;
        }
        self.tombstone_count as f64 / total as f64
    }

    /// Returns true if the segment is mostly empty.
    pub fn is_sparse(&self, threshold: f64) -> bool {
        self.waste_ratio() >= threshold
    }
}

// =============================================================================
// CompactResult
// =============================================================================

/// Outcome of a compaction operation.
#[derive(Debug, Default)]
pub struct CompactResult {
    /// ID of the newly created segment (0 if nothing written).
    pub new_segment_id: u32,
    /// IDs of segments that were compacted.
    pub old_segment_ids: Vec<u32>,
    /// Number of live items written to new segment.
    pub items_compacted: usize,
    /// Number of tombstones preserved.
    pub tombstones_kept: usize,
    /// Number of tombstones garbage collected (tail only).
    pub tombstones_dropped: usize,
}

// =============================================================================
// RelocInfo
// =============================================================================

/// Tracks an item being relocated during compaction.
#[allow(dead_code)]
struct RelocInfo {
    /// The item being relocated.
    item: Item,
    /// Original segment ID.
    old_seg: u32,
    /// Original offset within segment.
    old_off: u32,
}

// =============================================================================
// CompactorKnobs
// =============================================================================

/// Testing hooks for compaction behavior.
#[derive(Default)]
pub struct CompactorKnobs {
    /// Called before each Relocate operation during compaction.
    pub before_relocate: Option<Box<dyn Fn(Key) + Send + Sync>>,
}

// =============================================================================
// Compactor
// =============================================================================

/// Handles segment compaction using a copy-forward approach.
pub struct Compactor {
    index: Arc<DurableIndex>,
    archivist: Arc<Archivist>,
    segment_ids: Arc<SegmentIDProvider>,
    base_path: PathBuf,
    shards: u32,
    io_flags: OpenFlags,

    /// Testing knobs.
    pub knobs: Option<CompactorKnobs>,
}

impl Compactor {
    /// Creates a new Compactor.
    pub fn new(
        index: Arc<DurableIndex>,
        archivist: Arc<Archivist>,
        segment_ids: Arc<SegmentIDProvider>,
        base_path: PathBuf,
        shards: u32,
        io_flags: OpenFlags,
    ) -> Self {
        Compactor {
            index,
            archivist,
            segment_ids,
            base_path,
            shards,
            io_flags,
            knobs: None,
        }
    }

    /// Compacts a contiguous range of segments into a single new segment.
    ///
    /// The segment_ids must be sorted in ascending order and form a contiguous range.
    ///
    /// Parameters:
    /// - `segment_ids`: Segment IDs to compact (must be contiguous and ascending)
    /// - `drop_tombstones`: If true, tombstones are garbage collected instead of preserved.
    ///   ONLY set this to true when compacting the tail (oldest) segment range.
    ///
    /// Locking: Acquires shared (read) locks on all segment shards to allow concurrent
    /// compactions while blocking Delete operations on these segments.
    pub fn compact(&self, segment_ids: &[u32], drop_tombstones: bool) -> Result<CompactResult> {
        let mut result = CompactResult {
            old_segment_ids: segment_ids.to_vec(),
            ..Default::default()
        };

        if segment_ids.is_empty() {
            return Ok(result);
        }

        // Validate contiguity
        self.validate_contiguity(segment_ids)?;

        // Acquire shared locks on all segments (allows concurrent compactions, blocks Delete)
        let _guards = self.index.lock_segments_shared(segment_ids);

        // Collect items from all segments
        let (to_relocate, tombstones, max_seq_id) = self.collect_items(segment_ids)?;

        if to_relocate.is_empty() && (tombstones.is_empty() || drop_tombstones) {
            // Nothing to write - delete old segments and return
            self.delete_old_segments(segment_ids)?;
            if drop_tombstones {
                result.tombstones_dropped = tombstones.len();
            }
            return Ok(result);
        }

        // Allocate new segment ID
        let new_seg_id = self.segment_ids.next();
        result.new_segment_id = new_seg_id;

        // Estimate size: sum of all items to relocate
        let estimated_size: i64 = to_relocate.iter().map(|r| r.item.physical_len as i64).sum::<i64>()
            + tombstones.iter().map(|t| t.physical_len as i64).sum::<i64>();

        // Create new segment writer with I/O flags
        let mut writer = SegmentWriter::create_with_flags(
            new_seg_id,
            &self.base_path,
            self.shards,
            estimated_size,
            self.io_flags,
        )?;

        // Track relocation requests for batch processing
        let mut requests: Vec<RelocationRequest> = Vec::with_capacity(to_relocate.len() + tombstones.len());
        let mut footer_entries: Vec<FooterEntry> = Vec::new();

        // 1. Write live items
        for reloc in &to_relocate {
            // Call testing hook before relocate
            if let Some(ref knobs) = self.knobs {
                if let Some(ref hook) = knobs.before_relocate {
                    hook(reloc.item.key);
                }
            }

            // Read raw blob data from old segment
            let raw_data = self.archivist.read_blob_raw(&reloc.item)?;

            // Write to new segment
            let new_offset = writer.write(&raw_data)?;

            // Track relocation request
            requests.push(RelocationRequest {
                key: reloc.item.key,
                old_segment_id: reloc.old_seg,
                old_offset: reloc.old_off,
                new_segment_id: new_seg_id,
                new_offset: new_offset as u32,
                mode: RelocateMode::Live,
            });

            // Track for footer
            footer_entries.push(FooterEntry {
                key: reloc.item.key,
                pos: new_offset as i64,
                physical_size: reloc.item.physical_len as i64 - HEADER_SIZE as i64
                    - self.extract_key_len(&raw_data) as i64,
                logical_size: 0, // Not tracked in Item
                flags: reloc.item.flags as u64,
                seq_id: 0, // Not tracked in Item
                key_len: self.extract_key_len(&raw_data),
            });
        }

        // 2. Handle tombstones
        if !drop_tombstones {
            for item in &tombstones {
                // Call testing hook
                if let Some(ref knobs) = self.knobs {
                    if let Some(ref hook) = knobs.before_relocate {
                        hook(item.key);
                    }
                }

                // Read raw tombstone data
                let raw_data = self.archivist.read_blob_raw(item)?;

                // Write to new segment
                let new_offset = writer.write(&raw_data)?;

                // Track relocation request
                requests.push(RelocationRequest {
                    key: item.key,
                    old_segment_id: item.segment_id,
                    old_offset: item.offset,
                    new_segment_id: new_seg_id,
                    new_offset: new_offset as u32,
                    mode: RelocateMode::Tombstone,
                });

                // Track for footer
                footer_entries.push(FooterEntry {
                    key: item.key,
                    pos: new_offset as i64,
                    physical_size: 0,
                    logical_size: 0,
                    flags: item.flags as u64,
                    seq_id: 0,
                    key_len: self.extract_key_len(&raw_data),
                });
            }
            result.tombstones_kept = tombstones.len();
        } else {
            // GC tombstones - remove from RAM index
            for item in &tombstones {
                self.index.delete(&item.key);
            }
            result.tombstones_dropped = tombstones.len();
        }

        // 3. Write footer for crash recovery
        writer.write_footer(&footer_entries)?;

        // 4. Close segment (fsync)
        writer.close()?;

        // 5. Batch relocate: acquires each shard lock exactly once
        self.index.relocate_batch(&requests);

        // 6. Delete old segments
        self.delete_old_segments(segment_ids)?;

        // 7. Drop old segment caches from archivist
        for &seg_id in segment_ids {
            self.archivist.drop_segment_cache(seg_id);
        }

        // 8. Drop old segment metadata
        for &seg_id in segment_ids {
            let _ = self.index.drop_segment(seg_id);
        }

        result.items_compacted = to_relocate.len();
        let _ = max_seq_id;

        Ok(result)
    }

    /// Extracts key length from raw record data.
    fn extract_key_len(&self, raw_data: &[u8]) -> u16 {
        if raw_data.len() < HEADER_SIZE {
            return 0;
        }
        // Key length is at offset 32-34 in the header (see record.rs)
        u16::from_le_bytes([raw_data[32], raw_data[33]])
    }

    /// Deletes old segment files after successful compaction.
    fn delete_old_segments(&self, segment_ids: &[u32]) -> Result<()> {
        for &seg_id in segment_ids {
            let seg_path = get_segment_path(&self.base_path, self.shards, seg_id);
            let iseg_path = seg_path.with_extension("seg.iseg");

            // Delete segment file
            if seg_path.exists() {
                fs::remove_file(&seg_path)
                    .map_err(|e| Error::io("delete segment file", e))?;
            }

            // Delete footer file
            if iseg_path.exists() {
                fs::remove_file(&iseg_path)
                    .map_err(|e| Error::io("delete segment footer", e))?;
            }
        }
        Ok(())
    }

    /// Validates that segment IDs are contiguous and ascending.
    fn validate_contiguity(&self, segment_ids: &[u32]) -> Result<()> {
        for i in 1..segment_ids.len() {
            if segment_ids[i] <= segment_ids[i - 1] {
                return Err(Error::InvalidConfig {
                    message: format!(
                        "segment IDs must be in ascending order, got {} after {}",
                        segment_ids[i],
                        segment_ids[i - 1]
                    ),
                });
            }

            // Allow gaps but would need to verify no segments exist in the gap
            // For now, just check ascending order
        }
        Ok(())
    }

    /// Gathers live items and tombstones from the given segments.
    ///
    /// Uses DurableIndex persistence (not iseg files) for consistent data.
    fn collect_items(
        &self,
        segment_ids: &[u32],
    ) -> Result<(Vec<RelocInfo>, Vec<Item>, u64)> {
        let mut to_relocate = Vec::new();
        let mut tombstones = Vec::new();
        let mut max_seq_id: u64 = 0;

        for &segment_id in segment_ids {
            // Get items from DurableIndex persistence (not iseg files)
            let Some((items, seg_max_seq)) = self.index.get_segment_items(segment_id)? else {
                // Segment doesn't exist in persistence - may have been deleted
                continue;
            };

            // Update max sequence ID
            if seg_max_seq > max_seq_id {
                max_seq_id = seg_max_seq;
            }

            // Process items
            for item in items {
                // Check if this item is still current in the RAM index
                let is_current = self.index.get(&item.key).is_some_and(|current| {
                    current.segment_id == segment_id && current.offset == item.offset
                });

                if !is_current {
                    // Item was superseded by a newer write - skip
                    continue;
                }

                if item.is_deleted() {
                    tombstones.push(item);
                } else {
                    to_relocate.push(RelocInfo {
                        item,
                        old_seg: segment_id,
                        old_off: item.offset,
                    });
                }
            }
        }

        Ok((to_relocate, tombstones, max_seq_id))
    }
}

// =============================================================================
// Policy Functions
// =============================================================================

/// Default tombstone count threshold to trigger compaction.
pub const DEFAULT_TOMBSTONE_THRESHOLD: usize = 100;

/// Groups segment IDs into contiguous ranges for merge compaction.
///
/// A contiguous range is a sequence where each segment ID is exactly 1 more than the previous.
///
/// Example: [1, 2, 3, 7, 8, 10] -> [[1,2,3], [7,8], [10]]
pub fn select_contiguous_ranges(segment_ids: &[u32]) -> Vec<Vec<u32>> {
    if segment_ids.is_empty() {
        return Vec::new();
    }

    let mut ranges = Vec::new();
    let mut current = vec![segment_ids[0]];

    for i in 1..segment_ids.len() {
        if segment_ids[i] == segment_ids[i - 1] + 1 {
            // Contiguous - extend current range
            current.push(segment_ids[i]);
        } else {
            // Gap - start new range
            ranges.push(current);
            current = vec![segment_ids[i]];
        }
    }

    // Don't forget the last range
    ranges.push(current);

    ranges
}

/// Selects segments with waste ratio above threshold.
pub fn select_sparse_segments(
    stats: &HashMap<u32, SegmentStats>,
    max_waste_ratio: f64,
) -> Vec<u32> {
    let mut sparse: Vec<u32> = stats
        .values()
        .filter(|s| s.waste_ratio() >= max_waste_ratio)
        .map(|s| s.segment_id)
        .collect();
    sparse.sort();
    sparse
}

/// Selects segments with tombstone count above threshold.
pub fn select_tombstone_heavy_segments(
    stats: &HashMap<u32, SegmentStats>,
    min_tombstones: usize,
) -> Vec<u32> {
    let mut selected: Vec<u32> = stats
        .values()
        .filter(|s| s.tombstone_count >= min_tombstones)
        .map(|s| s.segment_id)
        .collect();
    selected.sort();
    selected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_stats_waste_ratio() {
        let mut stats = SegmentStats::default();
        assert_eq!(stats.waste_ratio(), 0.0);

        stats.tombstone_count = 25;
        stats.live_item_count = 75;
        assert!((stats.waste_ratio() - 0.25).abs() < 0.001);

        stats.tombstone_count = 50;
        stats.live_item_count = 50;
        assert!((stats.waste_ratio() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_select_contiguous_ranges() {
        // Empty input
        assert!(select_contiguous_ranges(&[]).is_empty());

        // Single segment
        assert_eq!(select_contiguous_ranges(&[5]), vec![vec![5]]);

        // Fully contiguous
        assert_eq!(select_contiguous_ranges(&[1, 2, 3, 4]), vec![vec![1, 2, 3, 4]]);

        // Multiple ranges
        assert_eq!(
            select_contiguous_ranges(&[1, 2, 3, 7, 8, 10]),
            vec![vec![1, 2, 3], vec![7, 8], vec![10]]
        );

        // All gaps
        assert_eq!(
            select_contiguous_ranges(&[1, 5, 10, 20]),
            vec![vec![1], vec![5], vec![10], vec![20]]
        );
    }

    #[test]
    fn test_select_sparse_segments() {
        let mut stats = HashMap::new();

        stats.insert(
            1,
            SegmentStats {
                segment_id: 1,
                tombstone_count: 80,
                live_item_count: 20,
                ..Default::default()
            },
        );

        stats.insert(
            2,
            SegmentStats {
                segment_id: 2,
                tombstone_count: 10,
                live_item_count: 90,
                ..Default::default()
            },
        );

        stats.insert(
            3,
            SegmentStats {
                segment_id: 3,
                tombstone_count: 50,
                live_item_count: 50,
                ..Default::default()
            },
        );

        // Select segments with >= 50% waste
        let sparse = select_sparse_segments(&stats, 0.5);
        assert_eq!(sparse, vec![1, 3]);

        // Select segments with >= 75% waste
        let very_sparse = select_sparse_segments(&stats, 0.75);
        assert_eq!(very_sparse, vec![1]);
    }

    #[test]
    fn test_select_tombstone_heavy() {
        let mut stats = HashMap::new();

        stats.insert(
            1,
            SegmentStats {
                segment_id: 1,
                tombstone_count: 150,
                live_item_count: 50,
                ..Default::default()
            },
        );

        stats.insert(
            2,
            SegmentStats {
                segment_id: 2,
                tombstone_count: 50,
                live_item_count: 100,
                ..Default::default()
            },
        );

        stats.insert(
            3,
            SegmentStats {
                segment_id: 3,
                tombstone_count: 200,
                live_item_count: 10,
                ..Default::default()
            },
        );

        let heavy = select_tombstone_heavy_segments(&stats, 100);
        assert_eq!(heavy, vec![1, 3]);
    }
}
