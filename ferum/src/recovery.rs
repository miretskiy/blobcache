//! Crash recovery: rebuild index from segment files.
//!
//! This module provides functionality to recover the index from segment
//! footer files (.iseg) after a crash or corruption.

use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::index::{BlobIndex, Item};
use crate::record::{read_segment_footer, FooterEntry, SegmentFooter, HEADER_SIZE};

// =============================================================================
// Recovery
// =============================================================================

/// Result of index recovery.
#[derive(Debug, Default)]
pub struct RecoveryResult {
    /// Number of valid segments recovered.
    pub valid_segments: usize,
    /// Number of corrupt segments removed.
    pub corrupt_segments: usize,
    /// Number of entries recovered.
    pub entries_recovered: usize,
    /// Maximum sequence ID found.
    pub max_seq_id: u64,
    /// Total physical size of recovered data.
    pub total_size: u64,
}

/// Recovers the index by scanning all segment files.
///
/// This function:
/// 1. Scans all shard directories for .seg files
/// 2. Reads the .iseg (footer) file for each segment
/// 3. Rebuilds the index from footer entries
///
/// Returns the recovered index and recovery statistics.
pub fn recover_index(base_path: &Path, shards: u32) -> Result<(Arc<BlobIndex>, RecoveryResult)> {
    let segments_dir = base_path.join("segments");

    let index = Arc::new(BlobIndex::new(1 << 20)); // 1M capacity hint
    let mut result = RecoveryResult::default();

    // Scan each shard directory
    for shard in 0..shards {
        let shard_dir = segments_dir.join(format!("{:02x}", shard));

        if !shard_dir.exists() {
            continue;
        }

        let entries = match fs::read_dir(&shard_dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };

        for entry in entries.flatten() {
            let path = entry.path();

            // Only process .seg files
            if !path.extension().map(|e| e == "seg").unwrap_or(false) {
                continue;
            }

            // Extract segment ID from filename
            let segment_id = match extract_segment_id(&path) {
                Some(id) => id,
                None => continue,
            };

            // Try to read footer from .iseg file
            let iseg_path = path.with_extension("iseg");
            match recover_segment(&iseg_path, segment_id) {
                Ok(footer) => {
                    // Update max sequence ID
                    if footer.max_seq_id > result.max_seq_id {
                        result.max_seq_id = footer.max_seq_id;
                    }

                    // Add entries to index
                    for entry in &footer.entries {
                        let item = footer_entry_to_item(entry, segment_id);
                        result.total_size += item.physical_len as u64;
                        index.put(item);
                        result.entries_recovered += 1;
                    }

                    result.valid_segments += 1;
                }
                Err(e) => {
                    eprintln!(
                        "recovery: corrupt segment {:?}, removing: {}",
                        path, e
                    );

                    // Remove corrupt segment files
                    let _ = fs::remove_file(&path);
                    let _ = fs::remove_file(&iseg_path);
                    result.corrupt_segments += 1;
                }
            }
        }
    }

    Ok((index, result))
}

/// Recovers a single segment from its .iseg file.
fn recover_segment(iseg_path: &Path, segment_id: u32) -> Result<SegmentFooter> {
    let file = File::open(iseg_path).map_err(|e| Error::io("open iseg file", e))?;
    let file_size = file.metadata().map_err(|e| Error::io("get file size", e))?.len();

    let mut reader = BufReader::new(file);
    let (footer, _) = read_segment_footer(&mut reader, file_size, Some(segment_id))?;

    Ok(footer)
}

/// Extracts segment ID from filename like "123456.seg".
fn extract_segment_id(path: &Path) -> Option<u32> {
    let stem = path.file_stem()?.to_str()?;
    stem.parse().ok()
}

/// Converts a FooterEntry to an index Item.
fn footer_entry_to_item(entry: &FooterEntry, segment_id: u32) -> Item {
    let physical_len = HEADER_SIZE as u32 + entry.key_len as u32 + entry.physical_size as u32;

    let mut item = Item {
        key: entry.key,
        segment_id,
        offset: entry.pos as u32,
        physical_len,
        flags: 0,
    };

    // Copy compression from entry flags
    let compression = entry.compression();
    item.set_compression(compression);

    // Copy deleted flag
    if entry.flags & (1 << 33) != 0 {
        item.set_deleted();
    }

    item
}

/// Lists all WAL files in the WAL directory.
pub fn list_wal_files(wal_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    if !wal_dir.exists() {
        return Ok(files);
    }

    let entries = fs::read_dir(wal_dir).map_err(|e| Error::io("read wal dir", e))?;

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().map(|e| e == "wal").unwrap_or(false) {
            files.push(path);
        }
    }

    // Sort by filename (which contains the first sequence ID)
    files.sort();

    Ok(files)
}

/// Computes the recovery checkpoint (highest SeqID across all segments).
/// WAL entries with SeqID > checkpoint need to be replayed.
pub fn compute_recovery_checkpoint(base_path: &Path, shards: u32) -> Result<u64> {
    let segments_dir = base_path.join("segments");
    let mut max_seq_id: u64 = 0;

    for shard in 0..shards {
        let shard_dir = segments_dir.join(format!("{:02x}", shard));

        if !shard_dir.exists() {
            continue;
        }

        let entries = match fs::read_dir(&shard_dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };

        for entry in entries.flatten() {
            let path = entry.path();

            if !path.extension().map(|e| e == "iseg").unwrap_or(false) {
                continue;
            }

            let segment_id = match extract_segment_id(&path.with_extension("seg")) {
                Some(id) => id,
                None => continue,
            };

            if let Ok(footer) = recover_segment(&path, segment_id) {
                if footer.max_seq_id > max_seq_id {
                    max_seq_id = footer.max_seq_id;
                }
            }
        }
    }

    Ok(max_seq_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_extract_segment_id() {
        let path = PathBuf::from("/data/segments/00/123456.seg");
        assert_eq!(extract_segment_id(&path), Some(123456));

        let path = PathBuf::from("/data/segments/00/invalid.seg");
        assert_eq!(extract_segment_id(&path), None);
    }

    #[test]
    fn test_recover_empty_directory() {
        let dir = tempdir().unwrap();
        let (index, result) = recover_index(dir.path(), 16).unwrap();

        assert_eq!(result.valid_segments, 0);
        assert_eq!(result.corrupt_segments, 0);
        assert_eq!(result.entries_recovered, 0);
        assert_eq!(index.stats().items, 0);
    }

    #[test]
    fn test_list_wal_files() {
        let dir = tempdir().unwrap();
        let wal_dir = dir.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        // Create some WAL files
        File::create(wal_dir.join("0000000001.wal")).unwrap();
        File::create(wal_dir.join("0000000002.wal")).unwrap();
        File::create(wal_dir.join("other.txt")).unwrap();

        let files = list_wal_files(&wal_dir).unwrap();
        assert_eq!(files.len(), 2);
    }
}
