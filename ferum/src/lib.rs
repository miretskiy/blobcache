//! Ferum - High-performance blob cache with CAS storage support
//!
//! A Rust port of BlobCache, implementing a dual-purpose storage system:
//! - **Cache Mode**: High-performance disk-first cache with SIEVE eviction
//! - **CAS Mode**: Durable Content Addressable Storage via Write-Ahead Log
//!
//! # Architecture
//!
//! Both modes share a unified log-structured architecture where segments ARE the log,
//! achieving write amplification of 1.00 by writing data exactly once.
//!
//! # Performance Targets
//!
//! - Saturate NVMe bandwidth: 1.1-1.2 GB/s sustained
//! - Zero-copy abstractions for read-after-write optimization
//! - Minimal GC impact via arena-backed structures
//! - Lock-free hot paths

pub mod bloom;
pub mod buffer_pool;
pub mod cache;
pub mod compaction;
pub mod compression;
pub mod config;
pub mod durable_index;
pub mod error;
pub mod index;
pub mod iosched;
pub mod key;
pub mod librarian;
pub mod mempool;
pub mod memtable;
pub mod persistence;
pub mod record;
pub mod recovery;
pub mod slab;
pub mod storage;
pub mod sys;
pub mod wal;

// Re-export main types for convenient access
pub use bloom::Filter as BloomFilter;
pub use cache::{cache, cas, Cache, CacheBuilder, CacheStats};
pub use compaction::{CompactResult, Compactor, SegmentStats, SparseSegment};
pub use compression::Codec;
pub use config::Config;
pub use error::{Error, Result};
pub use index::{BlobIndex, Item};
pub use key::Key;
pub use librarian::{Librarian, Publisher};
pub use mempool::{MmapBuffer, MmapPool};
pub use memtable::MemTable;
pub use slab::{ActiveSlab, PinnedBlob, SharedSlab, SlabEntry};
pub use storage::{Archivist, SegmentIDProvider, SegmentWriter};
pub use recovery::{recover_index, RecoveryResult};
pub use wal::Wal;
