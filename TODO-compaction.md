# Compaction Subsystem - Implementation Roadmap

**Created:** 2026-01-22
**Updated:** 2026-01-23
**Status:** COMPLETE - All tasks implemented

This document tracks the work required to complete the Delete/Compaction subsystem following the "Lazy Delete" architecture with incremental tombstones.

---

## Summary of Changes

All critical gaps and remaining work items have been addressed:

### Gap A: Key Verification in Delete() - FIXED
- Modified `slab.go` `SharedSlab.Acquire()` to return stored key bytes for collision detection
- Updated `librarian.go` `Acquire()` to pass through key bytes
- Updated `blobcache.go` `search()` to verify keys on RAM path using `bytes.Equal`
- `Delete()` now uses `search()` to verify key exists and matches before deletion

### Gap B: Relocate() for Tombstones - FIXED
- Added `expectDeleted bool` parameter to `Relocate()` in `internal/index/index.go`
- Updated `DurableIndex.Relocate()` wrapper in `internal/index/durable.go`
- Added tombstone relocation loop in `compaction.go` after live item relocation
- Added tests: `TestRelocate_TombstoneMigration`, `TestRelocate_TombstoneMigration_RaceDetection`

### Gap C: Dead Code Cleanup - FIXED
- Removed `numSegmentLockShards` and `segmentLockShardMask` constants
- Removed unused `segmentLocks [numSegmentLockShards]sync.Mutex` field

### Task 4: Segment Selection Policy - COMPLETE
- Created `compaction_policy.go` with:
  - `SegmentStats` struct for per-segment statistics
  - `computeSegmentStats()` for on-demand metric computation
  - `selectSegmentsForTombstoneCompaction()` for high-tombstone segments
  - `selectSegmentsForMerge()` for sparse segments
  - `selectContiguousRanges()` for grouping segment IDs

### Task 5: Track Oldest Live Segment - COMPLETE
- Added `oldestLiveSegmentID atomic.Uint32` to Cache struct
- Initialize during startup via `ForEachSegment` scan
- Added `IsTailSegment()` and `OldestLiveSegmentID()` methods
- Added `recalculateOldestSegmentID()` for post-compaction updates
- Modified `Compactor.Compact()` to accept `dropTombstones bool` parameter
- Added `TombstonesDropped` to `CompactResult`
- Added test: `TestCompactor_TombstoneDropping`

### Task 6: Compaction Execution Loop - COMPLETE
- Added `compactor *Compactor` field to Cache struct
- Initialize Compactor in `open()` function
- Implemented `maybeCompactSegments()` with two phases:
  1. Tombstone compaction (metadata cleanup + hole punching)
  2. Merge compaction (combine sparse segments)
- Implemented `maybeMergeSegments()`:
  - Selects segments with >75% waste ratio
  - Requires contiguous ranges of at least 2 segments
  - Automatically drops tombstones for tail segment compaction
  - Recalculates oldest segment after drops

### Task 7: Strict Contiguity Enforcer - COMPLETE
- `selectContiguousRanges()` groups segment IDs into contiguous ranges
- `selectSegmentsForMerge()` uses it to return only contiguous ranges
- `VerifyNoSegmentsInRange()` validates at runtime in `Compact()`
- Errors return without crashing, allowing compaction to continue

### Task 8: Wire CompactTombstones Trigger - COMPLETE
- Implemented `maybeCompactTombstones()` using `selectSegmentsForTombstoneCompaction()`
- Uses `DefaultTombstoneCompactionThreshold = 100` tombstones
- Acquires segment lock before calling `compactSegmentTombstones()`
- Collects errors and returns via `errors.Join()`

---

## Files Modified

### Core Changes
- `blobcache.go` - Delete key verification, compactor integration, oldest segment tracking
- `compaction.go` - `dropTombstones` parameter, `TombstonesDropped` result field
- `compaction_policy.go` - NEW FILE: segment selection policy
- `slab.go` - Return stored key bytes from `Acquire()`
- `librarian.go` - Pass through stored key bytes

### Index Changes
- `internal/index/index.go` - `expectDeleted` parameter in `Relocate()`
- `internal/index/durable.go` - Updated `Relocate()` wrapper

### Tests
- `compaction_test.go` - Updated all `Compact()` calls, added `TestCompactor_TombstoneDropping`
- `compaction_policy_test.go` - NEW FILE: selection policy tests
- `internal/index/index_test.go` - Updated `Relocate()` calls, added tombstone migration tests

---

## Remaining Considerations

### Future Enhancements (Not Blocking)

1. **Physical Bytes Tracking**: Currently selection uses tombstone count and waste ratio computed from items. Could enhance with actual disk usage from `stat.Blocks * 512` for more accurate merge decisions.

2. **Rate Limiting**: The execution loop runs all eligible compactions immediately. Consider adding rate limiting to avoid I/O storms during periods of high delete activity.

3. **Testing Hooks**: Could add `OnCompactionStart`, `OnCompactionFinish` to `TestingKnobs` for deterministic testing of compaction races.

4. **Metrics**: Consider exposing compaction metrics (segments compacted, tombstones dropped, bytes reclaimed) via a stats API.

---

## Architecture Notes

The implementation follows these key principles:

1. **Tombstone Safety**: Tombstones are only dropped when compacting the tail (oldest) segment, preventing resurrection of deleted keys.

2. **Contiguity Rule**: Only contiguous segment ranges are compacted, preventing the Leapfrog Hazard.

3. **Two-Phase Compaction**:
   - Phase 1: Tombstone compaction (hole punch + metadata cleanup)
   - Phase 2: Merge compaction (combine sparse segments)

4. **Atomic Relocation**: The `expectDeleted` parameter enables both live item and tombstone relocation while detecting races.

5. **Key Verification**: All paths now verify stored keys match user-provided keys, preventing hash collision corruption.

## TLA+ Compaction Model - COMPLETE

Verified in model/compaction/Compaction.tla and Compaction.cfg.

### Key Properties to Verify

**InvNoResurrection**: A deleted key must never become "Live" unless a User explicitly Puts it. Compaction must never resurrect a key.

**InvNoDataLoss**: If a key is updated by User (Version N+1), Compaction (Version N) must NOT overwrite it.

### State Model
```
index: Key -> [segID: Int, version: Int, deleted: Bool]
compactorState: "idle" | "reading" | "relocating"
snapshot: The item compactor read and is holding
```

### Actions
- Put(k): User writes new version to new segment
- Delete(k): User writes tombstone (deleted=TRUE)
- CompactorRead: Compactor reads index[k] into snapshot
- CompactorRelocate: CAS that succeeds IFF:
  - index[k].segID == snapshot.segID (location match)
  - index[k].version == snapshot.version (identity match)
  - index[k].deleted == snapshot.deleted (Ghost Guard)

### Mapping Table (to implement)
- index -> BlobIndex xmap
- Relocate -> idx.Relocate function in index.go
- version -> models Offset/Identity (if changed, it's a new write)

### Trace to Verify
Leapfrog Hazard: User Delete during Compaction must be blocked by Ghost Guard.
