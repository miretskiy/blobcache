# BlobCache TLA+ Formal Verification Report

**Date:** 2026-01-26
**Total States Explored:** 31,547,380
**Total Runtime:** ~22 minutes

---

> **Important Note:** This report documents the verification of **TLA+ models** that abstract BlobCache's concurrency algorithms. TLA+ verification proves the *algorithms* are correct, not the *implementation*. The Go code could still contain bugs not captured by the models (e.g., off-by-one errors, incorrect variable mappings, missing edge cases). The models and code should be reviewed together to ensure correspondence.

---

## Executive Summary

BlobCache's critical concurrency protocols have been formally verified using TLA+ and the TLC model checker. The verification exhaustively explores **all possible interleavings** of concurrent operations, proving that the modeled algorithms are correct.

| Model | States | Distinct | Depth | Runtime | Result |
|-------|--------|----------|-------|---------|--------|
| WAL Group Commit | 2,607,231 | 811,253 | 28 | 2m 25s | PASS |
| Slab Reference Counting | 24,123,724 | 3,926,371 | 18 | 16m 48s | PASS |
| Slab Mutant (ABA demo) | 83 | 45 | 6 | <1s | Bug demonstrated |
| MemTable Rotation | 2,281,718 | 744,315 | 37 | 2m 12s | PASS |
| Compaction Ghost Guard | 2,451,448 | 791,086 | 22 | 28s | PASS |

---

## Model 1: WAL Group Commit Protocol

**File:** `model/wal/WAL.tla`
**Go Code:** `internal/wal/wal.go`
**Configuration:** 3 writers, MaxSeqID=8, MaxQueueLen=4

### What It Models

The Write-Ahead Log uses a group commit protocol to amortize `fsync()` costs across concurrent writers. This model verifies:

1. **Leader Election:** Only one goroutine flushes at a time (`writerBusy` flag)
2. **Ping-Pong Swap:** Batch handoff is atomic and correct
3. **Sealed Guard:** Writes with stale SeqIDs are rejected after rotation
4. **Crash Recovery:** Acknowledged writes survive power failure

### Variable Mapping (TLA+ to Go)

| TLA+ Variable | Go Location | Go Field |
|---------------|-------------|----------|
| `writerBusy` | wal.go:122 | `WAL.writerBusy` |
| `pending` | wal.go:113 | `WAL.pending` |
| `flushing` | wal.go:114 | `WAL.flushing` |
| `lastRotatedSeq` | wal.go:108 | `WAL.lastRotatedSeq` |
| `currentMaxSeq` | wal.go:109 | `WAL.currentMaxSeq` |
| `disk` | (abstraction) | fsync'd file contents |

### Invariants Verified

| Invariant | Description | Status |
|-----------|-------------|--------|
| `InvType` | Type safety (all variables in valid domains) | PASS |
| `InvSingleLeader` | At most one leader/flusher at any time | PASS |
| `InvDurability` | If `Write()` returns success, data is on disk | PASS |

### Temporal Properties Verified

| Property | Description | Status |
|----------|-------------|--------|
| `PropCrashSafe` | Disk contents never shrink (fsync'd data persists) | PASS |
| `PropWriteCompletes` | Submitted writes eventually complete (no starvation) | PASS |

### Conclusion

The modeled group commit protocol is **correct**. The leader election ensures single-writer semantics, the ping-pong swap enables lock-free batch accumulation, and the sealed guard prevents stale writes from corrupting rotated files.

---

## Model 2: Slab Reference Counting (Fresh Struct Strategy)

**File:** `model/slab/Slab.tla`
**Go Code:** `mempool.go`, `slab.go`
**Configuration:** 3 memory blocks, 6 struct IDs, 3 readers

### What It Models

The memory pool uses lock-free reference counting for zero-copy access. The "Fresh Struct" strategy allocates a new `MmapBuffer` struct on each `Acquire()`, rather than pooling structs. This prevents the ABA problem.

### The ABA Problem

In lock-free systems, ABA occurs when:
1. Reader sees struct S with `refCount > 0`
2. Owner releases S, returns memory M to pool
3. New writer acquires M, gets reused struct S
4. Reader's CAS succeeds (same address!) but now points to wrong data

### Variable Mapping (TLA+ to Go)

| TLA+ Variable | Go Location | Go Field |
|---------------|-------------|----------|
| `memoryPool` | mempool.go:164 | `MmapPool.buffers` chan |
| `structs[s].memID` | mempool.go:19 | `MmapBuffer.raw` []byte |
| `structs[s].ref` | mempool.go:20 | `MmapBuffer.refCount` |
| `structs[s].leased` | mempool.go:28 | `MmapBuffer.leased` |
| `readerHolding` | (Librarian) | `SharedSlab` pointers |

### Invariants Verified

| Invariant | Description | Status |
|-----------|-------------|--------|
| `InvType` | Type safety | PASS |
| `InvSafeAccess` | Acquired memory is never in pool (prevents use-after-free) | PASS |
| `InvNoDoubleFree` | Memory cannot be returned twice to pool | PASS |
| `InvRefCountSanity` | `ref > 0` implies struct is leased | PASS |
| `InvMemoryAccounting` | Each memory block is either in pool OR owned (not both) | PASS |

### Temporal Properties Verified

| Property | Description | Status |
|----------|-------------|--------|
| `PropReaderResolves` | Readers holding pointers eventually acquire or fail | PASS |

### Conclusion

The modeled Fresh Struct strategy **correctly prevents the ABA problem**. When a reader holds a stale pointer to struct S1, and the memory is recycled to a new struct S2, the reader's `TryInc()` on S1 correctly fails because `S1.ref = 0`.

---

## Model 3: Slab Mutant (ABA Bug Demonstration)

**File:** `model/slab/Slab_Mutant.tla`
**Purpose:** Prove the Fresh Struct strategy is *necessary*

### The Mutant

This model uses struct **pooling** instead of fresh allocation. When memory is released, its struct ID is returned to a pool for reuse. This is the approach BlobCache intentionally *avoids*.

### Result: Bug Demonstrated in 6 Steps

```
State 1: Pool = {m1, m2}, structPool = {}
State 2: AllocFresh(m1) -> Struct 1 created, ref=1, points to m1
State 3: Share(r1, 1) -> Reader holds pointer to Struct 1, expects m1
State 4: OwnerRelease(1) -> Struct 1: ref=0, m1 back to pool, struct 1 to structPool
State 5: AllocReuse(m2, 1) -> Struct 1 REUSED, now points to m2!
State 6: TryIncSuccess(r1) -> Reader's TryInc SUCCEEDS but memory is WRONG!
```

**Invariant Violated:** `InvNoABA`
- Reader expected memory `m1` but struct now points to `m2`
- Reader has "valid" reference to someone else's buffer

### Conclusion

The mutant model **proves the Fresh Struct strategy is essential**. Without it, the ABA problem is exploitable in 6 steps. This is why BlobCache allocates fresh `MmapBuffer` structs rather than pooling them.

---

## Model 4: MemTable Rotation Barrier Synchronization

**File:** `model/memtable/Rotation.tla`
**Go Code:** `memtable.go`
**Configuration:** 3 writers, MaxSeqID=8, MaxSlabs=3

### What It Models

When a slab fills up, the MemTable must rotate to a new slab without losing writes from "slow writers" who reserved space but haven't completed.

### The Three Guards

1. **Time Travel Guard (`maxSealedSeq`):** Rejects writes with stale SeqIDs
2. **Barrier (`activeReady` channel):** Blocks new writers during rotation
3. **Drain (`pendingWrites` counter):** Waits for in-flight writes to complete

### Variable Mapping (TLA+ to Go)

| TLA+ Variable | Go Location | Go Field |
|---------------|-------------|----------|
| `slabs[s].pending` | slab.go:40 | `ActiveSlab.pendingWrites` |
| `slabs[s].sealed` | memtable.go:340 | `ActiveSlab.retired` |
| `activeSlabID` | memtable.go:46 | `mt.mu.active` |
| `barrierClosed` | memtable.go:47 | `mt.mu.activeReady != nil` |
| `maxSealedSeq` | memtable.go:52 | `mt.mu.maxSealedSeq` |

### Invariants Verified

| Invariant | Description | Status |
|-----------|-------------|--------|
| `InvType` | Type safety | PASS |
| `InvNoWriteAfterFlush` | No writer targets a flushed slab | PASS |
| `InvSeqIDTracked` | Reserved writer's seqID is recorded in target slab | PASS |
| `InvPendingCountSanity` | Pending count = actual reserved writers | PASS |
| `InvBarrierConsistency` | Closed barrier implies rotation in progress | PASS |
| `InvSealedNoNewEntry` | Sealed slabs cannot accept new reservations | PASS |

### Temporal Properties Verified

| Property | Description | Status |
|----------|-------------|--------|
| `PropRotationCompletes` | Started rotations eventually complete | PASS |
| `PropWritesComplete` | Reserved writers eventually complete | PASS |
| `PropWaitingResumes` | Blocked writers eventually proceed | PASS |

### Conclusion

The modeled barrier synchronization is **correct**. The atomic check-and-increment under the lock guarantees that any writer who passes the guards will have their `pendingWrites` counted, and the rotator will wait for them.

---

## Model 5: Compaction Ghost Guard

**File:** `model/compaction/Compaction.tla`
**Go Code:** `internal/index/index.go` (`Relocate` function)
**Configuration:** 3 keys, MaxVersion=8, MaxSegment=8

### What It Models

During compaction, items are relocated from old segments to new segments. The "Leapfrog Hazard" occurs when:

1. Key K exists in Segment A (live, version 1)
2. User deletes K -> tombstone in Segment B (deleted, version 2)
3. Compactor relocates K from A -> C, skipping the tombstone
4. **GHOST RESURRECTION:** K appears live in C, tombstone "leapfrogged"

### The Ghost Guard

The `Relocate()` function checks two conditions:

1. **Location Guard:** `segmentID` and `offset` must match snapshot
2. **State Guard (Ghost Guard):** `deleted` state must match expectation

### Variable Mapping (TLA+ to Go)

| TLA+ Variable | Go Location | Go Field |
|---------------|-------------|----------|
| `index[k].segID` | index.go:499 | `item.SegmentID` |
| `index[k].version` | index.go:499 | `item.Offset` (identity) |
| `index[k].deleted` | index.go:507 | `item.IsDeleted()` |
| `compactorSnapshot` | compaction.go | Item read from segment |
| `RelocateLive` | index.go:505 | `mode.ExpectDeleted()=FALSE` |
| `RelocateTombstone` | index.go:506 | `mode.ExpectDeleted()=TRUE` |

### Invariants Verified

| Invariant | Description | Status |
|-----------|-------------|--------|
| `InvType` | Type safety | PASS |
| `InvNoGhostResurrection` | Live relocation only succeeds if item was live | PASS |
| `InvNoDataLoss` | Relocation preserves exact version from snapshot | PASS |
| `InvTombstoneIntegrity` | Tombstone relocation only succeeds if item was deleted | PASS |
| `InvRelocationPreservesState` | Relocation preserves deleted flag | PASS |

### Temporal Properties Verified

| Property | Description | Status |
|----------|-------------|--------|
| `PropCompactorCompletes` | Compactor eventually completes (relocated or failed) | PASS |

### Conclusion

The modeled Ghost Guard **correctly prevents the Leapfrog Hazard**. If a user deletes a key between compactor read and relocate, the state guard fails (`index[k].deleted = TRUE != expected FALSE`), and the relocation is rejected.

---

## Technical Notes

### TLC Fingerprint Collision Probability

TLC uses 64-bit fingerprints internally to detect previously visited states. The reported collision probabilities (ranging from 10^-8 to 10^-6) refer to TLC's model checking accuracy, **not** BlobCache's bloom filter or any production code.

With N distinct states, birthday paradox gives collision probability ~ N^2 / 2^64:

| Model | Distinct States | Collision Probability |
|-------|-----------------|----------------------|
| WAL | 811,253 | ~10^-8 |
| Slab | 3,926,371 | ~10^-6 |
| MemTable | 744,315 | ~10^-8 |
| Compaction | 791,086 | ~10^-8 |

These probabilities are negligible for practical purposes. A collision would mean TLC missed exploring a state, potentially missing a bug.

### Bounded Model Checking

TLA+ models use finite bounds (e.g., 3 writers, 8 sequence IDs) to make state space exploration tractable. The verification proves correctness **within these bounds**. Arguments for why bounded verification implies unbounded correctness:

1. **Symmetry:** Additional writers/keys behave identically to modeled ones
2. **Monotonicity:** Larger sequence IDs don't introduce new behaviors
3. **Induction:** Properties that hold for N often hold for N+1

However, this is not a formal proof of unbounded correctness.

---

## Verification Scope

### What TLA+ Verification Provides

| Aspect | Coverage |
|--------|----------|
| Algorithm correctness | Proves modeled algorithms have no logical errors |
| Exhaustive exploration | Every reachable state within bounds is checked |
| Counterexamples | If a bug exists in the model, TLC produces an error trace |
| Concurrency coverage | All possible thread interleavings are explored |

### What TLA+ Verification Does NOT Provide

| Aspect | Limitation |
|--------|------------|
| Implementation correctness | Go code may differ from model; bugs in translation are not caught |
| Performance verification | TLA+ verifies correctness, not throughput or latency |
| Hardware fault tolerance | Assumes reliable memory, correct fsync semantics |
| Unbounded verification | Model uses finite bounds; extrapolation is informal |
| Memory safety | Go memory bugs (nil pointers, races) are not modeled |

### Model-Code Correspondence

Each TLA+ file contains a **Variable Mapping Table** that documents how TLA+ variables correspond to Go struct fields and line numbers. When the Go code changes, these mappings should be reviewed and updated.

**Recommended practice:** When modifying code covered by a TLA+ model:
1. Review the variable mapping table
2. Update the model if the algorithm changed
3. Re-run TLC to verify the modified algorithm
4. Update line number references in the mapping table

---

## Overall Conclusions

### Verified Algorithm Properties

1. **Durability:** Acknowledged WAL writes are on stable storage before returning
2. **Memory Safety:** No use-after-free in lock-free reference counting (via Fresh Struct)
3. **Data Integrity:** Slow writers cannot corrupt slab handoff during rotation
4. **Deletion Consistency:** Compaction cannot resurrect deleted keys

### Confidence Assessment

| Aspect | Assessment |
|--------|------------|
| **Algorithm Design** | High confidence - 31.5M states exhaustively explored |
| **Model Accuracy** | Medium confidence - requires manual review of variable mappings |
| **Implementation** | Not verified - Go code could have bugs not in model |
| **Liveness** | Verified with weak fairness (assumes Go scheduler is fair) |

### Recommendations

1. **Maintain model-code correspondence:** Update TLA+ models when algorithms change
2. **Increase bounds periodically:** Run overnight with larger state spaces for higher confidence
3. **Add new invariants:** As edge cases are discovered in production, encode them in models
4. **Code review with models:** Use variable mapping tables during code review
5. **Mutation testing:** Create "mutant" models (like Slab_Mutant) to verify guards are necessary

---

## Summary

BlobCache's core concurrency algorithms have been **formally verified at the model level**. The TLA+ models explore 31.5 million states across five critical subsystems, proving:

- WAL group commit maintains durability guarantees across crashes
- Lock-free reference counting is safe via the Fresh Struct strategy
- MemTable rotation correctly synchronizes slow writers via barriers
- Compaction cannot resurrect deleted keys due to the Ghost Guard

The verification provides high confidence in the **algorithmic correctness** of these protocols. However, the Go implementation should be independently reviewed to ensure it faithfully implements the verified algorithms.
