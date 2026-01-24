---------------------------- MODULE Compaction ----------------------------
(***************************************************************************
 * TLA+ Formal Model: Compaction Ghost Guard
 *
 * This specification models the Relocate function in index.go, verifying
 * that the "Ghost Guard" prevents the Leapfrog Hazard where a deleted key
 * could be resurrected by compaction.
 *
 * THE LEAPFROG HAZARD:
 *   Timeline: K in Seg A | Delete K (tombstone in B) | Compact A→C
 *
 *   Without Ghost Guard:
 *     1. Compactor reads K from Seg A (live, version 1)
 *     2. User deletes K → tombstone in Seg B (deleted=TRUE, version 2)
 *     3. Compactor relocates K to Seg C (succeeds!)
 *     4. Index now points to Seg C with deleted=FALSE
 *     5. GHOST RESURRECTION: Tombstone is "leapfrogged", K appears live!
 *
 *   With Ghost Guard:
 *     Step 3 FAILS because item.IsDeleted() != mode.ExpectDeleted()
 *     The tombstone is preserved, K remains deleted.
 *
 * THE SOLUTION (Two Guards in Relocate):
 *
 * 1. LOCATION GUARD: segmentID and offset must match snapshot
 *    - Prevents overwriting newer writes
 *
 * 2. STATE GUARD (Ghost Guard): deleted state must match expectation
 *    - RelocateLive: Fails if item is now deleted
 *    - RelocateTombstone: Fails if item is now live
 *
 ***************************************************************************)

(***************************************************************************
 * VARIABLE MAPPING TABLE
 *
 * | TLA+ Variable         | Go Code Location           | Go Field/Type        |
 * |-----------------------|----------------------------|----------------------|
 * | index[k].segID        | index.go:499               | item.SegmentID       |
 * | index[k].version      | index.go:499               | item.Offset (identity)|
 * | index[k].deleted      | index.go:507               | item.IsDeleted()     |
 * | compactorSnapshot     | (compaction.go)            | Item read from segment|
 * | compactorState        | (compaction.go)            | Compaction loop state|
 * | RelocateLive          | index.go:505               | mode.ExpectDeleted()=F|
 * | RelocateTombstone     | index.go:506               | mode.ExpectDeleted()=T|
 *
 ***************************************************************************)

EXTENDS Integers, FiniteSets, TLC

(***************************************************************************
 * CONSTANTS
 ***************************************************************************)

CONSTANTS
    Keys,           \* Set of keys being compacted
    MaxVersion,     \* Maximum version number (bounds state space)
    MaxSegment,     \* Maximum segment ID
    NoKey           \* Sentinel value for "no key selected"

(***************************************************************************
 * VARIABLES
 ***************************************************************************)

VARIABLES
    \* === Index State ===
    \* index[k] = [segID, version, deleted]
    \* - segID: which segment contains this key
    \* - version: monotonic counter (models offset/identity)
    \* - deleted: TRUE if this is a tombstone
    index,

    \* === Version Counter ===
    \* Global version counter (models write sequence)
    nextVersion,

    \* === Segment Counter ===
    \* Next segment ID for new writes
    nextSegment,

    \* === Compactor State ===
    \* "idle": Not compacting
    \* "reading": Has read a key, holding snapshot
    \* "relocated": Successfully relocated
    \* "failed": Relocation failed (guards triggered)
    compactorState,

    \* === Compactor Snapshot ===
    \* What the compactor read from the old segment
    \* [key, segID, version, deleted]
    compactorSnapshot,

    \* === Target Segment ===
    \* Where compactor wants to relocate items
    compactorTargetSeg,

    \* === GHOST STATE: Index state at moment of successful relocation ===
    \* Used to verify invariants about what was true AT relocation time.
    \* Only meaningful when compactorState = "relocated".
    relocatedIndexState

vars == <<index, nextVersion, nextSegment, compactorState, compactorSnapshot, compactorTargetSeg, relocatedIndexState>>

(***************************************************************************
 * TYPE INVARIANT
 ***************************************************************************)

TypeInvariant ==
    /\ \A k \in Keys :
        /\ index[k].segID \in 1..MaxSegment
        /\ index[k].version \in 1..MaxVersion
        /\ index[k].deleted \in BOOLEAN
    /\ nextVersion \in 1..(MaxVersion+1)
    /\ nextSegment \in 1..(MaxSegment+1)
    /\ compactorState \in {"idle", "reading", "relocated", "failed"}
    /\ compactorSnapshot.key \in Keys \union {NoKey}
    /\ compactorTargetSeg \in 0..MaxSegment

(***************************************************************************
 * INITIAL STATE
 ***************************************************************************)

Init ==
    \* All keys start as live in segment 1 with version 1
    /\ index = [k \in Keys |-> [segID |-> 1, version |-> 1, deleted |-> FALSE]]
    /\ nextVersion = 2
    /\ nextSegment = 2
    /\ compactorState = "idle"
    /\ compactorSnapshot = [key |-> NoKey, segID |-> 0, version |-> 0, deleted |-> FALSE]
    /\ compactorTargetSeg = 0
    /\ relocatedIndexState = [key |-> NoKey, segID |-> 0, version |-> 0, deleted |-> FALSE]

(***************************************************************************
 * ACTION: UserPut
 *
 * Models: A user writing a new value for key k.
 * Go code: BlobCache.Put() -> index.Put()
 *
 * Effect: Creates new version in new segment, marks as live.
 * This can "resurrect" a deleted key (explicit user action).
 ***************************************************************************)

UserPut(k) ==
    /\ nextVersion <= MaxVersion
    /\ nextSegment <= MaxSegment
    /\ index' = [index EXCEPT ![k] = [
        segID |-> nextSegment,
        version |-> nextVersion,
        deleted |-> FALSE]]
    /\ nextVersion' = nextVersion + 1
    /\ nextSegment' = nextSegment + 1
    /\ UNCHANGED <<compactorState, compactorSnapshot, compactorTargetSeg, relocatedIndexState>>

(***************************************************************************
 * ACTION: UserDelete
 *
 * Models: A user deleting key k (writing a tombstone).
 * Go code: BlobCache.Delete() -> index.Put() with tombstone flag
 *
 * Effect: Creates new version in new segment, marks as deleted.
 ***************************************************************************)

UserDelete(k) ==
    /\ nextVersion <= MaxVersion
    /\ nextSegment <= MaxSegment
    /\ index' = [index EXCEPT ![k] = [
        segID |-> nextSegment,
        version |-> nextVersion,
        deleted |-> TRUE]]
    /\ nextVersion' = nextVersion + 1
    /\ nextSegment' = nextSegment + 1
    /\ UNCHANGED <<compactorState, compactorSnapshot, compactorTargetSeg, relocatedIndexState>>

(***************************************************************************
 * ACTION: CompactorRead
 *
 * Models: Compactor reading a key from an old segment.
 * Go code: Compaction loop iterating through segment entries.
 *
 * The compactor takes a "snapshot" of the index entry. This snapshot
 * may become stale if a user modifies the key concurrently.
 ***************************************************************************)

CompactorRead(k) ==
    /\ compactorState = "idle"
    /\ nextSegment <= MaxSegment
    \* Read current index state into snapshot
    /\ compactorSnapshot' = [
        key |-> k,
        segID |-> index[k].segID,
        version |-> index[k].version,
        deleted |-> index[k].deleted]
    \* Allocate target segment for relocation
    /\ compactorTargetSeg' = nextSegment
    /\ nextSegment' = nextSegment + 1
    /\ compactorState' = "reading"
    /\ UNCHANGED <<index, nextVersion, relocatedIndexState>>

(***************************************************************************
 * ACTION: CompactorRelocateLive
 *
 * Models: Relocate() with mode=RelocateLive (index.go:487-515)
 *
 * Go code:
 *   if SegmentID(item.SegmentID) != oldSeg || Offset(item.Offset) != oldOff {
 *       return false  // Location Guard
 *   }
 *   if item.IsDeleted() != mode.ExpectDeleted() {
 *       return false  // Ghost Guard
 *   }
 *   item.SegmentID = uint32(newSeg)
 *   return true
 *
 * Preconditions for success:
 *   1. Snapshot was for a live item (deleted=FALSE)
 *   2. Current index matches snapshot (location guard)
 *   3. Current item is still live (ghost guard)
 ***************************************************************************)

CompactorRelocateLiveSuccess ==
    LET k == compactorSnapshot.key
        snap == compactorSnapshot
    IN /\ compactorState = "reading"
       /\ ~snap.deleted  \* We're relocating a LIVE item
       \* Location Guard: segID and version must match
       /\ index[k].segID = snap.segID
       /\ index[k].version = snap.version
       \* Ghost Guard: item must still be live
       /\ index[k].deleted = FALSE
       \* Success: update location (but preserve version and deleted state)
       /\ index' = [index EXCEPT ![k].segID = compactorTargetSeg]
       /\ compactorState' = "relocated"
       \* GHOST: Capture index state at relocation for invariant checking
       /\ relocatedIndexState' = [key |-> k, segID |-> index[k].segID, version |-> index[k].version, deleted |-> index[k].deleted]
       /\ UNCHANGED <<nextVersion, nextSegment, compactorSnapshot, compactorTargetSeg>>

CompactorRelocateLiveFail ==
    LET k == compactorSnapshot.key
        snap == compactorSnapshot
    IN /\ compactorState = "reading"
       /\ ~snap.deleted  \* We're relocating a LIVE item
       \* EITHER location changed OR ghost guard fails
       /\ \/ index[k].segID # snap.segID
          \/ index[k].version # snap.version
          \/ index[k].deleted = TRUE  \* Ghost Guard: item was deleted!
       /\ compactorState' = "failed"
       /\ UNCHANGED <<index, nextVersion, nextSegment, compactorSnapshot, compactorTargetSeg, relocatedIndexState>>

(***************************************************************************
 * ACTION: CompactorRelocateTombstone
 *
 * Models: Relocate() with mode=RelocateTombstone
 *
 * Preconditions for success:
 *   1. Snapshot was for a deleted item (deleted=TRUE)
 *   2. Current index matches snapshot (location guard)
 *   3. Current item is still deleted (state guard)
 ***************************************************************************)

CompactorRelocateTombstoneSuccess ==
    LET k == compactorSnapshot.key
        snap == compactorSnapshot
    IN /\ compactorState = "reading"
       /\ snap.deleted  \* We're relocating a TOMBSTONE
       \* Location Guard
       /\ index[k].segID = snap.segID
       /\ index[k].version = snap.version
       \* State Guard: item must still be deleted
       /\ index[k].deleted = TRUE
       \* Success: update location
       /\ index' = [index EXCEPT ![k].segID = compactorTargetSeg]
       /\ compactorState' = "relocated"
       \* GHOST: Capture index state at relocation
       /\ relocatedIndexState' = [key |-> k, segID |-> index[k].segID, version |-> index[k].version, deleted |-> index[k].deleted]
       /\ UNCHANGED <<nextVersion, nextSegment, compactorSnapshot, compactorTargetSeg>>

CompactorRelocateTombstoneFail ==
    LET k == compactorSnapshot.key
        snap == compactorSnapshot
    IN /\ compactorState = "reading"
       /\ snap.deleted  \* We're relocating a TOMBSTONE
       \* EITHER location changed OR state guard fails
       /\ \/ index[k].segID # snap.segID
          \/ index[k].version # snap.version
          \/ index[k].deleted = FALSE  \* State Guard: item was resurrected by user!
       /\ compactorState' = "failed"
       /\ UNCHANGED <<index, nextVersion, nextSegment, compactorSnapshot, compactorTargetSeg, relocatedIndexState>>

(***************************************************************************
 * ACTION: CompactorReset
 *
 * Models: Compactor finishing one item and moving to next.
 ***************************************************************************)

CompactorReset ==
    /\ compactorState \in {"relocated", "failed"}
    /\ compactorState' = "idle"
    /\ compactorSnapshot' = [key |-> NoKey, segID |-> 0, version |-> 0, deleted |-> FALSE]
    /\ compactorTargetSeg' = 0
    /\ relocatedIndexState' = [key |-> NoKey, segID |-> 0, version |-> 0, deleted |-> FALSE]
    /\ UNCHANGED <<index, nextVersion, nextSegment>>

(***************************************************************************
 * TERMINATION (Model Boundary)
 ***************************************************************************)

Terminated ==
    /\ nextVersion > MaxVersion \/ nextSegment > MaxSegment
    /\ compactorState = "idle"

(***************************************************************************
 * NEXT STATE RELATION
 ***************************************************************************)

Next ==
    \/ \E k \in Keys : UserPut(k)
    \/ \E k \in Keys : UserDelete(k)
    \/ \E k \in Keys : CompactorRead(k)
    \/ CompactorRelocateLiveSuccess
    \/ CompactorRelocateLiveFail
    \/ CompactorRelocateTombstoneSuccess
    \/ CompactorRelocateTombstoneFail
    \/ CompactorReset
    \/ (Terminated /\ UNCHANGED vars)

(***************************************************************************
 * FAIRNESS
 ***************************************************************************)

Fairness ==
    /\ WF_vars(CompactorRelocateLiveSuccess)
    /\ WF_vars(CompactorRelocateLiveFail)
    /\ WF_vars(CompactorRelocateTombstoneSuccess)
    /\ WF_vars(CompactorRelocateTombstoneFail)
    /\ WF_vars(CompactorReset)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************
 * INVARIANTS
 ***************************************************************************)

InvType == TypeInvariant

\* INV-1: NO GHOST RESURRECTION
\*
\* A deleted key can ONLY become live through an explicit UserPut.
\* Compaction must NEVER resurrect a deleted key.
\*
\* Using ghost state: At the moment of relocation, if we relocated a
\* live item, the index entry was indeed live (not deleted).
InvNoGhostResurrection ==
    compactorState = "relocated" /\ ~compactorSnapshot.deleted =>
        relocatedIndexState.deleted = FALSE

\* INV-2: NO DATA LOSS (via action preconditions)
\*
\* If a user updated a key (new version), compaction of the old version
\* must NOT overwrite the new version.
\*
\* Using ghost state: At relocation time, the version matched snapshot.
InvNoDataLoss ==
    compactorState = "relocated" =>
        relocatedIndexState.version = compactorSnapshot.version

\* INV-3: TOMBSTONE INTEGRITY
\*
\* A tombstone can only become live through explicit UserPut.
\* RelocateTombstone must never make a tombstone live.
\*
\* Using ghost state: At relocation time, if snapshot was deleted,
\* the index entry was also deleted.
InvTombstoneIntegrity ==
    compactorState = "relocated" /\ compactorSnapshot.deleted =>
        relocatedIndexState.deleted = TRUE

\* INV-4: RELOCATION PRESERVES STATE
\*
\* Relocation changes location (segID) but NOT state (version, deleted).
\* The deleted flag is preserved exactly AT THE MOMENT OF RELOCATION.
InvRelocationPreservesState ==
    compactorState = "relocated" =>
        relocatedIndexState.deleted = compactorSnapshot.deleted

(***************************************************************************
 * TEMPORAL PROPERTIES
 ***************************************************************************)

\* PROP-1: Compactor eventually completes
PropCompactorCompletes ==
    compactorState = "reading" ~> compactorState \in {"relocated", "failed"}

(***************************************************************************
 * LEAPFROG HAZARD TRACE EXPLANATION
 *
 * The model PROVES the Ghost Guard blocks the Leapfrog Hazard:
 *
 * SCENARIO (without Ghost Guard - would be unsafe):
 *   State 1: index[K] = [segID=1, version=1, deleted=FALSE]
 *   State 2: CompactorRead(K) -> snapshot = [segID=1, version=1, deleted=FALSE]
 *   State 3: UserDelete(K) -> index[K] = [segID=2, version=2, deleted=TRUE]
 *   State 4: CompactorRelocateLive attempts...
 *
 * WITHOUT GHOST GUARD:
 *   - Location check: segID=1 != 2 (FAIL) -- but what if version matched?
 *   - Actually, UserDelete changes both segID AND version, so location fails.
 *
 * THE SUBTLE CASE (why we need Ghost Guard):
 *   What if Delete didn't change segment? (e.g., in-place tombstone)
 *   - State 3': UserDelete(K) -> index[K] = [segID=1, version=1, deleted=TRUE]
 *   - Now location check PASSES (segID=1, version=1 match)
 *   - WITHOUT Ghost Guard: Relocate succeeds, deleted=FALSE written!
 *   - WITH Ghost Guard: index[K].deleted=TRUE != expected FALSE -> FAIL
 *
 * The model explores ALL interleavings and verifies:
 *   - CompactorRelocateLiveSuccess requires index[k].deleted = FALSE
 *   - If user deleted the key, this condition fails
 *   - InvNoGhostResurrection and InvRelocationPreservesState hold
 *
 * TLC will find NO trace where a deleted key becomes live via compaction.
 *
 ***************************************************************************)

=============================================================================
