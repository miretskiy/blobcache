------------------------------ MODULE Rotation ------------------------------
(***************************************************************************
 * TLA+ Formal Model: MemTable Rotation Barrier Synchronization
 *
 * This specification models the concurrent write/rotation protocol in
 * memtable.go, verifying that the barrier synchronization correctly
 * prevents "slow writers" from corrupting data during slab rotation.
 *
 * THE PROBLEM:
 * Writers must reserve space in the active slab. When a slab fills up,
 * the Rotator must seal the old slab, wait for pending writes to drain,
 * and install a new slab. A "slow writer" who passed the checks but
 * got preempted before incrementing pending could corrupt the handoff.
 *
 * THE SOLUTION (Three Guards):
 *
 * 1. TIME TRAVEL GUARD (maxSealedSeq):
 *    Writers with seqID <= maxSealedSeq are rejected. This prevents
 *    stale writes from landing in a NEW slab after rotation.
 *
 * 2. BARRIER (activeReady channel):
 *    When rotation starts, the barrier is closed. New writers wait
 *    until rotation completes and the barrier reopens.
 *
 * 3. DRAIN (pendingWrites counter):
 *    The Rotator waits for pendingWrites to hit 0 before completing
 *    rotation. This ensures all in-flight writes finish.
 *
 * KEY INSIGHT: The lock protects the entire check-and-increment sequence.
 * A writer who passes the checks and increments pending is GUARANTEED to
 * complete, and the Rotator WILL wait for them via the drain mechanism.
 *
 ***************************************************************************)

(***************************************************************************
 * VARIABLE MAPPING TABLE
 *
 * | TLA+ Variable      | Go Code Location           | Go Field/Type         |
 * |--------------------|----------------------------|-----------------------|
 * | slabs[s].pending   | slab.go:40                 | ActiveSlab.pendingWrites |
 * | slabs[s].sealed    | memtable.go:340            | ActiveSlab.retired    |
 * | slabs[s].flushed   | (logical state)            | Post-processFlush     |
 * | activeSlabID       | memtable.go:46             | mt.mu.active          |
 * | barrierClosed      | memtable.go:47             | mt.mu.activeReady != nil |
 * | maxSealedSeq       | memtable.go:52             | mt.mu.maxSealedSeq    |
 * | writers[w].state   | (control flow position)    | Writer goroutine PC   |
 * | writers[w].seqID   | memtable.go:161            | seqID parameter       |
 * | writers[w].targetSlab | (local variable)        | active := mt.mu.active |
 * | rotationInProgress | (logical state)            | Between prepare/finish |
 * | nextSlabID         | (allocation counter)       | Next slab to create   |
 *
 ***************************************************************************)

EXTENDS Integers, FiniteSets, TLC

(***************************************************************************
 * CONSTANTS
 ***************************************************************************)

CONSTANTS
    Writers,        \* Set of writer process IDs
    MaxSeqID,       \* Maximum sequence ID (bounds state space)
    MaxSlabs        \* Maximum slab IDs to allocate

(***************************************************************************
 * VARIABLES
 ***************************************************************************)

VARIABLES
    \* === Slab State ===
    \* slabs[s] = [pending, sealed, flushed, maxSeq]
    \* - pending: count of in-flight writes
    \* - sealed: TRUE if rotation has marked this slab as retired
    \* - flushed: TRUE if slab has been fully processed and handed off
    \* - maxSeq: highest seqID written to this slab
    slabs,

    \* === Active Slab Pointer ===
    activeSlabID,   \* ID of the current active slab (0 if none during rotation)

    \* === Barrier State ===
    barrierClosed,  \* TRUE if activeReady channel exists (writers must wait)

    \* === Time Travel Guard ===
    maxSealedSeq,   \* Highest seqID in any sealed slab

    \* === Writer State ===
    \* writers[w] = [state, seqID, targetSlab]
    \* - state: "idle", "checking", "reserved", "writing", "done", "rejected"
    \* - seqID: the sequence ID this writer is using
    \* - targetSlab: which slab this writer reserved space in
    writers,

    \* === Rotation State ===
    rotationInProgress,  \* TRUE if rotation is between prepare and finish
    pendingRotationSlab, \* Slab ID being rotated (waiting for drain)

    \* === Allocation Counter ===
    nextSlabID,     \* Next slab ID to allocate
    nextSeqID       \* Next seqID to assign to a writer

vars == <<slabs, activeSlabID, barrierClosed, maxSealedSeq, writers,
          rotationInProgress, pendingRotationSlab, nextSlabID, nextSeqID>>

(***************************************************************************
 * TYPE INVARIANT
 ***************************************************************************)

TypeInvariant ==
    /\ activeSlabID \in 0..MaxSlabs
    /\ barrierClosed \in BOOLEAN
    /\ maxSealedSeq \in 0..MaxSeqID
    /\ rotationInProgress \in BOOLEAN
    /\ pendingRotationSlab \in 0..MaxSlabs
    /\ nextSlabID \in 1..(MaxSlabs+1)
    /\ nextSeqID \in 1..(MaxSeqID+1)
    /\ \A w \in Writers :
        /\ writers[w].state \in {"idle", "checking", "reserved", "writing", "done", "rejected", "waiting"}
        /\ writers[w].seqID \in 0..MaxSeqID
        /\ writers[w].targetSlab \in 0..MaxSlabs
    /\ \A s \in DOMAIN slabs :
        /\ slabs[s].pending \in 0..Cardinality(Writers)
        /\ slabs[s].sealed \in BOOLEAN
        /\ slabs[s].flushed \in BOOLEAN
        /\ slabs[s].maxSeq \in 0..MaxSeqID

(***************************************************************************
 * INITIAL STATE
 ***************************************************************************)

Init ==
    /\ slabs = (1 :> [pending |-> 0, sealed |-> FALSE, flushed |-> FALSE, maxSeq |-> 0])
    /\ activeSlabID = 1
    /\ barrierClosed = FALSE
    /\ maxSealedSeq = 0
    /\ writers = [w \in Writers |-> [state |-> "idle", seqID |-> 0, targetSlab |-> 0]]
    /\ rotationInProgress = FALSE
    /\ pendingRotationSlab = 0
    /\ nextSlabID = 2
    /\ nextSeqID = 1

(***************************************************************************
 * HELPER OPERATORS
 ***************************************************************************)

SlabExists(s) == s \in DOMAIN slabs

(***************************************************************************
 * ACTION: WriterArrive
 *
 * Models: A writer starting a Put operation with a fresh seqID.
 * Go code: mt.Put() entry, seqID assigned by caller
 *
 * The writer transitions to "checking" state with their seqID.
 ***************************************************************************)

WriterArrive(w) ==
    /\ writers[w].state = "idle"
    /\ nextSeqID <= MaxSeqID
    /\ writers' = [writers EXCEPT ![w].state = "checking", ![w].seqID = nextSeqID]
    /\ nextSeqID' = nextSeqID + 1
    /\ UNCHANGED <<slabs, activeSlabID, barrierClosed, maxSealedSeq,
                   rotationInProgress, pendingRotationSlab, nextSlabID>>

(***************************************************************************
 * ACTION: WriterCheckSeqID
 *
 * Models: memtable.go:214-217 - The Time Travel Guard check
 *
 * Go code:
 *   if seqID <= mt.mu.maxSealedSeq {
 *       mt.mu.Unlock()
 *       return errSequenceTooOld
 *   }
 *
 * If seqID is too old, writer is rejected (must retry with new seqID).
 * This prevents stale writes from appearing after rotation.
 ***************************************************************************)

WriterCheckSeqID_Pass(w) ==
    /\ writers[w].state = "checking"
    /\ writers[w].seqID > maxSealedSeq  \* PASS: seqID is fresh enough
    /\ ~barrierClosed                    \* Barrier must be open
    /\ activeSlabID # 0                  \* Active slab exists
    \* Atomically: check passes, reserve space, increment pending
    \* This models the lock protecting lines 209-268
    /\ writers' = [writers EXCEPT
        ![w].state = "reserved",
        ![w].targetSlab = activeSlabID]
    /\ slabs' = [slabs EXCEPT
        ![activeSlabID].pending = @ + 1,
        ![activeSlabID].maxSeq = IF writers[w].seqID > @ THEN writers[w].seqID ELSE @]
    /\ UNCHANGED <<activeSlabID, barrierClosed, maxSealedSeq,
                   rotationInProgress, pendingRotationSlab, nextSlabID, nextSeqID>>

WriterCheckSeqID_Reject(w) ==
    /\ writers[w].state = "checking"
    /\ writers[w].seqID <= maxSealedSeq  \* FAIL: seqID too old
    /\ writers' = [writers EXCEPT ![w].state = "rejected"]
    /\ UNCHANGED <<slabs, activeSlabID, barrierClosed, maxSealedSeq,
                   rotationInProgress, pendingRotationSlab, nextSlabID, nextSeqID>>

WriterCheckBarrier_Wait(w) ==
    /\ writers[w].state = "checking"
    /\ writers[w].seqID > maxSealedSeq  \* seqID check passes
    /\ barrierClosed                     \* But barrier is closed!
    /\ writers' = [writers EXCEPT ![w].state = "waiting"]
    /\ UNCHANGED <<slabs, activeSlabID, barrierClosed, maxSealedSeq,
                   rotationInProgress, pendingRotationSlab, nextSlabID, nextSeqID>>

(***************************************************************************
 * ACTION: WriterResume
 *
 * Models: memtable.go:223-224 - Writer wakes up after barrier opens
 *
 * Go code:
 *   <-wait
 *   return mt.writeToSlab(seqID, hash, rec) // Retry
 *
 * When barrier opens, waiting writers retry from the beginning.
 ***************************************************************************)

WriterResume(w) ==
    /\ writers[w].state = "waiting"
    /\ ~barrierClosed  \* Barrier is now open
    /\ writers' = [writers EXCEPT ![w].state = "checking"]
    /\ UNCHANGED <<slabs, activeSlabID, barrierClosed, maxSealedSeq,
                   rotationInProgress, pendingRotationSlab, nextSlabID, nextSeqID>>

(***************************************************************************
 * ACTION: WriterComplete
 *
 * Models: memtable.go:316-320 - Writer finishes and decrements pending
 *
 * Go code:
 *   if active.pendingWrites.Add(-1) == 0 {
 *       if active.retired.Load() {
 *           active.writesDone.Close()
 *       }
 *   }
 *
 * Writer has finished writing data and decrements pending count.
 ***************************************************************************)

WriterComplete(w) ==
    LET targetSlab == writers[w].targetSlab
    IN /\ writers[w].state = "reserved"
       /\ SlabExists(targetSlab)
       /\ slabs[targetSlab].pending > 0
       /\ slabs' = [slabs EXCEPT ![targetSlab].pending = @ - 1]
       /\ writers' = [writers EXCEPT ![w].state = "done"]
       /\ UNCHANGED <<activeSlabID, barrierClosed, maxSealedSeq,
                      rotationInProgress, pendingRotationSlab, nextSlabID, nextSeqID>>

(***************************************************************************
 * ACTION: WriterReset
 *
 * Models: Writer returning to idle (ready for next operation)
 ***************************************************************************)

WriterReset(w) ==
    /\ writers[w].state \in {"done", "rejected"}
    /\ writers' = [writers EXCEPT ![w].state = "idle", ![w].seqID = 0, ![w].targetSlab = 0]
    /\ UNCHANGED <<slabs, activeSlabID, barrierClosed, maxSealedSeq,
                   rotationInProgress, pendingRotationSlab, nextSlabID, nextSeqID>>

(***************************************************************************
 * ACTION: TriggerRotation
 *
 * Models: memtable.go:327-347 - prepareRotationLocked()
 *
 * Go code:
 *   mt.mu.activeReady = make(chan struct{})  // Close barrier
 *   if old.currentMaxSeq > mt.mu.maxSealedSeq {
 *       mt.mu.maxSealedSeq = old.currentMaxSeq
 *   }
 *   old.retired.Store(true)
 *   mt.mu.active = nil
 *
 * This starts the rotation: close barrier, update gatekeeper, mark retired.
 ***************************************************************************)

TriggerRotation ==
    LET oldSlab == activeSlabID
        oldMaxSeq == slabs[activeSlabID].maxSeq
    IN /\ ~rotationInProgress
       /\ activeSlabID # 0
       /\ ~barrierClosed
       /\ nextSlabID <= MaxSlabs  \* Can allocate another slab
       /\ barrierClosed' = TRUE
       /\ maxSealedSeq' = IF oldMaxSeq > maxSealedSeq THEN oldMaxSeq ELSE maxSealedSeq
       /\ slabs' = [slabs EXCEPT ![oldSlab].sealed = TRUE]
       /\ activeSlabID' = 0  \* Detach active
       /\ rotationInProgress' = TRUE
       /\ pendingRotationSlab' = oldSlab
       /\ UNCHANGED <<writers, nextSlabID, nextSeqID>>

(***************************************************************************
 * ACTION: FinishRotation
 *
 * Models: memtable.go:350-387 - The unlocked closure in prepareRotationLocked
 *
 * Go code:
 *   if hasPending { <-waitCh }           // Wait for drain
 *   ... (WAL rotate, send to flusher) ...
 *   newSlab := mt.newActiveSlab(0)       // Allocate new
 *   mt.mu.active = newSlab               // Install
 *   close(mt.mu.activeReady)             // Open barrier
 *   mt.mu.activeReady = nil
 *
 * CRITICAL: Can only complete when pendingWrites == 0 on the old slab.
 ***************************************************************************)

FinishRotation ==
    /\ rotationInProgress
    /\ pendingRotationSlab # 0
    /\ slabs[pendingRotationSlab].pending = 0  \* DRAIN COMPLETE!
    /\ nextSlabID <= MaxSlabs
    \* Mark old slab as flushed
    /\ slabs' = (slabs @@ (nextSlabID :> [pending |-> 0, sealed |-> FALSE, flushed |-> FALSE, maxSeq |-> 0]))
                 @@ (pendingRotationSlab :> [slabs[pendingRotationSlab] EXCEPT !.flushed = TRUE])
    /\ activeSlabID' = nextSlabID
    /\ nextSlabID' = nextSlabID + 1
    /\ barrierClosed' = FALSE  \* Open barrier
    /\ rotationInProgress' = FALSE
    /\ pendingRotationSlab' = 0
    /\ UNCHANGED <<maxSealedSeq, writers, nextSeqID>>

(***************************************************************************
 * TERMINATION (Model Boundary)
 *
 * The model terminates when resources are exhausted (seqIDs or slabs).
 * This is not a real deadlock - in production, these are unbounded.
 ***************************************************************************)

Terminated ==
    \* All writers idle, no rotation in progress, resources exhausted
    /\ \A w \in Writers : writers[w].state = "idle"
    /\ ~rotationInProgress
    /\ (nextSeqID > MaxSeqID \/ nextSlabID > MaxSlabs)

(***************************************************************************
 * NEXT STATE RELATION
 ***************************************************************************)

Next ==
    \/ \E w \in Writers : WriterArrive(w)
    \/ \E w \in Writers : WriterCheckSeqID_Pass(w)
    \/ \E w \in Writers : WriterCheckSeqID_Reject(w)
    \/ \E w \in Writers : WriterCheckBarrier_Wait(w)
    \/ \E w \in Writers : WriterResume(w)
    \/ \E w \in Writers : WriterComplete(w)
    \/ \E w \in Writers : WriterReset(w)
    \/ TriggerRotation
    \/ FinishRotation
    \/ (Terminated /\ UNCHANGED vars)  \* Allow stuttering at termination

(***************************************************************************
 * FAIRNESS
 ***************************************************************************)

Fairness ==
    /\ \A w \in Writers : WF_vars(WriterComplete(w))
    /\ \A w \in Writers : WF_vars(WriterResume(w))
    /\ WF_vars(FinishRotation)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************
 * INVARIANTS
 ***************************************************************************)

InvType == TypeInvariant

\* INV-1: NO WRITE AFTER FLUSH
\* A writer must NEVER hold a reference to a flushed slab.
\* If this fails, a "slow writer" corrupted the handoff.
InvNoWriteAfterFlush ==
    \A w \in Writers :
        writers[w].state = "reserved" =>
            LET target == writers[w].targetSlab
            IN SlabExists(target) /\ ~slabs[target].flushed

\* INV-2: TIME TRAVEL GUARD CONSISTENCY
\* The Time Travel Guard (maxSealedSeq) is enforced by action preconditions:
\* WriterCheckSeqID_Pass requires seqID > maxSealedSeq.
\*
\* This invariant verifies a weaker property: if a writer is "reserved",
\* their seqID must be recorded in their target slab's maxSeq.
\* This ensures we don't lose track of which seqIDs are in which slab.
InvSeqIDTracked ==
    \A w \in Writers :
        writers[w].state = "reserved" =>
            LET target == writers[w].targetSlab
            IN SlabExists(target) /\ slabs[target].maxSeq >= writers[w].seqID

\* INV-3: PENDING COUNT SANITY
\* The pending count must equal the actual number of reserved writers.
InvPendingCountSanity ==
    \A s \in DOMAIN slabs :
        slabs[s].pending = Cardinality({w \in Writers :
            writers[w].state = "reserved" /\ writers[w].targetSlab = s})

\* INV-4: BARRIER CONSISTENCY
\* If barrier is closed, either rotation is in progress OR active is detached
InvBarrierConsistency ==
    barrierClosed => (rotationInProgress \/ activeSlabID = 0)

\* INV-5: SEALED IMPLIES NO NEW WRITES
\* If a slab is sealed, no writer can newly enter it.
\* (Existing writers can complete, but no new reservations)
InvSealedNoNewEntry ==
    \A s \in DOMAIN slabs :
        slabs[s].sealed =>
            ~\E w \in Writers :
                /\ writers[w].state = "checking"
                /\ activeSlabID = s

(***************************************************************************
 * TEMPORAL PROPERTIES
 ***************************************************************************)

\* PROP-1: PROGRESS - If rotation starts, it eventually completes
\* This proves no deadlock waiting for writers to drain.
PropRotationCompletes ==
    rotationInProgress ~> ~rotationInProgress

\* PROP-2: WRITES COMPLETE - Reserved writers eventually complete
PropWritesComplete ==
    \A w \in Writers :
        writers[w].state = "reserved" ~> writers[w].state = "done"

\* PROP-3: WAITING WRITERS RESUME - Writers blocked on barrier eventually proceed
PropWaitingResumes ==
    \A w \in Writers :
        writers[w].state = "waiting" ~> writers[w].state # "waiting"

(***************************************************************************
 * SLOW WRITER ANALYSIS
 *
 * The model proves that a "Slow Writer" cannot corrupt the handoff.
 * Here's the scenario and why it's safe:
 *
 * SCENARIO:
 * 1. Writer W1 (seqID=5) acquires lock, passes checks
 * 2. W1 calls Alloc, gets buffer, increments pending
 * 3. W1 releases lock
 * 4. W1 gets preempted (OS scheduler)
 * 5. Writer W2 triggers rotation (slab full)
 * 6. Rotation: barrier closed, maxSealedSeq=5, retired=true
 * 7. Rotation waits on writesDone...
 * 8. W1 wakes up, writes data, decrements pending
 * 9. pending hits 0, writesDone.Close() called
 * 10. Rotation completes, installs new slab
 *
 * WHY IT'S SAFE:
 * - W1 incremented pending BEFORE releasing the lock (step 2)
 * - Rotation MUST wait for pending to hit 0 (step 7)
 * - W1 WILL complete and decrement pending (step 8)
 * - Only THEN can rotation complete (step 10)
 *
 * The lock ensures the check-and-increment is atomic. A writer who
 * passes the checks is GUARANTEED to have their pending counted,
 * and the Rotator is GUARANTEED to wait for them.
 *
 * WHAT ABOUT A WRITER WHO PASSED SEQID CHECK BUT NOT BARRIER?
 * Impossible! In the Go code, both checks happen under the same lock.
 * If seqID check passes and barrier check fails, the writer waits
 * on the barrier channel (without incrementing pending). When the
 * barrier opens, they RETRY from the beginning, getting a fresh
 * view of maxSealedSeq and the new active slab.
 *
 ***************************************************************************)

=============================================================================
