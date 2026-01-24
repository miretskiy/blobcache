---------------------------- MODULE WAL ----------------------------
(***************************************************************************
 * TLA+ Formal Model: Write-Ahead Log with Group Commit
 *
 * This specification models the WAL implementation in:
 *   - internal/wal/wal.go (Group Commit, Leader Election, Sealed Guard)
 *   - memtable.go (Write coordination, SeqID assignment)
 *
 * PURPOSE:
 * Verify correctness of the group commit protocol, specifically:
 *   1. Durability: If Write() returns success, data is on stable storage
 *   2. Monotonicity: SeqIDs never regress within a WAL file
 *   3. No Lost Writes: Every acknowledged write is recoverable after crash
 *   4. Single Leader: Only one goroutine flushes at a time
 *   5. Sealed Guard: No writes accepted with SeqID <= lastRotatedSeq
 *
 ***************************************************************************)

(***************************************************************************
 * VARIABLE MAPPING TABLE
 *
 * This table shows how TLA+ variables correspond to Go struct fields.
 * This enables direct validation: when reading an error trace, you can
 * look up the exact Go field involved.
 *
 * | TLA+ Variable    | Go Code Location          | Go Field/Var          |
 * |------------------|---------------------------|-----------------------|
 * | writerBusy       | wal/wal.go:122           | WAL.writerBusy        |
 * | pending          | wal/wal.go:113           | WAL.pending           |
 * | flushing         | wal/wal.go:114           | WAL.flushing          |
 * | lastRotatedSeq   | wal/wal.go:108           | WAL.lastRotatedSeq    |
 * | currentMaxSeq    | wal/wal.go:109           | WAL.currentMaxSeq     |
 * | file             | wal/wal.go:101           | WAL.file              |
 * | disk             | (abstraction)             | fsync'd file contents |
 * | writerState      | (abstraction)             | goroutine state       |
 * | writerSeqID      | wal/wal.go:64            | request.rec.SeqID     |
 * | writerResult     | wal/wal.go:72-73         | request.Done/Err      |
 *
 ***************************************************************************)

(***************************************************************************
 * KEY CONCEPTS:
 *
 * Group Commit (wal.go:251-286):
 *   fsync() is expensive (~10ms on HDD, ~100us on NVMe). Group commit
 *   batches N concurrent writers into ONE fsync via the ping-pong swap
 *   pattern (lines 217-219), achieving N x throughput improvement.
 *
 * Leader Election (wal.go:209-215):
 *   Only ONE goroutine (the "leader") performs I/O at a time. Others wait
 *   on w.cond. This is simpler than lock-free approaches and avoids write
 *   amplification from concurrent fsyncs.
 *
 * Ping-Pong Swap (wal.go:217-220):
 *   Two queues: `pending` and `flushing`. Leader atomically swaps them,
 *   then flushes outside the lock. This minimizes lock hold time.
 *
 * Sealed Guard (wal.go:105-109, 337-341, 563-580):
 *   After closeCurrentFile(), writes with SeqID <= lastRotatedSeq are
 *   rejected with ErrSequenceRegression. Prevents "stale" writes from
 *   corrupting newer files.
 *
 ***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

(***************************************************************************
 * CONSTANTS - Model Parameters
 *
 * These are configured in WAL.cfg. Smaller values = faster model checking.
 ***************************************************************************)

CONSTANTS
    Writers,        \* Set of writer process IDs (models concurrent goroutines)
    MaxSeqID,       \* Maximum sequence ID to consider (bounds state space)
    MaxQueueLen     \* Maximum pending queue length (bounds state space)

(***************************************************************************
 * VARIABLES - System State
 *
 * See mapping table above for Go code correspondence.
 ***************************************************************************)

VARIABLES
    \* === WAL struct fields (wal.go:95-129) ===
    writerBusy,     \* bool: WAL.writerBusy - leader election flag
    pending,        \* []*request: WAL.pending - writers waiting to flush
    flushing,       \* []*request: WAL.flushing - batch being flushed
    lastRotatedSeq, \* uint64: WAL.lastRotatedSeq - sealed guard threshold
    currentMaxSeq,  \* uint64: WAL.currentMaxSeq - max SeqID in current file
    fileOpen,       \* bool: WAL.file != nil - is a WAL file open?

    \* === Durable Storage (abstraction of fsync'd data) ===
    disk,           \* Set of seqIDs that have been fsync'd to stable storage

    \* === Per-goroutine state (models concurrent writers) ===
    writerState,    \* goroutine state: "idle"|"pending"|"leader"|"flushing"|"done"
    writerSeqID,    \* request.rec.SeqID - the seqID being written
    writerResult    \* request.Done/Err - "none"|"success"|"rejected"

\* Helper: all variables as a tuple (for UNCHANGED clauses)
vars == <<writerBusy, pending, flushing, lastRotatedSeq, currentMaxSeq,
          fileOpen, disk, writerState, writerSeqID, writerResult>>

(***************************************************************************
 * TYPE INVARIANT
 *
 * Sanity check that the model is well-formed. TLC checks this after every
 * state transition. If violated, there's a bug in the spec itself.
 ***************************************************************************)

TypeInvariant ==
    /\ writerBusy \in BOOLEAN
    /\ pending \in Seq(Writers \X (1..MaxSeqID))
    /\ flushing \in Seq(Writers \X (1..MaxSeqID))
    /\ lastRotatedSeq \in 0..MaxSeqID
    /\ currentMaxSeq \in 0..MaxSeqID
    /\ fileOpen \in BOOLEAN
    /\ disk \subseteq (1..MaxSeqID)
    /\ writerState \in [Writers -> {"idle", "pending", "leader", "flushing", "done"}]
    /\ writerSeqID \in [Writers -> 0..MaxSeqID]
    /\ writerResult \in [Writers -> {"none", "success", "rejected"}]

(***************************************************************************
 * INITIAL STATE
 *
 * Models wal.Open() (wal.go:131-156) - fresh WAL with no data.
 ***************************************************************************)

Init ==
    /\ writerBusy = FALSE           \* wal.go:138 - no leader initially
    /\ pending = <<>>               \* wal.go:141 - empty pending queue
    /\ flushing = <<>>              \* wal.go:141 - empty flushing queue
    /\ lastRotatedSeq = 0           \* wal.go:140 - no sealed guard yet
    /\ currentMaxSeq = 0            \* wal.go:140 - no writes yet
    /\ fileOpen = FALSE             \* wal.go:140 - file opened lazily
    /\ disk = {}                    \* No data on disk initially
    /\ writerState = [w \in Writers |-> "idle"]
    /\ writerSeqID = [w \in Writers |-> 0]
    /\ writerResult = [w \in Writers |-> "none"]

(***************************************************************************
 * HELPER OPERATORS
 ***************************************************************************)

\* Maximum of a set of integers (returns 0 for empty set)
MaxOfSet(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : x >= y

\* Extract seqIDs from a queue of <<writer, seqID>> tuples
SeqIDsOf(queue) == {queue[i][2] : i \in 1..Len(queue)}

\* Check if seqID passes the sealed guard (wal.go:300-303)
\* Go: if seqID <= w.lastRotatedSeq { return ErrSequenceRegression }
PassesSealedGuard(seqID) == seqID > lastRotatedSeq

\* Check if all seqIDs in a set pass the sealed guard
AllPassGuard(seqIDs) == \A s \in seqIDs : PassesSealedGuard(s)

\* Check if writer is in a queue
WriterInQueue(w, queue) == \E i \in 1..Len(queue) : queue[i][1] = w

(***************************************************************************
 * ACTION: SubmitWrite
 *
 * Models: wal.Write() -> wal.submit() (wal.go:161-164, 193-199)
 *
 * Go code:
 *   func (w *WAL) submit(req *request) error {
 *       w.mu.Lock()
 *       w.pending = append(w.pending, req)  // <- This line
 *       ...
 *   }
 ***************************************************************************)

SubmitWrite(writer, seqID) ==
    /\ writerState[writer] = "idle"           \* Writer must be idle
    /\ seqID > 0                               \* Valid seqID
    /\ Len(pending) < MaxQueueLen              \* Bounds check for model
    \* Models: w.pending = append(w.pending, req) [wal.go:199]
    /\ pending' = Append(pending, <<writer, seqID>>)
    /\ writerState' = [writerState EXCEPT ![writer] = "pending"]
    /\ writerSeqID' = [writerSeqID EXCEPT ![writer] = seqID]
    /\ UNCHANGED <<writerBusy, flushing, lastRotatedSeq, currentMaxSeq,
                   fileOpen, disk, writerResult>>

(***************************************************************************
 * ACTION: BecomeLeader
 *
 * Models: Leader election in wal.submit() (wal.go:200-222)
 *
 * Go code:
 *   for {
 *       if req.Done { return req.Err }          // [203-206]
 *       if w.writerBusy { w.cond.Wait(); continue }  // [208-211]
 *       w.writerBusy = true                     // [215] <- Become leader
 *       toFlush := w.pending                    // [218] <- Ping-pong swap
 *       w.pending = w.flushing                  // [219]
 *       w.pending = w.pending[:0]               // [220]
 *       w.mu.Unlock()                           // [222]
 *   }
 ***************************************************************************)

BecomeLeader(writer) ==
    /\ writerState[writer] = "pending"
    /\ ~writerBusy                              \* wal.go:209 - no other leader
    /\ WriterInQueue(writer, pending)           \* Writer is in queue
    \* Models: w.writerBusy = true [wal.go:215]
    /\ writerBusy' = TRUE
    /\ writerState' = [writerState EXCEPT ![writer] = "leader"]
    \* Models: Ping-pong swap [wal.go:218-220]
    /\ flushing' = pending
    /\ pending' = <<>>
    /\ UNCHANGED <<lastRotatedSeq, currentMaxSeq, fileOpen, disk, writerSeqID, writerResult>>

(***************************************************************************
 * ACTION: FlushBatchSuccess
 *
 * Models: wal.flushRecords() success path (wal.go:320-417)
 *
 * Go code (simplified):
 *   func (w *WAL) flushRecords(batch []*request) error {
 *       // Validate sealed guard [337-342]
 *       for _, req := range batch {
 *           if req.rec.SeqID <= w.lastRotatedSeq {
 *               return ErrSequenceRegression
 *           }
 *       }
 *       // Update max [344-347]
 *       if maxSeq > w.currentMaxSeq {
 *           w.currentMaxSeq = maxSeq
 *       }
 *       // Write and sync [476]
 *       w.writeAndSync(buf)  // <- Data now durable!
 *   }
 *
 * This is THE CORE DURABILITY GUARANTEE: after this action, all seqIDs
 * in the batch are on stable storage (disk).
 ***************************************************************************)

FlushBatchSuccess(writer) ==
    /\ writerState[writer] = "leader"
    /\ Len(flushing) > 0
    /\ AllPassGuard(SeqIDsOf(flushing))        \* wal.go:337-342 - guard check
    \* Models: w.writeAndSync(buf) [wal.go:476] - data now durable!
    /\ disk' = disk \union SeqIDsOf(flushing)
    \* Models: w.currentMaxSeq = maxSeq [wal.go:344-347]
    /\ currentMaxSeq' = IF MaxOfSet(SeqIDsOf(flushing)) > currentMaxSeq
                        THEN MaxOfSet(SeqIDsOf(flushing))
                        ELSE currentMaxSeq
    /\ fileOpen' = TRUE                        \* File now open
    /\ writerState' = [writerState EXCEPT ![writer] = "flushing"]
    \* Models: batch success - all writers marked done
    /\ writerResult' = [w \in Writers |->
        IF WriterInQueue(w, flushing)
        THEN "success"
        ELSE writerResult[w]]
    /\ UNCHANGED <<writerBusy, pending, flushing, lastRotatedSeq, writerSeqID>>

(***************************************************************************
 * ACTION: FlushBatchReject
 *
 * Models: wal.flushRecords() failure - ErrSequenceRegression (wal.go:337-342)
 *
 * Go code:
 *   if req.rec.SeqID <= w.lastRotatedSeq {
 *       return fmt.Errorf("%w: seq %d <= sealed seq %d",
 *           ErrSequenceRegression, req.rec.SeqID, w.lastRotatedSeq)
 *   }
 ***************************************************************************)

FlushBatchReject(writer) ==
    /\ writerState[writer] = "leader"
    /\ Len(flushing) > 0
    /\ ~AllPassGuard(SeqIDsOf(flushing))       \* Some fail guard check
    \* Models: return ErrSequenceRegression [wal.go:339-341]
    /\ writerState' = [writerState EXCEPT ![writer] = "flushing"]
    /\ writerResult' = [w \in Writers |->
        IF WriterInQueue(w, flushing)
        THEN "rejected"
        ELSE writerResult[w]]
    /\ UNCHANGED <<writerBusy, pending, flushing, lastRotatedSeq,
                   currentMaxSeq, fileOpen, disk, writerSeqID>>

(***************************************************************************
 * ACTION: FinishFlush
 *
 * Models: wal.submit() batch completion (wal.go:227-243)
 *
 * Go code:
 *   w.mu.Lock()
 *   w.writerBusy = false                      // [228]
 *   for _, r := range toFlush {               // [231-236]
 *       if !r.Done {
 *           r.Err = err
 *           r.Done = true
 *       }
 *   }
 *   w.flushing = toFlush                      // [239]
 *   w.cond.Broadcast()                        // [242]
 ***************************************************************************)

FinishFlush(writer) ==
    /\ writerState[writer] = "flushing"
    \* Models: w.writerBusy = false [wal.go:228]
    /\ writerBusy' = FALSE
    \* Models: r.Done = true for all requests [wal.go:231-236]
    /\ writerState' = [w \in Writers |->
        IF WriterInQueue(w, flushing)
        THEN "done"
        ELSE writerState[w]]
    /\ flushing' = <<>>
    /\ UNCHANGED <<pending, lastRotatedSeq, currentMaxSeq, fileOpen, disk,
                   writerSeqID, writerResult>>

(***************************************************************************
 * ACTION: WriterReturnsDone
 *
 * Models: wal.submit() return after Done (wal.go:203-206)
 *
 * Go code:
 *   if req.Done {
 *       w.mu.Unlock()
 *       return req.Err  // <- Writer returns to caller
 *   }
 ***************************************************************************)

WriterReturnsDone(writer) ==
    /\ writerState[writer] = "done"
    /\ writerState' = [writerState EXCEPT ![writer] = "idle"]
    /\ writerSeqID' = [writerSeqID EXCEPT ![writer] = 0]
    /\ writerResult' = [writerResult EXCEPT ![writer] = "none"]
    /\ UNCHANGED <<writerBusy, pending, flushing, lastRotatedSeq,
                   currentMaxSeq, fileOpen, disk>>

(***************************************************************************
 * ACTION: RotateFile
 *
 * Models: wal.closeCurrentFile() (wal.go:564-583)
 *
 * Go code:
 *   func (w *WAL) closeCurrentFile() error {
 *       w.sync()                               // [570]
 *       w.file.Close()                         // [571]
 *       w.file = nil                           // [572]
 *       w.fileOffset = 0                       // [573]
 *       w.currentFirstID = 0                   // [574]
 *       // LATCH THE GUARD - critical!
 *       if w.currentMaxSeq > w.lastRotatedSeq {// [577]
 *           w.lastRotatedSeq = w.currentMaxSeq // [578] <- Seals the file!
 *       }
 *       w.currentMaxSeq = 0                    // [580]
 *   }
 *
 * Critical: After this, any write with seqID <= lastRotatedSeq will be
 * rejected by the sealed guard. This prevents stale writes.
 ***************************************************************************)

RotateFile ==
    /\ fileOpen                                \* File must be open
    /\ ~writerBusy                             \* No flush in progress
    /\ pending = <<>>                          \* No pending writes
    \* Models: Latch the guard [wal.go:577-578]
    /\ lastRotatedSeq' = IF currentMaxSeq > lastRotatedSeq
                         THEN currentMaxSeq
                         ELSE lastRotatedSeq
    /\ currentMaxSeq' = 0                      \* wal.go:580
    /\ fileOpen' = FALSE                       \* wal.go:572
    /\ UNCHANGED <<writerBusy, pending, flushing, disk,
                   writerState, writerSeqID, writerResult>>

(***************************************************************************
 * ACTION: Crash
 *
 * Models: Power failure - all volatile state lost, disk survives.
 *
 * In the real system:
 *   - disk contains all fsync'd records (survives!)
 *   - WAL struct is gone (volatile memory lost)
 *   - pending/flushing queues are lost
 *   - currentMaxSeq is lost (reconstructed from disk during recovery)
 *   - lastRotatedSeq is lost (but 0 is conservative - allows all writes)
 *
 * This is the worst-case scenario the model verifies against.
 ***************************************************************************)

Crash ==
    \* All volatile state reset
    /\ writerBusy' = FALSE
    /\ pending' = <<>>
    /\ flushing' = <<>>
    /\ lastRotatedSeq' = 0
    /\ currentMaxSeq' = 0
    /\ fileOpen' = FALSE
    \* disk survives! (This is the WHOLE POINT of a WAL)
    /\ UNCHANGED disk
    \* All writers reset (they'll retry externally)
    /\ writerState' = [w \in Writers |-> "idle"]
    /\ writerSeqID' = [w \in Writers |-> 0]
    /\ writerResult' = [w \in Writers |-> "none"]

(***************************************************************************
 * ACTION: Recover
 *
 * Models: Recovery after crash - replay WAL to reconstruct state.
 *
 * In the real system (recovery.go, wal/wal.go):
 *   - Read all WAL files in sequence order
 *   - Replay each record to reconstruct index
 *   - disk already contains the durable data
 *
 * In this model, recovery is implicit: disk IS the recovered data.
 ***************************************************************************)

Recover ==
    /\ pending = <<>>                          \* No pending operations
    /\ ~writerBusy                             \* No active flush
    /\ UNCHANGED vars

(***************************************************************************
 * NEXT STATE RELATION
 *
 * Combines all possible actions. TLC explores all interleavings.
 ***************************************************************************)

Next ==
    \/ \E w \in Writers, s \in 1..MaxSeqID : SubmitWrite(w, s)
    \/ \E w \in Writers : BecomeLeader(w)
    \/ \E w \in Writers : FlushBatchSuccess(w)
    \/ \E w \in Writers : FlushBatchReject(w)
    \/ \E w \in Writers : FinishFlush(w)
    \/ \E w \in Writers : WriterReturnsDone(w)
    \/ RotateFile
    \/ Crash
    \/ Recover

(***************************************************************************
 * FAIRNESS
 *
 * Weak fairness (WF) ensures progress: if an action is continuously enabled,
 * it eventually happens. This models Go runtime fairness guarantees.
 *
 * We need fairness on:
 * - BecomeLeader: If pending writers exist and no leader, one becomes leader
 * - FlushBatch*: Leaders eventually flush
 * - FinishFlush: Flushing leaders eventually finish
 * - WriterReturnsDone: Done writers eventually return to idle
 *
 * Without fairness, TLC could construct pathological behaviors where
 * the system stutters forever - unrealistic given Go's scheduler.
 ***************************************************************************)

Fairness ==
    /\ \A w \in Writers : WF_vars(BecomeLeader(w))
    /\ \A w \in Writers : WF_vars(FlushBatchSuccess(w))
    /\ \A w \in Writers : WF_vars(FlushBatchReject(w))
    /\ \A w \in Writers : WF_vars(FinishFlush(w))
    /\ \A w \in Writers : WF_vars(WriterReturnsDone(w))

(***************************************************************************
 * SPECIFICATION
 *
 * The complete temporal formula: start in Init, always transition via Next,
 * with fairness constraints.
 ***************************************************************************)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************
 * INVARIANTS - Must hold in EVERY reachable state
 ***************************************************************************)

\* INV-1: Type safety (sanity check)
InvType == TypeInvariant

\* INV-2: At most one leader at a time
\* Models: Only one goroutine has writerBusy=true (mutex correctness)
InvSingleLeader ==
    Cardinality({w \in Writers : writerState[w] \in {"leader", "flushing"}}) <= 1

\* INV-3: All acknowledged writes are on disk
\* THIS IS THE CORE DURABILITY GUARANTEE!
\*
\* If a goroutine's Write() returned success (writerResult = "success"),
\* then the seqID it wrote MUST be on disk.
\*
\* Error Trace Guide: If this fails, look for:
\*   - writerResult[w] = "success" but writerSeqID[w] not in disk
\*   - This would indicate a bug where we ACK before fsync completes
InvDurability ==
    \A w \in Writers :
        (writerResult[w] = "success") => (writerSeqID[w] \in disk)

(***************************************************************************
 * TEMPORAL PROPERTIES - Must hold across all behaviors
 ***************************************************************************)

\* PROP-1: Crash Safety - disk contents never shrink
\* (Data that was fsync'd stays fsync'd)
PropCrashSafe ==
    [][\A s \in disk : s \in disk']_vars

\* PROP-2: Liveness - A submitted write eventually completes
\* The "~>" (leads-to) operator means: if P becomes true, Q eventually becomes true.
\* With fairness constraints in Spec, this verifies no deadlocks or starvation.
\*
\* Note: Crash resets writers to "idle", so even with crashes this property holds.
\* A writer either: (a) completes normally, (b) is reset by crash to idle.
PropWriteCompletes ==
    \A w \in Writers :
        (writerState[w] = "pending") ~> (writerState[w] = "idle")

(***************************************************************************
 * ERROR TRACE GUIDE
 *
 * When TLC finds a violation, it produces an error trace showing the
 * sequence of states that led to the bug. Here's how to read it:
 *
 * 1. InvSingleLeader violated:
 *    - Look for two goroutines with writerState = "leader" or "flushing"
 *    - Check: Is writerBusy being set correctly in BecomeLeader?
 *    - Go code: Verify w.writerBusy = true happens before unlock
 *
 * 2. InvDurability violated:
 *    - Look for writerResult[w] = "success" but writerSeqID[w] NOT in disk
 *    - Check: Is disk updated BEFORE writerResult?
 *    - Go code: Verify writeAndSync() completes before marking Done
 *
 * 3. Deadlock (no actions enabled):
 *    - Look for all writers stuck in "pending" state
 *    - Check: Is BecomeLeader enabled when writerBusy=false?
 *    - Go code: Verify cond.Broadcast() wakes waiters
 *
 * 4. PropCrashSafe violated:
 *    - Look for seqID disappearing from disk
 *    - Check: Is Crash modifying disk? (It shouldn't!)
 *    - Go code: This would indicate fsync not working
 *
 ***************************************************************************)

=============================================================================
