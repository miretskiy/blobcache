------------------------------ MODULE Slab ------------------------------
(***************************************************************************
 * TLA+ Formal Model: Reference Counting with Fresh Struct Strategy
 *
 * This specification models the MmapBuffer reference counting implementation
 * in mempool.go and slab.go, specifically verifying that the "Fresh Struct"
 * strategy prevents the ABA problem.
 *
 * THE ABA PROBLEM:
 * In lock-free reference counting, the ABA problem occurs when:
 *   1. Reader R sees struct S with refCount > 0
 *   2. Owner decrements refCount to 0, returns memory M to pool
 *   3. New writer acquires memory M, gets struct S' (potentially same address)
 *   4. Reader R does CAS on refCount, succeeds (thinks it's the same object)
 *   5. Reader R now has a "valid" reference to someone else's buffer!
 *
 * THE FRESH STRUCT SOLUTION:
 * Instead of pooling MmapBuffer structs, we:
 *   - Pool only the raw []byte memory
 *   - Allocate a FRESH MmapBuffer struct on each Acquire
 *   - Old struct pointers remain valid but with refCount=0
 *   - TryInc on old struct correctly fails (refCount <= 0)
 *
 * This model PROVES that TryInc correctly rejects stale references,
 * even when the underlying memory has been recycled to a new owner.
 *
 ***************************************************************************)

(***************************************************************************
 * VARIABLE MAPPING TABLE
 *
 * | TLA+ Variable      | Go Code Location         | Go Field/Concept       |
 * |--------------------|--------------------------|------------------------|
 * | memoryPool         | mempool.go:164           | MmapPool.buffers chan  |
 * | structs            | (heap allocation)        | *MmapBuffer instances  |
 * | structs[s].memID   | mempool.go:19            | MmapBuffer.raw []byte  |
 * | structs[s].ref     | mempool.go:20            | MmapBuffer.refCount    |
 * | structs[s].leased  | mempool.go:28            | MmapBuffer.leased      |
 * | nextStructID       | (Go runtime)             | Heap allocation addr   |
 * | readerHolding      | (Librarian cache)        | SharedSlab pointers    |
 * | readerAcquired     | (Reader state)           | TryInc result          |
 *
 ***************************************************************************)

EXTENDS Integers, FiniteSets, TLC

(***************************************************************************
 * CONSTANTS - Model Parameters
 ***************************************************************************)

CONSTANTS
    MemoryIDs,      \* Set of raw memory block IDs (models []byte buffers)
    MaxStructID,    \* Maximum struct ID to allocate (bounds state space)
    Readers         \* Set of reader process IDs (models Librarian lookups)

(***************************************************************************
 * VARIABLES - System State
 ***************************************************************************)

VARIABLES
    \* === Memory Pool (mempool.go:164 - MmapPool.buffers) ===
    memoryPool,     \* Set of memID currently available in pool

    \* === Struct Registry (heap-allocated MmapBuffer instances) ===
    \* Maps StructID -> [memID, ref, leased]
    \* - memID: which raw memory this struct points to (MmapBuffer.raw)
    \* - ref: reference count (MmapBuffer.refCount)
    \* - leased: safety guard (MmapBuffer.leased)
    structs,

    \* === Struct Allocation Counter ===
    nextStructID,   \* Next struct ID to allocate (models heap addresses)

    \* === Reader State ===
    \* readerHolding[r] = structID that reader r has a pointer to (or 0)
    \* This models the Librarian holding a SharedSlab reference
    readerHolding,

    \* readerAcquired[r] = TRUE if reader r successfully called TryInc
    \* This models a successful acquisition that grants access to memory
    readerAcquired

\* All variables tuple
vars == <<memoryPool, structs, nextStructID, readerHolding, readerAcquired>>

(***************************************************************************
 * TYPE INVARIANT
 ***************************************************************************)

TypeInvariant ==
    /\ memoryPool \subseteq MemoryIDs
    /\ nextStructID \in 1..(MaxStructID+1)
    /\ readerHolding \in [Readers -> 0..MaxStructID]
    /\ readerAcquired \in [Readers -> BOOLEAN]
    /\ \A s \in DOMAIN structs :
        /\ s \in 1..MaxStructID
        /\ structs[s].memID \in MemoryIDs
        /\ structs[s].ref \in -1..10  \* Allow negative for debugging
        /\ structs[s].leased \in BOOLEAN

(***************************************************************************
 * INITIAL STATE
 *
 * Models system startup: pool is full, no structs allocated, no readers.
 * Corresponds to NewMmapPool() filling the buffers channel.
 ***************************************************************************)

Init ==
    /\ memoryPool = MemoryIDs       \* Pool starts full (mempool.go:176-179)
    /\ structs = <<>>               \* No structs allocated yet (empty function)
    /\ nextStructID = 1             \* First struct will be ID 1
    /\ readerHolding = [r \in Readers |-> 0]    \* No readers holding refs
    /\ readerAcquired = [r \in Readers |-> FALSE]  \* No readers acquired

(***************************************************************************
 * HELPER OPERATORS
 ***************************************************************************)

\* Check if a struct ID is valid (has been allocated)
StructExists(s) == s \in DOMAIN structs

\* Check if a struct is "alive" (ref > 0)
StructAlive(s) == StructExists(s) /\ structs[s].ref > 0

(***************************************************************************
 * ACTION: Alloc
 *
 * Models: MmapPool.Acquire() (mempool.go:183-209)
 *
 * Go code:
 *   func (p *MmapPool) Acquire() *MmapBuffer {
 *       raw = <-p.buffers                    // [189] Get memory from pool
 *       buf := &MmapBuffer{raw: raw, pool: p} // [200] FRESH struct!
 *       buf.refCount.Store(1)                 // [205]
 *       buf.leased.Store(true)                // [206]
 *       return buf
 *   }
 *
 * Critical: This allocates a NEW struct, not recycling an old one.
 * This is the heart of the "Fresh Struct" strategy.
 ***************************************************************************)

Alloc(memID) ==
    /\ memID \in memoryPool                    \* Memory available in pool
    /\ nextStructID <= MaxStructID             \* Haven't exhausted struct IDs
    \* Remove memory from pool
    /\ memoryPool' = memoryPool \ {memID}
    \* Create FRESH struct with new ID (the key insight!)
    /\ structs' = structs @@ (nextStructID :> [memID |-> memID, ref |-> 1, leased |-> TRUE])
    /\ nextStructID' = nextStructID + 1
    /\ UNCHANGED <<readerHolding, readerAcquired>>

(***************************************************************************
 * ACTION: Share
 *
 * Models: A reader (Librarian) obtaining a pointer to a struct.
 * This happens when looking up a key in the SharedSlab cache.
 *
 * Go code equivalent (slab.go:72, SharedSlab.Acquire):
 *   buf := s.buf  // Reader now has a pointer to the MmapBuffer
 *
 * The reader has a POINTER but has not yet called TryInc.
 * This is the dangerous window where the struct could be released.
 ***************************************************************************)

Share(reader, structID) ==
    /\ readerHolding[reader] = 0              \* Reader not already holding
    /\ StructExists(structID)                  \* Struct exists
    \* Reader now holds a pointer (but no ref count increment yet!)
    /\ readerHolding' = [readerHolding EXCEPT ![reader] = structID]
    /\ UNCHANGED <<memoryPool, structs, nextStructID, readerAcquired>>

(***************************************************************************
 * ACTION: TryIncSuccess
 *
 * Models: MmapBuffer.TryInc() succeeding (mempool.go:37-50)
 *
 * Go code:
 *   func (b *MmapBuffer) TryInc() bool {
 *       for {
 *           c := b.refCount.Load()
 *           if c <= 0 { return false }        // [40-42] DEAD - reject!
 *           if b.refCount.CompareAndSwap(c, c+1) {
 *               return true                    // [45-46] Success!
 *           }
 *       }
 *   }
 *
 * Precondition: ref > 0 (struct is alive)
 * Effect: ref incremented, reader has valid access
 ***************************************************************************)

TryIncSuccess(reader) ==
    LET structID == readerHolding[reader]
    IN /\ readerHolding[reader] # 0           \* Reader is holding a pointer
       /\ ~readerAcquired[reader]             \* Haven't already acquired
       /\ StructExists(structID)
       /\ structs[structID].ref > 0           \* CRITICAL: ref > 0 check!
       \* Increment ref count (CAS succeeds)
       /\ structs' = [structs EXCEPT ![structID].ref = @ + 1]
       /\ readerAcquired' = [readerAcquired EXCEPT ![reader] = TRUE]
       /\ UNCHANGED <<memoryPool, nextStructID, readerHolding>>

(***************************************************************************
 * ACTION: TryIncFail
 *
 * Models: MmapBuffer.TryInc() returning false (mempool.go:40-42)
 *
 * Go code:
 *   if c <= 0 { return false }  // It's dead, reject!
 *
 * Precondition: ref <= 0 (struct is dead)
 * Effect: Reader gives up, releases pointer
 ***************************************************************************)

TryIncFail(reader) ==
    LET structID == readerHolding[reader]
    IN /\ readerHolding[reader] # 0           \* Reader is holding a pointer
       /\ ~readerAcquired[reader]             \* Haven't already acquired
       /\ StructExists(structID)
       /\ structs[structID].ref <= 0          \* CRITICAL: ref <= 0 check!
       \* Reader gives up (TryInc returned false)
       /\ readerHolding' = [readerHolding EXCEPT ![reader] = 0]
       /\ UNCHANGED <<memoryPool, structs, nextStructID, readerAcquired>>

(***************************************************************************
 * ACTION: ReaderRelease
 *
 * Models: Reader calling Unpin after successful TryInc (mempool.go:73-85)
 * Also models Releaser.Release() in slab.go:38-50
 *
 * Go code:
 *   func (b *MmapBuffer) Unpin() {
 *       if b.refCount.Add(-1) == 0 {
 *           b.resetAndRelease()  // Return memory to pool
 *       }
 *   }
 ***************************************************************************)

ReaderRelease(reader) ==
    LET structID == readerHolding[reader]
    IN /\ readerAcquired[reader]              \* Reader has valid acquisition
       /\ StructExists(structID)
       /\ structs[structID].ref > 0           \* Sanity: should have ref
       \* Decrement ref
       /\ structs' = [structs EXCEPT ![structID].ref = @ - 1]
       /\ readerAcquired' = [readerAcquired EXCEPT ![reader] = FALSE]
       /\ readerHolding' = [readerHolding EXCEPT ![reader] = 0]
       /\ UNCHANGED <<memoryPool, nextStructID>>

(***************************************************************************
 * ACTION: OwnerRelease
 *
 * Models: The original owner (MemTable/Flusher) calling Unpin when done.
 * When ref hits 0, memory returns to pool.
 *
 * Go code (mempool.go:73-85, 92-108):
 *   func (b *MmapBuffer) Unpin() {
 *       if b.refCount.Add(-1) == 0 {
 *           b.resetAndRelease()
 *       }
 *   }
 *   func (b *MmapBuffer) resetAndRelease() {
 *       if !b.leased.CompareAndSwap(true, false) { return }
 *       b.pool.ReleaseBytes(b.raw)  // Memory back to pool!
 *   }
 *
 * This is where the struct "dies" and memory is recycled.
 * The struct remains in memory (for GC) but ref=0 makes TryInc fail.
 ***************************************************************************)

OwnerRelease(structID) ==
    LET memID == structs[structID].memID
    IN /\ StructExists(structID)
       /\ structs[structID].ref = 1            \* Owner has last ref
       /\ structs[structID].leased = TRUE      \* Still leased
       \* Decrement ref to 0 and mark as not leased
       /\ structs' = [structs EXCEPT
           ![structID].ref = 0,
           ![structID].leased = FALSE]
       \* Return memory to pool (resetAndRelease)
       /\ memoryPool' = memoryPool \union {memID}
       /\ UNCHANGED <<nextStructID, readerHolding, readerAcquired>>

(***************************************************************************
 * NEXT STATE RELATION
 ***************************************************************************)

Next ==
    \/ \E m \in MemoryIDs : Alloc(m)
    \/ \E r \in Readers, s \in 1..MaxStructID : Share(r, s)
    \/ \E r \in Readers : TryIncSuccess(r)
    \/ \E r \in Readers : TryIncFail(r)
    \/ \E r \in Readers : ReaderRelease(r)
    \/ \E s \in 1..MaxStructID : OwnerRelease(s)

(***************************************************************************
 * FAIRNESS
 *
 * We need fairness to ensure:
 * - Readers eventually attempt TryInc (don't hold pointers forever)
 * - Readers eventually release after acquiring
 ***************************************************************************)

Fairness ==
    /\ \A r \in Readers : WF_vars(TryIncSuccess(r))
    /\ \A r \in Readers : WF_vars(TryIncFail(r))
    /\ \A r \in Readers : WF_vars(ReaderRelease(r))

(***************************************************************************
 * SPECIFICATION
 ***************************************************************************)

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************
 * INVARIANTS - Must hold in EVERY reachable state
 ***************************************************************************)

\* INV-1: Type safety
InvType == TypeInvariant

\* INV-2: THE CRITICAL SAFETY PROPERTY
\* If a reader has successfully acquired (TryInc returned true), the
\* underlying memory MUST NOT be in the pool.
\*
\* This is what prevents use-after-free: you can't access memory that
\* has been returned to the pool and potentially given to someone else.
\*
\* Error Trace Guide: If this fails, look for:
\*   - readerAcquired[r] = TRUE
\*   - structs[readerHolding[r]].memID in memoryPool
\*   - This would mean TryInc succeeded but memory was recycled!
InvSafeAccess ==
    \A r \in Readers :
        readerAcquired[r] =>
            LET structID == readerHolding[r]
            IN structs[structID].memID \notin memoryPool

\* INV-3: No double-free
\* Memory cannot be returned to pool if it's already there.
\* Each memID appears at most once in the pool.
InvNoDoubleFree ==
    \A m \in MemoryIDs :
        m \in memoryPool =>
            ~\E s \in DOMAIN structs :
                /\ structs[s].memID = m
                /\ structs[s].leased = TRUE

\* INV-4: Reference count sanity
\* If a struct has ref > 0, it must be leased (not released to pool)
InvRefCountSanity ==
    \A s \in DOMAIN structs :
        structs[s].ref > 0 => structs[s].leased = TRUE

\* INV-5: Memory accounting
\* Each memory block is either in the pool OR owned by exactly one
\* leased struct (not both, not neither).
InvMemoryAccounting ==
    \A m \in MemoryIDs :
        \/ m \in memoryPool  \* In pool
        \/ \E s \in DOMAIN structs :  \* OR owned by a leased struct
            /\ structs[s].memID = m
            /\ structs[s].leased = TRUE

(***************************************************************************
 * TEMPORAL PROPERTIES
 ***************************************************************************)

\* PROP-1: Liveness - Readers holding pointers eventually resolve
\* (either acquire or fail)
PropReaderResolves ==
    \A r \in Readers :
        (readerHolding[r] # 0 /\ ~readerAcquired[r]) ~>
        (readerHolding[r] = 0 \/ readerAcquired[r])

(***************************************************************************
 * ABA PROBLEM TRACE EXPLANATION
 *
 * The model PROVES safety because of how Fresh Structs work:
 *
 * Consider this trace (which the model explores):
 *
 * State 1: Pool = {M1}, no structs
 * State 2: Alloc(M1) -> Struct S1 created, ref=1, points to M1
 * State 3: Share(Reader, S1) -> Reader holds pointer to S1
 * State 4: OwnerRelease(S1) -> S1.ref=0, M1 back to pool
 * State 5: Alloc(M1) -> Struct S2 created (NEW!), ref=1, points to M1
 * State 6: TryIncFail(Reader) -> Reader's TryInc on S1 FAILS (ref=0)
 *
 * THE KEY INSIGHT:
 * - Reader holds pointer to S1 (the OLD struct)
 * - S2 is a DIFFERENT struct (Fresh Struct strategy!)
 * - Even though S1 and S2 point to the SAME memory M1
 * - TryInc on S1 correctly fails because S1.ref = 0
 * - If we had pooled structs (S2 == S1), TryInc would wrongly succeed!
 *
 * InvSafeAccess guarantees: TryInc success => memory not in pool
 * This is EXACTLY what prevents the ABA bug.
 *
 * To see this trace, run TLC and examine the state graph. Any path where
 * a reader's TryInc succeeds MUST have the memory NOT in the pool.
 *
 ***************************************************************************)

=============================================================================
