------------------------------ MODULE Slab_Mutant ------------------------------
(***************************************************************************
 * MUTANT MODEL: Buggy Implementation with Struct Pooling
 *
 * This is a DELIBERATELY BROKEN version of Slab.tla that reuses struct IDs
 * instead of allocating fresh ones. This simulates the buggy pattern where
 * MmapBuffer structs are pooled along with the raw memory.
 *
 * EXPECTED RESULT: TLC should find a violation of InvNoABA,
 * demonstrating the ABA problem.
 *
 * THE BUG:
 * When we reuse struct IDs, a reader holding a stale pointer can
 * successfully TryInc on a struct that now belongs to someone else,
 * because the refCount was reset to 1 (not 0) upon reuse.
 *
 * GHOST STATE:
 * We add readerExpectedMem to track what memory the reader SAW when they
 * first obtained the pointer. This lets us detect when they acquire
 * access to DIFFERENT memory than expected (the ABA problem).
 *
 ***************************************************************************)

EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    MemoryIDs,
    MaxStructID,
    Readers

VARIABLES
    memoryPool,
    structs,
    nextStructID,
    readerHolding,
    readerAcquired,
    structPool,
    \* GHOST STATE: What memory did the reader expect when they called Share?
    \* This doesn't exist in the real code - it's for verification only.
    readerExpectedMem

vars == <<memoryPool, structs, nextStructID, readerHolding, readerAcquired, structPool, readerExpectedMem>>

TypeInvariant ==
    /\ memoryPool \subseteq MemoryIDs
    /\ nextStructID \in 1..(MaxStructID+1)
    /\ readerHolding \in [Readers -> 0..MaxStructID]
    /\ readerAcquired \in [Readers -> BOOLEAN]
    /\ structPool \subseteq 1..MaxStructID
    /\ \A r \in Readers : readerExpectedMem[r] \in MemoryIDs \union {0}
    /\ \A s \in DOMAIN structs :
        /\ s \in 1..MaxStructID
        /\ structs[s].memID \in MemoryIDs
        /\ structs[s].ref \in -1..10
        /\ structs[s].leased \in BOOLEAN

Init ==
    /\ memoryPool = MemoryIDs
    /\ structs = <<>>
    /\ nextStructID = 1
    /\ readerHolding = [r \in Readers |-> 0]
    /\ readerAcquired = [r \in Readers |-> FALSE]
    /\ structPool = {}
    /\ readerExpectedMem = [r \in Readers |-> 0]  \* No expectations yet

StructExists(s) == s \in DOMAIN structs
StructAlive(s) == StructExists(s) /\ structs[s].ref > 0

(***************************************************************************
 * AllocFresh - Correct behavior (used when pool is empty)
 ***************************************************************************)

AllocFresh(memID) ==
    /\ memID \in memoryPool
    /\ nextStructID <= MaxStructID
    /\ structPool = {}
    /\ memoryPool' = memoryPool \ {memID}
    /\ structs' = structs @@ (nextStructID :> [memID |-> memID, ref |-> 1, leased |-> TRUE])
    /\ nextStructID' = nextStructID + 1
    /\ UNCHANGED <<readerHolding, readerAcquired, structPool, readerExpectedMem>>

(***************************************************************************
 * AllocReuse - THE BUG!
 *
 * Reuse a struct ID from the pool. This causes ABA because readers
 * holding stale pointers will see ref reset to 1 and TryInc succeeds.
 ***************************************************************************)

AllocReuse(memID, structID) ==
    /\ memID \in memoryPool
    /\ structID \in structPool
    /\ memoryPool' = memoryPool \ {memID}
    /\ structs' = [structs EXCEPT
        ![structID].memID = memID,  \* NOW POINTS TO DIFFERENT MEMORY!
        ![structID].ref = 1,
        ![structID].leased = TRUE]
    /\ structPool' = structPool \ {structID}
    /\ UNCHANGED <<nextStructID, readerHolding, readerAcquired, readerExpectedMem>>

(***************************************************************************
 * Share - Reader obtains pointer and records expected memory (ghost state)
 ***************************************************************************)

Share(reader, structID) ==
    /\ readerHolding[reader] = 0
    /\ StructExists(structID)
    /\ StructAlive(structID)  \* Only share live structs
    /\ readerHolding' = [readerHolding EXCEPT ![reader] = structID]
    \* GHOST: Record what memory the reader expects to access
    /\ readerExpectedMem' = [readerExpectedMem EXCEPT ![reader] = structs[structID].memID]
    /\ UNCHANGED <<memoryPool, structs, nextStructID, readerAcquired, structPool>>

(***************************************************************************
 * TryIncSuccess - Reader successfully increments ref count
 ***************************************************************************)

TryIncSuccess(reader) ==
    LET structID == readerHolding[reader]
    IN /\ readerHolding[reader] # 0
       /\ ~readerAcquired[reader]
       /\ StructExists(structID)
       /\ structs[structID].ref > 0
       /\ structs' = [structs EXCEPT ![structID].ref = @ + 1]
       /\ readerAcquired' = [readerAcquired EXCEPT ![reader] = TRUE]
       /\ UNCHANGED <<memoryPool, nextStructID, readerHolding, structPool, readerExpectedMem>>

(***************************************************************************
 * TryIncFail - Reader sees ref <= 0, gives up
 ***************************************************************************)

TryIncFail(reader) ==
    LET structID == readerHolding[reader]
    IN /\ readerHolding[reader] # 0
       /\ ~readerAcquired[reader]
       /\ StructExists(structID)
       /\ structs[structID].ref <= 0
       /\ readerHolding' = [readerHolding EXCEPT ![reader] = 0]
       /\ readerExpectedMem' = [readerExpectedMem EXCEPT ![reader] = 0]
       /\ UNCHANGED <<memoryPool, structs, nextStructID, readerAcquired, structPool>>

(***************************************************************************
 * ReaderRelease - Reader releases their reference
 ***************************************************************************)

ReaderRelease(reader) ==
    LET structID == readerHolding[reader]
    IN /\ readerAcquired[reader]
       /\ StructExists(structID)
       /\ structs[structID].ref > 0
       /\ structs' = [structs EXCEPT ![structID].ref = @ - 1]
       /\ readerAcquired' = [readerAcquired EXCEPT ![reader] = FALSE]
       /\ readerHolding' = [readerHolding EXCEPT ![reader] = 0]
       /\ readerExpectedMem' = [readerExpectedMem EXCEPT ![reader] = 0]
       /\ UNCHANGED <<memoryPool, nextStructID, structPool>>

(***************************************************************************
 * OwnerRelease - Owner releases, struct goes to pool (THE BUG!)
 ***************************************************************************)

OwnerRelease(structID) ==
    LET memID == structs[structID].memID
    IN /\ StructExists(structID)
       /\ structs[structID].ref = 1
       /\ structs[structID].leased = TRUE
       /\ structs' = [structs EXCEPT
           ![structID].ref = 0,
           ![structID].leased = FALSE]
       /\ memoryPool' = memoryPool \union {memID}
       /\ structPool' = structPool \union {structID}  \* BUG: Pool the struct!
       /\ UNCHANGED <<nextStructID, readerHolding, readerAcquired, readerExpectedMem>>

(***************************************************************************
 * Next State Relation
 ***************************************************************************)

Next ==
    \/ \E m \in MemoryIDs : AllocFresh(m)
    \/ \E m \in MemoryIDs, s \in 1..MaxStructID : AllocReuse(m, s)
    \/ \E r \in Readers, s \in 1..MaxStructID : Share(r, s)
    \/ \E r \in Readers : TryIncSuccess(r)
    \/ \E r \in Readers : TryIncFail(r)
    \/ \E r \in Readers : ReaderRelease(r)
    \/ \E s \in 1..MaxStructID : OwnerRelease(s)

Spec == Init /\ [][Next]_vars

(***************************************************************************
 * INVARIANTS
 ***************************************************************************)

InvType == TypeInvariant

\* Original safety: acquired memory not in pool
InvSafeAccess ==
    \A r \in Readers :
        readerAcquired[r] =>
            LET structID == readerHolding[r]
            IN structs[structID].memID \notin memoryPool

\* THE ABA DETECTOR - This WILL FAIL on the mutant!
\*
\* If a reader has successfully acquired (TryInc returned true),
\* the memory they GOT must match the memory they EXPECTED.
\*
\* When struct IDs are reused, the reader expects memory A but
\* the struct now points to memory B. This invariant catches that!
InvNoABA ==
    \A r \in Readers :
        readerAcquired[r] =>
            LET structID == readerHolding[r]
            IN structs[structID].memID = readerExpectedMem[r]

\* No double-free
InvNoDoubleFree ==
    \A m \in MemoryIDs :
        m \in memoryPool =>
            ~\E s \in DOMAIN structs :
                /\ structs[s].memID = m
                /\ structs[s].leased = TRUE

\* Ref count sanity
InvRefCountSanity ==
    \A s \in DOMAIN structs :
        structs[s].ref > 0 => structs[s].leased = TRUE

=============================================================================
