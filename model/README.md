# Formal Verification with TLA+

This directory contains TLA+ specifications for formally verifying critical blobcache protocols.

## Why TLA+?

Concurrent systems are notoriously hard to get right. Race conditions, deadlocks, and subtle ordering bugs often hide for years before manifesting in production. Traditional testing can't explore all interleavings.

**TLA+ is different**: It exhaustively explores *every possible* interleaving of concurrent operations, crashes, and recoveries. If there's a bug, TLC (the model checker) will find it and show you the exact sequence of steps that triggers it.

Real-world wins from TLA+:
- **Amazon**: Found bugs in DynamoDB, S3, and EBS that would have been "extremely difficult" to find otherwise
- **Microsoft**: Used to verify Azure Cosmos DB protocols
- **MongoDB**: Verified Raft implementation for replication

## Quick Start

### 1. Install TLA+ Tools

**Option A: TLA+ Toolbox (Recommended for beginners)**
- Download from: https://github.com/tlaplus/tlaplus/releases
- GUI with syntax highlighting, model configuration, and error traces

**Option B: Command Line (CI/automation)**
```bash
# Download tla2tools.jar
curl -LO https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

# Or via Homebrew on macOS
brew install tla-plus/tap/tla-plus
```

### 2. Run the WAL Model

**With TLA+ Toolbox:**
1. Open `model/wal/WAL.tla`
2. Create a new model (Model → New Model)
3. In "What is the behavior spec?": Select "Temporal formula" and enter `Spec`
4. In "What is the model?": Add constants from WAL.cfg
5. In "What to check?": Add invariants and properties
6. Click "Run TLC"

**Command Line:**
```bash
cd model/wal

# Basic run (uses WAL.cfg)
java -jar /path/to/tla2tools.jar -config WAL.cfg WAL.tla

# With more workers for faster checking
java -jar /path/to/tla2tools.jar -config WAL.cfg -workers 4 WAL.tla

# Generate a PDF of the spec
java -jar /path/to/tla2tools.jar -dump dot WAL.tla && dot -Tpdf WAL.dot -o WAL.pdf
```

### 3. Interpret Results

**Success:**
```
Model checking completed. No error has been found.
  Estimates of the probability that TLC did not check all reachable states...
  State space estimated: 47,832 states
  Distinct states found: 47,832
```
This means the protocol is correct for the given parameters.

**Failure:**
```
Error: Invariant InvDurability is violated.
Error: The following sequence of states leads to the error...
```
TLC shows the exact sequence of operations that breaks the invariant. This is invaluable for debugging.

## Understanding the WAL Model

### What It Models

The WAL (Write-Ahead Log) with Group Commit from `internal/wal/wal.go`:

```
┌─────────────────────────────────────────────────────────────┐
│                      WAL Architecture                        │
├─────────────────────────────────────────────────────────────┤
│  Writer 1 ──┐                                               │
│  Writer 2 ──┼──► Pending Queue ──► Leader ──► fsync ──► Disk│
│  Writer 3 ──┘        (mutex)        (one)     (once)        │
└─────────────────────────────────────────────────────────────┘
```

**Key insight**: Multiple concurrent writers batch into a single `fsync`, amortizing the cost of durability. This is why blobcache achieves 1.1+ GB/s sustained writes.

### Variables in the Model

| TLA+ Variable    | Go Code Equivalent           | Purpose                           |
|------------------|------------------------------|-----------------------------------|
| `leaderActive`   | `writerBusy`                 | Is a leader currently flushing?   |
| `pendingQueue`   | `pending`                    | Writers waiting to be flushed     |
| `flushingQueue`  | `flushing` (ping-pong)       | Batch being flushed by leader     |
| `sealedSeq`      | `lastRotatedSeq`             | Max SeqID of closed files (guard) |
| `currentMaxSeq`  | `currentMaxSeq`              | Max SeqID in current file         |
| `disk`           | fsync'd file content         | Durable storage (survives crash)  |

### Properties Verified

1. **InvDurability** (THE critical property):
   > If `Write()` returns success, the data is on disk.

   This guarantees that acknowledged writes survive crashes.

2. **InvSingleLeader**:
   > At most one goroutine performs I/O at a time.

   This ensures the mutex is working correctly.

3. **PropWriteCompletes** (liveness):
   > A pending write eventually completes.

   This ensures no deadlocks.

4. **PropCrashSafe**:
   > Crash doesn't lose committed data.

   Disk contents are monotonically growing (ignoring rotation/cleanup).

## TLA+ Syntax Cheat Sheet

```tla
\* This is a comment

(* This is a
   multi-line comment *)

\* Logical operators
/\    \* AND (conjunction)
\/    \* OR (disjunction)
~     \* NOT (negation)
=>    \* IMPLIES
<=>   \* IF AND ONLY IF

\* Quantifiers
\A x \in S : P(x)    \* For all x in S, P(x) holds
\E x \in S : P(x)    \* There exists x in S such that P(x)

\* Set operations
x \in S              \* x is a member of S
S \union T           \* Union of S and T
S \subseteq T        \* S is a subset of T
{x \in S : P(x)}     \* Set of x in S where P(x) holds
{f(x) : x \in S}     \* Set comprehension (map f over S)

\* Sequences (like arrays)
<<a, b, c>>          \* A sequence with three elements
Len(seq)             \* Length of sequence
seq[i]               \* i-th element (1-indexed!)
Append(seq, x)       \* Append x to seq
Head(seq)            \* First element
Tail(seq)            \* All but first element

\* Functions (like maps/dicts)
[x \in S |-> f(x)]   \* Function mapping each x in S to f(x)
f[x]                 \* Apply function f to x
[f EXCEPT ![x] = y]  \* Function like f but f[x] = y

\* Temporal operators
[]P                  \* P is always true (invariant)
<>P                  \* P is eventually true (liveness)
P ~> Q               \* P leads to Q (if P becomes true, Q eventually becomes true)
[][A]_vars           \* Every step is either an A step or a stuttering step
WF_vars(A)           \* Weak fairness: if A is always enabled, it eventually happens
```

## Extending the Models

### Adding New Properties

To verify a new property, add it to the `.tla` file:

```tla
\* Example: No writer waits forever
PropNoStarvation ==
    \A w \in Writers :
        (writerState[w] = "pending") ~> (writerResult[w] = "success")
```

Then add to `.cfg`:
```
PROPERTY PropNoStarvation
```

### Modeling New Protocols

Create a new directory (e.g., `model/compaction/`) with:
1. `Protocol.tla` - The specification
2. `Protocol.cfg` - The configuration

Key steps:
1. Identify the key variables (state)
2. Identify the key transitions (actions)
3. Identify the invariants (safety properties)
4. Identify the temporal properties (liveness)

## Common Pitfalls

### 1. State Space Explosion

TLA+ explores ALL interleavings. With N processes and M states each, that's O(M^N) states. Keep models small:

```tla
\* BAD: Too many writers
CONSTANT Writers = {w1, w2, w3, w4, w5}  \* 5! interleavings per step

\* GOOD: Minimum to catch bugs
CONSTANT Writers = {w1, w2}  \* 2 writers finds most concurrency bugs
```

### 2. Missing Fairness

Without fairness, TLC can "cheat" by never executing certain actions:

```tla
\* Without fairness, a leader could hold the lock forever
Spec == Init /\ [][Next]_vars

\* With fairness, leaders must eventually release
Spec == Init /\ [][Next]_vars /\ WF_vars(FinishFlush)
```

### 3. Forgetting UNCHANGED

Every action must specify what variables change AND what stays the same:

```tla
\* BAD: Doesn't specify what happens to other variables
Action == x' = x + 1

\* GOOD: Explicit about unchanged variables
Action ==
    /\ x' = x + 1
    /\ UNCHANGED <<y, z>>
```

## Resources

- **TLA+ Home**: https://lamport.azurewebsites.net/tla/tla.html
- **Learn TLA+**: https://learntla.com/ (interactive tutorial)
- **Video Course**: Leslie Lamport's video course (free): https://lamport.azurewebsites.net/video/videos.html
- **Practical TLA+**: https://www.hillelwayne.com/post/practical-tla/ (blog series)
- **AWS TLA+ Usage**: https://lamport.azurewebsites.net/tla/amazon.html

## Directory Structure

```
model/
├── README.md           # This file
└── wal/
    ├── WAL.tla         # WAL Group Commit specification
    └── WAL.cfg         # TLC configuration
```

## Integration with Development

When to update the model:
- **New concurrent protocol**: Model it before implementing
- **Bug in production**: Reproduce in model, verify fix
- **Protocol change**: Update model, re-verify

When NOT to update the model:
- **Code refactoring** that doesn't change semantics
- **Performance optimizations** that don't change behavior
- **Adding logging or metrics**
