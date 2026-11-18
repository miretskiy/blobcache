# Benchmark findings

Durable conclusions from cache benchmarks; individual runs are not kept here.
Device ceilings are in [fio/README.md](fio/README.md).

## Measuring CPU on this host, 2026-11-17

Until 2026-11-17, `local/bench-tools/run.py` attached `perf stat -p` with nine
hardware events to the benchmark for the whole run. **Counting hardware events
on one process inflates its CPU, all of it kernel time.** Same steady blobcache
phase (16 GiB, 500 MB/s), on-CPU time from `schedstat` over one 15 s window:

| Attached to the process | CPU s / 15 s | vs none |
|---|---|---|
| nothing | 2.47 | 1× |
| Go CPU profile | 2.46 | 1× |
| `perf stat -p`, software events only | 2.58 | 1.05× |
| `perf stat -p -e cycles` | 4.57 | 1.85× |
| `perf stat -p`, run.py's nine hardware events | 6.28 | 2.54× |

- The cost is per context switch, when perf saves and reloads the process's
  counters: about 26 µs per switch with four hardware events (`nvme-bench` writes,
  8.5 → 43 µs per I/O at 1.3 switches per I/O). Why reprogramming is that slow
  is not verified; trapped PMU register accesses in the VM would explain it.
- **Cycle sampling cannot see this time**: a system-wide profile of an
  inflated run has no samples in perf's switching code. So system-wide
  `perf record -a` profiles taken during inflated runs still give the real
  work's proportions, while rusage, pidstat, `/proc` times and Go's CPU profile
  include the inflation, charged to the syscalls where threads block. This is
  why Go's profile showed more time in `io_uring_enter` than system-wide perf.
- `run.py` no longer runs `perf stat` on the benchmark. CPU figures in the
  registered-memory section were measured with the inflation.
- Without instrumentation, blobcache in that window used 0.16 cores at 10.9k
  commands/s: 15 µs per command for all its work, against 8.5 µs per command
  for `nvme-bench` writes. Its kernel time per command is 1.2× the
  microbenchmark's (tick-sampled split), not the 2.4× an inflated run showed.
  Pinning to 8 CPUs with `GOMAXPROCS=8` changed nothing (0.164 cores).

## blobcache vs ringcache, 2026-11-18

`BenchmarkBlobCache/bursty-variable` and ringcache's benchmark on one trace:
250k fills, 436,180 reads, 118 GB offered in 101.7 s; 500 MB/s background,
45 s bursts at 2 GB/s every 75 s; 400 GiB disk, 16 GiB memory, 500 ms write
backlog limit, 8 rings (blobcache: 7 read, 1 write). ringcache (its 2026-11-08
binary) ran on the XFS slice below with the page cache, in a 45 GiB cgroup;
blobcache (the tree with the reclaim changes below) on the raw NVMe range. The
trace uses the size bands from before `bd4bc6a`, which ringcache's binary has;
blobcache's benchmark binary was built with them for this comparison. No perf
instrumentation; two runs each, interleaved; ranges are over both runs.

| | ringcache | blobcache |
|---|---|---|
| writes stored / rejected | 218,571–218,720 / 31,280–31,429 | 156,112–158,033 / 91,967–93,888 |
| GB rejected; GiB written | 28.5–28.8; 83.4–83.6 | 33.8–33.9; 78.6–78.8 |
| hit rate | 90.8–90.9% | 91.5–91.7% |
| reads from memory / disk | 91k / 305k | 287k / 112k |
| read p99 / p99.9 / max, including misses (ms) | 3.9 / 12.4–12.8 / 50–57 | 2.8–2.9 / 9.0–9.2 / 26 |
| disk-read p50 / p99.9 (ms) | 0.46 / 16 | 0.81 / 13.2–13.5 |
| Put p99 / max (ms) | 0.74–0.75 / 16–17 | 0.034–0.035 / 2.9–3.2 |
| write completion p50 (ms) | 0.25 | 429–434 |
| process CPU, measured phase | 0.78–0.80 cores | 0.43 cores |
| CPU, whole run (user + system) | 123–125 s | 62–63 s |
| GC cycles; minor faults | 201–211; 7.7–7.9M | 9; 146–148k |

- **blobcache uses about half of ringcache's CPU** (system time 2.3× less),
  with better read tails, a higher hit rate, and 3× more reads from memory.
- **It rejects 3× as many writes**, but only 18% more bytes: its 500 ms write
  backlog limit sheds small writes that ringcache's staging absorbs. Stored
  writes wait about 0.43 s for disk at that limit.
- ringcache's disk reads have a lower median (0.46 against 0.81 ms) because
  some are page-cache hits.
- This trace's sizes are smaller than the current benchmark's; there is no
  ringcache binary with the current bands.

## ringcache on an XFS slice of the raw range, 2026-11-17

`/instance_storage` is now 256 GiB, too small for ringcache's 400 GiB disk.
`local/bench-tools/ringcache-xfs.sh` maps a 460 GiB slice of `dio_raw` (from
900 GiB) and mounts XFS on it at `/ringcache-xfs`. ringcache's 250k-fill run
there reproduces its 2026-11-08 run on the old full-size XFS: 90.4% vs 90.7%
hits, 217,221 vs 218,127 writes stored, read p99.9 14.6 vs 14.1 ms (both with
`perf stat` attached, so CPU is not compared). Run ringcache with the
settings recorded in its `run.json`; its defaults generate a different trace.

## Registered memory in blobcache, 2026-11-17

Production-shaped bursty workload, 1M fills (16 minutes measured), 2 GiB of
cache memory, 7 read rings and 1 write ring, raw range, Linux 6.8. The two runs
differ only in `WithRegisteredMemory`:

| | plain | registered |
|---|---|---|
| CPU, measured phase (inflated) | 1.29 cores | 1.28 cores |
| system CPU, whole run (inflated) | 835 s | 816 s |
| read p99 / p99.9 (ms) | 2.76 / 6.39 | 2.80 / 6.35 |
| writes stored / hit rate | 884,065 / 86.2% | 883,807 / 86.1% |

- **Registration saves little CPU and changes nothing else.** The 19 s of
  system time it saved is 2% of an inflated total.
  On 6.8 the allocator registers as 128 KiB entries, and 24% of commands
  still use ordinary addresses: the allocator joins adjacent claims, so a
  command can straddle two entries. The saving matches the per-command
  estimate: about 22k commands/s × 76% × 1.6 µs ≈ 0.03 cores.
- **Where CPU goes depends on the phase** (system-wide profile proportions,
  which exclude the inflation). In background phases, `io_uring_enter` takes 38% of the process (submission 22%, mostly
  the NVMe driver; completion waits 10%), the Go scheduler 13%, futex 8%, and
  the harness's copies into writers 11%. In bursts, memory
  reclaim takes 25% (walking the eviction list under 2 GiB pressure) and the
  allocator 8%; `io_uring_enter` falls to 6–9%.
- **blobcache batches more commands per `io_uring_enter` than the
  microbenchmark**: about 2.5 against 1.0 (`nvme-bench` submits one command
  per entry at these depths). Its higher kernel time per command is not from
  missing batching.
- **The host runs a security agent with kernel probes**, including a return
  probe on `io_issue_sqe`, so every io_uring request pays for it. Its cost is
  not measured; it applies to fio, dio, and blobcache alike.

## Memory reclaim skips pending writes, 2026-11-18

Under 2 GiB of cache memory with bursts to 2 GB/s (500k fills, 8m47s), reclaim
walked each shard's queue over entries whose writes were still pending: they
cannot be evicted, but they counted toward the bytes a shard was asked to free.
Two changes:

1. Reclaim sizes each shard's share by its evictable bytes (resident minus
   pending) and skips shards with none.
2. Put's completion queues a reclaim request when a resident value stops
   being pending.

Change 1 alone made reclaim return at once while everything was pending, and
nothing ran it again until the next `NewWriter` or `ReadRange`, which allocate
before queuing a request. The old walk had, by spinning, evicted each value as
soon as its write completed. Change 2 restores that promptly, without the walk.

Each column is the range over its runs, interleaved, without perf
instrumentation; both binaries built from one tree:

| | baseline (3 runs) | change 1 (2) | changes 1+2 (2) |
|---|---|---|---|
| queue entries visited | 6.20–6.23 billion | 31.5–31.7 million | 33.0–33.1 million |
| writes rejected for memory | 51,636–51,763 | 52,615–52,697 | 51,292–51,629 |
| reads rejected for memory | 44,759–45,170 | 47,400–47,949 | 43,590–43,752 |
| misses | 118,692–119,237 | 120,614–121,564 | 117,317–119,487 |
| process CPU, measured phase | 0.550–0.558 cores | 0.410–0.418 | 0.416–0.417 |
| user CPU, whole run | 198–199 s | 123–125 s | 124–125 s |
| read p99.9 (ms) | 6.77–6.82 | 6.75–6.77 | 6.74–6.86 |

- **Both changes together cut CPU 25% (user CPU 37%)** and match or improve
  on the baseline everywhere else: rejected reads drop 3%.
- Change 1 alone rejected 2% more writes and 6% more reads, repeatably.
- Runs of the same binary differ by up to 0.7% in rejected writes and 1.9%
  in misses; change 1's rises in rejected writes and reads exceed that.
