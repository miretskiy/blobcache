# I/O rings, budgets, and CPU affinity

Use **one shared ring** for the mixed blob-cache workload measured here.
Multiple rings did not improve throughput and generally worsened read tails.
One read ring plus one write ring matched the shared ring's read latency, so
the evidence favors the simpler default rather than proving that every
multi-ring arrangement is slower in every respect.

Retain independent DIO schedulers and optional CPU affinity. Their useful next
application is **isolation between different workloads**, with each workload's
traffic and admission policy kept together. Splitting the reads and writes of
one workload is not a demonstrated optimization in these measurements.

## What the latency numbers mean

| Measurement | Start | End |
|---|---|---|
| GET end-to-end | Entry to `Cache.Get` | Return after verification and callback |
| DIO scheduler wait | Entry to `Submit` | First kernel handoff |
| io_uring observed completion latency, or clat | First kernel handoff | Coordinator observes the operation's CQE |
| Write ticket latency | Entry to `Submit` | Ticket completion bookkeeping, including linked followers |
| PUT admission | Benchmark allocation/fill/retry work | `Put` accepts the value |

Clat includes kernel queuing, storage service, and delay before userspace reaps
the CQE. It is not pure device service time. Long clat alone does not identify
kernel or device head-of-line blocking. Scheduler wait is elapsed queuing time,
not CPU time spent executing scheduler code. Phase p99 values generally refer
to different requests and **must not be added**.

The diagnostic recorder samples one Submit in eight and records acceptance,
barrier release, placement, kernel handoff, individual operation CQEs, and ticket
completion. Reads also record entry before routing and handle locking. Linked
operations have separate CQE timestamps; merged writes share the handoff and
completion of their actual `writev`. Buffers are bounded and coordinator-owned;
trace files are written after shutdown. Warmup and Go benchmark calibration
are excluded from the analysis.

## Mixed-workload results, 2026-10-09

Host: AWS m7gd.8xlarge instance storage, Linux/arm64. Source: blobcache
`0dd94f2` and DIO `4b6ca62` (`v2.3.0`), with diagnostic-only instrumentation.
Each case used 1 GiB cache memory, 32 workers, 30,000 measured writes, 100 KB–2 MB
values, direct I/O, and no disk eviction. Reads and writes came from the same
closed-loop workers. Coordinators requested distinct CPUs, with ring depth 256.
All write budgets in this table are 1.5 ms. `7R + 1W` means eight rings total.

All latency columns below are **p99 milliseconds**. Throughput is logical writes
in GiB/s; physical reads remained about 2.48 GiB/s.

| Rings | Read budget | GET end-to-end | Read scheduler wait | Read clat | Write clat | Write ticket | GiB/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 shared | 1.5 ms | 15.56 | 14.25 | 1.91 | 2.16 | 4.80 | 0.562 |
| 1R + 1W | 1.5 ms | 15.55 | 14.24 | 1.92 | 3.18 | 4.08 | 0.566 |
| 3R + 1W | 1.5 ms | 32.21 | 30.75 | 2.09 | 3.37 | 4.67 | 0.559 |
| 3R + 1W | 6 ms | 28.25 | 23.27 | 6.14 | 6.99 | 14.09 | 0.570 |
| 7R + 1W | 1.5 ms | 42.24 | 39.78 | 3.83 | 7.53 | 12.27 | 0.570 |
| 7R + 1W | 6 ms | 42.40 | 39.12 | 5.31 | 3.67 | 5.41 | 0.561 |
| 7R + 1W | 12 ms | 32.39 | 24.51 | 10.31 | 13.00 | 19.41 | 0.567 |
| 7R + 1W | 24 ms | 18.78 | 6.00 | 16.13 | 22.66 | 44.10 | 0.561 |
| 7R + 1W | Off | 16.88 | 0.03 | 16.80 | 13.92 | 20.90 | 0.572 |

All nine cases passed, with no corruption or write errors, dropped trace rows,
timestamp-order violations, short submissions, or enter errors. Go runtime
thread counts were stable during the measured phases: 22–23 for one/two rings,
25–28 for four rings, and 29–35 for eight rings. These counts exclude kernel
io-workers. Different invocations used different random workloads; small
differences are not established improvements. Rare coalesced batches make
sampled write-tail percentiles particularly variable.

These results do not directly describe the earlier 8 GiB, 500,000-write run
with disk eviction. That workload had fewer disk reads per write and a much
deeper physical write queue. A larger memory cache and eviction change the
traffic reaching the device.

## What the traces establish

### Fixed budget shares cause the dominant read-tail regression

The model divides 2.56 GB/s of read bandwidth among read rings. At 1.5 ms, a
single reader has about 3.84 MB of admission capacity; seven readers each have
about 0.549 MB. Many individual blobs exceed a ring's entire allowance.
`fitsBudget` permits one oversized operation while its class is idle, ensuring
progress, but later reads wait behind the local allowance.

Reads route by segment ID and cannot borrow another ring's unused budget.
This loses the pooling of capacity and arrivals provided by one shared queue.
The typical request can become faster while requests routed to a busy ring
wait much longer, even though the disk remains saturated.

In the earlier six-read/two-write trace comparison, the slowest 1% of sampled
reads spent an average 43.56 ms ready but waiting for admission, versus 2.14 ms
between kernel handoff and observed completion. Every sampled read in that
slow group encountered a budget rejection. Routing and locking were around a
microsecond; they did not explain the tens of milliseconds of extra waiting.

Increasing only the read budget moves waiting into the kernel/device path.
With read admission disabled, its scheduler-wait p99 falls to 0.03 ms and
end-to-end p99 falls to 16.88 ms. It still does not beat the single ring here.
Keeping the write budget fixed does not keep write latency fixed: both classes
share the device, and write batches can exceed their nominal allowance.

### Write merging and SQE batching are different mechanisms

DIO can combine adjacent writes into one `writev`. Its idle-class exception
currently admits an oversized merged batch, not just an indivisible record.
Some slow sampled writes belonged to batches of dozens of requests, up to 79
in this matrix. Bounding merged batches by the allowance, while preserving
progress for one oversized record, is a separate candidate experiment.

A coordinator can also hand several SQEs to one `io_uring_enter`. That can
amortize submission overhead even when writes are not merged. A subsequent
matched-binary experiment counted actual SQEs per enter and disabled merging
through DIO's existing `WithCoalescing(false)` option. It kept the same 1 GiB
cache and 32 workers, with fixed per-worker random seeds. Worker scheduling
still changes the exact request sequence.

| Rings | Admission budgets | Write merging | GET p99, ms | Read clat p99, ms | SubmitAndWait calls/s | User SQEs per nonempty handoff |
|---|---|---|---:|---:|---:|---:|
| 1 shared | 1.5 ms | On | 15.47 | 1.92 | 4,784 | 1.253 |
| 1 shared | 1.5 ms | Off | 15.39 | 1.91 | 4,775 | 1.265 |
| 7R + 1W | 1.5 ms | On | 41.39 | 3.83 | 5,267 | 1.009 |
| 7R + 1W | 1.5 ms | Off | 42.86 | 3.83 | 5,403 | 1.027 |
| 1 shared | Both off | Off | 17.78 | 16.13 | 286 | 10.796 |
| 7R + 1W | Both off | Off | 16.84 | 16.63 | 5,775 | 1.028 |

Disabling write merging does not remove the large eight-ring read tail.
Disabling admission does, even with merging disabled. All six cases delivered
about 0.56 GiB/s logical writes. With admission disabled, the one-ring case
amortizes submission over much larger batches. Its scheduler-wait p99 is still
6.87 ms: disabling the modeled budgets does not remove ring-capacity limits.
The eight-ring case's scheduler-wait p99 is 0.07 ms.

These results separate write merging from the dominant admission effect.
They do not, by themselves, fully attribute residual clat or CPU differences
to SQE batching. The completed cases did not independently disable
cross-submission SQE batching while retaining the same admission policy.

Call counts are DIO's calls to Ringo `SubmitAndWait`; empty nonblocking calls
can return without a syscall. Batch sizes exclude doorbell SQEs and empty
handoffs. They count actual queued user SQEs, not logical requests merged into
a `writev`.

## CPU affinity

The earlier fio comparisons were not a matched blobcache control. A subsequent
single-ring ABBA comparison kept `runtime.LockOSThread` and removed only
`SchedSetaffinity`, using DIO's `WithCoordinatorCPU(-1)`.

| Order | Affinity | GET p99, ms | Read clat p99, ms | Logical writes, GiB/s |
|---|---|---:|---:|---:|
| 1 | CPU 0 | 15.47 | 1.92 | 0.562 |
| 2 | Unrestricted | 15.49 | 1.95 | 0.560 |
| 3 | Unrestricted | 15.43 | 1.90 | 0.560 |
| 4 | CPU 0 | 15.52 | 1.93 | 0.561 |

Actual affinity masks were checked. Pinned coordinators executed on CPU 0;
each unpinned run sampled the coordinator on 13 different CPUs, with an allowed
mask of 0–31. There was no meaningful throughput or latency improvement from
pinning in this comparison. Runtime thread counts were stable at 21–24 during
the measured phases.

Pinned coordinators own threads; pinning does not reserve CPUs from other Go
work or isolate the underlying storage device.

### Steady-state hardware-counter method

The affinity counter comparison uses one shared ring, 1.5 ms read/write
budgets, 1 GiB cache memory, 32 workers, and 70,000 measured writes. Each case
captures four 12-second process windows followed by four 12-second coordinator
windows, beginning seven seconds after the measured phase starts. Each pair
is collected separately with `sudo perf stat`, including user and kernel
execution on the attached tasks:

| Pair | What it measures |
|---|---|
| `cycles,instructions` | Retired instructions per CPU cycle (IPC) |
| `branches,branch-misses` | Branch events and mispredictions |
| `l1d_cache,l1d_cache_refill` | L1 data-cache accesses and refills |
| `l2d_cache,l2d_cache_refill` | L2 accesses and refills |

Software counters collect task-clock, context switches, migrations, and page
faults in every window. The host's full `perf -d` preset is unavailable, and a
large explicit event set is heavily multiplexed. Counter pairs achieve 100%
coverage. Cache ratios below mean refills/accesses at that level, not LLC miss
rates. Startup metadata identifies coordinator TIDs and verifies their affinity.

CPU cores means task-clock divided by wall time: 0.8 cores is about 2.5% of this
32-core host. These are task-scoped measurements, not all machine or interrupt
CPU costs. Process and coordinator counters are from separate windows; their
values should not be subtracted as if simultaneous. The binary includes the
same diagnostic recorder in every case. Per-window NVMe block counters verify
comparable physical throughput and permit normalization by transferred bytes.

### Single-ring counter results

Two completed runs per affinity mode used the same binary and settings. Every
counter window was inside the measured phase, with 100% event coverage. Reads
held at about 2.38 GiB/s and physical writes at about 0.54–0.56 GiB/s during the
process captures. Values below average the two runs; CPU use averages the four
windows per scope.

| Measurement | Pinned to CPU 0 | Unpinned |
|---|---:|---:|
| Process CPU cores | 0.794 | 0.779 |
| Process IPC | 2.086 | 2.113 |
| Process branch misses / branches | 0.279% | 0.267% |
| Process L1D refills / accesses | 2.159% | 2.142% |
| Process L2 refills / accesses | 3.079% | 2.799% |
| Process instructions / physical GiB | 846 million | 835 million |
| Process L2 refills / physical GiB | 2.04 million | 1.82 million |
| Coordinator CPU cores | 0.295 | 0.280 |
| Coordinator IPC | 1.559 | 1.581 |
| Coordinator migrations / second | 0 | 18.1 |
| Coordinator context switches / second | 3,975 | 3,981 |
| GET p99 across runs | 15.745 ms | 15.712–15.761 ms |
| Logical writes across runs | 0.546–0.547 GiB/s | 0.547 GiB/s |

Pinning did not improve latency or throughput and did not reduce the measured
CPU or cache cost. The small process CPU difference, about 1.8%, favors
unpinned execution. L1 refill ratios are effectively alike; L2 refills per
transferred GiB were about 10.5% lower unpinned. These counters describe the
difference, not a complete causal explanation of it. In particular, pinning
to CPU 0 shares that core with other runtime and kernel work; this is not an
experiment with an exclusively reserved CPU.

Go runtime thread counts stayed constant during each measured phase: 23 and
25 in the pinned runs, 24 in both unpinned runs. There was no progressive thread
growth. Both modes still dedicate an OS thread to the coordinator. All four
benchmarks and captures completed, with no dropped timing samples, invalid
timestamp order, short submissions, or enter errors. A failed intermediate
collector attempt is excluded: it selected a retired warmup scheduler, and
the corrected collector checks scheduler shutdown metadata as well as live TIDs.

## Direction for workload isolation

Keep one ring per DIO scheduler. A caller can use separate schedulers to give
different workloads independent admission queues, descriptor lifetimes,
coordinator CPU placement, and limits. For example, latency-sensitive cache
traffic and a background bulk workload can be routed by workload identity,
with each workload's reads and writes kept together.

This isolates userspace scheduling, not device bandwidth or hardware latency.
Device-level interference still needs measurement and appropriate bounds.
The current blobstore `Store` routes writes by key and reads by segment; it
does not provide workload-identity routing. A workload-routing API or ownership
model should be designed separately, rather than treating the existing
read/write split as equivalent isolation.

## Reproduction and retained evidence

The normal benchmark can reproduce the default-budget topology cases:

```sh
BLOBCACHE_RINGS=8 BLOBCACHE_WRITE_RINGS=1 \
  BLOBCACHE_CACHE_MEMORY_MB=1024 BLOBCACHE_MAX_SEGMENTS=0 \
  GOMAXPROCS=32 go test -run '^$' -bench '^BenchmarkBlobCache$' -benchtime=30000x
```

Independent read/write budget overrides and stage tracing in this report were
diagnostic additions, not production options. Raw logs, timestamp CSVs,
analysis scripts, and exact diagnostic patches are retained locally under
`local/bench-results/2026-10-09-read-budgets/` and
`local/bench-results/2026-10-09-latency-stages/`. These large artifacts are
ignored by Git; the measurements and interpretation above are the repository
record. The earlier full 1/4/8/32-ring budget matrix is under
`local/bench-results/2026-10-09-budget-matrix/`.
The matched affinity, coalescing, and CPU-counter controls are under
`local/bench-results/2026-10-09-ring-isolation/`.
