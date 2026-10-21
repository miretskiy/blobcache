# Small-request IOPS, rings, and CPU affinity

Four shared rings increase 4 KiB blobcache disk reads from **248,000 to
387,000/s**, with GET p99 falling from 0.73 to 0.61 ms and process CPU rising
from 2.9 to 5.4 cores. CPU pinning shows no consistent benefit. At 16–64 KiB,
one ring already saturates bandwidth. A follow-up with more outstanding reads
reaches 497k raw DIO reads/s with four rings and 512k with eight; see the
[16/32-ring extension](#extension-16-and-32-rings-and-more-outstanding-reads).

The earlier conclusion about one ring applies to the large-record mixed
workload. Small random reads can exhaust one coordinator's CPU capacity first.
This experiment tests shared rings, rather than dedicated read/write rings.

## Method

Measured 2026-10-09 on the same m7gd.8xlarge, Linux/arm64, XFS instance store
as the [fio baseline](nvme-baseline-m7gd.md). DIO is v2.3.0; blobcache is
`ccade2c`, built with Go 1.26.5 and diagnostic-only instrumentation in isolated
copies.

- Random reads at actual disk request sizes of 4, 8, 16, and 64 KiB.
- 128 reader goroutines **total**, independent of ring count, each waiting for
  its request before issuing another. This tests a fixed concurrency level,
  not the maximum IOPS obtainable with every possible queue depth.
- One, four, or eight shared schedulers. No dedicated write rings. Raw DIO
  assigns workers to rings; blobcache uses its normal segment-based routing.
- Ring depth 256 each; default 1.5 ms admission budgets, with bandwidth and
  IOPS shares divided across rings. GOMAXPROCS=32 throughout.
- Every coordinator uses `LockOSThread`. Pinned cases request CPUs 2 onward;
  unpinned cases use `WithCoordinatorCPU(-1)`. Actual thread masks and sampled
  executing CPUs are checked.
- Five seconds of warmup followed by 40 seconds measured, including draining
  the last reads. Fixture initialization and opening are outside measurement.
- Raw DIO reads an initialized 8 GiB file with O_DIRECT, reusing one aligned
  buffer per worker. It excludes cache indexing, allocation, and verification
  overhead; its timing covers Submit through ticket completion.
- The blobcache confirmation reads 262,144 records from 16 MiB segments, with
  1 GiB memory and memory hits disabled. Each record is 4 KiB including framing.
  This includes the real index, single-flight, allocator, checksum, and callback
  paths. GET timing excludes generating the requested key.

End-to-end latency samples one request in 64 per worker. The bounded DIO
recorder samples one Submit in 512 and records batching on every handoff.
Separate controls disable the recorder in the same binary. Scheduler latency
is Submit to first kernel handoff; observed clat is handoff to userspace CQE
observation, including reaping delay. Phase percentiles must not be added.

Each case captures iostat, vmstat, process/thread counters, and three consecutive
10-second process `perf stat` windows during steady state: cycles/instructions,
branches/branch misses, and L2 accesses/refills. CPU cores are process CPU
seconds divided by elapsed seconds; they are not whole-machine CPU usage.
PMU counts per operation use NVMe completed operations during each window.

## Raw DIO results with recording enabled

Throughput is reads/s; p99 is Submit through ticket return, in microseconds.
All rows below are unpinned.

| Request | Rings | Reads/s | Process CPU cores | CPU µs/read | Read p99, µs |
|---|---:|---:|---:|---:|---:|
| 4 KiB | 1 | 256,463 | 1.79 | 6.98 | 696 |
| 4 KiB | 4 | 398,902 | 3.96 | 9.92 | 552 |
| 4 KiB | 8 | 406,928 | 6.24 | 15.34 | 591 |
| 8 KiB | 1 | 223,859 | 1.69 | 7.55 | 786 |
| 8 KiB | 4 | 312,500 | 3.80 | 12.17 | 628 |
| 16 KiB | 1 | 156,250 | 1.79 | 11.48 | 953 |
| 16 KiB | 4 | 156,250 | 2.78 | 17.81 | 1107 |
| 64 KiB | 1 | 39,062 | 1.38 | 35.39 | 3389 |
| 64 KiB | 4 | 39,062 | 2.09 | 53.54 | 3977 |

Four rings increase 4 KiB IOPS by 56% and 8 KiB IOPS by 40%. Eight rings add
only 2% over four at 4 KiB, while using 58% more CPU. At 16 and 64 KiB, one
ring already reaches 2.56 decimal GB/s; more rings consume more CPU and worsen
p99 without increasing throughput.

### Where the 4 KiB time goes

Mean / p99 in microseconds, with recording enabled and no affinity:

| Rings | Scheduler wait | Observed clat | User SQEs per nonempty handoff |
|---|---:|---:|---:|
| 1 | 118.0 / 244.9 | 355.2 / 490.3 | 42.69 |
| 4 | 46.3 / 192.9 | 251.7 / 404.3 | 6.73 |
| 8 | 21.3 / 55.3 | 268.0 / 440.0 | 3.02 |

No sampled 4 KiB or 8 KiB read encountered an admission-budget rejection.
The single coordinator consumed approximately a full CPU; most scheduler
waiting preceded coordinator acceptance, rather than waiting for a budget.
Additional coordinators reduce that delay despite smaller batches. This is
evidence of a CPU/concurrency limit in the single-coordinator path, not a
read/write prioritization effect or proof that a hardware queue itself is slow.

Eight rings reduce scheduler waiting further but increase observed clat and
CPU cost. This experiment holds total outstanding work at 128; it does not
establish the maximum possible IOPS with a deeper workload.

## Blobcache confirmation with recording enabled

These are disk-backed GETs of 4 KiB records. The smaller fixture and different
allocation path mean raw DIO and blobcache are separate comparisons.

| Rings | Affinity | GET/s | CPU cores | GET p99, µs |
|---|---|---:|---:|---:|
| 1 | Unrestricted | 245,939 | 2.89 | 721 |
| 1 | Pinned | 245,445 | 2.87 | 720 |
| 4 | Unrestricted | 385,642 | 5.45 | 605 |
| 4 | Pinned | 385,898 | 5.44 | 622 |

The 57% IOPS gain survives the full cache path. Four rings use about 89% more
CPU; CPU cost per GET rises from 11.8 to 14.1 microseconds. All four cases
report zero memory hits, misses, corrupt records, and busy-read retries. Value
memory stays at the 1 GiB limit; sampled peak process RSS is 1.13–1.14 GiB.

## Affinity and hardware counters

Pinning did not improve throughput in either layer. Single-ring raw DIO
repeats reached 254,172 reads/s unrestricted and 252,534 pinned, consistent
with the first pair. Four-ring blobcache throughput differs by less than 0.1%
between affinity settings. Small p99 and counter differences do not establish
a repeatable affinity benefit.

Raw 4 KiB process counters, normalized per completed NVMe operation:

| Rings | Affinity | Instructions/op | Cycles/op | Branch misses/op | L2 refills/op |
|---|---|---:|---:|---:|---:|
| 1 | Unrestricted | 24,279 | 13,484 | 26.4 | 76.6 |
| 1 | Pinned | 24,299 | 13,701 | 26.5 | 79.0 |
| 4 | Unrestricted | 24,778 | 15,747 | 41.4 | 103.1 |
| 4 | Pinned | 24,735 | 15,787 | 41.5 | 104.2 |
| 8 | Unrestricted | 29,480 | 20,792 | 65.0 | 135.0 |
| 8 | Pinned | 29,067 | 20,421 | 64.7 | 136.1 |

More rings increase cycles, branch misses, and L2 refills per I/O in these
measurements. They buy throughput with parallel CPU work; they are not an
efficiency improvement per request. L2 refill events are not measurements of
cache-line bouncing.

Affinity does control placement. The pinned single ring sends essentially all
NVMe completion interrupts through `nvme1q3`; four pinned rings spread them
almost evenly across `nvme1q3`–`nvme1q6`. The unrestricted four-ring run spreads
interrupts across all 32 queues. Interrupt counts are a placement proxy, not
per-queue I/O counts.

Go runtime OS-thread counts remain bounded. The first raw 4 KiB cases have
36 threads with one ring, 39–40 with four, and 43–44 with eight; blobcache has
37 with one and 40–41 with four. These counts exclude kernel io-workers.
Both affinity settings keep the coordinator on a dedicated OS thread, so this
compares affinity, not `LockOSThread` against an unlocked coordinator.

## Controls with DIO recording disabled

The same binaries, workload, end-to-end sampling, and external monitors are
used; only the DIO request recorder and handoff counters are disabled.
All requests below are 4 KiB.

| Layer | Rings | Affinity | Reads/s | CPU cores | End-to-end p99, µs |
|---|---:|---|---:|---:|---:|
| DIO | 1 | Unrestricted | 255,015 | 1.79 | 714 |
| DIO | 1 | Pinned | 253,776 | 1.78 | 712 |
| DIO | 4 | Unrestricted | 397,889 | 3.95 | 601 |
| DIO | 8 | Unrestricted | 403,757 | 6.18 | 663 |
| blobcache | 1 | Unrestricted | 248,494 | 2.89 | 727 |
| blobcache | 4 | Unrestricted | 386,693 | 5.38 | 613 |

The throughput result survives disabling recording: four rings improve raw
DIO by 56% and blobcache by 56%. Eight raw rings add just 1.5% over four, with
56% more CPU. Tail latency varies more between runs than throughput, but four
rings improve the observed p99 in both recording settings.

All **28 cases passed**. All 84 PMU windows are fully inside measured steady
state and report 100% counter coverage. Recorded traces have no dropped rows,
short submissions, enter errors, operation errors, or timestamp-order failures.
All six blobcache cases have zero misses, corruption, and busy-read retries.
All runners have exited.

## Extension: 16 and 32 rings, and more outstanding reads

A follow-up on the same host used 4 KiB reads, the same fixture and workload,
and a fresh eight-ring control. These rows disable DIO recording. Depth is
**total concurrent readers**, not depth per ring. GOMAXPROCS remains 32 unless
shown otherwise.

| Rings | Depth | Reads/s | CPU cores | End-to-end p99, µs | Go OS threads |
|---|---:|---:|---:|---:|---:|
| 8 | 128 | 404,567 | 6.16 | 657 | 43–44 |
| 16 | 128 | 383,673 | 8.92 | 766 | 52 |
| 32 | 128 | 164,676 | 12.45 | 1,259 | 68 |
| 1 | 512 | 266,224 | 1.65 | 2,427 | 37 |
| 4 | 256 | 486,871 | 4.32 | 849 | 40 |
| 4 | 512 | 496,919 | 4.43 | 1,405 | 40 |
| 8 | 512 | 511,978 | 5.30 | 1,510 | 44 |
| 16 | 512 | 504,853 | 5.59 | 1,635 | 52 |
| 32 | 512 | 166,746 | 12.42 | 207,023 | 68 |
| 32, GOMAXPROCS=64 | 512 | 497,631 | 9.47 | 2,006 | 94 |

Four rings reach about 97% of the observed 512k ceiling at depth 512; eight
reach the ceiling. Increasing four-ring depth from 256 to 512 adds only 2%
throughput while increasing p99 by 65%. The earlier depth-128 experiment did
not find the maximum small-request IOPS. Sixteen rings provide no gain over
eight. A measured 512k does not contradict the model's rounded 500k parameter:
the budget limits concurrent work, rather than rate-limiting completions.

The 32-ring slowdown is strongly dependent on Go scheduling configuration.
Doubling GOMAXPROCS on the same 32 physical CPUs restores approximately 498k
IOPS and removes the enormous tail, but uses over twice the CPU of four rings.
This implicates interaction between the 32 locked coordinators and Go execution
slots; it does not establish the exact runtime mechanism or a fundamental
NVMe queue-count limit. Thread counts stabilize; this is not an observed
unbounded thread leak. At depth 128 the 32-ring p99.9 was 105 ms unpinned and
114 ms pinned, despite a much smaller p99.

Pinning remains unhelpful: at depth 512, 16 rings deliver 505k unpinned versus
506k pinned; 32 rings deliver 167k versus 159k with GOMAXPROCS=32. The 32-ring
pinned case uses every allowed CPU exactly once, wrapping the CPU list instead
of requesting nonexistent CPUs 32 and 33.

Recorded controls at depth 512 show 7.75 user SQEs per handoff for eight rings,
6.82 for sixteen, and just 1.02 for thirty-two at GOMAXPROCS=32. No sampled read
was blocked by an admission budget. The slow 32-ring run's recorded scheduler
and observed completion intervals do not account for its end-to-end outliers;
those intervals end before the waiting caller resumes, and the two samplers
are distinct. Do not label its entire latency increase device clat.

Process PMU counters per completed device operation, recording disabled:

| Rings / configuration | Instructions/op | Cycles/op | Branch misses/op | L2 refills/op |
|---|---:|---:|---:|---:|
| 4 / depth 512 | 23,533 | 15,167 | 39.0 | 119.3 |
| 8 / depth 512 | 22,658 | 15,380 | 43.0 | 118.3 |
| 16 / depth 512 | 23,353 | 16,001 | 49.4 | 128.2 |
| 32 / depth 512 | 52,816 | 55,033 | 183.7 | 774.2 |
| 32 / depth 512 / GOMAXPROCS=64 | 32,304 | 25,080 | 92.3 | 250.0 |

These are raw DIO results, not additional full-blobcache measurements. All
**17 additional cases passed**, bringing the total to **45 cases and 135 PMU
windows**, with 100% counter coverage and no trace errors, dropped rows, or
short submissions. No production implementation changed.

## Reproduction artifacts

The isolated workload and runner used `/private/tmp/blobcache-small-io`.
The ignored local source patches, scripts, configuration, raw counters, and
analysis were deleted during cleanup on 2026-10-21; this report retains the
findings. The experiments used DIO v2.3.0 and blobcache `ccade2c`.
At the time of the experiments, the Linux copy was under the authorized test
directory's `small-io/` subtree.
Production implementation files are unchanged.
