# Simultaneous read/write capacity and admission budgets

The mixed benchmark's roughly 0.57 GiB/s write rate is not BlobCache's write
ceiling. With independent readers and bounded writers, **one shared ring with
the default 1.5 ms budgets reached 2.42 GiB/s physical reads and 1.14 GiB/s
physical writes simultaneously**. No additional rings were needed.

## Workload and method

Measured 2026-10-09 on the AWS m7gd.8xlarge instance store used in the
[fio baseline](nvme-baseline-m7gd.md). Both cases used the same diagnostic
binary, one shared io_uring, an unpinned dedicated coordinator thread, ring
depth 256, write coalescing, O_DIRECT, 1 GiB value memory, normal 256 MiB
segments, and no disk eviction.

Thirty-two reader goroutines uniformly read a fully written 16 GiB fixture
of 1 MiB records. Memory hits were disabled. Four independent writers appended
distinct keys with 1 MiB values, each waiting for its ticket before allocating
another buffer. At most four writes were outstanding. ErrBusy was retried on
both paths; no reads were silently discarded.

Each case completed 70,000 writes. Readers ran until writers finished; final
read/write draining was timed. Fixture construction and Go calibration were
excluded. The timed buffer fill stands in for downloading into cache memory.
One completed run per setting; this is a capacity experiment, not an
application latency guarantee.

## Throughput and end-to-end latency

Bandwidth is **GiB/s**. Physical rates average complete 30-second monitor
windows; logical rates and end-to-end histograms cover the full measurement.

| Measurement | Budgets 1.5 ms | Budgets off |
|---|---:|---:|
| Physical reads | 2.424 | 2.401 |
| Physical writes | 1.136 | 0.378 |
| Logical writes | 1.130 | 0.376 |
| GET p50 | 13.11 ms | 14.37 ms |
| GET p99 | 13.75 ms | 15.70 ms |
| Write including allocation, fill, and ticket completion p50 | 3.45 ms | 9.39 ms |
| Write including allocation, fill, and ticket completion p99 | 4.33 ms | 19.92 ms |
| Allocation retries / busy reads / cache misses | 0 / 0 / 0 | 0 / 0 / 0 |

Both passed with zero corruption, failed segments, write errors, or busy Put
calls. The budgeted case delivered about 3.82 decimal GB/s combined, consistent
with the fio ceilings. No rejected reads inflated its apparent capacity.

## Queueing and completion

Sampled stages use the same steady-state interval in both cases: seconds 7–47
after measurement starts. Values are **mean / p99 in milliseconds**. Scheduler
wait starts at Submit and ends at kernel handoff. Observed clat ends when
userspace observes the CQE; it includes reaping delay, not just device service.
Phase p99s must not be added.

| Stage | Budgets 1.5 ms | Budgets off |
|---|---:|---:|
| Read scheduler wait | 11.845 / 12.425 | 1.879 / 6.719 |
| Read observed clat | 1.226 / 1.799 | 11.163 / 14.859 |
| Write scheduler wait | 2.046 / 2.590 | 2.586 / 7.049 |
| Write observed clat | 1.286 / 2.641 | 7.610 / 14.550 |
| Write Submit through ticket completion | 3.337 / 4.184 | 10.243 / 17.764 |

With budgets, read ready-queue waiting averaged 11.576 ms. Submit-to-coordinator
acceptance averaged 0.268 ms for reads and 0.217 ms for writes. Read routing
and handle locking averaged about 0.6 microseconds over the full run. Scheduler
wait is elapsed queueing time, not CPU spent executing scheduler code.

At approximately 2,450 one-MiB reads/s, 32 synchronous readers produce roughly
13 ms average turnaround. Little's law applied to the stage means suggests
about **29 reads waiting in DIO and three past kernel handoff**, with roughly
two writes waiting in DIO. These are inferred average occupancies, not limits.

Without budgets, most read waiting moved past kernel handoff, and write
turnaround lengthened. Four write slots at roughly 10 ms per write permit only
about 390 one-MiB writes/s, matching the observed 0.38 GiB/s. Admission protects
write progress as well as read completion latency. This experiment does not
separate block-layer waiting from delayed CQE reaping within observed clat.

## CPU, memory, threads, and batching at full bandwidth

CPU uses sampled `/proc/PID/task` user-plus-system tick deltas (100 Hz), every
two seconds. Within seconds 7–47, the endpoints span 38.10 seconds and no tasks
enter or depart in the budgeted case. This is approximate task CPU, including
benchmark work and charged kernel execution, not all machine/interrupt costs.

| Resource | Budgeted, full combined bandwidth |
|---|---:|
| Go coordinator | 0.185 CPU cores |
| Other Go threads | 0.330 CPU cores |
| Kernel io_uring workers | 0.048 CPU cores |
| Total sampled CPU | **0.563 CPU cores** |
| Median RSS in the sampling window | 1.043 GiB |
| Maximum observed RSS | 1.061 GiB |
| Go OS threads during measurement | 22, constant |
| User SQEs per nonempty kernel handoff | 1.024 |
| Nonempty handoffs containing one user SQE | 98.3% |
| DIO SubmitAndWait calls/s | 3,488 |
| Largest sampled adjacent-write merge | 3 records |

Total sampled CPU is about 1.8% of the 32-core host. The configured memory
budget bounds value mappings; index and runtime metadata are additional.

SQE batching and merging adjacent writes into writev are separate mechanisms.
The SQE counts exclude doorbells and count a merged writev as one operation.
SubmitAndWait calls are not guaranteed syscalls: empty nonblocking calls may
return without entering the kernel. Blocked submission time is not CPU use.

Without budgets, batches averaged 9.78 user SQEs and SubmitAndWait was called
about 282 times/s, but write bandwidth was three times lower with these four
writers. Larger submission batches alone do not establish better throughput.

## Original mixed-worker controls

The default mix selects 30% writes, 30% hot reads, 30% cold read groups, and
10% intentional misses. Each cold group issues four reads: **five data reads
per write** before memory hits. Workers wait for reads before generating their
next write. As the data set grows, more reads reach disk; read saturation then
limits write generation.

Changing `BLOBCACHE_WRITE_PERCENT` to 45 or 50 reached about 1.13 GiB/s writes
without cache or scheduler changes. However, asynchronous writers could retain
hundreds of buffers. Allocation pressure caused reads to return ErrBusy, which
the existing mixed harness counts but skips. The independent control above
avoids this confound. Mixed controls used 32 workers, 70,000 writes, and
100 KB–2 MB values, with all other settings as above.

| Write selections | Budgets | Physical reads GiB/s | Logical writes GiB/s | GET p99 ms | Busy reads |
|---|---|---:|---:|---:|---:|
| 45% | 1.5 ms | 2.342 | 1.133 | 14.43 | 11,962 |
| 45% | Off | 2.423 | 1.089 | 27.67 | 0 |
| 50% | 1.5 ms | 1.771 | 1.133 | 11.58 | 13,411 |
| 50% | Off | 1.972 | 1.133 | 47.35 | 0 |

The last 30-second window of the budgeted 45% case reached 2.38 GiB/s reads
and 1.14 GiB/s writes. Its admitted workload differs from the unbudgeted case,
so do not attribute its entire latency improvement to scheduling. DIO's device
budgets do not replace application backpressure; Submit itself is nonblocking.

## Evidence and reproduction

The ignored local logs, scripts, independent benchmark source, and analyses
were deleted during cleanup on 2026-10-21; this report retains the findings.
Baselines: blobcache `0dd94f2` plus the existing diagnostic recorder, DIO `4b6ca62` / v2.3.0;
`ccade2c` adds documentation. Only a benchmark file was added for the independent
cases. Production code was unchanged and no multi-ring cases were rerun.

The budgeted run retained every trace. The longer budget-off run reached the
recorder's limit, omitting 1,159 final rows. The common seconds 7–47 interval
above was fully recorded in both, without invalid timestamp order or operation
errors. Full-run end-to-end histograms, throughput counters, and per-second SQE
counts are unaffected. All six cases passed; no short submissions or enter
errors occurred.

The existing mixed controls need no source changes:

```sh
BLOBCACHE_WRITE_PERCENT=45 \
  BLOBCACHE_IO_BUDGET=1.5ms BLOBCACHE_CACHE_MEMORY_MB=1024 \
  BLOBCACHE_MAX_SEGMENTS=0 GOMAXPROCS=32 \
  go test -run '^$' -bench '^BenchmarkBlobCache$' -benchtime=70000x
```

The diagnostic runner also disables affinity while preserving LockOSThread;
normal blobstore requests affinity by default. See [ring and affinity
measurements](io-rings.md) for the earlier controls.
