# Raw NVMe baseline: m7gd.8xlarge instance store (2026-11-17)

The ceilings below are what any cache on this device can attain. They were
measured with fio's `io_uring_cmd` engine, which issues NVMe passthrough
commands on the namespace character device: the path dio uses. Raw output is in
[`results/2026-11-17-raw`](results/2026-11-17-raw/); `fio-raw.sh` reproduces it
and `fio-raw-summary.py` tabulates a results directory.

## Setup

- AWS m7gd.8xlarge (Graviton, 32 vCPU), Ubuntu 24.04, kernel 6.8.0-1065-aws,
  fio 3.36.
- Device: "Amazon EC2 NVMe Instance Storage", `/dev/ng1n1` (block interface
  `nvme1n1`), 1.9 TB. 127 entries per hardware queue, one queue per CPU,
  **128 KiB maximum transfer**: every command here is 128 KiB unless stated,
  and dio splits larger requests into 128 KiB commands.
- No NVMe poll queues (`nvme.poll_queues=0`), so polled completion (`hipri`,
  `IORING_SETUP_IOPOLL`) is unavailable.
- Each point: 2 s ramp, 10 s measured, one fio job per side, pinned to a CPU.
  Reads are random over a 16 GiB region written first; writes are random over
  a separate 32 GiB region. Latencies are completion latency in µs. CPU per I/O
  is fio's user + system time; it excludes kernel threads such as SQPOLL's.

## Ceilings

| Workload | MB/s | p50 | p99 | p99.9 | p99.99 |
|---|---|---|---|---|---|
| 4 KiB read, QD1 | 54 | 71 | 82 | 88 | 110 |
| 4 KiB write, QD1 | 172 | 22 | 24 | 28 | 32 |
| 128 KiB read, QD1 | 915 | 136 | 148 | 161 | 189 |
| 128 KiB write, QD1 | 1,293 | 102 | 110 | 114 | 208 |
| write, QD2 | 1,219 | 210 | 222 | 226 | 259 |
| write, QD8 | 1,219 | 856 | 872 | 881 | 1,155 |
| write, QD16 | 1,219 | 1,712 | 1,745 | 2,179 | 11,993 |
| write, QD64 | 1,219 | 6,849 | 6,914 | 6,914 | 12,648 |
| read, QD8 | 2,560 | 403 | 457 | 481 | 823 |
| read, QD32 | 2,560 | 1,630 | 1,679 | 1,712 | 2,212 |
| read, QD128 | 2,560 | 6,521 | 6,521 | 6,586 | 13,304 |

- **Writes cap at 1.22 GB/s and reads at 2.56 GB/s**, regardless of depth.
- **Two writes and four to eight reads in flight reach those caps.** More
  depth only queues: latency grows by about 107 µs per write and 51 µs per read
  in flight.
- **Writes stall occasionally for 10–24 ms** from 16 in flight (p99.99, and
  p99.9 at 32). These tails vary between identical runs: two QD16 write runs
  gave p99.9 of 2.2 and 12.5 ms. Compare write tails only across repeats.

## Reads during writes

Reads at 32 in flight while a writer runs, on a separate CPU (separate
hardware queues) or the same CPU (a shared queue):

| Writes in flight | Read MB/s, separate / same | Read p99.9, separate / same | Write MB/s | Write p99.9 |
|---|---|---|---|---|
| 8 | 2,560 / 2,560 | 2,736 / 2,540 | 1,261 | 9,503 |
| 16 | 2,360 / 2,152 | 4,227 / 4,293 | 1,315 | 11,469 |
| 32 | 2,090 / 2,095 | 2,933 / 3,195 | 1,219 | 3,817 |
| 64 | 2,596 / 2,254 | 3,097 / 3,457 | 1,121 | 40,108 |

- **Both caps hold together at 8 writes in flight: 3.8 GB/s total.** Deeper
  write queues take 8–18% from reads and push the write tail to 40 ms.
- **A shared hardware queue does not starve reads on the passthrough path**
  (on XFS in October it cut them to 0.2 GB/s). Two jobs on one CPU cost twice
  the CPU per I/O, from CPU contention rather than the queue.

## Findings for dio and blobcache

| 128 KiB commands, one ring | IOPS | p99.9 | p99.99 | CPU µs/IO |
|---|---|---|---|---|
| fio write, QD16 | 9,299 | 12,517 | 23,462 | 9.4 |
| fio write, registered buffers (`fixedbufs`) | 9,299 | 9,372 | 22,413 | 7.0 |
| dio write, completion callbacks, no in-flight budget | 9,301 | 1,769 | 11,925 | 10.0 |
| dio write, completion callbacks, default 1.5 ms budget | 9,301 | 1,752 | 1,821 | 10.0 |
| dio write, one goroutine per request waiting on its ticket | 9,299 | 1,929 | 2,055 | 35.4 |
| fio read, QD32 | 20,157 | 1,712 | 2,179 | 8.1 |
| fio read, registered buffers | 20,028 | 1,712 | 2,179 | 4.8 |
| dio read, completion callbacks | 19,736 | 1,802 | 2,172 | 10.8 |
| dio read, one goroutine per request | 19,531 | 1,822 | 1,914 | 27.0 |

(dio rows: `dio/cmd/nvme-bench`, same range and depths.)

1. **dio's own overhead is small**: through completion callbacks it costs
   0.6–2.7 µs per command more than fio. Kernel time in `io_uring_enter` is
   mostly inherent to passthrough commands.
2. **A goroutine waiting on each ticket costs about 3× the CPU** (3–4 context
   switches per I/O).
3. **Registered buffers cut kernel CPU 25% for writes and 40% for reads** in
   fio, and dio's total CPU per command by about 20% (writes 8.2 → 6.7 µs,
   reads 7.8 → 6.1 µs, two runs each; `nvme-bench -registered`), with unchanged
   throughput and latency. iosched uses a registered buffer for any NVMe
   command whose data lies inside one registration entry. **Linux 6.8 rejects
   commands in large entries with EINVAL**: `io_import_fixed` counts every page
   to the end of the entry, and `blk_rq_map_user_bvec` refuses more pages than
   the queue's segment limit (33 here). Linux 6.15 counts only the pages the
   command covers. On 6.8, entries must be at most 128 KiB, and a ring holds at
   most 16,384 entries, so at most 2 GiB per ring can be registered.
4. **SQPOLL does not help.** Unpinned, it kept throughput but made the write
   p99.9 about 7× worse (11.9 ms), and its kernel thread plus the reaper spin
   one to two cores. Pinned to the submitter's CPU, it collapsed throughput to
   350 MB/s for writes and 700 MB/s for reads.
5. **The in-flight write budget may fix the write tail**: in one run each,
   dio's p99.99 dropped from 11.9 ms to 1.8 ms at the same throughput. Given
   the run-to-run variance above, this needs repeats. The device needs only two
   writes in flight for full bandwidth, while the default 1.5 ms budget admits
   about 14; a smaller budget may cut write queueing further (not measured).
6. **Time inside `io_uring_enter` is about 4–5 µs per 128 KiB command for
   both fio and dio**: submission is about 30% of process CPU, user-buffer
   mapping about 15% of it (what registered buffers remove), and waiting for
   completions 11–20% (perf profiles at 499 Hz; proportions only, see 8).
   dio spends less kernel time per command than fio: 5.5–5.9 µs against 7.0 µs
   of system time per write.
7. **`IORING_SETUP_COOP_TASKRUN` changes nothing** for dio, in CPU or latency
   (two rounds each, reads and writes). fio sets it, together with
   `SINGLE_ISSUER | DEFER_TASKRUN` and a registered ring descriptor; dio uses
   none of them. `DEFER_TASKRUN` is untested: it needs the coordinator pinned
   to one OS thread.
8. **Per-process `perf record` or `perf stat` with hardware events inflates
   CPU per command 2.5–5× on this host**, even at 499 Hz (fio writes: 9.6 →
   25 µs), at every context switch; system-wide `-a` does not. See
   [Measuring CPU on this host](../benchmarks.md#measuring-cpu-on-this-host-2026-11-17).
9. **Waiting goroutines, not submission, drive kernel time** in the
   one-goroutine-per-request pattern: system time rises from 5.6 to 22 µs per
   command, and context switches from 1.3 to 4.3.

