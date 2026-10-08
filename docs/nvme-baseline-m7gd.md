# Instance-storage baseline: m7gd.8xlarge (2026-10-03)

What the local NVMe of the benchmark box sustains for reads and writes, alone
and together, measured with fio as the baseline for BlobCache benchmarks. All
XFS numbers below come from one session (2026-10-03, full-volume XFS), at two
request-size distributions. An earlier round on btrfs is kept at the end
because it explains earlier decisions; its mixed results are superseded.

## Setup

- AWS m7gd.8xlarge (Graviton, 32 vCPU, 123 GB RAM), Ubuntu 24.04, kernel
  6.8.0-1065-aws.
- Device: `nvme1n1`, 1.9 TB (1769 GiB) "Amazon EC2 NVMe Instance Storage"
  (Nitro), block scheduler `none`. From `nvme show-regs` / `get-feature`:
  **128 entries per queue** (Linux uses 127), **128 KiB maximum transfer** (a
  1 MiB I/O is 8 device commands), 64 queues offered, 32 used — **one hardware
  queue per CPU** (`/sys/block/nvme1n1/mq/*/cpu_list`: queue *i* serves CPU *i*).
- Filesystem: **XFS** (default `mkfs.xfs`, mounted `noatime`) on the whole
  1641 GiB LVM volume `vg_ws_ephemeral-volume1p1`; the rest of the disk
  (128 GiB) is the VM's swap. The workspace's btrfs, which the outer system
  keeps mounted, was migrated online onto a loop file to free the volume.
  No dedup daemon running.
- fio 3.36, `--ioengine=io_uring --direct=1`. Random reads over 16 × 8 GiB
  files; sequential writes into freshly `fallocate`d files (deleted before every
  run, so each run writes preallocated space, as BlobCache does),
  `--refill_buffers`. 5 s ramp, 30 s measured per point.
- Request sizes:
  - **1 MiB** fixed (`--bs=1M`);
  - **mixed** (`--bssplit=2m/10:1m/55:512k/15:256k/10:128k/5:64k/5`, weights
    by request count): median 1 MiB, mean ~880 KiB, 35% of requests smaller
    than 1 MiB. At the same `iodepth`, fewer bytes are in flight than at 1 MiB.

## 1. Single-workload ceilings

| Workload | 1 MiB | mixed |
|---|---|---|
| Writes, 4 jobs × 4 files, depth 64 | 1.22 GB/s (p50 219 ms) | 1.22 GB/s (p50 188 ms) |
| Writes, 4 jobs × **1 shared file**, depth 64 | 1.22 GB/s | 1.22 GB/s |
| Writes, **1 job × 1 file**, depth 64 | 1.22 GB/s (p50 53 ms) | 1.22 GB/s (p50 46 ms) |
| Random reads, 16 jobs × depth 8 (128 in flight) | 2.56 GB/s (p50 51 ms) | 2.56 GB/s (p50 43 ms) |

Writes top out at **1.22 GB/s**, reads at **2.56 GB/s**, regardless of the
size distribution. A single file, even from a single job, reaches the write
ceiling.

## 2. Mixed reads and writes

Writes unthrottled (4 jobs × depth 64 = 256 in flight); reads unthrottled at a
fixed number in flight (16 jobs):

| Reads in flight | 1 MiB: read / write | mixed: read / write |
|---|---|---|
| 32 | 0.63 / 1.22 GB/s (read p50 31 ms) | 0.77 / 1.22 GB/s (read p50 24 ms) |
| 128 | 1.05 / 1.22 GB/s (117 ms) | 1.05 / 1.22 GB/s (101 ms) |
| 512 | 1.79 / 1.22 GB/s (292 ms) | 1.73 / 1.22 GB/s (251 ms) |

Writes keep their full bandwidth at every read depth; reads get what is left.

## 3. Hardware-queue sharing and device depth

Mixed runs with jobs pinned to CPUs (hence to hardware queues), 128 reads and
256 writes in flight, with iostat during each run. 1 MiB (mixed in brackets
where it differs):

| Readers / writers | Read | Write | Device r_await | Device w_await | Commands at device |
|---|---|---|---|---|---|
| 1 / 1, **same CPU** | 0.21 (0.18) GB/s | 1.23 GB/s | 0.9 ms | 12.9 ms | 121 |
| 1 / 1, **different CPUs** | **2.56 GB/s** | **1.26 (1.25) GB/s** | 6.3 ms | 12.7 ms | 245 |
| 4 / 4, same CPUs | 0.23 (0.20) | 1.22 | 19.3 | 49.5 | 494 |
| 4 / 4, disjoint CPUs | 1.24 (1.25) | 1.22 | 52.4 | 51.9 | 978 |
| 4 / 4, unpinned | 0.48 (0.42) | 1.22 | 151 (169) | 147 (143) | 1896 |

The same split without pinning (`fio-xfs.sh`): 4 readers / 4 writers 0.45
(0.43) + 1.22 GB/s; 1 reader / 1 writer 2.56 + 1.26 GB/s.

1. **Readers sharing a hardware queue with a deep writer starve before the
   device.** One reader and one writer on the same CPU: reads get ~0.2 GB/s,
   yet the reads that reach the device complete in under 1 ms; the rest wait
   for one of the queue's 127 entries, which the writer holds (fio read p50
   650 ms). The submitting thread sleeps in the kernel until an entry frees
   (`blk_mq_get_tag`): iomap direct I/O does not issue requests with
   `REQ_NOWAIT` (`fs/iomap/direct-io.c`, v6.8), so io_uring's non-blocking
   first attempt does not cover the block layer.
2. **Separate queues at modest depth reach both ceilings**: 2.56 + 1.26 =
   3.82 GB/s with about 245 commands at the device.
3. **Deep device queues cost reads, not writes.** At ~980 and ~1900 commands
   outstanding, device read latency rises to 52 and 150–170 ms and reads fall
   to ~1.25 and ~0.45 GB/s; writes stay at 1.22 GB/s.
4. **The size distribution does not change any of this.** Mixed sizes give the
   same bandwidths within a few percent; latencies are lower roughly in
   proportion to the smaller mean request.

## 4. One CPU: how dio submits

dio submits all I/O from one coordinator thread, so at any moment its reads
and writes go through the hardware queue of the CPU it runs on. These runs pin
everything to CPU 0 (script `fio-onecpu.sh`), with submission-latency
percentiles enabled. Depths are requests in flight; every request is 1 MiB
(8 device commands) unless marked mixed. Latencies in ms.

**A. A reader job and a writer job on CPU 0**

| Writes / reads in flight | Read | Read clat p50 / p99 | Write | Write clat p50 / p99 | Total | Device r / w wait |
|---|---|---|---|---|---|---|
| 2 / 16 | 2.56 GB/s | 6.2 / 6.9 | 1.25 GB/s | 1.5 / 2.6 | **3.81** | 6.0 / 0.7 |
| 8 / 16 | 2.56 | 6.1 / 7.5 | 1.22 | 6.7 / 9.2 | **3.78** | 3.7 / 5.6 |
| 2 / 128 | 2.56 | 52 / 53 | 1.22 | 1.3 / 4.4 | **3.79** | 6.1 / 0.5 |
| 8 / 128 | 2.56 | 52 / 54 | 1.23 | 6.7 / 9.2 | **3.79** | 3.7 / 5.5 |
| 8 / 128, mixed sizes | 2.56 | 45 / 51 | 1.24 | 5.4 / 9.8 | **3.81** | 4.1 / 4.5 |
| 16 / 128 | 1.94 | 69 / 81 | 1.22 | 13 / 16 | 3.16 | 0.6 / 12.3 |
| 24 / 128 | 0.49 | 275 / 384 | 1.22 | 22 / 39 | 1.71 | 0.6 / 13.0 |
| 32 / 16 | 0.34 | 46 / 102 | 1.22 | 26 / 48 | 1.56 | 0.6 / 13.0 |
| 32 / 128 | 0.33 | 401 / 600 | 1.22 | 26 / 47 | 1.55 | 0.6 / 13.0 |

Submission latency (slat) p99 stays at 1–3 ms up to 16 writes in flight and
rises to 7–20 ms at 24–32, when submitters block waiting for queue entries.

**B. One job doing both (random reads, sequential writes) on CPU 0**

| Mix, depth | Read | Read clat p50 / p99 | Write | Write clat p50 / p99 | Total |
|---|---|---|---|---|---|
| 70% reads, 16 | 2.59 GB/s | 6.1 / 7.4 | 1.11 GB/s | 0.5 / 12.4 | 3.71 |
| 70% reads, 64 | 2.56 | 19.5 / 22.7 | 1.10 | 14.6 / 19.0 | 3.66 |
| 70% reads, 256 | 2.57 | 73.9 / 81.3 | 1.11 | 69.7 / 79.2 | 3.67 |
| 50% reads, 64 | 1.22 | 19.0 / 28.4 | 1.22 | 34.3 / 51.6 | 2.44 |

(This job overwrites a laid-out file, so its writes do not land in fresh
preallocated space.)

What this shows:

1. **One hardware queue carries the device's full combined ceiling, ~3.8
   GB/s, while writes in flight stay at or below ~8 MiB** (64 commands, half
   the queue's 127 entries).
2. **Neither side needs depth**: two 1 MiB writes in flight reach 1.25 GB/s,
   spending under 1 ms in the device, and 16 MiB of reads (the smallest read
   depth tested here) already reach 2.56 GB/s; section 5 shows ~1–2 requests
   of each suffice. More depth adds only latency (Little's law: 128 MiB ÷
   2.56 GB/s ≈ 50 ms).
3. **Past ~8 MiB of writes, a writer can fill the queue by itself**: at
   16 MiB (128 commands) reads lose a quarter; at 24–32 MiB they collapse to
   0.3–0.5 GB/s. The reads that get in still finish in 0.6 ms at the device;
   their 275–600 ms clat is time waiting for queue entries held by writes.
4. **A single submitter that interleaves reads and writes (B) never
   collapses**: the split follows the submission mix, and whichever side
   reaches its ceiling sets the total. dio is a single submitter too, but in
   arrival order, so a burst of queued writes is submitted ahead of reads —
   case A at high write depth.

## 5. Unloaded latency and the depth needed (Little's law)

One request in flight (QD1), on CPU 0, 5 s ramp / 20 s measured (script
`fio-latency.sh`). Latency in ms: mean (p99).

| Request size | Read alone | Write alone | Read, with a writer running | Write, with a reader running |
|---|---|---|---|---|
| 4 KiB | 0.078 (0.082) | 0.032 (0.036) | 0.092 (0.123) | 0.051 (0.068) |
| 128 KiB | 0.145 (0.146) | 0.096 (0.101) | 0.202 (0.412) | 0.125 (0.157) |
| 1 MiB | 0.460 (0.481) | 0.751 (0.758) | 0.676 (0.954) | 0.745 (0.823) |
| mixed (avg 880 KiB) | 0.444 (0.831) | 0.645 (1.499) | 0.632 (1.139) | 0.642 (1.516) |

Throughput at QD1: 1 MiB reads 2.28 GB/s, 1 MiB writes 1.22 GB/s; 128 KiB
writes 1.20 GB/s; 4 KiB reads 12.8k IOPS, writes 30.2k IOPS.

Depth needed to reach the ceilings, by Little's law (ceiling × mean latency ÷
request size):

| Request size | Reads (2.56 GB/s) | Writes (1.22 GB/s) |
|---|---|---|
| 1 MiB, alone | 1.1 requests (1.1 MiB) | 0.9 (one request) |
| 1 MiB, both at once | 1.7 requests | 0.9 |
| mixed | 1.3 requests (1.1 MiB) | 0.9 |
| 128 KiB | 2.8 requests (0.4 MiB) | 0.9 |

1. **The device is bandwidth-limited, not latency-limited**: ~2 MiB of reads
   and ~1 MiB of writes in flight reach both ceilings. Any further depth only
   queues, adding depth ÷ bandwidth of latency.
2. **Writes behave like a fixed bandwidth cap**: even 128 KiB writes reach
   1.20 GB/s at one request in flight, and write latency grows in proportion
   to size (~1.4 GB/s at the margin) — consistent with a provisioned
   write-throughput limit (not documented that we found).
3. **Writes slow concurrent reads even at QD1**: one concurrent write raises
   1 MiB read latency from 0.46 to 0.68 ms mean and roughly doubles its p99;
   reads barely affect writes.

## 6. Disk model: per-class IOPS and bandwidth (2026-10-06)

The numbers an in-flight budget needs, measured by
[`fio/fio-model.sh`](fio/fio-model.sh): 256 requests in flight (8 jobs ×
depth 32); random reads over 8 GiB files; sequential writes into freshly
fallocated files, as BlobCache writes.

| Request | Reads: IOPS, MB/s, clat p50 | Writes: IOPS, MB/s, clat p50 |
|---|---|---|
| 512 B | 502k, 257, 0.51 ms | 133k, 68, 2.0 ms |
| 4 KiB | **500k**, 2047, 0.51 ms | **83k**, 339, 3.2 ms |
| 16 KiB | 156k, 2560, 1.6 ms | 77k, 1256, 3.4 ms |
| 64 KiB | 39k, 2560, 6.5 ms | 18.6k, 1219, 13.7 ms |
| 1 MiB | 2.4k, **2567**, 102 ms | 1.2k, **1219**, 246 ms |

- **Model inputs:** reads 500k IOPS and 2.56 GB/s; writes 83k IOPS and
  1.22 GB/s.
- **Writes into fresh preallocated space are limited to ~80k operations/s**
  regardless of size until bandwidth binds; overwriting already-written blocks
  reached 269k 4 KiB IOPS (same shape, earlier run). The likely cause is XFS
  converting unwritten extents on each write's completion (not verified with
  tracing). BlobCache's ~1 MiB writes (~1.2k/s) are far from this limit.
- **Sector size:** 512 B reads match 4 KiB reads in IOPS, so nothing points to
  a hidden 4 KiB physical sector for reads; BlobCache issues only 4 KiB-aligned
  I/O either way.
- **IOPS and bandwidth are independent limits, not one shared budget.** At
  16 KiB, reads reached 156k IOPS = min(500k, 2.56 GB/s ÷ 16 KiB), and writes
  77k ≈ min(83k, 74k). A shared budget (cost = 1/IOPS + bytes/bandwidth, the
  form of Seastar's model) predicts 119k and 39k. So an operation costs
  **max(bytes ÷ bandwidth, 1 ÷ IOPS)** of its class's time.
- **Reads and writes are independent classes:** at once, 1 MiB reads reached
  2.56 GB/s and writes 1.27 GB/s (as in section 3). A combined budget would
  leave half the device unused.
- **Depth only adds latency past the knee** (1 MiB reads at 256 in flight:
  102 ms p50). By Little's law a class's in-flight cost equals the latency it
  adds, so a budget of L seconds of cost per class keeps device queueing near
  L: at L = 1 ms, about 2.5 MB of reads and 1.2 MB of writes in flight, in line
  with section 5.

**Latency goal from a depth sweep** (1 MiB, one job; dio's
`scripts/disk-model.sh`, 2026-10-06): reads reach 2.32 GB/s at depth 1 and
the full 2.56 GB/s at depth 2 (0.82 ms of read time in flight); writes reach
1.22 GB/s at depth 1 (0.86 ms). Deeper only adds latency (depth 8: reads
3.2 ms, writes 6.8 ms p50). So a 1 ms goal per class admits exactly the depth
each class needs. A rerun measured 74k 4 KiB write IOPS (83k before): small
run-to-run drift, irrelevant to large writes.

**Redoing this on another disk:** run dio's `scripts/disk-model.sh DIR` (or
`fio/fio-model.sh` here, adjusting `root`); take
the 4 KiB IOPS and 1 MiB MB/s for each class; check that the 16 KiB result
matches the independent-limits prediction and that check 4 reaches both
ceilings at once. If either fails, the device shares a budget and the cost
should be the sum, or reads and writes should share one budget.

## Conclusions

1. Ceilings: 1.22 GB/s writes and 2.56 GB/s reads; together up to ~3.8 GB/s,
   even through a single hardware queue, as long as writes in flight stay at
   or below ~8 MiB. About 2 MiB of reads and 1 MiB of writes in flight reach
   the ceilings; beyond that, depth only adds latency.
2. A hardware queue holds 127 commands, 16 MiB of 128 KiB commands. A thread
   submitting deep writes fills its CPU's queue and blocks in the kernel; reads
   submitted through the same queue wait behind them.
3. Deep outstanding I/O at the device lowers read throughput and leaves writes
   untouched.

## Implications for BlobCache

- dio submits all I/O from one coordinator thread. At any moment its
  submissions land in the queue of the CPU it runs on, so BlobCache's reads
  and writes share a queue — the "same CPU" case. With writes deep enough to
  fill that queue, reads wait behind them.
- Writes saturate with ~1 MiB in flight and reads with ~2 MiB; more than
  ~8 MiB of writes starves reads sharing the queue. The default write memory limit (256 MiB) allows 32× that;
  write memory in flight beyond a few MiB only queues. A cap on write I/O in
  flight, separate from write memory, would keep a single queue at both
  ceilings. (Not implemented.)

## Appendix: the btrfs round (2026-10-02, superseded)

The instance storage was first btrfs (`compress=zstd:1`, on LVM, with the
bees dedup daemon running); 1 MiB requests, 10 s ramp / 60 s per point.

- Ceilings: writes 1.20 GB/s (4 jobs), reads 2.41 GB/s; one file reached only
  0.87 GB/s (1 job, depth 32, measured in a copy-on-write directory; never
  measured with `nodatacow`).
- **Copy-on-write stalls.** With btrfs's default copy-on-write and data
  checksums, O_DIRECT writes under heavy read load stalled for 17–70 seconds
  (p99 17 s). Writing into a `nodatacow` directory (`chattr +C`) removed the
  stalls (p99 558 ms). On btrfs, mount with `nodatacow`; BlobCache does not set
  it. XFS has no data copy-on-write.
- Mixed runs (16 reader jobs against 4 writer jobs, unpinned, `nodatacow`
  write files) found reads and writes adding up to 3.84 GB/s at 128 reads in
  flight and deeper reads starving writes (0.21 GB/s at 512). The same job
  shapes on XFS did not reproduce this, and why btrfs behaved differently is
  not established. The conclusion drawn then — "the device favors reads" —
  does not hold.

## Reproducing

Scripts are in [`fio/`](fio/) (see its README): `fio-all.sh` runs
`fio-xfs.sh` (ceilings, mixed sweep, submitter counts) and `fio-pin.sh` (CPU
pinning with iostat) at `BS=1M` and the mixed `--bssplit`; `BS` takes one size
or a `--bssplit` list. `fio-onecpu.sh` and
`fio-onecpu-edge.sh` hold the one-CPU runs (section 4); `fio-latency.sh` the
QD1 latencies (section 5). The core shapes:

```sh
reader="--name=reader --directory=$READ_DIR --rw=randread --norandommap --randrepeat=0 --size=8G --numjobs=$N --iodepth=$D"
writer="--name=writer --new_group --directory=$WRITE_DIR --rw=write --size=32G --numjobs=$N --iodepth=$D --fallocate=posix --refill_buffers"
pin="--cpus_allowed=0-3 --cpus_allowed_policy=split"   # one job per CPU
fio --ioengine=io_uring --direct=1 --bs=1M --time_based --ramp_time=5 --runtime=30 --group_reporting \
    $reader [$pin] $writer [$pin]
```

Raw JSON and iostat logs: `/instance_storage/fio-xfs/results{,-pin}-{1M,mixed}/`
on the box (ephemeral).
