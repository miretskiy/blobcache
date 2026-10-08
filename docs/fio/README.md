# fio scripts for the instance-storage baseline

The scripts that produced [`../nvme-baseline-m7gd.md`](../nvme-baseline-m7gd.md),
kept as a record. They expect the benchmark box's layout: an XFS instance
store at `/instance_storage` (they work under `/instance_storage/fio-xfs`),
device `nvme1n1`, fio ≥ 3.36 with io_uring, `iostat` (sysstat), and `sudo`.

| Script | What it runs | Doc section |
|---|---|---|
| `fio-all.sh` | `fio-xfs.sh` and `fio-pin.sh` at 1 MiB and at the mixed size distribution | 1–3 |
| `fio-xfs.sh` | write and read ceilings, mixed read/write sweep, submitter counts | 1, 2 |
| `fio-pin.sh` | readers and writers pinned to the same vs. disjoint CPUs, with iostat | 3 |
| `fio-onecpu.sh` | everything on CPU 0 (how dio submits): write depth × read depth, and one job doing both | 4 |
| `fio-onecpu-edge.sh` | the write-depth cliff (16 and 24 in flight) and the mixed-size check | 4 |
| `fio-latency.sh` | one request in flight: latency per size, and the depth Little's law needs | 5 |
| `fio-model.sh` | per-class IOPS and bandwidth at 256 in flight, 512 B vs 4 KiB, reads and writes at once: the disk model | 6 |
| `fio-summary.py`, `fio-clat.py` | summarize a results directory: throughput and device waits; clat percentiles | — |

`BS` selects the request size: one size (`BS=1M`, the default) or a weighted
fio `--bssplit` list (`BS=2m/10:1m/55:512k/15:256k/10:128k/5:64k/5`).
Results land in `/instance_storage/fio-xfs/results-*`. Each run takes 5 s of
ramp and 30 s of measurement (20 s in `fio-latency.sh`); write files are
deleted before every run so writes always land in freshly preallocated space.
