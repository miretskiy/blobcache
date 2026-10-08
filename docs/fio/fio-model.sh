#!/bin/bash
# Disk model for an in-flight budget: per-class (read, write) maximum IOPS and
# bandwidth, and two checks that decide how to combine them.
#
#   1. IOPS and bandwidth at 256 requests in flight (8 jobs x depth 32) for
#      4 KiB, 16 KiB, 64 KiB and 1 MiB requests. Reads are random over 8 GiB
#      files; writes are sequential into freshly fallocated files, as BlobCache
#      writes. Small sizes give the IOPS limit, large sizes the bandwidth limit.
#   2. 512-byte requests against 4 KiB: a large gap means the device's real
#      physical sector is 4 KiB whatever it reports, and sub-4K I/O pays
#      read-modify-write.
#   3. Mid-size check: at 16 KiB, a device with independent IOPS and bandwidth
#      limits reaches min(iops, bw/16K); a device where they share one budget
#      (cost = 1/iops + bytes/bw) reaches less. Compare the two predictions
#      printed with the measurement.
#   4. Reads and writes at once (1 MiB, one reader job and one writer job on
#      different CPUs): if both reach their ceilings, reads and writes have
#      independent budgets; if their fractions sum to about 1, they share one.
#
# To model another disk, run this on it (adjust root/dirs) and take:
#   read_bw, write_bw   = the 1 MiB MB/s;  read_iops, write_iops = the 4 KiB IOPS;
# then use checks 3 and 4 to choose max-vs-sum and independent-vs-shared.
set -u
root=/instance_storage/fio-xfs; rd=$root/read; wd=$root/write; out=$root/results-model
mkdir -p $rd $wd $out
jobs="--numjobs=8 --iodepth=32 --group_reporting"
common="--ioengine=io_uring --direct=1 --time_based --ramp_time=5 --runtime=20"

summarize() {
  python3 - "$1" "$2" <<'PY'
import json, sys
label, path = sys.argv[1], sys.argv[2]
raw = open(path).read(); d = json.loads(raw[raw.index("{"):])
for j in d["jobs"]:
    for side in ("read", "write"):
        s = j[side]
        if s["bw_bytes"] == 0:
            continue
        p = s["clat_ns"]["percentile"]
        print(f"{label:<24} {side:<5} | {s['iops']:8.0f} IOPS {s['bw_bytes']/1e6:7.0f} MB/s"
              f" | clat p50 {p['50.000000']/1e3:7.0f} p99 {p['99.000000']/1e3:7.0f} us", flush=True)
PY
}
run() {
  local label=$1; shift
  local f=$out/$(echo "$label" | tr -c 'A-Za-z0-9\n' '_')
  sudo rm -f $wd/*
  sudo fio --output-format=json "$@" > "$f.json" 2> "$f.err" || { echo "$label: fio failed"; cat "$f.err"; return; }
  summarize "$label" "$f.json"
}
reader() { echo "--name=reader --directory=$rd --rw=randread --norandommap --randrepeat=0 --size=8G $common --bs=$1"; }
writer() { echo "--name=writer --new_group --directory=$wd --rw=write --size=8G --fallocate=posix --refill_buffers $common --bs=$1"; }

echo "=== 1-2. IOPS and bandwidth at 256 in flight"
for bs in 512 4k 16k 64k 1M; do
  run "$bs read"  $(reader $bs) $jobs
  run "$bs write" $(writer $bs) $jobs
done
echo "=== 3. predictions at 16 KiB from the 4 KiB IOPS (I) and 1 MiB bandwidth (B):"
echo "    independent limits: min(I, B/16384) IOPS; shared budget: 1/(1/I + 16384/B) IOPS"
echo "=== 4. reads and writes at once, 1 MiB, separate CPUs"
run "1M read+write" $(reader 1M) --numjobs=1 --iodepth=16 --cpus_allowed=0 $(writer 1M) --numjobs=1 --iodepth=4 --cpus_allowed=1
echo "=== fio-model finished"
