#!/bin/bash
# Unloaded latency: one request in flight (QD1), on CPU 0 like dio's single
# submitter. Reads, writes (fresh preallocated space), and one of each at once,
# at 4 KiB, 128 KiB (one device command), 1 MiB and the mixed distribution.
# Little's law then gives the depth needed to reach the measured ceilings:
# depth = ceiling (bytes/s) x mean latency / mean request size.
set -u
READ_CEIL=2.56e9; WRITE_CEIL=1.22e9      # GB/s ceilings from the baseline (section 1)
MIXED=2m/10:1m/55:512k/15:256k/10:128k/5:64k/5
root=/instance_storage/fio-xfs; rd=$root/read; wd=$root/write; out=$root/results-latency
mkdir -p $wd $out

summarize() {
  python3 - "$1" "$2" $READ_CEIL $WRITE_CEIL <<'PY'
import json, sys
label, path, rceil, wceil = sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4])
raw = open(path).read(); d = json.loads(raw[raw.index("{"):])
for j in d["jobs"]:
    for side, ceil in (("read", rceil), ("write", wceil)):
        s = j[side]
        if s["bw_bytes"] == 0:
            continue
        lat, clat = s["lat_ns"], s["clat_ns"]["percentile"]
        size = s["bw_bytes"] / s["iops"]
        depth = ceil * lat["mean"] / 1e9 / size
        print(f"{label:<22} {side:<5} | {s['iops']:7.0f} IOPS {s['bw_bytes']/1e9:5.2f} GB/s avg {size/1024:5.0f} KiB"
              f" | lat mean {lat['mean']/1e6:6.3f} | clat p50 {clat['50.000000']/1e6:6.3f} p99 {clat['99.000000']/1e6:6.3f}"
              f" p99.9 {clat['99.900000']/1e6:6.3f} ms | depth for ceiling {depth:5.1f} req = {depth*size/2**20:5.1f} MiB", flush=True)
PY
}
run() {
  local label=$1; shift
  local f=$out/$(echo "$label" | tr -c 'A-Za-z0-9\n' '_')
  sudo rm -f $wd/*
  sudo fio --output-format=json "$@" > "$f.json" 2> "$f.err" || { echo "$label: fio failed"; cat "$f.err"; return; }
  summarize "$label" "$f.json"
}
for BS in 4k 128k 1M $MIXED; do
  case $BS in */*) bsopt="--bssplit=$BS"; name=mixed ;; *) bsopt="--bs=$BS"; name=$BS ;; esac
  common="--ioengine=io_uring --direct=1 $bsopt --iodepth=1 --time_based --ramp_time=5 --runtime=20 --cpus_allowed=0"
  reader="--name=reader --directory=$rd --rw=randread --norandommap --randrepeat=0 --size=8G --numjobs=1 $common"
  writer="--name=writer --new_group --directory=$wd --rw=write --size=32G --numjobs=1 --fallocate=posix --refill_buffers $common"
  echo "=== $name, QD1"
  run "$name read"       $reader
  run "$name write"      $writer
  run "$name read+write" $reader $writer
done
echo "=== fio-latency finished"
