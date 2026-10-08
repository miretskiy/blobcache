#!/bin/bash
# fio baseline on the XFS instance storage: write ceilings (incl. one shared
# file), read ceiling, mixed read/write at three read depths, and a
# submitter-count check. 1 MiB O_DIRECT via io_uring; 5 s ramp, 30 s measured.
set -u
# BS: one size (BS=1M, the default) or a weighted fio --bssplit list
# (BS=2m/10:1m/55:...). Results go to results-$TAG.
BS=${BS:-1M}; TAG=${TAG:-$BS}
root=/instance_storage/fio-xfs; rd=$root/read; wd=$root/write; out=$root/results-$TAG
mkdir -p $rd $wd $out
case $BS in */*) bsopt="--bssplit=$BS" ;; *) bsopt="--bs=$BS" ;; esac
echo "=== block size: $bsopt"
common="--ioengine=io_uring --direct=1 $bsopt --time_based --ramp_time=5 --runtime=30 --group_reporting"

summarize() {
  python3 - "$1" "$2" <<'PY'
import json, sys
label, path = sys.argv[1], sys.argv[2]
raw = open(path).read(); d = json.loads(raw[raw.index("{"):])
st = {}
for j in d["jobs"]:
    for side in ("read", "write"):
        s = j[side]
        if s["bw_bytes"] > 0:
            p = s["clat_ns"]["percentile"]
            st[side] = (s["bw_bytes"] / 1e9, p["50.000000"] / 1e6, p["99.000000"] / 1e6)
r = st.get("read", (0, 0, 0)); w = st.get("write", (0, 0, 0))
print(f"{label:>34} | read {r[0]:5.2f} GB/s p50 {r[1]:6.1f} p99 {r[2]:6.1f} ms | write {w[0]:5.2f} GB/s p50 {w[1]:6.1f} p99 {w[2]:6.1f} ms | total {r[0]+w[0]:5.2f}", flush=True)
PY
}
# run <label> <fio job args...>: fresh write files for every run.
run() {
  local label=$1; shift
  sudo rm -f $wd/*
  sudo fio --output-format=json "$@" > "$out/$label.json" 2> "$out/$label.err" || { echo "$label: fio failed"; cat "$out/$label.err"; return; }
  summarize "$label" "$out/$label.json"
}
reader() { echo "--name=reader --directory=$rd --rw=randread --norandommap --randrepeat=0 --size=8G --numjobs=$1 --iodepth=$2 $common"; }
writer() { echo "--name=writer --new_group --directory=$wd --rw=write --size=32G --numjobs=$1 --iodepth=$2 --fallocate=posix --refill_buffers $common"; }

echo "=== layout: 16 x 8 GiB read files (named like the reader jobs, which reuse them)"
[ "$(ls $rd/reader.*.0 2>/dev/null | wc -l)" = 16 ] || sudo fio --name=reader --directory=$rd --rw=write --size=8G --numjobs=16 --iodepth=16 --ioengine=io_uring --direct=1 --bs=1M --refill_buffers > $out/layout.log 2>&1
echo "=== write ceilings (alone)"
run "W 4 jobs x 4 files, qd64" $(writer 4 64)
run "W 4 jobs x 1 shared file, qd64" --name=writer --filename=$wd/shared --filesize=128G --size=32G --offset_increment=32G \
  --rw=write --numjobs=4 --iodepth=64 --fallocate=posix --refill_buffers $common
run "W 1 job x 1 file, qd64" $(writer 1 64)
echo "=== read ceiling (alone)"
run "R 16 jobs x qd8 (128)" $(reader 16 8)
echo "=== mixed: reads at fixed depth vs. unthrottled writes (4 jobs x qd64)"
run "M reads 16 x qd2 (32)" $(reader 16 2) $(writer 4 64)
run "M reads 16 x qd8 (128)" $(reader 16 8) $(writer 4 64)
run "M reads 16 x qd32 (512)" $(reader 16 32) $(writer 4 64)
echo "=== submitter count: 128 reads in flight"
run "M reads 4 x qd32 vs 4 writers" $(reader 4 32) $(writer 4 64)
run "M reads 1 x qd128 vs 1 writer qd256" $(reader 1 128) $(writer 1 256)
echo "=== fio-xfs finished"
