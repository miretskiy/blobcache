#!/bin/bash
# Does sharing a CPU (and so a hardware queue) between readers and writers
# starve reads? Mixed runs with jobs pinned to the same vs. disjoint CPUs, with
# iostat during each. 1 MiB O_DIRECT via io_uring; 5 s ramp, 30 s measured.
set -u
# BS: one size (BS=1M, the default) or a weighted fio --bssplit list.
BS=${BS:-1M}; TAG=${TAG:-$BS}
root=/instance_storage/fio-xfs; rd=$root/read; wd=$root/write; out=$root/results-pin-$TAG
mkdir -p $wd $out
case $BS in */*) bsopt="--bssplit=$BS" ;; *) bsopt="--bs=$BS" ;; esac
echo "=== block size: $bsopt"
common="--ioengine=io_uring --direct=1 $bsopt --time_based --ramp_time=5 --runtime=30 --group_reporting"

echo "=== CPU -> hardware queue map (nvme1n1)"
for q in /sys/block/nvme1n1/mq/*; do printf "q%s:%s " "$(basename $q)" "$(cat $q/cpu_list)"; done; echo

summarize() {
  python3 - "$1" "$2" "$3" <<'PY'
import json, sys
label, path, io = sys.argv[1:4]
raw = open(path).read(); d = json.loads(raw[raw.index("{"):])
st = {}
for j in d["jobs"]:
    for side in ("read", "write"):
        s = j[side]
        if s["bw_bytes"] > 0:
            st[side] = (s["bw_bytes"] / 1e9, s["clat_ns"]["percentile"]["50.000000"] / 1e6)
r = st.get("read", (0, 0)); w = st.get("write", (0, 0))
# iostat -x -m 1: average the device rows of the measured window (skip the ramp).
rows = [l.split() for l in open(io) if l.startswith("nvme1n1")][6:36]
hdr = next(l.split() for l in open(io) if l.startswith("Device"))
col = lambda name: sum(float(x[hdr.index(name)]) for x in rows) / max(len(rows), 1)
print(f"{label:>36} | fio read {r[0]:4.2f} GB/s p50 {r[1]:6.1f} ms | write {w[0]:4.2f} GB/s p50 {w[1]:6.1f} ms"
      f" | device r_await {col('r_await'):5.1f} w_await {col('w_await'):5.1f} ms, queue {col('aqu-sz'):6.1f}", flush=True)
PY
}
# run <label> <fio args...>
run() {
  local label=$1; shift
  local f=$out/$(echo "$label" | tr -c 'A-Za-z0-9\n' '_')
  sudo rm -f $wd/*
  iostat -x -m 1 nvme1n1 > "$f.iostat" 2>&1 & local io=$!
  sudo fio --output-format=json "$@" > "$f.json" 2> "$f.err"
  kill $io; wait $io 2>/dev/null
  summarize "$label" "$f.json" "$f.iostat"
}
# reader/writer <jobs> <depth> [cpus]: pinned one job per CPU when cpus is given.
pin() { [ -n "${1:-}" ] && echo "--cpus_allowed=$1 --cpus_allowed_policy=split"; }
reader() { echo "--name=reader --directory=$rd --rw=randread --norandommap --randrepeat=0 --size=8G --numjobs=$1 --iodepth=$2 $(pin ${3:-}) $common"; }
writer() { echo "--name=writer --new_group --directory=$wd --rw=write --size=32G --numjobs=$1 --iodepth=$2 --fallocate=posix --refill_buffers $(pin ${3:-}) $common"; }

run "4R x qd32 / 4W x qd64, unpinned"  $(reader 4 32)        $(writer 4 64)
run "4R / 4W, disjoint CPUs"           $(reader 4 32 0-3)    $(writer 4 64 16-19)
run "4R / 4W, same CPUs"               $(reader 4 32 0-3)    $(writer 4 64 0-3)
run "1R x qd128 / 1W x qd256, same CPU" $(reader 1 128 0)    $(writer 1 256 0)
run "1R / 1W, different CPUs"          $(reader 1 128 0)     $(writer 1 256 16)
echo "=== fio-pin finished"
