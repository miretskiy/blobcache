#!/bin/bash
# How much gets through one CPU (one hardware queue), the way dio submits
# everything from one coordinator thread. All jobs pinned to CPU 0.
#  A. a reader job and a writer job on CPU 0: write depth x read depth
#  B. one job doing both (random reads, sequential writes) on CPU 0
# 1 MiB unless BS is set; 5 s ramp, 30 s measured; iostat during each run.
set -u
BS=${BS:-1M}; TAG=${TAG:-$BS}
root=/instance_storage/fio-xfs; rd=$root/read; wd=$root/write; out=$root/results-onecpu-$TAG
mkdir -p $wd $out
case $BS in */*) bsopt="--bssplit=$BS" ;; *) bsopt="--bs=$BS" ;; esac
common="--ioengine=io_uring --direct=1 $bsopt --time_based --ramp_time=5 --runtime=30 --group_reporting --slat_percentiles=1 --cpus_allowed=0"
echo "=== block size: $bsopt; all jobs on CPU 0"

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
            c, sl = s["clat_ns"]["percentile"], s["slat_ns"].get("percentile", {})
            st[side] = (s["bw_bytes"] / 1e9, c["50.000000"] / 1e6, c["99.000000"] / 1e6, sl.get("99.000000", 0) / 1e6)
r = st.get("read", (0, 0, 0, 0)); w = st.get("write", (0, 0, 0, 0))
rows = [l.split() for l in open(io) if l.startswith("nvme1n1")][6:36]
hdr = next(l.split() for l in open(io) if l.startswith("Device"))
col = lambda n: sum(float(x[hdr.index(n)]) for x in rows) / max(len(rows), 1)
print(f"{label:>30} | read {r[0]:4.2f} GB/s clat p50 {r[1]:6.1f} p99 {r[2]:6.1f} slat p99 {r[3]:5.1f}"
      f" | write {w[0]:4.2f} GB/s clat p50 {w[1]:6.1f} p99 {w[2]:6.1f} slat p99 {w[3]:5.1f}"
      f" | total {r[0]+w[0]:4.2f} | dev r {col('r_await'):5.1f} w {col('w_await'):5.1f} ms q {col('aqu-sz'):4.0f}", flush=True)
PY
}
run() {
  local label=$1; shift
  local f=$out/$(echo "$label" | tr -c 'A-Za-z0-9\n' '_')
  sudo rm -f $wd/*
  iostat -x -m 1 nvme1n1 > "$f.iostat" 2>&1 & local io=$!
  sudo fio --output-format=json "$@" > "$f.json" 2> "$f.err"
  kill $io; wait $io 2>/dev/null
  summarize "$label" "$f.json" "$f.iostat"
}
reader() { echo "--name=reader --directory=$rd --rw=randread --norandommap --randrepeat=0 --size=8G --numjobs=1 --iodepth=$1 $common"; }
writer() { echo "--name=writer --new_group --directory=$wd --rw=write --size=32G --numjobs=1 --iodepth=$1 --fallocate=posix --refill_buffers $common"; }
both() {  # one job: random reads + sequential writes over one laid-out 8 GiB file
  echo "--name=both --filename=$rd/reader.0.0 --rw=randrw --percentage_random=100,0 --rwmixread=$1 --norandommap --randrepeat=0 --refill_buffers --iodepth=$2 $common"
}

echo "=== A. reader + writer jobs on CPU 0 (depths in requests)"
for wq in 2 8 32; do
  for rq in 16 128; do
    run "A writes $wq / reads $rq" $(reader $rq) $(writer $wq)
  done
done
echo "=== B. one job, reads + writes, on CPU 0"
for qd in 16 64 256; do
  run "B 70% reads, depth $qd" $(both 70 $qd)
done
run "B 50% reads, depth 64" $(both 50 64)
echo "=== fio-onecpu finished"
