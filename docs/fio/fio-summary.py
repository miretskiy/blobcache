#!/usr/bin/env python3
"""Summarize a fio results directory: one line per JSON (GB/s, p50, avg request size)."""
import json, os, sys
for path in sorted(os.listdir(sys.argv[1]), key=lambda p: os.path.getmtime(os.path.join(sys.argv[1], p))):
    if not path.endswith(".json"):
        continue
    raw = open(os.path.join(sys.argv[1], path)).read()
    try:
        d = json.loads(raw[raw.index("{"):])
    except ValueError:
        continue
    st = {}
    for j in d["jobs"]:
        for side in ("read", "write"):
            s = j[side]
            if s["bw_bytes"] > 0:
                st[side] = (s["bw_bytes"] / 1e9, s["clat_ns"]["percentile"]["50.000000"] / 1e6,
                            s["bw_bytes"] / s["iops"] / 1024 if s["iops"] else 0)
    r = st.get("read", (0, 0, 0)); w = st.get("write", (0, 0, 0))
    io = os.path.join(sys.argv[1], path[:-5] + ".iostat")
    dev = ""
    if os.path.exists(io):
        lines = open(io).read().splitlines()
        hdr = next(l.split() for l in lines if l.startswith("Device"))
        rows = [l.split() for l in lines if l.startswith("nvme1n1")][6:36]
        col = lambda n: sum(float(x[hdr.index(n)]) for x in rows) / max(len(rows), 1)
        dev = f" | dev r_await {col('r_await'):5.1f} w_await {col('w_await'):5.1f} queue {col('aqu-sz'):6.0f}"
    print(f"{path[:-5].replace('_', ' ').strip():<38} read {r[0]:4.2f} GB/s p50 {r[1]:6.1f}ms avg {r[2]:5.0f}K"
          f" | write {w[0]:4.2f} GB/s p50 {w[1]:6.1f}ms avg {w[2]:5.0f}K | total {r[0]+w[0]:4.2f}{dev}")
