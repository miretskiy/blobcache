#!/usr/bin/env python3
"""Per-run read/write clat percentiles and slat from fio JSON (ms)."""
import json, os, sys
for d in sys.argv[1:]:
    print(f"## {os.path.basename(d)}")
    print(f"{'test':<34} {'side':<5} {'GB/s':>5} | clat p50   p90   p99  p99.9    max | slat mean   p99")
    for path in sorted(os.listdir(d), key=lambda p: os.path.getmtime(os.path.join(d, p))):
        if not path.endswith(".json"):
            continue
        raw = open(os.path.join(d, path)).read()
        try:
            j = json.loads(raw[raw.index("{"):])["jobs"]
        except ValueError:
            continue
        for side in ("read", "write"):
            for job in j:
                s = job[side]
                if s["bw_bytes"] == 0:
                    continue
                c, sl = s["clat_ns"], s["slat_ns"]
                p = c["percentile"]; ms = lambda v: v / 1e6
                slp = sl.get("percentile", {}).get("99.000000", 0)
                print(f"{path[:-5].replace('_', ' ').strip()[:34]:<34} {side:<5} {s['bw_bytes']/1e9:5.2f} |"
                      f" {ms(p['50.000000']):6.1f} {ms(p['90.000000']):5.0f} {ms(p['99.000000']):5.0f} {ms(p['99.900000']):6.0f} {ms(c['max']):6.0f} |"
                      f" {ms(sl['mean']):8.2f} {ms(slp):5.1f}")
                break
    print()
