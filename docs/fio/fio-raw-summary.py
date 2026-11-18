#!/usr/bin/env python3
"""Summarize a fio-raw.sh results directory as one table row per job."""
import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
cols = ["point", "job", "MB/s", "IOPS", "p50", "p99", "p99.9", "p99.99", "max", "CPU us/IO"]
print("| " + " | ".join(cols) + " |")
print("|" + "---|" * len(cols))
for path in sorted(out.glob("*.json"), key=lambda p: p.stat().st_mtime):
    for job in json.loads(path.read_text())["jobs"]:
        side = job["read"] if job["read"]["total_ios"] else job["write"]
        p = side["clat_ns"]["percentile"]
        us = lambda key: "%.0f" % (p[key] / 1e3)
        cpu = (job["usr_cpu"] + job["sys_cpu"]) / 100 / side["iops"] * 1e6
        print("| %s | %s | %.0f | %.0f | %s | %s | %s | %s | %.0f | %.1f |" % (
            path.stem, job["jobname"], side["bw_bytes"] / 1e6, side["iops"],
            us("50.000000"), us("99.000000"), us("99.900000"), us("99.990000"),
            side["clat_ns"]["max"] / 1e3, cpu))
