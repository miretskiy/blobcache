#!/bin/bash
# Baseline: the fio suite at fixed 1 MiB, then with a mixed size distribution
# (median 1 MiB, 35% of requests smaller), in one session.
MIXED=2m/10:1m/55:512k/15:256k/10:128k/5:64k/5
dir=$(dirname "$0")
for cfg in "1M 1M" "$MIXED mixed"; do
  set -- $cfg
  echo; echo "########## BS=$1 (tag $2)"
  BS=$1 TAG=$2 bash "$dir/fio-xfs.sh"
  BS=$1 TAG=$2 bash "$dir/fio-pin.sh"
done
echo "=== fio-all finished"
