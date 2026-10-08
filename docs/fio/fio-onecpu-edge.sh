#!/bin/bash
# Locate the write-depth cliff on one CPU, and check the good point with mixed sizes.
set -u
source <(sed -n '/^summarize()/,/^both()/p' "$(dirname "$0")/fio-onecpu.sh" | sed '$d')
root=/instance_storage/fio-xfs; rd=$root/read; wd=$root/write
for cfg in "1M 16 128" "1M 24 128" "2m/10:1m/55:512k/15:256k/10:128k/5:64k/5 8 128"; do
  set -- $cfg; BS=$1
  case $BS in */*) bsopt="--bssplit=$BS"; tag=mixed ;; *) bsopt="--bs=$BS"; tag=$BS ;; esac
  out=$root/results-onecpu-edge; mkdir -p $out
  common="--ioengine=io_uring --direct=1 $bsopt --time_based --ramp_time=5 --runtime=30 --group_reporting --slat_percentiles=1 --cpus_allowed=0"
  run "A $tag writes $2 / reads $3" $(reader $3) $(writer $2)
done
