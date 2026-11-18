#!/bin/bash
# Raw-device NVMe baseline with fio's io_uring_cmd engine (NVMe passthrough on
# the character device, the path dio uses). Every command is 128 KiB, the
# device's maximum transfer. Reads target a region written first; writes go to
# a separate region.
#
# Usage: sudo docs/fio/fio-raw.sh DEVICE OFFSET SIZE OUT
#   DEVICE  NVMe character device, e.g. /dev/ng1n1
#   OFFSET  byte offset of a dedicated scratch range (its data is destroyed)
#   SIZE    range bytes; at least 48 GiB
#   OUT     results directory (one JSON file per point)
set -euo pipefail
dev=$1 base=$2 size=$3 out=$4
mkdir -p "$out"
gib=$((1 << 30))
read_off=$base read_size=$((16 * gib))
write_off=$((base + 16 * gib)) write_size=$((32 * gib))
((size >= 48 * gib)) || { echo "range must be at least 48 GiB" >&2; exit 1; }

common=(--ioengine=io_uring_cmd --cmd_type=nvme --filename="$dev" --time_based
	--ramp_time=2 --runtime=10 --output-format=json
	--percentile_list=50:90:99:99.9:99.99)
reads=(--offset="$read_off" --size="$read_size" --rw=randread)
writes=(--offset="$write_off" --size="$write_size" --rw=randwrite)

run() { # name, fio arguments
	local name=$1
	shift
	fio "${common[@]}" "$@" >"$out/$name.json"
	echo "$name"
}

# Lay out the read region so reads never hit unwritten blocks.
fio --ioengine=io_uring_cmd --cmd_type=nvme --filename="$dev" --name=layout \
	--offset="$read_off" --size="$read_size" --rw=write --bs=128k --iodepth=16 >/dev/null

# 1. Unloaded latency: one command in flight.
for bs in 4k 128k; do
	run "qd1-read-$bs" --name=r --bs=$bs --iodepth=1 --cpus_allowed=0 "${reads[@]}"
	run "qd1-write-$bs" --name=w --bs=$bs --iodepth=1 --cpus_allowed=0 "${writes[@]}"
done

# 2. Ceilings: bandwidth and latency against commands in flight.
for qd in 2 4 8 16 32 64; do
	run "write-qd$qd" --name=w --bs=128k --iodepth=$qd --cpus_allowed=0 "${writes[@]}"
done
for qd in 4 8 16 32 64 128; do
	run "read-qd$qd" --name=r --bs=128k --iodepth=$qd --cpus_allowed=0 "${reads[@]}"
done

# 3. Reads at 32 in flight while a writer runs, on another CPU (separate
# hardware queues) or the same one (a shared queue), by write depth.
for qd in 8 16 32 64; do
	for cpus in "1:2" "1:1"; do
		IFS=: read rcpu wcpu <<<"$cpus"
		where=$([[ $rcpu == "$wcpu" ]] && echo same || echo separate)
		run "mixed-w$qd-$where" --bs=128k --group_reporting=0 \
			--name=r --iodepth=32 --cpus_allowed=$rcpu "${reads[@]}" \
			--name=w --iodepth=$qd --cpus_allowed=$wcpu "${writes[@]}"
	done
done

# 4. CPU per command: registered buffers and SQPOLL, at 16 writes and 32 reads.
for variant in base fixedbufs sqpoll; do
	extra=()
	case $variant in
	fixedbufs) extra=(--fixedbufs=1) ;;
	sqpoll) extra=(--sqthread_poll=1) ;;
	esac
	run "cpu-write-$variant" --name=w --bs=128k --iodepth=16 --cpus_allowed=0 "${writes[@]}" "${extra[@]}"
	run "cpu-read-$variant" --name=r --bs=128k --iodepth=32 --cpus_allowed=0 "${reads[@]}" "${extra[@]}"
done
