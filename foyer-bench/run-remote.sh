#!/bin/bash
# Run foyer benchmark on remote Graviton instance
# Usage: ./run-remote.sh

set -e

echo "=== Foyer Benchmark Runner ==="
echo "Remote: workspace-yevgeniy-m-g3 (m7gd.8xlarge)"
echo

# Check if we're on the remote machine
if [ ! -d "/instance_storage" ]; then
    echo "Error: /instance_storage not found"
    echo "This script should run on the Graviton instance"
    echo "SSH with: ssh workspace-yevgeniy-m-g3"
    exit 1
fi

# Build release binary
echo "Building release binary..."
cargo build --release

# Run benchmark
BENCH_PATH="/instance_storage/foyer-bench"
echo
echo "Running benchmark..."
echo "Path: $BENCH_PATH"
echo "Iterations: 2,560,000 (matching Go benchmark)"
echo

time ./target/release/foyer-bench \
    --iterations 2560000 \
    --path "$BENCH_PATH" \
    --capacity-gb 256

echo
echo "=== Benchmark Complete ==="
echo "Compare with blobcache results at: ~/src/blobcache"
