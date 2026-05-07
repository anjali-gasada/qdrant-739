#!/usr/bin/env bash
# Sweep replication factor x write consistency across both 3-node and 5-node
# clusters. The proposal asks us to measure the latency PENALTY of each
# (replication, consistency) tuple, which means we have to RECREATE the
# collection between cells - replication_factor and write_consistency_factor
# can only be set at create time (or via cluster_collection_update).
#
# We pick the cheapest path: tear down + reload. The dataset loader is fast
# enough that this is acceptable.

set -euo pipefail

cd "$(dirname "$0")/.."

HOSTS_3=localhost:6334,localhost:6434,localhost:6534
HOSTS_5=localhost:6334,localhost:6434,localhost:6534,localhost:6634,localhost:6734
HDF5=./data/sift-128-euclidean.hdf5

run_one() {
  local cluster=$1     # 3 or 5
  local rf=$2          # replication factor
  local wcf=$3         # write consistency factor
  local label=$4       # results subdir name

  echo "=== sweep: cluster=$cluster rf=$rf wcf=$wcf label=$label ==="

  if [[ "$cluster" == "3" ]]; then HOSTS="$HOSTS_3"; else HOSTS="$HOSTS_5"; fi

  ./bin/loader \
    -hosts "$HOSTS" \
    -collection sift1m \
    -hdf5 "$HDF5" \
    -shard-number 6 \
    -replication-factor "$rf" \
    -write-consistency "$wcf" \
    -batch 1024 -concurrency 8

  ./bin/bench \
    -hosts "$HOSTS" \
    -collection sift1m \
    -workload A,B,C \
    -concurrency 1,10,15,25,30 \
    -batch 1 \
    -duration 30s -warmup 5s \
    -run-label "$label" \
    -recall
}

# 3-node cluster sweeps
make cluster-down >/dev/null 2>&1 || true
make cluster-up-3
run_one 3 1 1  "n3_rf1_wcf1"   # baseline: no replication
run_one 3 2 1  "n3_rf2_wcf1"   # rf=2, ack-on-leader
run_one 3 2 2  "n3_rf2_wcf2"   # rf=2, ack-on-all (== majority for rf=2)
run_one 3 3 1  "n3_rf3_wcf1"   # rf=3, ack-on-leader (full replication, weak consistency)
run_one 3 3 2  "n3_rf3_wcf2"   # rf=3, majority
run_one 3 3 3  "n3_rf3_wcf3"   # rf=3, ack-on-all (most expensive)

# 5-node cluster sweeps
make cluster-down >/dev/null 2>&1 || true
make cluster-up-5
HOSTS=$HOSTS_5
run_one 5 3 2  "n5_rf3_wcf2"
run_one 5 5 1  "n5_rf5_wcf1"
run_one 5 5 3  "n5_rf5_wcf3"
run_one 5 5 5  "n5_rf5_wcf5"

echo "[sweep] done. Results under results/n*."
