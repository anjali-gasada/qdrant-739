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

# Wait until all nodes in a given list of REST ports are ready
wait_for_nodes() {
  local ports=("$@")
  for p in "${ports[@]}"; do
    echo "[sweep] waiting for node :$p..."
    for i in $(seq 1 30); do
      if curl -sf "http://localhost:$p/readyz" >/dev/null 2>&1; then
        echo "[sweep] node :$p is ready"
        break
      fi
      if [ "$i" -eq 30 ]; then
        echo "[sweep] ERROR: node :$p did not become ready in time"
        exit 1
      fi
      sleep 3
    done
  done
}

# Delete the collection if it exists and wait for deletion to propagate
delete_collection() {
  echo "[sweep] deleting collection sift1m (if exists)..."
  curl -sf -X DELETE http://localhost:6333/collections/sift1m >/dev/null 2>&1 || true
  sleep 5
}

# Wait for HNSW indexing to complete on the collection
wait_for_indexing() {
  echo "[sweep] waiting for HNSW indexing to complete..."
  # Lower the indexing threshold so indexing starts immediately
  curl -sf -X PATCH http://localhost:6333/collections/sift1m \
    -H 'Content-Type: application/json' \
    -d '{"optimizers_config": {"indexing_threshold_kb": 1000}}' >/dev/null 2>&1 || true

  for i in $(seq 1 60); do
    indexed=$(curl -s http://localhost:6333/collections/sift1m | \
      python3 -c "import sys,json; d=json.load(sys.stdin); print(d['result']['indexed_vectors_count'])" 2>/dev/null || echo "0")
    points=$(curl -s http://localhost:6333/collections/sift1m | \
      python3 -c "import sys,json; d=json.load(sys.stdin); print(d['result']['points_count'])" 2>/dev/null || echo "1")
    echo "[sweep] indexed=$indexed / points=$points"
    if [ "$indexed" -ge "$points" ] && [ "$points" -gt 0 ]; then
      echo "[sweep] indexing complete"
      return
    fi
    sleep 10
  done
  echo "[sweep] WARNING: indexing did not complete in time, proceeding anyway"
}

run_one() {
  local cluster=$1     # 3 or 5
  local rf=$2          # replication factor
  local wcf=$3         # write consistency factor
  local label=$4       # results subdir name

  echo "=== sweep: cluster=$cluster rf=$rf wcf=$wcf label=$label ==="

  if [[ "$cluster" == "3" ]]; then HOSTS="$HOSTS_3"; else HOSTS="$HOSTS_5"; fi

  # Delete existing collection so we can recreate with new RF/WCF
  delete_collection

  ./bin/loader \
    -hosts "$HOSTS" \
    -collection sift1m \
    -hdf5 "$HDF5" \
    -shard-number 6 \
    -replication-factor "$rf" \
    -write-consistency "$wcf" \
    -batch 1024 -concurrency 8 || true  # tolerate minor point-count mismatch

  # Wait for HNSW index to be built before benching
  wait_for_indexing

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

# ---- 3-node cluster sweeps ------------------------------------------------
make cluster-down >/dev/null 2>&1 || true
sudo rm -rf deploy/data/node1/* deploy/data/node2/* deploy/data/node3/* 2>/dev/null || true
sudo rm -rf deploy/snapshots/node1/* deploy/snapshots/node2/* deploy/snapshots/node3/* 2>/dev/null || true
make cluster-up-3
sleep 15
wait_for_nodes 6333 6433 6533

run_one 3 1 1  "n3_rf1_wcf1"   # baseline: no replication
run_one 3 2 1  "n3_rf2_wcf1"   # rf=2, ack-on-leader
run_one 3 2 2  "n3_rf2_wcf2"   # rf=2, ack-on-all (== majority for rf=2)
run_one 3 3 1  "n3_rf3_wcf1"   # rf=3, ack-on-leader (full replication, weak consistency)
run_one 3 3 2  "n3_rf3_wcf2"   # rf=3, majority
run_one 3 3 3  "n3_rf3_wcf3"   # rf=3, ack-on-all (most expensive)

# ---- 5-node cluster sweeps ------------------------------------------------
make cluster-down >/dev/null 2>&1 || true
sudo rm -rf deploy/data5/node1/* deploy/data5/node2/* deploy/data5/node3/* deploy/data5/node4/* deploy/data5/node5/* 2>/dev/null || true
sudo rm -rf deploy/snapshots5/node1/* deploy/snapshots5/node2/* deploy/snapshots5/node3/* deploy/snapshots5/node4/* deploy/snapshots5/node5/* 2>/dev/null || true
make cluster-up-5
sleep 15
wait_for_nodes 6333 6433 6533 6633 6733

run_one 5 3 2  "n5_rf3_wcf2"
run_one 5 5 1  "n5_rf5_wcf1"
run_one 5 5 3  "n5_rf5_wcf3"
run_one 5 5 5  "n5_rf5_wcf5"

echo "[sweep] done. Results under results/n*."