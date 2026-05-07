#!/usr/bin/env bash
# Compaction-impact sweep.
#
# The proposal explicitly asks us to "tweak the internal max_segment_size_kb
# to observe how forcing more frequent background index compaction impacts
# the system's ability to serve incoming read requests."
#
# Strategy:
#   1. Recreate the collection with different max_segment_size_kb values.
#   2. Load the full SIFT-1M corpus (this triggers many flushes & merges
#      with small max_segment_size_kb).
#   3. Run workload B (95% read / 5% update) for 60s with concurrency=25.
#   4. Compare p99 read latency across runs.
#
# Smaller max_segment_size_kb means more frequent merges (more CPU contention
# during reads); larger means fewer/longer merges. The Qdrant default is
# auto-selected; we include "default" (no override) as the control.

set -euo pipefail

cd "$(dirname "$0")/.."

HOSTS=localhost:6334,localhost:6434,localhost:6534
HDF5=./data/sift-128-euclidean.hdf5

# In KiloBytes. Note 1 KB = 1 vector at dim=256, so for SIFT (128-d, 4 bytes/dim
# = 512 bytes/vec = 0.5 KB/vec):
#   10000 KB ~  20K vectors per segment  -> aggressive merging
#   50000 KB ~ 100K vectors per segment
#   200000 KB ~ 400K vectors per segment -> infrequent merging
#   "default" lets Qdrant pick (=> a few segments per CPU core)
SEGMENT_SIZES=(10000 50000 200000 default)

# Raise file descriptor limit for RocksDB
ulimit -n 65536 2>/dev/null || true

# Verify all cluster nodes are reachable before starting
echo "[sweep] checking cluster health..."
for p in 6333 6433 6533; do
  for i in $(seq 1 20); do
    if curl -sf "http://localhost:$p/readyz" >/dev/null 2>&1; then
      echo "[sweep] node :$p ready"
      break
    fi
    if [ "$i" -eq 20 ]; then
      echo "[sweep] ERROR: node :$p not ready — is the cluster running?"
      exit 1
    fi
    sleep 3
  done
done

# Delete collection if it exists and wait for Raft to propagate
delete_collection() {
  echo "[sweep] deleting collection sift1m (if exists)..."
  curl -sf -X DELETE http://localhost:6333/collections/sift1m >/dev/null 2>&1 || true
  sleep 5
}

# Wait for HNSW indexing to complete
wait_for_indexing() {
  echo "[sweep] waiting for HNSW indexing to complete..."

  # Check collection exists first
  if ! curl -sf http://localhost:6333/collections/sift1m >/dev/null 2>&1; then
    echo "[sweep] ERROR: collection sift1m does not exist — loader likely failed"
    exit 1
  fi

  # Lower the threshold so indexing starts immediately
  curl -sf -X PATCH http://localhost:6333/collections/sift1m \
    -H 'Content-Type: application/json' \
    -d '{"optimizers_config": {"indexing_threshold_kb": 1000}}' >/dev/null 2>&1 || true

  for i in $(seq 1 60); do
    indexed=$(curl -s http://localhost:6333/collections/sift1m | \
      python3 -c "import sys,json; d=json.load(sys.stdin); print(d['result']['indexed_vectors_count'])" 2>/dev/null || echo "0")
    points=$(curl -s http://localhost:6333/collections/sift1m | \
      python3 -c "import sys,json; d=json.load(sys.stdin); print(d['result']['points_count'])" 2>/dev/null || echo "1")
    status=$(curl -s http://localhost:6333/collections/sift1m | \
      python3 -c "import sys,json; d=json.load(sys.stdin); print(d['result']['status'])" 2>/dev/null || echo "unknown")
    opt=$(curl -s http://localhost:6333/collections/sift1m | \
      python3 -c "import sys,json; d=json.load(sys.stdin); print(d['result']['optimizer_status'])" 2>/dev/null || echo "unknown")
    echo "  iter=$i status=$status optimizer=$opt indexed=$indexed points=$points"
    if [ "$indexed" -ge "$points" ] && [ "$points" -gt 0 ]; then
      echo "[sweep] indexing complete"
      return
    fi
    sleep 10
  done
  echo "[sweep] WARNING: indexing did not complete in time, proceeding anyway"
}

for seg in "${SEGMENT_SIZES[@]}"; do
  echo "==================================================="
  echo " compaction-sweep: max_segment_size_kb = $seg"
  echo "==================================================="

  if [[ "$seg" == "default" ]]; then
    SEG_FLAG="-1"
  else
    SEG_FLAG="$seg"
  fi

  # Delete existing collection so loader can recreate with new segment size
  delete_collection

  # Recreate + reload — tolerate minor point-count mismatch with || true
  ./bin/loader \
    -hosts "$HOSTS" \
    -collection sift1m \
    -hdf5 "$HDF5" \
    -shard-number 6 \
    -replication-factor 2 \
    -write-consistency 1 \
    -batch 1024 -concurrency 8 \
    -max-segment-size-kb "$SEG_FLAG" || true

  # Wait for HNSW index to be fully built before benching
  wait_for_indexing

  # Run the bench
  ./bin/bench \
    -hosts "$HOSTS" \
    -collection sift1m \
    -workload B \
    -concurrency 25 \
    -batch 1 \
    -duration 60s -warmup 10s \
    -run-label "compaction_seg_$seg" \
    -recall
done

echo "[sweep] compaction sweep done. Results under results/compaction_seg_*."