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

for seg in "${SEGMENT_SIZES[@]}"; do
  echo "==================================================="
  echo " compaction-sweep: max_segment_size_kb = $seg"
  echo "==================================================="

  if [[ "$seg" == "default" ]]; then
    SEG_FLAG="-1"
  else
    SEG_FLAG="$seg"
  fi

  # Recreate + reload
  ./bin/loader \
    -hosts "$HOSTS" \
    -collection sift1m \
    -hdf5 "$HDF5" \
    -shard-number 6 \
    -replication-factor 2 \
    -write-consistency 1 \
    -batch 1024 -concurrency 8 \
    -max-segment-size-kb "$SEG_FLAG"

  # Wait until the optimizer has had a chance to settle. We poll the
  # /collections/sift1m endpoint and wait for status == green and
  # optimizer_status == ok. (The Go loader returns when ALL points are
  # ingested, but indexing continues asynchronously.)
  echo "[sweep] waiting for collection to reach green status..."
  for i in $(seq 1 60); do
    out=$(curl -s http://localhost:6333/collections/sift1m | python3 -c '
import sys, json
j = json.load(sys.stdin)
r = j.get("result", {})
print(r.get("status"), r.get("optimizer_status"), r.get("indexed_vectors_count"), r.get("segments_count"))')
    echo "  iter=$i $out"
    case "$out" in
      green*ok*) break ;;
    esac
    sleep 5
  done

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
