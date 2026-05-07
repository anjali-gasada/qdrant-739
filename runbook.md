# Runbook — Qdrant Distributed Benchmark

This runbook gives an exact, copy-pasteable sequence to reproduce every experiment in the proposal. Each section corresponds to one research question. Estimated wall-clock time is given per experiment so you can plan your evening accordingly.

## 0. One-time setup (≈ 15 minutes)

```bash
# 1. Install prerequisites
#    - Docker 24+ with Compose v2
#    - Go 1.22+
#    - Python 3.10+ with matplotlib, numpy
#    - HDF5 dev headers (sudo apt-get install libhdf5-dev)

# 2. Clone / cd into the project
cd /home/claude/qdrant-bench

# 3. Download the SIFT-1M dataset (≈ 500 MB)
make data

# 4. Build the Go binaries
make build

# Sanity check — binaries exist
ls bin/
# Expected: loader bench chaos groundtruth
```

If `make data` fails because of network restrictions, manually download `sift-128-euclidean.hdf5` from `http://ann-benchmarks.com/sift-128-euclidean.hdf5` and place it at `data/sift-128-euclidean.hdf5`.

## 1. Smoke test — confirm everything works (≈ 5 minutes)

```bash
make cluster-up-3              # boot 3-node cluster
sleep 20                       # let Raft converge

# Load a small slice (100k vectors, ≈ 1 minute)
./bin/loader -addr localhost:6334,localhost:6434,localhost:6534 \
             -collection smoketest \
             -shards 6 -replication 2 -wcf 1 \
             -limit 100000 -batch 1024

# Run a 30-second read-only burst
./bin/bench -addr localhost:6334,localhost:6434,localhost:6534 \
            -collection smoketest \
            -workload C -concurrency 10 -duration 30s \
            -out results/smoke

# Inspect output
cat results/smoke/C_c10_b1.json | jq '.summary | {qps, p99_us}'
```

Expected: QPS in the low thousands, p99 under 50 ms. If you see errors mentioning "no leader" or "shard not ready", give the cluster another 10-15 seconds and retry.

```bash
make cluster-down
```

## 2. Experiment A — concurrency sweep (≈ 90 minutes)

**Research question:** how does throughput and p99 scale with concurrent clients on a 3-node cluster?

```bash
make cluster-up-3
sleep 25

# Full load — this is the "hot" collection re-used by every workload below.
make load              # 1M vectors, 6 shards, RF=2, WCF=1, batch=1024

# Sweep concurrency = {1, 10, 15, 25, 30} for workloads A–F.
make bench-full        # ≈ 60 min wall-clock for 6 workloads x 5 concurrency levels x 60s each

# Generate plots
make report

# Outputs land in:
#   results/full3/qps_vs_concurrency_<wl>.png
#   results/full3/p99_vs_concurrency_<wl>.png
```

Tail-latency knee usually shows up around c=15 for write-heavy (A, F) and c=25 for read-mostly (B, C, D, E). The plot will make the contention visible.

## 3. Experiment B — replication factor & write consistency (≈ 3 hours)

**Research question:** what is the latency/throughput penalty as you tighten consistency, on both 3-node and 5-node clusters?

```bash
# 3-node sweep — six (RF, WCF) combinations
make cluster-down
make cluster-up-3
./scripts/run_full_sweep.sh 3

# 5-node sweep — four (RF, WCF) combinations
make cluster-down
make cluster-up-5
./scripts/run_full_sweep.sh 5

# Each (RF, WCF) cell runs workloads A and B at concurrency 25 for 60s.
# Results land in results/sweep_3node_RF<x>_WCF<y>/ and results/sweep_5node_*/
```

Plot the `(RF, WCF)` axes with QPS as the bar height and p99 as a secondary line. Expected pattern: QPS roughly halves between WCF=1 and WCF=RF; p99 grows sub-linearly with RF because writes fan out in parallel.

## 4. Experiment C — chaos: kill a node mid-write (≈ 20 minutes)

**Research question:** how long does Raft take to elect a new leader, and how high does p99 spike during the recovery window?

```bash
make cluster-up-3
sleep 25
make load

# Kill node-2 70 seconds in, restart it 60 seconds later.
./bin/chaos -addr localhost:6334,localhost:6434,localhost:6534 \
            -collection bench \
            -workload A -concurrency 25 -duration 180s \
            -mode kill -target qdrant-node-2 \
            -inject-after 70s -recover-after 130s \
            -out results/chaos_kill_node2

# Plot the leader timeline + per-second latency
python3 scripts/plot_results.py --chaos results/chaos_kill_node2
```

Three numbers to extract from `summary.json`:
- `recovery_time_ms` — wall-clock from kill to first stable leader observation
- `p99_during_recovery_us` — p99 over the 10-second window starting at injection
- `error_rate_during_recovery` — fraction of failed RPCs in that window

## 5. Experiment D — chaos: network partition (≈ 20 minutes)

```bash
make cluster-up-3
sleep 25
make load

./bin/chaos -addr localhost:6334,localhost:6434,localhost:6534 \
            -collection bench \
            -workload A -concurrency 25 -duration 180s \
            -mode partition -target qdrant-node-2 \
            -inject-after 70s -recover-after 130s \
            -out results/chaos_partition_node2
```

Partition is implemented by `iptables` dropping packets on the Raft port (6335) for the target container, then flushing the rules at recovery. The cluster sees the node as unreachable for Raft but still up for everything else, which is closer to a real network failure than a SIGKILL.

## 6. Experiment E — segment size sweep (≈ 45 minutes)

**Research question:** how does forcing more frequent compaction (smaller segments) affect read latency?

```bash
make cluster-up-3
sleep 25

./scripts/run_compaction_sweep.sh
# Tries max_segment_size_kb in {10000, 50000, 200000, default}.
# Each value: drop+recreate collection, reload 1M vectors,
# wait until status=green, run Workload B at c=25 for 60s.
# Output: results/compaction_seg<size>_workloadB/
```

Expected: smaller segments → more compaction events → periodic CPU spikes visible in the per-second QPS timeline. The `_per_second.json` arrays are what you plot.

## 7. Experiment F — pipeline bottleneck localization (≈ 30 minutes)

**Research question:** of {network, ack-wait, indexing}, which dominates write latency?

The Qdrant Go client exposes a `Wait` flag on point upserts. With `Wait=false` the server acks as soon as the request hits the WAL; with `Wait=true` it acks only after indexing. The delta is the indexing cost.

```bash
make cluster-up-3
sleep 25
make load

# A1: WAL-acked writes
./bin/bench -addr localhost:6334,localhost:6434,localhost:6534 \
            -collection bench \
            -workload A -concurrency 25 -duration 60s \
            -wait=false -out results/pipeline_walack

# A2: index-acked writes
./bin/bench -addr localhost:6334,localhost:6434,localhost:6534 \
            -collection bench \
            -workload A -concurrency 25 -duration 60s \
            -wait=true -out results/pipeline_indexack
```

Compare the two p99 numbers. The difference is the indexing tax. Network RTT can be measured separately by running the same workload pointed at a single node (`-addr localhost:6334`) — the gap between that and the round-robin run is roughly the cluster routing/coordination cost.

## 8. Experiment G — batch size sweep (≈ 45 minutes)

```bash
make cluster-up-3
sleep 25

# Re-load with each batch size; bench writes only.
for B in 64 256 512 1024 2048 4096; do
    make cluster-down
    make cluster-up-3
    sleep 20
    ./bin/loader -addr localhost:6334,localhost:6434,localhost:6534 \
                 -collection bench -limit 1000000 \
                 -batch $B -concurrency 8 \
                 -shards 6 -replication 2 -wcf 1
    ./bin/bench -addr localhost:6334,localhost:6434,localhost:6534 \
                -collection bench -workload A -concurrency 25 \
                -duration 60s -out results/batch_b$B
done
```

Plot QPS-per-vector vs batch size. The curve usually flattens around 1024-2048 because past that point the bottleneck moves from request overhead to indexing CPU.

## 9. Final report assembly

```bash
# Generate the full plot set
make report

# All raw JSON + PNGs are in results/
# Architecture diagram is in docs/architecture.svg (rendered separately)
# This runbook + docs/architecture.md contain the experimental rationale
```

## Troubleshooting

**"Cluster never converges, no leader"** — almost always a port-binding issue. Verify `docker compose ps` shows all containers healthy and that ports 6335/6435/6535 are not in use on the host. The Raft P2P traffic uses these.

**"shard not ready" errors during loading** — your `replication_factor` is greater than the number of running nodes. Either reduce RF or wait for all nodes to come up before loading.

**Memory pressure** — 1M × 128-d float32 vectors plus HNSW graphs takes ~3 GB resident per replica. Each of the 3 containers has `mem_limit: 6g` in the compose file; adjust if your host is tight.

**Recall drops below 0.9** — usually means HNSW `ef` (search-time) is too low. Try `-ef 128` on the bench command. Loader sets `ef_construct=100` and `m=16` by default which is the standard ANN-Benchmarks recipe.

**Container can't run iptables for partitioning** — the compose file sets `cap_add: NET_ADMIN` on every node. If you're running on a hardened Docker setup that blocks capabilities, partition tests will silently no-op. Use kill-mode chaos instead.
