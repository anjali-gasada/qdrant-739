# Architecture

This document explains how the benchmark harness is organized and how each piece maps to the experiments in the proposal.

## High-level topology

```
                                                ┌────────────────────────────────┐
                                                │    Qdrant cluster (Docker)     │
                                                │                                │
   ┌──────────────────────────┐                 │   ┌────┐  ┌────┐  ┌────┐      │
   │   Go benchmark harness   │                 │   │ N1 │──│ N2 │──│ N3 │      │
   │                          │                 │   └────┘  └────┘  └────┘      │
   │  cmd/loader              │ ─── gRPC :633x ─┤    Raft consensus on :6335   │
   │  cmd/bench               │                 │    Shards 0..5 across nodes   │
   │  cmd/chaos               │ ─── HTTP :633x ─┤    Replication factor = 2     │
   │  cmd/groundtruth         │                 │                                │
   │                          │ ─── docker  ────┤    Volumes:                    │
   │                          │                 │     ./deploy/data/node{i}      │
   └──────────┬───────────────┘                 │     ./deploy/snapshots/node{i} │
              │                                 └────────────────────────────────┘
              ▼
   ┌──────────────────────────┐
   │  results/<run>/*.json    │
   │  results/<run>/plots/*   │
   └──────────────────────────┘
```

The benchmark process **never** runs inside the Qdrant containers — keeping it on the host means we get clean wall-clock latency including the network round-trip, which is what the proposal cares about.

## Package responsibilities

| Package | Responsibility |
|---|---|
| `internal/dataset` | Loads SIFT-128-Euclidean (`train`, `test`, `neighbors`) from the HDF5 file into row-major `[]float32` / `[]int32` buffers. |
| `internal/qclient` | Thin wrapper over `github.com/qdrant/go-client/qdrant` exposing the five proposal-mandated APIs (Add, Search, TopK, Delete, AddMany) plus collection management. Maintains a *pool* of one gRPC client per cluster node and round-robins requests. |
| `internal/workload` | Six YCSB-inspired profile generators (A–F) plus the shared dispatcher that picks an op per goroutine and times it. |
| `internal/metrics` | HDR histograms for accurate p99/p999 plus per-second buckets so we can plot tail latency *over time* (essential for chaos visualizations). |
| `internal/recall` | Validates ANN quality by comparing returned IDs to the ground-truth top-100 in the HDF5 file. |
| `internal/chaos` | Fault primitives (`docker kill`, iptables partition) and a `RecoveryWatcher` that polls every surviving node's `/cluster` endpoint to detect Raft convergence. |
| `cmd/loader` | One-shot bulk-load of SIFT-1M into a freshly-created collection. Exposes the optimizer / HNSW knobs as flags. |
| `cmd/bench` | Sweeps `(workload, concurrency, batch_size)` cells and writes a `metrics.Result` JSON per cell. |
| `cmd/chaos` | Runs a workload, injects a fault mid-stream, captures the leader-election timeline, and dumps everything for offline analysis. |
| `cmd/groundtruth` | Standalone recall@K validator. |

## Data flow for a single benchmark cell

1. `cmd/bench` parses flags → connects all gRPC pool members → confirms collection exists.
2. Picks the requested `Profile` (one of `workload.NewA…NewF`).
3. **Warmup**: runs the profile for `--warmup` (default 5s), discards the metrics. This ensures HNSW caches and Qdrant's internal worker pools are warm before we start timing.
4. **Measurement**: runs for `--duration`. Each goroutine in the workload pool independently
   - picks an op kind from the configured CDF
   - dispatches it via `qclient`
   - records `(latency, ok)` into the matching `metrics.Recorder`
5. After the deadline, `Snapshot()` produces a `metrics.Result` for every op kind that was exercised.
6. `metrics.SaveJSON` writes `results/<run>/<profile>_c<N>_b<B>.json`.

The HDR histograms are mutex-protected; on a 16-vCPU laptop the per-call locking overhead is below 1µs and well under the gRPC round-trip floor (~200µs for a 1-shard search), so it does not bias the measurement.

## Chaos experiment data flow

```
   t=0s     t=30s             t=30s+Δ                      t=90s
    │        │                   │                           │
    │  run starts          fault injected           recovery starts
    │                            │                           │
    ▼                            ▼                           ▼
    ┌─────────┬──────────────────┬──────────────────────────┬──────────────┐
    │ warmup  │ steady-state     │ degraded                 │ healed       │
    └─────────┴──────────────────┴──────────────────────────┴──────────────┘
                                  │                           │
                                  ▼                           ▼
                       RecoveryWatcher polls         Same watcher keeps
                       every surviving node's        polling until full
                       /cluster every 100ms.         3-of-3 convergence.
                       Stable when leader is
                       agreed for 1s.
```

`internal/chaos/RecoveryWatcher` is the key component. The convergence rule is conservative: every surviving node must independently report the *same* nonzero leader peer ID for `StableHoldTime` (1s default). This avoids the false-positive where one node briefly reports a stale leader from before the partition.

The latency recorder was already running through all of this. Because every observation is bucketed into a 1-second slot indexed from the run start, we can plot p99 vs second-of-experiment, which is exactly the chart the proposal asks for ("the spike in p99 read latency during the recovery phase").

## Mapping experiments to proposal requirements

| Proposal requirement | Implementation |
|---|---|
| Throughput (QPS) and tail latencies (p95/p99) under concurrent read/write | `cmd/bench -workload A,B,C,D,E,F -concurrency 1,10,15,25,30` |
| Cluster recovery time (Raft leader-election duration) | `cmd/chaos` records elapsed time from `KillContainer` to `WaitForStableLeader` returning |
| Performance degradation during node failures | The continuous workload's per-second buckets give p99 timeline through the chaos event |
| Impact of segment compaction on CPU contention / query latency | `scripts/run_compaction_sweep.sh` re-creates the collection at four `max_segment_size_kb` values and re-runs Workload B |
| Replication factor & write consistency tradeoffs (3-node and 5-node) | `scripts/run_full_sweep.sh` walks `(rf, wcf) ∈ {(1,1),(2,1),(2,2),(3,1),(3,2),(3,3)}` on the 3-node cluster and `(3,2),(5,1),(5,3),(5,5)` on the 5-node cluster |
| Network partition / chaos | `cmd/chaos -mode partition` uses iptables on the Raft P2P port |
| Bottleneck localization (network / embedding / index / query) | The harness measures end-to-end RPC time. We don't generate embeddings (SIFT comes pre-computed), so we have decomposed the pipeline correctly: client→server is network; server-side time = parse + WAL + indexing + reply. To split server-side, set `Wait=true` (forces ack-after-indexing) and compare to `Wait=false` (ack-after-WAL); the delta is indexing time. |
| Batch size impact | `cmd/bench -batch 1,16,64,256,1024` |
| Concurrent operations at 1, 10, 15, 25, 30 | Native `-concurrency` flag |

## Why these design choices

**Round-robin gRPC pool, not a single entry node.** A single entry node becomes a parse/dispatch bottleneck at high concurrency, hiding the cluster's actual throughput ceiling. Round-robin distributes the "coordinator" role across nodes, which more closely matches a real production deployment behind a load balancer.

**HDR histograms, not a percentile estimator.** The proposal asks for p99 and we may want p999 too. T-digest and similar streaming estimators introduce ~1% relative error which is fine in averaging, but we want to compare absolute tail latencies between two configs — error bars need to be tighter than the effect size.

**Per-second buckets in addition to whole-run summary.** For chaos experiments, the steady-state assumption breaks. We need to see the *shape* of the latency excursion, not just its average.

**HDF5 reads in Go via `gonum.org/v1/hdf5`.** Avoids round-tripping through Python and a JSON intermediate file. The whole 501MB SIFT file is loaded once into RAM at startup; for a benchmark process with a few GB of headroom, this is acceptable and lets the workload generators randomly index into the dataset without disk I/O confounding the measurements.

**`docker kill` (SIGKILL), not `docker stop`.** Stop sends SIGTERM and gives Qdrant up to 10 seconds to shutdown gracefully — which would let it flush WAL and gracefully transfer leadership, defeating the chaos experiment. We want the closest analog to a hardware failure.

**Containers run with `restart: "no"`.** Otherwise Docker's default restart policy would race with our chaos harness's explicit `docker start`, producing nondeterministic timelines.
