# Qdrant Distributed Systems Benchmark

A comprehensive Go-based benchmarking harness for evaluating Qdrant — a high-performance, open-source distributed vector database — across the metrics specified in the project proposal: performance & scalability, fault tolerance & recovery, and internal behavior (segment compaction effects).

## What this project does

This repository implements the entire experimental pipeline described in the proposal:

1. **Stands up** a multi-node Qdrant cluster (3-node and 5-node variants) using Docker Compose with Raft consensus enabled.
2. **Loads** the SIFT-128-Euclidean (1M × 128-d, L2 distance) ANN benchmark dataset.
3. **Runs** the six YCSB-inspired workloads (A through F) at varying client concurrency (1, 10, 15, 25, 30).
4. **Measures** throughput (QPS), p50/p95/p99/p999 latencies, and recall@10 against ground-truth neighbors.
5. **Injects faults** (process kills, network partitions) during sustained load and measures Raft leader-election time and the latency spike during recovery.
6. **Sweeps** internal optimizer parameters (`max_segment_size_kb`) to quantify the impact of background segment compaction on tail latency.
7. **Decomposes** end-to-end write/read latency into pipeline stages (network → embedding → upsert ack → indexing) to find bottlenecks.

All experiment code is in Go. Orchestration uses Docker Compose. Plots and reports are generated from the JSON output.

## Quick start

```bash
# 1. Bring up a 3-node cluster with replication factor 2
make cluster-up-3

# 2. Download SIFT-1M and load it into the cluster
make data
make load

# 3. Run the full benchmark suite (workloads A-F × concurrencies 1,10,15,25,30)
make bench

# 4. Run chaos experiments (kill a node mid-write, measure recovery)
make chaos

# 5. Sweep segment-size limits to study compaction impact
make compaction-sweep

# 6. Generate plots and the final report
make report
```

## Directory layout

```
qdrant-bench/
├── cmd/
│   ├── bench/         # Main benchmark harness entry point
│   ├── loader/        # Dataset loader (SIFT HDF5 → Qdrant)
│   ├── chaos/         # Fault injection orchestrator
│   └── groundtruth/   # Recall validation against true neighbors
├── internal/
│   ├── qclient/       # Wrapper around the official Qdrant Go gRPC client
│   ├── dataset/       # HDF5 reader for SIFT-128-euclidean
│   ├── workload/      # Six YCSB-style workload generators (A–F)
│   ├── metrics/       # HDR histograms, per-stage timing, JSON sinks
│   ├── chaos/         # Docker / network fault injection primitives
│   └── recall/        # Recall@k against ground truth
├── deploy/            # Docker-Compose files for 3-node and 5-node clusters
├── configs/           # Qdrant production.yaml configurations
├── scripts/           # Data-download and helper shell scripts
├── results/           # Raw JSON output of all experiments
└── docs/              # Architecture diagrams, run-book, notes
```

## What we measure (mapping to proposal)

| Proposal item | Implementation |
|---|---|
| Throughput (QPS) and p95/p99 tail latency | `internal/metrics` HDR histogram, dumped per-workload as JSON |
| Replication factor & write-consistency tradeoff | `cmd/bench --replication-factor {1,2,3}` and `--write-consistency {1,majority,all}` |
| Cluster recovery / Raft leader-election time | `cmd/chaos` reads `GET /cluster` from each node every 100ms after kill, records the timestamp at which `raft_info.leader` becomes stable across the surviving majority |
| p99 read latency spike during recovery | Read load runs continuously across the chaos event; latency is bucketed into 1-second windows |
| Segment-size impact on read latency | `make compaction-sweep` varies `max_segment_size_kb` ∈ {10000, 50000, 200000, null} and re-runs Workload B |
| Pipeline-stage breakdown | `internal/metrics/stages.go` records timestamps at: client send, server-acked, indexed (via `wait=true`); reads decompose into network + filter + ANN search |
| Batch-size sensitivity | `--batch-size` flag swept over {1, 16, 64, 256, 1024} |
| Concurrency sweep | `--concurrency` flag swept over {1, 10, 15, 25, 30} |

## APIs implemented (proposal §5)

| Operation | Qdrant primitive used |
|---|---|
| Add | `Upsert` with single point |
| Search by query | `Query` (the unified replacement for `Search`) |
| Top-k similar | `Query` with `Limit: k` |
| Delete | `Delete` (point-id list) |
| Add many (batch) | `Upsert` with `[]*PointStruct` of size N |

## Dataset

SIFT-128-Euclidean from ann-benchmarks: 1,000,000 base vectors + 10,000 query vectors, dimension 128, distance L2 (Euclidean). Each query has 100 ground-truth nearest neighbors precomputed, which we use for recall validation. Download URL: `http://ann-benchmarks.com/sift-128-euclidean.hdf5` (~501 MB).

## References

- Project proposal (this repo): `docs/proposal.md`
- Qdrant distributed deployment: https://qdrant.tech/documentation/guides/distributed_deployment/
- ANN-Benchmarks: https://github.com/erikbern/ann-benchmarks
- Reference papers: `docs/references.md`

See `docs/architecture.md` for the full system architecture and `docs/runbook.md` for a step-by-step experiment guide.
