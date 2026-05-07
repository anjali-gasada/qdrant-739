# Original Project Proposal

**Group Members:** Hemanjali Gasada, Prasanna Konyala, Sanjana Kallingal
**Option Picked:** Option 1 — Run, evaluate, and reason about an open-source distributed system.

## System Being Investigated

We will be investigating **Qdrant**, a high-performance, open-source distributed vector database built in Rust. We chose Qdrant because it sits at the intersection of classic distributed systems (Raft consensus, horizontal sharding, replication) and modern AI infrastructure (vector similarity, RAG workflows). We are particularly interested in exploring how the system balances the intense CPU requirements of building HNSW indexes with the network overhead of distributed broadcast-reduce queries, and how replication consistency settings, node failures, and background segment compaction interact and affect tail latency, throughput, and availability.

## Metrics of Focus

1. **Performance & Scalability** — throughput (QPS) and tail latencies (p95/p99) under concurrent read/write loads.
2. **Fault Tolerance & Recovery** — cluster recovery time (Raft leader election duration) and the performance degradation of read/write operations during node failures.
3. **Internal Behavior** — the impact of background segment compaction on CPU contention and query latency.

## Workloads and Parameters to Investigate

We simulate a real-world AI workload of dense vector similarity search paired with metadata payload filtering, and measure consistency-vs-fault-tolerance tradeoffs. We vary:

- **Replication Factor & Write Consistency** — 3-node and 5-node clusters; WCF varying from 1 (fast writes) to RF (synchronous replication). We measure exact latency and throughput penalties.
- **Fault Injection (Chaos Testing)** — during a sustained heavy insertion workload, kill a random node or partition the network. Measure leader-election time and the spike in p99 read latency during recovery.
- **Segment Size Limits** — tweak `max_segment_size_kb` to observe how more frequent compaction impacts ability to serve reads.
- **Pipeline Bottleneck Localization** — identify whether embedding generation, data insertion, index-building, or query runtime dominates end-to-end latency.
- **Batch Size** — measure latency/throughput as a function of batch size for reads and writes.
- **Concurrency** — clients = {1, 10, 15, 25, 30}, measure throughput response curve.

## Dataset

**SIFT-128-Euclidean** — the classic engineering benchmark.
- 1M corpus vectors + 10K query vectors, 128 dimensions, L2 distance
- Generated from SIFT image descriptors
- ~500 MB download
- URL: `http://ann-benchmarks.com/sift-128-euclidean.hdf5`
- Use `train` to load the dataset, `test` to run query workloads, `neighbors` for recall validation.

## Workload Profiles

| Letter | Mix |
|--------|-----|
| A | 50% read / 50% update (write-heavy) |
| B | 95% read / 5% update (read-mostly) |
| C | 100% read (read-only) |
| D | 95% read / 5% write (insert) |
| E | 95% k=10 similarity search / 5% write (range/filtered queries) |
| F | 50% read / 50% read-modify-write |

## APIs to Implement

1. `Add` — single-point upsert
2. `Search` — query by raw vector
3. `TopK` — get top-K most similar (parameterized K)
4. `Delete` — point deletion
5. `AddMany` — batched insert

## Reference Papers

- https://arxiv.org/pdf/2509.12384
- https://arxiv.org/pdf/2310.11703
