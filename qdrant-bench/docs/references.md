# References

## Papers cited in proposal

1. **arXiv:2509.12384** — Modern survey of distributed vector database architectures, with explicit treatment of HNSW + Raft tradeoffs and the broadcast-reduce query model. The "broadcast to all shards, reduce top-K at the coordinator" pattern in our `Search` API is taken directly from §4 of that paper.

2. **arXiv:2310.11703** — Benchmark study of vector index recall vs latency vs memory. Their HNSW parameter sweep (m ∈ {8, 16, 32}, ef_construct ∈ {64, 100, 200}) is the basis for our default of m=16, ef_construct=100.

## Qdrant documentation we relied on

- Distribution / sharding model — https://qdrant.tech/documentation/guides/distributed_deployment/
- Optimizer & segment configuration — https://qdrant.tech/documentation/concepts/optimizer/
- Write consistency factor — https://qdrant.tech/documentation/concepts/points/#write-ordering
- Go client library — https://github.com/qdrant/go-client
- gRPC API reference — https://api.qdrant.tech/api-reference/grpc

## Dataset

- **SIFT-1M (sift-128-euclidean)** — http://ann-benchmarks.com/sift-128-euclidean.hdf5
  - 1,000,000 train vectors, 10,000 test vectors, 128 dimensions, L2 (Euclidean) distance.
  - Ground-truth top-100 neighbors and distances are bundled in the same HDF5 file under the `neighbors` and `distances` datasets. We only use top-10 for recall@10 measurements.
  - The dataset is the de-facto standard for ANN benchmarks; results published against SIFT-1M are directly comparable to ann-benchmarks.com leaderboards.

## Workload profiles

The A-F profile letters and ratios match the YCSB convention (Yahoo! Cloud Serving Benchmark, B. Cooper et al., SoCC 2010), adapted for vector search semantics:

| Profile | Original YCSB intent       | Our adaptation                                |
|---------|----------------------------|-----------------------------------------------|
| A       | 50 read / 50 update         | Top-K search vs UPSERT-overwrite of an existing point |
| B       | 95 read / 5 update          | Same                                          |
| C       | 100 read                    | Pure top-K search                             |
| D       | 95 read / 5 insert          | Search vs INSERT of new point with fresh ID   |
| E       | 95 scan / 5 insert          | Filtered top-K search (payload bucket = X) vs insert |
| F       | 50 read / 50 RMW            | Read existing point, modify payload, upsert back |

Insertions in D and E append to the existing collection (IDs starting at `NumTrain = 1,000,000`).

## Tooling

- **HdrHistogram-Go v1.1.2** — https://github.com/HdrHistogram/hdrhistogram-go
  - Fixed-precision (3-significant-digit) latency recording. Required for accurate p99 / p999 because reservoir sampling distorts the tail. We bound recordable latencies to `[1 µs, 60 s]` which covers everything from sub-ms cache hits to chaos-induced timeouts.
- **gonum/hdf5** — https://pkg.go.dev/gonum.org/v1/hdf5
  - Pure Go HDF5 binding wrapping libhdf5. Reads contiguous 2D float32/int32 arrays directly into a flat `[]T` buffer, which is what we want for vector data.
- **Docker Compose v2** — for cluster orchestration. We chose Compose over Kubernetes because the cluster topology is fixed (3 or 5 nodes) and Compose lets us SIGKILL containers and run `iptables` inside them without a CNI plugin.

## Why these specific choices and not others

- **Why Qdrant and not Milvus / Weaviate / Vespa?** Qdrant's Raft consensus is exposed cleanly via REST (`/cluster`) and the leader/term fields are observable, which is what makes the recovery-time experiments tractable. Milvus uses etcd externally, so cluster recovery time becomes a property of etcd, not the vector DB. Weaviate's leader election is harder to observe from outside. Vespa is too heavyweight to spin up in compose for a class project.
- **Why Go and not Python?** GIL-bound Python clients become the bottleneck before Qdrant does at concurrency above ~10. A Go client with goroutines saturates the cluster and gives a true picture of server-side performance. Also: Qdrant publishes a first-party Go client.
- **Why HDR histograms and not a streaming reservoir?** Reservoir sampling has high variance at the 99th-percentile. HDR histograms record every observation in a bounded-precision bucket, and the memory cost is constant (~150 KB per histogram). For a 60-second 30-concurrency run that's billions of observations recorded with no loss of tail fidelity.
- **Why batch size 1024 default?** Empirically, on a 3-node cluster, throughput-per-vector flattens between batch=512 and batch=2048. 1024 is in the middle of the plateau. Smaller batches over-pay for request overhead; larger batches stress the gRPC frame size and start failing on default configurations.

## Recall validation

The HNSW algorithm is approximate, so any throughput/latency claim is meaningless without recall context. Our `groundtruth` binary computes recall@10 by:
1. Pulling the test set of 10K queries from the SIFT HDF5
2. Running each through `Search(top_k=10)`
3. Comparing to the bundled `neighbors[:10]` ground-truth
4. Reporting mean, min, and zero-recall count

A run is considered "valid" if mean recall ≥ 0.95 with the default parameters. This is the same threshold ann-benchmarks uses for its "recall = 0.95" comparison cells.
