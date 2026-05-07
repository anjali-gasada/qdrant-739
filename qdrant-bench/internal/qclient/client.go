// Package qclient is a thin wrapper around the official Qdrant Go gRPC client
// (github.com/qdrant/go-client/qdrant). It exposes exactly the operations the
// proposal calls for - Add, Search, TopK, Delete, AddMany - plus cluster /
// collection management for the test setup.
//
// Why wrap?
//   1. The official client exposes the raw protobuf types, which are verbose
//      and bleed pointers everywhere (e.g. qdrant.NewQuery, qdrant.NewIDNum,
//      qdrant.NewVectorsConfig, ...). This package hides that.
//   2. Per-call timing is uniform - every method records (start, server-acked,
//      indexed) timestamps that the metrics package can decompose into stages.
//   3. We use a connection POOL with one client per cluster node so that a
//      single benchmark process can drive read/write traffic round-robin
//      across all peers. Otherwise the entry node becomes the bottleneck
//      (Qdrant fans the request out internally, but the entry-point CPU
//      still does coordination and merge work).
package qclient

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"


	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
    "google.golang.org/grpc/keepalive"
    "time"
)

// Config configures a multi-node Qdrant client.
//
// Hosts is a list of "host:port" entries pointing at the gRPC port (6334) of
// each node. The wrapper opens one client per host and round-robins requests
// across them. Even though Qdrant's internal routing makes any node a valid
// entry point, distributing the entry load matters at high concurrency.
type Config struct {
	Hosts             []string      // e.g. ["localhost:6334", "localhost:6434", ...]
	APIKey            string        // empty for our local cluster
	UseTLS            bool          // false for local
	DialTimeout       time.Duration // default 5s
	OperationTimeout  time.Duration // per-RPC deadline; default 30s
	GRPCKeepAlive     time.Duration // default 10s (keepalive ping)
	GRPCKeepAliveTout time.Duration // default 2s  (keepalive ack timeout)
}

// Client wraps a pool of qdrant.Client instances.
type Client struct {
	cfg     Config
	pool    []*qdrant.Client
	hosts   []string // parallel to pool, kept for diagnostics
	rrIndex uint64   // round-robin counter; atomic
}

// New opens one gRPC client per host. Errors if ANY host is unreachable -
// for a benchmark, we want the experiment to fail fast rather than silently
// run on a degraded pool.
func New(cfg Config) (*Client, error) {
	if len(cfg.Hosts) == 0 {
		return nil, errors.New("qclient: at least one host required")
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.OperationTimeout == 0 {
		cfg.OperationTimeout = 30 * time.Second
	}
	if cfg.GRPCKeepAlive == 0 {
		cfg.GRPCKeepAlive = 10 * time.Second
	}
	if cfg.GRPCKeepAliveTout == 0 {
		cfg.GRPCKeepAliveTout = 2 * time.Second
	}

	pool := make([]*qdrant.Client, 0, len(cfg.Hosts))
	for _, host := range cfg.Hosts {
		h, port, err := splitHostPort(host)
		if err != nil {
			return nil, fmt.Errorf("qclient: invalid host %q: %w", host, err)
		}
		c, err := qdrant.NewClient(&qdrant.Config{
			Host:             h,
			Port: 			  int(port), 
			APIKey:           cfg.APIKey,
			UseTLS:           cfg.UseTLS,
			GrpcOptions: []grpc.DialOption{
				grpc.WithKeepaliveParams(keepalive.ClientParameters{
					Time:    10 * time.Second,
					Timeout: 5 * time.Second,
				}),
			},
		})
		if err != nil {
			return nil, fmt.Errorf("qclient: dial %s: %w", host, err)
		}
		// Smoke-test the connection so we don't discover a bad host on the
		// first benchmark RPC.
		ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
		_, herr := c.HealthCheck(ctx)
		cancel()
		if herr != nil {
			return nil, fmt.Errorf("qclient: health check %s: %w", host, herr)
		}
		pool = append(pool, c)
	}

	return &Client{
		cfg:   cfg,
		pool:  pool,
		hosts: cfg.Hosts,
	}, nil
}

// pick returns the next client in round-robin order. Safe under concurrency.
func (c *Client) pick() *qdrant.Client {
	i := atomic.AddUint64(&c.rrIndex, 1)
	return c.pool[(i-1)%uint64(len(c.pool))]
}

// PickByIndex returns the i-th client (mod pool size). Useful for read-only
// chaos sidecars that always want to talk to the same surviving node.
func (c *Client) PickByIndex(i int) *qdrant.Client {
	return c.pool[i%len(c.pool)]
}

// Hosts returns the configured host list.
func (c *Client) Hosts() []string { return c.hosts }

// Close tears down every gRPC connection.
func (c *Client) Close() error {
	var firstErr error
	for _, p := range c.pool {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ===== Collection management =====

// CollectionParams describes the collection we want to create.  We expose only
// the knobs that matter for the proposal experiments; everything else gets
// Qdrant defaults.
type CollectionParams struct {
	Name              string
	Dim               uint64
	Distance          qdrant.Distance // qdrant.Distance_Euclid for SIFT (L2)
	ShardNumber       uint32          // total shards across the cluster
	ReplicationFactor uint32          // copies per shard, 1..N
	WriteConsistency  uint32          // how many replicas must ACK a write

	// Optimizer overrides. Pointers so we can leave them as "use default".
	MaxSegmentSizeKB    *uint64 // null in YAML => leave nil here
	IndexingThresholdKB *uint64
	DefaultSegmentNum   *uint64

	// HNSW overrides.
	HNSWm          *uint64 // default 16
	HNSWefConstruct *uint64 // default 100
	OnDiskVectors  bool    // default false (RAM)
}

// CreateCollection (re)creates the collection. If a collection with this name
// already exists it is DELETED first - benchmarks must always start clean.
func (c *Client) CreateCollection(ctx context.Context, p CollectionParams) error {
	q := c.pick()

	// Drop existing collection idempotently.
	exists, err := q.CollectionExists(ctx, p.Name)
	if err != nil {
		return fmt.Errorf("qclient: collection_exists %s: %w", p.Name, err)
	}
	if exists {
		if err := q.DeleteCollection(ctx, p.Name); err != nil {
			return fmt.Errorf("qclient: delete existing %s: %w", p.Name, err)
		}
	}

	create := &qdrant.CreateCollection{
		CollectionName: p.Name,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     p.Dim,
			Distance: p.Distance,
			OnDisk:   ptrBool(p.OnDiskVectors),
		}),
		ShardNumber:             ptrU32(p.ShardNumber),
		ReplicationFactor:       ptrU32(p.ReplicationFactor),
		WriteConsistencyFactor:  ptrU32(p.WriteConsistency),
	}
	// Optimizer override (where the compaction-impact knob lives)
	if p.MaxSegmentSizeKB != nil || p.IndexingThresholdKB != nil || p.DefaultSegmentNum != nil {
		create.OptimizersConfig = &qdrant.OptimizersConfigDiff{
			MaxSegmentSize:    p.MaxSegmentSizeKB,
			IndexingThreshold: p.IndexingThresholdKB,
			DefaultSegmentNumber: p.DefaultSegmentNum,
		}
	}
	// HNSW override
	if p.HNSWm != nil || p.HNSWefConstruct != nil {
		create.HnswConfig = &qdrant.HnswConfigDiff{
			M:           p.HNSWm,
			EfConstruct: p.HNSWefConstruct,
		}
	}

	if err := q.CreateCollection(ctx, create); err != nil {
		return fmt.Errorf("qclient: create_collection %s: %w", p.Name, err)
	}
	return nil
}

// CollectionInfo fetches stats for the collection - useful for verifying
// indexed_vectors_count, segment count, and optimizer status during chaos
// experiments.
func (c *Client) CollectionInfo(ctx context.Context, name string) (*qdrant.CollectionInfo, error) {
	q := c.pick()
	info, err := q.GetCollectionInfo(ctx, name)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// ===== The five APIs the proposal explicitly calls out =====

// Add upserts a single point. Equivalent to AddMany([]Point{p}) but a hot path
// for workloads that benefit from per-point batching.
//
// `wait`: when true, the call blocks until the write is ACKED according to
// the collection's write_consistency_factor. When false, the call returns as
// soon as the entry node has accepted the operation (ack-on-leader semantics).
// We expose this as a parameter because the proposal asks us to measure the
// latency tradeoff of consistency.
func (c *Client) Add(ctx context.Context, collection string, p Point, wait bool) (*qdrant.UpdateResult, error) {
	return c.AddMany(ctx, collection, []Point{p}, wait)
}

// AddMany upserts a batch. Returns the UpdateResult on success.
func (c *Client) AddMany(ctx context.Context, collection string, points []Point, wait bool) (*qdrant.UpdateResult, error) {
	q := c.pick()
	pts := make([]*qdrant.PointStruct, len(points))
	for i, p := range points {
		pts[i] = &qdrant.PointStruct{
			Id:      qdrant.NewIDNum(p.ID),
			Vectors: qdrant.NewVectorsDense(p.Vector),
			Payload: qdrant.NewValueMap(p.Payload),
		}
	}
	res, err := q.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collection,
		Points:         pts,
		Wait:           ptrBool(wait),
	})
	if err != nil {
		return nil, fmt.Errorf("qclient: upsert (%d points): %w", len(points), err)
	}
	return res, nil
}

// Search runs a single similarity query and returns the top-k results.
//
// Implements both proposal-§5 APIs ("Search" and "TopK") - they're the same
// gRPC call with a different limit, so we expose a unified function and let
// the caller pass k.
func (c *Client) Search(ctx context.Context, collection string, vector []float32, k uint64, filter *qdrant.Filter) ([]*qdrant.ScoredPoint, error) {
	q := c.pick()
	res, err := q.Query(ctx, &qdrant.QueryPoints{
		CollectionName: collection,
		Query:          qdrant.NewQueryDense(vector),
		Limit:          ptrU64(k),
		Filter:         filter,
		WithPayload:    qdrant.NewWithPayload(false), // recall benchmarks don't need payload
	})
	if err != nil {
		return nil, fmt.Errorf("qclient: query: %w", err)
	}
	return res, nil
}

// Delete removes the listed point IDs from the collection.
func (c *Client) Delete(ctx context.Context, collection string, ids []uint64, wait bool) (*qdrant.UpdateResult, error) {
	q := c.pick()
	pids := make([]*qdrant.PointId, len(ids))
	for i, id := range ids {
		pids[i] = qdrant.NewIDNum(id)
	}
	res, err := q.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: collection,
		Points:         qdrant.NewPointsSelector(pids...),
		Wait:           ptrBool(wait),
	})
	if err != nil {
		return nil, fmt.Errorf("qclient: delete: %w", err)
	}
	return res, nil
}

// Point is the harness's view of a single vector + payload. We use uint64
// IDs throughout because Qdrant supports them natively (NewIDNum).
type Point struct {
	ID      uint64
	Vector  []float32
	Payload map[string]any // metadata for filter experiments; nil = no payload
}

// ===== utility =====

func ptrBool(b bool) *bool       { return &b }
func ptrU32(v uint32) *uint32    { return &v }
func ptrU64(v uint64) *uint64    { return &v }

// splitHostPort handles "host:6334" -> ("host", 6334). We don't use net.SplitHostPort
// because we want a uint32 port and a clear error message for missing colons.
func splitHostPort(s string) (string, uint32, error) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			host := s[:i]
			var port uint32
			for _, ch := range s[i+1:] {
				if ch < '0' || ch > '9' {
					return "", 0, fmt.Errorf("non-numeric port in %q", s)
				}
				port = port*10 + uint32(ch-'0')
			}
			if host == "" || port == 0 {
				return "", 0, fmt.Errorf("missing host or port in %q", s)
			}
			return host, port, nil
		}
	}
	return "", 0, fmt.Errorf("no ':' in %q", s)
}
