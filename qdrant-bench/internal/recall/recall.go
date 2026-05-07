// Package recall validates how accurate Qdrant's HNSW search is for the
// experiment's collection configuration.
//
// We pull each query in sift.Test, run a top-K search against the cluster,
// and compute |intersect(returned_ids, ground_truth_top_K)| / K. Average
// over all 10K queries is the recall@K number we report alongside latency.
//
// Why measure this at all? The proposal asks us to study throughput and
// latency, but those numbers are meaningless without recall: a lower-quality
// HNSW index will be FASTER but return wrong neighbors. Reporting (latency,
// recall) jointly is the standard ANN-Benchmarks methodology.
package recall

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sai/qdrant-bench/internal/dataset"
	"github.com/sai/qdrant-bench/internal/qclient"
)

// Result summarizes a recall sweep across the test split.
type Result struct {
	K            int     `json:"k"`
	Queries      int     `json:"queries"`
	MeanRecall   float64 `json:"mean_recall"`
	MinRecall    float64 `json:"min_recall"`
	ZeroRecallCt int     `json:"zero_recall_count"` // queries that got 0/K right - sanity check
}

// Compute runs sift.NumTest queries against the cluster, returns the mean
// recall@K. Concurrency is the number of parallel queries in flight. We
// keep this single-collection / single-shot and trust qclient's connection
// pool to spread load.
func Compute(ctx context.Context, c *qclient.Client, collection string, sift *dataset.SIFT, k int, concurrency int) (Result, error) {
	if k <= 0 {
		return Result{}, fmt.Errorf("recall: k must be > 0")
	}
	if k > sift.NumNeighbors {
		return Result{}, fmt.Errorf("recall: k=%d exceeds ground-truth depth %d", k, sift.NumNeighbors)
	}

	type slot struct{ recall float64; zero bool }
	results := make([]slot, sift.NumTest)

	var wg sync.WaitGroup
	tokens := make(chan struct{}, concurrency)
	var firstErr atomic.Value

	for i := 0; i < sift.NumTest; i++ {
		// Bail out if anyone already failed.
		if e, ok := firstErr.Load().(error); ok && e != nil {
			break
		}
		tokens <- struct{}{}
		wg.Add(1)
		go func(qi int) {
			defer wg.Done()
			defer func() { <-tokens }()

			vec := sift.TestVec(qi)
			hits, err := c.Search(ctx, collection, vec, uint64(k), nil)
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}

			// Build a small set of ground-truth IDs for this query
			gt := sift.NeighborsOf(qi)[:k]
			gtSet := make(map[uint64]struct{}, k)
			for _, g := range gt {
				gtSet[uint64(g)] = struct{}{}
			}

			matches := 0
			for _, h := range hits {
				if _, ok := gtSet[h.Id.GetNum()]; ok {
					matches++
				}
			}
			r := float64(matches) / float64(k)
			results[qi] = slot{recall: r, zero: matches == 0}
		}(i)
	}
	wg.Wait()
	if e, ok := firstErr.Load().(error); ok && e != nil {
		return Result{}, e
	}

	var sum float64
	min := 1.0
	zeroCt := 0
	for _, r := range results {
		sum += r.recall
		if r.recall < min {
			min = r.recall
		}
		if r.zero {
			zeroCt++
		}
	}
	return Result{
		K:            k,
		Queries:      sift.NumTest,
		MeanRecall:   sum / float64(sift.NumTest),
		MinRecall:    min,
		ZeroRecallCt: zeroCt,
	}, nil
}
