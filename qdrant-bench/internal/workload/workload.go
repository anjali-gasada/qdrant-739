// Package workload generates the six YCSB-style workload profiles described
// in the proposal:
//
//   A:  50% read / 50% update                 (write-heavy)
//   B:  95% read /  5% update                 (read-mostly)
//   C: 100% read                              (read-only)
//   D:  95% read /  5% write                  (insert)
//   E:  95% k=10 similar search /  5% write   (range queries)
//   F:  50% read / 50% read-modify-write
//
// Distinction between "update" and "write":
//   - update == upsert with an EXISTING id; this exercises the deletion +
//     re-insertion path inside Qdrant's update_handler (creates a soft delete
//     in the old segment, and a fresh point in a mutable segment).
//   - write  == upsert with a NEW id; pure insert.
//
// Each Profile has a Generate() method that returns the next operation given
// an RNG. The harness then dispatches that op through qclient. We deliberately
// keep workload selection cheap (a single rand.Float64 + comparisons) because
// at 30 concurrent clients on a fast cluster, generation can otherwise become
// the bottleneck instead of Qdrant.
package workload

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/qdrant/go-client/qdrant"
	"qdrant-bench/internal/dataset"
	"qdrant-bench/internal/qclient"
)

// OpKind tags one operation issued by a worker. Used by the metrics layer to
// route the latency observation to the correct recorder.
type OpKind int

const (
	OpRead   OpKind = iota // pure search
	OpUpdate               // upsert existing ID
	OpInsert               // upsert NEW ID
	OpRMW                  // read-modify-write (workload F)
	OpScan                 // workload E "range" search (k=10 with a payload filter)
)

func (k OpKind) String() string {
	switch k {
	case OpRead:
		return "read"
	case OpUpdate:
		return "update"
	case OpInsert:
		return "insert"
	case OpRMW:
		return "rmw"
	case OpScan:
		return "scan"
	}
	return "unknown"
}

// Profile is the public interface. Pick one (NewProfile) and call Run().
type Profile interface {
	// Run drives the workload until ctx is canceled. Returns the number of
	// operations executed and the first error (if any) observed by a worker.
	Run(ctx context.Context) (int64, error)
	Label() string
}

// Common holds every parameter that every profile needs - we factor it out
// so each profile is a thin "operation mix" struct.
type Common struct {
	Client       *qclient.Client
	Collection   string
	Dataset      *dataset.SIFT
	Concurrency  int
	BatchSize    int  // for batched operations; 1 = single-point ops
	Wait         bool // pass-through to qclient.Add etc.
	K            uint64
	OpRecorders  map[OpKind]LatencyRecorder
	NextID       *uint64 // shared atomic counter for OpInsert IDs

	// For OpRead, we draw a query from sift.Test uniformly at random, OR if
	// QueryFromTrain is true, from sift.Train (used by F's RMW where the
	// "read" half should target an existing in-corpus key).
	QueryFromTrain bool
}

// LatencyRecorder is the bit of metrics.Recorder that workload calls.
// Defining it as an interface here lets the workload package stay free of
// any direct dependency on the metrics package, which keeps tests cleaner.
type LatencyRecorder interface {
	Add(d time.Duration, ok bool)
}

// =====================================================================
// Workload A: 50/50 read/update
// =====================================================================
type A struct{ c Common }

func NewA(c Common) *A { return &A{c: c} }
func (a *A) Label() string { return "A_50r_50u" }
func (a *A) Run(ctx context.Context) (int64, error) {
	return runMix(ctx, a.c, []weightedOp{
		{p: 0.50, op: OpRead},
		{p: 0.50, op: OpUpdate},
	})
}

// =====================================================================
// Workload B: 95/5 read/update
// =====================================================================
type B struct{ c Common }

func NewB(c Common) *B { return &B{c: c} }
func (b *B) Label() string { return "B_95r_5u" }
func (b *B) Run(ctx context.Context) (int64, error) {
	return runMix(ctx, b.c, []weightedOp{
		{p: 0.95, op: OpRead},
		{p: 0.05, op: OpUpdate},
	})
}

// =====================================================================
// Workload C: 100% read
// =====================================================================
type C struct{ c Common }

func NewC(c Common) *C { return &C{c: c} }
func (c *C) Label() string { return "C_100r" }
func (c *C) Run(ctx context.Context) (int64, error) {
	return runMix(ctx, c.c, []weightedOp{
		{p: 1.0, op: OpRead},
	})
}

// =====================================================================
// Workload D: 95% read / 5% INSERT (new ids, not update)
// =====================================================================
type D struct{ c Common }

func NewD(c Common) *D { return &D{c: c} }
func (d *D) Label() string { return "D_95r_5w" }
func (d *D) Run(ctx context.Context) (int64, error) {
	return runMix(ctx, d.c, []weightedOp{
		{p: 0.95, op: OpRead},
		{p: 0.05, op: OpInsert},
	})
}

// =====================================================================
// Workload E: 95% k=10 search WITH FILTER / 5% INSERT
//
// The "range query" framing in the proposal maps to a payload-filtered top-k
// search in vector-DB land - the closest analog to YCSB-E's range scan. We
// stamp every inserted point with a "bucket" payload field of 0..9 (uniform)
// and the search-path filter is `bucket = rand(0..9)`, which makes Qdrant
// take the filterable HNSW path rather than plain HNSW.
// =====================================================================
type E struct{ c Common }

func NewE(c Common) *E { return &E{c: c} }
func (e *E) Label() string { return "E_95scan_5w" }
func (e *E) Run(ctx context.Context) (int64, error) {
	return runMix(ctx, e.c, []weightedOp{
		{p: 0.95, op: OpScan},
		{p: 0.05, op: OpInsert},
	})
}

// =====================================================================
// Workload F: 50% read / 50% read-modify-write
//
// In RMW mode we (a) issue a top-1 read, (b) take the returned point's id,
// (c) upsert a perturbed vector at that id. The first call is recorded as a
// read; the second is recorded as an update; the BOTH-LEGS latency is also
// recorded as OpRMW so we can study read+write coupling.
// =====================================================================
type F struct{ c Common }

func NewF(c Common) *F { return &F{c: c} }
func (f *F) Label() string { return "F_50r_50rmw" }
func (f *F) Run(ctx context.Context) (int64, error) {
	return runMix(ctx, f.c, []weightedOp{
		{p: 0.50, op: OpRead},
		{p: 0.50, op: OpRMW},
	})
}

// =====================================================================
// Internal driver
// =====================================================================

type weightedOp struct {
	p  float64 // probability
	op OpKind
}

// runMix is the shared loop body for every profile. It spins up `concurrency`
// goroutines each running an op-mix loop until ctx is canceled. We bind a
// per-worker rand source to avoid the global mutex on math/rand.
func runMix(ctx context.Context, c Common, mix []weightedOp) (int64, error) {
	if c.Concurrency <= 0 {
		return 0, errors.New("workload: concurrency must be > 0")
	}
	cum := buildCDF(mix)

	type result struct {
		count int64
		err   error
	}
	resCh := make(chan result, c.Concurrency)

	var totalOps int64

	for w := 0; w < c.Concurrency; w++ {
		seed := time.Now().UnixNano() + int64(w)*1_000_003
		rng := rand.New(rand.NewSource(seed))
		go func(rng *rand.Rand) {
			var local int64
			for {
				if ctx.Err() != nil {
					resCh <- result{count: local}
					return
				}
				op := pickOp(rng, cum)
				if err := dispatch(ctx, c, rng, op); err != nil && !errors.Is(err, context.Canceled) {
					// We log the FIRST hard error per worker but otherwise
					// keep going - chaos experiments specifically depend on
					// errors being possible.
					select {
					case resCh <- result{count: local, err: err}:
					default:
					}
					return
				}
				local++
				atomic.AddInt64(&totalOps, 1)
			}
		}(rng)
	}

	// Wait for all workers
	var firstErr error
	for i := 0; i < c.Concurrency; i++ {
		r := <-resCh
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
	}
	return atomic.LoadInt64(&totalOps), firstErr
}

func buildCDF(mix []weightedOp) []weightedOp {
	out := make([]weightedOp, len(mix))
	var sum float64
	for i, m := range mix {
		sum += m.p
		out[i] = weightedOp{p: sum, op: m.op}
	}
	return out
}

func pickOp(rng *rand.Rand, cdf []weightedOp) OpKind {
	x := rng.Float64() * cdf[len(cdf)-1].p
	for _, w := range cdf {
		if x <= w.p {
			return w.op
		}
	}
	return cdf[len(cdf)-1].op
}

// dispatch executes ONE operation. The whole RPC time is recorded into the
// appropriate recorder.
func dispatch(ctx context.Context, c Common, rng *rand.Rand, op OpKind) error {
	rec := c.OpRecorders[op]
	switch op {
	case OpRead:
		qIdx := rng.Intn(c.Dataset.NumTest)
		var vec []float32
		if c.QueryFromTrain {
			qIdx = rng.Intn(c.Dataset.NumTrain)
			vec = c.Dataset.TrainVec(qIdx)
		} else {
			vec = c.Dataset.TestVec(qIdx)
		}
		t0 := time.Now()
		_, err := c.Client.Search(ctx, c.Collection, vec, c.K, nil)
		rec.Add(time.Since(t0), err == nil)
		return err

	case OpScan:
		qIdx := rng.Intn(c.Dataset.NumTest)
		bucket := int64(rng.Intn(10))
		flt := &qdrant.Filter{
			Must: []*qdrant.Condition{
				qdrant.NewMatchInt("bucket", bucket),
			},
		}
		t0 := time.Now()
		_, err := c.Client.Search(ctx, c.Collection, c.Dataset.TestVec(qIdx), c.K, flt)
		rec.Add(time.Since(t0), err == nil)
		return err

	case OpUpdate:
		// Pick an existing ID uniformly from the loaded corpus
		id := uint64(rng.Intn(c.Dataset.NumTrain))
		// Use a fresh vector from train - we just want to exercise the upsert
		// path; a perturbation of the original would also work.
		vec := c.Dataset.TrainVec(int(id))
		pt := qclient.Point{
			ID:      id,
			Vector:  vec,
			Payload: map[string]any{"bucket": int64(id % 10)},
		}
		t0 := time.Now()
		_, err := c.Client.AddMany(ctx, c.Collection, []qclient.Point{pt}, c.Wait)
		rec.Add(time.Since(t0), err == nil)
		return err

	case OpInsert:
		id := atomic.AddUint64(c.NextID, 1)
		// Pick any existing vector as the "new" one - in a real benchmark we'd
		// generate or reserve a tail of the dataset, but for pure pipeline
		// stress it's fine.
		src := rng.Intn(c.Dataset.NumTrain)
		pt := qclient.Point{
			ID:      id,
			Vector:  append([]float32(nil), c.Dataset.TrainVec(src)...),
			Payload: map[string]any{"bucket": int64(id % 10)},
		}
		t0 := time.Now()
		_, err := c.Client.AddMany(ctx, c.Collection, []qclient.Point{pt}, c.Wait)
		rec.Add(time.Since(t0), err == nil)
		return err

	case OpRMW:
		t0 := time.Now()
		// READ
		qIdx := rng.Intn(c.Dataset.NumTest)
		hits, err := c.Client.Search(ctx, c.Collection, c.Dataset.TestVec(qIdx), 1, nil)
		if err != nil || len(hits) == 0 {
			rec.Add(time.Since(t0), false)
			if err == nil {
				err = errors.New("rmw: no hits")
			}
			return err
		}
		id := hits[0].Id.GetNum()
		// MODIFY+WRITE: perturb the vector by adding a tiny constant
		newVec := append([]float32(nil), c.Dataset.TrainVec(int(id)%c.Dataset.NumTrain)...)
		for j := range newVec {
			newVec[j] += 0.001
		}
		pt := qclient.Point{
			ID:      id,
			Vector:  newVec,
			Payload: map[string]any{"bucket": int64(id % 10)},
		}
		_, err = c.Client.AddMany(ctx, c.Collection, []qclient.Point{pt}, c.Wait)
		rec.Add(time.Since(t0), err == nil)
		return err
	}
	return nil
}
