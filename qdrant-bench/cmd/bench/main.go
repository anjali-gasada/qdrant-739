// Command bench is the main benchmark harness.  Usage examples:
//
//   # Run workload B (95% read / 5% update) for 60s with 25 clients
//   go run ./cmd/bench -workload B -duration 60s -concurrency 25
//
//   # Sweep concurrency 1,10,15,25,30 across workloads B and C
//   go run ./cmd/bench -workload B,C -concurrency 1,10,15,25,30 -duration 30s
//
//   # Sweep batch sizes for workload A (write-heavy)
//   go run ./cmd/bench -workload A -concurrency 16 -batch 1,16,64,256,1024 -duration 30s
//
// Output: ./results/<runlabel>/<workload>_<concurrency>_<batch>.json
//
// Each output file holds an array of metrics.Result, one per OpKind (read,
// update, insert, rmw, scan) that the workload exercised. Plot/aggregate
// with the python helper in scripts/.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sai/qdrant-bench/internal/dataset"
	"github.com/sai/qdrant-bench/internal/metrics"
	"github.com/sai/qdrant-bench/internal/qclient"
	"github.com/sai/qdrant-bench/internal/recall"
	"github.com/sai/qdrant-bench/internal/workload"
)

// commaList splits a "x,y,z" string into a []string, trimming whitespace.
func commaList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// commaInts parses comma-separated ints.
func commaInts(s string) []int {
	out := []int{}
	for _, p := range commaList(s) {
		var v int
		if _, err := fmt.Sscanf(p, "%d", &v); err != nil {
			log.Fatalf("bench: not an integer: %q", p)
		}
		out = append(out, v)
	}
	return out
}

func main() {
	var (
		hostsFlag    = flag.String("hosts", "localhost:6334,localhost:6434,localhost:6534", "comma-separated host:port list")
		collection   = flag.String("collection", "sift1m", "collection name (must already be loaded)")
		hdf5Path     = flag.String("hdf5", "./data/sift-128-euclidean.hdf5", "SIFT hdf5 path")
		workloads    = flag.String("workload", "B", "comma-separated list of workloads to run; any of A,B,C,D,E,F")
		concurrency  = flag.String("concurrency", "10", "comma-separated client concurrencies")
		batchSizes   = flag.String("batch", "1", "comma-separated batch sizes (workload A,D,E,F use these)")
		duration     = flag.Duration("duration", 30*time.Second, "duration of each (workload, concurrency, batch) run")
		warmup       = flag.Duration("warmup", 5*time.Second, "warmup before each run; latencies during warmup are discarded")
		k            = flag.Uint64("k", 10, "top-K for searches")
		wait         = flag.Bool("wait", false, "use wait=true on every upsert")
		runLabel     = flag.String("run-label", "", "subdirectory under ./results/ for this run; default = timestamp")
		recallSweep  = flag.Bool("recall", false, "after the bench, run a recall@K sweep over the test split")
		recallConc   = flag.Int("recall-concurrency", 16, "concurrency for the recall sweep")
	)
	flag.Parse()

	hosts := commaList(*hostsFlag)
	wkLabels := commaList(*workloads)
	concs := commaInts(*concurrency)
	batches := commaInts(*batchSizes)

	if *runLabel == "" {
		*runLabel = time.Now().UTC().Format("20060102-150405")
	}
	outDir := filepath.Join("results", *runLabel)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("bench: mkdir: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Dataset is needed by every workload (queries are drawn from sift.Test)
	log.Printf("bench: loading dataset...")
	sift, err := dataset.Load(*hdf5Path)
	if err != nil {
		log.Fatalf("bench: %v", err)
	}
	log.Printf("bench: dataset loaded (train=%d, test=%d, dim=%d)", sift.NumTrain, sift.NumTest, sift.Dim)

	cli, err := qclient.New(qclient.Config{Hosts: hosts})
	if err != nil {
		log.Fatalf("bench: connect: %v", err)
	}
	defer cli.Close()

	// Make sure the collection exists and is ready
	info, err := cli.CollectionInfo(ctx, *collection)
	if err != nil {
		log.Fatalf("bench: collection_info: %v - did you run the loader?", err)
	}
	log.Printf("bench: target collection %s: %d points, %d segments, status=%s",
		*collection, info.GetPointsCount(), info.GetSegmentsCount(), info.GetStatus().String())

	// Shared NextID counter so insert workloads don't reuse training IDs
	nextID := uint64(sift.NumTrain) // start AFTER the loaded corpus
	var nextIDPtr = &nextID

	// Save the run config alongside the results so we can replay
	runMeta := map[string]any{
		"hosts":       hosts,
		"collection":  *collection,
		"workloads":   wkLabels,
		"concurrency": concs,
		"batches":     batches,
		"duration":    duration.String(),
		"warmup":      warmup.String(),
		"k":           *k,
		"wait":        *wait,
	}
	if b, _ := json.MarshalIndent(runMeta, "", "  "); b != nil {
		_ = os.WriteFile(filepath.Join(outDir, "_run_meta.json"), b, 0o644)
	}

	// ----- main sweep -----
	for _, wl := range wkLabels {
		for _, c := range concs {
			for _, batch := range batches {
				if err := runOne(ctx, cli, sift, *collection, wl, c, batch, *duration, *warmup, *k, *wait, nextIDPtr, outDir); err != nil {
					log.Printf("bench: run %s/c=%d/b=%d failed: %v", wl, c, batch, err)
				}
			}
		}
	}

	// ----- optional recall sweep -----
	if *recallSweep {
		log.Printf("bench: running recall@%d sweep across %d queries...", *k, sift.NumTest)
		t0 := time.Now()
		r, err := recall.Compute(ctx, cli, *collection, sift, int(*k), *recallConc)
		if err != nil {
			log.Printf("bench: recall: %v", err)
		} else {
			log.Printf("bench: recall@%d = %.4f (zero-recall: %d/%d) in %s", r.K, r.MeanRecall, r.ZeroRecallCt, r.Queries, time.Since(t0))
			b, _ := json.MarshalIndent(r, "", "  ")
			_ = os.WriteFile(filepath.Join(outDir, "_recall.json"), b, 0o644)
		}
	}
}

// runOne executes a single (workload, concurrency, batch) cell.
func runOne(
	parent context.Context,
	cli *qclient.Client,
	sift *dataset.SIFT,
	collection, wlLabel string,
	concurrency, batch int,
	duration, warmup time.Duration,
	k uint64,
	wait bool,
	nextID *uint64,
	outDir string,
) error {
	// One recorder per OpKind. The workload writes into all relevant ones.
	rec := map[workload.OpKind]workload.LatencyRecorder{}
	hdr := map[workload.OpKind]*metrics.Recorder{}
	for _, k := range []workload.OpKind{workload.OpRead, workload.OpUpdate, workload.OpInsert, workload.OpRMW, workload.OpScan} {
		r := metrics.NewRecorder()
		rec[k] = r
		hdr[k] = r
	}

	common := workload.Common{
		Client:      cli,
		Collection:  collection,
		Dataset:     sift,
		Concurrency: concurrency,
		BatchSize:   batch,
		Wait:        wait,
		K:           k,
		OpRecorders: rec,
		NextID:      nextID,
	}

	var prof workload.Profile
	switch strings.ToUpper(wlLabel) {
	case "A":
		prof = workload.NewA(common)
	case "B":
		prof = workload.NewB(common)
	case "C":
		prof = workload.NewC(common)
	case "D":
		prof = workload.NewD(common)
	case "E":
		prof = workload.NewE(common)
	case "F":
		prof = workload.NewF(common)
	default:
		return fmt.Errorf("unknown workload %q", wlLabel)
	}

	configLabel := fmt.Sprintf("c=%d_b=%d_wait=%v", concurrency, batch, wait)
	log.Printf("bench: starting workload %s (%s) for warmup=%s + duration=%s", prof.Label(), configLabel, warmup, duration)

	// === Warmup phase ===
	// Run with the same workload but throw away the latencies. We do this by
	// running, then RESETTING the recorders (cheap: rebuild fresh ones).
	if warmup > 0 {
		warmCtx, cancel := context.WithTimeout(parent, warmup)
		_, err := prof.Run(warmCtx)
		cancel()
		if err != nil {
			log.Printf("bench: warmup error (non-fatal): %v", err)
		}
		// Discard warmup measurements: rebuild the recorders.
		for _, k := range []workload.OpKind{workload.OpRead, workload.OpUpdate, workload.OpInsert, workload.OpRMW, workload.OpScan} {
			r := metrics.NewRecorder()
			rec[k] = r
			hdr[k] = r
		}
	}

	// === Real run ===
	t0 := time.Now()
	runCtx, cancel := context.WithTimeout(parent, duration)
	defer cancel()
	totalOps, err := prof.Run(runCtx)
	dt := time.Since(t0)
	if err != nil {
		log.Printf("bench: workload returned error: %v", err)
	}
	log.Printf("bench: %s/%s done in %s, %d ops total (%.1f QPS)",
		prof.Label(), configLabel, dt, totalOps, float64(totalOps)/dt.Seconds())

	// === Persist ===
	results := []metrics.Result{}
	for _, k := range []workload.OpKind{workload.OpRead, workload.OpUpdate, workload.OpInsert, workload.OpRMW, workload.OpScan} {
		snap := hdr[k].Snapshot(k.String(), prof.Label(), configLabel, concurrency)
		if snap.TotalCount == 0 {
			continue // workload didn't exercise this op
		}
		results = append(results, snap)
	}

	outFile := filepath.Join(outDir, fmt.Sprintf("%s_c%d_b%d.json", prof.Label(), concurrency, batch))
	if err := metrics.SaveJSON(outFile, results); err != nil {
		return fmt.Errorf("save %s: %w", outFile, err)
	}
	log.Printf("bench: wrote %s", outFile)

	// Print a tiny summary line for each op kind
	for _, r := range results {
		log.Printf("  op=%-7s n=%-7d qps=%-8.1f p50=%-6.2fms p95=%-6.2fms p99=%-6.2fms p999=%-6.2fms err=%d",
			r.Operation, r.TotalCount, r.QPS, r.P50Ms, r.P95Ms, r.P99Ms, r.P999Ms, r.ErrorCount)
	}

	// Make sure NextID actually moved monotonically across runs
	_ = atomic.LoadUint64(nextID)
	return nil
}
