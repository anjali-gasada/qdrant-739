// Command groundtruth runs a standalone recall@K sweep against an already-
// loaded SIFT collection. Run this AFTER the loader has finished and AFTER
// the optimizer has had a chance to build the HNSW indexes (i.e. wait until
// CollectionInfo.GetStatus() == Green).
//
// Usage:
//
//   go run ./cmd/groundtruth -k 10 -concurrency 16
//
// Output: prints recall@K to stdout and writes results/recall-<ts>.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"qdrant-bench/internal/dataset"
	"qdrant-bench/internal/qclient"
	"qdrant-bench/internal/recall"
)

func main() {
	var (
		hostsFlag   = flag.String("hosts", "localhost:6334,localhost:6434,localhost:6534", "gRPC host:port list")
		collection  = flag.String("collection", "sift1m", "collection name")
		hdf5Path    = flag.String("hdf5", "./data/sift-128-euclidean.hdf5", "SIFT hdf5 path")
		k           = flag.Int("k", 10, "top-K")
		concurrency = flag.Int("concurrency", 16, "parallel queries")
		outDir      = flag.String("out-dir", "results", "output directory")
	)
	flag.Parse()

	hosts := strings.Split(*hostsFlag, ",")
	for i := range hosts {
		hosts[i] = strings.TrimSpace(hosts[i])
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	sift, err := dataset.Load(*hdf5Path)
	if err != nil {
		log.Fatalf("groundtruth: %v", err)
	}
	cli, err := qclient.New(qclient.Config{Hosts: hosts})
	if err != nil {
		log.Fatalf("groundtruth: %v", err)
	}
	defer cli.Close()

	log.Printf("groundtruth: computing recall@%d over %d queries (concurrency=%d)...", *k, sift.NumTest, *concurrency)
	t0 := time.Now()
	r, err := recall.Compute(ctx, cli, *collection, sift, *k, *concurrency)
	if err != nil {
		log.Fatalf("groundtruth: %v", err)
	}
	dt := time.Since(t0)
	log.Printf("groundtruth: recall@%d = %.4f (zero-recall queries: %d/%d) in %s -> %.0f QPS", r.K, r.MeanRecall, r.ZeroRecallCt, r.Queries, dt, float64(r.Queries)/dt.Seconds())

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("groundtruth: %v", err)
	}
	outPath := filepath.Join(*outDir, "recall-"+time.Now().UTC().Format("20060102-150405")+".json")
	f, _ := os.Create(outPath)
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	_ = enc.Encode(r)
	log.Printf("groundtruth: wrote %s", outPath)
}
