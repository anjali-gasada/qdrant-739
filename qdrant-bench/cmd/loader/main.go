// Command loader populates a Qdrant collection with the SIFT-128-Euclidean
// training split (1,000,000 × 128 vectors, L2 distance).
//
// Usage:
//
//   go run ./cmd/loader \
//       -hosts localhost:6334,localhost:6434,localhost:6534 \
//       -collection sift1m \
//       -hdf5 ./data/sift-128-euclidean.hdf5 \
//       -shard-number 6 \
//       -replication-factor 2 \
//       -write-consistency 1 \
//       -batch 1024 \
//       -concurrency 8 \
//       -wait=false
//
// We deliberately decouple the loader from the benchmark - loading 1M
// vectors with HNSW indexing on takes a while, and we want to be able to
// re-run benchmarks against the same loaded corpus without reloading.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/qdrant/go-client/qdrant"

	"qdrant-bench/internal/dataset"
	"qdrant-bench/internal/qclient"
)

func main() {
	var (
		hostsFlag     = flag.String("hosts", "localhost:6334,localhost:6434,localhost:6534", "comma-separated host:port list of Qdrant gRPC endpoints")
		collection    = flag.String("collection", "sift1m", "collection name")
		hdf5Path      = flag.String("hdf5", "./data/sift-128-euclidean.hdf5", "path to SIFT hdf5 file")
		shardNumber   = flag.Uint("shard-number", 6, "number of shards across the cluster")
		replication   = flag.Uint("replication-factor", 2, "replication factor (copies per shard)")
		writeConsist  = flag.Uint("write-consistency", 1, "write consistency factor (replicas that must ack)")
		batch         = flag.Int("batch", 1024, "vectors per upsert batch")
		concurrency   = flag.Int("concurrency", 8, "parallel batch uploaders")
		wait          = flag.Bool("wait", false, "wait=true on every upsert (much slower, used to measure pure indexing latency)")
		maxSegKB      = flag.Int64("max-segment-size-kb", -1, "override max_segment_size_kb (-1 = leave default)")
		hnswM         = flag.Int64("hnsw-m", -1, "override HNSW m (-1 = default)")
		hnswEf        = flag.Int64("hnsw-ef-construct", -1, "override HNSW ef_construct (-1 = default)")
	)
	flag.Parse()

	hosts := strings.Split(*hostsFlag, ",")
	for i := range hosts {
		hosts[i] = strings.TrimSpace(hosts[i])
	}

	// Cancel cleanly on Ctrl-C.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Printf("loader: opening dataset from %s", *hdf5Path)
	t0 := time.Now()
	sift, err := dataset.Load(*hdf5Path)
	if err != nil {
		log.Fatalf("loader: %v", err)
	}
	log.Printf("loader: loaded in %s: train=%d test=%d dim=%d", time.Since(t0), sift.NumTrain, sift.NumTest, sift.Dim)

	cli, err := qclient.New(qclient.Config{Hosts: hosts})
	if err != nil {
		log.Fatalf("loader: connect: %v", err)
	}
	defer cli.Close()

	// Build collection params (with optional optimizer overrides)
	params := qclient.CollectionParams{
		Name:              *collection,
		Dim:               uint64(sift.Dim),
		Distance:          qdrant.Distance_Euclid, // SIFT is L2 distance
		ShardNumber:       uint32(*shardNumber),
		ReplicationFactor: uint32(*replication),
		WriteConsistency:  uint32(*writeConsist),
		OnDiskVectors:     false, // RAM is fine for 1M × 128
	}
	if *maxSegKB >= 0 {
		v := uint64(*maxSegKB)
		params.MaxSegmentSizeKB = &v
	}
	if *hnswM >= 0 {
		v := uint64(*hnswM)
		params.HNSWm = &v
	}
	if *hnswEf >= 0 {
		v := uint64(*hnswEf)
		params.HNSWefConstruct = &v
	}

	log.Printf("loader: creating collection %s (shards=%d, rf=%d, write_consist=%d)", *collection, params.ShardNumber, params.ReplicationFactor, params.WriteConsistency)
	if err := cli.CreateCollection(ctx, params); err != nil {
		log.Fatalf("loader: create_collection: %v", err)
	}

	// Bulk insert. We round up to a multiple of *batch, partition the index
	// space across worker goroutines, and let each worker do its slice
	// strictly in order (sequential IDs). Avoiding interleaving keeps the
	// WAL pretty and the benchmark output debuggable.
	if err := bulkLoad(ctx, cli, sift, *collection, *batch, *concurrency, *wait); err != nil {
		log.Fatalf("loader: %v", err)
	}

	// Sanity check: how many points actually landed?
	info, err := cli.CollectionInfo(ctx, *collection)
	if err != nil {
		log.Fatalf("loader: get_collection_info: %v", err)
	}
	log.Printf("loader: done. points=%d indexed_vectors=%d segments=%d status=%s",
		info.GetPointsCount(), info.GetIndexedVectorsCount(), info.GetSegmentsCount(), info.GetStatus().String())

	if info.GetPointsCount() < uint64(sift.NumTrain) {
		log.Printf("loader: WARNING - expected %d points, got %d (some upserts may have failed)", sift.NumTrain, info.GetPointsCount())
		os.Exit(1)
	}
}

func bulkLoad(ctx context.Context, cli *qclient.Client, sift *dataset.SIFT, collection string, batch, concurrency int, wait bool) error {
	if batch <= 0 {
		batch = 1024
	}
	totalBatches := (sift.NumTrain + batch - 1) / batch
	jobs := make(chan int, totalBatches)
	for b := 0; b < totalBatches; b++ {
		jobs <- b
	}
	close(jobs)

	var (
		done       int64
		failed     int64
		bytesSent  int64
		startWall  = time.Now()
		wg         sync.WaitGroup
		fatalOnce  sync.Once
		fatalError error
	)

	progress := time.NewTicker(2 * time.Second)
	defer progress.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-progress.C:
				d := atomic.LoadInt64(&done)
				if d == 0 {
					continue
				}
				log.Printf("loader: progress %d/%d batches (%.1f%%) - %.0f vec/sec",
					d, totalBatches, 100.0*float64(d)/float64(totalBatches),
					float64(d*int64(batch))/time.Since(startWall).Seconds())
			}
		}
	}()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for bi := range jobs {
				if ctx.Err() != nil {
					return
				}
				start := bi * batch
				end := start + batch
				if end > sift.NumTrain {
					end = sift.NumTrain
				}
				pts := make([]qclient.Point, end-start)
				for i := start; i < end; i++ {
					pts[i-start] = qclient.Point{
						ID:     uint64(i),
						Vector: sift.TrainVec(i),
						// Stamp a "bucket" for workload-E filter queries
						Payload: map[string]any{"bucket": int64(i % 10)},
					}
				}
				if _, err := cli.AddMany(ctx, collection, pts, wait); err != nil {
					atomic.AddInt64(&failed, 1)
					fatalOnce.Do(func() { fatalError = err })
					return
				}
				atomic.AddInt64(&done, 1)
				atomic.AddInt64(&bytesSent, int64(len(pts)*sift.Dim*4))
			}
		}()
	}
	wg.Wait()

	if fatalError != nil {
		return fmt.Errorf("loader: %d batches failed; first error: %w", failed, fatalError)
	}
	log.Printf("loader: ingested %d batches in %s (%.0f vec/s, %.1f MB sent)",
		done, time.Since(startWall),
		float64(done*int64(batch))/time.Since(startWall).Seconds(),
		float64(bytesSent)/(1024*1024))
	return nil
}
