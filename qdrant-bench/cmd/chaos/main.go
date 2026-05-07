// Command chaos runs the fault-tolerance experiments from the proposal:
//
//   1. Start a sustained workload (default: heavy inserts) on the cluster
//   2. After --inject-after time, kill a random container or partition it
//   3. Continue the workload through the chaos event
//   4. Poll every surviving node's /cluster endpoint to detect when a new
//      Raft leader is elected and stable
//   5. Optionally restart the killed node and watch it rejoin
//   6. Dump:
//        - the latency timeline (per-second p99 across the chaos event)
//        - the leader-election timeline
//        - elapsed wall time from kill -> stable consensus
//
// Usage:
//
//   go run ./cmd/chaos \
//       -hosts localhost:6334,localhost:6434,localhost:6534 \
//       -rest-urls http://localhost:6333,http://localhost:6433,http://localhost:6533 \
//       -containers qdrant_node1,qdrant_node2,qdrant_node3 \
//       -workload A -concurrency 16 \
//       -inject-after 30s -duration 120s \
//       -kill-target qdrant_node2 \
//       -recover-after 60s \
//       -mode kill              # or "partition"
//       -out-dir results/chaos-1
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sai/qdrant-bench/internal/chaos"
	"github.com/sai/qdrant-bench/internal/dataset"
	"github.com/sai/qdrant-bench/internal/metrics"
	"github.com/sai/qdrant-bench/internal/qclient"
	"github.com/sai/qdrant-bench/internal/workload"
)

func splitCSV(s string) []string {
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

func main() {
	var (
		hostsFlag    = flag.String("hosts", "localhost:6334,localhost:6434,localhost:6534", "gRPC host:port list")
		restURLs     = flag.String("rest-urls", "http://localhost:6333,http://localhost:6433,http://localhost:6533", "REST URLs of all nodes")
		containers   = flag.String("containers", "qdrant_node1,qdrant_node2,qdrant_node3", "docker container names, in the same order as --hosts and --rest-urls")
		collection   = flag.String("collection", "sift1m", "collection name")
		hdf5Path     = flag.String("hdf5", "./data/sift-128-euclidean.hdf5", "SIFT hdf5 path")
		wkLabel      = flag.String("workload", "A", "workload to run during chaos (A,B,C,D,E,F)")
		concurrency  = flag.Int("concurrency", 16, "concurrent clients")
		k            = flag.Uint64("k", 10, "search top-K")
		duration     = flag.Duration("duration", 120*time.Second, "total experiment duration")
		injectAfter  = flag.Duration("inject-after", 30*time.Second, "wait this long before injecting fault")
		recoverAfter = flag.Duration("recover-after", 0, "if >0, restart killed node / heal partition this long after injection")
		killTarget   = flag.String("kill-target", "", "container to kill/partition (default: random non-leader)")
		mode         = flag.String("mode", "kill", "fault mode: kill | partition")
		p2pPort      = flag.Int("p2p-port", 6335, "Raft P2P port (only used by partition mode)")
		outDir       = flag.String("out-dir", "", "output directory; default = results/chaos-<timestamp>")
	)
	flag.Parse()

	hosts := splitCSV(*hostsFlag)
	rests := splitCSV(*restURLs)
	conts := splitCSV(*containers)
	if len(hosts) != len(rests) || len(hosts) != len(conts) {
		log.Fatalf("chaos: --hosts, --rest-urls, --containers must have the same length (got %d/%d/%d)", len(hosts), len(rests), len(conts))
	}

	if *outDir == "" {
		*outDir = filepath.Join("results", "chaos-"+time.Now().UTC().Format("20060102-150405"))
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("chaos: mkdir: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	sift, err := dataset.Load(*hdf5Path)
	if err != nil {
		log.Fatalf("chaos: dataset: %v", err)
	}
	cli, err := qclient.New(qclient.Config{Hosts: hosts})
	if err != nil {
		log.Fatalf("chaos: connect: %v", err)
	}
	defer cli.Close()

	// Pick chaos target if not provided
	if *killTarget == "" {
		// Find the current leader and pick a NON-leader to kill - that's the
		// more interesting case (no leader election needed, but shard
		// replicas still need to react).
		var leaderRest string
		for i, u := range rests {
			ci, err := chaos.FetchClusterInfo(ctx, u)
			if err != nil {
				continue
			}
			if ci.Result.RaftInfo.Role == "Leader" {
				leaderRest = u
				_ = i
				break
			}
		}
		// Default to RANDOMLY picking; the experiment is more powerful
		// when we INCLUDE the leader, so we toss a coin
		idx := rand.Intn(len(conts))
		_ = leaderRest
		*killTarget = conts[idx]
		log.Printf("chaos: no -kill-target given, picked %s at random", *killTarget)
	}

	// Convert killTarget to its REST URL
	var killedREST string
	for i, c := range conts {
		if c == *killTarget {
			killedREST = rests[i]
			break
		}
	}
	if killedREST == "" {
		log.Fatalf("chaos: kill-target %q not in --containers", *killTarget)
	}

	// Surviving REST list (used by the recovery watcher)
	surviving, err := chaos.SurvivingRESTURLs(rests, killedREST)
	if err != nil {
		log.Fatalf("chaos: %v", err)
	}

	// Snapshot pre-chaos cluster state
	preState := snapshotAllNodes(ctx, rests)
	log.Printf("chaos: pre-chaos state: %s", mustJSON(preState))

	// Start the workload in a goroutine
	rec := map[workload.OpKind]workload.LatencyRecorder{}
	hdr := map[workload.OpKind]*metrics.Recorder{}
	for _, k := range []workload.OpKind{workload.OpRead, workload.OpUpdate, workload.OpInsert, workload.OpRMW, workload.OpScan} {
		r := metrics.NewRecorder()
		rec[k] = r
		hdr[k] = r
	}
	nextID := uint64(sift.NumTrain)
	common := workload.Common{
		Client:      cli,
		Collection:  *collection,
		Dataset:     sift,
		Concurrency: *concurrency,
		BatchSize:   1,
		Wait:        false,
		K:           *k,
		OpRecorders: rec,
		NextID:      &nextID,
	}
	var prof workload.Profile
	switch strings.ToUpper(*wkLabel) {
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
		log.Fatalf("chaos: unknown workload %q", *wkLabel)
	}

	wlCtx, wlCancel := context.WithTimeout(ctx, *duration)
	defer wlCancel()
	wlDone := make(chan int64, 1)
	go func() {
		ops, _ := prof.Run(wlCtx)
		wlDone <- ops
	}()

	// Wait for the inject point
	log.Printf("chaos: workload %s started (%d clients), injecting fault in %s", prof.Label(), *concurrency, *injectAfter)
	select {
	case <-time.After(*injectAfter):
	case <-ctx.Done():
		log.Fatalf("chaos: aborted before fault injection")
	}

	// === FAULT INJECTION ===
	injectAt := time.Now()
	log.Printf("chaos: injecting fault on %s (mode=%s) at t+%s", *killTarget, *mode, *injectAfter)
	switch *mode {
	case "kill":
		if err := chaos.KillContainer(ctx, *killTarget); err != nil {
			log.Fatalf("chaos: kill: %v", err)
		}
	case "partition":
		if err := chaos.PartitionContainer(ctx, *killTarget, *p2pPort); err != nil {
			log.Fatalf("chaos: partition: %v", err)
		}
	default:
		log.Fatalf("chaos: unknown -mode %q", *mode)
	}

	// === RECOVERY WATCH ===
	w := chaos.NewRecoveryWatcher(surviving, 100*time.Millisecond)
	leader, timeline, elapsed := w.WaitForStableLeader(ctx, injectAt, 60*time.Second)
	if leader == 0 {
		log.Printf("chaos: TIMED OUT waiting for stable leader (elapsed=%s, %d observations)", elapsed, len(timeline))
	} else {
		log.Printf("chaos: stable leader=%d after %s (%d observations)", leader, elapsed, len(timeline))
	}

	// === OPTIONAL RECOVERY ===
	if *recoverAfter > 0 {
		// Wait until injectAt + recoverAfter before healing
		if remain := time.Until(injectAt.Add(*recoverAfter)); remain > 0 {
			select {
			case <-time.After(remain):
			case <-ctx.Done():
			}
		}
		log.Printf("chaos: recovering %s", *killTarget)
		switch *mode {
		case "kill":
			if err := chaos.StartContainer(ctx, *killTarget); err != nil {
				log.Printf("chaos: restart: %v", err)
			}
		case "partition":
			if err := chaos.HealPartition(ctx, *killTarget, *p2pPort); err != nil {
				log.Printf("chaos: heal: %v", err)
			}
		}
		// Watch a re-converged 3-node state
		log.Printf("chaos: waiting for full-cluster convergence...")
		fullW := chaos.NewRecoveryWatcher(rests, 100*time.Millisecond)
		fullLeader, fullTL, fullElapsed := fullW.WaitForStableLeader(ctx, time.Now(), 60*time.Second)
		log.Printf("chaos: full-cluster leader=%d after %s (%d observations)", fullLeader, fullElapsed, len(fullTL))
		// Append to timeline
		timeline = append(timeline, fullTL...)
	}

	// === Wait for workload to finish ===
	totalOps := int64(0)
	select {
	case n := <-wlDone:
		totalOps = n
	case <-time.After(*duration):
		log.Printf("chaos: workload didn't return cleanly, canceling")
		wlCancel()
	}
	atomic.StoreInt64(&totalOps, totalOps)

	// === Persist results ===
	postState := snapshotAllNodes(ctx, rests)

	// 1. Per-op latency snapshots (per-second buckets capture the spike)
	results := []metrics.Result{}
	for _, k := range []workload.OpKind{workload.OpRead, workload.OpUpdate, workload.OpInsert, workload.OpRMW, workload.OpScan} {
		snap := hdr[k].Snapshot(k.String(), prof.Label(), fmt.Sprintf("chaos_%s_kill_%s", *mode, *killTarget), *concurrency)
		if snap.TotalCount == 0 {
			continue
		}
		results = append(results, snap)
	}
	if err := metrics.SaveJSON(filepath.Join(*outDir, "latency.json"), results); err != nil {
		log.Printf("chaos: write latency: %v", err)
	}

	// 2. Leader timeline + summary
	summary := map[string]any{
		"mode":              *mode,
		"target":            *killTarget,
		"target_rest":       killedREST,
		"inject_after":      injectAfter.String(),
		"recover_after":     recoverAfter.String(),
		"final_leader":      leader,
		"recovery_elapsed":  elapsed.String(),
		"recovery_ms":       float64(elapsed.Microseconds()) / 1000.0,
		"workload":          prof.Label(),
		"concurrency":       *concurrency,
		"duration":          duration.String(),
		"total_ops":         atomic.LoadInt64(&totalOps),
		"pre_state":         preState,
		"post_state":        postState,
	}
	_ = writeJSON(filepath.Join(*outDir, "summary.json"), summary)
	_ = writeJSON(filepath.Join(*outDir, "leader_timeline.json"), timeline)

	log.Printf("chaos: done. wrote %s/{summary,leader_timeline,latency}.json", *outDir)
}

func snapshotAllNodes(ctx context.Context, rests []string) map[string]any {
	out := map[string]any{}
	for _, u := range rests {
		ci, err := chaos.FetchClusterInfo(ctx, u)
		if err != nil {
			out[u] = map[string]string{"error": err.Error()}
			continue
		}
		out[u] = ci
	}
	return out
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
