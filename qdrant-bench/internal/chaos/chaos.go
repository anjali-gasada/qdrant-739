// Package chaos contains the fault-injection primitives the proposal asks for:
//
//   1. Kill a random Qdrant container during heavy write load.
//   2. Partition the network between two subsets of nodes.
//   3. Poll each surviving node's /cluster endpoint to detect when a NEW
//      Raft leader has been elected and is stable.
//
// We deliberately do NOT use docker SDK - shelling out to `docker` keeps the
// binary lean and lets the harness be cross-version compatible. Network
// partitions use `iptables` inside the container, which only works if the
// containers run with NET_ADMIN; we document that requirement.
package chaos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"sort"
	"sync"
	"time"
)

// ClusterInfo is the parsed shape of GET /cluster on any node.
type ClusterInfo struct {
	Result struct {
		Status   string                       `json:"status"`
		PeerID   uint64                       `json:"peer_id"`
		Peers    map[string]map[string]string `json:"peers"`
		RaftInfo struct {
			Term              uint64 `json:"term"`
			Commit            uint64 `json:"commit"`
			PendingOperations uint64 `json:"pending_operations"`
			Leader            uint64 `json:"leader"` // 0 = no leader
			Role              string `json:"role"`   // "Leader" / "Follower" / "Candidate"
			IsVoter           bool   `json:"is_voter"`
		} `json:"raft_info"`
	} `json:"result"`
}

// FetchClusterInfo asks the node at restURL ("http://host:port") for its
// current cluster view.
func FetchClusterInfo(ctx context.Context, restURL string) (*ClusterInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", restURL+"/cluster", nil)
	if err != nil {
		return nil, err
	}
	cl := &http.Client{Timeout: 2 * time.Second}
	resp, err := cl.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("cluster: %s -> %d: %s", restURL, resp.StatusCode, string(body))
	}
	var ci ClusterInfo
	if err := json.NewDecoder(resp.Body).Decode(&ci); err != nil {
		return nil, err
	}
	return &ci, nil
}

// KillContainer hard-stops the named container. We use SIGKILL (`docker kill`)
// rather than `docker stop` because stop sends SIGTERM and gives the process
// time to shutdown gracefully - which is the OPPOSITE of what we want when
// simulating a node failure.
func KillContainer(ctx context.Context, name string) error {
	cmd := exec.CommandContext(ctx, "docker", "kill", "--signal", "SIGKILL", name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("chaos: docker kill %s: %w (output: %s)", name, err, string(out))
	}
	return nil
}

// StartContainer brings the named container back. Used after the chaos
// experiment to verify the cluster reconciles state from snapshot+WAL.
func StartContainer(ctx context.Context, name string) error {
	cmd := exec.CommandContext(ctx, "docker", "start", name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("chaos: docker start %s: %w (output: %s)", name, err, string(out))
	}
	return nil
}

// PartitionContainer adds an iptables DROP rule so the container can no
// longer talk to the rest of the cluster. We block ingress on the Raft P2P
// port (6335 default) so the node is "alive" but isolated from consensus -
// classic split-brain test.
//
// REQUIRES: container started with --cap-add NET_ADMIN. Add this to the
// docker-compose file before running these experiments. We document this
// in docs/runbook.md.
func PartitionContainer(ctx context.Context, name string, p2pPort int) error {
	args := []string{
		"exec", name, "iptables",
		"-I", "INPUT",
		"-p", "tcp",
		"--dport", fmt.Sprintf("%d", p2pPort),
		"-j", "DROP",
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("chaos: partition %s: %w (output: %s)", name, err, string(out))
	}
	return nil
}

// HealPartition removes the iptables rule we added.
func HealPartition(ctx context.Context, name string, p2pPort int) error {
	args := []string{
		"exec", name, "iptables",
		"-D", "INPUT",
		"-p", "tcp",
		"--dport", fmt.Sprintf("%d", p2pPort),
		"-j", "DROP",
	}
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("chaos: heal %s: %w (output: %s)", name, err, string(out))
	}
	return nil
}

// RecoveryWatcher tracks Raft state across the surviving nodes after a fault.
// Use it like:
//
//   w := NewRecoveryWatcher(survivingRESTURLs, 100*time.Millisecond)
//   start := time.Now()
//   chaos.KillContainer(ctx, "qdrant_node1")
//   leader, elapsed := w.WaitForStableLeader(ctx, start, 30*time.Second)
//
// "stable" means: every surviving node reports the same non-zero leader_id
// for at least StableHoldTime (default 1s) consecutively.
type RecoveryWatcher struct {
	restURLs       []string
	pollInterval   time.Duration
	StableHoldTime time.Duration
}

func NewRecoveryWatcher(restURLs []string, pollInterval time.Duration) *RecoveryWatcher {
	return &RecoveryWatcher{
		restURLs:       append([]string(nil), restURLs...),
		pollInterval:   pollInterval,
		StableHoldTime: time.Second,
	}
}

// LeaderObservation records what every surviving node thought the leader was
// at one moment in time. Used for both stability-checking and for offline
// analysis (we serialize the whole timeline to JSON).
type LeaderObservation struct {
	At         time.Time          `json:"at"`
	ElapsedMs  float64            `json:"elapsed_ms"` // since chaos start
	PerNode    map[string]uint64  `json:"per_node"`   // restURL -> leader_id (0 = none)
	Term       map[string]uint64  `json:"term"`       // restURL -> raft term
	Errors     map[string]string  `json:"errors,omitempty"`
}

// WaitForStableLeader polls each surviving node every pollInterval until
// every node reports the same nonzero leader for StableHoldTime, OR until
// timeout elapses.
//
// Returns:
//   - leaderID: the agreed leader (0 if timeout)
//   - timeline: every observation taken (good for plotting "leader=X for Tms,
//               then leader=Y for Tms, ..." charts)
//   - elapsed: time from anchor (the moment chaos was injected) to stable
//              consensus on the new leader.
func (w *RecoveryWatcher) WaitForStableLeader(ctx context.Context, anchor time.Time, timeout time.Duration) (uint64, []LeaderObservation, time.Duration) {
	deadline := anchor.Add(timeout)
	tick := time.NewTicker(w.pollInterval)
	defer tick.Stop()

	timeline := make([]LeaderObservation, 0, 1024)
	var stableSince time.Time
	var stableLeader uint64

	for {
		obs := w.takeObservation(ctx, anchor)
		timeline = append(timeline, obs)

		// Did all surviving nodes agree on a single nonzero leader?
		if leader, agreed := majorityLeader(obs.PerNode); agreed {
			if leader == stableLeader {
				if !stableSince.IsZero() && time.Since(stableSince) >= w.StableHoldTime {
					return leader, timeline, obs.At.Sub(anchor)
				}
			} else {
				stableLeader = leader
				stableSince = time.Now()
			}
		} else {
			stableLeader = 0
			stableSince = time.Time{}
		}

		select {
		case <-ctx.Done():
			return 0, timeline, time.Since(anchor)
		case <-tick.C:
			if time.Now().After(deadline) {
				return 0, timeline, time.Since(anchor)
			}
		}
	}
}

func (w *RecoveryWatcher) takeObservation(ctx context.Context, anchor time.Time) LeaderObservation {
	obs := LeaderObservation{
		At:        time.Now(),
		ElapsedMs: float64(time.Since(anchor).Microseconds()) / 1000.0,
		PerNode:   map[string]uint64{},
		Term:      map[string]uint64{},
		Errors:    map[string]string{},
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, u := range w.restURLs {
		u := u
		wg.Add(1)
		go func() {
			defer wg.Done()
			ci, err := FetchClusterInfo(ctx, u)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				obs.Errors[u] = err.Error()
				return
			}
			obs.PerNode[u] = ci.Result.RaftInfo.Leader
			obs.Term[u] = ci.Result.RaftInfo.Term
		}()
	}
	wg.Wait()
	return obs
}

// majorityLeader returns (leader_id, true) iff every node in the map reports
// the SAME nonzero leader. (We require unanimity rather than raw majority
// because we already filtered to surviving nodes; unanimity here = "the
// majority of the original cluster has converged".)
func majorityLeader(per map[string]uint64) (uint64, bool) {
	if len(per) == 0 {
		return 0, false
	}
	counts := map[uint64]int{}
	for _, l := range per {
		if l == 0 {
			return 0, false // no leader anywhere => not converged
		}
		counts[l]++
	}
	if len(counts) > 1 {
		return 0, false
	}
	for l := range counts {
		return l, true
	}
	return 0, false
}

// SurvivingRESTURLs returns the list of REST URLs that does NOT contain the
// killed-container's URL. Caller maintains the host->container mapping.
func SurvivingRESTURLs(allRESTURLs []string, killedRESTURL string) ([]string, error) {
	out := make([]string, 0, len(allRESTURLs))
	found := false
	for _, u := range allRESTURLs {
		if u == killedRESTURL {
			found = true
			continue
		}
		out = append(out, u)
	}
	if !found {
		return nil, errors.New("chaos: killed REST URL not in all-list")
	}
	sort.Strings(out)
	return out, nil
}
