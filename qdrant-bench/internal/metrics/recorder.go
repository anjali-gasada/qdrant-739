// Package metrics records latency and throughput for the benchmark.
//
// The reason we use HDR histograms (github.com/HdrHistogram/hdrhistogram-go)
// instead of a regular bucketed histogram is that we need accurate p99/p999
// values, and HDR keeps relative-error precision uniform across the entire
// dynamic range. The proposal specifically asks for p95 and p99 - on a
// uniform-bucket histogram those numbers are dominated by bucket-boundary
// rounding once you hit the long tail.
//
// Each operation type (read, write, delete) gets its own Recorder. A Recorder
// is safe under concurrent Add() calls. At the end of a run we Snapshot() it
// to a JSON-serializable Result and write that to disk.
package metrics

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

// Recorder tracks latencies for one operation kind. The HDR backing histogram
// covers 1µs..60s with 3 significant digits - more than enough for vector
// search workloads.
type Recorder struct {
	mu    sync.Mutex
	hist  *hdr.Histogram
	count uint64
	errs  uint64
	// One-second bucketed counts, used to plot QPS and latency over time
	// during chaos experiments where the steady-state assumption breaks.
	startWall    time.Time
	secondCounts []bucket
}

type bucket struct {
	Second   int     `json:"second"`
	Count    uint64  `json:"count"`
	Errors   uint64  `json:"errors"`
	P50Ms    float64 `json:"p50_ms"`
	P95Ms    float64 `json:"p95_ms"`
	P99Ms    float64 `json:"p99_ms"`
	bucketHist *hdr.Histogram // not exported - rebuilt each second
}

// NewRecorder builds an empty Recorder. The histogram covers
// [1µs, 60s] inclusive at 3 sig figs.
func NewRecorder() *Recorder {
	return &Recorder{
		hist:      hdr.New(1, int64(60*time.Second/time.Microsecond), 3),
		startWall: time.Now(),
	}
}

// Add records one observation. d is the observed latency, ok==false means
// the operation returned an error (we count the count, but record latency
// in the histogram regardless so the caller can study failed-call latency
// separately).
func (r *Recorder) Add(d time.Duration, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.count++
	if !ok {
		r.errs++
	}
	micros := d.Microseconds()
	if micros < 1 {
		micros = 1
	}
	if max := r.hist.HighestTrackableValue(); micros > max {
		micros = max
	}
	_ = r.hist.RecordValue(micros)

	// Per-second bucket
	sec := int(time.Since(r.startWall) / time.Second)
	for len(r.secondCounts) <= sec {
		r.secondCounts = append(r.secondCounts, bucket{
			Second:     len(r.secondCounts),
			bucketHist: hdr.New(1, int64(60*time.Second/time.Microsecond), 3),
		})
	}
	b := &r.secondCounts[sec]
	b.Count++
	if !ok {
		b.Errors++
	}
	_ = b.bucketHist.RecordValue(micros)
}

// Result is a JSON-friendly snapshot.  The percentile fields are in milliseconds.
type Result struct {
	Operation     string    `json:"operation"`
	Concurrency   int       `json:"concurrency"`
	StartedAt     time.Time `json:"started_at"`
	DurationSec   float64   `json:"duration_sec"`
	TotalCount    uint64    `json:"total_count"`
	ErrorCount    uint64    `json:"error_count"`
	QPS           float64   `json:"qps"`
	MeanMs        float64   `json:"mean_ms"`
	P50Ms         float64   `json:"p50_ms"`
	P90Ms         float64   `json:"p90_ms"`
	P95Ms         float64   `json:"p95_ms"`
	P99Ms         float64   `json:"p99_ms"`
	P999Ms        float64   `json:"p999_ms"`
	MaxMs         float64   `json:"max_ms"`
	PerSecond     []bucket  `json:"per_second"`
	WorkloadLabel string    `json:"workload_label"`
	ConfigLabel   string    `json:"config_label"`
}

// Snapshot fills out a Result by reading the histogram. Safe to call mid-run;
// the histogram is not reset.
func (r *Recorder) Snapshot(operation, workload, config string, concurrency int) Result {
	r.mu.Lock()
	defer r.mu.Unlock()

	dur := time.Since(r.startWall).Seconds()
	if dur <= 0 {
		dur = 1e-9
	}
	res := Result{
		Operation:     operation,
		Concurrency:   concurrency,
		StartedAt:     r.startWall,
		DurationSec:   dur,
		TotalCount:    r.count,
		ErrorCount:    r.errs,
		QPS:           float64(r.count) / dur,
		MeanMs:        usToMs(r.hist.Mean()),
		P50Ms:         usToMs(float64(r.hist.ValueAtQuantile(50))),
		P90Ms:         usToMs(float64(r.hist.ValueAtQuantile(90))),
		P95Ms:         usToMs(float64(r.hist.ValueAtQuantile(95))),
		P99Ms:         usToMs(float64(r.hist.ValueAtQuantile(99))),
		P999Ms:        usToMs(float64(r.hist.ValueAtQuantile(99.9))),
		MaxMs:         usToMs(float64(r.hist.Max())),
		WorkloadLabel: workload,
		ConfigLabel:   config,
	}

	// Materialize per-second percentiles
	res.PerSecond = make([]bucket, len(r.secondCounts))
	for i := range r.secondCounts {
		b := r.secondCounts[i]
		if b.bucketHist != nil && b.bucketHist.TotalCount() > 0 {
			b.P50Ms = usToMs(float64(b.bucketHist.ValueAtQuantile(50)))
			b.P95Ms = usToMs(float64(b.bucketHist.ValueAtQuantile(95)))
			b.P99Ms = usToMs(float64(b.bucketHist.ValueAtQuantile(99)))
		}
		b.bucketHist = nil // strip before serializing
		res.PerSecond[i] = b
	}
	return res
}

func usToMs(us float64) float64 { return us / 1000.0 }

// SaveJSON writes a slice of Results (one per operation kind) to disk.
func SaveJSON(path string, results []Result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}
