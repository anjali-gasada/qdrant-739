#!/usr/bin/env python3
"""
Render plots from the harness's JSON output.

Usage:
    python3 scripts/plot_results.py results

For every results/<label>/ subdirectory we look at, we generate a few PNGs:
  - qps_vs_concurrency_<workload>.png       (one curve per config_label)
  - p99_vs_concurrency_<workload>.png
  - latency_timeline_<workload>_c<N>.png    (per-second p99 across the run)

The chaos result directories also get a leader-election visualization:
  - leader_timeline.png                     (each line == one node, y == leader id over time)
"""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def load_run(run_dir: Path):
    """Load every *.json under run_dir into a flat list of {file, results}."""
    runs = []
    for f in sorted(run_dir.glob("*.json")):
        if f.name.startswith("_"):
            continue
        with f.open() as fh:
            try:
                data = json.load(fh)
            except json.JSONDecodeError:
                print(f"  skip {f.name}: not json")
                continue
        runs.append((f, data))
    return runs


def plot_qps_p99(run_dir: Path):
    """For each (workload, op) pair, plot QPS and p99 vs concurrency."""
    by_wl_op = defaultdict(list)  # (wl, op) -> [(c, qps, p99)]
    for f, data in load_run(run_dir):
        if not isinstance(data, list):
            continue  # not a metrics-snapshot file
        for r in data:
            wl = r.get("workload_label", "?")
            op = r.get("operation", "?")
            c = r.get("concurrency", 0)
            qps = r.get("qps", 0)
            p99 = r.get("p99_ms", 0)
            by_wl_op[(wl, op)].append((c, qps, p99))

    out = run_dir / "plots"
    out.mkdir(exist_ok=True)
    # QPS plot
    workloads = sorted({wl for wl, _ in by_wl_op})
    for wl in workloads:
        fig, ax = plt.subplots(figsize=(7, 4))
        for (w, op), pts in by_wl_op.items():
            if w != wl:
                continue
            pts = sorted(pts)
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            ax.plot(xs, ys, marker="o", label=op)
        ax.set_xlabel("Concurrency (clients)")
        ax.set_ylabel("Throughput (ops/sec)")
        ax.set_title(f"QPS vs concurrency - workload {wl}")
        ax.grid(alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(out / f"qps_vs_concurrency_{wl}.png", dpi=120)
        plt.close(fig)
    # p99 plot
    for wl in workloads:
        fig, ax = plt.subplots(figsize=(7, 4))
        for (w, op), pts in by_wl_op.items():
            if w != wl:
                continue
            pts = sorted(pts)
            xs = [p[0] for p in pts]
            ys = [p[2] for p in pts]
            ax.plot(xs, ys, marker="o", label=op)
        ax.set_xlabel("Concurrency (clients)")
        ax.set_ylabel("p99 latency (ms)")
        ax.set_title(f"p99 latency vs concurrency - workload {wl}")
        ax.grid(alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(out / f"p99_vs_concurrency_{wl}.png", dpi=120)
        plt.close(fig)
    print(f"  wrote {len(workloads)*2} plots under {out}/")


def plot_chaos(run_dir: Path):
    """For chaos runs: latency timeline + leader timeline."""
    out = run_dir / "plots"
    out.mkdir(exist_ok=True)

    lat = run_dir / "latency.json"
    if lat.exists():
        with lat.open() as f:
            results = json.load(f)
        for r in results:
            ps = r.get("per_second", [])
            if not ps:
                continue
            xs = [b["second"] for b in ps]
            p50 = [b["p50_ms"] for b in ps]
            p99 = [b["p99_ms"] for b in ps]
            count = [b["count"] for b in ps]

            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 5), sharex=True)
            ax1.plot(xs, p50, label="p50", alpha=0.7)
            ax1.plot(xs, p99, label="p99", alpha=0.9, color="C3")
            ax1.set_ylabel("Latency (ms)")
            ax1.set_title(f"Chaos timeline - op={r['operation']}")
            ax1.grid(alpha=0.3)
            ax1.legend()
            ax2.plot(xs, count, color="C2")
            ax2.set_ylabel("ops/sec")
            ax2.set_xlabel("seconds since run start")
            ax2.grid(alpha=0.3)
            fig.tight_layout()
            fig.savefig(out / f"chaos_latency_{r['operation']}.png", dpi=120)
            plt.close(fig)

    tl = run_dir / "leader_timeline.json"
    if tl.exists():
        with tl.open() as f:
            obs = json.load(f)
        if obs:
            nodes = sorted({k for o in obs for k in o.get("per_node", {})})
            fig, ax = plt.subplots(figsize=(8, 4))
            for n in nodes:
                xs = [o["elapsed_ms"] for o in obs if n in o.get("per_node", {})]
                ys = [o["per_node"][n] for o in obs if n in o.get("per_node", {})]
                ax.plot(xs, ys, marker=".", label=n, alpha=0.8)
            ax.set_xlabel("ms since fault injection")
            ax.set_ylabel("Reported leader peer_id")
            ax.set_title("Raft leader observation timeline")
            ax.grid(alpha=0.3)
            ax.legend(loc="best", fontsize=8)
            fig.tight_layout()
            fig.savefig(out / "leader_timeline.png", dpi=120)
            plt.close(fig)
            print(f"  wrote leader_timeline.png ({len(obs)} obs over {len(nodes)} nodes)")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    root = Path(sys.argv[1])
    if not root.exists():
        print(f"no such dir: {root}")
        sys.exit(1)

    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        print(f"[{child.name}]")
        if (child / "leader_timeline.json").exists():
            plot_chaos(child)
        else:
            plot_qps_p99(child)


if __name__ == "__main__":
    main()
