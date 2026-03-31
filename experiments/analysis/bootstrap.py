#!/usr/bin/env python3
import argparse
import csv
import json
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _iter_run_dirs(root: str) -> Iterable[str]:
    # A run dir is identified by the presence of both labels.json and metrics.jsonl.
    for dirpath, dirnames, filenames in os.walk(root):
        fn = set(filenames)
        if "labels.json" in fn and "metrics.jsonl" in fn:
            yield dirpath


def _count_online(nodes: List[Dict[str, Any]]) -> int:
    online = 0
    for n in nodes or []:
        status = (n.get("status") or "").strip().lower()
        if status in ("online", "alive", "1", "true"):
            online += 1
    return online


def _iter_metrics(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (rec.get("kind") or "").strip().lower() != "metrics":
                continue
            yield rec


def _extract_topo_counts(rec: Dict[str, Any]) -> Tuple[int, int, int]:
    topo = rec.get("topology") or {}
    nodes = topo.get("nodes") or []
    edges = topo.get("edges") or []
    total = len(nodes)
    online = _count_online(nodes)
    edges_total = len(edges)
    return total, online, edges_total


def _find_first_ms_where(
    metrics: Iterable[Dict[str, Any]], predicate
) -> Optional[int]:
    for rec in metrics:
        ms = rec.get("since_start_ms")
        if not isinstance(ms, int):
            continue
        try:
            ok = predicate(rec)
        except Exception:
            ok = False
        if ok:
            return ms
    return None


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Summarize trace_replay bootstrap/convergence time from metrics.jsonl.")
    ap.add_argument("--root", required=True, help="Root directory to scan (e.g. experiments/out/bootstrap)")
    ap.add_argument("--out", required=True, help="Output CSV path")
    args = ap.parse_args(argv)

    rows: List[Dict[str, Any]] = []

    for run_dir in sorted(_iter_run_dirs(args.root)):
        labels_path = os.path.join(run_dir, "labels.json")
        metrics_path = os.path.join(run_dir, "metrics.jsonl")
        config_path = os.path.join(run_dir, "config.json")

        try:
            labels = _read_json(labels_path)
        except Exception:
            continue
        expected_nodes = len((labels.get("nodes") or {}).keys())

        cfg: Dict[str, Any] = {}
        if os.path.exists(config_path):
            try:
                cfg = _read_json(config_path)
            except Exception:
                cfg = {}

        metrics = list(_iter_metrics(metrics_path))

        boot_ms = _find_first_ms_where(metrics, lambda r: (r.get("note") or "").strip() == "bootstrapped")

        # Topology snapshots include an Admin->root edge, so for N agents we expect at least N edges.
        converge_ms = _find_first_ms_where(
            metrics,
            lambda r: (
                (lambda total, online, edges: total == expected_nodes and online == expected_nodes and edges >= expected_nodes)(
                    *_extract_topo_counts(r)
                )
            ),
        )

        final_total, final_online, final_edges = (0, 0, 0)
        if metrics:
            final_total, final_online, final_edges = _extract_topo_counts(metrics[-1])

        rows.append(
            {
                "run_dir": run_dir,
                "topology": (cfg.get("topology") or "").strip() or "",
                "nodes": cfg.get("nodes", expected_nodes),
                "duration": (cfg.get("duration") or "").strip() or "",
                "metrics_every": (cfg.get("metrics_every") or "").strip() or "",
                "bootstrapped_ms": boot_ms if boot_ms is not None else "",
                "converged_ms": converge_ms if converge_ms is not None else "",
                "final_nodes_total": final_total,
                "final_nodes_online": final_online,
                "final_edges_total": final_edges,
            }
        )

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "run_dir",
                "topology",
                "nodes",
                "duration",
                "metrics_every",
                "bootstrapped_ms",
                "converged_ms",
                "final_nodes_total",
                "final_nodes_online",
                "final_edges_total",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(r)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(os.sys.argv[1:]))

