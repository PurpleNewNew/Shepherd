#!/usr/bin/env python3
import argparse
import csv
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _iter_run_dirs(root: str) -> Iterable[str]:
    for dirpath, dirnames, filenames in os.walk(root):
        fn = set(filenames)
        if "metrics.jsonl" in fn and "labels.json" in fn:
            yield dirpath


def _iter_jsonl(path: str) -> Iterable[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _as_int(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        v = v.strip()
        if v == "":
            return default
        try:
            return int(v)
        except ValueError:
            return default
    return default


def _expected_wait_seconds(sleep_s: int, work_s: int) -> Optional[float]:
    # As used in the opening report: E[T_w] = Tsleep^2 / (2*(Tsleep+Twork)).
    if sleep_s <= 0:
        return 0.0
    if work_s <= 0:
        work_s = 10
    denom = 2.0 * float(sleep_s + work_s)
    return float(sleep_s * sleep_s) / denom


@dataclass
class RunResult:
    run_dir: str
    topology: str
    nodes: int
    sleep_seconds: int
    work_seconds: int
    jitter_percent: float
    enqueues_ms: List[int]
    deliveries_ms: List[int]


def _extract_sleep_config(records: List[Dict[str, Any]], node_label: str) -> Tuple[int, int, float]:
    sleep_s = 0
    work_s = 0
    jitter = 0.0
    for r in records:
        if (r.get("kind") or "").strip().lower() != "trace_action":
            continue
        if (r.get("action") or "").strip().lower() != "sleep":
            continue
        if (r.get("node") or "").strip() != node_label:
            continue
        ss = r.get("sleep_seconds")
        ws = r.get("work_seconds")
        jp = r.get("jitter_percent")
        sleep_s = _as_int(ss, 0)
        work_s = _as_int(ws, 0)
        try:
            jitter = float(jp) if jp is not None else 0.0
        except Exception:
            jitter = 0.0
        break
    return sleep_s, work_s, jitter


def _extract_enqueue_times(records: List[Dict[str, Any]], target_label: str) -> List[int]:
    out: List[int] = []
    for r in records:
        if (r.get("kind") or "").strip().lower() != "trace_action":
            continue
        if (r.get("action") or "").strip().lower() != "dtn_enqueue":
            continue
        if (r.get("target") or "").strip() != target_label:
            continue
        ms = r.get("since_start_ms")
        if isinstance(ms, int):
            out.append(ms)
    out.sort()
    return out


def _extract_delivery_times(records: List[Dict[str, Any]]) -> List[int]:
    deliveries: List[int] = []
    prev = 0
    for r in records:
        if (r.get("kind") or "").strip().lower() != "metrics":
            continue
        ms = r.get("since_start_ms")
        if not isinstance(ms, int):
            continue
        metrics = r.get("metrics") or {}
        dtn = metrics.get("dtn_metrics") or {}
        delivered = _as_int(dtn.get("delivered"), 0)
        if delivered > prev:
            # If it jumps by k between snapshots, attribute all k deliveries to this timestamp.
            for _ in range(delivered - prev):
                deliveries.append(ms)
        prev = delivered
    return deliveries


def _load_run(run_dir: str, target_label: str) -> Optional[RunResult]:
    cfg = {}
    cfg_path = os.path.join(run_dir, "config.json")
    if os.path.exists(cfg_path):
        try:
            cfg = _read_json(cfg_path)
        except Exception:
            cfg = {}

    metrics_path = os.path.join(run_dir, "metrics.jsonl")
    if not os.path.exists(metrics_path):
        return None
    records = list(_iter_jsonl(metrics_path))

    sleep_s, work_s, jitter = _extract_sleep_config(records, target_label)
    enqueues = _extract_enqueue_times(records, target_label)
    deliveries = _extract_delivery_times(records)

    return RunResult(
        run_dir=run_dir,
        topology=(cfg.get("topology") or "").strip(),
        nodes=_as_int(cfg.get("nodes"), 0),
        sleep_seconds=sleep_s,
        work_seconds=work_s,
        jitter_percent=jitter,
        enqueues_ms=enqueues,
        deliveries_ms=deliveries,
    )


def _summarize_latencies(latencies_ms: List[int]) -> Dict[str, Any]:
    if not latencies_ms:
        return {"n": 0, "mean_ms": "", "p50_ms": "", "min_ms": "", "max_ms": ""}
    xs = sorted(latencies_ms)
    n = len(xs)
    mean = sum(xs) / float(n)
    p50 = xs[n // 2]
    return {
        "n": n,
        "mean_ms": int(round(mean)),
        "p50_ms": int(p50),
        "min_ms": int(xs[0]),
        "max_ms": int(xs[-1]),
    }


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Extract DTN delivery latency samples from trace_replay metrics.jsonl.")
    ap.add_argument("--root", required=True, help="Root directory to scan (e.g. experiments/out/dtn)")
    ap.add_argument("--target", default="n3", help="Target node label (default: n3)")
    ap.add_argument("--out-samples", required=True, help="Output CSV for per-message samples")
    ap.add_argument("--out-summary", required=True, help="Output CSV for per-run summary")
    args = ap.parse_args(argv)

    samples_rows: List[Dict[str, Any]] = []
    summary_rows: List[Dict[str, Any]] = []

    for run_dir in sorted(_iter_run_dirs(args.root)):
        rr = _load_run(run_dir, args.target)
        if rr is None:
            continue

        pairs = min(len(rr.enqueues_ms), len(rr.deliveries_ms))
        latencies: List[int] = []
        for i in range(pairs):
            enq = rr.enqueues_ms[i]
            dlv = rr.deliveries_ms[i]
            lat = dlv - enq
            latencies.append(lat)
            samples_rows.append(
                {
                    "run_dir": rr.run_dir,
                    "topology": rr.topology,
                    "nodes": rr.nodes,
                    "target": args.target,
                    "sleep_seconds": rr.sleep_seconds,
                    "work_seconds": rr.work_seconds,
                    "jitter_percent": rr.jitter_percent,
                    "msg_index": i + 1,
                    "enqueue_ms": enq,
                    "deliver_ms": dlv,
                    "latency_ms": lat,
                }
            )

        stats = _summarize_latencies(latencies)
        summary_rows.append(
            {
                "run_dir": rr.run_dir,
                "topology": rr.topology,
                "nodes": rr.nodes,
                "target": args.target,
                "sleep_seconds": rr.sleep_seconds,
                "work_seconds": rr.work_seconds,
                "jitter_percent": rr.jitter_percent,
                "expected_wait_s": _expected_wait_seconds(rr.sleep_seconds, rr.work_seconds),
                **stats,
                "enqueued_n": len(rr.enqueues_ms),
                "delivered_n": len(rr.deliveries_ms),
            }
        )

    os.makedirs(os.path.dirname(os.path.abspath(args.out_samples)), exist_ok=True)
    with open(args.out_samples, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "run_dir",
                "topology",
                "nodes",
                "target",
                "sleep_seconds",
                "work_seconds",
                "jitter_percent",
                "msg_index",
                "enqueue_ms",
                "deliver_ms",
                "latency_ms",
            ],
        )
        w.writeheader()
        for r in samples_rows:
            w.writerow(r)

    with open(args.out_summary, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "run_dir",
                "topology",
                "nodes",
                "target",
                "sleep_seconds",
                "work_seconds",
                "jitter_percent",
                "expected_wait_s",
                "n",
                "mean_ms",
                "p50_ms",
                "min_ms",
                "max_ms",
                "enqueued_n",
                "delivered_n",
            ],
        )
        w.writeheader()
        for r in summary_rows:
            w.writerow(r)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(os.sys.argv[1:]))

