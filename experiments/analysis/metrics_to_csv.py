#!/usr/bin/env python3
import argparse
import csv
import json
import sys


def _count_online(nodes):
    online = 0
    for n in nodes or []:
        status = (n.get("status") or "").strip().lower()
        if status in ("online", "alive", "1", "true"):
            online += 1
    return online


def _count_supp_edges(edges):
    supp = 0
    for e in edges or []:
        if e.get("supplemental") is True:
            supp += 1
    return supp


def main(argv):
    ap = argparse.ArgumentParser(description="Convert trace_replay metrics.jsonl into a flat CSV.")
    ap.add_argument("--in", dest="in_path", required=True, help="Path to metrics.jsonl")
    ap.add_argument("--out", dest="out_path", default="-", help="Output CSV path (default: stdout)")
    args = ap.parse_args(argv)

    inp = open(args.in_path, "r", encoding="utf-8")
    out = sys.stdout if args.out_path == "-" else open(args.out_path, "w", encoding="utf-8", newline="")

    fieldnames = [
        "since_start_ms",
        "note",
        "nodes_total",
        "nodes_online",
        "edges_total",
        "supp_edges",
        "streams_total",
        "dtn_enqueued",
        "dtn_delivered",
        "dtn_failed",
        "dtn_retried",
        "dtn_queue_total",
        "dtn_queue_ready",
        "dtn_queue_held",
        "supp_enabled",
        "supp_queue_length",
        "supp_pending_actions",
        "supp_active_links",
        "supp_dispatched",
        "supp_success",
        "supp_failures",
        "supp_dropped",
    ]

    w = csv.DictWriter(out, fieldnames=fieldnames)
    w.writeheader()

    for line in inp:
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if (rec.get("kind") or "").strip().lower() != "metrics":
            continue

        topo = rec.get("topology") or {}
        nodes = topo.get("nodes") or []
        edges = topo.get("edges") or []
        streams = (rec.get("streams") or {}).get("streams") or []
        metrics = rec.get("metrics") or {}
        dtn = metrics.get("dtn_metrics") or {}
        dtnq = dtn.get("global_queue") or {}
        supp_status = rec.get("supp_status") or {}
        supp_metrics = rec.get("supp_metrics") or {}

        row = {
            "since_start_ms": rec.get("since_start_ms", ""),
            "note": rec.get("note", ""),
            "nodes_total": len(nodes),
            "nodes_online": _count_online(nodes),
            "edges_total": len(edges),
            "supp_edges": _count_supp_edges(edges),
            "streams_total": len(streams),
            "dtn_enqueued": dtn.get("enqueued", ""),
            "dtn_delivered": dtn.get("delivered", ""),
            "dtn_failed": dtn.get("failed", ""),
            "dtn_retried": dtn.get("retried", ""),
            "dtn_queue_total": dtnq.get("total", ""),
            "dtn_queue_ready": dtnq.get("ready", ""),
            "dtn_queue_held": dtnq.get("held", ""),
            "supp_enabled": supp_status.get("enabled", ""),
            "supp_queue_length": supp_status.get("queue_length", ""),
            "supp_pending_actions": supp_status.get("pending_actions", ""),
            "supp_active_links": supp_status.get("active_links", ""),
            "supp_dispatched": supp_metrics.get("dispatched", ""),
            "supp_success": supp_metrics.get("success", ""),
            "supp_failures": supp_metrics.get("failures", ""),
            "supp_dropped": supp_metrics.get("dropped", ""),
        }
        w.writerow(row)

    if out is not sys.stdout:
        out.close()
    inp.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

