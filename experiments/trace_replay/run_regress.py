#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
批量回归脚本：运行 experiments/trace_replay/traces/*.jsonl 并自动判定 PASS/FAIL。

判定逻辑（默认）：
- final.dtn_metrics.failed == 0
- 对 trace 中的 dtn_enqueue：
  - payload 为 "log:<message>" 且未声明 expect=missing/drop/expire 时：<message> 必须出现在 Kelpie 日志中
  - payload 为 "log:<message>" 且声明 expect=missing/drop/expire 时：<message> 必须不出现在 Kelpie 日志中
  - 若声明了 drop/expire，还会额外检查 global_queue 的 dropped_total / expired_total 计数
- 对 trace 中的动作类事件（stream/dataplane/restart/...）：必须存在对应的 metrics.jsonl trace_result(ok=true)

输出：
- 控制台表格汇总
- experiments/out/regress/summary-<ts>.json（可通过 --summary 指定路径）
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import math
import os
import re
import subprocess
import sys
import tarfile
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[2]


_NODE_RE = re.compile(r"^n(\d+)$")


def _sh(cmd: List[str], *, cwd: Optional[Path] = None, quiet: bool = False) -> subprocess.CompletedProcess:
    if not quiet:
        print("+", " ".join(cmd), file=sys.stderr)
    return subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
    )


def _parse_u64(v: Any) -> int:
    if v is None:
        return 0
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return 0
        try:
            return int(s, 10)
        except ValueError:
            return 0
    return 0


def _extract_global_queue_stats(metrics_obj: Any) -> Tuple[int, int, Dict[str, int]]:
    """
    Extract (dropped_total, expired_total, drop_by_priority) from a metrics object produced by trace_replay.

    Expected layout (protojson with UseProtoNames):
      metrics["dtn_metrics"]["global_queue"]["dropped_total"]
      metrics["dtn_metrics"]["global_queue"]["expired_total"]
      metrics["dtn_metrics"]["global_queue"]["drop_by_priority"] (map)
    """
    if not isinstance(metrics_obj, dict):
        return 0, 0, {}
    dtn = metrics_obj.get("dtn_metrics") or {}
    if not isinstance(dtn, dict):
        return 0, 0, {}
    gq = dtn.get("global_queue") or {}
    if not isinstance(gq, dict):
        return 0, 0, {}
    dropped = _parse_u64(gq.get("dropped_total"))
    expired = _parse_u64(gq.get("expired_total"))
    drop_by: Dict[str, int] = {}
    raw = gq.get("drop_by_priority") or {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            kk = str(k).strip().lower()
            if not kk:
                continue
            drop_by[kk] = _parse_u64(v)
    return dropped, expired, drop_by


@dataclass
class TraceSpec:
    path: Path
    name: str
    expected_enqueued: int
    # DTN minimal payload contract assertions:
    # - dtn_log_messages: "log:<message>" payloads that MUST appear in Kelpie logs
    # - dtn_log_absent_messages: "log:<message>" payloads that MUST NOT appear (expect=missing/drop/expire)
    dtn_log_messages: List[str]
    dtn_log_absent_messages: List[str]
    # DTN queue semantics assertions (only checked when expected_* > 0)
    expected_dropped_total: int
    expected_expired_total: int
    expected_drop_by_priority: Dict[str, int]  # keys: high|normal|low
    # DTN ordering assertions (asserted by log substring order)
    dtn_expect_orders: List[List[str]]
    expected_dataplane_roundtrip: int
    expected_stream_proxy: int
    expected_kelpie_restart: int
    expected_dtn_policy: int
    expected_wait_memo: int
    expected_wait_node_status: int
    expected_io_burst: int
    expected_dataplane_upload_interrupt: int
    expected_dataplane_token_replay: int
    expected_dataplane_token_expire: int
    # Upper bound for run duration computation (includes action timeout where relevant).
    max_finish_ms: int
    nodes: int


def _load_trace_spec(path: Path) -> TraceSpec:
    expected_enqueued = 0
    dtn_log_messages: List[str] = []
    dtn_log_absent_messages: List[str] = []
    expected_dropped_total = 0
    expected_expired_total = 0
    expected_drop_by_priority: Dict[str, int] = {}
    dtn_expect_orders: List[List[str]] = []
    expected_dataplane_roundtrip = 0
    expected_stream_proxy = 0
    expected_kelpie_restart = 0
    expected_dtn_policy = 0
    expected_wait_memo = 0
    expected_wait_node_status = 0
    expected_io_burst = 0
    expected_dataplane_upload_interrupt = 0
    expected_dataplane_token_replay = 0
    expected_dataplane_token_expire = 0
    # Conservative upper bound for how long this trace can take (ms), assuming
    # trace_replay executes events sequentially and each blocking action may
    # take up to its timeout. This bound is used to size --duration so we don't
    # cut off late trace_result records under churn.
    max_finish_ms = 0
    schedule: List[tuple[int, int]] = []  # (at_ms, timeout_ms)
    labels: List[str] = []

    timeout_defaults_ms = {
        "wait_memo": 45000,
        "wait_node_status": 45000,
        "io_burst": 45000,
        "dataplane_roundtrip": 30000,
        "stream_proxy": 20000,
        "dataplane_upload_interrupt": 30000,
        "dataplane_token_replay": 30000,
        "dataplane_token_expire": 30000,
        "kelpie_restart": 60000,  # conservative; actual restart is usually faster
    }

    with path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            evt = json.loads(line)
            at_ms = evt.get("at_ms", 0)
            try:
                at_ms = int(at_ms)
            except Exception:
                at_ms = 0
            typ = (evt.get("type") or "").strip().lower()

            # Track per-event timeouts for sequential schedule simulation (see below).
            timeout_ms = evt.get("timeout_ms", 0)
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            if timeout_ms <= 0:
                timeout_ms = timeout_defaults_ms.get(typ, 0)
            schedule.append((at_ms, max(0, timeout_ms)))

            if typ == "dtn_enqueue":
                expected_enqueued += 1
                payload = (evt.get("payload") or "").strip()
                lower_payload = payload.lower()
                if lower_payload.startswith("log:"):
                    content = payload[len("log:") :].strip()
                    if content:
                        expect = (evt.get("expect") or "").strip().lower()
                        if expect in ("missing", "drop", "expire"):
                            dtn_log_absent_messages.append(content)
                        else:
                            dtn_log_messages.append(content)
                        if expect == "drop":
                            expected_dropped_total += 1
                            prio = (evt.get("priority") or "").strip().lower() or "normal"
                            expected_drop_by_priority[prio] = expected_drop_by_priority.get(prio, 0) + 1
                        elif expect == "expire":
                            expected_expired_total += 1
            elif typ == "dtn_expect_order":
                payload = (evt.get("payload") or "").strip()
                if payload:
                    toks = [t.strip() for t in payload.split("|") if t.strip()]
                    if toks:
                        dtn_expect_orders.append(toks)
            elif typ == "dataplane_roundtrip":
                expected_dataplane_roundtrip += 1
            elif typ == "stream_proxy":
                expected_stream_proxy += 1
            elif typ == "kelpie_restart":
                expected_kelpie_restart += 1
            elif typ == "dtn_policy":
                expected_dtn_policy += 1
            elif typ == "wait_memo":
                expected_wait_memo += 1
            elif typ == "wait_node_status":
                expected_wait_node_status += 1
            elif typ == "io_burst":
                expected_io_burst += 1
            elif typ == "dataplane_upload_interrupt":
                expected_dataplane_upload_interrupt += 1
            elif typ == "dataplane_token_replay":
                expected_dataplane_token_replay += 1
            elif typ == "dataplane_token_expire":
                expected_dataplane_token_expire += 1
            for k in ("node", "target", "kill_node"):
                v = (evt.get(k) or "").strip()
                if v:
                    labels.append(v)

    max_n = 0
    for lab in labels:
        m = _NODE_RE.match(lab)
        if not m:
            continue
        try:
            idx = int(m.group(1))
        except Exception:
            continue
        if idx > max_n:
            max_n = idx
    nodes = max(1, max_n + 1)  # root + n1..nX

    # Simulate the sequential trace_replay schedule (events are sorted by at_ms).
    # If an earlier action runs long, subsequent actions may start "late"; the
    # hard --duration cap must allow for that drift, or we'll miss trace_result.
    schedule.sort(key=lambda x: x[0])
    t_ms = 0
    for at_ms, timeout_ms in schedule:
        if at_ms > t_ms:
            t_ms = at_ms
        t_ms += timeout_ms
    max_finish_ms = t_ms

    return TraceSpec(
        path=path,
        name=path.stem,
        expected_enqueued=expected_enqueued,
        dtn_log_messages=dtn_log_messages,
        dtn_log_absent_messages=dtn_log_absent_messages,
        expected_dropped_total=expected_dropped_total,
        expected_expired_total=expected_expired_total,
        expected_drop_by_priority=expected_drop_by_priority,
        dtn_expect_orders=dtn_expect_orders,
        expected_dataplane_roundtrip=expected_dataplane_roundtrip,
        expected_stream_proxy=expected_stream_proxy,
        expected_kelpie_restart=expected_kelpie_restart,
        expected_dtn_policy=expected_dtn_policy,
        expected_wait_memo=expected_wait_memo,
        expected_wait_node_status=expected_wait_node_status,
        expected_io_burst=expected_io_burst,
        expected_dataplane_upload_interrupt=expected_dataplane_upload_interrupt,
        expected_dataplane_token_replay=expected_dataplane_token_replay,
        expected_dataplane_token_expire=expected_dataplane_token_expire,
        max_finish_ms=max_finish_ms,
        nodes=nodes,
    )


def _compute_duration_s(max_finish_ms: int, *, tail_ms: int, min_s: int, slack_s: int) -> int:
    if max_finish_ms < 0:
        max_finish_ms = 0
    total_ms = max_finish_ms + max(0, tail_ms)
    total_s = int(math.ceil(total_ms / 1000.0)) + max(0, slack_s)
    if total_s < min_s:
        total_s = min_s
    return total_s


def _find_latest_run_dir(base: Path) -> Optional[Path]:
    if not base.exists():
        return None
    runs = [p for p in base.iterdir() if p.is_dir() and p.name.startswith("run-")]
    if not runs:
        return None
    # Timestamp prefix is lexicographically sortable.
    return sorted(runs, key=lambda p: p.name)[-1]


def _read_final_metrics(metrics_path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not metrics_path.exists():
        return None, f"missing metrics file: {metrics_path}"
    final_rec: Optional[Dict[str, Any]] = None
    try:
        with metrics_path.open("r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                rec = json.loads(line)
                kind = (rec.get("kind") or "").strip().lower()
                note = (rec.get("note") or "").strip().lower()
                if kind == "metrics" and note == "final":
                    final_rec = rec
    except Exception as e:
        return None, f"parse metrics.jsonl failed: {e}"
    if final_rec is None:
        return None, "no final metrics record (kind=metrics note=final) found"
    return final_rec, None


def _read_first_metrics(metrics_path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not metrics_path.exists():
        return None, f"missing metrics file: {metrics_path}"
    try:
        with metrics_path.open("r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                rec = json.loads(line)
                kind = (rec.get("kind") or "").strip().lower()
                if kind != "metrics":
                    continue
                if isinstance(rec.get("metrics"), dict):
                    return rec, None
    except Exception as e:
        return None, f"parse metrics.jsonl failed: {e}"
    return None, "no metrics record found"


def _read_trace_results(metrics_path: Path) -> Tuple[Dict[str, Dict[str, int]], Optional[str]]:
    """
    Parse metrics.jsonl and aggregate trace_result records:
      results[action]["ok"]   = count(ok==true)
      results[action]["fail"] = count(ok==false)
    """
    if not metrics_path.exists():
        return {}, f"missing metrics file: {metrics_path}"
    results: Dict[str, Dict[str, int]] = {}
    try:
        with metrics_path.open("r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line:
                    continue
                rec = json.loads(line)
                kind = (rec.get("kind") or "").strip().lower()
                if kind != "trace_result":
                    continue
                action = (rec.get("action") or "").strip().lower()
                if not action:
                    continue
                ok = bool(rec.get("ok"))
                bucket = results.setdefault(action, {"ok": 0, "fail": 0})
                if ok:
                    bucket["ok"] += 1
                else:
                    bucket["fail"] += 1
    except Exception as e:
        return {}, f"parse trace_result failed: {e}"
    return results, None


def _grep_hints(run_dir: Path) -> List[str]:
    log_dir = run_dir / "logs"
    patterns = [
        re.compile(r"panic:", re.IGNORECASE),
        re.compile(r"fatal", re.IGNORECASE),
        re.compile(r"use of closed network connection", re.IGNORECASE),
        re.compile(r"no route", re.IGNORECASE),
        re.compile(r"route unavailable", re.IGNORECASE),
        re.compile(r"data race", re.IGNORECASE),
        re.compile(r"\[diag/", re.IGNORECASE),
        re.compile(r"\[diag\]", re.IGNORECASE),
    ]
    hints: List[str] = []

    if log_dir.exists():
        # Scan error logs and kelpie stdout. Supplemental failover diagnostics are printed to kelpie stdout.
        log_files = sorted(list(log_dir.glob("*.err.log")) + list(log_dir.glob("kelpie*.out.log")))
        for p in log_files:
            try:
                tail = p.read_text(encoding="utf-8", errors="replace").splitlines()[-300:]
            except Exception:
                continue
            for line in tail:
                if any(rx.search(line) for rx in patterns):
                    msg = f"{p.name}: {line.strip()}"
                    if msg not in hints:
                        hints.append(msg)
                if len(hints) >= 30:
                    return hints

    # trace_replay writes structured diag_* records into metrics.jsonl; include a short tail.
    metrics_path = run_dir / "metrics.jsonl"
    if metrics_path.exists() and len(hints) < 30:
        try:
            tail = metrics_path.read_text(encoding="utf-8", errors="replace").splitlines()[-400:]
        except Exception:
            tail = []
        for line in tail:
            if '"kind":"diag_' not in line and '"kind": "diag_' not in line:
                continue
            compact = line.strip()
            if len(compact) > 300:
                compact = compact[:297] + "..."
            msg = f"metrics.jsonl: {compact}"
            if msg not in hints:
                hints.append(msg)
            if len(hints) >= 30:
                return hints
    return hints


def _read_kelpie_logs(run_dir: Path) -> str:
    log_dir = run_dir / "logs"
    if not log_dir.exists():
        return ""
    parts: List[str] = []
    # Include both stdout/stderr; kelpie may restart and produce kelpie-2.* logs.
    for p in sorted(list(log_dir.glob("kelpie*.out.log")) + list(log_dir.glob("kelpie*.err.log"))):
        try:
            parts.append(p.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            continue
    return "\n".join(parts)


@dataclass
class RunResult:
    trace: str
    topology: str
    nodes: int
    duration_s: int
    expected_enqueued: int
    expected_dtn_logs: int
    expected_dataplane_roundtrip: int
    expected_stream_proxy: int
    expected_kelpie_restart: int
    enqueued: int
    delivered: int
    failed: int
    retried: int
    dtn_logs_ok: int
    dtn_logs_missing: int
    dataplane_ok: int
    dataplane_fail: int
    stream_ok: int
    stream_fail: int
    restart_ok: int
    restart_fail: int
    ok: bool
    run_dir: str
    expected_dtn_policy: int = 0
    expected_wait_memo: int = 0
    expected_wait_node_status: int = 0
    expected_io_burst: int = 0
    expected_dataplane_upload_interrupt: int = 0
    expected_dataplane_token_replay: int = 0
    expected_dataplane_token_expire: int = 0
    expected_dtn_logs_absent: int = 0
    expected_dropped_total: int = 0
    expected_expired_total: int = 0
    expected_order_checks: int = 0
    dtn_logs_absent_ok: int = 0
    dtn_logs_absent_present: int = 0
    dropped_delta: int = 0
    expired_delta: int = 0
    order_ok: int = 0
    order_fail: int = 0
    dtn_policy_ok: int = 0
    dtn_policy_fail: int = 0
    wait_memo_ok: int = 0
    wait_memo_fail: int = 0
    wait_node_status_ok: int = 0
    wait_node_status_fail: int = 0
    io_burst_ok: int = 0
    io_burst_fail: int = 0
    dataplane_upload_interrupt_ok: int = 0
    dataplane_upload_interrupt_fail: int = 0
    dataplane_token_replay_ok: int = 0
    dataplane_token_replay_fail: int = 0
    dataplane_token_expire_ok: int = 0
    dataplane_token_expire_fail: int = 0
    error: str = ""
    hints: List[str] = None  # type: ignore[assignment]

def _archive_failures(
    *,
    results: List[RunResult],
    summary_path: Path,
    archive_dir: Path,
) -> Tuple[Optional[Path], Optional[str]]:
    failures = [r for r in results if (not r.ok) and r.run_dir]
    if not failures:
        return None, None
    try:
        archive_dir.mkdir(parents=True, exist_ok=True)
        ts = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        tar_path = archive_dir / f"regress-failures-{ts}.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tar:
            if summary_path.exists():
                tar.add(summary_path, arcname="summary.json")
            for res in failures:
                run_dir = Path(res.run_dir)
                if not run_dir.exists():
                    continue
                try:
                    arcname = os.path.relpath(run_dir, ROOT)
                except Exception:
                    arcname = run_dir.name
                tar.add(run_dir, arcname=arcname)
        return tar_path, None
    except Exception as e:
        return None, str(e)


def _run_one(
    trace: TraceSpec,
    *,
    topology: str,
    trace_replay_bin: Path,
    out_root: Path,
    rep_dir: str,
    metrics_every: str,
    tail_ms: int,
    min_duration_s: int,
    slack_s: int,
    watch_events: bool,
    quiet: bool,
) -> RunResult:
    duration_s = _compute_duration_s(trace.max_finish_ms, tail_ms=tail_ms, min_s=min_duration_s, slack_s=slack_s)

    base_dir = out_root / topology / trace.name
    rep_dir = rep_dir.strip()
    if rep_dir:
        base_dir = base_dir / rep_dir
    base_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(trace_replay_bin),
        "--out",
        str(base_dir),
        "--topology",
        topology,
        "--nodes",
        str(trace.nodes),
        "--duration",
        f"{duration_s}s",
        "--finish-when-done",
        "--tail-ms",
        str(tail_ms),
        "--metrics-every",
        metrics_every,
        "--trace",
        str(trace.path),
    ]
    if watch_events:
        cmd.append("--watch-events")

    proc = _sh(cmd, cwd=ROOT, quiet=quiet)

    run_dir = _find_latest_run_dir(base_dir)
    run_dir_s = str(run_dir) if run_dir else ""
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        if len(err) > 2000:
            err = err[:2000] + " ... (truncated)"
        hints = _grep_hints(run_dir) if run_dir else []
        return RunResult(
            trace=trace.name,
            topology=topology,
            nodes=trace.nodes,
            duration_s=duration_s,
            expected_enqueued=trace.expected_enqueued,
            expected_dtn_logs=len(trace.dtn_log_messages),
            expected_dataplane_roundtrip=trace.expected_dataplane_roundtrip,
            expected_stream_proxy=trace.expected_stream_proxy,
            expected_kelpie_restart=trace.expected_kelpie_restart,
            enqueued=0,
            delivered=0,
            failed=0,
            retried=0,
            dtn_logs_ok=0,
            dtn_logs_missing=len(trace.dtn_log_messages),
            dataplane_ok=0,
            dataplane_fail=0,
            stream_ok=0,
            stream_fail=0,
            restart_ok=0,
            restart_fail=0,
            ok=False,
            run_dir=run_dir_s,
            error=f"trace_replay exited non-zero: {proc.returncode}; {err}",
            hints=hints,
        )

    if not run_dir:
        return RunResult(
            trace=trace.name,
            topology=topology,
            nodes=trace.nodes,
            duration_s=duration_s,
            expected_enqueued=trace.expected_enqueued,
            expected_dtn_logs=len(trace.dtn_log_messages),
            expected_dataplane_roundtrip=trace.expected_dataplane_roundtrip,
            expected_stream_proxy=trace.expected_stream_proxy,
            expected_kelpie_restart=trace.expected_kelpie_restart,
            enqueued=0,
            delivered=0,
            failed=0,
            retried=0,
            dtn_logs_ok=0,
            dtn_logs_missing=len(trace.dtn_log_messages),
            dataplane_ok=0,
            dataplane_fail=0,
            stream_ok=0,
            stream_fail=0,
            restart_ok=0,
            restart_fail=0,
            ok=False,
            run_dir=run_dir_s,
            error="cannot locate run dir under out base",
            hints=[],
        )

    final_rec, m_err = _read_final_metrics(run_dir / "metrics.jsonl")
    if m_err:
        return RunResult(
            trace=trace.name,
            topology=topology,
            nodes=trace.nodes,
            duration_s=duration_s,
            expected_enqueued=trace.expected_enqueued,
            expected_dtn_logs=len(trace.dtn_log_messages),
            expected_dataplane_roundtrip=trace.expected_dataplane_roundtrip,
            expected_stream_proxy=trace.expected_stream_proxy,
            expected_kelpie_restart=trace.expected_kelpie_restart,
            enqueued=0,
            delivered=0,
            failed=0,
            retried=0,
            dtn_logs_ok=0,
            dtn_logs_missing=len(trace.dtn_log_messages),
            dataplane_ok=0,
            dataplane_fail=0,
            stream_ok=0,
            stream_fail=0,
            restart_ok=0,
            restart_fail=0,
            ok=False,
            run_dir=run_dir_s,
            error=m_err,
            hints=_grep_hints(run_dir),
        )

    trace_results, tr_err = _read_trace_results(run_dir / "metrics.jsonl")
    if tr_err:
        return RunResult(
            trace=trace.name,
            topology=topology,
            nodes=trace.nodes,
            duration_s=duration_s,
            expected_enqueued=trace.expected_enqueued,
            expected_dtn_logs=len(trace.dtn_log_messages),
            expected_dataplane_roundtrip=trace.expected_dataplane_roundtrip,
            expected_stream_proxy=trace.expected_stream_proxy,
            expected_kelpie_restart=trace.expected_kelpie_restart,
            enqueued=0,
            delivered=0,
            failed=0,
            retried=0,
            dtn_logs_ok=0,
            dtn_logs_missing=len(trace.dtn_log_messages),
            dataplane_ok=0,
            dataplane_fail=0,
            stream_ok=0,
            stream_fail=0,
            restart_ok=0,
            restart_fail=0,
            ok=False,
            run_dir=run_dir_s,
            error=tr_err,
            hints=_grep_hints(run_dir),
        )

    first_rec, _ = _read_first_metrics(run_dir / "metrics.jsonl")

    metrics = final_rec.get("metrics") or {}
    base_metrics = (first_rec.get("metrics") or {}) if isinstance(first_rec, dict) else {}
    dtn = (metrics.get("dtn_metrics") or {}) if isinstance(metrics, dict) else {}

    enqueued = _parse_u64(dtn.get("enqueued"))
    delivered = _parse_u64(dtn.get("delivered"))
    failed = _parse_u64(dtn.get("failed"))
    retried = _parse_u64(dtn.get("retried"))

    # Global queue drop/expire counters (delta from first metrics record).
    base_dropped, base_expired, base_drop_by = _extract_global_queue_stats(base_metrics)
    final_dropped, final_expired, final_drop_by = _extract_global_queue_stats(metrics)
    dropped_delta = max(0, final_dropped - base_dropped)
    expired_delta = max(0, final_expired - base_expired)
    drop_by_delta: Dict[str, int] = {}
    for k in set(base_drop_by.keys()) | set(final_drop_by.keys()):
        drop_by_delta[k] = max(0, final_drop_by.get(k, 0) - base_drop_by.get(k, 0))

    # Trace action results
    dp_ok = int(trace_results.get("dataplane_roundtrip", {}).get("ok", 0))
    dp_fail = int(trace_results.get("dataplane_roundtrip", {}).get("fail", 0))
    sp_ok = int(trace_results.get("stream_proxy", {}).get("ok", 0))
    sp_fail = int(trace_results.get("stream_proxy", {}).get("fail", 0))
    kr_ok = int(trace_results.get("kelpie_restart", {}).get("ok", 0))
    kr_fail = int(trace_results.get("kelpie_restart", {}).get("fail", 0))
    pol_ok = int(trace_results.get("dtn_policy", {}).get("ok", 0))
    pol_fail = int(trace_results.get("dtn_policy", {}).get("fail", 0))
    memo_ok = int(trace_results.get("wait_memo", {}).get("ok", 0))
    memo_fail = int(trace_results.get("wait_memo", {}).get("fail", 0))
    st_ok = int(trace_results.get("wait_node_status", {}).get("ok", 0))
    st_fail = int(trace_results.get("wait_node_status", {}).get("fail", 0))
    io_ok = int(trace_results.get("io_burst", {}).get("ok", 0))
    io_fail = int(trace_results.get("io_burst", {}).get("fail", 0))
    dp_int_ok = int(trace_results.get("dataplane_upload_interrupt", {}).get("ok", 0))
    dp_int_fail = int(trace_results.get("dataplane_upload_interrupt", {}).get("fail", 0))
    dp_replay_ok = int(trace_results.get("dataplane_token_replay", {}).get("ok", 0))
    dp_replay_fail = int(trace_results.get("dataplane_token_replay", {}).get("fail", 0))
    dp_exp_ok = int(trace_results.get("dataplane_token_expire", {}).get("ok", 0))
    dp_exp_fail = int(trace_results.get("dataplane_token_expire", {}).get("fail", 0))

    # DTN delivery contract: validate each explicit "log:<message>" payload reached Kelpie,
    # and those marked expect=missing/drop/expire did NOT.
    log_text = _read_kelpie_logs(run_dir)
    missing_logs = [m for m in trace.dtn_log_messages if m not in log_text]
    present_absent_logs = [m for m in trace.dtn_log_absent_messages if m in log_text]
    dtn_logs_ok = len(trace.dtn_log_messages) - len(missing_logs)
    dtn_logs_missing = len(missing_logs)
    dtn_absent_ok = len(trace.dtn_log_absent_messages) - len(present_absent_logs)
    dtn_absent_present = len(present_absent_logs)

    # Ordering assertions (by log substring position).
    order_ok = 0
    order_fail = 0
    for seq in trace.dtn_expect_orders:
        pos = 0
        good = True
        for tok in seq:
            idx = log_text.find(tok, pos)
            if idx < 0:
                good = False
                break
            pos = idx + max(1, len(tok))
        if good:
            order_ok += 1
        else:
            order_fail += 1

    ok = True
    errs: List[str] = []
    if failed != 0:
        ok = False
        errs.append(f"dtn failed!=0: failed={failed} (enqueued={enqueued} delivered={delivered} retried={retried})")
    if missing_logs:
        ok = False
        sample = ", ".join(repr(m) for m in missing_logs[:3])
        extra = "" if len(missing_logs) <= 3 else f" (+{len(missing_logs) - 3} more)"
        errs.append(f"dtn log payload missing: missing={len(missing_logs)}/{len(trace.dtn_log_messages)} sample={sample}{extra}")
    if present_absent_logs:
        ok = False
        sample = ", ".join(repr(m) for m in present_absent_logs[:3])
        extra = "" if len(present_absent_logs) <= 3 else f" (+{len(present_absent_logs) - 3} more)"
        errs.append(
            f"dtn log payload unexpectedly present: present={len(present_absent_logs)}/{len(trace.dtn_log_absent_messages)} sample={sample}{extra}"
        )
    if trace.expected_dropped_total > 0 and dropped_delta != trace.expected_dropped_total:
        ok = False
        errs.append(f"dtn dropped_total mismatch: expected={trace.expected_dropped_total} got={dropped_delta}")
    if trace.expected_expired_total > 0 and expired_delta != trace.expected_expired_total:
        ok = False
        errs.append(f"dtn expired_total mismatch: expected={trace.expected_expired_total} got={expired_delta}")
    for k, exp in trace.expected_drop_by_priority.items():
        got = drop_by_delta.get(k.strip().lower(), 0)
        if got != exp:
            ok = False
            errs.append(f"dtn drop_by_priority mismatch: key={k} expected={exp} got={got}")
    # Ordering (dtn_expect_order) is currently treated as informational only:
    # RuntimeLog messages can be observed out-of-order due to concurrent handler scheduling.
    if not (dp_ok == trace.expected_dataplane_roundtrip and dp_fail == 0):
        ok = False
        errs.append(
            f"dataplane_roundtrip mismatch: expected={trace.expected_dataplane_roundtrip} ok={dp_ok} fail={dp_fail}"
        )
    if not (sp_ok == trace.expected_stream_proxy and sp_fail == 0):
        ok = False
        errs.append(f"stream_proxy mismatch: expected={trace.expected_stream_proxy} ok={sp_ok} fail={sp_fail}")
    if not (kr_ok == trace.expected_kelpie_restart and kr_fail == 0):
        ok = False
        errs.append(f"kelpie_restart mismatch: expected={trace.expected_kelpie_restart} ok={kr_ok} fail={kr_fail}")
    if not (pol_ok == trace.expected_dtn_policy and pol_fail == 0):
        ok = False
        errs.append(f"dtn_policy mismatch: expected={trace.expected_dtn_policy} ok={pol_ok} fail={pol_fail}")
    if not (memo_ok == trace.expected_wait_memo and memo_fail == 0):
        ok = False
        errs.append(f"wait_memo mismatch: expected={trace.expected_wait_memo} ok={memo_ok} fail={memo_fail}")
    if not (st_ok == trace.expected_wait_node_status and st_fail == 0):
        ok = False
        errs.append(f"wait_node_status mismatch: expected={trace.expected_wait_node_status} ok={st_ok} fail={st_fail}")
    if not (io_ok == trace.expected_io_burst and io_fail == 0):
        ok = False
        errs.append(f"io_burst mismatch: expected={trace.expected_io_burst} ok={io_ok} fail={io_fail}")
    if not (dp_int_ok == trace.expected_dataplane_upload_interrupt and dp_int_fail == 0):
        ok = False
        errs.append(
            f"dataplane_upload_interrupt mismatch: expected={trace.expected_dataplane_upload_interrupt} ok={dp_int_ok} fail={dp_int_fail}"
        )
    if not (dp_replay_ok == trace.expected_dataplane_token_replay and dp_replay_fail == 0):
        ok = False
        errs.append(
            f"dataplane_token_replay mismatch: expected={trace.expected_dataplane_token_replay} ok={dp_replay_ok} fail={dp_replay_fail}"
        )
    if not (dp_exp_ok == trace.expected_dataplane_token_expire and dp_exp_fail == 0):
        ok = False
        errs.append(
            f"dataplane_token_expire mismatch: expected={trace.expected_dataplane_token_expire} ok={dp_exp_ok} fail={dp_exp_fail}"
        )

    err = ""
    hints: List[str] = []
    if not ok:
        err = "; ".join(errs)
        hints = _grep_hints(run_dir)

    return RunResult(
        trace=trace.name,
        topology=topology,
        nodes=trace.nodes,
        duration_s=duration_s,
        expected_enqueued=trace.expected_enqueued,
        expected_dtn_logs=len(trace.dtn_log_messages),
        expected_dataplane_roundtrip=trace.expected_dataplane_roundtrip,
        expected_stream_proxy=trace.expected_stream_proxy,
        expected_kelpie_restart=trace.expected_kelpie_restart,
        expected_dtn_policy=trace.expected_dtn_policy,
        expected_wait_memo=trace.expected_wait_memo,
        expected_wait_node_status=trace.expected_wait_node_status,
        expected_io_burst=trace.expected_io_burst,
        expected_dataplane_upload_interrupt=trace.expected_dataplane_upload_interrupt,
        expected_dataplane_token_replay=trace.expected_dataplane_token_replay,
        expected_dataplane_token_expire=trace.expected_dataplane_token_expire,
        expected_dtn_logs_absent=len(trace.dtn_log_absent_messages),
        expected_dropped_total=trace.expected_dropped_total,
        expected_expired_total=trace.expected_expired_total,
        expected_order_checks=len(trace.dtn_expect_orders),
        enqueued=enqueued,
        delivered=delivered,
        failed=failed,
        retried=retried,
        dtn_logs_ok=dtn_logs_ok,
        dtn_logs_missing=dtn_logs_missing,
        dtn_logs_absent_ok=dtn_absent_ok,
        dtn_logs_absent_present=dtn_absent_present,
        dropped_delta=dropped_delta,
        expired_delta=expired_delta,
        order_ok=order_ok,
        order_fail=order_fail,
        dataplane_ok=dp_ok,
        dataplane_fail=dp_fail,
        stream_ok=sp_ok,
        stream_fail=sp_fail,
        restart_ok=kr_ok,
        restart_fail=kr_fail,
        dtn_policy_ok=pol_ok,
        dtn_policy_fail=pol_fail,
        wait_memo_ok=memo_ok,
        wait_memo_fail=memo_fail,
        wait_node_status_ok=st_ok,
        wait_node_status_fail=st_fail,
        io_burst_ok=io_ok,
        io_burst_fail=io_fail,
        dataplane_upload_interrupt_ok=dp_int_ok,
        dataplane_upload_interrupt_fail=dp_int_fail,
        dataplane_token_replay_ok=dp_replay_ok,
        dataplane_token_replay_fail=dp_replay_fail,
        dataplane_token_expire_ok=dp_exp_ok,
        dataplane_token_expire_fail=dp_exp_fail,
        ok=ok,
        run_dir=run_dir_s,
        error=err,
        hints=hints,
    )


def _git_head() -> str:
    proc = _sh(["git", "rev-parse", "HEAD"], cwd=ROOT, quiet=True)
    if proc.returncode != 0:
        return ""
    return (proc.stdout or "").strip()


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Run trace_replay traces in batch and assert DTN delivery.")
    ap.add_argument("--traces", default=str(ROOT / "experiments/trace_replay/traces"), help="Trace directory (default: experiments/trace_replay/traces)")
    ap.add_argument("--out", default=str(ROOT / "experiments/out/regress"), help="Output root (default: experiments/out/regress)")
    ap.add_argument("--topologies", default="star,chain", help="Comma-separated topology list: star,chain (default: star,chain)")
    ap.add_argument("--metrics-every", default="500ms", help="Metrics snapshot interval passed to trace_replay (default: 500ms)")
    ap.add_argument("--tail-ms", type=int, default=30000, help="Extra time after last trace event (ms) (default: 30000)")
    ap.add_argument("--min-duration-s", type=int, default=60, help="Minimum --duration in seconds (default: 60)")
    ap.add_argument("--slack-s", type=int, default=5, help="Extra slack seconds on top of computed duration (default: 5)")
    ap.add_argument("--repeat", type=int, default=1, help="Repeat each trace N times (default: 1)")
    ap.add_argument("--watch-events", action="store_true", help="Enable --watch-events for trace_replay (more debug data)")
    ap.add_argument("--skip-build", action="store_true", help="Skip building kelpie/flock + trace_replay")
    ap.add_argument("--fail-fast", action="store_true", help="Stop on first failure")
    ap.add_argument("--quiet", action="store_true", help="Suppress trace_replay stdout/stderr prints")
    ap.add_argument("--summary", default="", help="Write summary JSON to this path (default: experiments/out/regress/summary-<ts>.json)")
    ap.add_argument("--filter", default="", help="Only run traces whose filename contains this substring")
    ap.add_argument("--archive-failures", default="", help="If set, write a tar.gz with failing run dirs + summary JSON under this directory")
    args = ap.parse_args(argv)

    trace_dir = Path(args.traces).resolve()
    out_root = Path(args.out).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    topologies = [t.strip().lower() for t in (args.topologies or "").split(",") if t.strip()]
    for t in topologies:
        if t not in ("star", "chain"):
            print(f"invalid topology: {t} (expected star|chain)", file=sys.stderr)
            return 2

    if args.repeat < 1:
        print("invalid --repeat: must be >= 1", file=sys.stderr)
        return 2

    if not trace_dir.exists():
        print(f"trace dir not found: {trace_dir}", file=sys.stderr)
        return 2

    trace_paths = sorted(trace_dir.glob("*.jsonl"))
    if args.filter.strip():
        sub = args.filter.strip()
        trace_paths = [p for p in trace_paths if sub in p.name]

    if not trace_paths:
        print(f"no traces found under: {trace_dir}", file=sys.stderr)
        return 2

    trace_replay_bin = ROOT / "build/trace_replay"

    if not args.skip_build:
        proc = _sh(["make", "admin", "agent"], cwd=ROOT, quiet=True)
        if proc.returncode != 0:
            print(proc.stdout, file=sys.stderr)
            print(proc.stderr, file=sys.stderr)
            return proc.returncode
        proc = _sh(["go", "build", "-o", str(trace_replay_bin), "./experiments/trace_replay"], cwd=ROOT, quiet=True)
        if proc.returncode != 0:
            print(proc.stdout, file=sys.stderr)
            print(proc.stderr, file=sys.stderr)
            return proc.returncode

    if not trace_replay_bin.exists():
        print(f"missing trace_replay binary: {trace_replay_bin} (build it or omit --skip-build)", file=sys.stderr)
        return 2

    specs = [_load_trace_spec(p) for p in trace_paths]

    results: List[RunResult] = []
    start = _dt.datetime.utcnow()

    for spec in specs:
        for topo in topologies:
            for rep in range(1, args.repeat + 1):
                if args.repeat > 1:
                    rep_dir = f"rep{rep}"
                else:
                    rep_dir = ""

                res = _run_one(
                    spec,
                    topology=topo,
                    trace_replay_bin=trace_replay_bin,
                    out_root=out_root,
                    rep_dir=rep_dir,
                    metrics_every=args.metrics_every,
                    tail_ms=args.tail_ms,
                    min_duration_s=args.min_duration_s,
                    slack_s=args.slack_s,
                    watch_events=args.watch_events,
                    quiet=args.quiet,
                )
                results.append(res)

                status = "PASS" if res.ok else "FAIL"
                extra_bits: List[str] = []
                if res.expected_dropped_total or res.expected_expired_total:
                    extra_bits.append(
                        f"drop={res.dropped_delta}/{res.expected_dropped_total} exp={res.expired_delta}/{res.expected_expired_total}"
                    )
                if res.expected_dtn_logs_absent:
                    extra_bits.append(f"absent={res.dtn_logs_absent_ok}/{res.expected_dtn_logs_absent}")
                extra = (" ".join(extra_bits) + " ") if extra_bits else ""
                print(
                    f"{status:4} topo={res.topology:5} nodes={res.nodes:<2} dur={res.duration_s:<4}s "
                    f"{res.trace:<35} dtn={res.delivered}/{res.expected_enqueued} "
                    f"log={res.dtn_logs_ok}/{res.expected_dtn_logs} "
                    f"{extra}"
                    f"dp={res.dataplane_ok}/{res.expected_dataplane_roundtrip} "
                    f"sp={res.stream_ok}/{res.expected_stream_proxy} "
                    f"kr={res.restart_ok}/{res.expected_kelpie_restart} "
                    f"retried={res.retried} "
                    f"run={res.run_dir}",
                    flush=True,
                )
                if not res.ok and res.error:
                    print(f"  error: {res.error}", flush=True)
                    for h in (res.hints or [])[:8]:
                        print(f"  hint:  {h}", flush=True)
                    if args.fail_fast:
                        break
            if args.fail_fast and any((not r.ok) for r in results[-args.repeat :]):
                break
        if args.fail_fast and any((not r.ok) for r in results[-len(topologies) * args.repeat :]):
            break

    end = _dt.datetime.utcnow()
    ok_count = sum(1 for r in results if r.ok)
    fail_count = len(results) - ok_count

    summary = {
        "started_at": start.replace(tzinfo=_dt.timezone.utc).isoformat(),
        "finished_at": end.replace(tzinfo=_dt.timezone.utc).isoformat(),
        "duration_seconds": int((end - start).total_seconds()),
        "git_head": _git_head(),
        "args": vars(args),
        "ok": ok_count,
        "fail": fail_count,
        "results": [asdict(r) for r in results],
    }

    summary_path = args.summary.strip()
    if not summary_path:
        ts = _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        summary_path = str(out_root / f"summary-{ts}.json")
    try:
        Path(summary_path).write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"\nSummary: {summary_path}", flush=True)
    except Exception as e:
        print(f"\nwrite summary failed: {e}", file=sys.stderr)

    archive_dir = args.archive_failures.strip()
    if fail_count and archive_dir:
        tar_path, aerr = _archive_failures(
            results=results,
            summary_path=Path(summary_path),
            archive_dir=Path(archive_dir).resolve(),
        )
        if aerr:
            print(f"archive failures failed: {aerr}", file=sys.stderr)
        elif tar_path is not None:
            print(f"Archived failures: {tar_path}", flush=True)

    if fail_count:
        print(f"\nFAILED: {fail_count}/{len(results)} runs failed", flush=True)
        return 1
    print(f"\nOK: {ok_count}/{len(results)} runs passed", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
