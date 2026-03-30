#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Soak 压测脚本：对“高风险 trace”做多次重复回放，用于主动捕获 flake/竞态类问题。

设计目标：
- 默认只跑核心场景（拓扑自愈 + DTN + 睡眠协同相关的高风险 trace）
- 产出独立的 run 根目录（带时间戳），避免污染常规 regress 输出
- 一旦出现 FAIL，自动将失败 run 目录 + summary 打包留档（tar.gz）

典型用法：
  make trace_replay
  python3 experiments/trace_replay/run_soak.py --repeat 10 --topologies star,chain --profile core --skip-build

更“狠”的过夜跑法（建议 fail-fast）：
  SOAK_REPEAT=50 make soak
"""

from __future__ import annotations

import argparse
import datetime as _dt
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List


ROOT = Path(__file__).resolve().parents[2]
TRACE_DIR = ROOT / "experiments/trace_replay/traces"
RUN_REGRESS = ROOT / "experiments/trace_replay/run_regress.py"


SOAK_PROFILES = {
    # 核心：围绕“中间节点睡眠 + 父节点 kill + 补链/救援并发 + DTN 重试窗口”。
    "core": [
        "dtn_sleep_effect_sleep8_work2",
        "dtn_mid_sleep_kill_parent_n1",
        "dtn_deepchain_leaf_sleep_kill_parent_n3",
        "dtn_burst_leaf_sleep_kill_parent_n2",
    ],
    # 扩展：把更轻量但覆盖面更广的 trace 也纳入 soak。
    "extended": [
        "dtn_sleep_effect_baseline",
        "dtn_sleep_effect_sleep8_work2",
        "dtn_sleep_effect_sleep16_work2",
        "dtn_midchain_sleep_parent_n2",
        "dtn_leaf_sleep_kill_parent_n2",
        "dtn_mid_sleep_kill_parent_n1",
        "dtn_deepchain_leaf_sleep_kill_parent_n3",
        "dtn_burst_leaf_sleep_kill_parent_n2",
        "dtn_latency_baseline",
        "dtn_latency_sleep8_work2",
        "dtn_latency_sleep16_work2",
    ],
    # IO/恢复：覆盖 stream 引擎、dataplane 数据通路，以及 Kelpie 中途重启后的恢复能力。
    "io": [
        "stream_proxy_roundtrip_n3",
        "dataplane_roundtrip_n3",
        "dataplane_token_replay_n3",
        "dataplane_token_expire_n3",
        "kelpie_restart_recovery_n3",
        "io_burst_after_kelpie_restart_n3",
    ],
    # 深挖：三条“最容易出隐性 bug 且可本机稳定复现”的线：
    # - DTN 队列语义（capacity drop / TTL expire）
    # - stream/dataplane 并发 + 中断
    # - Gossip 规模/分区（用 memo 收敛做可观测断言）
    "deep": [
        "dtn_queue_ttl_expire_n3",
        "dtn_queue_capacity_drop_n3",
        "io_burst_interrupt_target_n3",
        "kelpie_restart_recovery_n3",
        "dataplane_token_replay_n3",
        "dataplane_token_expire_n3",
        "gossip_memo_scale_n8",
        "gossip_memo_heal_kill_parent_n3",
    ],
    # 深挖加强版：在 deep 的基础上加入更深子树断链自愈（kill n2）与重启后 IO burst。
    "deep_plus": [
        "dtn_queue_ttl_expire_n3",
        "dtn_queue_capacity_drop_n3",
        "io_burst_interrupt_target_n3",
        "kelpie_restart_recovery_n3",
        "io_burst_after_kelpie_restart_n3",
        "dataplane_token_replay_n3",
        "dataplane_token_expire_n3",
        "gossip_memo_scale_n8",
        "gossip_memo_heal_kill_parent_n3",
        "gossip_memo_heal_kill_parent_n2",
    ],
    # 深挖（更狠）：在 deep_plus 的基础上再提高覆盖密度，适合过夜跑。
    "deep_hard": [
        "dtn_queue_ttl_expire_n3",
        "dtn_queue_capacity_drop_n3",
        "dtn_priority_order_n3",
        "dtn_backpressure_fairness_n3",
        "dtn_hold_ttl_interaction_n3",
        "io_burst_interrupt_target_n3",
        "io_burst_heal_kill_parent_n3",
        "dtn_persist_kelpie_restart_n3",
        "kelpie_restart_recovery_n3",
        "io_burst_after_kelpie_restart_n3",
        "dataplane_token_replay_n3",
        "dataplane_token_expire_n3",
        "gossip_memo_scale_n8",
        "gossip_memo_partition_kill_mid_n4_n8",
        "gossip_memo_heal_kill_parent_n3",
        "gossip_memo_heal_kill_parent_n2",
    ],
    # 过夜：在 deep_hard 基础上加入更重负载的数据面（大文件/高并发/中断/重启恢复）。
    "overnight": [
        "dtn_mid_sleep_kill_parent_n1",
        "dtn_deepchain_leaf_sleep_kill_parent_n3",
        "dtn_burst_leaf_sleep_kill_parent_n2",
        "dtn_queue_ttl_expire_n3",
        "dtn_queue_capacity_drop_n3",
        "dtn_priority_order_n3",
        "dtn_backpressure_fairness_n3",
        "dtn_hold_ttl_interaction_n3",
        "dtn_persist_kelpie_restart_n3",
        "kelpie_restart_recovery_n3",
        "io_burst_after_kelpie_restart_n3",
        "io_burst_heavy_after_kelpie_restart_n3",
        "io_burst_interrupt_target_n3",
        "io_burst_heavy_interrupt_target_n3",
        "io_burst_heal_kill_parent_n3",
        "stream_dataplane_long_soak_n3",
        "dataplane_big_roundtrip_n3",
        "dataplane_restart_interrupt_mix_n3",
        "dataplane_token_replay_n3",
        "dataplane_token_expire_n3",
        "gossip_memo_scale_n8",
        "gossip_memo_partition_kill_mid_n4_n8",
        "gossip_memo_heal_kill_parent_n3",
        "gossip_partition_churn_n12",
        "gossip_memo_scale_n16",
        "topology_flap_kelpie_restart_n10",
    ],
    # 过夜全量：在 overnight 上叠加更大规模拓扑/分区场景，用于“打满未覆盖点”。
    "overnight_full": [
        "dtn_mid_sleep_kill_parent_n1",
        "dtn_deepchain_leaf_sleep_kill_parent_n3",
        "dtn_burst_leaf_sleep_kill_parent_n2",
        "dtn_queue_ttl_expire_n3",
        "dtn_queue_capacity_drop_n3",
        "dtn_priority_order_n3",
        "dtn_backpressure_fairness_n3",
        "dtn_hold_ttl_interaction_n3",
        "dtn_persist_kelpie_restart_n3",
        "kelpie_restart_recovery_n3",
        "io_burst_after_kelpie_restart_n3",
        "io_burst_heavy_after_kelpie_restart_n3",
        "io_burst_interrupt_target_n3",
        "io_burst_heavy_interrupt_target_n3",
        "io_burst_heal_kill_parent_n3",
        "stream_dataplane_long_soak_n3",
        "dataplane_big_roundtrip_n3",
        "dataplane_restart_interrupt_mix_n3",
        "dataplane_token_replay_n3",
        "dataplane_token_expire_n3",
        "gossip_memo_scale_n8",
        "gossip_memo_partition_kill_mid_n4_n8",
        "gossip_memo_heal_kill_parent_n3",
        "gossip_partition_churn_n12",
        "gossip_memo_scale_n16",
        "topology_flap_kelpie_restart_n10",
        "topology_flap_kelpie_restart_n14",
    ],
}


def _utc_ts() -> str:
    return _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def _sh(cmd: List[str], *, quiet: bool = False) -> int:
    if not quiet:
        print("+", " ".join(cmd), file=sys.stderr)
    proc = subprocess.run(cmd, cwd=str(ROOT))
    return proc.returncode


def _copy_selected_traces(dst: Path, stems: List[str]) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    missing: List[str] = []
    for stem in stems:
        src = TRACE_DIR / f"{stem}.jsonl"
        if not src.exists():
            missing.append(str(src))
            continue
        shutil.copy2(src, dst / src.name)
    if missing:
        raise SystemExit("missing trace(s):\n  - " + "\n  - ".join(missing))


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Run high-risk trace_replay traces repeatedly (soak) and archive failures.")
    ap.add_argument("--profile", default="core", choices=sorted(SOAK_PROFILES.keys()) + ["all"], help="Soak profile (default: core)")
    ap.add_argument("--repeat", type=int, default=10, help="Repeat each trace N times (default: 10)")
    ap.add_argument("--topologies", default="star,chain", help="Comma-separated topology list: star,chain (default: star,chain)")
    ap.add_argument("--out-root", default=str(ROOT / "experiments/out/soak"), help="Output root (default: experiments/out/soak)")
    ap.add_argument("--run-id", default="", help="Optional fixed run id (default: soak-<utc-ts>)")

    # Pass-through knobs to run_regress.py
    ap.add_argument("--metrics-every", default="500ms", help="Metrics snapshot interval (default: 500ms)")
    ap.add_argument("--tail-ms", type=int, default=30000, help="Extra time after last trace event (ms) (default: 30000)")
    ap.add_argument("--min-duration-s", type=int, default=60, help="Minimum --duration in seconds (default: 60)")
    ap.add_argument("--slack-s", type=int, default=5, help="Extra slack seconds on top of computed duration (default: 5)")
    ap.add_argument("--watch-events", action="store_true", help="Enable --watch-events (more debug data)")
    ap.add_argument("--skip-build", action="store_true", help="Skip building kelpie/flock + trace_replay")
    ap.add_argument("--fail-fast", action="store_true", help="Stop on first failure")
    ap.add_argument("--quiet", action="store_true", help="Suppress trace_replay stdout/stderr prints")
    ap.add_argument("--no-archive", action="store_true", help="Do not archive failures")
    args = ap.parse_args(argv)

    if args.repeat < 1:
        print("invalid --repeat: must be >= 1", file=sys.stderr)
        return 2

    out_root = Path(args.out_root).resolve()
    run_id = args.run_id.strip() or f"soak-{_utc_ts()}"
    run_root = out_root / run_id
    runs_out = run_root / "runs"
    summary_path = run_root / "summary.json"
    archive_dir = run_root / "archives"

    run_root.mkdir(parents=True, exist_ok=True)

    trace_dir = TRACE_DIR
    if args.profile != "all":
        selected = run_root / "selected_traces"
        _copy_selected_traces(selected, SOAK_PROFILES[args.profile])
        trace_dir = selected

    cmd = [
        sys.executable,
        str(RUN_REGRESS),
        "--traces",
        str(trace_dir),
        "--out",
        str(runs_out),
        "--summary",
        str(summary_path),
        "--topologies",
        str(args.topologies),
        "--repeat",
        str(args.repeat),
        "--metrics-every",
        str(args.metrics_every),
        "--tail-ms",
        str(args.tail_ms),
        "--min-duration-s",
        str(args.min_duration_s),
        "--slack-s",
        str(args.slack_s),
    ]
    if args.watch_events:
        cmd.append("--watch-events")
    if args.skip_build:
        cmd.append("--skip-build")
    if args.fail_fast:
        cmd.append("--fail-fast")
    if args.quiet:
        cmd.append("--quiet")
    if not args.no_archive:
        cmd.extend(["--archive-failures", str(archive_dir)])

    rc = _sh(cmd, quiet=False)
    print(f"\nSoak root: {run_root}", flush=True)
    if not args.no_archive:
        print(f"Archives:  {archive_dir}", flush=True)
    return rc


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
