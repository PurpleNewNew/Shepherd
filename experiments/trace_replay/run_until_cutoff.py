#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
在本地时间截止点前持续执行一个命令；到达截止时间后优雅中断。

示例：
  python3 experiments/trace_replay/run_until_cutoff.py \
    --cutoff 17:30 -- \
    python3 experiments/trace_replay/run_regress.py --traces ... --repeat 10
"""

from __future__ import annotations

import argparse
import datetime as dt
import signal
import subprocess
import sys
import time
from typing import List


def _parse_cutoff(cutoff: str) -> dt.datetime:
    now = dt.datetime.now()
    try:
        hh, mm = cutoff.strip().split(":")
        hour = int(hh)
        minute = int(mm)
    except Exception as exc:
        raise ValueError(f"invalid --cutoff: {cutoff!r}, expected HH:MM") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"invalid --cutoff: {cutoff!r}, expected HH:MM")
    return now.replace(hour=hour, minute=minute, second=0, microsecond=0)


def _trim_cmd(raw: List[str]) -> List[str]:
    cmd = list(raw)
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    return [c for c in cmd if c.strip()]


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Run command until local cutoff time, then stop gracefully.")
    ap.add_argument("--cutoff", default="17:30", help="Local cutoff time HH:MM (default: 17:30)")
    ap.add_argument(
        "--grace-seconds",
        type=int,
        default=10,
        help="Seconds to wait after SIGINT before TERM/KILL escalation (default: 10)",
    )
    ap.add_argument("cmd", nargs=argparse.REMAINDER, help="Command to run (prefix with --)")
    args = ap.parse_args(argv)

    cmd = _trim_cmd(args.cmd)
    if not cmd:
        print("missing command: provide command after --", file=sys.stderr)
        return 2

    try:
        deadline = _parse_cutoff(args.cutoff)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    now = dt.datetime.now()
    if now >= deadline:
        print(f"cutoff already reached: now={now.isoformat(timespec='seconds')} cutoff={deadline.isoformat(timespec='seconds')}")
        return 124

    print(
        f"[run_until_cutoff] start={now.isoformat(timespec='seconds')} "
        f"cutoff={deadline.isoformat(timespec='seconds')} cmd={' '.join(cmd)}",
        flush=True,
    )

    proc = subprocess.Popen(cmd)

    try:
        while True:
            rc = proc.poll()
            if rc is not None:
                print(f"[run_until_cutoff] command exited rc={rc}", flush=True)
                return rc
            if dt.datetime.now() >= deadline:
                print("[run_until_cutoff] cutoff reached, sending SIGINT ...", flush=True)
                proc.send_signal(signal.SIGINT)
                try:
                    return proc.wait(timeout=max(1, args.grace_seconds))
                except subprocess.TimeoutExpired:
                    print("[run_until_cutoff] SIGINT timeout, sending SIGTERM ...", flush=True)
                    proc.terminate()
                    try:
                        return proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        print("[run_until_cutoff] SIGTERM timeout, sending SIGKILL ...", flush=True)
                        proc.kill()
                        return proc.wait(timeout=5)
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("[run_until_cutoff] interrupted by user, forwarding SIGINT ...", flush=True)
        try:
            proc.send_signal(signal.SIGINT)
        except Exception:
            pass
        try:
            return proc.wait(timeout=max(1, args.grace_seconds))
        except subprocess.TimeoutExpired:
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                return proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                except Exception:
                    pass
                return proc.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
