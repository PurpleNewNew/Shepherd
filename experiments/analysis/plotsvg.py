#!/usr/bin/env python3
import argparse
import csv
import math
import os
from typing import Any, Dict, List, Tuple


def _read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _mean_std(xs: List[float]) -> Tuple[float, float]:
    if not xs:
        return 0.0, 0.0
    m = sum(xs) / float(len(xs))
    if len(xs) == 1:
        return m, 0.0
    var = sum((x - m) ** 2 for x in xs) / float(len(xs) - 1)
    return m, math.sqrt(var)


def _svg_header(w: int, h: int) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">\n'
        "<style>\n"
        "  .axis { stroke: #111; stroke-width: 1; }\n"
        "  .grid { stroke: #ddd; stroke-width: 1; }\n"
        "  .label { fill: #111; font: 12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }\n"
        "  .title { fill: #111; font: 14px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-weight: 700; }\n"
        "  .legend { fill: #111; font: 12px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }\n"
        "</style>\n"
    )


def _svg_footer() -> str:
    return "</svg>\n"


def _line_chart_bootstrap(rows: List[Dict[str, str]], out_path: str) -> None:
    # Group: topology -> nodes -> [seconds]
    data: Dict[str, Dict[int, List[float]]] = {}
    for r in rows:
        topo = (r.get("topology") or "").strip().lower()
        if topo == "":
            continue
        conv = (r.get("converged_ms") or "").strip()
        if conv == "":
            continue
        try:
            nodes = int((r.get("nodes") or "").strip())
            sec = int(conv) / 1000.0
        except Exception:
            continue
        data.setdefault(topo, {}).setdefault(nodes, []).append(sec)

    series = []
    for topo, by_nodes in sorted(data.items()):
        pts = []
        for n in sorted(by_nodes.keys()):
            m, s = _mean_std(by_nodes[n])
            pts.append((n, m, s))
        if pts:
            series.append((topo, pts))

    # Layout
    W, H = 760, 420
    ml, mr, mt, mb = 70, 20, 40, 60
    pw, ph = W - ml - mr, H - mt - mb

    xs = sorted({n for _, pts in series for (n, _, _) in pts})
    if not xs:
        raise RuntimeError("no data to plot for bootstrap")
    x_pos = {n: ml + int(round((i / max(1, len(xs) - 1)) * pw)) for i, n in enumerate(xs)}

    max_y = 0.0
    for _, pts in series:
        for _, m, s in pts:
            max_y = max(max_y, m + s)
    max_y = max(1.0, max_y * 1.15)

    def ypix(v: float) -> int:
        v = max(0.0, min(max_y, v))
        return mt + int(round(ph - (v / max_y) * ph))

    colors = {
        "star": "#1f77b4",
        "chain": "#ff7f0e",
    }

    parts: List[str] = []
    parts.append(_svg_header(W, H))
    parts.append(f'<text class="title" x="{ml}" y="{mt-18}">拓扑收敛时间（Trace 回放，均值±标准差）</text>\n')

    # Grid + y ticks
    ticks = 5
    for i in range(ticks + 1):
        yv = (max_y / ticks) * i
        y = ypix(yv)
        parts.append(f'<line class="grid" x1="{ml}" y1="{y}" x2="{W-mr}" y2="{y}"/>\n')
        parts.append(f'<text class="label" x="{ml-10}" y="{y+4}" text-anchor="end">{yv:.1f}s</text>\n')

    # Axes
    parts.append(f'<line class="axis" x1="{ml}" y1="{mt}" x2="{ml}" y2="{H-mb}"/>\n')
    parts.append(f'<line class="axis" x1="{ml}" y1="{H-mb}" x2="{W-mr}" y2="{H-mb}"/>\n')

    # X ticks
    for n in xs:
        x = x_pos[n]
        parts.append(f'<line class="grid" x1="{x}" y1="{mt}" x2="{x}" y2="{H-mb}"/>\n')
        parts.append(f'<text class="label" x="{x}" y="{H-mb+22}" text-anchor="middle">{n}</text>\n')
    parts.append(f'<text class="label" x="{ml+pw//2}" y="{H-20}" text-anchor="middle">节点数（包含 root）</text>\n')
    parts.append(f'<text class="label" x="18" y="{mt+ph//2}" transform="rotate(-90 18 {mt+ph//2})" text-anchor="middle">收敛时间（秒）</text>\n')

    # Series lines
    for topo, pts in series:
        col = colors.get(topo, "#333")
        path = []
        for (n, m, _) in pts:
            path.append(f"{x_pos[n]},{ypix(m)}")
        parts.append(f'<polyline fill="none" stroke="{col}" stroke-width="2" points="{" ".join(path)}"/>\n')
        # Markers + error bars
        for (n, m, s) in pts:
            x = x_pos[n]
            y = ypix(m)
            y1 = ypix(max(0.0, m - s))
            y2 = ypix(min(max_y, m + s))
            if s > 0:
                parts.append(f'<line stroke="{col}" stroke-width="1.5" x1="{x}" y1="{y1}" x2="{x}" y2="{y2}"/>\n')
                parts.append(f'<line stroke="{col}" stroke-width="1.5" x1="{x-6}" y1="{y1}" x2="{x+6}" y2="{y1}"/>\n')
                parts.append(f'<line stroke="{col}" stroke-width="1.5" x1="{x-6}" y1="{y2}" x2="{x+6}" y2="{y2}"/>\n')
            parts.append(f'<circle cx="{x}" cy="{y}" r="4" fill="{col}"/>\n')

    # Legend
    lx, ly = W - mr - 160, mt + 10
    parts.append(f'<rect x="{lx}" y="{ly}" width="150" height="54" fill="#fff" stroke="#ccc"/>\n')
    for i, (topo, _) in enumerate(series):
        col = colors.get(topo, "#333")
        y = ly + 18 + i * 18
        parts.append(f'<line x1="{lx+10}" y1="{y-4}" x2="{lx+34}" y2="{y-4}" stroke="{col}" stroke-width="2"/>\n')
        parts.append(f'<circle cx="{lx+22}" cy="{y-4}" r="4" fill="{col}"/>\n')
        parts.append(f'<text class="legend" x="{lx+44}" y="{y}" text-anchor="start">{topo}</text>\n')

    parts.append(_svg_footer())
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("".join(parts))


def _bar_chart_dtn(rows: List[Dict[str, str]], out_path: str) -> None:
    # Aggregate per sleep_seconds across runs: use mean_ms.
    by_sleep: Dict[int, List[float]] = {}
    expected: Dict[int, float] = {}
    for r in rows:
        try:
            sleep_s = int((r.get("sleep_seconds") or "0").strip() or "0")
        except Exception:
            continue
        mean_ms = (r.get("mean_ms") or "").strip()
        if mean_ms == "":
            continue
        try:
            by_sleep.setdefault(sleep_s, []).append(int(mean_ms) / 1000.0)
        except Exception:
            continue
        ew = (r.get("expected_wait_s") or "").strip()
        if ew != "":
            try:
                expected[sleep_s] = float(ew)
            except Exception:
                pass

    xs = sorted(by_sleep.keys())
    if not xs:
        raise RuntimeError("no data to plot for dtn latency")

    stats = []
    max_y = 0.0
    for s in xs:
        m, sd = _mean_std(by_sleep[s])
        max_y = max(max_y, m + sd, expected.get(s, 0.0))
        stats.append((s, m, sd))
    max_y = max(1.0, max_y * 1.2)

    W, H = 760, 420
    ml, mr, mt, mb = 70, 20, 40, 70
    pw, ph = W - ml - mr, H - mt - mb

    def ypix(v: float) -> int:
        v = max(0.0, min(max_y, v))
        return mt + int(round(ph - (v / max_y) * ph))

    # Discrete x positions
    step = pw / float(len(xs))
    centers = {s: ml + int(round((i + 0.5) * step)) for i, s in enumerate(xs)}
    bar_w = int(round(step * 0.55))

    parts: List[str] = []
    parts.append(_svg_header(W, H))
    parts.append(f'<text class="title" x="{ml}" y="{mt-18}">DTN 交付时延 vs Duty-Cycle（链式拓扑，均值±标准差）</text>\n')

    # Grid + y ticks
    ticks = 5
    for i in range(ticks + 1):
        yv = (max_y / ticks) * i
        y = ypix(yv)
        parts.append(f'<line class="grid" x1="{ml}" y1="{y}" x2="{W-mr}" y2="{y}"/>\n')
        parts.append(f'<text class="label" x="{ml-10}" y="{y+4}" text-anchor="end">{yv:.1f}s</text>\n')

    # Axes
    parts.append(f'<line class="axis" x1="{ml}" y1="{mt}" x2="{ml}" y2="{H-mb}"/>\n')
    parts.append(f'<line class="axis" x1="{ml}" y1="{H-mb}" x2="{W-mr}" y2="{H-mb}"/>\n')

    parts.append(f'<text class="label" x="{ml+pw//2}" y="{H-24}" text-anchor="middle">目标节点 sleep_seconds（秒）</text>\n')
    parts.append(f'<text class="label" x="18" y="{mt+ph//2}" transform="rotate(-90 18 {mt+ph//2})" text-anchor="middle">平均交付时延（秒）</text>\n')

    # Bars
    bar_col = "#2ca02c"
    for (s, m, sd) in stats:
        cx = centers[s]
        x0 = cx - bar_w // 2
        y0 = ypix(m)
        yb = ypix(0.0)
        parts.append(f'<rect x="{x0}" y="{y0}" width="{bar_w}" height="{max(0, yb - y0)}" fill="{bar_col}" opacity="0.85"/>\n')
        # error bar
        if sd > 0:
            y1 = ypix(max(0.0, m - sd))
            y2 = ypix(min(max_y, m + sd))
            parts.append(f'<line stroke="#1b5e20" stroke-width="1.5" x1="{cx}" y1="{y1}" x2="{cx}" y2="{y2}"/>\n')
            parts.append(f'<line stroke="#1b5e20" stroke-width="1.5" x1="{cx-6}" y1="{y1}" x2="{cx+6}" y2="{y1}"/>\n')
            parts.append(f'<line stroke="#1b5e20" stroke-width="1.5" x1="{cx-6}" y1="{y2}" x2="{cx+6}" y2="{y2}"/>\n')
        parts.append(f'<text class="label" x="{cx}" y="{H-mb+22}" text-anchor="middle">{s}</text>\n')

    # Expected line
    exp_col = "#d62728"
    exp_pts = []
    for s in xs:
        exp_pts.append((centers[s], ypix(expected.get(s, 0.0))))
    parts.append(
        f'<polyline fill="none" stroke="{exp_col}" stroke-width="2" points="{" ".join(f"{x},{y}" for x,y in exp_pts)}"/>\n'
    )
    for x, y in exp_pts:
        parts.append(f'<circle cx="{x}" cy="{y}" r="4" fill="{exp_col}"/>\n')

    # Legend
    lx, ly = W - mr - 220, mt + 10
    parts.append(f'<rect x="{lx}" y="{ly}" width="210" height="54" fill="#fff" stroke="#ccc"/>\n')
    parts.append(f'<rect x="{lx+10}" y="{ly+14}" width="18" height="10" fill="{bar_col}" opacity="0.85"/>\n')
    parts.append(f'<text class="legend" x="{lx+36}" y="{ly+24}" text-anchor="start">测量均值（±标准差）</text>\n')
    parts.append(f'<line x1="{lx+10}" y1="{ly+40}" x2="{lx+28}" y2="{ly+40}" stroke="{exp_col}" stroke-width="2"/>\n')
    parts.append(f'<circle cx="{lx+19}" cy="{ly+40}" r="4" fill="{exp_col}"/>\n')
    parts.append(f'<text class="legend" x="{lx+36}" y="{ly+44}" text-anchor="start">理论期望 E[T_w]</text>\n')

    parts.append(_svg_footer())
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("".join(parts))


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="Generate simple SVG plots from experiment CSV summaries.")
    ap.add_argument("--bootstrap-csv", required=True, help="bootstrap_summary.csv")
    ap.add_argument("--dtn-summary-csv", required=True, help="dtn_latency_summary.csv")
    ap.add_argument("--out-bootstrap-svg", required=True, help="Output SVG path for bootstrap plot")
    ap.add_argument("--out-dtn-svg", required=True, help="Output SVG path for DTN plot")
    args = ap.parse_args(argv)

    bootstrap_rows = _read_csv(args.bootstrap_csv)
    dtn_rows = _read_csv(args.dtn_summary_csv)

    _line_chart_bootstrap(bootstrap_rows, args.out_bootstrap_svg)
    _bar_chart_dtn(dtn_rows, args.out_dtn_svg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(os.sys.argv[1:]))

