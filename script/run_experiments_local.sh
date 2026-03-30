#!/usr/bin/env bash
set -euo pipefail

# Local, reproducible experiment runner for the midterm/experiment report.
# Outputs raw runs under experiments/out/ (gitignored), and writes aggregated
# CSV + SVG artifacts under docs/ for versioning.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"

BOOT_ROOT="experiments/out/paper_bootstrap_${RUN_TS}"
DTN_ROOT="experiments/out/paper_dtn_${RUN_TS}"

echo "[1/4] build kelpie/flock + trace_replay"
make admin agent >/dev/null
go build -o build/trace_replay ./experiments/trace_replay

echo "[2/4] bootstrap experiment -> ${BOOT_ROOT}"
for topo in star chain; do
  for n in 4 6 8; do
    for rep in 1 2 3; do
      echo "  - topo=${topo} nodes=${n} rep=${rep}"
      ./build/trace_replay \
        --out "${BOOT_ROOT}/${topo}/n${n}/rep${rep}" \
        --topology "${topo}" \
        --nodes "${n}" \
        --duration 20s \
        --metrics-every 500ms >/dev/null
    done
  done
done

echo "[3/4] DTN latency experiment -> ${DTN_ROOT}"
for trace in \
  experiments/trace_replay/traces/dtn_sleep_effect_baseline.jsonl \
  experiments/trace_replay/traces/dtn_sleep_effect_sleep8_work2.jsonl \
  experiments/trace_replay/traces/dtn_sleep_effect_sleep16_work2.jsonl; do
  name="$(basename "${trace}" .jsonl)"
  for rep in 1 2; do
    echo "  - trace=${name} rep=${rep}"
    ./build/trace_replay \
      --out "${DTN_ROOT}/${name}/rep${rep}" \
      --topology chain \
      --nodes 4 \
      --duration 70s \
      --metrics-every 500ms \
      --trace "${trace}" >/dev/null
  done
done

echo "[4/4] aggregate -> docs/data + docs/figures"
mkdir -p docs/data docs/figures
python3 experiments/analysis/analyze_bootstrap.py --root "${BOOT_ROOT}" --out docs/data/bootstrap_summary.csv
python3 experiments/analysis/analyze_dtn_latency.py \
  --root "${DTN_ROOT}" \
  --target n3 \
  --out-samples docs/data/dtn_latency_samples.csv \
  --out-summary docs/data/dtn_latency_summary.csv
python3 experiments/analysis/plot_svg.py \
  --bootstrap-csv docs/data/bootstrap_summary.csv \
  --dtn-summary-csv docs/data/dtn_latency_summary.csv \
  --out-bootstrap-svg docs/figures/bootstrap_convergence.svg \
  --out-dtn-svg docs/figures/dtn_latency.svg

echo ""
echo "Done."
echo "Raw runs:"
echo "  ${BOOT_ROOT}"
echo "  ${DTN_ROOT}"
echo "Artifacts:"
echo "  docs/data/bootstrap_summary.csv"
echo "  docs/data/dtn_latency_samples.csv"
echo "  docs/data/dtn_latency_summary.csv"
echo "  docs/figures/bootstrap_convergence.svg"
echo "  docs/figures/dtn_latency.svg"
