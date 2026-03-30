#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/2] go test -race ./..."
go test -race ./...

echo "[2/2] make regress"
make regress

echo ""
echo "OK"

