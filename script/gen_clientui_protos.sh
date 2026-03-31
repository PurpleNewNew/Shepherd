#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PROTOC="$(command -v protoc || true)"
GRPC_CPP_PLUGIN="$(command -v grpc_cpp_plugin || true)"

if [[ -z "$PROTOC" ]]; then
  echo "error: protoc not found in PATH; please install protobuf (Homebrew: brew install protobuf)" >&2
  exit 1
fi
if [[ -z "$GRPC_CPP_PLUGIN" ]]; then
  echo "error: grpc_cpp_plugin not found in PATH; please install grpc (Homebrew: brew install grpc)" >&2
  exit 1
fi

KELPIEUI_PROTO=proto/kelpieui/v1/kelpieui.proto
DATAPLANE_PROTO=proto/dataplane/v1/dataplane.proto

echo "[gen] C++ kelpieui protos -> clientui/generated/proto"
"$PROTOC" \
  -I proto \
  --cpp_out=clientui/generated/proto \
  --grpc_out=clientui/generated/proto \
  --plugin=protoc-gen-grpc="$GRPC_CPP_PLUGIN" \
  "$KELPIEUI_PROTO"

echo "[gen] C++ dataplane protos -> clientui/generated"
"$PROTOC" \
  -I proto \
  --cpp_out=clientui/generated \
  --grpc_out=clientui/generated \
  --plugin=protoc-gen-grpc="$GRPC_CPP_PLUGIN" \
  "$DATAPLANE_PROTO"

echo "[gen] Done."
