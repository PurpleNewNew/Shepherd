#!/usr/bin/env bash
set -euo pipefail

# Prefer toolchain from PATH to keep local/CI environments portable.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PROTOC="$(command -v protoc || true)"
GRPC_CPP_PLUGIN="$(command -v grpc_cpp_plugin || true)"

if [[ -z "$PROTOC" ]]; then
  echo "error: protoc not found in PATH; please install protobuf-compiler (3.21.x recommended)" >&2
  exit 1
fi
if [[ -z "$GRPC_CPP_PLUGIN" ]]; then
  echo "error: grpc_cpp_plugin not found in PATH" >&2
  exit 1
fi

# Ensure Go plugins are on PATH (expected in \$HOME/go/bin).
export PATH="$HOME/go/bin:$PATH"

PROTO_FILE=proto/kelpieui/v1/kelpieui.proto

echo "[gen] Go kelpieui protos -> internal/kelpie/uipb"
"$PROTOC" \
  -I proto \
  --go_out=internal/kelpie/uipb \
  --go_opt=module=codeberg.org/agnoie/shepherd/internal/kelpie/uipb \
  --go-grpc_out=internal/kelpie/uipb \
  --go-grpc_opt=module=codeberg.org/agnoie/shepherd/internal/kelpie/uipb \
  "$PROTO_FILE"

echo "[gen] C++ kelpieui protos -> clientui/generated/proto"
"$PROTOC" \
  -I proto \
  --cpp_out=clientui/generated/proto \
  --grpc_out=clientui/generated/proto \
  --plugin=protoc-gen-grpc="$GRPC_CPP_PLUGIN" \
  "$PROTO_FILE"

echo "[gen] Done."
