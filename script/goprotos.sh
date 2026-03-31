#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export PATH="$HOME/go/bin:$PATH"

PROTOC="$(command -v protoc || true)"
PROTOC_GEN_GO="$(command -v protoc-gen-go || true)"
PROTOC_GEN_GO_GRPC="$(command -v protoc-gen-go-grpc || true)"

if [[ -z "$PROTOC" ]]; then
  echo "error: protoc not found in PATH; please install protobuf (Homebrew: brew install protobuf)" >&2
  exit 1
fi
if [[ -z "$PROTOC_GEN_GO" ]]; then
  echo "error: protoc-gen-go not found in PATH; please install google.golang.org/protobuf/cmd/protoc-gen-go" >&2
  exit 1
fi
if [[ -z "$PROTOC_GEN_GO_GRPC" ]]; then
  echo "error: protoc-gen-go-grpc not found in PATH; please install google.golang.org/grpc/cmd/protoc-gen-go-grpc" >&2
  exit 1
fi

KELPIEUI_PROTO=proto/kelpieui/v1/kelpieui.proto
DATAPLANE_PROTO=proto/dataplane/v1/dataplane.proto

echo "[gen] Go kelpieui protos -> internal/kelpie/uipb"
"$PROTOC" \
  -I proto \
  --go_out=internal/kelpie/uipb \
  --go_opt=module=codeberg.org/agnoie/shepherd/internal/kelpie/uipb \
  --go-grpc_out=internal/kelpie/uipb \
  --go-grpc_opt=module=codeberg.org/agnoie/shepherd/internal/kelpie/uipb \
  "$KELPIEUI_PROTO"

echo "[gen] Go dataplane protos -> internal/dataplanepb"
"$PROTOC" \
  -I . \
  --go_out=. \
  --go_opt=module=codeberg.org/agnoie/shepherd \
  --go-grpc_out=. \
  --go-grpc_opt=module=codeberg.org/agnoie/shepherd \
  "$DATAPLANE_PROTO"

echo "[gen] Done."
