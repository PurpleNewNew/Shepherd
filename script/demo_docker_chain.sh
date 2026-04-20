#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker/docker-compose.yml"
PROJECT_NAME="${SHEPHERD_DEMO_PROJECT:-shepherd-demo}"

export SHEPHERD_SECRET="${SHEPHERD_SECRET:-Shepherd1234}"
export SHEPHERD_UI_TOKEN="${SHEPHERD_UI_TOKEN:-stockman-dev-token}"
export SHEPHERD_UI_PORT="${SHEPHERD_UI_PORT:-50061}"
export SHEPHERD_DATAPLANE_PORT="${SHEPHERD_DATAPLANE_PORT:-60080}"
export FLOCK_RECONNECT_SECONDS="${FLOCK_RECONNECT_SECONDS:-2}"

compose() {
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" "$@"
}

usage() {
  cat <<EOF
Usage: $(basename "$0") <up|down|ps|logs> [service]

Commands:
  up     Recreate the demo stack, bootstrap the chain, and print Stockman connection info.
  down   Stop the demo stack and remove its volume.
  ps     Show demo container status.
  logs   Show logs for the whole stack or for one service.

Environment:
  SHEPHERD_SECRET          Shared secret for Kelpie/Flock.
  SHEPHERD_UI_TOKEN        Stockman token for Kelpie gRPC UI.
  SHEPHERD_UI_PORT         Host port for Stockman -> Kelpie gRPC.
  SHEPHERD_DATAPLANE_PORT  Host port for dataplane.
  FLOCK_RECONNECT_SECONDS  Active reconnect interval for demo agents.
  SHEPHERD_DEMO_PROJECT    Docker Compose project name.
EOF
}

print_connection_info() {
  cat <<EOF

Demo environment is ready.

Stockman connection:
  Host: 127.0.0.1
  Port: ${SHEPHERD_UI_PORT}
  Token: ${SHEPHERD_UI_TOKEN}
  Use TLS: off

Suggested demo actions:
  1. Open Stockman and connect to 127.0.0.1:${SHEPHERD_UI_PORT}.
  2. Confirm three online Flock nodes in the topology.
  3. Simulate middle-node failure:
       docker compose -p ${PROJECT_NAME} -f ${COMPOSE_FILE} stop flock-mid
  4. Watch the leaf go offline and then recover through self-heal/rescue.

Useful commands:
  docker compose -p ${PROJECT_NAME} -f ${COMPOSE_FILE} ps
  docker compose -p ${PROJECT_NAME} -f ${COMPOSE_FILE} logs -f kelpie
  docker compose -p ${PROJECT_NAME} -f ${COMPOSE_FILE} logs -f flock-mid
EOF
}

cmd="${1:-up}"
case "${cmd}" in
  up)
    compose down -v --remove-orphans >/dev/null 2>&1 || true
    compose up -d --build kelpie flock-root flock-mid flock-leaf
    compose run --rm bootstrap-chain
    compose ps
    print_connection_info
    ;;
  down)
    compose down -v --remove-orphans
    ;;
  ps)
    compose ps
    ;;
  logs)
    shift || true
    if [[ $# -gt 0 ]]; then
      compose logs -f "$1"
    else
      compose logs -f
    fi
    ;;
  *)
    usage
    exit 1
    ;;
esac
