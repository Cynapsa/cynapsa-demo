#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SDK_ROOT=$(cd -- "$ROOT/../../.." && pwd)
PLATFORM_ENV="$ROOT/.runtime/platform.env"
AGENT_ENV=${CYNAPSA_AGENT_ENV_FILE:-$ROOT/.runtime/agents.env}

usage() {
  cat >&2 <<'EOF'
usage: run-agent.sh ROLE LABEL [application arguments...]

ROLE is one of:
  native-server, fastapi-server, native-sync-client, native-async-client,
  requests-client, httpx-client

LABEL names the portal agent/profile and selects:
  .runtime/agents/LABEL/token
  Docker volume cynapsa-manual-LABEL-state
EOF
  exit 64
}

[[ $# -ge 2 ]] || usage
ROLE=$1
LABEL=$2
shift 2
[[ "$LABEL" =~ ^[A-Za-z0-9_.-]+$ ]] || {
  printf 'invalid agent label: %s\n' "$LABEL" >&2
  exit 64
}
[[ -f "$PLATFORM_ENV" ]] || {
  printf 'start the local platform first\n' >&2
  exit 1
}
[[ -f "$AGENT_ENV" ]] || {
  printf 'copy %s to %s and fill in the portal values\n' \
    "$ROOT/agents.env.example" "$AGENT_ENV" >&2
  exit 1
}

set -a
# shellcheck disable=SC1090
source "$PLATFORM_ENV"
# shellcheck disable=SC1090
source "$AGENT_ENV"
set +a

: "${CYNAPSA_MESH_ID:?set CYNAPSA_MESH_ID in agents.env}"
IMAGE=cynapsa-manual-agent:local
DOCKER_BUILDKIT=1 docker build \
  --build-context "core=$CYNAPSA_GO_CORE_PATH" \
  -f "$ROOT/Dockerfile.agent" \
  -t "$IMAGE" \
  "$SDK_ROOT"

TOKEN="$ROOT/.runtime/agents/$LABEL/token"
mkdir -p "$(dirname -- "$TOKEN")"
if [[ -f "$TOKEN" ]]; then
  [[ ! -L "$TOKEN" && -s "$TOKEN" ]] || {
    printf 'invalid token file: %s\n' "$TOKEN" >&2
    exit 1
  }
  chmod 0600 "$TOKEN"
fi

volume="cynapsa-manual-${LABEL}-state"
docker volume create "$volume" >/dev/null
docker run --rm --user root --network none \
  -v "$volume:/state" alpine:3.23 \
  sh -ec 'chown 10001:10001 /state; chmod 0700 /state'

docker_args=(
  --rm
  --network cynapsa-local-platform
  -e "CYNAPSA_MESH_ID=$CYNAPSA_MESH_ID"
  -e "CYNAPSA_PROFILE_ID=$LABEL"
  -e CYNAPSA_STATE_DIRECTORY=/var/lib/cynapsa
  -e SSL_CERT_FILE=/run/cynapsa/ca.pem
  --mount "type=volume,source=$volume,target=/var/lib/cynapsa,volume-nocopy"
  -v "$CYNAPSA_PLATFORM_STATE/certs/combined-ca.crt:/run/cynapsa/ca.pem:ro"
)
if [[ -t 0 && -t 1 ]]; then
  docker_args+=(-it)
fi
token_args=()
if [[ -f "$TOKEN" ]]; then
  docker_args+=(
    -e CYNAPSA_ENROLLMENT_TOKEN_FILE=/run/cynapsa/token
    -v "$TOKEN:/run/cynapsa/token:ro"
  )
  token_args=(--token-file /run/cynapsa/token)
fi

case "$ROLE" in
  native-server)
    command=(python -m manual_mesh.native_server "$@")
    ;;
  native-sync-client)
    : "${CYNAPSA_NATIVE_SERVER_AGENT_ID:?set CYNAPSA_NATIVE_SERVER_AGENT_ID in agents.env}"
    : "${CYNAPSA_FASTAPI_SERVER_AGENT_ID:?set CYNAPSA_FASTAPI_SERVER_AGENT_ID in agents.env}"
    docker_args+=(
      -e "CYNAPSA_NATIVE_SERVER_AGENT_ID=$CYNAPSA_NATIVE_SERVER_AGENT_ID"
      -e "CYNAPSA_FASTAPI_SERVER_AGENT_ID=$CYNAPSA_FASTAPI_SERVER_AGENT_ID"
    )
    command=(python -m manual_mesh.native_sync_client "$@")
    ;;
  native-async-client)
    : "${CYNAPSA_NATIVE_SERVER_AGENT_ID:?set CYNAPSA_NATIVE_SERVER_AGENT_ID in agents.env}"
    : "${CYNAPSA_FASTAPI_SERVER_AGENT_ID:?set CYNAPSA_FASTAPI_SERVER_AGENT_ID in agents.env}"
    docker_args+=(
      -e "CYNAPSA_NATIVE_SERVER_AGENT_ID=$CYNAPSA_NATIVE_SERVER_AGENT_ID"
      -e "CYNAPSA_FASTAPI_SERVER_AGENT_ID=$CYNAPSA_FASTAPI_SERVER_AGENT_ID"
    )
    command=(python -m manual_mesh.native_async_client "$@")
    ;;
  fastapi-server)
    command=(
      cynapsa run "${token_args[@]}"
      --mesh-id "$CYNAPSA_MESH_ID"
      --profile-id "$LABEL"
      --allow '*' '*'
      -- uvicorn manual_mesh.fastapi_server:app --host 0.0.0.0 --port 8000
    )
    ;;
  requests-client|httpx-client)
    : "${CYNAPSA_NATIVE_SERVER_AGENT_ID:?set CYNAPSA_NATIVE_SERVER_AGENT_ID in agents.env}"
    : "${CYNAPSA_FASTAPI_SERVER_AGENT_ID:?set CYNAPSA_FASTAPI_SERVER_AGENT_ID in agents.env}"
    module=manual_mesh.requests_client
    [[ "$ROLE" == httpx-client ]] && module=manual_mesh.httpx_client
    command=(
      cynapsa run "${token_args[@]}"
      --mesh-id "$CYNAPSA_MESH_ID"
      --profile-id "$LABEL"
      --map https://native-server.local "$CYNAPSA_NATIVE_SERVER_AGENT_ID" rpc
      --map https://fastapi-server.local "$CYNAPSA_FASTAPI_SERVER_AGENT_ID" rpc
      --allow '*' '*'
      -- python -m "$module" "$@"
    )
    ;;
  *) usage ;;
esac

exec docker run "${docker_args[@]}" "$IMAGE" "${command[@]}"
