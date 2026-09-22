#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
IMAGE=${CYNAPSA_DEMO_ORCHESTRATOR_IMAGE:-cynapsa-demo-orchestrator:local}
VOLUME=${CYNAPSA_DEMO_ORCHESTRATOR_VOLUME:-cynapsa-demo-orchestrator-state}

command -v docker >/dev/null || { echo "Docker is required." >&2; exit 69; }
docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 69; }
if [[ ${1:-} == "--build" ]]; then docker image rm "$IMAGE" >/dev/null 2>&1 || true; shift; fi
[[ $# -eq 0 ]] || { echo "usage: ./run.sh [--build]" >&2; exit 64; }
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then "$ROOT/build.sh"; fi

mkdir -p "$ROOT/.private"
chmod 700 "$ROOT/.private"
gemini_file="$ROOT/.private/gemini_api_key"
if [[ ! -s "$gemini_file" ]]; then
  key=${GEMINI_API_KEY:-}
  if [[ -z "$key" ]]; then read -r -s -p "Gemini API key: " key; printf '\n'; fi
  [[ -n "$key" ]] || { echo "Gemini API key cannot be empty." >&2; exit 64; }
  umask 077
  printf '%s' "$key" > "$gemini_file"
  unset key GEMINI_API_KEY
fi
chmod 600 "$gemini_file"

docker volume create "$VOLUME" >/dev/null
mounts=(
  --mount "type=volume,src=$VOLUME,dst=/var/lib/cynapsa"
  --mount "type=bind,src=$gemini_file,dst=/run/secrets/gemini_api_key,readonly"
)
environment=()
[[ -z ${DEMO_MESH_ID:-} ]] || environment+=(--env DEMO_MESH_ID)
[[ -z ${DEMO_MAPS_AGENT_ID:-} ]] || environment+=(--env DEMO_MAPS_AGENT_ID)
[[ -z ${DEMO_GEMINI_MODEL:-} ]] || environment+=(--env DEMO_GEMINI_MODEL)
token_file=
cleanup() { [[ -z "$token_file" || ! -f "$token_file" ]] || rm -f -- "$token_file"; }
trap cleanup EXIT INT TERM
if ! docker run --rm --entrypoint sh --mount "type=volume,src=$VOLUME,dst=/state" \
  "$IMAGE" -c 'for p in /state/profile-v2-*.state; do [ -f "$p" ] && exit 0; done; exit 1'
then
  token=${CYNAPSA_TOKEN:-}
  if [[ -z "$token" ]]; then read -r -s -p "Cynapsa enrollment token: " token; printf '\n'; fi
  [[ -n "$token" ]] || { echo "Enrollment token cannot be empty." >&2; exit 64; }
  token_file=$(mktemp "$ROOT/.private/enrollment-token.XXXXXX")
  chmod 600 "$token_file"
  printf '%s' "$token" > "$token_file"
  unset token CYNAPSA_TOKEN
  mounts+=(--mount "type=bind,src=$token_file,dst=/run/secrets/enrollment_token,readonly")
fi
docker run --rm -it "${mounts[@]}" "${environment[@]}" "$IMAGE"
