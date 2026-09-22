#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
IMAGE=${CYNAPSA_DEMO_MAPS_IMAGE:-cynapsa-demo-maps:local}
VOLUME=${CYNAPSA_DEMO_MAPS_VOLUME:-cynapsa-demo-maps-state}

command -v docker >/dev/null || { echo "Docker is required." >&2; exit 69; }
docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 69; }
if [[ ${1:-} == "--build" ]]; then docker image rm "$IMAGE" >/dev/null 2>&1 || true; shift; fi
[[ $# -eq 0 ]] || { echo "usage: ./run.sh [--build]" >&2; exit 64; }
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then "$ROOT/build.sh"; fi

mkdir -p "$ROOT/.private"
chmod 700 "$ROOT/.private"
read_secret() {
  local file=$1 variable=$2 prompt=$3 value
  if [[ ! -s "$file" ]]; then
    value=${!variable:-}
    if [[ -z "$value" ]]; then read -r -s -p "$prompt" value; printf '\n'; fi
    [[ -n "$value" ]] || { echo "Secret cannot be empty." >&2; exit 64; }
    umask 077
    printf '%s' "$value" > "$file"
    unset "$variable"
  fi
  chmod 600 "$file"
}
gemini_file="$ROOT/.private/gemini_api_key"
maps_file="$ROOT/.private/google_maps_api_key"
read_secret "$gemini_file" GEMINI_API_KEY "Gemini API key: "
read_secret "$maps_file" GOOGLE_MAPS_API_KEY "Google Maps API key: "

docker volume create "$VOLUME" >/dev/null
mounts=(
  --mount "type=volume,src=$VOLUME,dst=/var/lib/cynapsa"
  --mount "type=bind,src=$gemini_file,dst=/run/secrets/gemini_api_key,readonly"
  --mount "type=bind,src=$maps_file,dst=/run/secrets/google_maps_api_key,readonly"
)
[[ -z ${DEMO_MESH_ID:-} ]] || mounts+=(--env DEMO_MESH_ID)
[[ -z ${DEMO_GEMINI_MODEL:-} ]] || mounts+=(--env DEMO_GEMINI_MODEL)
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
docker run --rm -it "${mounts[@]}" "$IMAGE"
