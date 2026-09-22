#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
VERSION=0.1.0
command -v docker >/dev/null || { echo "Docker is required." >&2; exit 69; }
docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 69; }
case "$(docker version --format '{{.Server.Arch}}')" in
  arm64|aarch64)
    ARCH=arm64
    EXPECTED_SHA256=fe0b6eebd6a03f6400a45d5509d97f92a41720521a71e53e6939880da574fea0
    ;;
  amd64|x86_64)
    ARCH=amd64
    EXPECTED_SHA256=26d5fb0dce55519851f6969e576aa0d831b0cfd034f9ca76b2290ef4330d1b61
    ;;
  *) echo "Unsupported Docker architecture." >&2; exit 69 ;;
esac
IMAGE=${CYNAPSA_DEMO_CLIENT_IMAGE:-cynapsa-demo-client:$VERSION-$ARCH}
VOLUME=${CYNAPSA_DEMO_CLIENT_VOLUME:-cynapsa-demo-client-state}
ASSET_URL="https://github.com/Cynapsa/cynapsa-demo/releases/download/v$VERSION/cynapsa-demo-client-linux-$ARCH.tar.gz"

if [[ ${1:-} == "--pull" ]]; then
  docker image rm "$IMAGE" >/dev/null 2>&1 || true
  shift
elif [[ ${1:-} == "--build" ]]; then
  CYNAPSA_DEMO_CLIENT_IMAGE="$IMAGE" "$ROOT/build.sh"
  shift
fi
if [[ $# -ne 0 ]]; then
  echo "usage: ./run.sh [--pull|--build]" >&2
  exit 64
fi

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  command -v curl >/dev/null || { echo "curl is required to download the client image." >&2; exit 69; }
  image_archive=$(mktemp "${TMPDIR:-/tmp}/cynapsa-demo-client.XXXXXX.tar.gz")
  trap 'rm -f -- "$image_archive"' EXIT INT TERM
  echo "Downloading Cynapsa demo client $VERSION for $ARCH..."
  curl --fail --location --retry 3 --output "$image_archive" "$ASSET_URL"
  if command -v sha256sum >/dev/null; then
    actual_sha256=$(sha256sum "$image_archive" | awk '{print $1}')
  else
    actual_sha256=$(shasum -a 256 "$image_archive" | awk '{print $1}')
  fi
  if [[ "$actual_sha256" != "$EXPECTED_SHA256" ]]; then
    echo "Client image checksum verification failed." >&2
    exit 65
  fi
  gzip -dc "$image_archive" | docker load >/dev/null
  rm -f -- "$image_archive"
  trap - EXIT INT TERM
  docker image inspect "$IMAGE" >/dev/null 2>&1 || {
    echo "Downloaded archive did not contain $IMAGE." >&2
    exit 70
  }
fi

docker volume create "$VOLUME" >/dev/null
mounts=(--mount "type=volume,src=$VOLUME,dst=/var/lib/cynapsa")
[[ -z ${DEMO_MESH_ID:-} ]] || mounts+=(--env DEMO_MESH_ID)
[[ -z ${DEMO_ORCHESTRATOR_AGENT_ID:-} ]] || mounts+=(--env DEMO_ORCHESTRATOR_AGENT_ID)
token_file=
cleanup() {
  if [[ -n "$token_file" && -f "$token_file" ]]; then
    rm -f -- "$token_file"
  fi
}
trap cleanup EXIT INT TERM

if ! docker run --rm --entrypoint sh \
  --mount "type=volume,src=$VOLUME,dst=/state" \
  "$IMAGE" -c 'for p in /state/profile-v2-*.state; do [ -f "$p" ] && exit 0; done; exit 1'
then
  token=${CYNAPSA_TOKEN:-}
  if [[ -z "$token" ]]; then
    read -r -s -p "Cynapsa enrollment token: " token
    printf '\n'
  fi
  [[ -n "$token" ]] || { echo "Enrollment token cannot be empty." >&2; exit 64; }
  mkdir -p "$ROOT/.private"
  chmod 700 "$ROOT/.private"
  token_file=$(mktemp "$ROOT/.private/enrollment-token.XXXXXX")
  chmod 600 "$token_file"
  printf '%s' "$token" > "$token_file"
  unset token CYNAPSA_TOKEN
  mounts+=(--mount "type=bind,src=$token_file,dst=/run/secrets/enrollment_token,readonly")
fi

docker run --rm -it "${mounts[@]}" "$IMAGE"
