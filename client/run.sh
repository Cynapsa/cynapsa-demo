#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
VERSION=0.2.0
ROLE=client

command -v docker >/dev/null || { echo "Docker is required." >&2; exit 69; }
docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 69; }
case "$(docker version --format '{{.Server.Arch}}')" in
  arm64|aarch64)
    ARCH=arm64
    EXPECTED_SHA256=1ad022aa7e69dfb0df59f4ca0846daa9da8bdad393b7b8f059ce23ef984e2260
    ;;
  amd64|x86_64)
    ARCH=amd64
    EXPECTED_SHA256=d5091abfffa9eb40c31842195784451b53a50009d062fe78d9f342dd929cc55d
    ;;
  *) echo "Unsupported Docker architecture." >&2; exit 69 ;;
esac

IMAGE=${CYNAPSA_DEMO_CLIENT_IMAGE:-cynapsa-demo-$ROLE:$VERSION-$ARCH}
VOLUME=${CYNAPSA_DEMO_CLIENT_VOLUME:-cynapsa-demo-$ROLE-state}
ENV_FILE=${CYNAPSA_DEMO_CLIENT_ENV_FILE:-$ROOT/.env}
ASSET_URL="https://github.com/Cynapsa/cynapsa-demo/releases/download/v$VERSION/cynapsa-demo-$ROLE-linux-$ARCH.tar.gz"

if [[ ${1:-} == "--pull" ]]; then
  docker image rm "$IMAGE" >/dev/null 2>&1 || true
  shift
elif [[ ${1:-} == "--build" ]]; then
  CYNAPSA_DEMO_CLIENT_IMAGE="$IMAGE" "$ROOT/build.sh"
  shift
fi
[[ $# -eq 0 ]] || { echo "usage: ./run.sh [--pull|--build]" >&2; exit 64; }

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  command -v curl >/dev/null || { echo "curl is required to download the client image." >&2; exit 69; }
  image_archive=$(mktemp "${TMPDIR:-/tmp}/cynapsa-demo-$ROLE.XXXXXX.tar.gz")
  trap 'rm -f -- "$image_archive"' EXIT INT TERM
  echo "Downloading Cynapsa demo $ROLE $VERSION for $ARCH..."
  curl --fail --location --retry 3 --output "$image_archive" "$ASSET_URL"
  if command -v sha256sum >/dev/null; then
    actual_sha256=$(sha256sum "$image_archive" | awk '{print $1}')
  else
    actual_sha256=$(shasum -a 256 "$image_archive" | awk '{print $1}')
  fi
  [[ "$actual_sha256" == "$EXPECTED_SHA256" ]] || {
    echo "Client image checksum verification failed." >&2
    exit 65
  }
  gzip -dc "$image_archive" | docker load >/dev/null
  rm -f -- "$image_archive"
  trap - EXIT INT TERM
  docker image inspect "$IMAGE" >/dev/null 2>&1 || {
    echo "Downloaded archive did not contain $IMAGE." >&2
    exit 70
  }
fi

docker volume create "$VOLUME" >/dev/null
docker_args=(
  --rm
  -it
  --mount "type=volume,src=$VOLUME,dst=/var/lib/cynapsa"
)
[[ ! -f "$ENV_FILE" ]] || docker_args+=(--env-file "$ENV_FILE")
for variable in CYNAPSA_TOKEN DEMO_MESH_ID DEMO_ORCHESTRATOR_AGENT_ID; do
  [[ -z ${!variable:-} ]] || docker_args+=(--env "$variable")
done

docker run "${docker_args[@]}" "$IMAGE"
