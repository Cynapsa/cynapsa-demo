#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROLE=client

command -v docker >/dev/null || { echo "Docker is required." >&2; exit 69; }
docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 69; }
case "$(docker version --format '{{.Server.Arch}}')" in
  arm64|aarch64) ARCH=arm64 ;;
  amd64|x86_64) ARCH=amd64 ;;
  *) echo "Unsupported Docker architecture." >&2; exit 69 ;;
esac

IMAGE=${CYNAPSA_DEMO_CLIENT_IMAGE:-cynapsa-demo-$ROLE:cli-force-enroll-$ARCH}
ENV_FILE=${CYNAPSA_DEMO_CLIENT_ENV_FILE:-$ROOT/.env}
ACTIVE_VOLUME_FILE=$ROOT/.private/active-volume
force_enroll=false
build_requested=false

for option in "$@"; do
  case "$option" in
    --build) build_requested=true ;;
    --force-enroll) force_enroll=true ;;
    *) echo "usage: ./run.sh [--build] [--force-enroll]" >&2; exit 64 ;;
  esac
done
if [[ -n ${CYNAPSA_DEMO_CLIENT_VOLUME:-} ]]; then
  VOLUME=$CYNAPSA_DEMO_CLIENT_VOLUME
elif [[ -f $ACTIVE_VOLUME_FILE ]]; then
  IFS= read -r VOLUME < "$ACTIVE_VOLUME_FILE"
  [[ $VOLUME =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || { echo "invalid active volume pointer" >&2; exit 78; }
else
  VOLUME=cynapsa-demo-$ROLE-state
fi

if [[ $force_enroll == true ]] &&
   { ! grep -q 'parser.add_argument("--force-enroll"' "$ROOT/vendor/cynapsa-python-sdk/src/cynapsa/_cli.py" ||
     ! grep -q 'force_enroll: bool = False' "$ROOT/vendor/cynapsa-python-sdk/src/cynapsa/session.py" ||
     ! grep -q 'ForceEnroll bool' "$ROOT/vendor/cynapsagocore/api/v1/auth.go"; }; then
  echo "Bundled SDK/Core source lacks force-enroll; wait for the finalized vendor sync." >&2
  exit 78
fi

if [[ $build_requested == true ]] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "Building the self-contained $ROLE image..."
  CYNAPSA_DEMO_CLIENT_IMAGE="$IMAGE" "$ROOT/build.sh"
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

if [[ $force_enroll == true ]]; then
  docker_args+=(--env DEMO_FORCE_ENROLL=1)
  echo "Force-enrolling $ROLE in its selected state volume."
fi
docker run "${docker_args[@]}" "$IMAGE"
