#!/usr/bin/env bash
set -Eeuo pipefail

# Switch modes by moving the # from one line to the other. Keep one active.
# The Cynapsa config is generated from .env inside the container at startup.
# DEMO_RUN_COMMAND='python /app/app.py'
DEMO_RUN_COMMAND='cynapsa run --config /tmp/cynapsa-private/cynapsa.json -- python /app/app.py'
export DEMO_RUN_COMMAND

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROLE=client

command -v docker >/dev/null || { echo "Docker is required." >&2; exit 69; }
docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 69; }
case "$(docker version --format '{{.Server.Arch}}')" in
  arm64|aarch64) ARCH=arm64 ;;
  amd64|x86_64) ARCH=amd64 ;;
  *) echo "Unsupported Docker architecture." >&2; exit 69 ;;
esac

IMAGE=${CYNAPSA_DEMO_CLIENT_IMAGE:-cynapsa-demo-$ROLE:main-$ARCH}
ENV_FILE=${CYNAPSA_DEMO_CLIENT_ENV_FILE:-$ROOT/.env}
ACTIVE_VOLUME_FILE=$ROOT/.private/active-volume
env_file_value() {
  [[ -f "$ENV_FILE" ]] || return 0
  awk -v key="$1" 'index($0, key "=") == 1 { print substr($0, length(key) + 2); exit }' "$ENV_FILE"
}
force_enroll=false
build_requested=false
quiet=false
runtime_env=
build_log=
cleanup() {
  [[ -z "$runtime_env" ]] || rm -f -- "$runtime_env"
  [[ -z "$build_log" ]] || rm -f -- "$build_log"
}
trap cleanup EXIT

for option in "$@"; do
  case "$option" in
    --build) build_requested=true ;;
    --force-enroll) force_enroll=true ;;
    --quiet) quiet=true ;;
    *) echo "usage: ./run.sh [--build] [--force-enroll] [--quiet]" >&2; exit 64 ;;
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

if [[ $build_requested == true ]] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  if [[ $quiet == true ]]; then
    build_log=$(mktemp "${TMPDIR:-/tmp}/cynapsa-demo-client-build.XXXXXX")
    chmod 0600 "$build_log"
    if CYNAPSA_DEMO_CLIENT_IMAGE="$IMAGE" "$ROOT/build.sh" > "$build_log" 2>&1; then
      rm -f -- "$build_log"
      build_log=
    else
      build_status=$?
      cat "$build_log" >&2
      exit "$build_status"
    fi
  else
    echo "Building the GitHub-backed $ROLE image..."
    CYNAPSA_DEMO_CLIENT_IMAGE="$IMAGE" "$ROOT/build.sh"
  fi
fi

docker volume create "$VOLUME" >/dev/null
docker_args=(
  --rm
  -it
  --mount "type=volume,src=$VOLUME,dst=/var/lib/cynapsa"
)
if [[ -n ${CYNAPSA_DEMO_DOCKER_NETWORK:-} ]]; then
  docker network inspect "$CYNAPSA_DEMO_DOCKER_NETWORK" >/dev/null 2>&1 || {
    echo "Docker network $CYNAPSA_DEMO_DOCKER_NETWORK is unavailable." >&2
    exit 69
  }
  docker_args+=(--network "$CYNAPSA_DEMO_DOCKER_NETWORK")
fi
if [[ -f "$ENV_FILE" ]]; then
  runtime_env=$(mktemp "${TMPDIR:-/tmp}/cynapsa-demo-$ROLE-runtime.XXXXXX")
  chmod 0600 "$runtime_env"
  awk '$0 !~ /^[[:space:]]*CYNAPSA_GITHUB_TOKEN=/' "$ENV_FILE" > "$runtime_env"
  docker_args+=(--env-file "$runtime_env")
fi

if [[ -z ${DEMO_ORCHESTRATOR_ORIGIN:-} ]]; then
  DEMO_ORCHESTRATOR_ORIGIN=$(env_file_value DEMO_ORCHESTRATOR_ORIGIN)
fi
DEMO_ORCHESTRATOR_ORIGIN=${DEMO_ORCHESTRATOR_ORIGIN:-https://de-4f1d206529514541bb8cb316c5b0b51e.ecs.us-east-1.on.aws}
export DEMO_ORCHESTRATOR_ORIGIN

if [[ $DEMO_RUN_COMMAND == 'python /app/app.py' ]]; then
  if [[ -z ${DEMO_HTTP_ACCESS_KEY:-} ]]; then
    DEMO_HTTP_ACCESS_KEY=$(env_file_value DEMO_HTTP_ACCESS_KEY)
  fi
  if [[ -z ${DEMO_HTTP_ACCESS_KEY:-} || $DEMO_HTTP_ACCESS_KEY == replace-with-private-demo-key ]]; then
    command -v gcloud >/dev/null || {
      echo "gcloud is required to fetch the demo HTTP access key." >&2
      exit 69
    }
    DEMO_HTTP_ACCESS_KEY=$(gcloud secrets versions access 2 \
      --secret=cynapsa-demo-http-access-key --project=aztm-amesh)
  fi
  [[ -n $DEMO_HTTP_ACCESS_KEY ]] || {
    echo "The demo HTTP access key is missing." >&2
    exit 78
  }
  export DEMO_HTTP_ACCESS_KEY
fi
for variable in CYNAPSA_TOKEN DEMO_MESH_ID DEMO_ORCHESTRATOR_AGENT_ID DEMO_ORCHESTRATOR_ORIGIN DEMO_CONVERSATION_ID DEMO_RUN_COMMAND DEMO_HTTP_ACCESS_KEY; do
  [[ -z ${!variable:-} ]] || docker_args+=(--env "$variable")
done

if [[ $force_enroll == true ]]; then
  docker_args+=(--env DEMO_FORCE_ENROLL=1)
  [[ $quiet == true ]] || echo "Force-enrolling $ROLE in its selected state volume."
fi
[[ $quiet == false ]] || docker_args+=(--env DEMO_CLIENT_QUIET=1)
docker run "${docker_args[@]}" "$IMAGE"
