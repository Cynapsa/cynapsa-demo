#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROLE=files

command -v docker >/dev/null || { echo "Docker is required." >&2; exit 69; }
docker info >/dev/null 2>&1 || { echo "Docker is not running." >&2; exit 69; }
case "$(docker version --format '{{.Server.Arch}}')" in
  arm64|aarch64) ARCH=arm64 ;;
  amd64|x86_64) ARCH=amd64 ;;
  *) echo "Unsupported Docker architecture." >&2; exit 69 ;;
esac

IMAGE=${CYNAPSA_DEMO_FILES_IMAGE:-cynapsa-demo-$ROLE:github-main-$ARCH}
ENV_FILE=${CYNAPSA_DEMO_FILES_ENV_FILE:-$ROOT/.env}
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
if [[ -n ${CYNAPSA_DEMO_FILES_VOLUME:-} ]]; then
  VOLUME=$CYNAPSA_DEMO_FILES_VOLUME
elif [[ -f $ACTIVE_VOLUME_FILE ]]; then
  IFS= read -r VOLUME < "$ACTIVE_VOLUME_FILE"
  [[ $VOLUME =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || { echo "invalid active volume pointer" >&2; exit 78; }
else
  VOLUME=cynapsa-demo-$ROLE-state
fi

database_directory=${DEMO_FILES_DIRECTORY:-}
if [[ -f "$ENV_FILE" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    case "$line" in
      DEMO_FILES_DIRECTORY=*) database_directory=${line#*=}; database_directory=${database_directory%$'\r'} ;;
    esac
  done < "$ENV_FILE"
fi
[[ "$database_directory" == /* && -d "$database_directory" && ! -L "$database_directory" ]] || {
  echo "DEMO_FILES_DIRECTORY must be an existing absolute directory (not a symlink)." >&2
  exit 78
}
[[ "$database_directory" != *','* ]] || { echo "Directory paths containing commas are unsupported by Docker --mount." >&2; exit 78; }

if [[ $build_requested == true ]] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "Building the GitHub-backed $ROLE image..."
  CYNAPSA_DEMO_FILES_IMAGE="$IMAGE" "$ROOT/build.sh"
fi

docker volume create "$VOLUME" >/dev/null
docker_args=(
  --rm
  -it
  --mount "type=volume,src=$VOLUME,dst=/var/lib/cynapsa"
  --mount "type=bind,src=$database_directory,dst=/data,readonly"
)
runtime_env=
cleanup() {
  [[ -z "$runtime_env" ]] || rm -f -- "$runtime_env"
}
trap cleanup EXIT
if [[ -f "$ENV_FILE" ]]; then
  runtime_env=$(mktemp "${TMPDIR:-/tmp}/cynapsa-demo-$ROLE-runtime.XXXXXX")
  chmod 0600 "$runtime_env"
  awk '$0 !~ /^[[:space:]]*CYNAPSA_GITHUB_TOKEN=/' "$ENV_FILE" > "$runtime_env"
  docker_args+=(--env-file "$runtime_env")
fi
# Old local .env files may still contain Gemini settings. Never pass those
# retired credentials into the new container.
docker_args+=(--env GEMINI_API_KEY= --env DEMO_GEMINI_API_KEY= --env DEMO_GEMINI_MODEL=)
if [[ -f "$ROOT/.private/litellm_api_key" ]]; then
  docker_args+=(--mount "type=bind,src=$ROOT/.private/litellm_api_key,dst=/run/secrets/litellm_api_key,readonly")
fi
for variable in CYNAPSA_TOKEN LITELLM_API_KEY LITELLM_BASE_URL LITELLM_MODEL DEMO_MESH_ID DEMO_ORCHESTRATOR_AGENT_ID; do
	# Values in .env are authoritative; use the shell only for missing keys.
	if [[ -f "$ENV_FILE" ]] && grep -q "^${variable}=" "$ENV_FILE"; then
		continue
	fi
	# A private mounted key takes precedence over an inherited shell variable.
	if [[ "$variable" == LITELLM_API_KEY && -f "$ROOT/.private/litellm_api_key" ]]; then
		continue
	fi
	[[ -z ${!variable:-} ]] || docker_args+=(--env "$variable")
done

# The host path is only a mount source, not the container's database path.
docker_args+=(--env DEMO_FILES_DIRECTORY=/data)
if [[ $force_enroll == true ]]; then
  docker_args+=(--env DEMO_FORCE_ENROLL=1)
  echo "Force-enrolling $ROLE in its selected state volume."
fi
docker run "${docker_args[@]}" "$IMAGE"
