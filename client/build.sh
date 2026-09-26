#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
IMAGE=${CYNAPSA_DEMO_CLIENT_IMAGE:-cynapsa-demo-client:local}
PLATFORM=${CYNAPSA_DEMO_PLATFORM:-}
ENV_FILE=${CYNAPSA_DEMO_CLIENT_ENV_FILE:-$ROOT/.env}
github_token=${CYNAPSA_GITHUB_TOKEN:-}

if [[ -f "$ENV_FILE" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    case "$line" in
      CYNAPSA_GITHUB_TOKEN=*)
        candidate=${line#*=}
        candidate=${candidate%$'\r'}
        [[ -z "$candidate" ]] || github_token=$candidate
        ;;
    esac
  done < "$ENV_FILE"
fi
github_token=${github_token%$'\r'}
[[ -n "$github_token" && "$github_token" != replace-with-github-token ]] || {
  echo "CYNAPSA_GITHUB_TOKEN is required in $ENV_FILE or the shell to build." >&2
  exit 78
}

build_args=(
  --load
  --tag "$IMAGE"
  --secret id=github_token,env=CYNAPSA_GITHUB_TOKEN
  --build-arg "SOURCE_REFRESH=$(date -u +%Y%m%dT%H%M%S)-$$"
)
[[ -z "$PLATFORM" ]] || build_args+=(--platform "$PLATFORM")
CYNAPSA_GITHUB_TOKEN="$github_token" docker buildx build "${build_args[@]}" "$ROOT"
