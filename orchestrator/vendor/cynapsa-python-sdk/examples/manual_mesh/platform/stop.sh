#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ENV_FILE="$ROOT/.runtime/platform.env"
[[ -f "$ENV_FILE" ]] || {
  printf 'cynapsa-local-platform: no local runtime was found\n' >&2
  exit 1
}

docker compose --env-file "$ENV_FILE" -f "$ROOT/compose.yaml" down --remove-orphans
printf 'Cynapsa local platform stopped; database and authority volumes were preserved.\n'
