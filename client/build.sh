#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SDK_ROOT=$ROOT/vendor/cynapsa-python-sdk
CORE_ROOT=$ROOT/vendor/cynapsagocore
IMAGE=${CYNAPSA_DEMO_CLIENT_IMAGE:-cynapsa-demo-client:local}
PLATFORM=${CYNAPSA_DEMO_PLATFORM:-}

[[ -f "$SDK_ROOT/pyproject.toml" ]] || { echo "Bundled SDK source is missing." >&2; exit 66; }
[[ -f "$CORE_ROOT/go.mod" ]] || { echo "Bundled Go Core source is missing." >&2; exit 66; }

if [[ -n "$PLATFORM" ]]; then
  docker buildx build --load --platform "$PLATFORM" --tag "$IMAGE" "$ROOT"
else
  docker buildx build --load --tag "$IMAGE" "$ROOT"
fi
