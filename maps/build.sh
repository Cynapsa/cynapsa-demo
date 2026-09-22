#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SDK_ROOT=${CYNAPSA_SDK_ROOT:-$HOME/git/cynapsa-python-sdk}
CORE_ROOT=${CYNAPSA_CORE_ROOT:-$HOME/git/cynapsa/cynapsagocore}
IMAGE=${CYNAPSA_DEMO_MAPS_IMAGE:-cynapsa-demo-maps:local}

[[ -f "$SDK_ROOT/pyproject.toml" ]] || { echo "SDK checkout not found: $SDK_ROOT" >&2; exit 66; }
[[ -f "$CORE_ROOT/go.mod" ]] || { echo "Go Core checkout not found: $CORE_ROOT" >&2; exit 66; }

docker buildx build --load \
  --build-context "cynapsa_sdk=$SDK_ROOT" \
  --build-context "cynapsa_core=$CORE_ROOT" \
  --tag "$IMAGE" "$ROOT"
