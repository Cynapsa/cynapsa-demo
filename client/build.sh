#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SDK_ROOT=${CYNAPSA_SDK_ROOT:-$HOME/git/cynapsa-python-sdk}
CORE_ROOT=${CYNAPSA_CORE_ROOT:-$HOME/git/cynapsa/cynapsagocore}
IMAGE=${CYNAPSA_DEMO_CLIENT_IMAGE:-cynapsa-demo-client:local}
PLATFORM=${CYNAPSA_DEMO_PLATFORM:-}

[[ -f "$SDK_ROOT/pyproject.toml" ]] || { echo "SDK checkout not found: $SDK_ROOT" >&2; exit 66; }
[[ -f "$CORE_ROOT/go.mod" ]] || { echo "Go Core checkout not found: $CORE_ROOT" >&2; exit 66; }

platform_args=()
[[ -z "$PLATFORM" ]] || platform_args=(--platform "$PLATFORM")
docker buildx build --load "${platform_args[@]}" \
  --build-context "cynapsa_sdk=$SDK_ROOT" \
  --build-context "cynapsa_core=$CORE_ROOT" \
  --tag "$IMAGE" "$ROOT"
