#!/usr/bin/env bash
set -Eeuo pipefail

PLATFORM_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SDK_ROOT=$(cd -- "$PLATFORM_ROOT/../../.." && pwd)
WORKSPACE_ROOT=${CYNAPSA_WORKSPACE_ROOT:-$(cd -- "$SDK_ROOT/../cynapsa" && pwd)}
STATE_ROOT="$PLATFORM_ROOT/.runtime"
STATE="$STATE_ROOT/state"
ENV_FILE="$STATE_ROOT/platform.env"
COMPOSE_FILE="$PLATFORM_ROOT/compose.yaml"

TESTS=${CYNAPSA_TESTS_PATH:-$WORKSPACE_ROOT/CynapsaTests}
MANAGEMENT=${CYNAPSA_MANAGEMENT_PATH:-$WORKSPACE_ROOT/aztmmanagment}
ENROLLMENT=${CYNAPSA_ENROLLMENT_PATH:-$WORKSPACE_ROOT/Enrollment}
EJABBERD=${CYNAPSA_EJABBERD_PATH:-$WORKSPACE_ROOT/ejabberd}
GO_CORE=${CYNAPSA_GO_CORE_PATH:-$WORKSPACE_ROOT/cynapsagocore}

die() {
  printf 'cynapsa-local-platform: %s\n' "$*" >&2
  exit 1
}

for command in docker openssl python3 curl; do
  command -v "$command" >/dev/null 2>&1 || die "required command not found: $command"
done
docker compose version >/dev/null

[[ -d "$TESTS/services/auth0-emulator" ]] || die "CynapsaTests checkout not found: $TESTS"
[[ -d "$MANAGEMENT/backend" && -d "$MANAGEMENT/frontend" ]] || die "Management checkout not found: $MANAGEMENT"
[[ -f "$ENROLLMENT/Dockerfile" ]] || die "Enrollment checkout not found: $ENROLLMENT"
[[ -f "$EJABBERD/Dockerfile" ]] || die "ejabberd checkout not found: $EJABBERD"
[[ -d "$GO_CORE/internal" ]] || die "Go Core checkout not found: $GO_CORE"

mkdir -p "$STATE_ROOT"
chmod 0700 "$STATE_ROOT"
if [[ ! -d "$STATE" ]]; then
  python3 "$SDK_ROOT/e2e/simple/auth_state.py" \
    --state "$STATE" \
    --template "$PLATFORM_ROOT/ejabberd.yml"
elif [[ ! -f "$STATE/config/ejabberd.yml" ]]; then
  die "existing local state is incomplete; remove $STATE_ROOT and start again"
fi
umask 077
{
  printf 'CYNAPSA_PLATFORM_STATE=%s\n' "$STATE"
  printf 'CYNAPSA_TESTS_PATH=%s\n' "$TESTS"
  printf 'CYNAPSA_MANAGEMENT_PATH=%s\n' "$MANAGEMENT"
  printf 'CYNAPSA_ENROLLMENT_PATH=%s\n' "$ENROLLMENT"
  printf 'CYNAPSA_EJABBERD_PATH=%s\n' "$EJABBERD"
  printf 'CYNAPSA_GO_CORE_PATH=%s\n' "$GO_CORE"
} >"$ENV_FILE"

# Match bind-mounted private inputs to each image's unprivileged runtime UID.
chmod 0600 "$STATE/secrets/erlang-cookie"
docker run --rm --network none -v "$STATE:/state" alpine:3.23 sh -ec '
  chown 10001:10001 /state/secrets/auth0-client-secret /state/secrets/auth0-bootstrap-secret /state/secrets/auth0-signing-key.pem /state/certs/auth0.key
  chown 65532:65532 /state/secrets/enrollment-management-service-token /state/certs/enrollment.key
  chown 9000:9000 /state/secrets/ejabberd-api-password /state/secrets/erlang-cookie /state/config/ejabberd.yml /state/certs/ejabberd.key
  chmod 0600 /state/secrets/auth0-client-secret /state/secrets/auth0-bootstrap-secret /state/secrets/auth0-signing-key.pem /state/certs/auth0.key /state/secrets/enrollment-management-service-token /state/certs/enrollment.key /state/secrets/ejabberd-api-password /state/config/ejabberd.yml /state/certs/ejabberd.key
  chmod 0400 /state/secrets/erlang-cookie
'

compose=(docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE")
"${compose[@]}" config --quiet
COMPOSE_PROGRESS=plain "${compose[@]}" up -d --build --remove-orphans

wait_healthy() {
  local service=$1 deadline=$((SECONDS + 180)) container health
  while (( SECONDS < deadline )); do
    container=$("${compose[@]}" ps -q "$service" 2>/dev/null || true)
    if [[ -n "$container" ]]; then
      health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || true)
      [[ "$health" == healthy ]] && return 0
      if [[ "$health" == exited || "$health" == unhealthy ]]; then
        "${compose[@]}" logs --no-color "$service" >&2 || true
        die "$service entered $health state"
      fi
    fi
    sleep 2
  done
  "${compose[@]}" logs --no-color "$service" >&2 || true
  die "$service readiness timed out"
}

for service in postgres auth0 ejabberd management enrollment portal-session; do
  wait_healthy "$service"
done

deadline=$((SECONDS + 90))
until curl --fail --silent --show-error http://127.0.0.1:5173/local-login.html >/dev/null; do
  (( SECONDS < deadline )) || {
    "${compose[@]}" logs --no-color portal portal-bootstrap >&2 || true
    die "portal readiness timed out"
  }
  sleep 2
done

printf '\nCynapsa local platform is ready.\n'
printf 'Open: http://localhost:5173/local-login.html\n'
printf 'The local login opens the organization setup page on first use.\n'
printf 'Logs: %s logs -f\n' "${compose[*]}"
printf 'Stop: %s/stop.sh\n' "$PLATFORM_ROOT"
