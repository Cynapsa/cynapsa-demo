#!/usr/bin/env bash
# Baseline SDK matrix against the dedicated ejabberd remove-snapshot branch.
# The older run.sh remains the legacy deep/fault campaign until its assertions
# are migrated to session-generation/revoke evidence.
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SDK_ROOT=$(cd -- "$ROOT/../.." && pwd)
GO_CORE=${CYNAPSA_GO_CORE_PATH:-$SDK_ROOT/../cynapsa/cynapsagocore-remove-snapshot}
EJABBERD=${CYNAPSA_EJABBERD_PATH:-$SDK_ROOT/../cynapsa/ejabberd-remove-snapshot}
TRUNK_PREFIX=${CYNAPSA_E2E_TRUNK_PREFIX:-}
RUN_ID=$(date -u +%Y%m%dt%H%M%Sz)-$$
PROJECT="cynapsa-peer-authority-e2e-$RUN_ID"
RUNTIME="$ROOT/.runtime/$RUN_ID"
ARTIFACTS="$ROOT/artifacts/$RUN_ID-peer-authority"
CLIENTS=(native-sync-client native-async-client monkey-sync-client monkey-async-client)
USERS=(native-sync-client native-async-client monkey-sync-client monkey-async-client native-server monkey-server)

die() { printf 'peer-authority-e2e: %s\n' "$*" >&2; exit 1; }

choose_trunk_prefix() {
  if [[ -n "$TRUNK_PREFIX" ]]; then
    return 0
  fi
  local third probe
  for third in {240..254}; do
    probe="cynapsa-peer-authority-prefix-probe-$RUN_ID-$third"
    if docker network create --internal --subnet "198.18.$third.0/24" "$probe" >/dev/null 2>&1; then
      docker network rm "$probe" >/dev/null 2>&1 || die "could not remove probe network"
      TRUNK_PREFIX="198.18.$third"
      return 0
    fi
  done
  die "no free E2E trunk /24; set CYNAPSA_E2E_TRUNK_PREFIX"
}

compose() {
  docker compose --project-name "$PROJECT" --env-file "$RUNTIME/credentials.env" \
    -f "$ROOT/compose.yaml" -f "$ROOT/compose.peer-authority.yaml" "$@"
}

ejabberdctl() { compose exec -T ejabberd ejabberdctl "$@"; }

wait_healthy() {
  local service=$1 deadline=$((SECONDS + 100)) container state
  while (( SECONDS < deadline )); do
    container=$(compose ps -a -q "$service" 2>/dev/null || true)
    if [[ -n "$container" ]]; then
      state=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || true)
      case "$state" in
        healthy) return 0 ;;
        unhealthy|exited|dead) die "$service became $state" ;;
      esac
    fi
    sleep 2
  done
  die "$service did not become healthy"
}

check_server_sessions() {
  # Both servers must stay connected while clients come and go. A completed
  # RPC matrix alone can mask an XMPP reconnect storm between client runs.
  compose logs --no-color ejabberd >"$RUNTIME/ejabberd-session-check.log"
  if grep -E 'Closing c2s session for (native-server|monkey-server)@mesh[.]test' \
    "$RUNTIME/ejabberd-session-check.log" >"$ARTIFACTS/unexpected-server-session-closures.log"; then
    die "a server XMPP session closed unexpectedly; see unexpected-server-session-closures.log"
  fi
}

capture_logs() {
  compose logs --no-color >"$ARTIFACTS/compose.log" 2>&1 || true
  compose logs --no-color native-server >"$ARTIFACTS/native-server.log" 2>&1 || true
  compose logs --no-color monkey-server >"$ARTIFACTS/monkey-server.log" 2>&1 || true
  compose ps --all --format json >"$ARTIFACTS/compose-ps.jsonl" 2>&1 || true
}

scrub_artifacts() {
  python3 - "$RUNTIME" "$ARTIFACTS" <<'PY'
from pathlib import Path
import re
import sys
runtime, artifacts = map(Path, sys.argv[1:])
secrets = []
for path in runtime.glob("*.token"):
    secrets.append(path.read_bytes().strip())
for line in (runtime / "credentials.env").read_bytes().splitlines():
    if b"=" in line:
        key, value = line.split(b"=", 1)
        if key.endswith(b"_PASSWORD") or key == b"CYNAPSA_TURN_STATIC_AUTH_SECRET":
            secrets.append(value)
for path in artifacts.iterdir():
    if not path.is_file():
        continue
    data = path.read_bytes()
    for secret in sorted((s for s in secrets if s), key=len, reverse=True):
        data = data.replace(secret, b"[REDACTED]")
    data = re.sub(rb"eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", b"[JWT_REDACTED]", data)
    path.write_bytes(data)
    if any(secret in data for secret in secrets if secret):
        raise SystemExit(f"unsanitized artifact: {path.name}")
PY
}

cleanup() {
  local status=${1:-$?}
  trap - EXIT INT TERM
  set +e
  capture_logs
  scrub_artifacts || status=1
  compose down --volumes --remove-orphans --timeout 10 >/dev/null 2>&1
  local container network
  for container in $(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT"); do
    docker rm -f "$container" >/dev/null 2>&1 || status=1
  done
  for network in $(docker network ls -q --filter "label=com.docker.compose.project=$PROJECT"); do
    docker network rm "$network" >/dev/null 2>&1 || status=1
  done
  docker image rm \
    "cynapsa-simple-e2e-agent:$RUN_ID" \
    "cynapsa-simple-e2e-ejabberd:$RUN_ID" \
    "cynapsa-simple-e2e-enrollment:$RUN_ID" \
    "cynapsa-simple-e2e-router:$RUN_ID" \
    "cynapsa-simple-e2e-sidecar:$RUN_ID" >/dev/null 2>&1 || true
  if [[ "$RUNTIME" == "$ROOT/.runtime/$RUN_ID" && -d "$RUNTIME" ]]; then
    rm -r -- "$RUNTIME"
  fi
  if [[ $status -ne 0 ]]; then
    printf 'peer-authority-e2e: failed; artifacts: %s\n' "$ARTIFACTS" >&2
  fi
  exit "$status"
}

command -v docker >/dev/null || die "Docker is required"
command -v openssl >/dev/null || die "OpenSSL is required"
command -v python3 >/dev/null || die "Python 3 is required"
choose_trunk_prefix
[[ -d "$GO_CORE/.git" || -f "$GO_CORE/.git" ]] || die "Go Core worktree not found: $GO_CORE"
[[ -d "$EJABBERD/.git" || -f "$EJABBERD/.git" ]] || die "ejabberd worktree not found: $EJABBERD"
[[ -f "$EJABBERD/Dockerfile" && -f "$EJABBERD/test/sdk-e2e/peer-authority.yml" ]] || \
  die "dedicated ejabberd image and SDK E2E fixture are required"
[[ $(git -C "$GO_CORE" branch --show-current) == remove-snapshot ]] || die "Go Core must be on remove-snapshot"
[[ $(git -C "$EJABBERD" branch --show-current) == remove-snapshot ]] || die "dedicated ejabberd must be on remove-snapshot"
umask 077
mkdir -p "$RUNTIME" "$ARTIFACTS"
trap 'cleanup $?' EXIT
trap 'cleanup 130' INT
trap 'cleanup 143' TERM

python3 "$ROOT/record_core_provenance.py" "$GO_CORE" "$ARTIFACTS/core-provenance.json"
python3 "$ROOT/record_core_provenance.py" "$EJABBERD" "$ARTIFACTS/ejabberd-provenance.json" \
  https://github.com/Cynapsa/ejabberd
python3 "$ROOT/prepare_mock_enrollment.py" "$RUNTIME"
printf '%s\n' '127.0.0.1 localhost' '::1 localhost ip6-localhost' \
  '10.240.70.2 enrollment.cynapsa.com' >"$RUNTIME/hosts"
chmod 0444 "$RUNTIME/hosts"

openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 2 \
  -subj '/CN=Cynapsa peer-authority E2E CA' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' \
  -keyout "$RUNTIME/ca-key.pem" -out "$RUNTIME/ca.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -sha256 -nodes -subj '/CN=mesh.test' \
  -keyout "$RUNTIME/server-key.pem" -out "$RUNTIME/server.csr" >/dev/null 2>&1
printf '%s\n' 'subjectAltName=DNS:mesh.test,IP:10.240.70.2' 'extendedKeyUsage=serverAuth' >"$RUNTIME/server.ext"
openssl x509 -req -sha256 -days 2 -in "$RUNTIME/server.csr" \
  -CA "$RUNTIME/ca.pem" -CAkey "$RUNTIME/ca-key.pem" -CAcreateserial \
  -extfile "$RUNTIME/server.ext" -out "$RUNTIME/server-cert.pem" >/dev/null 2>&1
cp "$RUNTIME/server-cert.pem" "$RUNTIME/server.pem"
cat "$RUNTIME/server-key.pem" >>"$RUNTIME/server.pem"
openssl req -newkey rsa:2048 -sha256 -nodes -subj '/CN=enrollment.cynapsa.com' \
  -keyout "$RUNTIME/enrollment-key.pem" -out "$RUNTIME/enrollment.csr" >/dev/null 2>&1
printf '%s\n' 'subjectAltName=DNS:enrollment.cynapsa.com' 'extendedKeyUsage=serverAuth' >"$RUNTIME/enrollment.ext"
openssl x509 -req -sha256 -days 2 -in "$RUNTIME/enrollment.csr" \
  -CA "$RUNTIME/ca.pem" -CAkey "$RUNTIME/ca-key.pem" -CAcreateserial \
  -extfile "$RUNTIME/enrollment.ext" -out "$RUNTIME/enrollment-cert.pem" >/dev/null 2>&1

{
  printf 'CYNAPSA_E2E_RUNTIME_DIR=%s\n' "$RUNTIME"
  printf 'CYNAPSA_E2E_IMAGE_TAG=%s\n' "$RUN_ID"
  printf 'CYNAPSA_E2E_TRUNK_PREFIX=%s\n' "$TRUNK_PREFIX"
  printf 'CYNAPSA_GO_CORE_PATH=%s\n' "$GO_CORE"
  printf 'CYNAPSA_EJABBERD_PATH=%s\n' "$EJABBERD"
  printf 'CYNAPSA_TURN_STATIC_AUTH_SECRET=%s\n' "$(openssl rand -hex 32)"
  for user in "${USERS[@]}"; do
    key=$(printf '%s' "$user" | tr '[:lower:]-' '[:upper:]_')_PASSWORD
    printf '%s=%s\n' "$key" "$(openssl rand -hex 24)"
  done
} >"$RUNTIME/credentials.env"
python3 - "$RUNTIME/credentials.env" "$EJABBERD/test/sdk-e2e/peer-authority.yml" "$RUNTIME/ejabberd-peer-authority.yml" <<'PY'
from pathlib import Path
import sys
values = dict(line.split("=", 1) for line in Path(sys.argv[1]).read_text().splitlines() if "=" in line)
template = Path(sys.argv[2]).read_text()
Path(sys.argv[3]).write_text(template.replace("@CYNAPSA_TURN_STATIC_AUTH_SECRET@", values["CYNAPSA_TURN_STATIC_AUTH_SECRET"]))
PY
cp "$ROOT/coturn-stun.conf" "$RUNTIME/coturn-stun.conf"
python3 - "$RUNTIME/credentials.env" "$ROOT/coturn-turn.conf.in" "$RUNTIME/coturn-turn.conf" <<'PY'
from pathlib import Path
import sys
values = dict(line.split("=", 1) for line in Path(sys.argv[1]).read_text().splitlines() if "=" in line)
template = Path(sys.argv[2]).read_text()
Path(sys.argv[3]).write_text(template.replace("@CYNAPSA_TURN_STATIC_AUTH_SECRET@", values["CYNAPSA_TURN_STATIC_AUTH_SECRET"]))
PY
chmod 0444 "$RUNTIME/ca.pem" "$RUNTIME/server.pem" "$RUNTIME/ejabberd-peer-authority.yml" \
  "$RUNTIME/coturn-stun.conf" "$RUNTIME/coturn-turn.conf"

compose config --quiet
compose build router native-server-net ejabberd native-server enrollment
compose up -d router stun turn enrollment ejabberd
for service in router ejabberd-net stun-net turn-net stun turn enrollment ejabberd; do
  wait_healthy "$service"
done
ready=$(ejabberdctl cynapsa_mesh_ready mesh.test)
[[ "$ready" == $'ready\tmesh.test\tsingle_node_mnesia' ]] || die "dedicated mesh module was not ready"
ejabberdctl cynapsa_mesh_create simple-e2e mesh.test >/dev/null
for user in "${USERS[@]}"; do
  ejabberdctl cynapsa_mesh_add "$user" mesh.test simple-e2e >/dev/null
done

compose up -d native-server-net monkey-server-net
wait_healthy native-server-net
wait_healthy monkey-server-net
compose up -d native-server monkey-server
wait_healthy native-server
wait_healthy monkey-server

client_logs=()
for client in "${CLIENTS[@]}"; do
  compose --profile clients up -d "$client-net"
  wait_healthy "$client-net"
  if ! compose --profile clients run --rm --no-deps "$client" >"$ARTIFACTS/$client.log" 2>&1; then
    die "$client failed; see its sanitized artifact"
  fi
  client_logs+=("$ARTIFACTS/$client.log")
  check_server_sessions
done
compose logs --no-color native-server >"$ARTIFACTS/native-server.log"
compose logs --no-color monkey-server >"$ARTIFACTS/monkey-server.log"
python3 "$ROOT/verify_results.py" \
  --require-hop \
  --native-server-log "$ARTIFACTS/native-server.log" \
  --monkey-server-log "$ARTIFACTS/monkey-server.log" \
  "${client_logs[@]}" >/dev/null
printf 'peer-authority-e2e: PASS 80/80 and 6 three-agent error checks; artifacts: %s\n' "$ARTIFACTS"
