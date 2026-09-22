#!/bin/sh
set -eu

TOXIPROXY_IMAGE='ghcr.io/shopify/toxiproxy@sha256:9378ed52a28bc50edc1350f936f518f31fa95f0d15917d6eb40b8e376d1a214e'
AGENT_IMAGE='ghcr.io/processone/ejabberd@sha256:68482e33ff11934e73ef2881cf455ccefc1e03b31203f3ab651f4b2b1152b4a8'
ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
REPOSITORY=$(CDPATH='' cd -- "$ROOT/../../.." && pwd)
EJABBERD="$REPOSITORY/test/e2e/ejabberd/run.sh"

die() {
  printf '%s\n' "fault-environment: $*" >&2
  exit 1
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"
}

allocate_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

read_one() {
  [ -f "$1" ] || return 1
  sed -n '1p' "$1"
}

container_ipv4() {
  container=$1
  network=$2
  docker inspect "$container" --format "{{with index .NetworkSettings.Networks \"$network\"}}{{.IPAddress}}{{end}}"
}

capture_artifacts() {
  state=$1
  mkdir -p "$state/artifacts"
  proxy=$(read_one "$state/proxy-container-id" 2>/dev/null || true)
  object=$(read_one "$state/object-container-id" 2>/dev/null || true)
  if [ -n "$proxy" ]; then
    docker logs "$proxy" >"$state/artifacts/toxiproxy.log" 2>&1 || true
    docker inspect "$proxy" --format '{{json .State}}' >"$state/artifacts/toxiproxy-state.json" 2>/dev/null || true
  fi
  if [ -n "$object" ]; then
    docker logs "$object" >"$state/artifacts/objectserver.log" 2>&1 || true
    docker inspect "$object" --format '{{json .State}}' >"$state/artifacts/objectserver-state.json" 2>/dev/null || true
  fi
  cp "$ROOT/profiles-v1.json" "$state/artifacts/profiles-v1.json"
  find "$state/artifacts" -type f -exec chmod 600 {} \;
}

ensure_image() {
  image=$1
  log=$2
  if docker image inspect "$image" >/dev/null 2>&1; then
    return 0
  fi
  if ! docker pull "$image" >"$log" 2>&1; then
    chmod 600 "$log" 2>/dev/null || true
    die "failed to pull pinned image; artifact: $log"
  fi
  chmod 600 "$log"
}

stop_environment() {
  state=$1
  [ -d "$state" ] || return 0
  [ "$(read_one "$state/kind" 2>/dev/null || true)" = cynapsa-fault-environment-v1 ] || die "refusing to stop unknown state directory"
  capture_artifacts "$state"
  proxy=$(read_one "$state/proxy-container-id" 2>/dev/null || true)
  object=$(read_one "$state/object-container-id" 2>/dev/null || true)
  child=$(read_one "$state/ejabberd-state" 2>/dev/null || true)
  if [ -n "$proxy" ]; then
    docker rm -f "$proxy" >/dev/null 2>&1 || true
  fi
  if [ -n "$object" ]; then
    docker rm -f -v "$object" >/dev/null 2>&1 || true
  fi
  if [ -n "$child" ]; then
    "$EJABBERD" stop "$child"
  fi
  rm -f "$state/credentials.env" "$state/objectserver"
}

wait_proxy_api() {
  state=$1
  port=$(read_one "$state/api-port")
  deadline=$(( $(date +%s) + 20 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if python3 - "$port" <<'PY' >/dev/null 2>&1
import sys, urllib.request
with urllib.request.urlopen("http://127.0.0.1:%s/version" % sys.argv[1], timeout=1) as response:
    assert response.status == 200
PY
    then
      return 0
    fi
    sleep 1
  done
  capture_artifacts "$state"
  die "Toxiproxy readiness timed out; artifacts: $state/artifacts"
}

start_environment() {
  require_tool docker
  require_tool go
  require_tool python3
  state=$(mktemp -d "${TMPDIR:-/tmp}/cynapsa-faults.XXXXXX")
  chmod 700 "$state"
  printf '%s\n' cynapsa-fault-environment-v1 >"$state/kind"
  trap 'stop_environment "$state"' INT TERM HUP EXIT

  # Keep first-use image progress out of the stdout state-path contract. The
  # test runner intentionally treats stdout as one absolute path.
  ensure_image "$TOXIPROXY_IMAGE" "$state/toxiproxy-pull.log"
  ensure_image "$AGENT_IMAGE" "$state/agent-image-pull.log"

  child=$("$EJABBERD" start shared-group)
  printf '%s\n' "$child" >"$state/ejabberd-state"
  cp "$child/credentials.env" "$state/credentials.env"
  network=$(read_one "$child/network")
  ejabberd=$(read_one "$child/container-id")
  ejabberd_ip=$(container_ipv4 "$ejabberd" "$network")
  [ -n "$ejabberd_ip" ] || die "ejabberd network address unavailable"

  GOTOOLCHAIN=go1.26.6 GOOS=linux GOARCH="$(go env GOARCH)" CGO_ENABLED=0 \
    go build -trimpath -o "$state/objectserver" ./test/production/faults/objectserver
  chmod 0555 "$state/objectserver"
  suffix=$(basename "$state" | tr -cd 'a-zA-Z0-9')
  object=$(docker run -d \
    --name "cynapsa-fault-object-${suffix}" \
    --network "$network" \
    --read-only \
    --cpus 0.5 \
    --memory 96m \
    --pids-limit 64 \
    --security-opt no-new-privileges \
    --cap-drop ALL \
    --mount "type=bind,src=${state}/objectserver,dst=/objectserver,readonly" \
    --entrypoint /objectserver \
    "$AGENT_IMAGE" -listen :8080)
  printf '%s\n' "$object" >"$state/object-container-id"
  object_ip=$(container_ipv4 "$object" "$network")
  [ -n "$object_ip" ] || die "object endpoint network address unavailable"

  api_port=$(allocate_port)
  xmpp_port=$(allocate_port)
  object_port=$(allocate_port)
  printf '%s\n' "$api_port" >"$state/api-port"
  printf '%s\n' "$xmpp_port" >"$state/xmpp-port"
  printf '%s\n' "$object_port" >"$state/object-port"
  printf '%s\n' "$TOXIPROXY_IMAGE" >"$state/proxy-image.txt"
  proxy=$(docker run -d \
    --name "cynapsa-fault-proxy-${suffix}" \
    --network "$network" \
    --read-only \
    --cpus 0.5 \
    --memory 96m \
    --pids-limit 64 \
    --security-opt no-new-privileges \
    --cap-drop ALL \
    -p "127.0.0.1:${api_port}:8474" \
    -p "127.0.0.1:${xmpp_port}:8666" \
    -p "127.0.0.1:${object_port}:8667" \
    "$TOXIPROXY_IMAGE" -host=0.0.0.0)
  printf '%s\n' "$proxy" >"$state/proxy-container-id"
  printf '%s\n' "$ejabberd_ip:5222" >"$state/xmpp-upstream"
  printf '%s\n' "$object_ip:8080" >"$state/object-upstream"
  wait_proxy_api "$state"
  trap - INT TERM HUP EXIT
  printf '%s\n' "$state"
}

restart_proxy() {
  state=$1
  [ "$(read_one "$state/kind" 2>/dev/null || true)" = cynapsa-fault-environment-v1 ] || die "unknown state directory"
  proxy=$(read_one "$state/proxy-container-id")
  docker restart --time 1 "$proxy" >/dev/null
  wait_proxy_api "$state"
}

case "${1:-}" in
  start)
    [ "$#" -eq 1 ] || die 'usage: run.sh start'
    start_environment
    ;;
  stop)
    [ "$#" -eq 2 ] || die 'usage: run.sh stop STATE_DIRECTORY'
    stop_environment "$2"
    ;;
  restart-proxy)
    [ "$#" -eq 2 ] || die 'usage: run.sh restart-proxy STATE_DIRECTORY'
    restart_proxy "$2"
    ;;
  *)
    die 'usage: run.sh {start|stop STATE_DIRECTORY|restart-proxy STATE_DIRECTORY}'
    ;;
esac
