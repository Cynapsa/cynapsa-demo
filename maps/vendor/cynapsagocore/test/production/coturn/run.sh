#!/bin/sh
set -eu

COTURN_IMAGE='coturn/coturn@sha256:0feee4fc1f45c7c053c8fee3e1ab941b1a1b9a0429bc01e18126735410770bfd'
NAT_GATEWAY_IMAGE='nicolaka/netshoot@sha256:47b907d662d139d1e2f22bfe14f4efca1e3f1feed283572f47c970c780c03b61'
ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
REPOSITORY=$(CDPATH='' cd -- "$ROOT/../../.." && pwd)
EJABBERD_HARNESS="$REPOSITORY/test/e2e/ejabberd/run.sh"

die() {
  printf '%s\n' "p2p-environment: $*" >&2
  exit 1
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"
}

read_first() {
  [ -f "$1" ] || return 1
  sed -n '1p' "$1"
}

container_id() {
  read_first "$1/$2-container-id"
}

capture_artifacts() {
  state=$1
  [ -d "$state" ] || return 0
  mkdir -p "$state/artifacts"
  for service in stun turn; do
    id=$(container_id "$state" "$service" 2>/dev/null || true)
    if [ -n "$id" ]; then
      docker logs "$id" >"$state/artifacts/${service}.log" 2>&1 || true
      docker inspect "$id" --format '{{json .State}}' >"$state/artifacts/${service}-state.json" 2>/dev/null || true
    fi
  done
  if [ -f "$state/readiness.log" ]; then
    cp "$state/readiness.log" "$state/artifacts/readiness.log"
  fi
  if [ -f "$state/image.txt" ]; then
    cp "$state/image.txt" "$state/artifacts/image.txt"
  fi
  if [ -f "$state/gateway-image.txt" ]; then
    cp "$state/gateway-image.txt" "$state/artifacts/gateway-image.txt"
  fi
  if [ -f "$state/turn-mode" ]; then
    cp "$state/turn-mode" "$state/artifacts/turn-mode.txt"
  fi
  find "$state/artifacts" -type f -exec chmod 600 {} \;
}

stop_environment() {
  state=$1
  [ -d "$state" ] || return 0
  capture_artifacts "$state"
  for service in stun turn; do
    id=$(container_id "$state" "$service" 2>/dev/null || true)
    if [ -n "$id" ]; then
      docker rm -f -v "$id" >/dev/null 2>&1 || true
    fi
  done
  child=$(read_first "$state/ejabberd-state" 2>/dev/null || true)
  if [ -n "$child" ] && [ -d "$child" ]; then
    "$EJABBERD_HARNESS" stop "$child" >/dev/null 2>&1 || true
  fi
  rm -f "$state/credentials.env" "$state/extdisco-credential.json" "$state/stun.conf" "$state/turn.conf" "$state/turn-mode" "$state/turn-rest-secret"
}

wait_container() {
  state=$1
  service=$2
  id=$(container_id "$state" "$service")
  deadline=$(( $(date +%s) + 40 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ "$(docker inspect -f '{{.State.Running}}' "$id" 2>/dev/null || true)" = true ]; then
      if [ "$service" = stun ]; then
        if docker exec "$id" turnutils_stunclient -p 3478 127.0.0.1 >/dev/null 2>&1; then
          printf 'stun=ready\n' >>"$state/readiness.log"
          return 0
        fi
      else
        if docker exec "$id" turnutils_stunclient -p 3478 127.0.0.1 >/dev/null 2>&1; then
          printf 'turn=ready listener=true mode=%s\n' "$(read_first "$state/turn-mode")" >>"$state/readiness.log"
          return 0
        fi
      fi
    fi
    sleep 1
  done
  capture_artifacts "$state"
  die "$service protocol readiness timed out; artifacts: $state/artifacts"
}

write_turn_config() {
  state=$1
  mode=$2
  rest_secret=$(sed -n '1p' "$state/turn-rest-secret")
  [ "${#rest_secret}" -eq 64 ] || die 'TURN REST secret file is malformed'
  case "$rest_secret" in *[!0-9a-f]*) die 'TURN REST secret file is malformed' ;; esac
  case "$mode" in
    normal)
      auth_options="use-auth-secret
static-auth-secret=$rest_secret"
      fault_option=''
      ;;
    allocation-rejected)
      auth_options="use-auth-secret
static-auth-secret=$rest_secret"
      # Reject the only server-authorized UDP relay transport at allocation
      # time. Quotas cannot reject deterministically because XEP-0215 mints a
      # distinct temporary username for each authenticated agent and a single
      # successful relay allocation can be sufficient for ICE.
      fault_option='no-udp-relay'
      ;;
    permission-rejected)
      auth_options="use-auth-secret
static-auth-secret=$rest_secret"
      fault_option='denied-peer-ip=0.0.0.0-255.255.255.255'
      ;;
    *) die "unknown TURN mode: $mode" ;;
  esac
  cat >"$state/turn.conf" <<EOF
listening-port=3478
listening-ip=0.0.0.0
fingerprint
$auth_options
realm=mesh.test
stale-nonce=600
min-port=49160
max-port=49179
no-tls
no-dtls
no-software-attribute
no-multicast-peers
verbose
log-file=stdout
simple-log
$fault_option
EOF
  chmod 600 "$state/turn.conf"
  printf '%s\n' "$mode" >"$state/turn-mode"
  chmod 600 "$state/turn-mode"
}

wait_external_readiness() {
  state=$1
  child=$(read_first "$state/ejabberd-state")
  turn=$(container_id "$state" turn)
  credential_file="$state/extdisco-credential.json"
  rm -f "$credential_file"
  "$EJABBERD_HARNESS" admin "$child" create readiness >/dev/null
  "$EJABBERD_HARNESS" admin "$child" add agent-a readiness >/dev/null
  CYNAPSA_EJABBERD_MESH=readiness CYNAPSA_EXTDISCO_CREDENTIAL_FILE="$credential_file" \
    "$EJABBERD_HARNESS" probe "$child" external >/dev/null
  [ -f "$credential_file" ] || die 'authenticated XEP-0215 probe produced no credentials'
  username=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["username"])' "$credential_file")
  credential=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["password"])' "$credential_file")
  host=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["host"])' "$credential_file")
  port=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["port"])' "$credential_file")
  lifetime=$(python3 -c 'import datetime,json,sys; value=json.load(open(sys.argv[1], encoding="utf-8"))["expires_at"]; expires=datetime.datetime.fromisoformat(value.replace("Z", "+00:00")); remaining=int((expires-datetime.datetime.now(datetime.timezone.utc)).total_seconds()); print(remaining)' "$credential_file")
  if [ "$host" != turn.test ] || [ "$port" != 3478 ] || [ -z "$username" ] || [ -z "$credential" ]; then
    die 'authenticated XEP-0215 probe returned an unexpected TURN service'
  fi
  if [ "$lifetime" -le 0 ] || [ "$lifetime" -gt 120 ]; then
    die 'authenticated XEP-0215 credential lifetime is not short-lived'
  fi
  # Keep the short-lived credential out of the host Docker command line. The
  # pinned Coturn utility accepts credentials only as flags, so a private stdin
  # handoff expands them entirely inside the disposable container.
  if ! printf '%s\n%s\n' "$username" "$credential" | docker exec -i "$turn" /bin/sh -c '
    IFS= read -r username || exit 2
    IFS= read -r credential || exit 2
    turnutils_uclient -y -c -n 1 -m 2 -p 3478 -u "$username" -w "$credential" 127.0.0.1
  ' >/dev/null 2>&1; then
    rm -f "$credential_file"
    die 'XEP-0215 TURN credential allocation failed'
  fi
  rm -f "$credential_file"
  printf 'xmpp=authenticated extdisco=valid turn=authenticated-allocation lifetime=%ss\n' "$lifetime" >>"$state/readiness.log"
}

start_environment() {
  state_report=${1:-}
  require_tool docker
  require_tool openssl
  [ -x "$EJABBERD_HARNESS" ] || die "ejabberd harness is unavailable"
  state=$(mktemp -d "${TMPDIR:-/tmp}/cynapsa-p2p.XXXXXX")
  chmod 700 "$state"
  trap 'stop_environment "$state"' INT TERM HUP EXIT
  if [ -n "$state_report" ]; then
    case "$state_report" in
      /*) ;;
      *) die 'state report path must be absolute' ;;
    esac
    if [ -e "$state_report" ] || [ -L "$state_report" ]; then
      die 'state report path already exists'
    fi
    [ -d "$(dirname -- "$state_report")" ] || die 'state report parent is unavailable'
    (umask 077; set -C; printf '%s\n' "$state" >"$state_report") || die 'state report could not be created'
  fi
  suffix=$(basename "$state" | tr -cd 'a-zA-Z0-9')
  rest_secret_file="$state/turn-rest-secret"
  (umask 077; openssl rand -hex 32 >"$rest_secret_file")
  chmod 600 "$rest_secret_file"
  child=$(CYNAPSA_EJABBERD_TURN_REST_SECRET_FILE="$rest_secret_file" "$EJABBERD_HARNESS" start shared-group-extdisco)
  printf '%s\n' "$child" >"$state/ejabberd-state"
  network=$(read_first "$child/network")
  host_identity="$(id -u):$(id -g)"

  umask 077
  {
    cat "$child/credentials.env"
    printf 'CYNAPSA_P2P_STATE=%s\n' "$state"
    printf 'CYNAPSA_P2P_NETWORK=%s\n' "$network"
  } >"$state/credentials.env"
  chmod 600 "$state/credentials.env"

  cat >"$state/stun.conf" <<'EOF'
listening-port=3478
listening-ip=0.0.0.0
fingerprint
stun-only
no-tls
no-dtls
no-software-attribute
no-multicast-peers
verbose
log-binding
log-file=stdout
simple-log
EOF
  write_turn_config "$state" normal
  chmod 600 "$state/stun.conf"
  printf '%s\n' "$COTURN_IMAGE" >"$state/image.txt"
  printf '%s\n' "$NAT_GATEWAY_IMAGE" >"$state/gateway-image.txt"

  stun=$(docker run -d \
    --name "cynapsa-stun-${suffix}" \
    --network "$network" --network-alias stun.test \
    --user "$host_identity" \
    --read-only --cpus 0.5 --memory 128m --pids-limit 64 \
    --security-opt no-new-privileges --cap-drop ALL --cap-add NET_BIND_SERVICE \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,size=8m \
    --mount "type=bind,src=${state}/stun.conf,dst=/config/turnserver.conf,readonly" \
    --entrypoint turnserver "$COTURN_IMAGE" -c /config/turnserver.conf)
  printf '%s\n' "$stun" >"$state/stun-container-id"

  turn=$(docker run -d \
    --name "cynapsa-turn-${suffix}" \
    --network "$network" --network-alias turn.test \
    --user "$host_identity" \
    --read-only --cpus 0.5 --memory 128m --pids-limit 64 \
    --security-opt no-new-privileges --cap-drop ALL --cap-add NET_BIND_SERVICE \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,size=8m \
    --mount "type=bind,src=${state}/turn.conf,dst=/config/turnserver.conf,readonly" \
    --entrypoint turnserver "$COTURN_IMAGE" -c /config/turnserver.conf)
  printf '%s\n' "$turn" >"$state/turn-container-id"

  wait_container "$state" stun
  wait_container "$state" turn
  wait_external_readiness "$state"
  trap - INT TERM HUP EXIT
  printf '%s\n' "$state"
}

delegate_admin() {
  state=$1
  shift
  child=$(read_first "$state/ejabberd-state")
  "$EJABBERD_HARNESS" admin "$child" "$@"
}

restart_service() {
  state=$1
  service=$2
  case "$service" in
    ejabberd)
      child=$(read_first "$state/ejabberd-state")
      "$EJABBERD_HARNESS" restart "$child"
      ;;
    stun|turn)
      id=$(container_id "$state" "$service")
      docker restart "$id" >/dev/null
      wait_container "$state" "$service"
      ;;
    *) die "unknown service: $service" ;;
  esac
}

set_external_service_mode() {
  state=$1
  mode=$2
  child=$(read_first "$state/ejabberd-state")
  "$EJABBERD_HARNESS" external-service-mode "$child" "$mode"
}

set_turn_mode() {
  state=$1
  mode=$2
  id=$(container_id "$state" turn)
  docker stop "$id" >/dev/null
  write_turn_config "$state" "$mode"
  docker start "$id" >/dev/null
  wait_container "$state" turn
}

case "${1:-}" in
  start)
    [ "$#" -le 2 ] || die 'usage: run.sh start [STATE_REPORT]'
    start_environment "${2:-}"
    ;;
  stop)
    [ "$#" -eq 2 ] || die 'usage: run.sh stop STATE_DIRECTORY'
    stop_environment "$2"
    ;;
  admin)
    [ "$#" -ge 3 ] || die 'usage: run.sh admin STATE ACTION [ARG...]'
    state=$2
    shift 2
    delegate_admin "$state" "$@"
    ;;
  restart)
    [ "$#" -eq 3 ] || die 'usage: run.sh restart STATE_DIRECTORY {ejabberd|stun|turn}'
    restart_service "$2" "$3"
    ;;
  turn-mode)
    [ "$#" -eq 3 ] || die 'usage: run.sh turn-mode STATE_DIRECTORY {normal|allocation-rejected|permission-rejected}'
    set_turn_mode "$2" "$3"
    ;;
  external-service-mode)
    [ "$#" -eq 3 ] || die 'usage: run.sh external-service-mode STATE_DIRECTORY {all|stun|relay-udp|relay-tcp}'
    set_external_service_mode "$2" "$3"
    ;;
  *)
    die 'usage: run.sh {start|stop STATE_DIRECTORY|admin STATE ACTION [ARG...]|restart STATE_DIRECTORY SERVICE|turn-mode STATE_DIRECTORY MODE|external-service-mode STATE_DIRECTORY MODE}'
    ;;
esac
