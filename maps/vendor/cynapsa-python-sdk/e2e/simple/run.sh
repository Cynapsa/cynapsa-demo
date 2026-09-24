#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SDK_ROOT=$(cd -- "$ROOT/../.." && pwd)
DEFAULT_GO_CORE=$(cd -- "$SDK_ROOT/../cynapsa/cynapsagocore" 2>/dev/null && pwd || true)
GO_CORE=${CYNAPSA_GO_CORE_PATH:-$DEFAULT_GO_CORE}
EJABBERD=${CYNAPSA_EJABBERD_PATH:-$SDK_ROOT/../cynapsa/ejabberd-remove-snapshot}
COMPOSE_FILE="$ROOT/compose.yaml"
ARTIFACT_ROOT="$ROOT/artifacts"
RUN_ID=$(date -u +%Y%m%dt%H%M%Sz)-$$
RUNTIME="$ROOT/.runtime/$RUN_ID"
ARTIFACTS="$ARTIFACT_ROOT/$RUN_ID"
PROJECT="cynapsa-simple-e2e-$RUN_ID"
IMAGE_TAG="$RUN_ID"
CORE_BASE=a21e4a23cf0c5b334e2818e2faa8b025d3d1e2b2
COTURN_IMAGE="coturn/coturn@sha256:aa68aab64a3b929d57fc2924c98ea447bf996cf8dade2508e7b71eaf23f1f14e"
TRUNK_PREFIX=${CYNAPSA_E2E_TRUNK_PREFIX:-}
CLIENTS=(native-sync-client native-async-client monkey-sync-client monkey-async-client)
USERS=(native-sync-client native-async-client monkey-sync-client monkey-async-client native-server monkey-server)
DEEP_EVIDENCE_NATIVE_TURN=0
DEEP_EVIDENCE_MONKEY_XMPP=0
DEEP_EVIDENCE_NATIVE_FAULT=0
DEEP_EVIDENCE_MONKEY_FAULT=0
DEEP_EVIDENCE_SERVER_RESUME=0
DEEP_EVIDENCE_CLIENT_RESUME=0
NATIVE_NAT_DELTA=0
NATIVE_TURN_OUT_DELTA=0
NATIVE_TURN_IN_DELTA=0
MONKEY_XMPP_OUT_DELTA=0
MONKEY_XMPP_IN_DELTA=0
MONKEY_NON_XMPP_DENIED=0
BACKGROUND_PIDS=()

die() {
  printf 'simple-e2e: %s\n' "$*" >&2
  exit 1
}

if [[ -n ${CYNAPSA_E2E_CLIENT_FILTER:-} ]]; then
  IFS=, read -r -a requested_clients <<<"$CYNAPSA_E2E_CLIENT_FILTER"
  CLIENTS=()
  for requested_client in "${requested_clients[@]}"; do
    case "$requested_client" in
      native-sync-client|native-async-client|monkey-sync-client|monkey-async-client)
        CLIENTS+=("$requested_client")
        ;;
      *) die "unknown CYNAPSA_E2E_CLIENT_FILTER entry: $requested_client" ;;
    esac
  done
  [[ ${#CLIENTS[@]} -gt 0 ]] || die "CYNAPSA_E2E_CLIENT_FILTER selected no clients"
fi

if [[ ${CYNAPSA_E2E_FOCUSED:-0} == 1 ]]; then
  CLIENTS=(native-sync-client native-async-client)
fi

require() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

choose_trunk_prefix() {
  if [[ -n "$TRUNK_PREFIX" ]]; then
    printf '%s\n' "$TRUNK_PREFIX"
    return 0
  fi
  local first second third candidate network
  for first in 198 100 172; do
    for second in {0..255}; do
      case "$first.$second" in
        172.2[4-9]|172.3[0-1]|100.6[4-9]|100.[7-9][0-9]|100.1[0-1][0-9]|100.12[0-7]|198.18|198.19) ;;
        *) continue ;;
      esac
      for third in {0..255}; do
        candidate="$first.$second.$third"
        network="cynapsa-simple-e2e-prefix-probe-$RUN_ID-$first-$second-$third"
        if docker network create --internal --subnet "$candidate.0/24" "$network" >/dev/null 2>&1; then
          docker network rm "$network" >/dev/null 2>&1 || true
          printf '%s\n' "$candidate"
          return 0
        fi
      done
    done
  done
  die "could not find a free Docker trunk /24; set CYNAPSA_E2E_TRUNK_PREFIX explicitly"
}

compose() {
  docker compose --project-name "$PROJECT" --env-file "$RUNTIME/credentials.env" -f "$COMPOSE_FILE" "$@"
}

ejabberdctl() {
  compose exec -T ejabberd ejabberdctl "$@"
}

client_sidecars() {
  local client
  printf 'native-sync-client-net\n'
  for client in "${CLIENTS[@]}"; do
    if [[ "$client" != native-sync-client ]]; then
      printf '%s-net\n' "$client"
    fi
  done
}

timeline() {
  mkdir -p "$RUNTIME"
  printf '%s\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"$RUNTIME/timeline.log"
}

sanitize_file() {
  local source=$1
  local target=$2
  if [[ ! -f "$source" ]]; then
    return 0
  fi
  python3 - "$RUNTIME/credentials.env" "$source" "$target" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

env_path = Path(sys.argv[1])
source = Path(sys.argv[2])
target = Path(sys.argv[3])
secrets: list[str] = []
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if value and (key.endswith("_PASSWORD") or key in {"CYNAPSA_TURN_STATIC_AUTH_SECRET"}):
            secrets.append(value)
for candidate in ("coturn-turn.conf", "ejabberd.yml"):
    path = env_path.parent / candidate
    if path.exists():
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("static-auth-secret="):
                _, value = line.split("=", 1)
                if value:
                    secrets.append(value.strip().strip('"'))
            elif line.lstrip().startswith("secret:"):
                _, value = line.split(":", 1)
                if value:
                    secrets.append(value.strip().strip('"'))
secrets = sorted(set(secrets), key=len, reverse=True)
text = source.read_text(encoding="utf-8", errors="replace")
for secret in secrets:
    if secret:
        text = text.replace(secret, "[REDACTED]")
target.write_text(text, encoding="utf-8")
PY
}

capture_artifact() {
  local source=$1
  local target=$2
  sanitize_file "$source" "$ARTIFACTS/$target"
}

assert_artifacts_sanitized() {
  python3 - "$RUNTIME/credentials.env" "$ARTIFACTS" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

env_path = Path(sys.argv[1])
artifact_root = Path(sys.argv[2])
secrets: list[bytes] = []
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if value and (key.endswith("_PASSWORD") or key == "CYNAPSA_TURN_STATIC_AUTH_SECRET"):
            secrets.append(value.encode())
for path in artifact_root.rglob("*"):
    if not path.is_file():
        continue
    data = path.read_bytes()
    for secret in secrets:
        if secret in data:
            raise SystemExit(f"runtime secret leaked into artifact: {path}")
PY
}

capture() {
  mkdir -p "$ARTIFACTS"
  if [[ -n "$(compose ps -q ejabberd 2>/dev/null || true)" ]]; then
    printf '%s\n' 'P = gen_mod:get_module_proc(<<"mesh.test">>, mod_cynapsa_mesh), S = sys:get_state(P), Ready = lists:sort([{U,M} || {_Pid,{_SID,U,M}} <- maps:to_list(element(3,S))]), Pending = map_size(element(5,S)), Terminal = map_size(element(8,S)), Rows = lists:append([mnesia:dirty_read(cynapsa_mesh_mailbox,K) || K <- mnesia:dirty_all_keys(cynapsa_mesh_mailbox)]), Owners = lists:sort([{element(3,R),element(4,R)} || R <- Rows]), io:format("ready=~p~npending_sync=~B~nterminal_sync=~B~nmailbox=~p~n", [Ready,Pending,Terminal,Owners]), ok.' |
      compose exec -T ejabberd su-exec ejabberd /opt/ejabberd-26.04/erts-16.3.1/bin/erl_call \
        -e -n ejabberd@localhost -fetch_stdout -no_result_term \
        >"$RUNTIME/ejabberd-mesh-state.txt.raw" 2>&1 || true
    capture_artifact "$RUNTIME/ejabberd-mesh-state.txt.raw" ejabberd-mesh-state.txt
    if authority_trace_available; then
      capture_authority_trace || true
    fi
  fi
  if compose logs --no-color >"$RUNTIME/compose.log.raw" 2>&1; then
    capture_artifact "$RUNTIME/compose.log.raw" compose.log
  fi
  if compose ps --all --format json >"$RUNTIME/compose-ps.jsonl.raw" 2>&1; then
    capture_artifact "$RUNTIME/compose-ps.jsonl.raw" compose-ps.jsonl
  fi
  local services=(router ejabberd-net stun-net turn-net native-server-net monkey-server-net)
  local client service
  while IFS= read -r service; do
    services+=("$service")
  done < <(client_sidecars)
  for service in "${services[@]}"; do
    if [[ -n "$(compose ps -q "$service" 2>/dev/null || true)" ]]; then
      compose exec -T "$service" sh -c \
        'id; ip -brief address; ip -d link show type vlan; ip route; ss -H -lunpt 2>/dev/null || true; sysctl net.ipv4.ip_forward net.ipv4.conf.all.send_redirects net.ipv4.conf.default.send_redirects 2>/dev/null || true; iptables -nvxL INPUT; iptables -nvxL OUTPUT; iptables-save -c' \
        >"$RUNTIME/$service-network.txt.raw" 2>&1 || true
      capture_artifact "$RUNTIME/$service-network.txt.raw" "$service-network.txt"
    fi
  done
  for client in "${CLIENTS[@]}"; do
    if [[ -f "$RUNTIME/$client.log" ]]; then
      capture_artifact "$RUNTIME/$client.log" "$client.log"
    fi
  done
  for service in native-server monkey-server ejabberd stun turn; do
    if [[ -f "$RUNTIME/$service.log" ]]; then
      capture_artifact "$RUNTIME/$service.log" "$service.log"
    elif [[ -n "$(compose ps -q "$service" 2>/dev/null || true)" ]]; then
      compose logs --no-color "$service" >"$RUNTIME/$service.log.raw" 2>&1 || true
      capture_artifact "$RUNTIME/$service.log.raw" "$service.log"
    fi
  done
  for file in summary.json timeline.log deep-evidence.txt core-provenance.json ejabberd-full-sessions.txt authority-trace.txt native-async-router-counters.txt native-async-turn-delta.log native-async-network-delay.txt native-async-turn-warmup-before.txt native-async-turn-warmup-after.txt native-async-turn-warmup-evidence.txt monkey-async-router-counters.txt native-server-response-qdisc.txt native-server-recovery-response-qdisc.txt native-server-fault-counters.txt native-server-fault-socket-state.txt native-server-fault-response-qdisc.txt monkey-async-client-fault-counters.txt monkey-async-client-fault-socket-state.txt monkey-async-client-fault-response-qdisc.txt monkey-async-rank2-evidence.txt short-resume-monkey-server-evidence.txt short-resume-monkey-server-counters.txt short-resume-monkey-sync-client-evidence.txt short-resume-monkey-sync-client-counters.txt monkey-server-short-resume-delay.txt; do
    if [[ -f "$RUNTIME/$file" ]]; then
      capture_artifact "$RUNTIME/$file" "$file"
    fi
  done
}

cleanup() {
  status=${1:-$?}
  trap - EXIT INT TERM
  set +e
  local pid
  for pid in "${BACKGROUND_PIDS[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  for pid in "${BACKGROUND_PIDS[@]:-}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
  capture
  assert_artifacts_sanitized
  compose down --volumes --remove-orphans --timeout 10 >/dev/null 2>&1 || true
  local container network
  for container in $(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT"); do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  for network in $(docker network ls -q --filter "label=com.docker.compose.project=$PROJECT"); do
    docker network rm "$network" >/dev/null 2>&1 || true
  done
  docker image rm \
    "cynapsa-simple-e2e-agent:$IMAGE_TAG" \
    "cynapsa-simple-e2e-ejabberd:$IMAGE_TAG" \
    "cynapsa-simple-e2e-router:$IMAGE_TAG" \
    "cynapsa-simple-e2e-sidecar:$IMAGE_TAG" >/dev/null 2>&1 || true
  rm -rf "$RUNTIME"
  if [[ $status -ne 0 ]]; then
    printf 'simple-e2e: failed; artifacts: %s\n' "$ARTIFACTS" >&2
  fi
  exit "$status"
}

wait_healthy() {
  local service=$1
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    local container health
    container=$(compose ps -q "$service" 2>/dev/null || true)
    if [[ -n "$container" ]]; then
      health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || true)
      if [[ "$health" == healthy ]]; then
        return 0
      fi
      if [[ "$health" == exited || "$health" == unhealthy ]]; then
        compose logs --no-color "$service" >&2 || true
        die "$service entered $health state"
      fi
    fi
    sleep 2
  done
  compose logs --no-color "$service" >&2 || true
  die "$service readiness timed out"
}

router_sh() {
  compose exec -T router sh -c "$1"
}

service_sh() {
  local service=$1
  shift
  compose exec -T "$service" sh -c "$*"
}

iptables_counter_field() {
  local file=$1
  local comment=$2
  local field=$3
  awk -v comment="$comment" -v field="$field" '
    index($0, "--comment " comment) {
      gsub(/^\[/, "", $1)
      gsub(/\]$/, "", $1)
      split($1, parts, ":")
      total += parts[field]
    }
    END { print total + 0 }
  ' "$file"
}

iptables_counter() {
  iptables_counter_field "$1" "$2" 1
}

iptables_byte_counter() {
  iptables_counter_field "$1" "$2" 2
}

assert_counter_positive() {
  local file=$1
  local comment=$2
  local value
  value=$(iptables_counter "$file" "$comment")
  if [[ ${value:-0} -le 0 ]]; then
    die "iptables counter $comment was not positive in $file"
  fi
}

assert_fault_direction_positive() {
  local file=$1
  local label=$2
  local direction=$3
  local namespace_count router_count
  namespace_count=$(iptables_counter "$file" "e2e-$label-fault-drop-$direction")
  router_count=$(iptables_counter "$file" "e2e-$label-router-drop-$direction")
  if [[ $((namespace_count + router_count)) -le 0 ]]; then
    die "fault $label had no $direction DROP packets in namespace or router"
  fi
}

capture_router_counters() {
  local target=$1
  router_sh 'iptables-save -c' >"$RUNTIME/$target" 2>&1 || true
}

ejabberd_session_count() {
  local user=$1
  { compose logs --no-color ejabberd 2>/dev/null |
    grep -F "Opened c2s session for $user@mesh.test/simple-e2e" || true; } |
    wc -l |
    tr -d '[:space:]'
}

ejabberd_auth_count() {
  local user=$1
  ejabberd_log_count "Accepted c2s SCRAM-SHA-256-PLUS authentication for $user@mesh.test"
}

ejabberd_resume_pending_count() {
  local user=$1
  { compose logs --no-color ejabberd 2>/dev/null |
      grep -F "Closing c2s connection for $user@mesh.test/simple-e2e:" |
      grep -E "waiting [0-9]+ seconds for stream resumption" || true; } |
    wc -l |
    tr -d '[:space:]'
}

ejabberd_resume_count() {
  local user=$1
  ejabberd_log_count "Resumed session for $user@mesh.test/simple-e2e"
}

ejabberd_resume_expiry_count() {
  local user=$1
  ejabberd_log_count \
    "Closing c2s session for $user@mesh.test/simple-e2e: Stream closed by local host: Timed out waiting for stream resumption"
}

# Passive VM call tracing gives the harness deterministic protocol counters
# without adding a test command to either the agents or the production module.
start_authority_trace() {
  compose exec -T ejabberd su-exec ejabberd \
    /opt/ejabberd-26.04/erts-16.3.1/bin/erl_call \
    -e -n ejabberd@localhost -fetch_stdout -no_result_term \
    <"$EJABBERD/test/sdk-e2e/authority-trace-start.eval" 2>/dev/null |
    grep -Fxq ready || die "could not start ejabberd authority trace"
}

authority_trace_count() {
  local key=$1
  local value
  value=$(printf '%s\n' "case ets:lookup(cynapsa_e2e_authority_trace_counts, $key) of [{_, N}] -> io:format(\"~B~n\", [N]); _ -> io:format(\"missing~n\") end, ok." |
    compose exec -T ejabberd su-exec ejabberd \
      /opt/ejabberd-26.04/erts-16.3.1/bin/erl_call \
      -e -n ejabberd@localhost -fetch_stdout -no_result_term 2>/dev/null |
    tail -1)
  [[ $value =~ ^[0-9]+$ ]] || die "authority trace counter $key is unavailable: ${value:-empty}"
  printf '%s\n' "$value"
}

capture_authority_trace() {
  local key
  : >"$RUNTIME/authority-trace.txt"
  for key in authority_discovery authority_snapshot resume_hook resume_authority_ready resume_authority_not_ready; do
    printf '%s=%s\n' "$key" "$(authority_trace_count "$key")" >>"$RUNTIME/authority-trace.txt"
  done
}

authority_trace_available() {
  printf '%s\n' 'io:format("~p~n", [is_pid(whereis(cynapsa_e2e_authority_trace)) andalso ets:info(cynapsa_e2e_authority_trace_counts) =/= undefined]), ok.' |
    compose exec -T ejabberd su-exec ejabberd \
      /opt/ejabberd-26.04/erts-16.3.1/bin/erl_call \
      -e -n ejabberd@localhost -fetch_stdout -no_result_term 2>/dev/null |
    grep -Fxq true
}

wait_for_resource_ready() {
  local user=$1
  local deadline=$((SECONDS + 25))
  while (( SECONDS < deadline )); do
    if ejabberd_resource_ready "$user"; then
      return 0
    fi
    sleep 0.2
  done
  die "$user did not become authority-ready"
}

ejabberd_resource_ready() {
  local user=$1
  printf '%s\n' "P = gen_mod:get_module_proc(<<\"mesh.test\">>, mod_cynapsa_mesh), S = sys:get_state(P), Ready = element(3,S), Found = lists:any(fun({_Pid,{_SID,U,M}}) -> U =:= <<\"$user\">> andalso M =:= <<\"simple-e2e\">> end, maps:to_list(Ready)), io:format(\"~p~n\", [Found]), ok." |
    compose exec -T ejabberd su-exec ejabberd /opt/ejabberd-26.04/erts-16.3.1/bin/erl_call \
      -e -n ejabberd@localhost -fetch_stdout -no_result_term 2>/dev/null |
    grep -Fxq true
}

wait_for_resource_quiescent() {
  local user=$1
  local quiet_seconds=${2:-12}
  local deadline=$((SECONDS + 45)) stable_since=$SECONDS auth sessions next_auth next_sessions
  auth=$(ejabberd_auth_count "$user")
  sessions=$(ejabberd_session_count "$user")
  while (( SECONDS < deadline )); do
    next_auth=$(ejabberd_auth_count "$user")
    next_sessions=$(ejabberd_session_count "$user")
    if ! ejabberd_resource_ready "$user" || [[ $next_auth != "$auth" || $next_sessions != "$sessions" ]]; then
      stable_since=$SECONDS
      auth=$next_auth
      sessions=$next_sessions
    elif (( SECONDS - stable_since >= quiet_seconds )); then
      timeline "$user recovered resource quiescent sessions=$sessions auth=$auth"
      return 0
    fi
    sleep 0.5
  done
  die "$user did not remain resource-ready and session-stable for ${quiet_seconds}s"
}

ejabberd_log_count() {
  local pattern=$1
  { compose logs --no-color ejabberd 2>/dev/null | grep -F "$pattern" || true; } |
    wc -l |
    tr -d '[:space:]'
}

wait_for_count_above() {
  local description=$1
  local baseline=$2
  shift 2
  local deadline=$((SECONDS + 25)) value
  while (( SECONDS < deadline )); do
    value=$("$@")
    if [[ ${value:-0} -gt $baseline ]]; then
      printf '%s\n' "$value"
      return 0
    fi
    sleep 0.2
  done
  die "$description did not increase above $baseline"
}

install_native_async_turn_rules() {
  timeline "native-async network-forced TURN rules install"
  router_sh '
    iptables -t nat -N E2E_NATIVE_ASYNC_NAT 2>/dev/null || true
    iptables -N E2E_NATIVE_ASYNC_OUT 2>/dev/null || true
    iptables -N E2E_NATIVE_ASYNC_IN 2>/dev/null || true
    iptables -t nat -F E2E_NATIVE_ASYNC_NAT
    iptables -F E2E_NATIVE_ASYNC_OUT
    iptables -F E2E_NATIVE_ASYNC_IN
    iptables -t nat -C POSTROUTING -s 10.240.20.2 -p udp -j E2E_NATIVE_ASYNC_NAT 2>/dev/null ||
      iptables -t nat -I POSTROUTING 1 -s 10.240.20.2 -p udp -j E2E_NATIVE_ASYNC_NAT
    iptables -C FORWARD -s 10.240.20.2 -j E2E_NATIVE_ASYNC_OUT 2>/dev/null ||
      iptables -I FORWARD 1 -s 10.240.20.2 -j E2E_NATIVE_ASYNC_OUT
    iptables -C FORWARD -d 10.240.20.2 -j E2E_NATIVE_ASYNC_IN 2>/dev/null ||
      iptables -I FORWARD 1 -d 10.240.20.2 -j E2E_NATIVE_ASYNC_IN
    iptables -t nat -A E2E_NATIVE_ASYNC_NAT -m comment --comment e2e-native-async-nat -j MASQUERADE
    iptables -A E2E_NATIVE_ASYNC_OUT -p udp -d 10.240.80.2 --dport 3478 -m comment --comment e2e-native-async-stun-out -j RETURN
    iptables -A E2E_NATIVE_ASYNC_IN -p udp -s 10.240.80.2 --sport 3478 -m comment --comment e2e-native-async-stun-in -j RETURN
    iptables -A E2E_NATIVE_ASYNC_OUT -p udp -d 10.240.90.2 --dport 3478 -m u32 --u32 "0>>22&0x3C@8>>30&0x3=1" -m comment --comment e2e-native-async-turn-channeldata-out
    iptables -A E2E_NATIVE_ASYNC_IN -p udp -s 10.240.90.2 --sport 3478 -m u32 --u32 "0>>22&0x3C@8>>30&0x3=1" -m comment --comment e2e-native-async-turn-channeldata-in
    iptables -A E2E_NATIVE_ASYNC_OUT -p udp -d 10.240.90.2 --dport 3478 -m comment --comment e2e-native-async-turn-out -j RETURN
    iptables -A E2E_NATIVE_ASYNC_IN -p udp -s 10.240.90.2 --sport 3478 -m comment --comment e2e-native-async-turn-in -j RETURN
    iptables -A E2E_NATIVE_ASYNC_OUT -p udp -d 10.240.50.0/24 -m comment --comment e2e-native-async-direct-out -j DROP
    iptables -A E2E_NATIVE_ASYNC_OUT -p udp -d 10.240.60.0/24 -m comment --comment e2e-native-async-direct-out -j DROP
    iptables -A E2E_NATIVE_ASYNC_OUT -p udp -m comment --comment e2e-native-async-other-udp-out -j DROP
    iptables -A E2E_NATIVE_ASYNC_OUT -j RETURN
    iptables -A E2E_NATIVE_ASYNC_IN -p udp -s 10.240.50.0/24 -m comment --comment e2e-native-async-direct-in -j DROP
    iptables -A E2E_NATIVE_ASYNC_IN -p udp -s 10.240.60.0/24 -m comment --comment e2e-native-async-direct-in -j DROP
    iptables -A E2E_NATIVE_ASYNC_IN -p udp -m comment --comment e2e-native-async-other-udp-in -j DROP
    iptables -A E2E_NATIVE_ASYNC_IN -j RETURN
  '
}

install_monkey_async_xmpp_rules() {
  timeline "monkey-async xmpp-only rules install"
  router_sh '
    iptables -N E2E_MONKEY_ASYNC_OUT 2>/dev/null || true
    iptables -N E2E_MONKEY_ASYNC_IN 2>/dev/null || true
    iptables -F E2E_MONKEY_ASYNC_OUT
    iptables -F E2E_MONKEY_ASYNC_IN
    iptables -C FORWARD -s 10.240.40.2 -j E2E_MONKEY_ASYNC_OUT 2>/dev/null ||
      iptables -I FORWARD 1 -s 10.240.40.2 -j E2E_MONKEY_ASYNC_OUT
    iptables -C FORWARD -d 10.240.40.2 -j E2E_MONKEY_ASYNC_IN 2>/dev/null ||
      iptables -I FORWARD 1 -d 10.240.40.2 -j E2E_MONKEY_ASYNC_IN
    iptables -A E2E_MONKEY_ASYNC_OUT -p tcp -d 10.240.70.2 --dport 5222 -m comment --comment e2e-monkey-async-xmpp-out -j ACCEPT
    iptables -A E2E_MONKEY_ASYNC_OUT -m comment --comment e2e-monkey-async-non-xmpp-out -j REJECT
    iptables -A E2E_MONKEY_ASYNC_IN -p tcp -s 10.240.70.2 --sport 5222 -m conntrack --ctstate ESTABLISHED,RELATED -m comment --comment e2e-monkey-async-xmpp-in -j ACCEPT
    iptables -A E2E_MONKEY_ASYNC_IN -m comment --comment e2e-monkey-async-non-xmpp-in -j REJECT
  '
}

blackhole_service() {
  local service=$1
  local label=$2
  local fault_ip established_before_blackhole tcp_retries2 state
  case "$service" in
    native-sync-client-net) fault_ip=10.240.10.2 ;;
    native-async-client-net) fault_ip=10.240.20.2 ;;
    monkey-sync-client-net) fault_ip=10.240.30.2 ;;
    monkey-async-client-net) fault_ip=10.240.40.2 ;;
    native-server-net) fault_ip=10.240.50.2 ;;
    monkey-server-net) fault_ip=10.240.60.2 ;;
    *) die "$label has no fixed LAN address for $service" ;;
  esac
  # Router drops catch packets which already passed the service's OUTPUT hook
  # and are waiting in its fixed response-delay qdisc. Namespace-local drops
  # then isolate all newly generated traffic in both directions.
  router_sh "
    iptables -I FORWARD 1 -s '$fault_ip' -m comment --comment e2e-$label-router-drop-out -j DROP
    iptables -I FORWARD 1 -d '$fault_ip' -m comment --comment e2e-$label-router-drop-in -j DROP
  "
  if ! state=$(service_sh "$service" "
    set -eu
    iptables -I INPUT 1 -i cynapsa-lan -m comment --comment e2e-$label-fault-drop-in -j DROP
    iptables -I OUTPUT 1 -o cynapsa-lan -m comment --comment e2e-$label-fault-drop-out -j DROP
    sockets=\$(ss -Hnt state established \"( dst 10.240.70.2 and dport = :5222 )\")
    if [ -z \"\$sockets\" ]; then established=0; else established=\$(printf '%s\\n' \"\$sockets\" | awk 'END { print NR }'); fi
    printf '%s\\t%s\\n' \"\$established\" \"\$(sysctl -n net.ipv4.tcp_retries2)\"
  "); then
    die "$label could not install local blackhole and inspect its XMPP socket"
  fi
  IFS=$'\t' read -r established_before_blackhole tcp_retries2 <<<"$state"
  [[ $established_before_blackhole =~ ^[0-9]+$ ]] ||
    die "$label returned an invalid pre-blackhole local XMPP socket count: ${established_before_blackhole:-empty}"
  [[ $tcp_retries2 =~ ^[0-9]+$ ]] ||
    die "$label returned an invalid TCP retry policy: ${tcp_retries2:-empty}"
  printf 'tcp_retries2=%s\nestablished_before_blackhole=%s\n' \
    "$tcp_retries2" "$established_before_blackhole" \
    >"$RUNTIME/$label-socket-state.txt"
  # Local TCP state is diagnostic only. Ejabberd's exact-resource lifecycle is
  # the authoritative stream evidence, so a zero count cannot veto this fault.
  timeline "$label blackhole install local_established=$established_before_blackhole"
  service_sh "$service" 'ping -c 1 -W 1 "$CYNAPSA_LAN_GATEWAY" >/dev/null 2>&1 || true'
  router_sh "ping -c 1 -W 1 '$fault_ip' >/dev/null 2>&1 || true"
}

diagnostic_xmpp_socket_count() {
  local service=$1
  service_sh "$service" '
    sockets=$(ss -Hnt state established "( dst 10.240.70.2 and dport = :5222 )") || exit $?
    if [ -z "$sockets" ]; then
      printf "0\n"
    else
      printf "%s\n" "$sockets" | awk "END { print NR }"
    fi
  '
}

record_fault_socket_state() {
  local service=$1
  local label=$2
  local established_during_blackhole
  if ! established_during_blackhole=$(diagnostic_xmpp_socket_count "$service"); then
    die "$label could not record its local XMPP socket state during the blackhole"
  fi
  established_during_blackhole=${established_during_blackhole//[[:space:]]/}
  if [[ ! $established_during_blackhole =~ ^[0-9]+$ ]]; then
    die "$label returned an invalid local XMPP socket count: ${established_during_blackhole:-empty}"
  fi
  # A silently blackholed endpoint may retain an ESTABLISHED TCP control block
  # after ejabberd has expired the authoritative XEP-0198 stream. Keep this as
  # timing diagnostics; it is not a stream-lifecycle invariant.
  printf 'established_during_blackhole=%s\n' "$established_during_blackhole" \
    >>"$RUNTIME/$label-socket-state.txt"
}

restore_service_network() {
  local service=$1
  local label=$2
  local fault_ip
  timeline "$label blackhole restore"
  fault_ip=$(service_sh "$service" 'printf %s "${CYNAPSA_LAN_ADDRESS%/*}"')
  service_sh "$service" "
    while iptables -D INPUT -i cynapsa-lan -m comment --comment e2e-$label-fault-drop-in -j DROP 2>/dev/null; do :; done
    while iptables -D OUTPUT -o cynapsa-lan -m comment --comment e2e-$label-fault-drop-out -j DROP 2>/dev/null; do :; done
  "
  router_sh "
    while iptables -D FORWARD -s '$fault_ip' -m comment --comment e2e-$label-router-drop-out -j DROP 2>/dev/null; do :; done
    while iptables -D FORWARD -d '$fault_ip' -m comment --comment e2e-$label-router-drop-in -j DROP 2>/dev/null; do :; done
  "
}

validated_network_delay_state() {
  local service=$1
  local delay=${2:-15s}
  service_sh "$service" "
    set -eu
    qdisc=\$(tc -s -d qdisc show dev cynapsa-lan)
    test \"\$(printf '%s\\n' \"\$qdisc\" | grep -c '^qdisc ')\" -eq 1
    printf '%s\\n' \"\$qdisc\" | grep -Eq '^qdisc netem 1: root( |\$)'
    printf '%s\\n' \"\$qdisc\" | grep -Fq 'limit 10000'
    printf '%s\\n' \"\$qdisc\" | grep -Fq 'delay $delay'
    printf 'qdisc:\\n%s\\n' \"\$qdisc\"
  "
}

record_network_delay_state() {
  local service=$1
  local delay=$2
  local target=${3:-}
  local phase=${4:-validated}
  local state
  if ! state=$(validated_network_delay_state "$service" "$delay"); then
    die "$service does not have the expected single FIFO netem delay"
  fi
  if [[ -n $target ]]; then
    {
      printf 'phase=%s service=%s expected_delay=%s\n' "$phase" "$service" "$delay"
      printf '%s\n' "$state"
    } >>"$RUNTIME/$target"
  fi
}

delay_service_network() {
  local service=$1
  local delay=${2:-15s}
  local evidence=${3:-}
  service_sh "$service" "
    set -eu
    tc qdisc del dev cynapsa-lan root 2>/dev/null || true
    tc qdisc add dev cynapsa-lan root handle 1: netem limit 10000 delay $delay
  "
  if [[ -n $evidence ]]; then
    : >"$RUNTIME/$evidence"
  fi
  record_network_delay_state "$service" "$delay" "$evidence" installed
}

change_service_network_delay() {
  local service=$1
  local delay=$2
  local evidence=${3:-}
  service_sh "$service" "
    set -eu
    tc qdisc change dev cynapsa-lan root handle 1: netem limit 10000 delay $delay
  "
  if [[ -n $evidence ]]; then
    : >"$RUNTIME/$evidence"
  fi
  record_network_delay_state "$service" "$delay" "$evidence" changed
}

clear_network_delay() {
  local service=$1
  service_sh "$service" 'tc qdisc del dev cynapsa-lan root 2>/dev/null || true'
}

pause_client_run_container() {
  local service=$1
  local deadline=$((SECONDS + 10)) container=
  while (( SECONDS < deadline )); do
    container=$(docker ps -q \
      --filter "label=com.docker.compose.project=$PROJECT" \
      --filter "label=com.docker.compose.service=$service" | head -1)
    [[ -n $container ]] && break
    sleep 0.05
  done
  [[ -n $container ]] || die "could not find running $service container to pause"
  docker pause "$container" >/dev/null
  [[ $(docker inspect -f '{{.State.Paused}}' "$container") == true ]] ||
    die "$service container did not enter paused state"
  PAUSED_CLIENT_CONTAINER=$container
  timeline "$service application container paused for qdisc transition"
}

unpause_client_run_container() {
  local service=$1
  [[ -n ${PAUSED_CLIENT_CONTAINER:-} ]] || die "$service has no paused application container"
  docker unpause "$PAUSED_CLIENT_CONTAINER" >/dev/null
  [[ $(docker inspect -f '{{.State.Paused}}' "$PAUSED_CLIENT_CONTAINER") == false ]] ||
    die "$service container remained paused"
  timeline "$service application container unpaused after qdisc transition"
  PAUSED_CLIENT_CONTAINER=
}

validated_xmpp_flow_delay_state() {
  local service=$1
  local delay=${2:-1s}
  service_sh "$service" "
    set -eu
    qdisc=\$(tc -s -d qdisc show dev cynapsa-lan)
    filters=\$(tc -s -d filter show dev cynapsa-lan parent 1:)
    marks=\$(iptables -t mangle -S OUTPUT)
    printf '%s\\n' \"\$qdisc\" | grep -Eq '^qdisc prio 1: root( |\$)'
    printf '%s\\n' \"\$qdisc\" | grep -Eq '^qdisc netem 10: parent 1:1( |\$)'
    printf '%s\\n' \"\$qdisc\" | grep -Fq 'limit 10000'
    printf '%s\\n' \"\$qdisc\" | grep -Fq 'delay $delay'
    printf '%s\\n' \"\$filters\" | grep -Eq '(classid|flowid) 1:1'
    printf '%s\\n' \"\$filters\" | grep -Fq 'flowid 1:2'
    printf '%s\\n' \"\$marks\" | grep -F -- '--dport 5222' | grep -Fq -- '--set-xmark 0xc1/0xffffffff'
    ! printf '%s\\n' \"\$marks\" | grep -Fq -- '--length'
    printf 'qdisc:\\n%s\\nfilters:\\n%s\\nmarks:\\n%s\\n' \"\$qdisc\" \"\$filters\" \"\$marks\"
  "
}

record_xmpp_flow_delay_state() {
  local service=$1
  local delay=$2
  local target=$3
  local phase=$4
  local state
  if ! state=$(validated_xmpp_flow_delay_state "$service" "$delay"); then
    die "$service does not have the expected complete-XMPP-flow delay"
  fi
  {
    printf 'phase=%s service=%s expected_delay=%s\\n' "$phase" "$service" "$delay"
    printf '%s\\n' "$state"
  } >>"$RUNTIME/$target"
}

delay_service_xmpp_flow() {
  local service=$1
  local delay=${2:-1s}
  local evidence=$3
  service_sh "$service" "
    set -eu
    tc qdisc del dev cynapsa-lan root 2>/dev/null || true
    while iptables -t mangle -D OUTPUT -o cynapsa-lan -p tcp --dport 5222 -m comment --comment e2e-xmpp-flow-delay -j MARK --set-mark 0xc1 2>/dev/null; do :; done
    iptables -t mangle -I OUTPUT 1 -o cynapsa-lan -p tcp --dport 5222 -m comment --comment e2e-xmpp-flow-delay -j MARK --set-mark 0xc1
    tc qdisc add dev cynapsa-lan root handle 1: prio
    tc qdisc add dev cynapsa-lan parent 1:1 handle 10: netem limit 10000 delay $delay
    tc filter add dev cynapsa-lan parent 1: protocol ip prio 1 handle 0xc1 fw flowid 1:1
    tc filter add dev cynapsa-lan parent 1: protocol all prio 2 matchall flowid 1:2
  "
  : >"$RUNTIME/$evidence"
  record_xmpp_flow_delay_state "$service" "$delay" "$evidence" installed
}

change_service_xmpp_flow_delay() {
  local service=$1
  local delay=$2
  local evidence=$3
  service_sh "$service" "
    set -eu
    tc qdisc change dev cynapsa-lan parent 1:1 handle 10: netem limit 10000 delay $delay
  "
  record_xmpp_flow_delay_state "$service" "$delay" "$evidence" changed
}

capture_service_filter() {
  local service=$1
  local target=$2
  {
    printf '%s\n' 'namespace:'
    service_sh "$service" 'iptables-save -c; iptables -nvxL INPUT; iptables -nvxL OUTPUT'
    printf '%s\n' 'router:'
    router_sh 'iptables-save -c; iptables -nvxL FORWARD'
  } >"$RUNTIME/$target" 2>&1 || true
}

wait_for_request_start() {
  local log=$1
  local target=$2
  local deadline=$((SECONDS + 25))
  while (( SECONDS < deadline )); do
    if [[ -f "$log" ]] && grep -F "\"event\": \"request-start\"" "$log" | grep -Fq "\"target\": \"$target\""; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

wait_for_request_failure() {
  local log=$1
  local target=$2
  local deadline=$((SECONDS + 10))
  while (( SECONDS < deadline )); do
    if [[ -f "$log" ]] && grep -F '"event": "request-fail"' "$log" | grep -Fq "\"target\": \"$target\""; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

service_dispatch_count() {
  local target=$1
  local service pattern
  case "$target" in
    native)
      service=native-server
      pattern='"event": "native-dispatch"'
      ;;
    monkey)
      service=monkey-server
      pattern='"event": "asgi-dispatch"'
      ;;
    *) die "unknown dispatch target: $target" ;;
  esac
  { compose logs --no-color "$service" 2>/dev/null | grep -F "$pattern" || true; } |
    wc -l |
    tr -d '[:space:]'
}

mesh_mailbox_admission_count() {
  local sender=$1
  local recipient=$2
  { compose logs --no-color ejabberd 2>/dev/null |
      grep -F 'cynapsa_mailbox admission ' |
      grep -F "sender=$sender@mesh.test/simple-e2e recipient=$recipient@mesh.test/simple-e2e" || true; } |
    wc -l |
    tr -d '[:space:]'
}

assert_faulted_request_elapsed() {
  local log=$1
  local client=$2
  local target=$3
  python3 - "$log" "$client" "$target" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
client = sys.argv[2].removesuffix("-client")
target = sys.argv[3]
events: list[dict[str, object]] = []
for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
    if not line.startswith("E2E_DIAGNOSTIC="):
        continue
    events.append(json.loads(line.removeprefix("E2E_DIAGNOSTIC=")))

passes = [
    event
    for event in events
    if event.get("client") == client
    and event.get("target") == target
    and event.get("event") == "request-pass"
]
matches = [
    event
    for event in passes
    if isinstance(event.get("elapsed_ms"), int)
    and 12_000 <= event["elapsed_ms"] < 60_000
]
if len(passes) != 10:
    raise SystemExit(
        f"expected ten successful {client}/{target} RPCs around the fault, got {len(passes)}"
    )
if len(matches) != 1:
    raise SystemExit(
        f"expected exactly one {client}/{target} RPC held through the fresh-auth outage, "
        f"got {[(event.get('message'), event.get('elapsed_ms')) for event in matches]!r}"
    )
message = matches[0].get("message")
if message not in {f"message-{index}" for index in range(10)}:
    raise SystemExit(f"fault-held {client}/{target} RPC has invalid message key {message!r}")
PY
}

request_event_count() {
  local log=$1
  local target=$2
  local event=$3
  python3 - "$log" "$target" "$event" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
target = sys.argv[2]
event_name = sys.argv[3]
count = 0
if path.exists():
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith("E2E_DIAGNOSTIC="):
            continue
        record = json.loads(line.removeprefix("E2E_DIAGNOSTIC="))
        if record.get("target") == target and record.get("event") == event_name:
            count += 1
print(count)
PY
}

request_message_event_count() {
  local log=$1
  local target=$2
  local message=$3
  local event=$4
  python3 - "$log" "$target" "$message" "$event" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
target = sys.argv[2]
message = sys.argv[3]
event_name = sys.argv[4]
count = 0
if path.exists():
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith("E2E_DIAGNOSTIC="):
            continue
        record = json.loads(line.removeprefix("E2E_DIAGNOSTIC="))
        if (
            record.get("target") == target
            and record.get("message") == message
            and record.get("event") == event_name
        ):
            count += 1
print(count)
PY
}

wait_for_request_after() {
  local log=$1
  local target=$2
  local event=$3
  local baseline=$4
  local deadline=$((SECONDS + 25)) message
  while (( SECONDS < deadline )); do
    if message=$(python3 - "$log" "$target" "$event" "$baseline" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
target = sys.argv[2]
event_name = sys.argv[3]
baseline = int(sys.argv[4])
matches = []
if path.exists():
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith("E2E_DIAGNOSTIC="):
            continue
        record = json.loads(line.removeprefix("E2E_DIAGNOSTIC="))
        if record.get("target") == target and record.get("event") == event_name:
            matches.append(record)
if len(matches) <= baseline:
    raise SystemExit(1)
message = matches[baseline].get("message")
if not isinstance(message, str) or not message:
    raise SystemExit(2)
print(message)
PY
    ); then
      printf '%s\n' "$message"
      return 0
    fi
    sleep 0.05
  done
  return 1
}

wait_for_exact_request_pass() {
  local log=$1
  local target=$2
  local message=$3
  local deadline=$((SECONDS + 20)) status
  while (( SECONDS < deadline )); do
    if python3 - "$log" "$target" "$message" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
target = sys.argv[2]
message = sys.argv[3]
if path.exists():
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith("E2E_DIAGNOSTIC="):
            continue
        record = json.loads(line.removeprefix("E2E_DIAGNOSTIC="))
        if record.get("target") != target or record.get("message") != message:
            continue
        if record.get("event") == "request-fail":
            raise SystemExit(2)
        if record.get("event") == "request-pass":
            raise SystemExit(0)
raise SystemExit(1)
PY
    then
      return 0
    else
      status=$?
      [[ $status -ne 2 ]] || return 1
    fi
    sleep 0.05
  done
  return 1
}

establish_native_async_turn_rpc() {
  local log=$1
  local target=$2
  local message=$3
  local request_index=$4
  local dispatch_before=$5
  local dispatch_after=$6
  local request_mailbox_before=$7
  local response_mailbox_before=$8
  local out_data_packets_before=$9
  local out_data_bytes_before=${10}
  local in_data_packets_before=${11}
  local in_data_bytes_before=${12}
  local request_mailbox_after response_mailbox_after
  local out_data_packets_after out_data_bytes_after in_data_packets_after in_data_bytes_after
  local request_turn_data_packets request_turn_data_bytes response_turn_data_packets response_turn_data_bytes
  local candidate=0

  while (( request_index <= 6 )); do
    capture_router_counters native-async-turn-warmup-after.txt
    request_mailbox_after=$(mesh_mailbox_admission_count native-async-client native-server)
    out_data_packets_after=$(iptables_counter "$RUNTIME/native-async-turn-warmup-after.txt" e2e-native-async-turn-channeldata-out)
    out_data_bytes_after=$(iptables_byte_counter "$RUNTIME/native-async-turn-warmup-after.txt" e2e-native-async-turn-channeldata-out)
    request_turn_data_packets=$((out_data_packets_after - out_data_packets_before))
    request_turn_data_bytes=$((out_data_bytes_after - out_data_bytes_before))

    # A native-server dispatch with no matching exact-resource mailbox
    # admission can only have arrived over Rank1. Core uses its ordinary `all`
    # ICE policy, while the router denies every host/srflx/direct UDP path and
    # permits TURN. The u32 counter matches TURN ChannelData (top bits 01), not
    # STUN/TURN allocation, refresh, permission, or channel-bind chatter.
    if [[ $request_mailbox_after -eq $request_mailbox_before && \
          $request_turn_data_packets -gt 0 && $request_turn_data_bytes -gt 0 ]]; then
      candidate=1
    fi

    wait_for_exact_request_pass "$log" "$target" "$message" ||
      die "native-async TURN warm-up RPC $message did not succeed"
    capture_router_counters native-async-turn-warmup-after.txt
    response_mailbox_after=$(mesh_mailbox_admission_count native-server native-async-client)
    in_data_packets_after=$(iptables_counter "$RUNTIME/native-async-turn-warmup-after.txt" e2e-native-async-turn-channeldata-in)
    in_data_bytes_after=$(iptables_byte_counter "$RUNTIME/native-async-turn-warmup-after.txt" e2e-native-async-turn-channeldata-in)
    response_turn_data_packets=$((in_data_packets_after - in_data_packets_before))
    response_turn_data_bytes=$((in_data_bytes_after - in_data_bytes_before))

    if [[ $candidate -eq 1 && \
          $dispatch_after -eq $((dispatch_before + 1)) && \
          $response_mailbox_after -eq $response_mailbox_before && \
          $response_turn_data_packets -gt 0 && $response_turn_data_bytes -gt 0 ]]; then
      NATIVE_TURN_WARMUP_INDEX=$request_index
      NATIVE_TURN_WARMUP_DISPATCH=$dispatch_after
      {
        printf 'client=native-async\n'
        printf 'target=native\n'
        printf 'message=%s\n' "$message"
        printf 'request_result=pass\n'
        printf 'native_dispatch_delta=%s\n' "$((dispatch_after - dispatch_before))"
        printf 'request_rank2_mailbox_admission_delta=0\n'
        printf 'response_rank2_mailbox_admission_delta=0\n'
        printf 'standalone_stun_role=advertised_discovery_only\n'
        printf 'core_ice_policy=all\n'
        printf 'connectivity_and_data_path=turn_relay_forced_by_network\n'
        printf 'turn_channeldata_egress_packets=%s\n' "$request_turn_data_packets"
        printf 'turn_channeldata_egress_bytes=%s\n' "$request_turn_data_bytes"
        printf 'turn_channeldata_ingress_packets=%s\n' "$response_turn_data_packets"
        printf 'turn_channeldata_ingress_bytes=%s\n' "$response_turn_data_bytes"
      } >"$RUNTIME/native-async-turn-warmup-evidence.txt"
      timeline "native-async exact TURN ChannelData warm-up pass message=$message index=$request_index out_packets=$request_turn_data_packets in_packets=$response_turn_data_packets"
      return 0
    fi

    candidate=0
    request_index=$((request_index + 1))
    (( request_index <= 6 )) || break
    cp "$RUNTIME/native-async-turn-warmup-after.txt" "$RUNTIME/native-async-turn-warmup-before.txt"
    request_mailbox_before=$request_mailbox_after
    response_mailbox_before=$response_mailbox_after
    out_data_packets_before=$(iptables_counter "$RUNTIME/native-async-turn-warmup-before.txt" e2e-native-async-turn-channeldata-out)
    out_data_bytes_before=$(iptables_byte_counter "$RUNTIME/native-async-turn-warmup-before.txt" e2e-native-async-turn-channeldata-out)
    in_data_packets_before=$(iptables_counter "$RUNTIME/native-async-turn-warmup-before.txt" e2e-native-async-turn-channeldata-in)
    in_data_bytes_before=$(iptables_byte_counter "$RUNTIME/native-async-turn-warmup-before.txt" e2e-native-async-turn-channeldata-in)
    dispatch_before=$dispatch_after
    message=$(wait_for_request_after "$log" "$target" request-start "$request_index") ||
      die "native-async bounded TURN warm-up request $request_index did not start"
    dispatch_after=$(wait_for_count_above "$target TURN warm-up dispatch" "$dispatch_before" service_dispatch_count "$target")
  done

  die "native-async did not produce an exact bidirectional TURN RPC within seven ordinary requests"
}

run_client_plain() {
  local client=$1
  compose --profile clients run --rm --no-deps "$client" >"$RUNTIME/$client.log" 2>&1
}

run_client_with_short_resume() {
  local client=$1
  local target=$2
  local fault_service=$3
  local label=$4
  local user=$5
  local before_sessions before_auth before_pending before_resumed before_expired
  local before_discovery before_snapshot before_resume_hook before_ready before_not_ready
  local after_sessions after_auth after_pending after_resumed after_expired
  local after_discovery after_snapshot after_resume_hook after_ready after_not_ready
  local dispatch_before dispatch_after request_message request_baseline client_pid
  local initial_session_delta initial_auth_delta recovery_auth_delta

  before_sessions=$(ejabberd_session_count "$user")
  before_auth=$(ejabberd_auth_count "$user")
  dispatch_before=$(service_dispatch_count "$target")
  timeline "$client start with $label short-resume campaign"
  set +e
  run_client_plain "$client" &
  client_pid=$!
  BACKGROUND_PIDS+=("$client_pid")
  set -e
  initial_session_delta=0
  initial_auth_delta=0
  if [[ $before_sessions -eq 0 ]]; then
    wait_for_count_above "$user initial c2s session" "$before_sessions" \
      ejabberd_session_count "$user" >/dev/null
    initial_session_delta=1
    initial_auth_delta=1
  else
    ejabberd_resource_ready "$user" || die "$label faulted server was not authority-ready"
  fi
  wait_for_resource_ready "$user"
  # Authority trace counters are global.  The active client has only just
  # started, so wait for its initial bound resource to cross the authority
  # barrier before taking the campaign baseline as well.  This is redundant
  # when the client itself is the fault target, and intentionally does not
  # change the fault target's initial-session accounting above.
  wait_for_resource_ready "$client"

  # Baseline authority and stream lifecycle before the target request starts.
  # Both clients first execute ten 500-millisecond-delayed native-server RPCs, which
  # gives this environment-only observer deterministic setup time without any
  # application coordination or fault awareness.
  before_pending=$(ejabberd_resume_pending_count "$user")
  before_resumed=$(ejabberd_resume_count "$user")
  before_expired=$(ejabberd_resume_expiry_count "$user")
  before_discovery=$(authority_trace_count authority_discovery)
  before_snapshot=$(authority_trace_count authority_snapshot)
  before_resume_hook=$(authority_trace_count resume_hook)
  before_ready=$(authority_trace_count resume_authority_ready)
  before_not_ready=$(authority_trace_count resume_authority_not_ready)
  request_baseline=$(request_event_count "$RUNTIME/$client.log" "$target" request-start)
  dispatch_before=$(service_dispatch_count "$target")
  request_message=$(wait_for_request_after "$RUNTIME/$client.log" "$target" request-start "$request_baseline") ||
    die "$label client did not start its target RPC"
  dispatch_after=$(wait_for_count_above "$target server dispatch" "$dispatch_before" \
    service_dispatch_count "$target")
  timeline "$label in-flight RPC dispatch observed message=$request_message dispatch=$dispatch_after"

  blackhole_service "$fault_service" "$label"
  after_pending=$(wait_for_count_above "$user resumable-stream transition" \
    "$before_pending" ejabberd_resume_pending_count "$user")
  timeline "$label ejabberd resumable-stream transition count=$after_pending"
  capture_service_filter "$fault_service" "$label-counters.txt"
  assert_fault_direction_positive "$RUNTIME/$label-counters.txt" "$label" in
  assert_fault_direction_positive "$RUNTIME/$label-counters.txt" "$label" out
  [[ $(request_message_event_count "$RUNTIME/$client.log" "$target" "$request_message" request-pass) -eq 0 ]] ||
    die "$label selected RPC completed before the interrupted stream became resumable"

  # The transition above starts ejabberd's fifteen-second resume window. Restore
  # immediately on that semantic edge; successful resume plus zero expiry is
  # the authoritative proof that the campaign stayed inside the window.
  restore_service_network "$fault_service" "$label"
  after_resumed=$(wait_for_count_above "$user successful XEP-0198 resume" \
    "$before_resumed" ejabberd_resume_count "$user")
  after_ready=$(wait_for_count_above "$user ready resume-authority result" \
    "$before_ready" authority_trace_count resume_authority_ready)
  timeline "$label resumed count=$after_resumed authority_ready=$after_ready"

  if ! wait "$client_pid"; then
    BACKGROUND_PIDS=()
    printf 'simple-e2e: %s failed during %s\n' "$client" "$label" >&2
    sed -n '1,240p' "$RUNTIME/$client.log" >&2
    return 1
  fi
  BACKGROUND_PIDS=()

  after_sessions=$(ejabberd_session_count "$user")
  after_auth=$(ejabberd_auth_count "$user")
  after_expired=$(ejabberd_resume_expiry_count "$user")
  after_discovery=$(authority_trace_count authority_discovery)
  after_snapshot=$(authority_trace_count authority_snapshot)
  after_resume_hook=$(authority_trace_count resume_hook)
  after_not_ready=$(authority_trace_count resume_authority_not_ready)
  [[ $after_sessions -eq $((before_sessions + initial_session_delta)) ]] ||
    die "$label opened an unexpected replacement full session"
  recovery_auth_delta=$((after_auth - before_auth - initial_auth_delta))
  [[ $recovery_auth_delta -eq 1 ]] ||
    die "$label did not perform exactly one pre-resume SASL reconnect"
  [[ $after_expired -eq $before_expired ]] ||
    die "$label expired the resumable stream"
  [[ $after_discovery -eq $before_discovery ]] ||
    die "$label performed fresh authority discovery after successful resume"
  [[ $after_snapshot -eq $before_snapshot ]] ||
    die "$label requested a fresh authority snapshot after successful resume"
  [[ $after_not_ready -eq $before_not_ready ]] ||
    die "$label received a not-ready resume-authority result"
  [[ $after_resumed -eq $((before_resumed + 1)) ]] ||
    die "$label produced more than one XEP-0198 resume"
  [[ $after_resume_hook -eq $((before_resume_hook + 1)) ]] ||
    die "$label did not execute exactly one authority resume hook"
  [[ $after_ready -eq $((before_ready + 1)) ]] ||
    die "$label did not cross exactly one ready resume-authority barrier"
  wait_for_exact_request_pass "$RUNTIME/$client.log" "$target" "$request_message" ||
    die "$label original in-flight RPC did not complete"

  {
    printf 'client=%s\n' "${client%-client}"
    printf 'faulted_resource=%s@mesh.test/simple-e2e\n' "$user"
    printf 'faulted_role=%s\n' "${label#short-resume-}"
    printf 'target=%s\n' "$target"
    printf 'in_flight_message=%s\n' "$request_message"
    printf 'original_workload_passed_exactly_once=1\n'
    printf 'replacement_bound_session_delta=0\n'
    printf 'pre_resume_scram_reconnect_delta=%s\n' "$recovery_auth_delta"
    printf 'resume_pending_delta=%s\n' "$((after_pending - before_pending))"
    printf 'successful_resume_delta=%s\n' "$((after_resumed - before_resumed))"
    printf 'authority_resume_hook_delta=%s\n' "$((after_resume_hook - before_resume_hook))"
    printf 'resume_expiry_delta=%s\n' "$((after_expired - before_expired))"
    printf 'authority_discovery_delta=%s\n' "$((after_discovery - before_discovery))"
    printf 'authority_snapshot_delta=%s\n' "$((after_snapshot - before_snapshot))"
    printf 'resume_authority_ready_delta=%s\n' "$((after_ready - before_ready))"
    printf 'resume_authority_not_ready_delta=%s\n' "$((after_not_ready - before_not_ready))"
    printf 'core_resume_barrier=ready-result-observed-before-workload-completion\n'
  } >"$RUNTIME/$label-evidence.txt"
}

run_client_with_fault() {
  local client=$1
  local target=$2
  local fault_service=$3
  local fault_label=$4
  local user=$5
  local shaping_service=$6
  local response_service=$7
  local before connected after connected_auth auth_after resumed_before resumed_after expired_before expired_after
  local dispatch_before dispatch_after response_delay request_message start_baseline
  local warmup_request_mailbox_before warmup_response_mailbox_before
  local warmup_out_packets_before warmup_out_bytes_before warmup_in_packets_before warmup_in_bytes_before
  local transition_index transition_message started_count fault_index
  response_delay=500ms
  if [[ $fault_label == native-server-fault ]]; then
    response_delay=10ms
  fi
  before=$(ejabberd_session_count "$user")
  timeline "$client start with $fault_label fault before_sessions=$before"
  # Keep one client-side FIFO for the whole run. Native TURN warm-up uses a
  # benign 10 ms delay so ICE is not distorted. The native server's separately
  # managed outbound-XMPP FIFO also remains at 10 ms during warm-up. The
  # generic async workload cadence, not whole-flow network shaping, gives
  # complete-gathering TURN Rank1 negotiation time to become live.
  if [[ $fault_label == native-server-fault ]]; then
    delay_service_network "$shaping_service" 10ms native-async-network-delay.txt
    capture_router_counters native-async-turn-warmup-before.txt
    warmup_request_mailbox_before=$(mesh_mailbox_admission_count native-async-client native-server)
    warmup_response_mailbox_before=$(mesh_mailbox_admission_count native-server native-async-client)
    warmup_out_packets_before=$(iptables_counter "$RUNTIME/native-async-turn-warmup-before.txt" e2e-native-async-turn-channeldata-out)
    warmup_out_bytes_before=$(iptables_byte_counter "$RUNTIME/native-async-turn-warmup-before.txt" e2e-native-async-turn-channeldata-out)
    warmup_in_packets_before=$(iptables_counter "$RUNTIME/native-async-turn-warmup-before.txt" e2e-native-async-turn-channeldata-in)
    warmup_in_bytes_before=$(iptables_byte_counter "$RUNTIME/native-async-turn-warmup-before.txt" e2e-native-async-turn-channeldata-in)
  else
    delay_service_network "$shaping_service" 500ms
  fi
  # The response-side FIFO delay was installed before this server opened its
  # stream. Revalidate it without replacing a live root qdisc.
  record_xmpp_flow_delay_state \
    "$response_service" "$response_delay" "$fault_label-response-qdisc.txt" pre-request
  dispatch_before=$(service_dispatch_count "$target")
  set +e
  run_client_plain "$client" &
  local client_pid=$!
  BACKGROUND_PIDS+=("$client_pid")
  set -e
  if [[ $before -eq 0 ]]; then
    if ! connected=$(wait_for_count_above "$user initial c2s session" "$before" ejabberd_session_count "$user"); then
      die "$user did not open its initial full c2s session"
    fi
  else
    connected=$before
  fi
  start_baseline=0
  request_message=$(wait_for_request_after "$RUNTIME/$client.log" "$target" request-start "$start_baseline") || {
    restore_service_network "$fault_service" "$fault_label"
    clear_network_delay "$shaping_service"
    wait "$client_pid" || true
    die "$client did not start an active $target RPC before the fault deadline"
  }
  if ! dispatch_after=$(wait_for_count_above "$target server dispatch" "$dispatch_before" service_dispatch_count "$target"); then
    # Let only the active bounded RPC finish so its Core diagnostics reach the
    # retained client log. Do not wait for the client's remaining requests to
    # repeat the same 30-second failure after the scenario has already failed.
    clear_network_delay "$shaping_service"
    wait_for_request_failure "$RUNTIME/$client.log" "$target" || true
    kill "$client_pid" >/dev/null 2>&1 || true
    wait "$client_pid" || true
    BACKGROUND_PIDS=()
    die "$target server did not dispatch the active RPC"
  fi
  if [[ $fault_label == native-server-fault ]]; then
    establish_native_async_turn_rpc \
      "$RUNTIME/$client.log" "$target" "$request_message" 0 \
      "$dispatch_before" "$dispatch_after" \
      "$warmup_request_mailbox_before" "$warmup_response_mailbox_before" \
      "$warmup_out_packets_before" "$warmup_out_bytes_before" \
      "$warmup_in_packets_before" "$warmup_in_bytes_before"
    # Freeze only the application container while its LAN sidecar stays live,
    # then reserve the next ordinary request for the native-server outage. The
    # pause only linearizes environment changes; the application and Core do
    # not receive a fault signal or alter their behavior.
    pause_client_run_container "$client"
    started_count=$(request_event_count "$RUNTIME/$client.log" "$target" request-start)
    [[ $started_count -le 9 ]] ||
      die "native-async left no request for the native-server fault after TURN warm-up ($started_count started)"
    change_service_xmpp_flow_delay \
      "$response_service" 1s "$fault_label-response-qdisc.txt"
    change_service_network_delay "$shaping_service" 500ms native-async-network-delay.txt
    unpause_client_run_container "$client"
    transition_index=$((NATIVE_TURN_WARMUP_INDEX + 1))
    while (( transition_index < started_count )); do
      transition_message=$(wait_for_request_after \
        "$RUNTIME/$client.log" "$target" request-start "$transition_index") ||
        die "native-async response-qdisc transition RPC $transition_index was not recorded"
      wait_for_exact_request_pass "$RUNTIME/$client.log" "$target" "$transition_message" ||
        die "native-async response-qdisc transition RPC $transition_message did not succeed"
      transition_index=$((transition_index + 1))
    done
    response_delay=1s
    dispatch_before=$(service_dispatch_count "$target")
    fault_index=$started_count
    request_message=$(wait_for_request_after "$RUNTIME/$client.log" "$target" request-start "$fault_index") ||
      die "native-async fault RPC did not start after exact TURN warm-up proof"
    if ! dispatch_after=$(wait_for_count_above "$target server dispatch" "$dispatch_before" service_dispatch_count "$target"); then
      clear_network_delay "$shaping_service"
      wait_for_request_failure "$RUNTIME/$client.log" "$target" || true
      kill "$client_pid" >/dev/null 2>&1 || true
      wait "$client_pid" || true
      BACKGROUND_PIDS=()
      die "$target server did not dispatch the faulted RPC after exact TURN warm-up proof"
    fi
  fi
  timeline "$client $target fault-target server dispatch observed count=$dispatch_after message=$request_message"
  record_xmpp_flow_delay_state \
    "$response_service" "$response_delay" "$fault_label-response-qdisc.txt" dispatch-observed
  timeline "$client active $target rpc observed"
  # Baseline immediately before installing the fault, after the active dispatch
  # and the potentially long TURN warm-up. Sampling after blackhole_service
  # would leave a gap for its bounded rejection pings in which the campaign's
  # XEP-0198 expiry could occur before the baseline is recorded.
  expired_before=$(ejabberd_log_count \
    "Closing c2s session for $user@mesh.test/simple-e2e: Stream closed by local host: Timed out waiting for stream resumption")
  timeline "$client $user XEP-0198 expiry baseline count=$expired_before"
  blackhole_service "$fault_service" "$fault_label"
  # Router-level fault rules catch the response already waiting in the fixed
  # server qdisc. The qdisc itself remains unchanged for the entire run.
  # TCP failure detection is deliberately independent from the agents and can
  # consume most of a fixed outage interval. Keep the environment blackholed
  # until ejabberd has actually expired this exact resource's XEP-0198 resume
  # window. Restoring earlier can accidentally resume the stale stream and no
  # longer proves process-lifetime outbox replay through fresh authentication.
  expired_after=$(wait_for_count_above \
    "$user XEP-0198 resume-window expiry" \
    "$expired_before" \
    ejabberd_log_count \
    "Closing c2s session for $user@mesh.test/simple-e2e: Stream closed by local host: Timed out waiting for stream resumption")
  timeline "$client $user stale XEP-0198 session expired count=$expired_after"
  record_fault_socket_state "$fault_service" "$fault_label"
  capture_service_filter "$fault_service" "$fault_label-counters.txt"
  assert_fault_direction_positive "$RUNTIME/$fault_label-counters.txt" "$fault_label" in
  assert_fault_direction_positive "$RUNTIME/$fault_label-counters.txt" "$fault_label" out
  if [[ $fault_label == native-server-fault ]]; then
    # Ejabberd has authoritatively expired the exact old XMPP stream and the
    # network is still blackholed, so changing this qdisc cannot affect live
    # stream traffic. Keep the larger delay only for deterministic fault
    # capture; the smaller recovery delay leaves room for fresh authentication
    # and replay inside the RPC deadline.
    delay_service_xmpp_flow \
      "$response_service" 500ms native-server-recovery-response-qdisc.txt
  fi
  # Baseline immediately before restoration. Any later increments therefore
  # prove full-session establishment and SCRAM authentication after recovery,
  # while an unchanged resumption count rejects reuse of the stale stream.
  connected=$(ejabberd_session_count "$user")
  connected_auth=$(ejabberd_auth_count "$user")
  resumed_before=$(ejabberd_log_count "Resumed session for $user@mesh.test/simple-e2e")
  timeline "$client $user pre-restore full_sessions=$connected authentications=$connected_auth resumptions=$resumed_before"
  restore_service_network "$fault_service" "$fault_label"
  if [[ $shaping_service != "$fault_service" ]]; then
    clear_network_delay "$shaping_service"
  fi
  if ! wait "$client_pid"; then
    printf 'simple-e2e: %s failed\n' "$client" >&2
    sed -n '1,240p' "$RUNTIME/$client.log" >&2
    return 1
  fi
  BACKGROUND_PIDS=()
  after=$(ejabberd_session_count "$user")
  auth_after=$(ejabberd_auth_count "$user")
  resumed_after=$(ejabberd_log_count "Resumed session for $user@mesh.test/simple-e2e")
  timeline "$client complete with $fault_label fault after_sessions=$after"
  if [[ $after -le $connected ]]; then
    die "$fault_label outage did not produce a fresh full c2s session for $user@mesh.test"
  fi
  if [[ $auth_after -le $connected_auth ]]; then
    die "$fault_label outage did not produce fresh SCRAM authentication for $user@mesh.test"
  fi
  if [[ $resumed_after -ne $resumed_before ]]; then
    die "$fault_label outage resumed the stale XEP-0198 session for $user@mesh.test"
  fi
  # Installing a router rule and observing a container log are independent
  # environment operations. The already-dispatched response can cross the
  # router just before the rule linearizes; in that race, the immediately next
  # sequential RPC is the one retained in the sender outbox. Require exactly
  # one successful RPC in this target group to span the fresh-auth outage,
  # without coupling the assertion to a particular application message key.
  assert_faulted_request_elapsed "$RUNTIME/$client.log" "$client" "$target"
}

counter_value() {
  local comment=$1
  router_sh 'iptables-save -c' | iptables_counter /dev/stdin "$comment"
}

turn_log_lines() {
  compose logs --no-color turn 2>/dev/null | wc -l | tr -d '[:space:]'
}

capture_turn_delta() {
  local baseline=$1
  compose logs --no-color turn 2>/dev/null | tail -n "+$((baseline + 1))" >"$RUNTIME/native-async-turn-delta.log"
}

run_restricted_probe_in_monkey_namespace() {
  timeline "monkey-async restriction rejection probes"
  service_sh monkey-async-client-net '
    (printf x | socat -T 1 - TCP:10.240.70.2:5443,bind=10.240.40.2 >/dev/null 2>&1 || true)
    (printf x | socat -T 1 - UDP:10.240.80.2:3478,bind=10.240.40.2 >/dev/null 2>&1 || true)
    (printf x | socat -T 1 - UDP:10.240.90.2:3478,bind=10.240.40.2 >/dev/null 2>&1 || true)
    (printf x | socat -T 1 - UDP:10.240.50.2:39001,bind=10.240.40.2 >/dev/null 2>&1 || true)
  '
}

run_native_direct_rejection_probe() {
  timeline "native-async direct-path UDP rejection probes"
  service_sh native-async-client-net '
    (printf x | socat -T 1 - UDP:10.240.50.2:39001,bind=10.240.20.2 >/dev/null 2>&1 || true)
    (printf x | socat -T 1 - UDP:10.240.60.2:39001,bind=10.240.20.2 >/dev/null 2>&1 || true)
    (printf x | socat -T 1 - UDP:10.240.80.2:3478,bind=10.240.20.2 >/dev/null 2>&1 || true)
    (printf x | socat -T 1 - UDP:10.240.70.2:39002,bind=10.240.20.2 >/dev/null 2>&1 || true)
  '
}

require docker
require openssl
require python3
docker compose version >/dev/null
TRUNK_PREFIX=$(choose_trunk_prefix)
[[ -n "$GO_CORE" ]] || die "set CYNAPSA_GO_CORE_PATH to a Go-core worktree"
[[ -d "$GO_CORE/.git" || -f "$GO_CORE/.git" ]] || die "Go-core worktree not found: $GO_CORE"
[[ -f "$EJABBERD/Dockerfile" && -f "$EJABBERD/test/sdk-e2e/legacy.yml" ]] || \
  die "dedicated ejabberd worktree not found: $EJABBERD"
git -C "$GO_CORE" merge-base --is-ancestor "$CORE_BASE" HEAD || \
  die "Go-core worktree must be based on pinned main $CORE_BASE"

mkdir -m 0700 -p "$RUNTIME" "$ARTIFACTS"
trap 'cleanup $?' EXIT
trap 'cleanup 130' INT
trap 'cleanup 143' TERM
python3 "$ROOT/record_core_provenance.py" "$GO_CORE" "$RUNTIME/core-provenance.json"

openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 2 \
  -subj '/CN=Cynapsa simple E2E CA' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' \
  -addext 'subjectKeyIdentifier=hash' \
  -keyout "$RUNTIME/ca-key.pem" -out "$RUNTIME/ca.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -sha256 -nodes -subj '/CN=mesh.test' \
  -addext 'subjectAltName=DNS:mesh.test,IP:10.240.70.2' \
  -keyout "$RUNTIME/server-key.pem" -out "$RUNTIME/server.csr" >/dev/null 2>&1
printf '%s\n' 'subjectAltName=DNS:mesh.test,IP:10.240.70.2' 'extendedKeyUsage=serverAuth' >"$RUNTIME/server.ext"
openssl x509 -req -sha256 -days 2 -in "$RUNTIME/server.csr" \
  -CA "$RUNTIME/ca.pem" -CAkey "$RUNTIME/ca-key.pem" -CAcreateserial \
  -extfile "$RUNTIME/server.ext" -out "$RUNTIME/server-cert.pem" >/dev/null 2>&1
cp "$RUNTIME/server-cert.pem" "$RUNTIME/server.pem"
cat "$RUNTIME/server-key.pem" >>"$RUNTIME/server.pem"
chmod 0644 "$RUNTIME/ca.pem" "$RUNTIME/server.pem"

umask 077
{
  printf 'CYNAPSA_E2E_RUNTIME_DIR=%s\n' "$RUNTIME"
  printf 'CYNAPSA_GO_CORE_PATH=%s\n' "$GO_CORE"
  printf 'CYNAPSA_EJABBERD_PATH=%s\n' "$EJABBERD"
  printf 'CYNAPSA_E2E_IMAGE_TAG=%s\n' "$IMAGE_TAG"
  printf 'CYNAPSA_E2E_TRUNK_PREFIX=%s\n' "$TRUNK_PREFIX"
  printf 'CYNAPSA_TURN_STATIC_AUTH_SECRET=%s\n' "$(openssl rand -hex 32)"
  for user in "${USERS[@]}"; do
    variable=$(printf '%s' "$user" | tr '[:lower:]-' '[:upper:]_')_PASSWORD
    printf '%s=%s\n' "$variable" "$(openssl rand -hex 24)"
  done
} >"$RUNTIME/credentials.env"

# shellcheck disable=SC1091
source "$RUNTIME/credentials.env"
python3 - "$RUNTIME/credentials.env" "$EJABBERD/test/sdk-e2e/legacy.yml" "$RUNTIME/ejabberd.yml" <<'PY'
from pathlib import Path
import sys

values = dict(
    line.split("=", 1)
    for line in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()
    if "=" in line
)
text = Path(sys.argv[2]).read_text(encoding="utf-8")
Path(sys.argv[3]).write_text(
    text.replace("@CYNAPSA_TURN_STATIC_AUTH_SECRET@", values["CYNAPSA_TURN_STATIC_AUTH_SECRET"]),
    encoding="utf-8",
)
PY
cp "$ROOT/coturn-stun.conf" "$RUNTIME/coturn-stun.conf"
python3 - "$RUNTIME/credentials.env" "$ROOT/coturn-turn.conf.in" "$RUNTIME/coturn-turn.conf" <<'PY'
from pathlib import Path
import sys

values = dict(
    line.split("=", 1)
    for line in Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()
    if "=" in line
)
text = Path(sys.argv[2]).read_text(encoding="utf-8")
Path(sys.argv[3]).write_text(
    text.replace("@CYNAPSA_TURN_STATIC_AUTH_SECRET@", values["CYNAPSA_TURN_STATIC_AUTH_SECRET"]),
    encoding="utf-8",
)
PY
chmod 0644 "$RUNTIME/ejabberd.yml" "$RUNTIME/coturn-stun.conf" "$RUNTIME/coturn-turn.conf"

# Compose validates interpolated configuration before any state is created.
compose config --quiet
compose build router native-server-net ejabberd native-server
docker pull "$COTURN_IMAGE" >/dev/null
compose up -d router stun turn ejabberd
wait_healthy router
wait_healthy ejabberd-net
wait_healthy stun-net
wait_healthy turn-net
wait_healthy stun
wait_healthy turn
wait_healthy ejabberd

ready=$(ejabberdctl cynapsa_mesh_ready mesh.test)
[[ "$ready" == $'ready\tmesh.test\tsingle_node_mnesia' ]] || die "mesh module readiness mismatch: $ready"

for user in "${USERS[@]}"; do
  variable=$(printf '%s' "$user" | tr '[:lower:]-' '[:upper:]_')_PASSWORD
  password=${!variable}
  ejabberdctl register "$user" mesh.test "$password" >/dev/null
done
ejabberdctl cynapsa_mesh_create simple-e2e mesh.test >/dev/null
for user in "${USERS[@]}"; do
  ejabberdctl cynapsa_mesh_add "$user" mesh.test simple-e2e >/dev/null
done
snapshot=$(ejabberdctl cynapsa_mesh_snapshot simple-e2e mesh.test)
for user in "${USERS[@]}"; do
  [[ "$snapshot" == *"$user@mesh.test"* ]] || die "mesh snapshot omits $user: $snapshot"
done
printf '%s\n' "$snapshot" >"$ARTIFACTS/mesh-snapshot.txt"

compose up -d native-server-net monkey-server-net
wait_healthy native-server-net
wait_healthy monkey-server-net
# Install response shaping before the application process opens its XMPP
# connection. Replacing a root qdisc on a live TCP stream can discard an
# already-queued control packet and turn test setup into an unintended fault.
delay_service_xmpp_flow native-server-net 10ms native-server-response-qdisc.txt
# Start both servers with a normal XMPP-only FIFO. The short campaigns change
# this existing qdisc only after initial auth, discovery, and snapshot finish.
delay_service_xmpp_flow monkey-server-net 10ms monkey-server-short-resume-delay.txt
compose up -d native-server monkey-server
wait_healthy native-server
wait_healthy monkey-server
start_authority_trace
CLIENT_SIDECARS=()
while IFS= read -r sidecar; do
  CLIENT_SIDECARS+=("$sidecar")
done < <(client_sidecars)
compose --profile clients up -d "${CLIENT_SIDECARS[@]}"
for service in "${CLIENT_SIDECARS[@]}"; do
  wait_healthy "$service"
done

# Before application RPCs, prove XMPP STARTTLS plus tagged, source-preserving
# cross-VLAN TCP and UDP. The probe runs only in dedicated sidecar namespaces.
compose exec -T native-server-net rm -f \
  /tmp/cynapsa-network-probe-ready /tmp/cynapsa-network-probe
compose exec -T -d native-server-net cynapsa-network-probe-receiver
receiver_deadline=$((SECONDS + 20))
while (( SECONDS < receiver_deadline )); do
  compose exec -T native-server-net test -f /tmp/cynapsa-network-probe-ready \
    >/dev/null 2>&1 && break
  sleep 1
done
compose exec -T native-server-net test -f /tmp/cynapsa-network-probe-ready || \
  die "tagged network probe receiver did not become ready"
compose exec -T native-sync-client-net cynapsa-network-probe >"$ARTIFACTS/network-probe.log" 2>&1
probe_deadline=$((SECONDS + 20))
probe_result=
while (( SECONDS < probe_deadline )); do
  probe_result=$(compose exec -T native-server-net sh -c \
    'test -f /tmp/cynapsa-network-probe && cat /tmp/cynapsa-network-probe' 2>/dev/null || true)
  [[ -n "$probe_result" ]] && break
  sleep 1
done
[[ "$probe_result" == $'10.240.10.2\t10.240.10.2' ]] || \
  die "tagged TCP/UDP source mismatch: ${probe_result:-no result received}"
compose exec -T router sh -c \
  '! ip -4 -o address show dev eth0 scope global | grep -q . && ! ip -4 route show dev eth0 | grep -q . && [ "$(ip -d link show type vlan | grep -c "^[0-9]")" -eq 9 ] && [ "$(sysctl -n net.ipv4.conf.all.send_redirects)" -eq 0 ] && ! iptables -t nat -S | grep -Eq "(MASQUERADE|SNAT|DNAT)"' || \
  die "router trunk/VLAN/redirect/NAT invariant failed"
compose exec -T stun turnutils_stunclient -p 3478 10.240.80.2 \
  >"$RUNTIME/stun-protocol-ready.log" 2>&1 || die "STUN protocol readiness failed"
compose exec -T turn sh -c \
  'secret=$(sed -n "s/^static-auth-secret=//p" /etc/coturn/turnserver.conf | sed -n "1p"); test -n "$secret" && turnutils_uclient -y -W "$secret" -m 1 -n 1 -p 3478 10.240.90.2' \
  >"$RUNTIME/turn-protocol-ready.log" 2>&1 || die "TURN protocol readiness failed"
compose exec -T native-server-net rm -f \
  /tmp/cynapsa-network-probe-ready /tmp/cynapsa-network-probe
capture_artifact "$RUNTIME/stun-protocol-ready.log" stun-protocol-ready.log
capture_artifact "$RUNTIME/turn-protocol-ready.log" turn-protocol-ready.log
printf 'simple-e2e: tagged network probe PASS (XMPP STARTTLS, TCP+UDP source 10.240.10.2, routed gateway, 9 VLANs, STUN/TURN ready, untagged IP absent, NAT empty)\n'
if [[ ${CYNAPSA_E2E_PROBE_ONLY:-0} == 1 ]]; then
  printf 'simple-e2e: probe-only PASS; artifacts: %s\n' "$ARTIFACTS"
  exit 0
fi

if [[ ${CYNAPSA_E2E_FOCUSED:-0} == 1 ]]; then
  run_sequence() {
    local label=$1
    local sequence=$2
    local client=$3
    if ! compose --profile clients run --rm --no-deps \
      -e CYNAPSA_E2E_SEQUENCE_LABEL="$label" \
      -e CYNAPSA_E2E_SEQUENCE="$sequence" "$client" \
      python -m cynapsa_e2e.sequence_client \
      >"$RUNTIME/$client.log" 2>&1; then
      printf 'simple-e2e: focused sequence failed: %s\n' "$label" >&2
      return 1
    fi
  }
  focused_failure=0
  run_sequence native-then-monkey 'native,monkey' native-sync-client || focused_failure=1
  sleep 5
  run_sequence monkey-then-native 'monkey,native' native-async-client || focused_failure=1
  if [[ $focused_failure -ne 0 ]]; then
    for client in "${CLIENTS[@]}"; do
      printf '%s\n' "--- $client ---" >&2
      sed -n '1,240p' "$RUNTIME/$client.log" >&2
    done
    die "one or more focused sequences failed"
  fi
  printf 'simple-e2e: focused switch sequences PASS 4/4; artifacts: %s\n' "$ARTIFACTS"
  exit 0
fi

client_failure=0
for client in "${CLIENTS[@]}"; do
  client_failed=0
  case "$client" in
    native-sync-client)
      change_service_xmpp_flow_delay \
        native-server-net 500ms native-server-response-qdisc.txt
      change_service_xmpp_flow_delay \
        monkey-server-net 500ms monkey-server-short-resume-delay.txt
      run_client_with_short_resume "$client" monkey monkey-server-net \
        short-resume-monkey-server monkey-server
      change_service_xmpp_flow_delay \
        native-server-net 10ms native-server-response-qdisc.txt
      change_service_xmpp_flow_delay \
        monkey-server-net 10ms monkey-server-short-resume-delay.txt
      DEEP_EVIDENCE_SERVER_RESUME=1
      ;;
    native-async-client)
      install_native_async_turn_rules
      run_native_direct_rejection_probe
      capture_router_counters native-async-router-counters.txt
      assert_counter_positive "$RUNTIME/native-async-router-counters.txt" e2e-native-async-direct-out
      assert_counter_positive "$RUNTIME/native-async-router-counters.txt" e2e-native-async-other-udp-out
      native_nat_before=$(counter_value e2e-native-async-nat)
      native_turn_out_before=$(counter_value e2e-native-async-turn-out)
      native_turn_in_before=$(counter_value e2e-native-async-turn-in)
      native_turn_log_before=$(turn_log_lines)
      run_client_with_fault "$client" native native-server-net native-server-fault native-server native-async-client-net native-server-net
      compose logs --no-color turn >"$RUNTIME/turn.log" 2>&1 || true
      capture_router_counters native-async-router-counters.txt
      native_nat_after=$(counter_value e2e-native-async-nat)
      native_turn_out_after=$(counter_value e2e-native-async-turn-out)
      native_turn_in_after=$(counter_value e2e-native-async-turn-in)
      [[ $native_nat_after -gt $native_nat_before ]] || die "native-async run produced no attributable NAT counter delta"
      [[ $native_turn_out_after -gt $native_turn_out_before ]] || die "native-async run produced no attributable TURN egress delta"
      [[ $native_turn_in_after -gt $native_turn_in_before ]] || die "native-async run produced no attributable TURN ingress delta"
      NATIVE_NAT_DELTA=$((native_nat_after - native_nat_before))
      NATIVE_TURN_OUT_DELTA=$((native_turn_out_after - native_turn_out_before))
      NATIVE_TURN_IN_DELTA=$((native_turn_in_after - native_turn_in_before))
      capture_turn_delta "$native_turn_log_before"
      grep -Eiq 'ALLOCATE|allocation' "$RUNTIME/native-async-turn-delta.log" || die "native-async TURN log delta omits allocation evidence"
      grep -Eiq 'CHANNEL_BIND|channel.?bind' "$RUNTIME/native-async-turn-delta.log" || die "native-async TURN log delta omits channel-bind evidence"
      DEEP_EVIDENCE_NATIVE_TURN=1
      DEEP_EVIDENCE_NATIVE_FAULT=1
      # Keep the two independent fault campaigns separate. A Core may still
      # be reconciling the destination's expired stream after the faulted
      # native RPC has returned; starting the client outage during that work
      # would make one fault-campaign RPC absorb two full reauthentication cycles.
      wait_for_resource_quiescent native-server 12
      ;;
    monkey-sync-client)
      change_service_xmpp_flow_delay \
        native-server-net 500ms native-server-response-qdisc.txt
      change_service_xmpp_flow_delay \
        monkey-server-net 500ms monkey-server-short-resume-delay.txt
      run_client_with_short_resume "$client" monkey monkey-sync-client-net \
        short-resume-monkey-sync-client monkey-sync-client
      change_service_xmpp_flow_delay \
        native-server-net 10ms native-server-response-qdisc.txt
      change_service_xmpp_flow_delay \
        monkey-server-net 10ms monkey-server-short-resume-delay.txt
      DEEP_EVIDENCE_CLIENT_RESUME=1
      ;;
    monkey-async-client)
      install_monkey_async_xmpp_rules
      run_restricted_probe_in_monkey_namespace
      capture_router_counters monkey-async-router-counters.txt
      assert_counter_positive "$RUNTIME/monkey-async-router-counters.txt" e2e-monkey-async-non-xmpp-out
      MONKEY_NON_XMPP_DENIED=$(counter_value e2e-monkey-async-non-xmpp-out)
      monkey_xmpp_out_before=$(counter_value e2e-monkey-async-xmpp-out)
      monkey_xmpp_in_before=$(counter_value e2e-monkey-async-xmpp-in)
      # The preceding successful-resume campaign restores this live FIFO to
      # its benign 10 ms setting. Move it back to the 500 ms fault-capture
      # setting before run_client_with_fault validates the response path.
      change_service_xmpp_flow_delay \
        native-server-net 500ms native-server-response-qdisc.txt
      run_client_with_fault "$client" native monkey-async-client-net monkey-async-client-fault monkey-async-client monkey-async-client-net native-server-net
      capture_router_counters monkey-async-router-counters.txt
      monkey_xmpp_out_after=$(counter_value e2e-monkey-async-xmpp-out)
      monkey_xmpp_in_after=$(counter_value e2e-monkey-async-xmpp-in)
      [[ $monkey_xmpp_out_after -gt $monkey_xmpp_out_before ]] || die "monkey-async run produced no attributable allowed-XMPP egress delta"
      [[ $monkey_xmpp_in_after -gt $monkey_xmpp_in_before ]] || die "monkey-async run produced no attributable allowed-XMPP ingress delta"
      MONKEY_XMPP_OUT_DELTA=$((monkey_xmpp_out_after - monkey_xmpp_out_before))
      MONKEY_XMPP_IN_DELTA=$((monkey_xmpp_in_after - monkey_xmpp_in_before))
      python3 - "$ROOT" "$RUNTIME/$client.log" <<'PY'
from pathlib import Path
import sys

sys.path.insert(0, sys.argv[1])
from verify_results import _validate_log

report = _validate_log(Path(sys.argv[2]))
if report["client"] != "monkey-async" or report["passed"] != 20:
    raise RuntimeError(f"unexpected monkey-async delivery report: {report!r}")
PY
      {
        printf 'transport=rank-2-xmpp\n'
        printf 'only_allowed_destination=10.240.70.2:5222/tcp\n'
        printf 'exact_unique_deliveries=20\n'
        printf 'allowed_xmpp_egress_delta=%s\n' "$MONKEY_XMPP_OUT_DELTA"
        printf 'allowed_xmpp_ingress_delta=%s\n' "$MONKEY_XMPP_IN_DELTA"
        printf 'denied_non_xmpp_packets=%s\n' "$MONKEY_NON_XMPP_DENIED"
      } >"$RUNTIME/monkey-async-rank2-evidence.txt"
      DEEP_EVIDENCE_MONKEY_XMPP=1
      DEEP_EVIDENCE_MONKEY_FAULT=1
      ;;
    *)
      if ! run_client_plain "$client"; then
        client_failed=1
      fi
      ;;
  esac
  if [[ $client_failed -ne 0 ]]; then
    client_failure=1
    printf 'simple-e2e: %s failed\n' "$client" >&2
    sed -n '1,240p' "$RUNTIME/$client.log" >&2
  fi
done
[[ $client_failure -eq 0 ]] || die "one or more client containers failed"
if (( ${#CLIENTS[@]} == 4 )); then
  [[ $DEEP_EVIDENCE_NATIVE_TURN -eq 1 ]] || die "full run missed native-async TURN relay evidence"
  [[ $DEEP_EVIDENCE_MONKEY_XMPP -eq 1 ]] || die "full run missed monkey-async XMPP-only evidence"
  [[ $DEEP_EVIDENCE_NATIVE_FAULT -eq 1 ]] || die "full run missed native-server outage evidence"
  [[ $DEEP_EVIDENCE_MONKEY_FAULT -eq 1 ]] || die "full run missed monkey-async-client outage evidence"
  [[ $DEEP_EVIDENCE_SERVER_RESUME -eq 1 ]] || die "full run missed monkey-server successful-resume evidence"
  [[ $DEEP_EVIDENCE_CLIENT_RESUME -eq 1 ]] || die "full run missed monkey-sync-client successful-resume evidence"
fi

compose logs --no-color native-server >"$RUNTIME/native-server.log"
compose logs --no-color monkey-server >"$RUNTIME/monkey-server.log"
compose logs --no-color ejabberd >"$RUNTIME/ejabberd.log"
compose logs --no-color stun >"$RUNTIME/stun.log" 2>&1 || true
compose logs --no-color turn >"$RUNTIME/turn.log" 2>&1 || true
capture_authority_trace
{
  for user in "${USERS[@]}"; do
    count=$({ grep -F "Opened c2s session for $user@mesh.test/simple-e2e" "$RUNTIME/ejabberd.log" || true; } | wc -l | tr -d '[:space:]')
    printf '%s\t%s\n' "$user@mesh.test/simple-e2e" "$count"
  done
} >"$RUNTIME/ejabberd-full-sessions.txt"
SESSION_USERS=(native-server monkey-server "${CLIENTS[@]}")
for user in "${SESSION_USERS[@]}"; do
  grep -Fq "Opened c2s session for $user@mesh.test/simple-e2e" \
    "$RUNTIME/ejabberd.log" || \
    die "ejabberd log omits exact mesh-resource session for $user@mesh.test"
done
if [[ $DEEP_EVIDENCE_NATIVE_FAULT -eq 1 ]]; then
  native_fault_sessions=$(awk '$1 == "native-server@mesh.test/simple-e2e" {print $2}' "$RUNTIME/ejabberd-full-sessions.txt")
  [[ ${native_fault_sessions:-0} -ge 2 ]] || die "native-server outage did not leave fresh full c2s evidence"
fi
if [[ $DEEP_EVIDENCE_MONKEY_FAULT -eq 1 ]]; then
  monkey_fault_sessions=$(awk '$1 == "monkey-async-client@mesh.test/simple-e2e" {print $2}' "$RUNTIME/ejabberd-full-sessions.txt")
  [[ ${monkey_fault_sessions:-0} -ge 2 ]] || die "monkey-async-client outage did not leave fresh full c2s evidence"
fi
{
  printf 'native_async_forced_turn=%s\n' "$DEEP_EVIDENCE_NATIVE_TURN"
  printf 'monkey_async_xmpp_only=%s\n' "$DEEP_EVIDENCE_MONKEY_XMPP"
  printf 'native_server_midflight_fault=%s\n' "$DEEP_EVIDENCE_NATIVE_FAULT"
  printf 'monkey_async_client_midflight_fault=%s\n' "$DEEP_EVIDENCE_MONKEY_FAULT"
  printf 'monkey_server_midflight_successful_resume=%s\n' "$DEEP_EVIDENCE_SERVER_RESUME"
  printf 'monkey_sync_client_midflight_successful_resume=%s\n' "$DEEP_EVIDENCE_CLIENT_RESUME"
  printf 'xep0198_resume_timeout_seconds=15\n'
  printf 'short_fault_release_condition=ejabberd_resumable_stream_transition\n'
  printf 'short_resume_pre_resume_scram_reconnect_delta=%s\n' \
    "$((DEEP_EVIDENCE_SERVER_RESUME + DEEP_EVIDENCE_CLIENT_RESUME))"
  printf 'short_resume_replacement_bound_session_delta=0\n'
  printf 'short_resume_authority_discovery_delta=0\n'
  printf 'short_resume_authority_snapshot_delta=0\n'
  printf 'short_resume_authority_barrier_ready_delta=%s\n' \
    "$((DEEP_EVIDENCE_SERVER_RESUME + DEEP_EVIDENCE_CLIENT_RESUME))"
  printf 'fault_release_condition=xep0198_resume_window_expired\n'
  printf 'native_async_nat_packet_delta=%s\n' "$NATIVE_NAT_DELTA"
  printf 'native_async_standalone_stun_advertised=1\n'
  printf 'standalone_stun_role=advertised_discovery_only\n'
  printf 'native_async_core_ice_policy=all\n'
  printf 'native_async_connectivity_and_data_path=turn_relay_forced_by_network\n'
  printf 'native_async_turn_egress_packet_delta=%s\n' "$NATIVE_TURN_OUT_DELTA"
  printf 'native_async_turn_ingress_packet_delta=%s\n' "$NATIVE_TURN_IN_DELTA"
  printf 'native_async_exact_turn_warmup_rpc=%s\n' "$DEEP_EVIDENCE_NATIVE_TURN"
  printf 'monkey_async_xmpp_egress_packet_delta=%s\n' "$MONKEY_XMPP_OUT_DELTA"
  printf 'monkey_async_xmpp_ingress_packet_delta=%s\n' "$MONKEY_XMPP_IN_DELTA"
  printf 'monkey_async_denied_non_xmpp_packets=%s\n' "$MONKEY_NON_XMPP_DENIED"
  printf 'turn_image=%s\n' "$COTURN_IMAGE"
} >"$RUNTIME/deep-evidence.txt"

if (( ${#CLIENTS[@]} < 4 )); then
  filtered_logs=()
  for client in "${CLIENTS[@]}"; do
    filtered_logs+=("$RUNTIME/$client.log" "$client")
  done
  python3 - "$ROOT" "${filtered_logs[@]}" <<'PY'
from pathlib import Path
import sys

sys.path.insert(0, sys.argv[1])
from verify_results import _validate_log

arguments = sys.argv[2:]
if len(arguments) % 2:
    raise RuntimeError("filtered client arguments are not paired")
for index in range(0, len(arguments), 2):
    report = _validate_log(Path(arguments[index]))
    expected = arguments[index + 1].removesuffix("-client")
    if report["client"] != expected:
        raise RuntimeError(
            f"filtered report client mismatch: expected {expected!r}, got {report['client']!r}"
        )
PY
  filtered_expected=$((20 * ${#CLIENTS[@]}))
  printf 'simple-e2e: filtered clients PASS %s/%s (%s); artifacts: %s\n' \
    "$filtered_expected" "$filtered_expected" "${CLIENTS[*]}" "$ARTIFACTS"
  exit 0
fi

python3 "$ROOT/verify_results.py" \
  --native-server-log "$RUNTIME/native-server.log" \
  --monkey-server-log "$RUNTIME/monkey-server.log" \
  "$RUNTIME/native-sync-client.log" \
  "$RUNTIME/native-async-client.log" \
  "$RUNTIME/monkey-sync-client.log" \
  "$RUNTIME/monkey-async-client.log" >"$RUNTIME/summary.base.json"
python3 - "$RUNTIME/summary.base.json" "$RUNTIME/deep-evidence.txt" "$RUNTIME/summary.json" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

summary = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
evidence: dict[str, object] = {}
boolean_keys = {
    "native_async_forced_turn",
    "monkey_async_xmpp_only",
    "native_server_midflight_fault",
    "monkey_async_client_midflight_fault",
    "monkey_server_midflight_successful_resume",
    "monkey_sync_client_midflight_successful_resume",
    "native_async_standalone_stun_advertised",
    "native_async_exact_turn_warmup_rpc",
}
for line in Path(sys.argv[2]).read_text(encoding="utf-8").splitlines():
    if "=" not in line:
        continue
    key, value = line.split("=", 1)
    if key in boolean_keys and value in {"0", "1"}:
        evidence[key] = value == "1"
    else:
        try:
            evidence[key] = int(value)
        except ValueError:
            evidence[key] = value
summary["deep_evidence"] = evidence
Path(sys.argv[3]).write_text(json.dumps(summary, sort_keys=True) + "\n", encoding="utf-8")
PY
capture_artifact "$RUNTIME/summary.json" summary.json
printf 'simple-e2e: PASS 80/80; artifacts: %s\n' "$ARTIFACTS"
