#!/bin/sh
set -eu

ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/../../.." && pwd)
HARNESS="$ROOT/test/e2e/ejabberd/run.sh"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/cynapsa-mailbox-test.XXXXXX")
STATE=

cleanup() {
  if [ -n "$STATE" ] && [ -d "$STATE" ]; then
    "$HARNESS" stop "$STATE" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK"
}
trap cleanup INT TERM HUP EXIT

go build -o "$WORK/group-probe" ./test/e2e/ejabberd/group_probe
STATE=$($HARNESS start shared-group-mailbox)
set -a
# shellcheck disable=SC1090
. "$STATE/credentials.env"
set +a

admin() {
  "$HARNESS" admin "$STATE" "$@"
}

probe() {
  "$WORK/group-probe" "$@"
}

expect_count() {
  expected=$1
  shift
  actual=$(admin mailbox-count "$@")
  [ "$actual" = "$expected" ] || {
    printf 'mailbox count for %s = %s, want %s\n' "$*" "$actual" "$expected" >&2
    exit 1
  }
}

wait_count() {
  expected=$1
  shift
  attempts=0
  while [ "$attempts" -lt 50 ]; do
    actual=$(admin mailbox-count "$@")
    [ "$actual" = "$expected" ] && return 0
    attempts=$((attempts + 1))
    sleep 0.1
  done
  printf 'mailbox count for %s = %s, want %s\n' "$*" "$actual" "$expected" >&2
  exit 1
}

admin create Mesh-A >/dev/null
admin add agent-a Mesh-A >/dev/null
admin add agent-b Mesh-A >/dev/null
admin create Mesh-B >/dev/null
admin add agent-a Mesh-B >/dev/null
admin add agent-b Mesh-B >/dev/null

# Replaying one scoped message identity after a lost custody receipt reuses its
# stable FIFO row and cannot consume per-resource quota with duplicates.
probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_A" CYNAPSA_EJABBERD_PASSWORD_A \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-A duplicate-custody >/dev/null
probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_A" CYNAPSA_EJABBERD_PASSWORD_A \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-A duplicate-custody >/dev/null
expect_count 1 agent-b Mesh-A
probe mailbox-receive "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_B" CYNAPSA_EJABBERD_PASSWORD_B \
  Mesh-A duplicate-custody >/dev/null
expect_count 0 agent-b Mesh-A

# Offline exact-resource commit survives an ejabberd service restart and drains
# only after that same resource completes a fresh authority sync.
probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_A" CYNAPSA_EJABBERD_PASSWORD_A \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-A offline-one >/dev/null
expect_count 1 agent-b Mesh-A
admin mailbox-count agent-b Mesh-B | grep -qx 0
"$HARNESS" restart "$STATE"
expect_count 1 agent-b Mesh-A
probe mailbox-receive "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_B" CYNAPSA_EJABBERD_PASSWORD_B \
  Mesh-A offline-one >/dev/null
expect_count 0 agent-b Mesh-A

# Another live resource for the same bare account must not receive or suppress
# storage for the addressed resource.
other_signal="$WORK/other-release"
probe wait-preserved "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_B" CYNAPSA_EJABBERD_PASSWORD_B Mesh-B \
  "$CYNAPSA_EJABBERD_AGENT_A" "$other_signal" >"$WORK/other.log" 2>&1 &
other_pid=$!
while ! grep -q 'other-mesh-session-ready' "$WORK/other.log" 2>/dev/null; do
  if ! kill -0 "$other_pid" 2>/dev/null; then
    cat "$WORK/other.log" >&2
    printf '%s\n' 'other-resource probe exited before readiness' >&2
    exit 1
  fi
  sleep 0.1
done
probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_A" CYNAPSA_EJABBERD_PASSWORD_A \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-A exact-resource-only >/dev/null
expect_count 1 agent-b Mesh-A
expect_count 0 agent-b Mesh-B
: >"$other_signal"
wait "$other_pid"
probe mailbox-receive "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_B" CYNAPSA_EJABBERD_PASSWORD_B \
  Mesh-A exact-resource-only >/dev/null

# An online SM destination receives the frame but sends no SM ack. After the
# one-second resume window expires, a new session drains the same stable row.
ready_signal="$WORK/sm-ready"
drop_signal="$WORK/sm-drop"
probe mailbox-hold-sm "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_B" CYNAPSA_EJABBERD_PASSWORD_B Mesh-A \
  "$ready_signal" "$drop_signal" >"$WORK/sm.log" 2>&1 &
sm_pid=$!
while [ ! -f "$ready_signal" ]; do
  if ! kill -0 "$sm_pid" 2>/dev/null; then
    cat "$WORK/sm.log" >&2
    printf '%s\n' 'stream-management probe exited before readiness' >&2
    exit 1
  fi
  sleep 0.1
done
probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_A" CYNAPSA_EJABBERD_PASSWORD_A \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-A sm-timeout >/dev/null
expect_count 1 agent-b Mesh-A
: >"$drop_signal"
wait "$sm_pid"
sleep 2
expect_count 1 agent-b Mesh-A
probe mailbox-receive "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_B" CYNAPSA_EJABBERD_PASSWORD_B \
  Mesh-A sm-timeout >/dev/null
expect_count 0 agent-b Mesh-A

# A fresh exact-resource epoch must both drain an old row and remain eligible
# for later admissions from a newly authenticated sender. This mirrors the
# deep E2E ordering: destination resume expiry, fresh authority sync, then a
# different client session sends new traffic. Both rows are deleted only after
# this replacement stream explicitly acknowledges them.
replacement_ready="$WORK/replacement-first-acked"
hold_ready="$WORK/replacement-old-ready"
hold_drop="$WORK/replacement-old-drop"
probe mailbox-hold-sm "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_B" CYNAPSA_EJABBERD_PASSWORD_B Mesh-A \
  "$hold_ready" "$hold_drop" >"$WORK/replacement-old.log" 2>&1 &
hold_pid=$!
while [ ! -f "$hold_ready" ]; do
  if ! kill -0 "$hold_pid" 2>/dev/null; then
    cat "$WORK/replacement-old.log" >&2
    printf '%s\n' 'replacement old session exited before readiness' >&2
    exit 1
  fi
  sleep 0.1
done
probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_A" CYNAPSA_EJABBERD_PASSWORD_A \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-A replacement-before-expiry >/dev/null
expect_count 1 agent-b Mesh-A
: >"$hold_drop"
wait "$hold_pid"
sleep 2
expect_count 1 agent-b Mesh-A
probe mailbox-receive-sequence "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_B" CYNAPSA_EJABBERD_PASSWORD_B Mesh-A \
  replacement-before-expiry "$replacement_ready" replacement-after-expiry \
  >"$WORK/replacement-new.log" 2>&1 &
replacement_pid=$!
while [ ! -f "$replacement_ready" ]; do
  if ! kill -0 "$replacement_pid" 2>/dev/null; then
    cat "$WORK/replacement-new.log" >&2
    printf '%s\n' 'replacement session exited before first SM ack' >&2
    exit 1
  fi
  sleep 0.1
done
wait_count 0 agent-b Mesh-A
probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_A" CYNAPSA_EJABBERD_PASSWORD_A \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-A replacement-after-expiry >/dev/null
wait "$replacement_pid"
wait_count 0 agent-b Mesh-A
grep -q 'mailbox-replacement-sequence=replacement-before-expiry,replacement-after-expiry' \
  "$WORK/replacement-new.log"
container=$(sed -n '1p' "$STATE/container-id")
docker logs "$container" >"$WORK/mailbox-audit.log" 2>&1
# Admission proves enqueue into the exact live c2s process; only the later
# resource-scoped SM acknowledgement proves recipient handling and deletes the
# durable row.
grep -q 'cynapsa_mailbox admission .*dispatch=:c2s_enqueued custody=:ok' \
  "$WORK/mailbox-audit.log"
grep -q 'cynapsa_mailbox ack .*mode=:sm .*result={:ok,' \
  "$WORK/mailbox-audit.log"

# A membership removal invalidates authority but preserves the retained
# resource and its indivisible XEP-0198 FIFO. This recipient answers SM
# requests with a frozen pre-message handled count while continuing to process
# stanzas. The transport stays responsive, but neither sender row can retire
# before mutation. The removed sender's row is purged, the retained sender's
# row survives, and the retained session gets snapshot-required control
# without being kicked. Only the retained row replays after an ejabberd
# restart and fresh recipient authentication.
admin add agent-c Mesh-A >/dev/null
mixed_ready="$WORK/mixed-sm-ready"
mixed_mutated="$WORK/mixed-mutated"
probe mailbox-wait-preserved-sm \
  "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_B" CYNAPSA_EJABBERD_PASSWORD_B Mesh-A \
  "$mixed_ready" "$mixed_mutated" mixed-removed mixed-retained \
  >"$WORK/mixed-sm.log" 2>&1 &
mixed_pid=$!
while [ ! -f "$mixed_ready" ]; do
  if ! kill -0 "$mixed_pid" 2>/dev/null; then
    cat "$WORK/mixed-sm.log" >&2
    printf '%s\n' 'mixed SM recipient exited before readiness' >&2
    exit 1
  fi
  sleep 0.1
done
probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_A" CYNAPSA_EJABBERD_PASSWORD_A \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-A mixed-removed \
  "$CYNAPSA_EJABBERD_AGENT_C" >/dev/null
probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_C" CYNAPSA_EJABBERD_PASSWORD_C \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-A mixed-retained \
  "$CYNAPSA_EJABBERD_AGENT_A" >/dev/null
wait_count 2 agent-b Mesh-A
admin remove agent-a Mesh-A >/dev/null
: >"$mixed_mutated"
wait "$mixed_pid"
grep -q 'mailbox-resumable-stream=preserved' "$WORK/mixed-sm.log"
grep -q 'mailbox-frozen-sm-frames=2 requests=' "$WORK/mixed-sm.log"
expect_count 1 agent-b Mesh-A
"$HARNESS" restart "$STATE"
expect_count 1 agent-b Mesh-A
probe mailbox-receive "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_B" CYNAPSA_EJABBERD_PASSWORD_B \
  Mesh-A mixed-retained >"$WORK/mixed-replay.log"
grep -q 'mailbox-received=mixed-retained' "$WORK/mixed-replay.log"
expect_count 0 agent-b Mesh-A
admin add agent-a Mesh-A >/dev/null
admin remove agent-c Mesh-A >/dev/null

# Recipient and sender removals both purge exact-resource rows, while another
# mesh remains isolated. Re-add never resurrects the purged rows.
probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_A" CYNAPSA_EJABBERD_PASSWORD_A \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-A remove-recipient >/dev/null
admin remove agent-b Mesh-A >/dev/null
expect_count 0 agent-b Mesh-A
admin add agent-b Mesh-A >/dev/null
expect_count 0 agent-b Mesh-A

probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_A" CYNAPSA_EJABBERD_PASSWORD_A \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-A remove-sender >/dev/null
probe mailbox-send "$CYNAPSA_EJABBERD_ENDPOINT" "$CYNAPSA_EJABBERD_CA" \
  "$CYNAPSA_EJABBERD_AGENT_A" CYNAPSA_EJABBERD_PASSWORD_A \
  "$CYNAPSA_EJABBERD_AGENT_B" Mesh-B preserve-other-mesh >/dev/null
admin remove agent-a Mesh-A >/dev/null
expect_count 0 agent-b Mesh-A
expect_count 1 agent-b Mesh-B
admin add agent-a Mesh-A >/dev/null
expect_count 0 agent-b Mesh-A

printf '%s\n' 'resource-mailbox-e2e=passed'
cleanup
trap - INT TERM HUP EXIT
