#!/bin/sh
set -eu

state_directory=${CYNAPSA_STATE_DIRECTORY:-/var/lib/cynapsa}
private_directory=${DEMO_PRIVATE_DIRECTORY:-/tmp/cynapsa-private}
secret_source=${DEMO_SECRET_SOURCE_DIRECTORY:-/run/secrets}
origin=${DEMO_ORCHESTRATOR_ORIGIN:-http://demo-orchestrator.local}

run_command=${DEMO_RUN_COMMAND:-"cynapsa run --config $private_directory/cynapsa.json -- python /app/app.py"}
case "$run_command" in
  "python /app/app.py")
    : "${DEMO_HTTP_ACCESS_KEY:?DEMO_HTTP_ACCESS_KEY is required for HTTP mode}"
    unset CYNAPSA_TOKEN
    export DEMO_TRANSPORT=http
    if [ "${DEMO_CLIENT_QUIET:-}" = 1 ]; then
      exec python /app/app.py --quiet
    fi
    exec python /app/app.py
    ;;
  "cynapsa run --config $private_directory/cynapsa.json -- python /app/app.py") ;;
  *) echo "unsupported DEMO_RUN_COMMAND" >&2; exit 64 ;;
esac

: "${DEMO_ORCHESTRATOR_AGENT_ID:?DEMO_ORCHESTRATOR_AGENT_ID is required}"

mkdir -p "$state_directory" "$private_directory"
chmod 700 "$state_directory" "$private_directory"

profile_present=false
for profile_path in "$state_directory"/profile-v2-*.state; do
  if [ -f "$profile_path" ]; then
    profile_present=true
    break
  fi
done

: "${DEMO_MESH_ID:?DEMO_MESH_ID is required}"
if [ "${DEMO_FORCE_ENROLL:-}" = 1 ] || [ "$profile_present" = false ]; then
  if [ -n "${CYNAPSA_TOKEN:-}" ]; then
    printf '%s' "$CYNAPSA_TOKEN" > "$private_directory/enrollment_token"
  elif [ -s "$secret_source/enrollment_token" ]; then
    cp "$secret_source/enrollment_token" "$private_directory/enrollment_token"
  else
    echo "an enrollment token is required for enrollment or --force-enroll" >&2
    exit 78
  fi
  chmod 600 "$private_directory/enrollment_token"
fi

unset CYNAPSA_TOKEN
export DEMO_ORCHESTRATOR_ORIGIN=$origin
python3 "$(dirname "$0")/make_config.py" "$private_directory/cynapsa.json"
export DEMO_TRANSPORT=cynapsa
if [ "${DEMO_CLIENT_QUIET:-}" = 1 ]; then
  exec cynapsa run --config "$private_directory/cynapsa.json" -- python /app/app.py --quiet
fi
export CYNAPSA_CLI_NETWORK_EVENTS=1
exec cynapsa run --config "$private_directory/cynapsa.json" -- python /app/app.py
