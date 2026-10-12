#!/bin/sh
set -eu

state_directory=${CYNAPSA_STATE_DIRECTORY:-/var/lib/cynapsa}
private_directory=${DEMO_PRIVATE_DIRECTORY:-/tmp/cynapsa-private}
secret_source=${DEMO_SECRET_SOURCE_DIRECTORY:-/run/secrets}
origin=${DEMO_ORCHESTRATOR_ORIGIN:-http://demo-orchestrator.local}
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

set -- cynapsa run --mesh-id "${DEMO_MESH_ID:?DEMO_MESH_ID is required}" \
  --profile-id demo-client --map "$origin" "$DEMO_ORCHESTRATOR_AGENT_ID" rpc \
  --rpc-timeout-ms 140000
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
  unset CYNAPSA_TOKEN
  set -- "$@" --token-file "$private_directory/enrollment_token"
  if [ "${DEMO_FORCE_ENROLL:-}" = 1 ]; then
    set -- "$@" --force-enroll
  fi
fi

unset CYNAPSA_TOKEN
if [ "${DEMO_CLIENT_QUIET:-}" = 1 ]; then
  exec "$@" -- python /app/app.py --quiet
fi
exec "$@" -- python /app/app.py
