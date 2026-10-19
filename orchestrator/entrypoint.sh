#!/bin/sh
set -eu

state_directory=${CYNAPSA_STATE_DIRECTORY:-/var/lib/cynapsa}
private_directory=${DEMO_PRIVATE_DIRECTORY:-/tmp/cynapsa-private}
secret_source=${DEMO_SECRET_SOURCE_DIRECTORY:-/run/secrets}
mkdir -p "$state_directory" "$private_directory"
chmod 700 "$state_directory" "$private_directory"

if [ -n "${LITELLM_API_KEY:-}" ]; then
  printf '%s' "$LITELLM_API_KEY" > "$private_directory/litellm_api_key"
  chmod 600 "$private_directory/litellm_api_key"
elif [ -s "$secret_source/litellm_api_key" ]; then
  cp "$secret_source/litellm_api_key" "$private_directory/litellm_api_key"
  chmod 600 "$private_directory/litellm_api_key"
fi
unset LITELLM_API_KEY
[ -s "$private_directory/litellm_api_key" ] || { echo "LiteLLM API key is required" >&2; exit 78; }

profile_present=false
for profile_path in "$state_directory"/profile-v2-*.state; do
  if [ -f "$profile_path" ]; then profile_present=true; break; fi
done

if [ "$profile_present" = false ] && \
   [ -n "${DEMO_STATE_KEY_B64:-}" ] && \
   [ -n "${DEMO_STATE_PROFILE_B64:-}" ]; then
  profile_filename=${DEMO_STATE_PROFILE_FILENAME:-}
  case "$profile_filename" in
    profile-v2-*.state) case "$profile_filename" in */*) exit 78;; esac ;;
    *) echo "invalid state profile filename" >&2; exit 78 ;;
  esac
  printf '%s' "$DEMO_STATE_KEY_B64" | base64 -d > "$state_directory/.auth-v2.key"
  printf '%s' "$DEMO_STATE_PROFILE_B64" | base64 -d > "$state_directory/$profile_filename"
  chmod 600 "$state_directory/.auth-v2.key" "$state_directory/$profile_filename"
  profile_present=true
fi
unset DEMO_STATE_KEY_B64 DEMO_STATE_PROFILE_B64 DEMO_STATE_PROFILE_FILENAME

# An explicit image command remains available for local Docker overrides.
if [ "$#" -gt 0 ]; then
  unset CYNAPSA_TOKEN
  exec "$@"
fi

run_command=${DEMO_RUN_COMMAND:-"cynapsa run --config $private_directory/cynapsa.json -- app:app"}
case "$run_command" in
  "uvicorn app:app --host 0.0.0.0 --port 8000 --no-access-log")
    : "${DEMO_HTTP_ACCESS_KEY:?DEMO_HTTP_ACCESS_KEY is required for HTTP mode}"
    unset CYNAPSA_TOKEN
    export DEMO_TRANSPORT=http
    exec uvicorn app:app --host 0.0.0.0 --port 8000 --no-access-log
    ;;
  "cynapsa run --config $private_directory/cynapsa.json -- app:app") ;;
  *) echo "unsupported DEMO_RUN_COMMAND" >&2; exit 64 ;;
esac

: "${DEMO_MESH_ID:?DEMO_MESH_ID is required}"
: "${DEMO_MAPS_AGENT_ID:?DEMO_MAPS_AGENT_ID is required}"
if [ -n "${DEMO_FILES_AGENT_ID:-}" ]; then
  export DEMO_FILES_ORIGIN=${DEMO_FILES_ORIGIN:-http://demo-files.local}
else
  unset DEMO_FILES_ORIGIN
fi

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
python3 "$(dirname "$0")/make_config.py" "$private_directory/cynapsa.json"
export DEMO_TRANSPORT=cynapsa
if [ -n "${ECS_CONTAINER_METADATA_URI_V4:-}" ]; then
  python3 /app/health_listener.py &
fi
export CYNAPSA_CLI_NETWORK_EVENTS=1
exec cynapsa run --config "$private_directory/cynapsa.json" -- app:app
