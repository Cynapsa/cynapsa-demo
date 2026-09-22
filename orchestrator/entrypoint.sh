#!/bin/sh
set -eu

state_directory=${CYNAPSA_STATE_DIRECTORY:-/var/lib/cynapsa}
private_directory=${DEMO_PRIVATE_DIRECTORY:-/tmp/cynapsa-private}
secret_source=${DEMO_SECRET_SOURCE_DIRECTORY:-/run/secrets}
mkdir -p "$state_directory" "$private_directory"
chmod 700 "$state_directory" "$private_directory"

if [ -n "${GEMINI_API_KEY:-}" ]; then
  printf '%s' "$GEMINI_API_KEY" > "$private_directory/gemini_api_key"
  chmod 600 "$private_directory/gemini_api_key"
elif [ -s "$secret_source/gemini_api_key" ]; then
  cp "$secret_source/gemini_api_key" "$private_directory/gemini_api_key"
  chmod 600 "$private_directory/gemini_api_key"
elif [ -n "${DEMO_GEMINI_API_KEY:-}" ]; then
  printf '%s' "$DEMO_GEMINI_API_KEY" > "$private_directory/gemini_api_key"
  chmod 600 "$private_directory/gemini_api_key"
fi
unset GEMINI_API_KEY DEMO_GEMINI_API_KEY
[ -s "$private_directory/gemini_api_key" ] || { echo "Gemini API key is required" >&2; exit 78; }

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

if [ "$profile_present" = false ]; then
  if [ -n "${CYNAPSA_TOKEN:-}" ]; then
    printf '%s' "$CYNAPSA_TOKEN" > "$private_directory/enrollment_token"
  elif [ -s "$secret_source/enrollment_token" ]; then
    cp "$secret_source/enrollment_token" "$private_directory/enrollment_token"
  else
    echo "no saved Cynapsa profile; an enrollment token is required" >&2
    exit 78
  fi
  chmod 600 "$private_directory/enrollment_token"
  unset CYNAPSA_TOKEN
  exec python /app/app.py --enroll
fi
unset CYNAPSA_TOKEN
exec python /app/app.py
