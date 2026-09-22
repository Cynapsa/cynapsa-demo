#!/bin/sh
set -eu

state_directory=${CYNAPSA_STATE_DIRECTORY:-/var/lib/cynapsa}
private_directory=${DEMO_PRIVATE_DIRECTORY:-/tmp/cynapsa-private}
token_source=/run/secrets/enrollment_token

mkdir -p "$state_directory" "$private_directory"
chmod 700 "$state_directory" "$private_directory"

profile_present=false
for profile_path in "$state_directory"/profile-v2-*.state; do
  if [ -f "$profile_path" ]; then
    profile_present=true
    break
  fi
done

if [ "$profile_present" = false ]; then
  if [ ! -s "$token_source" ]; then
    echo "no saved Cynapsa profile; an enrollment token is required" >&2
    exit 78
  fi
  cp "$token_source" "$private_directory/enrollment_token"
  chmod 600 "$private_directory/enrollment_token"
  exec python /app/app.py --enroll
fi

exec python /app/app.py
