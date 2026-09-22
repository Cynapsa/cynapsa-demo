#!/bin/sh
set -eu

read_secret() {
  path=$1
  variable=$2
  test -s "$path" || {
    echo "required Auth0 emulator secret is missing: $variable" >&2
    exit 70
  }
  value=$(cat "$path")
  export "$variable=$value"
}

read_secret /run/cynapsa/auth0-client-secret AUTH0_EMULATOR_CLIENT_SECRET
read_secret /run/cynapsa/auth0-bootstrap-secret AUTH0_EMULATOR_BOOTSTRAP_SECRET
exec python -m app
