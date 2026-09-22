#!/bin/sh
set -eu

AUTH0_EMULATOR_CLIENT_SECRET=$(cat /run/cynapsa/auth0-client-secret)
AUTH0_EMULATOR_BOOTSTRAP_SECRET=$(cat /run/cynapsa/auth0-bootstrap-secret)
export AUTH0_EMULATOR_CLIENT_SECRET AUTH0_EMULATOR_BOOTSTRAP_SECRET

exec python -m app
