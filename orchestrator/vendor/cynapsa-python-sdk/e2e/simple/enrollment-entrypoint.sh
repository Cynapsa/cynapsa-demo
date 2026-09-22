#!/bin/sh
set -eu

secret_path=/run/cynapsa/secrets/enrollment-management-service-token
test -s "$secret_path" || {
  echo 'required Enrollment service credential is missing' >&2
  exit 70
}
MANAGEMENT_SERVICE_TOKEN=$(cat "$secret_path")
export MANAGEMENT_SERVICE_TOKEN

exec uvicorn enrollment.app:app \
  --host 0.0.0.0 \
  --port "${PORT:-443}" \
  --ssl-certfile /run/cynapsa/tls.crt \
  --ssl-keyfile /run/cynapsa/tls.key \
  --no-access-log
