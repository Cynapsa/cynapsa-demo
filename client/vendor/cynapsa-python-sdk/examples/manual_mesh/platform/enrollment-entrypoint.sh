#!/bin/sh
set -eu

MANAGEMENT_SERVICE_TOKEN=$(cat /run/cynapsa/enrollment-management-service-token)
export MANAGEMENT_SERVICE_TOKEN

exec uvicorn enrollment.app:app \
  --host 0.0.0.0 \
  --port 443 \
  --ssl-certfile /run/cynapsa/tls.crt \
  --ssl-keyfile /run/cynapsa/tls.key \
  --no-access-log
