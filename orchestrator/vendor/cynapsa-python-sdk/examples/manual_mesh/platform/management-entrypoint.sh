#!/bin/sh
set -eu

read_secret() {
  value=$(cat "$1")
  export "$2=$value"
}

read_secret /run/cynapsa/secrets/postgres-password CYNAPSA_POSTGRES_PASSWORD
read_secret /run/cynapsa/secrets/auth0-client-secret AUTH0_M2M_CLIENT_SECRET
read_secret /run/cynapsa/secrets/management-jwt-secret JWT_SECRET_KEY
read_secret /run/cynapsa/secrets/enrollment-credential-key ENROLLMENT_CREDENTIAL_KEY
read_secret /run/cynapsa/secrets/management-enrollment-service-token ENROLLMENT_SERVICE_TOKEN
read_secret /run/cynapsa/secrets/ejabberd-api-password EJABBERD_ADMIN_PASSWORD

export DATABASE_URL="postgresql+asyncpg://cynapsa:${CYNAPSA_POSTGRES_PASSWORD}@postgres:5432/cynapsa"
export PYTHONPATH=/app
unset CYNAPSA_POSTGRES_PASSWORD

alembic upgrade head
exec uvicorn main:app \
  --host 0.0.0.0 \
  --port 8443 \
  --ssl-certfile /run/cynapsa/tls.crt \
  --ssl-keyfile /run/cynapsa/tls.key \
  --no-access-log
