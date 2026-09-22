#!/bin/sh
set -eu

secret_path=/run/cynapsa/ejabberd-api-password
test -s "$secret_path" || {
  echo 'required ejabberd API credential is missing' >&2
  exit 70
}

export ERL_CRASH_DUMP=/dev/null
/opt/ejabberd/bin/ejabberdctl foreground &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true' TERM INT EXIT

attempt=0
until /opt/ejabberd/bin/ejabberdctl status >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  test "$attempt" -lt 60 || {
    echo 'ejabberd did not become ready' >&2
    exit 1
  }
  sleep 1
done

registration='case file:read_file("/run/cynapsa/ejabberd-api-password") of {ok, Secret} when byte_size(Secret) > 0 -> ejabberd_auth:try_register(<<"admin">>, <<"mesh.test">>, Secret); _ -> erlang:error(missing_admin_secret) end.'
if ! printf '%s\n' "$registration" \
  | /opt/ejabberd-26.04/erts-16.3.1/bin/erl_call -sname ejabberd -e -no_result_term; then
  echo 'ejabberd controller account registration failed' >&2
  exit 1
fi

wait "$server_pid"
