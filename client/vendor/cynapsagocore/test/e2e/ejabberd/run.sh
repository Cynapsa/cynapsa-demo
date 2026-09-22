#!/bin/sh
set -eu

IMAGE='ghcr.io/processone/ejabberd@sha256:68482e33ff11934e73ef2881cf455ccefc1e03b31203f3ab651f4b2b1152b4a8'
MESH_MODULE_SHA256='149bd73a6e3752a8e81f0234386f92f1011fe4902337f5c52bab22c90e5fbfcb'
ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)

die() {
  printf '%s\n' "ejabberd-e2e: $*" >&2
  exit 1
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"
}

file_mode() {
  path=$1
  if mode=$(stat -f '%Lp' "$path" 2>/dev/null); then
    printf '%s\n' "$mode"
    return
  fi
  if mode=$(stat -c '%a' "$path" 2>/dev/null); then
    printf '%s\n' "$mode"
    return
  fi
  die "cannot inspect staged file mode"
}

require_file_mode() {
  path=$1
  expected=$2
  label=$3
  [ -f "$path" ] || die "$label is unavailable"
  actual=$(file_mode "$path")
  [ "$actual" = "$expected" ] || die "$label has mode $actual; expected $expected"
}

require_public_mount() {
  path=$1
  label=$2
  require_file_mode "$path" 444 "$label"
  [ -r "$path" ] || die "$label is not host-readable"
  # Exact mode 0444 makes this bind-mounted input readable by the image's
  # unprivileged uid 9000 without exposing anything secret-bearing.
  find "$path" -prune -type f -perm -004 -print | grep -q . ||
    die "$label is not readable by container uid 9000"
}

require_private_secret_file() {
  path=$1
  label=$2
  case "$path" in
    /*) ;;
    *) die "$label path must be absolute" ;;
  esac
  [ ! -L "$path" ] || die "$label must not be a symbolic link"
  require_file_mode "$path" 600 "$label"
  [ -r "$path" ] || die "$label is not host-readable"
}

inject_turn_rest_secret() {
  secret_file=$1
  input=$2
  output=$3
  # The secret is read as input data, never interpolated into an awk/sed/python
  # program or argument. Lowercase hexadecimal has no replacement metacharacters.
  awk '
    FILENAME == ARGV[1] {
      secret_lines++
      if (secret_lines != 1 || length($0) != 64 || $0 !~ /^[0-9a-f]+$/) exit 70
      secret=$0
      next
    }
    FILENAME == ARGV[2] {
      replacements += gsub(/__TURN_REST_SECRET__/, secret)
      print
    }
    END {
      if (secret_lines != 1 || replacements != 1) exit 71
    }
  ' "$secret_file" "$input" >"$output"
}

allocate_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

render_config() {
  input=$1
  output=$2
  profile=$3
  https_port=$4
  max_stanza=262144
  resume_timeout=300
  max_upload=134217760
  put_url="https://127.0.0.1:${https_port}/upload"
  upload_handler=mod_http_upload

  turn_rest_secret_file=${CYNAPSA_EJABBERD_TURN_REST_SECRET_FILE:-}

  case "$profile" in
    normal|offline|shared-group) ;;
    shared-group-mailbox) resume_timeout=1 ;;
    shared-group-extdisco)
      if [ -z "$turn_rest_secret_file" ]; then
        die 'XEP-0215 TURN REST secret is unavailable'
      fi
      require_private_secret_file "$turn_rest_secret_file" 'XEP-0215 TURN REST secret file'
      ;;
    # Zero disables resumability during initial XEP-0198 enablement. A one-second
    # window keeps establishment valid while making an expired resume fixture
    # deterministic and bounded.
    resume-rejected) resume_timeout=1 ;;
    upload-disabled) ;;
    upload-unavailable|upload-failing) put_url='https://127.0.0.1:1/upload' ;;
    quota-rejected) max_upload=32 ;;
    download-failing) ;;
    chunk-rejected) max_stanza=1024 ;;
    *) die "unknown profile: $profile" ;;
  esac

  if [ "$profile" = shared-group ] || [ "$profile" = shared-group-mailbox ] || [ "$profile" = shared-group-extdisco ]; then
    upload_handler=mod_cynapsa_mesh
  fi

  sed \
    -e "s|__MAX_STANZA_SIZE__|${max_stanza}|g" \
    -e "s|__RESUME_TIMEOUT__|${resume_timeout}|g" \
    -e "s|__PUT_URL__|${put_url}|g" \
    -e "s|__MAX_UPLOAD_SIZE__|${max_upload}|g" \
    -e "s|__UPLOAD_HANDLER__|${upload_handler}|g" \
    "$input" >"$output.tmp"

  if [ "$profile" = upload-disabled ]; then
    awk '
      /PROFILE_UPLOAD_LISTENER_BEGIN/ { route=1; next }
      /PROFILE_UPLOAD_LISTENER_END/ { route=0; next }
      /PROFILE_UPLOAD_MODULE_BEGIN/ { module=1; next }
      /PROFILE_UPLOAD_MODULE_END/ { module=0; next }
      !route && !module { print }
    ' "$output.tmp" >"$output"
    rm -f "$output.tmp"
  else
    mv "$output.tmp" "$output"
  fi

  if [ "$profile" != shared-group ] && [ "$profile" != shared-group-mailbox ] && [ "$profile" != shared-group-extdisco ]; then
    awk '
      /PROFILE_SHARED_GROUP_RESOURCE_BEGIN/ { resource=1; next }
      /PROFILE_SHARED_GROUP_RESOURCE_END/ { resource=0; next }
      /PROFILE_SHARED_GROUP_MODULE_BEGIN/ { group=1; next }
      /PROFILE_SHARED_GROUP_MODULE_END/ { group=0; next }
      !resource && !group { print }
    ' "$output" >"$output.no-group"
    mv "$output.no-group" "$output"
  fi

  if [ "$profile" != shared-group-extdisco ]; then
    awk '
      /PROFILE_EXTDISCO_MODULE_BEGIN/ { extdisco=1; next }
      /PROFILE_EXTDISCO_MODULE_END/ { extdisco=0; next }
      !extdisco { print }
    ' "$output" >"$output.no-extdisco"
    mv "$output.no-extdisco" "$output"
  fi

  if [ "$profile" != shared-group ] && [ "$profile" != shared-group-mailbox ]; then
    awk '
      /PROFILE_EXTDISCO_EMPTY_BEGIN/ { extdisco=1; next }
      /PROFILE_EXTDISCO_EMPTY_END/ { extdisco=0; next }
      !extdisco { print }
    ' "$output" >"$output.no-empty-extdisco"
    mv "$output.no-empty-extdisco" "$output"
  fi

  if [ "$profile" = shared-group-extdisco ]; then
    secret_output="$output.with-turn-secret"
    rm -f "$secret_output"
    if ! (umask 077; inject_turn_rest_secret "$turn_rest_secret_file" "$output" "$secret_output"); then
      rm -f "$secret_output" "$output" "$output.tmp" "$output.no-group" "$output.no-extdisco" "$output.no-empty-extdisco"
      die 'XEP-0215 TURN REST secret file is malformed or the configuration placeholder is invalid'
    fi
    mv "$secret_output" "$output"
    chmod 0600 "$output"
    require_file_mode "$output" 600 'secret-bearing rendered ejabberd configuration'
  else
    chmod 0444 "$output"
    require_public_mount "$output" 'rendered ejabberd configuration'
  fi
  if grep -q '__TURN_REST_SECRET__' "$output"; then
    die 'rendered ejabberd configuration retained a TURN REST placeholder'
  fi
}

prepare_mesh_module() {
  state=$1
  module_root=$(CDPATH='' cd -- "$ROOT/../../.." && pwd)
  source_file="$module_root/server/ejabberd/mod_cynapsa_mesh/src/mod_cynapsa_mesh.erl"
  [ -f "$source_file" ] || die "shared-group module source unavailable: $source_file"
  mkdir -p "$state/module-source"
  staged_source="$state/module-source/mod_cynapsa_mesh.erl"
  cp "$source_file" "$staged_source"
  cmp -s "$source_file" "$staged_source" || die "shared-group staged source verification failed"
  chmod 0555 "$state/module-source"
  chmod 0444 "$staged_source"
  require_public_mount "$staged_source" 'staged shared-group module source'
  mkdir -p "$state/module"
  chmod 0777 "$state/module"

  docker run --rm \
    --network none \
    --read-only \
    --cpus 1 \
    --memory 512m \
    --pids-limit 128 \
    --security-opt no-new-privileges \
    --cap-drop ALL \
    --cap-add NET_BIND_SERVICE \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m,uid=9000,gid=9000 \
    --mount "type=bind,src=${staged_source},dst=/src/mod_cynapsa_mesh.erl,readonly" \
    --mount "type=bind,src=${state}/module,dst=/out" \
    --entrypoint /bin/sh \
    "$IMAGE" -c '
      set -eu
      cd /opt/ejabberd-26.04/bin
      ERL=../erts-16.3.1/bin/erl
      BOOT=../releases/26.4.0/start_clean
      LIB=../lib
      EJABBERD_INCLUDE=../lib/ejabberd-26.4.0/include
      XMPP_INCLUDE=../lib/xmpp-1.13.3/include
      "$ERL" +S 2:2 +A 2 -noshell -boot "$BOOT" -boot_var RELEASE_LIB "$LIB" \
        -pa "$LIB/ejabberd-26.4.0/ebin" -pa "$LIB/xmpp-1.13.3/ebin" \
        -eval "case compile:file(\"/src/mod_cynapsa_mesh.erl\", [deterministic,warnings_as_errors,{i,\"$EJABBERD_INCLUDE\"},{i,\"$XMPP_INCLUDE\"},{outdir,\"/out\"},report]) of {ok,_}->halt(0); Other->io:format(\"compile failed: ~p~n\",[Other]),halt(1) end."
      mkdir -p /tmp/test-ebin
      "$ERL" +S 2:2 +A 2 -noshell -boot "$BOOT" -boot_var RELEASE_LIB "$LIB" \
        -pa /tmp/test-ebin -pa "$LIB/ejabberd-26.4.0/ebin" -pa "$LIB/xmpp-1.13.3/ebin" \
        -eval "case compile:file(\"/src/mod_cynapsa_mesh.erl\", [{d,list_to_atom(\"TEST\")},deterministic,warnings_as_errors,{i,\"$EJABBERD_INCLUDE\"},{i,\"$XMPP_INCLUDE\"},{outdir,\"/tmp/test-ebin\"},report]) of {ok,_}->code:load_abs(\"/tmp/test-ebin/mod_cynapsa_mesh\"),case mod_cynapsa_mesh:test() of ok->halt(0); TestOther->io:format(\"tests failed: ~p~n\",[TestOther]),halt(1) end; Other->io:format(\"test compile failed: ~p~n\",[Other]),halt(1) end."
    ' \
    >"$state/module-compile.log" 2>&1 || {
      sed -n '1,200p' "$state/module-compile.log" >&2
      die "shared-group module compilation failed"
    }

  [ -f "$state/module/mod_cynapsa_mesh.beam" ] || die "shared-group compiler produced no beam"
  chmod 0444 "$state/module/mod_cynapsa_mesh.beam"
  shasum -a 256 "$state/module/mod_cynapsa_mesh.beam" >"$state/module.sha256"
  chmod 0600 "$state/module.sha256" "$state/module-compile.log"
  actual_digest=$(sed -n '1s/ .*//p' "$state/module.sha256")
  [ "$actual_digest" = "$MESH_MODULE_SHA256" ] || die "shared-group module digest mismatch"
  chmod 0555 "$state/module"
  require_public_mount "$state/module/mod_cynapsa_mesh.beam" 'compiled shared-group module'
}

generate_certificates() {
  state=$1
  openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 2 \
    -subj '/CN=Cynapsa Pod 6 Test CA' \
    -keyout "$state/ca-key.pem" -out "$state/ca.pem" >/dev/null 2>&1
  chmod 0600 "$state/ca-key.pem"
  openssl req -newkey rsa:2048 -sha256 -nodes -subj '/CN=mesh.test' \
    -addext 'subjectAltName=DNS:mesh.test,IP:127.0.0.1' \
    -keyout "$state/server-key.pem" -out "$state/server.csr" >/dev/null 2>&1
  chmod 0600 "$state/server-key.pem" "$state/server.csr"
  printf '%s\n' 'subjectAltName=DNS:mesh.test,IP:127.0.0.1' 'extendedKeyUsage=serverAuth' >"$state/server.ext"
  chmod 0600 "$state/server.ext"
  openssl x509 -req -sha256 -days 2 -in "$state/server.csr" \
    -CA "$state/ca.pem" -CAkey "$state/ca-key.pem" -CAcreateserial \
    -extfile "$state/server.ext" -out "$state/server-cert.pem" >/dev/null 2>&1
  chmod 0600 "$state/server-cert.pem"
  cp "$state/server-cert.pem" "$state/server.pem"
  chmod 0600 "$state/server.pem"
  cat "$state/server-key.pem" >>"$state/server.pem"
  chmod 0444 "$state/ca.pem"
  chmod 0600 "$state/server.pem"
  require_public_mount "$state/ca.pem" 'test CA certificate'
  require_file_mode "$state/server.pem" 600 'server certificate and private-key bundle'
}

stage_server_secrets() {
  state=$1
  volume=$2
  # The host copy remains mode 0600. A one-shot pinned container installs the
  # secret into the disposable volume as uid 9000 mode 0400, avoiding a
  # world-readable secret-bearing bind mount.
  docker run --rm \
    --user 0:0 \
    --network none \
    --read-only \
    --cpus 0.5 \
    --memory 128m \
    --pids-limit 32 \
    --security-opt no-new-privileges \
    --cap-drop ALL \
    --cap-add CHOWN \
    --cap-add DAC_OVERRIDE \
    --mount "type=volume,src=${volume},dst=/opt/ejabberd" \
    --mount "type=bind,src=${state}/server.pem,dst=/source/server.pem,readonly" \
    --mount "type=bind,src=${state}/ejabberd.yml,dst=/source/ejabberd.yml,readonly" \
    --entrypoint /bin/sh \
    "$IMAGE" -c '
      set -eu
      destination=/opt/ejabberd/conf/server.pem
      cp /source/server.pem "${destination}.tmp"
      chmod 0400 "${destination}.tmp"
      chown 9000:9000 "${destination}.tmp"
      mv "${destination}.tmp" "$destination"
      [ "$(stat -c %a "$destination")" = 400 ]
      [ "$(stat -c %u "$destination")" = 9000 ]
      [ -r "$destination" ]
      configuration=/opt/ejabberd/conf/ejabberd.yml
      cp /source/ejabberd.yml "${configuration}.tmp"
      chmod 0400 "${configuration}.tmp"
      chown 9000:9000 "${configuration}.tmp"
      mv "${configuration}.tmp" "$configuration"
      [ "$(stat -c %a "$configuration")" = 400 ]
      [ "$(stat -c %u "$configuration")" = 9000 ]
      [ -r "$configuration" ]
    ' >/dev/null 2>&1 || die 'failed to stage uid-9000 server secrets'
}

stage_server_configuration() {
  state=$1
  source=$2
  volume=$(sed -n '1p' "$state/volume")
  [ -f "$source" ] || die 'rendered server configuration is unavailable'
  require_file_mode "$source" 600 'secret-bearing rendered ejabberd configuration'
  docker run --rm \
    --user 0:0 \
    --network none \
    --read-only \
    --cpus 0.5 \
    --memory 128m \
    --pids-limit 32 \
    --security-opt no-new-privileges \
    --cap-drop ALL \
    --cap-add CHOWN \
    --cap-add DAC_OVERRIDE \
    --mount "type=volume,src=${volume},dst=/opt/ejabberd" \
    --mount "type=bind,src=${source},dst=/source/ejabberd.yml,readonly" \
    --entrypoint /bin/sh \
    "$IMAGE" -c '
      set -eu
      configuration=/opt/ejabberd/conf/ejabberd.yml
      cp /source/ejabberd.yml "${configuration}.tmp"
      chmod 0400 "${configuration}.tmp"
      chown 9000:9000 "${configuration}.tmp"
      mv "${configuration}.tmp" "$configuration"
      [ "$(stat -c %a "$configuration")" = 400 ]
      [ "$(stat -c %u "$configuration")" = 9000 ]
      [ -r "$configuration" ]
    ' >/dev/null 2>&1 || die 'failed to stage the uid-9000 server configuration'
}

container_id_for() {
  state=$1
  [ -f "$state/container-id" ] || return 1
  sed -n '1p' "$state/container-id"
}

capture_artifacts() {
  state=$1
  mkdir -p "$state/artifacts"
  container=$(container_id_for "$state" 2>/dev/null || true)
  if [ -n "$container" ]; then
    docker logs "$container" >"$state/artifacts/container.log" 2>&1 || true
    docker inspect "$container" \
      --format '{{json .State}}' >"$state/artifacts/container-state.json" 2>/dev/null || true
  fi
  if [ -f "$state/ejabberd.yml" ]; then
    sed 's/^\([[:space:]]*secret:[[:space:]]*\).*/\1"[REDACTED]"/' "$state/ejabberd.yml" >"$state/artifacts/ejabberd.yml"
  fi
  if [ -f "$state/profile" ]; then
    cp "$state/profile" "$state/artifacts/profile"
  fi
  if [ -f "$state/module-compile.log" ]; then
    cp "$state/module-compile.log" "$state/artifacts/module-compile.log"
  fi
  if [ -f "$state/module.sha256" ]; then
    cp "$state/module.sha256" "$state/artifacts/module.sha256"
  fi
  if [ -f "$state/mesh-readiness.log" ]; then
    cp "$state/mesh-readiness.log" "$state/artifacts/mesh-readiness.log"
  fi
  if [ -f "$state/admin.log" ]; then
    cp "$state/admin.log" "$state/artifacts/admin.log"
  fi
  for probe_log in "$state"/*-probe.log; do
    if [ -f "$probe_log" ]; then
      cp "$probe_log" "$state/artifacts/$(basename "$probe_log")"
    fi
  done
  find "$state/artifacts" -type f -exec chmod 600 {} \;
  if [ -n "${CYNAPSA_E2E_ARTIFACT_DIR:-}" ]; then
    artifact_root=$CYNAPSA_E2E_ARTIFACT_DIR
    artifact_run="$artifact_root/ejabberd-$(basename "$state")"
    mkdir -p "$artifact_run"
    chmod 0700 "$artifact_run"
    for artifact in "$state"/artifacts/*; do
      if [ -f "$artifact" ]; then
        cp "$artifact" "$artifact_run/$(basename "$artifact")"
      fi
    done
    find "$artifact_run" -type f -exec chmod 0600 {} \;
  fi
}

stop_environment() {
  state=$1
  [ -d "$state" ] || return 0
  capture_artifacts "$state"
  profile=$(sed -n '1p' "$state/profile" 2>/dev/null || true)
  container=$(container_id_for "$state" 2>/dev/null || true)
  network=$(sed -n '1p' "$state/network" 2>/dev/null || true)
  volume=$(sed -n '1p' "$state/volume" 2>/dev/null || true)
  if [ -n "$container" ]; then
    docker rm -f "$container" >/dev/null 2>&1 || true
  fi
  if [ -n "$network" ]; then
    docker network rm "$network" >/dev/null 2>&1 || true
  fi
  if [ -n "$volume" ]; then
    docker volume rm -f "$volume" >/dev/null 2>&1 || true
  fi
  rm -f "$state/credentials.env" "$state/ca-key.pem" "$state/ca-key.srl" \
    "$state/ca.srl" "$state/server-key.pem" "$state/server.csr" "$state/server.ext" "$state/server.pem"
  if [ "$profile" = shared-group-extdisco ]; then
    rm -f "$state/ejabberd.yml" "$state/ejabberd.yml.tmp" "$state/ejabberd-extdisco-all.yml"
  fi
}

wait_ready() {
  state=$1
  container=$(container_id_for "$state")
  deadline=$(( $(date +%s) + 40 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if docker exec "$container" ejabberdctl status >/dev/null 2>&1; then
      if openssl s_client -starttls xmpp -xmpphost mesh.test \
        -connect "127.0.0.1:$(sed -n '1p' "$state/c2s-port")" \
        -servername mesh.test -verify_return_error -CAfile "$state/ca.pem" \
        </dev/null >"$state/readiness.xml" 2>"$state/readiness-tls.log"; then
        grep -q 'Verification: OK' "$state/readiness-tls.log" || true
        return 0
      fi
    fi
    sleep 1
  done
  capture_artifacts "$state"
  die "protocol readiness timed out; artifacts: $state/artifacts"
}

wait_mesh_ready() {
  state=$1
  container=$(container_id_for "$state")
  output=$(docker exec "$container" ejabberdctl cynapsa_mesh_ready mesh.test)
  printf '%s\n' "$output" >"$state/mesh-readiness.log"
  expected=$(printf 'ready\tmesh.test\tsingle_node_mnesia')
  [ "$output" = "$expected" ] || die "mesh authority readiness mismatch"
}

probe_protocol() {
  state=$1
  mode=${2:-adapter}
  mesh=${CYNAPSA_EJABBERD_MESH:-readiness}
  module_root=$(CDPATH='' cd -- "$ROOT/../../.." && pwd)
  set -a
  # shellcheck disable=SC1091
  . "$state/credentials.env"
  set +a
  if (
    cd "$module_root"
    go run ./test/e2e/ejabberd/probe \
      "$mode" \
      "127.0.0.1:$(sed -n '1p' "$state/c2s-port")" \
      "$state/ca.pem" agent-a@mesh.test "$mesh"
  ) >"$state/${mode}-probe.log" 2>&1; then
    sed -n '1,200p' "$state/${mode}-probe.log"
  else
    probe_result=$?
    sed -n '1,200p' "$state/${mode}-probe.log" >&2
    return "$probe_result"
  fi
}

register_test_account() {
  container=$1
  username=$2
  password=$3
  validate_admin_token "$username" user
  # ejabberdctl requires the password as a positional argument. Feed it to a
  # short-lived Erlang distribution client instead, so neither the host Docker
  # command nor the in-container process table contains the generated secret.
  registration_status=0
  printf '%s\n%s\n' "$username" "$password" | docker exec -i \
    --workdir /opt/ejabberd-26.04/bin "$container" \
    /opt/ejabberd-26.04/erts-16.3.1/bin/erl \
      -boot ../releases/26.4.0/start_clean -boot_var RELEASE_LIB ../lib \
      -noshell -sname cynapsa_register -eval '
        case {io:get_line(""), io:get_line("")} of
          {eof, _} -> halt(2);
          {_, eof} -> halt(2);
          {UserLine, PasswordLine} ->
            User = iolist_to_binary(string:trim(UserLine)),
            Password = iolist_to_binary(string:trim(PasswordLine)),
            Result = rpc:call(ejabberd@localhost, ejabberd_auth, try_register,
                              [User, <<"mesh.test">>, Password]),
            halt(case Result of
              ok -> 0;
              {badrpc, nodedown} -> 3;
              {badrpc, _} -> 4;
              _ -> 5
            end)
        end.' >/dev/null 2>&1 || registration_status=$?
  [ "$registration_status" -eq 0 ] ||
    die "failed to register synthetic account $username (status $registration_status)"
}

start_environment() {
  profile=$1
  require_tool docker
  require_tool openssl
  require_tool python3
  state=$(mktemp -d "${TMPDIR:-/tmp}/cynapsa-ejabberd.XXXXXX")
  chmod 700 "$state"
  trap 'stop_environment "$state"' INT TERM HUP EXIT
  printf '%s\n' "$profile" >"$state/profile"
  suffix=$(basename "$state" | tr -cd 'a-zA-Z0-9')
  network="cynapsa-e2e-net-${suffix}"
  volume="cynapsa-e2e-opt-${suffix}"
  printf '%s\n' "$network" >"$state/network"
  printf '%s\n' "$volume" >"$state/volume"
  c2s_port=$(allocate_port)
  https_port=$(allocate_port)
  printf '%s\n' "$c2s_port" >"$state/c2s-port"
  printf '%s\n' "$https_port" >"$state/https-port"
  generate_certificates "$state"
  render_config "$ROOT/ejabberd.yml.in" "$state/ejabberd.yml" "$profile" "$https_port"
  if [ "$profile" = shared-group-extdisco ]; then
    cp "$state/ejabberd.yml" "$state/ejabberd-extdisco-all.yml"
    chmod 0600 "$state/ejabberd-extdisco-all.yml"
  fi
  if [ "$profile" = shared-group ] || [ "$profile" = shared-group-mailbox ] || [ "$profile" = shared-group-extdisco ]; then
    prepare_mesh_module "$state"
  fi

  # A unique bridge isolates each run; only the two random loopback-published
  # ports are reachable from the host test process.
  docker network create --driver bridge "$network" >/dev/null
  docker volume create "$volume" >/dev/null
  stage_server_secrets "$state" "$volume"
  set --
  if [ "$profile" = shared-group ] || [ "$profile" = shared-group-mailbox ] || [ "$profile" = shared-group-extdisco ]; then
    set -- \
      -e 'ERL_OPTIONS=-pa /opt/cynapsa/ebin' \
      --mount "type=bind,src=${state}/module,dst=/opt/cynapsa/ebin,readonly"
  fi
  container=$(docker run -d \
    --name "cynapsa-e2e-${suffix}" \
    --network "$network" \
    --read-only \
    --cpus 1 \
    --memory 512m \
    --pids-limit 256 \
    --security-opt no-new-privileges \
    --cap-drop ALL \
    --cap-add NET_BIND_SERVICE \
    -p "127.0.0.1:${c2s_port}:5222" \
    -p "127.0.0.1:${https_port}:5443" \
    --mount "type=volume,src=${volume},dst=/opt/ejabberd" \
    --mount "type=bind,src=${state}/ca.pem,dst=/opt/ejabberd/conf/ca.pem,readonly" \
    "$@" \
    --tmpfs /opt/ejabberd/database:rw,noexec,nosuid,nodev,size=64m,uid=9000,gid=9000 \
    --tmpfs /opt/ejabberd/logs:rw,noexec,nosuid,nodev,size=16m,uid=9000,gid=9000 \
    --tmpfs /opt/ejabberd/upload:rw,noexec,nosuid,nodev,size=32m,uid=9000,gid=9000 \
    "$IMAGE")
  printf '%s\n' "$container" >"$state/container-id"
  wait_ready "$state"
  if [ "$profile" = shared-group ] || [ "$profile" = shared-group-mailbox ] || [ "$profile" = shared-group-extdisco ]; then
    wait_mesh_ready "$state"
  fi

  password_a=$(openssl rand -hex 24)
  password_b=$(openssl rand -hex 24)
  register_test_account "$container" agent-a "$password_a"
  register_test_account "$container" agent-b "$password_b"
  password_c=
  if [ "$profile" = shared-group ] || [ "$profile" = shared-group-mailbox ] || [ "$profile" = shared-group-extdisco ]; then
    password_c=$(openssl rand -hex 24)
    register_test_account "$container" agent-c "$password_c"
  fi
  umask 077
  {
    printf 'CYNAPSA_EJABBERD_STATE=%s\n' "$state"
    printf 'CYNAPSA_EJABBERD_PROFILE=%s\n' "$profile"
    printf 'CYNAPSA_EJABBERD_ENDPOINT=127.0.0.1:%s\n' "$c2s_port"
    printf 'CYNAPSA_EJABBERD_HTTPS=https://127.0.0.1:%s\n' "$https_port"
    printf 'CYNAPSA_EJABBERD_CA=%s\n' "$state/ca.pem"
    printf 'CYNAPSA_EJABBERD_AGENT_A=agent-a@mesh.test\n'
    printf 'CYNAPSA_EJABBERD_AGENT_B=agent-b@mesh.test\n'
    printf 'CYNAPSA_EJABBERD_PASSWORD_A=%s\n' "$password_a"
    printf 'CYNAPSA_EJABBERD_PASSWORD_B=%s\n' "$password_b"
    if [ "$profile" = shared-group ] || [ "$profile" = shared-group-mailbox ] || [ "$profile" = shared-group-extdisco ]; then
      printf 'CYNAPSA_EJABBERD_AGENT_C=agent-c@mesh.test\n'
      printf 'CYNAPSA_EJABBERD_PASSWORD_C=%s\n' "$password_c"
    fi
  } >"$state/credentials.env"
  chmod 0600 "$state/credentials.env"
  require_file_mode "$state/credentials.env" 600 'credential environment'
  trap - INT TERM HUP EXIT
  printf '%s\n' "$state"
}

validate_admin_token() {
  value=$1
  label=$2
  if [ -z "$value" ] || [ "${#value}" -gt 256 ]; then
    die "invalid $label"
  fi
  case "$value" in
    *[!A-Za-z0-9_.-]*) die "invalid $label" ;;
  esac
}

admin_environment() {
  state=$1
  action=$2
  shift 2
  [ -d "$state" ] || die "state directory unavailable"
  profile=$(sed -n '1p' "$state/profile")
  [ "$profile" = shared-group ] || [ "$profile" = shared-group-mailbox ] || [ "$profile" = shared-group-extdisco ] || die "admin requires shared-group profile"
  container=$(container_id_for "$state")
  eval_expression=
  case "$action" in
    ready)
      [ "$#" -eq 0 ] || die 'usage: run.sh admin STATE ready'
      set -- cynapsa_mesh_ready mesh.test
      ;;
    create)
      [ "$#" -eq 1 ] || die 'usage: run.sh admin STATE create MESH'
      validate_admin_token "$1" mesh
      set -- cynapsa_mesh_create "$1" mesh.test
      ;;
    add|remove)
      [ "$#" -eq 2 ] || die "usage: run.sh admin STATE $action USER MESH"
      validate_admin_token "$1" user
      validate_admin_token "$2" mesh
      user=$1
      mesh=$2
      set -- "cynapsa_mesh_${action}" "$user" mesh.test "$mesh"
      ;;
    remove-many)
      [ "$#" -eq 2 ] || die 'usage: run.sh admin STATE remove-many USER1,USER2 MESH'
      users=$1
      mesh=$2
      case "$users" in ''|,*|*,|*,,*|*[!A-Za-z0-9_.@,-]*) die 'invalid users list' ;; esac
      validate_admin_token "$mesh" mesh
      set -- cynapsa_mesh_remove_many "$users" mesh.test "$mesh"
      ;;
    seed)
      [ "$#" -eq 3 ] || die 'usage: run.sh admin STATE seed PREFIX COUNT MESH'
      prefix=$1
      count=$2
      mesh=$3
      validate_admin_token "$prefix" prefix
      validate_admin_token "$mesh" mesh
      case "$count" in ''|*[!0-9]*) die 'invalid seed count' ;; esac
      [ "$count" -ge 1 ] && [ "$count" -le 512 ] || die 'invalid seed count'
      printf 'action=%s\n' "$action" >>"$state/admin.log"
      if output=$(docker exec "$container" /bin/sh -c '
          set -eu
          prefix=$1
          count=$2
          mesh=$3
          index=0
          while [ "$index" -lt "$count" ]; do
            user=$(printf "%s-%03d" "$prefix" "$index")
            ejabberdctl cynapsa_mesh_add "$user" mesh.test "$mesh" >/dev/null
            index=$((index + 1))
          done
        ' sh "$prefix" "$count" "$mesh" 2>&1); then
        printf 'result=ok output=%s\n' "$output" >>"$state/admin.log"
        printf '%s\n' "$output"
        return 0
      else
        status=$?
        printf 'result=error status=%s output=%s\n' "$status" "$output" >>"$state/admin.log"
        printf '%s\n' "$output" >&2
        return "$status"
      fi
      ;;
    mam-seed)
      [ "$#" -eq 3 ] || die 'usage: run.sh admin STATE mam-seed OWNER PEER MESH'
      owner=$1
      peer=$2
      mesh=$3
      validate_admin_token "$owner" owner
      validate_admin_token "$peer" peer
      validate_admin_token "$mesh" mesh
      eval_expression=$(printf 'H = <<"mesh.test">>, O = <<"%s">>, P = <<"%s">>, M = <<"%s">>, From = jid:make(O,H,M), To = jid:make(P,H,M), Raw = {xmlel,<<"message">>,[{<<"from">>,jid:encode(From)},{<<"to">>,jid:encode(To)}],[]}, Decoded = xmpp:decode(Raw,<<"jabber:client">>,[ignore_els]), TS = erlang:timestamp(), mnesia:dirty_write({archive_msg,{O,H},<<"9000000000000000001">>,TS,{P,H,M},{P,H,<<>>},Raw,<<>>,chat,<<>>}), mnesia:dirty_write({archive_msg,{O,H},<<"9000000000000000002">>,TS,{P,H,M},{P,H,<<>>},Decoded,<<>>,chat,<<>>}), io:format("ok~n"), ok.' "$owner" "$peer" "$mesh")
      ;;
    mam-prefs-legacy)
      [ "$#" -eq 2 ] || die 'usage: run.sh admin STATE mam-prefs-legacy USER MESH'
      user=$1
      mesh=$2
      validate_admin_token "$user" user
      validate_admin_token "$mesh" mesh
      eval_expression=$(printf 'mnesia:dirty_write({archive_prefs,{<<"%s">>,<<"mesh.test">>},always,[{<<"agent-b">>,<<"mesh.test">>,<<"%s">>}],[]}), io:format("ok~n"), ok.' "$user" "$mesh")
      ;;
    mam-prefs-state)
      [ "$#" -eq 1 ] || die 'usage: run.sh admin STATE mam-prefs-state USER'
      validate_admin_token "$1" user
      eval_expression=$(printf 'io:format("~p~n",[mnesia:dirty_read(archive_prefs,{<<"%s">>,<<"mesh.test">>})]), ok.' "$1")
      ;;
    upload-seed)
      [ "$#" -eq 3 ] || die 'usage: run.sh admin STATE upload-seed USER MESH TOKEN'
      user=$1
      mesh=$2
      token=$3
      validate_admin_token "$user" user
      validate_admin_token "$mesh" mesh
      validate_admin_token "$token" token
      [ "${#token}" -eq 26 ] || die 'upload token must be 26 bytes'
      filename="cynapsa-${token}.bin"
      path="/opt/ejabberd/upload/${token}/${token}/${filename}"
      docker exec "$container" /bin/sh -c 'set -eu; mkdir -p "$1"; printf seeded >"$2"' sh "/opt/ejabberd/upload/${token}/${token}" "$path"
      eval_expression=$(printf 'H = <<"mesh.test">>, U = <<"%s">>, M = <<"%s">>, T = <<"%s">>, F = <<"cynapsa-%s.bin">>, Path = <<"/opt/ejabberd/upload/%s/%s/cynapsa-%s.bin">>, mnesia:dirty_write({cynapsa_mesh_upload_object,{H,Path},{U,H,M,completed,6},Path,[T,T,F]}), io:format("ok~n"), ok.' "$user" "$mesh" "$token" "$token" "$token" "$token" "$token")
      ;;
    upload-expired-seed)
      [ "$#" -eq 3 ] || die 'usage: run.sh admin STATE upload-expired-seed USER MESH TOKEN'
      user=$1
      mesh=$2
      token=$3
      validate_admin_token "$user" user
      validate_admin_token "$mesh" mesh
      validate_admin_token "$token" token
      [ "${#token}" -eq 26 ] || die 'expired upload token must be 26 bytes'
      filename="cynapsa-${token}.bin"
      path="/opt/ejabberd/upload/${token}/${token}/${filename}"
      docker exec "$container" /bin/sh -c 'set -eu; mkdir -p "$1"; printf expired >"$2"' sh "/opt/ejabberd/upload/${token}/${token}" "$path"
      eval_expression=$(printf 'H = <<"mesh.test">>, U = <<"%s">>, M = <<"%s">>, T = <<"%s">>, F = <<"cynapsa-%s.bin">>, Path = <<"/opt/ejabberd/upload/%s/%s/cynapsa-%s.bin">>, mnesia:dirty_write({cynapsa_mesh_upload_object,{H,Path},{U,H,M,pending,1,<<0:128>>,0},Path,[T,T,F]}), io:format("ok~n"), ok.' "$user" "$mesh" "$token" "$token" "$token" "$token" "$token")
      ;;
    upload-count)
      [ "$#" -eq 2 ] || die 'usage: run.sh admin STATE upload-count USER MESH'
      validate_admin_token "$1" user
      validate_admin_token "$2" mesh
      eval_expression=$(printf 'H = <<"mesh.test">>, U = <<"%s">>, M = <<"%s">>, Rows = lists:append([mnesia:dirty_read(cynapsa_mesh_upload_object,K) || K <- mnesia:dirty_all_keys(cynapsa_mesh_upload_object)]), N = length([ok || {cynapsa_mesh_upload_object,_,Owner,_,_} <- Rows, (Owner =:= {U,H,M} orelse (tuple_size(Owner) >= 3 andalso element(1,Owner) =:= U andalso element(2,Owner) =:= H andalso element(3,Owner) =:= M))]), io:format("~B~n",[N]), ok.' "$1" "$2")
      ;;
    upload-file)
      [ "$#" -eq 2 ] || die 'usage: run.sh admin STATE upload-file USER TOKEN'
      validate_admin_token "$1" user
      validate_admin_token "$2" token
      filename="cynapsa-$2.bin"
      if docker exec "$container" test -f "/opt/ejabberd/upload/$2/$2/$filename"; then
        printf 'present\n'
      else
        printf 'absent\n'
      fi
      return 0
      ;;
    mailbox-count)
      [ "$#" -eq 2 ] || die 'usage: run.sh admin STATE mailbox-count USER MESH'
      validate_admin_token "$1" user
      validate_admin_token "$2" mesh
      eval_expression=$(printf 'O = {<<"%s">>,<<"mesh.test">>,<<"%s">>}, Rows = mnesia:dirty_index_read(cynapsa_mesh_mailbox,O,3), io:format("~B~n",[length(Rows)]), ok.' "$1" "$2")
      ;;
    mailbox-sender-count)
      [ "$#" -eq 2 ] || die 'usage: run.sh admin STATE mailbox-sender-count USER MESH'
      validate_admin_token "$1" user
      validate_admin_token "$2" mesh
      eval_expression=$(printf 'S = {<<"%s">>,<<"mesh.test">>,<<"%s">>}, Rows = mnesia:dirty_index_read(cynapsa_mesh_mailbox,S,4), io:format("~B~n",[length(Rows)]), ok.' "$1" "$2")
      ;;
    snapshot)
      [ "$#" -eq 1 ] || die "usage: run.sh admin STATE $action MESH"
      validate_admin_token "$1" mesh
      mesh=$1
      set -- cynapsa_mesh_snapshot "$mesh" mesh.test
      ;;
    offline-count|mam-count)
      [ "$#" -eq 1 ] || die "usage: run.sh admin STATE $action USER"
      validate_admin_token "$1" user
      case "$action" in
        offline-count) set -- get_offline_count "$1" mesh.test ;;
        mam-count) set -- get_mam_count "$1" mesh.test ;;
      esac
      ;;
    *) die "unknown admin action: $action" ;;
  esac
  printf 'action=%s\n' "$action" >>"$state/admin.log"
  if [ -n "$eval_expression" ]; then
    if output=$(printf '%s\n' "$eval_expression" | docker exec -i "$container" /opt/ejabberd-26.04/erts-16.3.1/bin/erl_call -e -n ejabberd@localhost -fetch_stdout -no_result_term 2>&1); then
      printf 'result=ok output=%s\n' "$output" >>"$state/admin.log"
      printf '%s\n' "$output"
      return 0
    else
      status=$?
      printf 'result=error status=%s output=%s\n' "$status" "$output" >>"$state/admin.log"
      printf '%s\n' "$output" >&2
      return "$status"
    fi
  elif output=$(docker exec "$container" ejabberdctl "$@" 2>&1); then
    printf 'result=ok output=%s\n' "$output" >>"$state/admin.log"
    printf '%s\n' "$output"
  else
    status=$?
    printf 'result=error status=%s output=%s\n' "$status" "$output" >>"$state/admin.log"
    printf '%s\n' "$output" >&2
    return "$status"
  fi
}

restart_environment() {
  state=$1
  [ -d "$state" ] || die "state directory unavailable"
  profile=$(sed -n '1p' "$state/profile")
  [ "$profile" = shared-group ] || [ "$profile" = shared-group-mailbox ] || [ "$profile" = shared-group-extdisco ] || die "restart requires shared-group profile"
  container=$(container_id_for "$state")
  # Restart the Erlang service, not the disposable container: recreating the
  # container tmpfs would deliberately erase Mnesia and would not exercise
  # the V1 server-process persistence contract.
  docker exec "$container" ejabberdctl restart >/dev/null
  wait_ready "$state"
  wait_mesh_ready "$state"
  printf 'action=restart result=ok\n' >>"$state/admin.log"
}

set_external_service_mode() {
  state=$1
  mode=$2
  [ -d "$state" ] || die 'state directory unavailable'
  profile=$(sed -n '1p' "$state/profile")
  [ "$profile" = shared-group-extdisco ] || die 'external service mode requires shared-group-extdisco profile'
  source="$state/ejabberd-extdisco-all.yml"
  output="$state/ejabberd.yml"
  case "$mode" in
    all) cp "$source" "$output.tmp" ;;
    stun|relay-udp|relay-tcp)
      awk -v mode="$mode" '
        /PROFILE_EXTDISCO_STUN_BEGIN/ { omit=(mode != "stun"); next }
        /PROFILE_EXTDISCO_STUN_END/ { omit=0; next }
        /PROFILE_EXTDISCO_TURN_UDP_BEGIN/ { omit=(mode != "relay-udp"); next }
        /PROFILE_EXTDISCO_TURN_UDP_END/ { omit=0; next }
        /PROFILE_EXTDISCO_TURN_TCP_BEGIN/ { omit=(mode != "relay-tcp"); next }
        /PROFILE_EXTDISCO_TURN_TCP_END/ { omit=0; next }
        !omit { print }
      ' "$source" >"$output.tmp"
      ;;
    *) die 'unknown external service mode'
      ;;
  esac
  mv "$output.tmp" "$output"
  chmod 0600 "$output"
  stage_server_configuration "$state" "$output"
  container=$(container_id_for "$state")
  docker exec "$container" ejabberdctl reload_config >/dev/null
  wait_ready "$state"
  wait_mesh_ready "$state"
  printf 'action=external-service-mode mode=%s result=ok\n' "$mode" >>"$state/admin.log"
}

case "${1:-}" in
  start)
    [ "$#" -eq 2 ] || die 'usage: run.sh start PROFILE'
    start_environment "$2"
    ;;
  stop)
    [ "$#" -eq 2 ] || die 'usage: run.sh stop STATE_DIRECTORY'
    stop_environment "$2"
    ;;
  probe)
    if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
      die 'usage: run.sh probe STATE_DIRECTORY [adapter|application|client|control|external|resume|slot|time]'
    fi
    probe_protocol "$2" "${3:-adapter}"
    ;;
  admin)
    [ "$#" -ge 3 ] || die 'usage: run.sh admin STATE ACTION [ARG...]'
    state=$2
    action=$3
    shift 3
    admin_environment "$state" "$action" "$@"
    ;;
  restart)
    [ "$#" -eq 2 ] || die 'usage: run.sh restart STATE_DIRECTORY'
    restart_environment "$2"
    ;;
  external-service-mode)
    [ "$#" -eq 3 ] || die 'usage: run.sh external-service-mode STATE_DIRECTORY {all|stun|relay-udp|relay-tcp}'
    set_external_service_mode "$2" "$3"
    ;;
  render-config)
    [ "$#" -eq 5 ] || die 'usage: run.sh render-config INPUT OUTPUT PROFILE HTTPS_PORT'
    render_config "$2" "$3" "$4" "$5"
    ;;
  run)
    if [ "$#" -lt 4 ] || [ "$3" != -- ]; then
      die 'usage: run.sh run PROFILE -- COMMAND [ARG...]'
    fi
    state=$(start_environment "$2")
    trap 'stop_environment "$state"' INT TERM HUP EXIT
    set -a
    # shellcheck disable=SC1091
    . "$state/credentials.env"
    set +a
    shift 3
    "$@"
    result=$?
    stop_environment "$state"
    trap - INT TERM HUP EXIT
    exit "$result"
    ;;
  *)
    die 'usage: run.sh {start PROFILE|stop STATE_DIRECTORY|probe STATE_DIRECTORY|admin STATE ACTION [ARG...]|restart STATE_DIRECTORY|external-service-mode STATE_DIRECTORY MODE|render-config INPUT OUTPUT PROFILE HTTPS_PORT|run PROFILE -- COMMAND [ARG...]}'
    ;;
esac
