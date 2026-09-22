#!/bin/sh
set -eu

IMAGE='ghcr.io/processone/ejabberd@sha256:68482e33ff11934e73ef2881cf455ccefc1e03b31203f3ab651f4b2b1152b4a8'
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SOURCE_DIR="$SCRIPT_DIR/mod_cynapsa_mesh/src"
OUTPUT_DIR="$SCRIPT_DIR/mod_cynapsa_mesh/ebin"

mkdir -p "$OUTPUT_DIR"

docker run --rm \
  --network none \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=32m \
  --cpus 2 \
  --memory 512m \
  --pids-limit 128 \
  --cap-drop ALL \
  --cap-add NET_BIND_SERVICE \
  --mount "type=bind,src=$SOURCE_DIR,dst=/src,readonly" \
  --mount "type=bind,src=$OUTPUT_DIR,dst=/out" \
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
  '

test -s "$OUTPUT_DIR/mod_cynapsa_mesh.beam"
echo "$OUTPUT_DIR/mod_cynapsa_mesh.beam"
