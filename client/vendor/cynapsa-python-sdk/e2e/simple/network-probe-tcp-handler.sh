#!/bin/sh
set -eu

marker=
IFS= read -r marker || true
if [ "$marker" = cynapsa-vlan-tcp ]; then
  printf 'TCP:%s\n' "${SOCAT_PEERADDR:-}" >>/tmp/cynapsa-network-probe.raw
  printf ok
fi
