#!/bin/sh
set -eu

marker=
IFS= read -r marker || true
if [ "$marker" = cynapsa-vlan-udp ]; then
  printf 'UDP:%s\n' "${SOCAT_PEERADDR:-}" >>/tmp/cynapsa-network-probe.raw
fi
