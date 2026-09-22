#!/bin/sh
set -eu

rm -f /tmp/cynapsa-network-probe-ready /tmp/cynapsa-network-probe /tmp/cynapsa-network-probe.raw

socat -T 20 TCP-LISTEN:39000,bind=10.240.50.2,reuseaddr,fork SYSTEM:/usr/local/bin/cynapsa-network-probe-tcp-handler &
tcp_pid=$!
socat -T 20 UDP-RECVFROM:39001,bind=10.240.50.2,reuseaddr,fork SYSTEM:/usr/local/bin/cynapsa-network-probe-udp-handler &
udp_pid=$!
trap 'kill "$tcp_pid" "$udp_pid" 2>/dev/null || true' EXIT INT TERM

touch /tmp/cynapsa-network-probe-ready
deadline=$(($(date +%s) + 20))
while [ "$(date +%s)" -lt "$deadline" ]; do
  tcp_source=$(awk -F: '$1 == "TCP" {print $2; exit}' /tmp/cynapsa-network-probe.raw 2>/dev/null || true)
  udp_source=$(awk -F: '$1 == "UDP" {print $2; exit}' /tmp/cynapsa-network-probe.raw 2>/dev/null || true)
  if [ -n "$tcp_source" ] && [ -n "$udp_source" ]; then
    printf '%s\t%s\n' "$tcp_source" "$udp_source" >/tmp/cynapsa-network-probe
    exit 0
  fi
  sleep 1
done

echo "timed out waiting for TCP and UDP probe packets" >&2
exit 1
