#!/bin/sh
set -eu

route=$(ip -oneline route get 10.240.50.2)
case "$route" in
  *"via 10.240.10.1"*"dev cynapsa-lan"*"src 10.240.10.2"*) ;;
  *)
    echo "cross-VLAN route bypasses the router: $route" >&2
    exit 1
    ;;
esac

if ip -4 -o address show dev eth0 scope global | grep -q .; then
  echo "untagged eth0 retained an IPv4 address" >&2
  exit 1
fi
if ip -4 route show | grep -Eq '(^| )eth0( |$)|172\.31\.0\.0/24'; then
  echo "Docker trunk remained IP-routable" >&2
  ip -4 route show >&2
  exit 1
fi

openssl s_client \
  -starttls xmpp \
  -connect 10.240.70.2:5222 \
  -xmpphost mesh.test \
  -servername mesh.test \
  -CAfile /ca.pem \
  -verify_hostname mesh.test \
  -verify_return_error </dev/null >/tmp/cynapsa-xmpp-starttls.log 2>&1 || {
    cat /tmp/cynapsa-xmpp-starttls.log >&2
    exit 1
  }

reply=$(printf 'cynapsa-vlan-tcp\n' | socat -T 10 - TCP:10.240.50.2:39000,bind=10.240.10.2)
if [ "$reply" != ok ]; then
  echo "tagged TCP probe acknowledgement mismatch: $reply" >&2
  exit 1
fi
printf 'cynapsa-vlan-udp\n' | socat -T 10 - UDP:10.240.50.2:39001,bind=10.240.10.2

printf 'E2E_DIAGNOSTIC={"event":"probe-xmpp-starttls","source_lan":"10.240.10.2"}\n'
printf 'E2E_DIAGNOSTIC={"event":"probe-tagged-routing","route":%s,"tcp":true,"udp":true,"untagged_ip":false,"untagged_route":false}\n' \
  "$(printf '%s' "$route" | sed 's/\\/\\\\/g; s/"/\\"/g; s/^/"/; s/$/"/')"
