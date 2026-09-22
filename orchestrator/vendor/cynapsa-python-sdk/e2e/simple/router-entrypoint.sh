#!/bin/sh
set -eu

if [ "$(cat /proc/sys/net/ipv4/ip_forward)" != 1 ]; then
  echo 'router IP forwarding is disabled' >&2
  exit 1
fi

trunk_address=$(ip -4 -o address show dev eth0 scope global | awk 'NR == 1 {print $4}')
test -n "$trunk_address"

create_vlan() {
  vlan=$1
  address=$2
  name="eth0.$vlan"
  ip link add link eth0 name "$name" type vlan id "$vlan"
  ip addr add "$address" dev "$name"
  ip link set "$name" addrgenmode none
  ip link set "$name" mtu 1496 up
  ip -6 addr flush dev "$name"
}

create_vlan 101 10.240.10.1/24
create_vlan 102 10.240.20.1/24
create_vlan 103 10.240.30.1/24
create_vlan 104 10.240.40.1/24
create_vlan 105 10.240.50.1/24
create_vlan 106 10.240.60.1/24
create_vlan 107 10.240.70.1/24
create_vlan 108 10.240.80.1/24
create_vlan 109 10.240.90.1/24
create_vlan 110 10.240.100.1/24
create_vlan 111 10.240.110.1/24
create_vlan 112 10.240.120.1/24
create_vlan 113 10.240.130.1/24
create_vlan 114 10.240.140.1/24
ip addr del "$trunk_address" dev eth0
ip route flush dev eth0
ip link set eth0 addrgenmode none
ip -6 addr flush dev eth0

iptables -t nat -F
if iptables -t nat -S | grep -Eq '(MASQUERADE|SNAT|DNAT)'; then
  echo 'router unexpectedly contains a NAT rule' >&2
  exit 1
fi

# The Docker bridge carries Ethernet frames only; untagged IP is forbidden.
iptables -A INPUT -i eth0 -j DROP
iptables -A OUTPUT -o eth0 -j REJECT

echo 'cynapsa E2E router ready: 802.1Q forwarding enabled, redirects disabled, NAT empty'
exec tail -f /dev/null
