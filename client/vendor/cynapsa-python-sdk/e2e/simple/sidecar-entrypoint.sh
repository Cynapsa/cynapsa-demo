#!/bin/sh
set -eu

: "${CYNAPSA_VLAN_ID:?CYNAPSA_VLAN_ID is required}"
: "${CYNAPSA_LAN_ADDRESS:?CYNAPSA_LAN_ADDRESS is required}"
: "${CYNAPSA_LAN_GATEWAY:?CYNAPSA_LAN_GATEWAY is required}"

trunk_address=$(ip -4 -o address show dev eth0 scope global | awk 'NR == 1 {print $4}')
test -n "$trunk_address"

ip link add link eth0 name cynapsa-lan type vlan id "$CYNAPSA_VLAN_ID"
ip addr add "$CYNAPSA_LAN_ADDRESS" dev cynapsa-lan
ip link set cynapsa-lan addrgenmode none
ip link set cynapsa-lan mtu 1496 up
ip -6 addr flush dev cynapsa-lan
ip route replace 10.240.0.0/16 via "$CYNAPSA_LAN_GATEWAY" dev cynapsa-lan
ip addr del "$trunk_address" dev eth0
ip route flush dev eth0
ip link set eth0 addrgenmode none
ip -6 addr flush dev eth0

iptables -t nat -F
if iptables -t nat -S | grep -Eq '(MASQUERADE|SNAT|DNAT)'; then
  echo 'sidecar unexpectedly contains a NAT rule' >&2
  exit 1
fi

# The Docker bridge is an internal 802.1Q trunk only. Application and ejabberd
# processes share this namespace but never receive a usable untagged eth0 path.
iptables -A INPUT -i eth0 -j DROP
iptables -A OUTPUT -o eth0 -j REJECT

echo "cynapsa sidecar ready: trunk=eth0 vlan=$CYNAPSA_VLAN_ID lan=$CYNAPSA_LAN_ADDRESS"
exec tail -f /dev/null
