#!/bin/sh
set -eu

die() {
  printf '%s\n' "nat-gateway: $*" >&2
  exit 1
}

interface_for_address() {
  address=$1
  ip -o -4 address show | awk -v address="$address" '$4 ~ ("^" address "/") { print $2; exit }'
}

wan_interface() {
  private_interface=$1
  ip -o -4 address show scope global | awk -v private_interface="$private_interface" '$2 != private_interface { print $2; exit }'
}

snapshot() {
  mkdir -p /evidence
  interfaces="/evidence/interfaces.txt.$$"
  interface_counters="/evidence/interface-counters.txt.$$"
  routes="/evidence/routes.txt.$$"
  rules="/evidence/iptables.txt.$$"
  packet_counters="/evidence/packet-counters.txt.$$"
  conntrack="/evidence/conntrack.txt.$$"
  ip -o -4 address show > "$interfaces"
  ip -s link show > "$interface_counters"
  ip route show table all > "$routes"
  iptables-save -c > "$rules"
  iptables -v -n -x -L CYNAPSA_EVIDENCE > "$packet_counters"
  if [ -r /proc/net/nf_conntrack ]; then
    cat /proc/net/nf_conntrack > "$conntrack"
  elif [ -r /proc/net/ip_conntrack ]; then
    cat /proc/net/ip_conntrack > "$conntrack"
  else
    printf '%s\n' 'conntrack table unavailable in gateway namespace' > "$conntrack"
  fi
  chmod 600 "$interfaces" "$interface_counters" "$routes" "$rules" "$packet_counters" "$conntrack"
  mv -f "$interfaces" /evidence/interfaces.txt
  mv -f "$interface_counters" /evidence/interface-counters.txt
  mv -f "$routes" /evidence/routes.txt
  mv -f "$rules" /evidence/iptables.txt
  mv -f "$packet_counters" /evidence/packet-counters.txt
  mv -f "$conntrack" /evidence/conntrack.txt
}

fault_udp() {
  iptables -F CYNAPSA_FAULT
  iptables -A CYNAPSA_FAULT -p udp -j DROP
  printf 'udp=blocked\n' > /evidence/fault.txt
  chmod 600 /evidence/fault.txt
  snapshot
}

fault_xmpp() {
  : "${CYNAPSA_NAT_XMPP_ADDRESS:?}"
  iptables -F CYNAPSA_XMPP
  iptables -A CYNAPSA_XMPP -p tcp -d "$CYNAPSA_NAT_XMPP_ADDRESS" --dport 5222 -j DROP
  printf 'xmpp=blocked address=%s port=5222\n' "$CYNAPSA_NAT_XMPP_ADDRESS" > /evidence/xmpp-fault.txt
  chmod 600 /evidence/xmpp-fault.txt
  snapshot
}

restore_xmpp() {
  iptables -F CYNAPSA_XMPP
  printf 'xmpp=available address=%s port=5222\n' "$CYNAPSA_NAT_XMPP_ADDRESS" > /evidence/xmpp-fault.txt
  chmod 600 /evidence/xmpp-fault.txt
  snapshot
}

restore_udp() {
  iptables -F CYNAPSA_FAULT
  printf 'udp=available\n' > /evidence/fault.txt
  chmod 600 /evidence/fault.txt
  snapshot
}

configure() {
  : "${CYNAPSA_NAT_PROFILE:?}"
  : "${CYNAPSA_NAT_PRIVATE_ADDRESS:?}"
  : "${CYNAPSA_NAT_PRIVATE_CIDR:?}"
  : "${CYNAPSA_NAT_AGENT_ADDRESS:?}"
  : "${CYNAPSA_NAT_STUN_ADDRESS:?}"
  : "${CYNAPSA_NAT_XMPP_ADDRESS:?}"

  deadline=$(( $(date +%s) + 20 ))
  private_interface=""
  wan_interface_name=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    private_interface=$(interface_for_address "$CYNAPSA_NAT_PRIVATE_ADDRESS")
    if [ -n "$private_interface" ]; then
      wan_interface_name=$(wan_interface "$private_interface")
    fi
    [ -n "$private_interface" ] && [ -n "$wan_interface_name" ] && break
    sleep 1
  done
  [ -n "$private_interface" ] || die "private interface was not attached"
  [ -n "$wan_interface_name" ] || die "WAN interface was not attached"
  wan_address=$(ip -o -4 address show dev "$wan_interface_name" | awk '{ sub(/\/.*/, "", $4); print $4; exit }')
  [ -n "$wan_address" ] || die "WAN address is unavailable"

  iptables -P FORWARD DROP
  iptables -N CYNAPSA_FAULT
  iptables -I FORWARD 1 -j CYNAPSA_FAULT
  iptables -N CYNAPSA_XMPP
  iptables -I FORWARD 2 -j CYNAPSA_XMPP
  # These overlapping, counters-only rules observe packets before source NAT.
  # They deliberately have no verdict target, mark, or rewrite and therefore
  # cannot alter the forwarding decision or NAT profile under test.
  iptables -N CYNAPSA_EVIDENCE
  iptables -I FORWARD 3 -j CYNAPSA_EVIDENCE
  iptables -A CYNAPSA_EVIDENCE -i "$private_interface" -o "$wan_interface_name" -s "$CYNAPSA_NAT_PRIVATE_CIDR" -p udp -d "$CYNAPSA_NAT_STUN_ADDRESS" --dport 3478 -m comment --comment cynapsa-egress-stun-turn
  iptables -A CYNAPSA_EVIDENCE -i "$private_interface" -o "$wan_interface_name" -s "$CYNAPSA_NAT_PRIVATE_CIDR" -p udp -m comment --comment cynapsa-egress-udp
  iptables -A CYNAPSA_EVIDENCE -i "$private_interface" -o "$wan_interface_name" -s "$CYNAPSA_NAT_PRIVATE_CIDR" -p tcp -d "$CYNAPSA_NAT_XMPP_ADDRESS" --dport 5222 -m comment --comment cynapsa-egress-xmpp
  iptables -A CYNAPSA_EVIDENCE -i "$private_interface" -o "$wan_interface_name" -s "$CYNAPSA_NAT_PRIVATE_CIDR" -p tcp -m comment --comment cynapsa-egress-tcp
  iptables -A CYNAPSA_EVIDENCE -i "$private_interface" -o "$wan_interface_name" -s "$CYNAPSA_NAT_PRIVATE_CIDR" -m comment --comment cynapsa-egress-any
  iptables -A CYNAPSA_EVIDENCE -i "$wan_interface_name" -o "$private_interface" -d "$CYNAPSA_NAT_PRIVATE_CIDR" -p udp -s "$CYNAPSA_NAT_STUN_ADDRESS" --sport 3478 -m comment --comment cynapsa-ingress-stun-turn
  iptables -A CYNAPSA_EVIDENCE -i "$wan_interface_name" -o "$private_interface" -d "$CYNAPSA_NAT_PRIVATE_CIDR" -p udp -m comment --comment cynapsa-ingress-udp
  iptables -A CYNAPSA_EVIDENCE -i "$wan_interface_name" -o "$private_interface" -d "$CYNAPSA_NAT_PRIVATE_CIDR" -p tcp -s "$CYNAPSA_NAT_XMPP_ADDRESS" --sport 5222 -m comment --comment cynapsa-ingress-xmpp
  iptables -A CYNAPSA_EVIDENCE -i "$wan_interface_name" -o "$private_interface" -d "$CYNAPSA_NAT_PRIVATE_CIDR" -p tcp -m comment --comment cynapsa-ingress-tcp
  iptables -A CYNAPSA_EVIDENCE -i "$wan_interface_name" -o "$private_interface" -d "$CYNAPSA_NAT_PRIVATE_CIDR" -m comment --comment cynapsa-ingress-any
  iptables -A FORWARD -i "$private_interface" -o "$wan_interface_name" -s "$CYNAPSA_NAT_PRIVATE_CIDR" -j ACCEPT
  iptables -A FORWARD -i "$wan_interface_name" -o "$private_interface" -d "$CYNAPSA_NAT_PRIVATE_CIDR" -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT

  case "$CYNAPSA_NAT_PROFILE" in
    full-cone)
      # Preserve private UDP source ports across remote endpoints and map
      # peer-originated UDP back to the same private port. This is an explicit
      # endpoint-independent full-cone profile; it does not rely on conntrack's
      # endpoint-specific reverse tuple.
      iptables -t nat -A POSTROUTING -s "$CYNAPSA_NAT_PRIVATE_CIDR" -o "$wan_interface_name" -j SNAT --to-source "$wan_address"
      iptables -t nat -A PREROUTING -i "$wan_interface_name" -d "$wan_address" -p udp -j DNAT --to-destination "$CYNAPSA_NAT_AGENT_ADDRESS"
      iptables -I FORWARD 2 -i "$wan_interface_name" -o "$private_interface" -d "$CYNAPSA_NAT_AGENT_ADDRESS" -p udp -j ACCEPT
      ;;
    symmetric)
      # Give the STUN server and every other UDP destination different stable
      # external ports. The advertised STUN mapping therefore cannot be used
      # for peer-directed traffic, deterministically modeling endpoint-
      # dependent mapping without relying on kernel randomness.
      iptables -t nat -A POSTROUTING -s "$CYNAPSA_NAT_PRIVATE_CIDR" -p udp -d "$CYNAPSA_NAT_STUN_ADDRESS" --dport 3478 -o "$wan_interface_name" -j SNAT --to-source "${wan_address}:40000"
      # Use a disjoint bounded port pool for non-STUN flows. Each local socket
      # receives an endpoint-dependent mapping, while a replacement connection
      # can coexist with the old TURN allocation during make-before-break.
      iptables -t nat -A POSTROUTING -s "$CYNAPSA_NAT_PRIVATE_CIDR" -p udp -o "$wan_interface_name" -j SNAT --to-source "${wan_address}:40001-40999" --random-fully
      iptables -t nat -A POSTROUTING -s "$CYNAPSA_NAT_PRIVATE_CIDR" -o "$wan_interface_name" -j SNAT --to-source "$wan_address"
      ;;
    *) die "unsupported NAT profile: $CYNAPSA_NAT_PROFILE" ;;
  esac

  mkdir -p /evidence
  {
    printf 'profile=%s\n' "$CYNAPSA_NAT_PROFILE"
    printf 'private_cidr=%s\n' "$CYNAPSA_NAT_PRIVATE_CIDR"
    printf 'private_interface=%s\n' "$private_interface"
    printf 'wan_interface=%s\n' "$wan_interface_name"
    printf 'wan_address=%s\n' "$wan_address"
    printf 'stun_address=%s\n' "$CYNAPSA_NAT_STUN_ADDRESS"
  } > /evidence/profile.txt
  chmod 600 /evidence/profile.txt
  snapshot
  : > /evidence/ready
  chmod 600 /evidence/ready

  while :; do
    snapshot
    sleep 1
  done
}

case "${1:-configure}" in
  configure) configure ;;
  snapshot) snapshot ;;
  fault-udp) fault_udp ;;
  restore-udp) restore_udp ;;
  fault-xmpp) fault_xmpp ;;
  restore-xmpp) restore_xmpp ;;
  *) die "usage: nat_gateway.sh {configure|snapshot|fault-udp|restore-udp|fault-xmpp|restore-xmpp}" ;;
esac
