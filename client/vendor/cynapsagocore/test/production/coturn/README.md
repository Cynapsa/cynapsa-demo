# Disposable P2P Service Environment

This test-only environment starts three independent pinned service containers:

- the existing ejabberd shared-group authority;
- a STUN-only Coturn process;
- a separately configured authenticated TURN relay.

Every invocation creates synthetic identities and one TURN REST shared secret.
Ejabberd owns endpoint discovery and mints short-lived XEP-0215 credentials;
agents never mount a connectivity profile. Each
unprivileged agent runs on its own private Docker bridge and has its sole
default route replaced with a separate two-interface NAT gateway. Ejabberd,
STUN, and TURN exist only on the service-side network. The agents therefore
have distinct private address domains, no shared host-candidate path, and no
route that bypasses their gateway. The harness applies CPU/memory/PID limits,
drops all agent capabilities, grants only `NET_ADMIN` to the disposable NAT
gateway and route controller, mounts only exact generated files, and tears
down every container, network, volume, credential, and secret-bearing file.

Start and provision an environment:

```sh
state=$(test/production/coturn/run.sh start)
test/production/coturn/run.sh admin "$state" create e2e-mesh
test/production/coturn/run.sh admin "$state" add agent-a e2e-mesh
test/production/coturn/run.sh admin "$state" add agent-b e2e-mesh
trap 'test/production/coturn/run.sh stop "$state"' EXIT
```

The child ejabberd profile enables `mod_stun_disco` with an explicit STUN,
TURN/UDP, and TURN/TCP list and a two-minute credential lifetime. The generated
shared secret is staged only in the disposable server volume and configured in
the separately pinned Coturn process. Agent containers receive only XMPP
authentication inputs; the Core discovers and copies ICE configuration over
its exact authenticated Rank 2 session.

The harness may select `all`, `stun`, `relay-udp`, or `relay-tcp` by changing
only ejabberd's XEP-0215 advertisement and restarting that disposable service.
The NAT matrix uses the authoritative TCP-only result for the blocked-UDP case
and UDP-relay result for relay failure/recovery cases; it never mounts or
injects a connectivity profile into an agent.

The Coturn image is the official multi-platform `4.15.0-r0` release pinned by
its immutable manifest-list digest. The gateway image is also pinned by its
immutable multi-platform digest. Protocol readiness requires an RFC 5389
binding response, authenticated XMPP and authoritative group discovery, a
strict XEP-0215 result, and a successful TURN allocation using the returned
short-lived credential.

The matrix uses two explicit NAT profiles:

- `full-cone`: endpoint-independent, port-preserving UDP mapping with inbound
  UDP mapped to the same private port. ICE must become live with the complete
  server-authoritative candidate configuration.
- `symmetric`: the STUN server and other UDP destinations use disjoint
  external port ranges, with endpoint-dependent mappings inside the non-STUN
  range. Direct ICE cannot connect and the server-authorized TURN path must
  become live. The bounded range permits the old and
  refreshed TURN allocations to coexist during path recovery.

The same symmetric topology proves authenticated TCP relay while UDP is blocked
at both gateways, exact unsupported-relay allocation rejection, exact
peer-permission rejection, and a stopped relay followed by live recovery after
restart. Negative traversal rows exchange and accept a reverse durable
bootstrap before observing the bounded peer result, forcing both independent
NAT sides to initiate private-link establishment and contribute gateway/TURN
fault evidence. A negative result is accepted only after an observed recovery
cycle completes or the complete bounded attempt window expires. The matrix
also blocks an established UDP relay channel at both NAT gateways
for longer than the frozen 30-second no-progress demotion boundary while
leaving XMPP/TCP available. A request must fall back durably during the fault;
after UDP is restored, one coalesced Jingle
`transport-replace`/`transport-accept` exchange must refresh the existing
authenticated peer link, and a final request/reply must succeed. The
production recovery controller falls back to complete make-before-break link
replacement if that bounded refresh cannot complete. Gateway fault counters,
routes, interfaces, TURN allocations and rejection codes, service logs, and
sanitized agent reports are retained as test evidence. Every gateway also
installs overlapping, counters-only pre-NAT observations for STUN/TURN UDP,
all UDP, XMPP TCP, all TCP, and total traffic in both directions. These rules
have no packet verdict, mark, or rewrite. They do not alter the topology; they
distinguish a missing protocol flow from a POSTROUTING counter/backend
mismatch. Snapshots retain the concise evidence chain, complete iptables
counters, interface counters, routes, and the namespace conntrack table when
the host exposes it. Verbose test output records a compact pre-NAT and
translation-counter checkpoint for every completed gateway. If a scenario
fails before its normal terminal snapshot, the harness captures and reports
the same checkpoint during failure unwinding. Event waits also identify the
exact missing event and its elapsed bound. A failure-only observation window
then lets the public agent finish its longer Core establishment budget before
the harness removes it; the resulting agent state, peer-state transitions,
bounded support-safe diagnostics, Coturn protocol counters/logs, ejabberd
logs, and service restart state are reported with generated secrets redacted.
This window cannot turn a failed assertion into a pass. The package-level
real-Pion test proves that the same data-channel object carries frames before
and after the structured restart; the container scenario proves recovery
through independent NAT gateways and the authenticated relay. Its public agent
allows 70 seconds for the initial bounded establishment attempt plus one
automatic retry and stable-health observation. Recovery-specific phases keep
their own tighter fault and refresh assertions.

A locally opened SCTP/DataChannel is not advertised as available by itself.
The Go core waits for authenticated peer-side progress, normally the first
health-probe reply. A failed or closed DataChannel read side cannot be revived
by a late Pion callback. Restartable ICE failure remains eligible for the
bounded in-place Jingle refresh; terminal channel failure falls through to the
make-before-break replacement path.

## Focused remote smoke

The protected qualification workflow first runs:

```sh
go test -v -count=1 ./test/production/coturn -run '^TestRemoteMeshSmoke$'
```

This is a narrow reuse of the same environment, not another Compose topology.
It selects server-authoritative TURN/UDP and symmetric NAT for two public Core
processes. After both peers report stable public diagnostic availability, both
gateways drop only XMPP TCP egress to the exact ejabberd address and port 5222.
Fresh bidirectional nonce-bound traffic then crosses explicit host barriers.
The request/reply, reverse delivery with mandatory acceptance, and post-health
phases each receive a fresh ten-second bound only after the preceding barrier.
Both exact DROP rules remain installed throughout, and their packet counters
and Coturn allocation/channel-binding counters must increase. The harness
restores XMPP before graceful shutdown and retains the full existing cleanup
and secret scan. The subsequent unfiltered package test remains the
comprehensive matrix.

The subsequent comprehensive package intentionally repeats that smoke after
the longer traversal/relay matrix. Its explicit package timeout bounds the
combined real-environment runtime without relying on Go's ten-minute default:

```sh
go test -v -count=1 -timeout=30m ./test/production/coturn
```

The harness generates the shared TURN REST secret directly into a mode-0600
file inside its mode-0700 state directory. It passes only that file's path to
the ejabberd renderer, uses the same private file to create Coturn's
configuration, and removes the file during unconditional teardown. The secret
is never included in a host Docker, renderer, Python, sed, or awk argument.
