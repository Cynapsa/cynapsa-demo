# Simple real-mesh E2E

This harness runs six Python SDK application containers, one real ejabberd
container, and separate Coturn STUN-only and authenticated TURN containers
behind a single router-on-a-stick container. One private internal
Docker bridge carries Ethernet frames only. It is an 802.1Q trunk, not an
application IP network. Each application container and ejabberd container shares
the network namespace of a dedicated LAN sidecar through
`network_mode: service:<sidecar>`.

Each sidecar owns one unique VLAN and `/24`; the router terminates all fourteen
VLANs on subinterfaces, forwards between them with ICMP redirects disabled, and
keeps its NAT table empty. Sidecar entrypoints create `cynapsa-lan`, assign only
the `10.240.x.2` application address there, route other application LANs through
that VLAN's `10.240.x.1` router address, and remove Docker's untagged IPv4
address and every untagged route from `eth0`. IPv6 link-local generation is also
suppressed. Pion can therefore discover only loopback and the entity's tagged
application address; there is no untagged candidate, direct IP bypass, NAT, or
source-address rewrite. The harness does not use host networking.

Only the router and LAN sidecars have `NET_ADMIN`. Python application containers
run as an unprivileged user, have no network entrypoint, do not install network
tools, and contain no VLAN, route, or probe code. Topology verification is done
by shell helpers inside sidecar namespaces.

The harness runs actual PostgreSQL, local Auth0-emulator, Management,
Enrollment, and custom ejabberd repository builds. Those five service entities
also receive distinct routed VLANs; agents never receive a direct Docker
service-network bypass. The result is explicitly a `local-offline-contract`
test with `identity-provider=local-auth0-emulator`, not live-Auth0 acceptance or
production qualification.

The runner provisions one tenant, environment, and authoritative mesh through
Management, then provisions six distinct logical agents, adds all six direct
memberships, issues one mesh-bound single-installation `cpsa_e1` grant per
agent, and revokes each automatically-created unbound grant. Core authenticates
through Enrollment, persists six independent installation profiles, and binds
the server-issued `r2.<installation UUID>.<nonce>` resource. Each fresh Core session must also
discover exactly `urn:cynapsa:mesh-authority:1`, install and acknowledge its
complete authority snapshot, and process membership-control IQs on the
independent control lane. The same ejabberd extension preserves the verified
runtime context across a successful XEP-0198 resume without negotiating a fresh
membership snapshot.
Replayed application messages may carry one XEP-0203 delayed-delivery element
before or after their single Cynapsa frame, matching ejabberd's offline replay
shape. Core strictly validates and discards that transport metadata before the
unchanged frame reaches the SDK.

The matrix is:

| Client | Local call | Result from native server | Result from monkey server |
| --- | --- | --- | --- |
| Native sync | `AztmSession.request` | `CynapsaResponse` | `CynapsaResponse` |
| Native async | `AsyncAztmSession.request` | `CynapsaResponse` | `CynapsaResponse` |
| Monkey sync | ordinary Requests source, wrapped by `cynapsa run` | `requests.Response` | `requests.Response` |
| Monkey async | patched `httpx.AsyncClient` | `httpx.Response` | `httpx.Response` |

The native `session.on('/reverse')` server receives the universal request and
returns `[reversed, "native"]`. The monkey server is an ordinary FastAPI app
with no Cynapsa import; Compose wraps `uvicorn module:app` with `cynapsa run`,
and it returns `[reversed, "monkey"]`. The monkey sync client is likewise a
standalone Requests program with no Cynapsa import or Cynapsa-aware helper;
Compose supplies its two RPC origin mappings. Each client makes ten RPCs to
each server and validates 20 results; the run succeeds only at 80/80. Each
client retains one identity, container, and Core session while talking to both
servers.

The generic async client helper schedules requests at a fixed two-second cadence:
it sleeps before every request after the first. This ordinary workload pacing
applies equally to native-async and monkey-async. It does not inspect topology,
transport state, injected failures, or retries, and it does not change the ten
requests per target or any expected result count.

The Go Core starts with an empty fail-closed application policy. Native agents
and the remaining monkey async agent retain their test-only internal policy
setup until a high-level library policy API exists. The zero-source-change
FastAPI server and monkey sync client instead receive an explicit
`--allow '*' '*'` from Compose; `cynapsa run` installs that rule before their
ordinary HTTP code starts. The CLI does not install a default allow rule.

The application programs live under `e2e/simple/cynapsa_e2e/`: native sync
client, native async client, monkey sync client, monkey async client, native
server, monkey FastAPI server, and a focused sequence client used by the short
transition check.

## Deep network scenario

XEP-0215 from ejabberd is the sole ICE-service authority. It advertises the
standalone STUN discovery service on `10.240.80.2:3478/udp` and the authenticated
TURN service on `10.240.90.2:3478/udp`; no application container receives an ICE
server override. TURN credentials and all authentication credentials are
generated per run. Enrollment tokens, JWTs, provider credentials, and service
secrets stay below the mode-0700 runtime tree, are removed unconditionally, and
are scanned out of retained artifacts. Evidence contains only labels and
identity hashes.

The suite never sets a `CYNAPSA_ICE_TRANSPORT_POLICY=relay` runtime override.
Core keeps the ordinary `all` ICE policy; TURN-only behavior is a property of
the test network topology and firewall rules.

The native async client is placed behind UDP source NAT. Its environment permits
STUN discovery and authenticated TURN while denying both server LANs and every
other direct, peer, or arbitrary UDP path. Every application, including this
client, runs Core's ordinary `all` ICE policy; the transparent router rules make
TURN the sole viable connectivity and application-data path for this client.
TCP/5222 remains available throughout relay establishment and
the exact relay proof. Specific
server-LAN and catch-all other-UDP counters retain denial evidence. The native
server's outbound-XMPP FIFO begins with a benign
10-millisecond warm-up delay; the fixed async workload cadence keeps the
unchanged ten-request workload active long enough for complete-gathering TURN
Rank1 negotiation. The harness watches a
bounded sequence of ordinary RPCs instead of assuming ICE is ready at a fixed
message number. Relay readiness requires one exact RPC with a
single native-server dispatch, no matching exact-resource Rank2 mailbox
admission in either direction, and positive bidirectional TURN ChannelData
packet and byte deltas. The ChannelData counters match TURN frames whose top
bits are `01`, excluding allocation, refresh, permission, and channel-bind
control chatter. Together with the router's direct-path denials, this exact
warm-up RPC is the relay application-carriage proof; there
is no separate XMPP-blackout proof because dropping the client XMPP path would
intentionally fence Rank1 authority rather than model the requested NAT
restriction. Per-run counters and a post-baseline Coturn log delta additionally
prove NAT, allocation, and channel binding. The native server's complete XMPP
flow starts with the 10-millisecond warm-up netem child. After the exact relay
proof, the harness briefly pauses the client while its LAN sidecar stays live,
changes that child in place to one second, and reserves the next ordinary RPC
for the native-server outage campaign; the root qdisc is never replaced and
the application receives no test signal.
The dispatch-time evidence must still report exactly one second. All ten native
requests, all ten monkey requests, and the final 80/80 matrix remain unchanged.
The monkey async client is allowed only TCP to
`10.240.70.2:5222`; positive XMPP counters, denied non-XMPP probes, and exact
20/20 delivery prove rank-2/XMPP operation.

During active RPCs, the harness blackholes the native server and monkey async
client until ejabberd reports that the exact resource's fifteen-second XEP-0198
resume window has expired. Waiting for the server lifecycle event accounts for
variable TCP failure-detection time while remaining below the fault campaign's
60-second RPC TTL. The margin covers Core's 30-second Rank1 no-progress window
followed by stale-stream expiry, fresh runtime-JWT authentication, resource binding,
authority restoration, and durable replay; these phases are intentionally
sequential in the injected outage. Only the native-async and monkey-async deployments use this
margin: their command and shutdown timeout, all normal E2E authentication, and
the focused sequence checks remain at 30, 30, and 10 seconds respectively. The
10-millisecond outbound-XMPP warm-up FIFO and the later one-second
outbound-XMPP fault delay combine with application-log and exact dispatch
observation to establish that
each fault begins while one RPC is in flight. The response qdisc is validated
when installed and again immediately after dispatch. Router DROP rules catch a
response already waiting in that qdisc without replacing it on a live TCP
stream. Once ejabberd authoritatively expires the old XEP-0198 stream and while
the namespace is still blackholed, the harness safely reduces the response
delay to 500 ms so fresh authentication and replay retain margin inside the RPC
deadline. The endpoint's local TCP state before and during the blackhole is
retained as diagnostic evidence, but neither count is treated as a
stream-lifecycle invariant: a local kernel can report zero sockets while
ejabberd still owns the exact-resource session, or continue to report an
`ESTABLISHED` control block after the server has expired the stream. A failed or
malformed local inspection is reported explicitly, but a valid zero count does
not veto the campaign. The expiry baseline is sampled immediately before fault installation,
after active dispatch observation and the potentially long TURN warm-up; a
strictly newer expiry is then required. This avoids both earlier unrelated
expiries and a race with the fault helper's bounded rejection pings.
Full-session, runtime-JWT-authentication, and resumption baselines are sampled
immediately before network restoration. The applications and Core receive no
fault flag and perform no test retry. Each outage must increment both DROP counters,
produce a post-fault XEP-0198 expiry, then produce fresh runtime-JWT authentication
and a new full c2s session without resumption after restoration, and still
preserve exact unique delivery.

After the native-server campaign, the harness requires that exact resource to
remain authority-ready with unchanged authentication and full-session counters
for 12 seconds. This separates the two independent fault campaigns so the
monkey-client RPC is not accidentally charged for a still-running server
reconciliation before its own injected outage begins.

Two additional environment-only campaigns exercise successful short
resumption without changing the workload. During a native-sync RPC to the
monkey server, the harness interrupts the monkey server's XMPP socket. During
a monkey-sync RPC to that same server, it interrupts the client socket. A
temporary 500-millisecond XMPP-only monkey-server egress qdisc and
namespace/router DROP rules retain the already-dispatched response. A temporary
500-millisecond native-server egress delay gives the passive observer time to
baseline counters during each client's preceding ten native RPCs. Both servers
finish initial authentication, authority discovery, and snapshot at 10
milliseconds before the harness changes those live FIFOs; each returns to 10
milliseconds after the campaign. Existing TCP retry and ejabberd ping policy
move the exact resource into its resumable-stream state while all network
traffic remains blocked. The harness restores the LAN as soon as ejabberd logs
that semantic transition, before the test profile's fifteen-second resume
window expires. This window includes TCP failure detection and the new
TCP/TLS/SASL stream negotiation required before the client can send
`<resume/>`; it is test-environment margin, not an application timeout.

Passive Erlang call tracing counts authority snapshot requests and verified
runtime-context copies during resume without adding a test API or changing
protocol behavior. Those trace counters are global, so
the harness waits for the pre-existing fault target and the newly started
active client to have open, authority-ready resources before recording the
campaign baseline. By the time that baseline is taken, every participant that
can still move those global counters in the campaign is therefore already
ready. Initial snapshot work from any participant therefore
cannot be misattributed to the later resumed resource. Each short campaign requires exactly
one successful resume, one pre-resume runtime-JWT reconnect, and one
verified-context copy; zero resume expiry, new bound session, or snapshot
request; and completion of the exact
original request. XEP-0198 requires authentication on the replacement XML
stream before pre-bind resumption; that authentication exchange is therefore counted
rather than misclassified as a fresh bound Cynapsa session. The final
client/server validator still requires one start,
one pass, and one server dispatch for every original matrix cell, proving the
80 RPCs complete exactly once. Per-campaign evidence and aggregate trace
counters are retained as artifacts.

## Run

Docker, Docker Compose, OpenSSL, Python 3, and current checkouts of
`CynapsaTests`, `aztmmanagement`, `Enrollment`, custom `ejabberd`, and Go Core
are required. The service checkouts are explicit so the test never silently
substitutes mocks or stale vendored code:

```sh
CYNAPSA_TESTS_PATH=/absolute/path/to/CynapsaTests \
CYNAPSA_MANAGEMENT_PATH=/absolute/path/to/aztmmanagement \
CYNAPSA_ENROLLMENT_PATH=/absolute/path/to/Enrollment \
CYNAPSA_EJABBERD_PATH=/absolute/path/to/ejabberd \
CYNAPSA_GO_CORE_PATH=/absolute/path/to/cynapsagocore \
./e2e/simple/run.sh
```

The runner verifies that commit `a21e4a23cf0c5b334e2818e2faa8b025d3d1e2b2` is
an ancestor, builds
the Linux shared library in the agent image, generates test-only TLS and
private service credentials under a unique per-run `.runtime` directory,
provisions the mesh and six agents through Management,
starts both servers, starts the client sidecars, runs all four clients
sequentially by default, and tears down its uniquely named containers, networks,
volumes, locally built images, and runtime secrets on success or failure. The
pinned shared Coturn image is retained. Sanitized logs, the
mesh snapshot, sidecar VLAN interfaces, routes, trunk firewall rules, router NAT
state, TURN per-run delta, the exact warm-up RPC's before/after counters and
packet/byte evidence, fault timeline/counters/socket-state diagnostics, rank-2
proof, fresh-session counts, short-resume authority counters, and the result
summary are retained under `artifacts/`. `core-provenance.json` records the
Core branch and HEAD plus hashes for every tracked or untracked, non-ignored
source entry used by the dirty worktree build, so the exact source snapshot is
identifiable rather than represented only by its base commit.

For a short conversation-transition check, run one native-server and one
monkey-server RPC in each order. Each two-RPC sequence keeps one Core session,
and the sequences are separated by a five-second idle interval to expose stale
Rank1-upgrade failures without application traffic masking them:

```sh
CYNAPSA_E2E_FOCUSED=1 ./e2e/simple/run.sh
```

To validate only the network prerequisite before spending time on the 80 RPCs,
run:

```sh
CYNAPSA_E2E_PROBE_ONLY=1 ./e2e/simple/run.sh
```

To run and strictly validate one client's 20-message slice, use:

```sh
CYNAPSA_E2E_CLIENT_FILTER=monkey-async-client ./e2e/simple/run.sh
```

Use a comma-separated list to preserve a specific cross-client sequence, for
example `native-async-client,monkey-async-client`.

The probe completes an XMPP STARTTLS exchange, sends tagged cross-VLAN TCP and
UDP from `10.240.10.2` to `10.240.50.2`, checks both observed source addresses,
verifies the client route uses `10.240.10.1`, rejects any untagged address or
route, checks all fourteen router VLANs and the routed auth services, proves standalone STUN infrastructure and
authenticated TURN protocol readiness, and verifies redirects and baseline
router NAT are disabled. The standalone STUN readiness probe does not make that
service part of the XEP-0215 authority exposed to agents.

Client logs contain request start/pass/fail timing records. The final validator
requires each client to contain exactly one start and pass for every
target/message pair—not merely a claimed total of 20. It also requires exactly
40 native-server dispatches (20 native payloads and 20 HTTP request payloads)
and exactly 40 monkey-server `/reverse` dispatches. The runner verifies that
ejabberd opened every Core session with its exact server-issued runtime-v2 resource. This
makes an ICE/connectivity failure distinguishable from an application dispatch,
resource-binding, or response-conversion failure.
