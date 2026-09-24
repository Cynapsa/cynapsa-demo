# Simple real-mesh E2E

The SDK does not contain an ejabberd image, module, or configuration. Both
runners build the Dockerfile in the dedicated ejabberd checkout selected by
`CYNAPSA_EJABBERD_PATH`, and read their server fixtures from its
`test/sdk-e2e/` directory. `run-peer-authority.sh` is the current 80-RPC
baseline for the `remove-snapshot` Go Core and dedicated ejabberd branches.
`run.sh` is a historical password/resource/snapshot deep-fault campaign;
do not use it to qualify the current peer-authority protocol.

The peer-authority runner compiles every custom module from the dedicated
ejabberd worktree, checks that both worktrees are on `remove-snapshot`, and
records exact source manifests. It uses the same six SDK applications and
router/VLAN topology, but authenticates through disposable runtime-v2 grants.
A local HTTPS enrollment fixture answers the Core's fixed enrollment URL only
inside the test containers; its per-run CA, JWT signing key, and grants are
deleted during cleanup. The test-only ejabberd profile uses signed runtime
claims, while production uses the Management-backed attachment registry. This
runner therefore checks SDK/Core/server login, peer handshake, and the 80
request/reply cells. Client exits exercise installation-revoke handling and
the separate packet/action acknowledgments: both server XMPP sessions must
remain open as each client finishes. This matters because an 80/80 request
count can otherwise conceal a server reconnect storm between client runs. An
unexpected server disconnect fails the run and is captured in
`unexpected-server-session-closures.log`. It does **not** qualify production
Enrollment, Management, registry provisioning, membership revokes, resume
expiry, or fault recovery.

The evidence build records bounded routing metadata and exact peer-authority
IQ IDs on inbound rejection and authorization send/return. It does not log
message bodies or credentials. The verifier accepts a complete JSON diagnostic
followed immediately by a Go evidence record, because Docker may concatenate
concurrent stdout/stderr writes on one line; malformed diagnostics still fail.
The peer-authority runner also checks a real A→B→C error path: native and
ordinary Requests clients (A) call the native server (B), which calls the
FastAPI server (C). C returns 429 with a body and headers. B explicitly
forwards it, deliberately remaps it to a 503 `RPCException`, or leaves it
unhandled for a sanitized 500. The verifier requires all six client checks
and matching B/C dispatch evidence without changing the 80-cell matrix.

This harness runs six Python SDK application containers, one real ejabberd
container, and separate Coturn STUN-only and authenticated TURN containers
behind a single router-on-a-stick container. One private internal
Docker bridge carries Ethernet frames only. It is an 802.1Q trunk, not an
application IP network. Each application container and ejabberd container shares
the network namespace of a dedicated LAN sidecar through
`network_mode: service:<sidecar>`.

Each sidecar owns one unique VLAN and `/24`; the router terminates all nine
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

The historical runner still requires a Go Core worktree based on pinned commit
`a21e4a23cf0c5b334e2818e2faa8b025d3d1e2b2`, but its ejabberd image is
built only from the dedicated server checkout. Its old snapshot assertions
require a compatible historical server implementation and are not a current
release gate.
`run.sh` registers six runtime-generated accounts,
creates `simple-e2e`, adds every account through `cynapsa_mesh_add`, and checks
that all six bare JIDs appear in the authoritative snapshot. Every Core binds
the exact `simple-e2e` XMPP resource. This exercises membership-gated resource
binding and same-resource server routing. Each fresh Core session must also
discover exactly `urn:cynapsa:mesh-authority:1`, install and acknowledge its
complete authority snapshot, and process membership-control IQs on the
independent control lane. The same ejabberd extension transfers that exact
ready state across a successful XEP-0198 resume and emits the server-owned
resume-authority result that releases Core's replay barrier.
Replayed application messages may carry one XEP-0203 delayed-delivery element
before or after their single Cynapsa frame, matching ejabberd's offline replay
shape. Core strictly validates and discards that transport metadata before the
unchanged frame reaches the SDK.

The original `run.sh` intentionally remains the legacy-credential
compatibility qualification: it registers test accounts directly and binds the
legacy mesh resource. The local enrollment fixture in `run-peer-authority.sh`
is only a protocol test seam, not production authentication. Full
Management/Enrollment integration, installed-profile restart, and the
provider-backed matrix belong to the cross-repository `CynapsaTests`
acceptance environment.

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
each server and validates 20 results; the peer-authority baseline requires both
80/80 and stable server XMPP sessions. Each
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
server override. TURN credentials use a per-run random static-auth secret. The
secret and account passwords stay in a mode-0700 runtime directory, are removed
unconditionally, and are scanned out of retained artifacts.

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
client until ejabberd reports that the exact resource's five-second XEP-0198
resume window has expired. Waiting for the server lifecycle event accounts for
variable TCP failure-detection time while remaining below the fault campaign's
60-second RPC TTL. The margin covers Core's 30-second Rank1 no-progress window
followed by stale-stream expiry, fresh SCRAM authentication, resource binding,
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
Full-session, SCRAM-authentication, and resumption baselines are sampled
immediately before network restoration. The applications and Core receive no
fault flag and perform no test retry. Each outage must increment both DROP counters,
produce a post-fault XEP-0198 expiry, then produce fresh SCRAM authentication
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

Passive Erlang call tracing counts authority discovery, snapshot requests, the
resume hook, and ready/not-ready resume-authority results without adding a
test API or changing protocol behavior. Those trace counters are global, so
the harness waits for the pre-existing fault target and the newly started
active client to have open, authority-ready resources before recording the
campaign baseline. By the time that baseline is taken, every participant that
can still move those global counters in the campaign is therefore already
ready. Initial discovery or snapshot work from any participant therefore
cannot be misattributed to the later resumed resource. Each short campaign requires exactly
one successful resume, one pre-resume SCRAM reconnect, one resume hook, and one
ready barrier result; zero resume expiry, new bound session, authority
discovery, snapshot request, or not-ready result; and completion of the exact
original request. XEP-0198 requires authentication on the replacement XML
stream before pre-bind resumption; that SCRAM exchange is therefore counted
rather than misclassified as a fresh bound Cynapsa session. XEP-0215 external-
service discovery, including any resume-time external-service replay or
requery, concerns ICE service configuration; it is distinct from Cynapsa
mesh-authority discovery and does not increment these mesh-authority discovery
counters. The final
client/server validator still requires one start,
one pass, and one server dispatch for every original matrix cell, proving the
80 RPCs complete exactly once. Per-campaign evidence and aggregate trace
counters are retained as artifacts.

## Run

For the current peer-authority baseline, use:

```sh
./e2e/simple/run-peer-authority.sh
```

It defaults to sibling `cynapsagocore-remove-snapshot` and
`ejabberd-remove-snapshot` worktrees. Override either path explicitly with
`CYNAPSA_GO_CORE_PATH` or `CYNAPSA_EJABBERD_PATH`. The runner requires the
`remove-snapshot` branch in both, builds the actual dirty source trees, checks
all four client reports and both server dispatch counts, and saves sanitized
evidence plus both source manifests in `e2e/simple/artifacts/`. The local
enrollment hostname override is confined to the test's internal Docker network.
It does not modify host DNS or contact production Enrollment.

The legacy deep-fault runner below is retained as historical test machinery,
not a current release gate. It also requires `CYNAPSA_EJABBERD_PATH` to point
at a dedicated checkout with `test/sdk-e2e/legacy.yml`.

Docker, Docker Compose, OpenSSL, and Python 3 are required. By default the
harness discovers the Go worktree at `../cynapsa/cynapsagocore` relative to
this repository:

```sh
./e2e/simple/run.sh
```

Override it with another worktree based on the pinned Core commit:

```sh
CYNAPSA_GO_CORE_PATH=/absolute/path/to/cynapsagocore ./e2e/simple/run.sh
```

The runner verifies that commit `a21e4a23cf0c5b334e2818e2faa8b025d3d1e2b2` is
an ancestor, builds
the Linux shared library in the agent image, generates test-only TLS and
passwords under a unique per-run `.runtime` directory, provisions the mesh,
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
route, checks all nine router VLANs, proves standalone STUN infrastructure and
authenticated TURN protocol readiness, and verifies redirects and baseline
router NAT are disabled. The standalone STUN readiness probe does not make that
service part of the XEP-0215 authority exposed to agents.

Client logs contain request start/pass/fail timing records. The final validator
requires each client to contain exactly one start and pass for every
target/message pair—not merely a claimed total of 20. It also requires exactly
40 native-server dispatches (20 native payloads and 20 HTTP request payloads)
and exactly 40 monkey-server `/reverse` dispatches. The runner verifies that
ejabberd opened every Core session with the exact `/simple-e2e` resource. This
makes an ICE/connectivity failure distinguishable from an application dispatch,
resource-binding, or response-conversion failure.
