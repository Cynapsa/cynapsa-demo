# ADR 0006: Server-Owned Mesh Group Authentication

> Authentication v2 extends this accepted V1 decision with registered
> installation resources. The `bound_full_identity` formula below is the legacy
> password-session form. An e2 token session binds
> `authenticated_bare_identity/r2.<installation-uuid>.<nonce>` and the server
> resolves its mesh through the authoritative attachment registry. See
> [`ENROLLMENT_TOKEN_LOGIN.md`](../development/ENROLLMENT_TOKEN_LOGIN.md).

- Status: Accepted
- Scope: Membership authority and carrier-authenticated message provenance
- Supersedes: ADR 0002 per-envelope credential-proof requirement and the deferred membership assumptions in ADR 0003

## Context

The original Pod 3 interfaces assumed that a service could obtain a server-issued credential for every application envelope and independently construct a trusted membership snapshot. The pinned Mesh Server exposes neither primitive. Deriving a proof from an account password, accepting a static proof, trusting roster or presence data, or fabricating topology would create a second and weaker authority.

The deployed trust boundary already authenticates the durable session and owns mesh-group membership and routing. V1 therefore uses that server-owned group directly and carries authenticated provenance beside, never inside, the logical application envelope.

The production Rank 2 XMPP connection requires TLS 1.3 or later at its final
dial/session construction seam. System or explicitly injected trust roots and
exact endpoint-hostname verification remain mandatory. A lower or implicit
caller minimum is raised to TLS 1.3, while verification-disabled, TLS-1.2-only,
and otherwise unsupported configurations fail closed.

Each clean Rank 2 connection attempt has one absolute private setup deadline
derived from `ReconnectOperationTimeout`. It begins before dialing and remains
the sole attempt lease through TCP, TLS/XMPP stream negotiation, SASL,
exact-resource binding, stream-management activation, time calibration,
initial mailbox catch-up, proof consumption, and atomic publication. Individual
phases may shorten, but never reset, that attempt budget. A phase timeout or
cancellation closes only the unpublished candidate socket before best-effort
protocol cleanup. Cancellation callbacks are stopped or joined before the
candidate can become live, and caller cancellation takes precedence over
dependency errors. A retired attempt cannot publish a session or deliver
authentication proof after a newer client lifecycle or terminal close wins.

## Decision

### One server-owned group is one mesh authority

The authenticated Mesh Server owns a group whose exact name begins with `cynapsa_mesh_` and whose remaining exact name is the session `mesh_id`. Its current bare-account membership is the sole V1 membership authority. Group creation, removal, member addition, and member removal are authoritative server mutations. V1 supports one server node only; clustered membership replication is explicitly unsupported. Roster entries, presence, envelope fields, SDK mappings, local policy, cached traffic, and peer assertions are never membership evidence.

For a legacy password session, an authenticated account may bind the group
resource only when that account is a current member of the exact group. Its
authorized private session identity is exactly:

```text
bound_full_identity = authenticated_bare_identity + "/" + mesh_id
```

An e2 token session instead binds the registered installation resource shown
in the extension note above. In both forms, the server maps the exact session
to its mesh independently of the resource text.

At authenticated client ingress, before archive or offline retention, the
server applies the rule to every agent-directed message, all IQ types
(`get`, `set`, `result`, and `error`), and every directed presence type. It
requires the sender to equal the exact canonical bound full JID and the
recipient to be a canonical full local JID whose authenticated server session
is attached to the same mesh. It rejects bare, missing, foreign,
cross-mesh, normalization-alias, and near-match addresses. Undirected or
bare-user presence cannot be used as roster fan-out. The global routing hook
and final recipient-session hook repeat the rule, so offline storage, MAM,
resource replacement, and delayed delivery are not alternate authority paths.

Carbon sent/received, forwarded, MAM-result, and nested standard stanza
wrappers are rejected on the agent plane even when their outer address is
valid. Default namespaces are inherited from the outer `jabber:client` stanza;
prefixed element names or namespace declarations fail closed, while ordinary
non-prefixed extensions in their own default namespace remain available.
XEP-0280 wrapped delivery is intentionally unsupported because forwarding to a
different resource conflicts with this authority model. Carbon enable/disable
and legitimate local service IQs remain processable, but server metadata never
authorizes a wrapped copy. The primary server/service identity is the configured,
canonical, exactly encoded local bare authority domain with no user or resource
part. The peer endpoint must be a canonical full JID on that same host. Foreign
domains and every case, Unicode, punycode, suffix/prefix, trailing-dot, port,
resource, localpart, or bare-user variant fail closed rather than inheriting an
empty-localpart exemption. Service traffic is also payload-allowlisted to the
private authority/group IQs and the pinned time, disco, extdisco, ping, carbon
control, correlated result/error, and directed-presence flows.

XEP-0363 has one separate typed auxiliary identity: the exact canonical bare
domain `upload.` plus the configured authority host, matching the pinned
`mod_http_upload` default and Core's authenticated-domain derivation. It is not
a generic subdomain or service exemption. From a current synchronized local
full session, only one strictly bounded XEP-0363 IQ `get` request is admitted.
Back to that exact current session, only the correlated result slot or strict
echoed-request stanza error is admitted. A bounded per-session set retains only
IQ identifiers, their registration epoch, and monotonic expiries; it never
retains slot URLs, headers, or credentials. Session replacement clears
incompatible leases before the replacement is published, so a late response
cannot inherit the current session. Other stanza types, payload
namespaces, endpoint spellings, and upload-domain lookalikes fail closed.

Pod 3 repeats
the exact sender, recipient, mesh, conversation, and current-membership checks
locally; server routing does not replace local fail-closed validation.

Policy denial cannot turn a server wrapper into a reflected message. Carbon,
MAM/forwarded, and other internal packets without the exact authenticated c2s
origin marker are silently dropped. Only a denied packet stamped at client
ingress for the still-current bound PID/session epoch receives a policy error;
the error is stripped of the original payload and wrapper and can route only
from the local bare server to that exact full session. Both the global router
and final recipient hook validate the structured marker rather than trusting a
boolean metadata flag.

### Exact-session authority synchronization

Production authorization downloads and installs the complete current group
snapshot. After every fresh bind, the exact live c2s session is unsynchronized.
A successful XEP-0198 resume preserves the same logical authority session only
when no membership mutation invalidated it. New link establishment, restart,
Rank 2 publication, and authority updates remain suspended until the
post-resume `<r/>` phase separator and exactly one strict current
server-originated `resume-authority: ready` result complete on the independent
control lane. An already published authenticated Rank 1 link may continue
using its retained exact membership capability. Old results replayed before
`<r/>` are ignored. Any missing,
duplicate, malformed, stale-session, or `not-ready` current result forces a
clean bind and complete snapshot before communication.
Before fresh-session mailbox publication, application replay, or new Rank 1 establishment,
Core sends a private IQ in
`urn:cynapsa:mesh-authority:1`. The server infers the local account, host, and
mesh from that exact authenticated full-JID session. Wire input contains only
a random correlation nonce and page cursor. It contains no caller-selected
local identity, mesh, membership, or prior authority evidence.

Once installed, current membership remains authoritative for that exact
authenticated logical session without periodic refresh or local expiration.
Disconnect synchronously suspends control authority and blocks publication of
new live links. It does not itself revoke an already published Rank 1 data
capability. A successful XEP-0198 resume restores control only after the replay
and `resume-authority: ready` barrier; a fresh bind hard-fences the old snapshot
before feature discovery and a complete new snapshot. Missing peers are
authoritatively absent and do not trigger an ad hoc synchronization. The
explicit membership-refresh command remains a manual fail-closed
synchronization operation.

One page contains at most 256 strictly ascending canonical full JIDs and the
whole working set is bounded by the configured Core capacity and the 65,536
protocol maximum. The first page fixes the nonce, exact c2s PID/session epoch,
and total member count. Later pages require the same session-owned values;
mutation, replacement, duplicates, aliases, gaps, or non-ascending peers cause
a conflict and no partial publication. Only complete assembly atomically marks
that exact `{c2s PID, session epoch, mesh}` ready and returns the complete
current member set.

An effective membership mutation invalidates ready sessions before later
routing and sends each retained exact session one constant-size
snapshot-required control. The server tracks that control by its generated IQ
ID, c2s PID, SID, and bound full JID. A wrong acknowledgement, failed enqueue,
pending-control capacity failure, replacement mismatch, or acknowledgement
timeout closes only that exact session. A valid processed acknowledgement
allows the retained session to fetch and install a fresh complete snapshot.

The server permits durable application submission only from a current member
whose exact sending session is ready; the recipient must be a current member
but may be offline. Later online delivery requires the exact recipient session
to be ready and does not require the original sender still to be online.
Rank 1/Jingle establishment requires both endpoint sessions live, current
members, and ready. Final routing repeats the exact current-session check.

Core caches only the local identity and peers in the installed complete
snapshot. An unknown peer is authoritatively absent and does not trigger a
network query. While the authority gate is blocked, admitted peer work stays
bounded and performs no carrier publication; RPC deadlines continue to run.
Snapshot replacement fences all peers first, atomically applies retained and
removed identities, and only then reopens retained peer lanes. Removed peers
are closed and their queued work is retired. Local lifecycle epochs prevent a
late callback from a replaced transport or authority operation from publishing
into the current session; those epochs are private ownership guards, not
membership evidence.

Final carrier publication and application delivery retain the existing
context-cancellable topology admission through ownership transfer. Authority
replacement terminally retires revoked peer work while preserving retained
peer outbox identities for retry. A one-way message that has been accepted
locally does not gain a later SDK-visible delivery error. An RPC whose response
does not arrive before its requested lifetime ends returns the normal
`rpc_timeout` result. Payload and proof bytes are zeroized when their owning
operation reaches a terminal state.

`mesh.list` retains its public shape and reveals only the installed current
session mesh and active state without network I/O. Only the explicit
`mesh.membership.refresh` command performs a fresh private authority
synchronization. Removed or unsynchronized sessions return
`connectivity_unavailable`, never stale `active=true`; missing, revoked, or
cross-mesh peers remain rejected.

### Carrier-authenticated provenance

An envelope is accepted only with one immutable typed provenance value created by the trusted carrier adapter:

- `group_rank2`: the durable session authenticated the outer stanza sender and recipient as exact full identities in the named mesh group.
- `bound_rank1`: a Rank 1 session was established only through an authenticated same-mesh Rank 2 XEP-0166 Jingle handshake. The closed Jingle profile carries XEP-0176 ICE-UDP candidates, XEP-0320 DTLS fingerprints, and XEP-0343 SCTP DataChannel parameters; raw SDP is never sent through XMPP. Both peers require current authority before negotiation and immediately before publishing the link. Pion authenticates the negotiated DTLS peer certificates against the fingerprints delivered by the authenticated full JIDs. The provenance includes a nonzero 32-byte channel-binding identifier derived from a domain-separated digest of the exact mesh, Jingle SID, initiator and responder full JIDs, and both authenticated DTLS fingerprints. This identifier is local evidence binding the DTLS-protected, infrastructure-confidential RTCDataChannel to that authenticated transcript; it is neither a session secret nor a reusable credential. Path recovery retains that binding and data channel: the lower full identity exclusively initiates a complete-gathering `transport-replace`, the peer returns `transport-accept`, both sides require the original SID/endpoints and current authority before and after application, and success additionally requires refreshed ICE connectivity. A bounded failure falls back to a new fully authorized link.

Rank 1 establishment is rejected unless both full identities are current members
of the same authoritative group and the offer, answer, live link, and current
topology agree on their exact identities and mesh. A membership fence blocks
old Rank 1 provenance immediately; reauthorization requires a current snapshot
and a newly validated link publication. Rank 1 performs no independent clock
or membership exchange.

The provenance sender, recipient, and mesh must exactly equal the envelope fields and local authenticated session. Carrier kind and channel binding are included in the opaque inbound permit used between pre-materialization and final authorization. A permit obtained on one carrier cannot be substituted for another carrier attempt, although separately authenticated attempts with the same logical envelope still converge through stable message identity and bounded deduplication.

### No per-envelope credential

V1 does not request, derive, store, sign, or verify a server credential for each envelope. Account passwords remain input-only authentication material. There is no password-derived proof and no static proof.

ADR 0002 key 12 is reserved and must be omitted. A received key 12, including a nonempty legacy `credential_proof`, is rejected by canonical decoding. The private Go `CredentialProof` compatibility member remains temporarily present only so transport-owner branches can migrate without an unsafe partial type split; it is bounded, ignored by canonical encoders and digests, and explicitly rejected at authenticated messaging ingress. It has no authorization meaning and will be removed at the next private wire-version boundary.

The deterministic complete envelope encoding is proof-free. Reserved legacy
wire positions remain forbidden. `CanonicalIntegrityBytes` remains as a
compatibility name for the complete bytes, and `EnvelopeIntegrityDigest`
remains the carrier-descriptor-sensitive digest.

### Fail-closed order

Inbound processing performs these stages in order:

1. bounded canonical envelope validation;
2. typed carrier provenance validation and exact sender/recipient/mesh/conversation binding;
3. deterministic envelope and permit binding;
4. calibrated creation/expiry checks;
5. exact current authoritative-membership check;
6. bounded message-identity dedupe reservation;
7. bounded payload materialization and integrity validation;
8. current-membership recheck and application policy;
9. current-authority read admission held through mandatory local delivery acceptance.

Invalid provenance performs no clock read, topology read, dedupe insertion,
materialization, or application delivery. No later stage can turn rejected
provenance into an accepted message.

### Offline delivery and replay

Offline Rank 2 delivery is accepted only when the delivered outer stanza has
authenticated full-identity provenance and both participants are current
members at receipt. The original logical message ID, creation time, request
expiry, correlation, and payload digest remain unchanged. Reconnect, offline
delivery, ambiguous retry, and carrier fallback all pass through the same
bounded dedupe table. Duplicate identity is exactly authenticated sender plus
mesh plus canonical message ID; later copies with that identity are dropped
without comparing their content. Membership removal fences the peer and
retires its local queued work before a later re-add can authorize new work.

Rank 2 applies the same retirement boundary to transport-owned durability. One
carrier-neutral, process-RAM Core outbox is authoritative; XEP-0198 is only the
sent/unacknowledged stream ledger. Envelope ledger records retain bounded
identity metadata and hydrate their wire bytes from that Outbox, rather than
retaining a second full payload copy. Rank 2 claims an entry at its serialized
live write gate, assigns a transport-private ownership ordinal, and never
migrates that owned entry to Rank 1. A raw XEP-0198 handled count is not
mailbox-custody evidence: it may clear the transient ledger but cannot retire
an envelope from the Core Outbox. Only a strict custody-accepted control from
the authenticated bare authority, correlated to the canonical message ID and
emitted after the mailbox transaction commits, retires that exact owned entry.
After a clean bind, exact-session discovery and snapshot installation precede
outbox publication. After an accepted resume, the server replay barrier and
its independent authority-control lane precede peer publication; a replayed
invalidation requires a complete snapshot before outbox publication. Any
ownership inconsistency fails closed while the original outbox entry remains
available to a later authorized session.

Mailbox custody can retire an RPC request's payload before its response returns. Therefore the bounded RPC table retains a process-lifetime, fixed-size cryptographic binding of the issued request message ID, correlation, conversation, mesh, sender, and recipient. It retains no payload or application path. An exact authenticated response received after local timeout or cancellation is consumed and acknowledged using that binding; unknown or field-confused responses remain rejected. This prevents a valid delayed mailbox response from becoming a permanently replayed poison message without duplicating request data.

The server delivers that control only to the exact authenticated c2s PID, SID,
and full JID that submitted the committed packet. A replacement session cannot
inherit it. If the original session disappears after commit, the acceptance is
lost, the Core retains ownership, and a fresh authenticated session replays the
same canonical message ID for idempotent server custody.

That replay reuses the existing mailbox row when destination resource,
authenticated sender resource, and canonical message ID match. It neither
compares content nor consumes another mailbox quota slot.

Once the sender Core validates that positive server custody control, the
exact-resource Mesh Server mailbox owns the eligible frame until the destination
stream acknowledges it. A stream acknowledgement without that control does not
transfer ownership. Its durable key is destination
`{user, host, exact resource}` plus a server-private mailbox position and
contains no sender or recipient session identity. A fresh session
for that exact resource must complete authority synchronization before drain.
Removing a member atomically purges the destination mailboxes and queued rows
for exact resources attached to that mesh, so re-addition cannot resurrect
them. Resources attached to other meshes are untouched.

Rank 1 has no terminal DataChannel-write acknowledgement. After an exact live
write, the single Core outbox changes the entry from a borrowed attempt to
`Rank1PendingACK` and releases the attempt slot and envelope borrow. The
receiving Core emits a private receipt on that exact authenticated Rank 1
session only after provenance, envelope, dedupe, payload, membership, and
policy checks have succeeded and that exact envelope is terminally consumed or
accepted by the SDK event boundary. The receipt binds message, conversation,
sender, recipient, mesh, and the local channel binding. Exact terminal
duplicates may be receipted; duplicates still in flight, capacity or policy
rejection, and failed admission are not. An exact current receipt retires the
outbox entry. Timeout, disconnect, or authority loss makes the same immutable
entry Rank-2-only and wakes automatic draining; it does not allocate a second
queue item. Late, duplicate, stale-session, or forged receipts are inert. Once
Rank 2 owns the entry, it never migrates back to Rank 1.

Every delayed current-session loss, clock invalidation, authority notice,
ingress completion, and reconnect request is fenced by the exact lifecycle,
state-publication, Session, and ingress owner that observed it. Close,
replacement, recovery, or any newer same-session state edge makes the older
work inert. Authority-fence panic cannot leave an admitted current loss live:
it publishes pending and one bounded recovery request without reopening
authority. Activation of a still-current ingress is one-shot and
continues to receive after a stale live publication is rejected, while a
replacement ingress receives an exact handoff.

An expired request releases no application delivery. Replay of an already-seen
scoped message ID is idempotently dropped without a content comparison, and a
carrier transition cannot create a second logical delivery. V1 retains ADR
0003's explicit process-lifetime limitation: restart clears dedupe state, so
durable cross-process replay continuity remains future work.

## Security consequences

- Authentication authority is not duplicated in Pod 3.
- A compromised group member can send only as its authenticated full identity; it cannot claim another sender, mesh, recipient resource, or Rank 1 binding.
- Server group misconfiguration or compromise remains in the trusted computing base and cannot be repaired by an application-envelope field.
- A lost notification cannot preserve authority because server routing checks
  exact current ready sessions and the mutation fences them before notification.
  The local gate nevertheless blocks synchronously on every observed authority
  loss before asynchronous cleanup.
- Public results remain transport opaque and never reveal full private identities, carrier kind, or channel-binding bytes.

## Cross-owner implementation

Pod 4 supplies authenticated outer-stanza provenance, exact-session authority
synchronization/control, and the XEP-0166/Pion bound Rank 1 handshake result.
Root composition owns the synchronous shared authority fence, bounded
peer-selective transition, live-link registry, transport selection, inbound
provenance pumps, and reverse-order shutdown. Pod 6 configures and observes a
disposable server-owned group to verify exact-session readiness, offline durable
delivery, mutation races, selective removal/re-addition, cross-resource
suppression, fresh Rank 1 establishment, and public message/request/reply
behavior. No owner may manufacture successful provenance or authorization
when exact server evidence is absent.
