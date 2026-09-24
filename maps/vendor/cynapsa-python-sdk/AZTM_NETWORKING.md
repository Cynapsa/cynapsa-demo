# AZTM Networking Contract

Status: current internal transport contract

Core owns connectivity, authenticated identity, policy, route choice, replay,
and transport recovery. SDK callers provide an agent, mesh credentials, and
application payload; they cannot select or inspect a carrier.

## Carriers and retry

Rank 1 is the preferred live peer path. Rank 2 is the authenticated XMPP path.
Core tries Rank 1 first and uses Rank 2 when the
live attempt cannot reach an exact terminal receipt. Every attempt for one
logical send keeps the same Core-issued message ID. Receiver deduplication by
authenticated mesh, authenticated sender, and message ID makes fallback safe.

The one bounded carrier-neutral outbox lives in Core process memory, not in the
SDK or one XMPP connection. Replacing an XMPP session preserves unexpired
outbox entries. Process restart does not; persistence is deferred.

Rank 2 uses XEP-0198 transport counters for stanza acknowledgement, replay, and
resume safety. Those counters and the XEP-0198 session identifier are private
transport state.

## Identity, membership, and mailbox

An e2 token session binds its exact server-issued
`r2.<installation-uuid>.<nonce>` resource. The server separately associates that
resource with the requested mesh. Legacy password sessions use the mesh
resource only in legacy deployments. The server authenticates both sessions
into one mesh during a Cynapsa peer handshake before Core admits first
contact; Core does not download or infer a complete mesh-member roster.
Inbound first contact authorizes the transport-authenticated exact sender.
Membership removal closes matching peer state through an action-acknowledged
revoke control.

The current dedicated ejabberd profile rejects application writes to
`mod_offline`; it does not implement a per-installation offline mailbox.
XEP-0198 can replay stanzas during the 30-second resume window, but an
unacknowledged stanza may be lost after final session expiry. A future
exact-resource mailbox must remain independent of XEP-0198's session ID and
must purge entries on mesh-member removal, not ordinary logout.

During server mailbox or XEP-0198 replay, one strictly valid XEP-0203 `delay`
element may accompany the single Cynapsa frame either before or after that
frame. Core validates and discards this transport metadata; it never changes
or reaches the application payload. Duplicate delay/frame elements, malformed
delay metadata, unknown children, and message text fail closed.

## Fresh session

Before a fresh Rank 2 session becomes usable, Core must complete:

1. TLS with endpoint identity verification.
2. SASL authentication of the bare account.
3. Binding of the exact requested resource.
4. XEP-0198 enablement.
5. Correlated server-time calibration.
6. Local authenticated-session publication, with peers initially closed.

Peer-authority IQs are bounded, strictly decoded, and correlated to their
Core-created ID, expected server sender, exact local recipient, and active
session. A forbidden handshake returns authorization failure; a currently
unavailable target returns unavailable before a send/request is accepted.

## Resume and reconnect

A successful XEP-0198 resume establishes fresh TLS and SASL while retaining the
server session. It does not repeat resource binding or download a membership
snapshot. Core closes its old peer links on transport interruption and
reauthorizes exact peers before their queued work resumes. The server emits no
installation revoke for a successful resume. A failed resume makes a fresh
bind; the server revokes the expired old installation after its 30-second
resume window.

The bounded control lane is independent of peer traffic. Core sends an action
acknowledgment for a revoke only after all specified peer state is closed; an
XEP-0198 stanza ACK alone is not enough. A missing action ACK for 30 seconds
causes the server to close and revoke the nonresponsive installation.
Both sides probe idle XMPP connections on a 10-second interval with a
10-second reply deadline.

If resume fails, Core performs the fresh-session procedure, including exact
bind and time calibration. The process-memory outbox remains Core-owned through
either path; it does not reroute an unacknowledged message to a different
installation in this phase.

## Application outcomes

Core does not convert transport uncertainty into a public intermediate result.
Future operator telemetry may record uncertainty privately. RPC expiry is
reported as `rpc_timeout`; a one-way `msg` has no later application signal.
Intentional and sanitized remote handler errors use the canonical response
model, independent of the receiver's SDK or HTTP library.
