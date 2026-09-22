# AZTM Networking Contract

Status: current internal transport contract

Core owns connectivity, authenticated identity, policy, route choice, replay,
and transport recovery. SDK callers provide an agent, mesh credentials, and
application payload; they cannot select or inspect a carrier.

## Carriers and retry

Rank 1 is the preferred live peer path. Rank 2 is the authenticated XMPP path
and server mailbox fallback. Core tries Rank 1 first and uses Rank 2 when the
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
resource with the requested mesh. A legacy password session uses the requested
mesh ID as its resource. Current membership is the complete authority snapshot
for the authenticated session; there is no membership lease. Live authority
notices trigger a fresh authority decision and cannot grant access by
themselves.

The server mailbox is keyed to exact sender and recipient resources and has a
lifetime independent of any XEP-0198 session identifier. Removing either the
sender resource or recipient resource purges matching mailbox entries. A new
resource does not inherit another resource's mailbox authority.

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
5. Discovery of exactly the supported authority feature.
6. Correlated server-time calibration.
7. Installation of one complete current-membership snapshot.

Each feature and IQ response is bounded, strictly decoded, and correlated to
its Core-created ID, expected server sender, exact local recipient, and active
session. Any mismatch fails closed.

## Resume and reconnect

A successful XEP-0198 resume establishes fresh TLS and SASL but does not repeat
resource binding, feature discovery, or snapshot download. Replayed control
work enters a dedicated bounded control lane. Application traffic remains
fenced until all preceding control effects are committed and one exact
correlated `resume-authority` IQ reports `ready` for that session. After that
barrier and retained transport replay complete, Core recalibrates server time,
restores authority, and publishes the session as live.

The control lane is independent of peer traffic. Core marks each exact IQ as
processed only after the consumer commits its authority effect. If the lane is
full, the IQ is malformed or mismatched, acknowledgement cannot be committed,
or `resume-authority` is absent, duplicated, or not ready, Core closes the
session. It does not guess authority.

If resume fails, Core performs the full fresh-session procedure, including a
new exact bind, feature discovery, time calibration, and complete snapshot.
The process-memory outbox remains Core-owned through either path.

## Application outcomes

Core does not convert transport uncertainty into a public intermediate result.
Future operator telemetry may record uncertainty privately. RPC expiry is
reported as `rpc_timeout`; a one-way `msg` has no later application signal.
Remote handler-error responses are not currently defined.
