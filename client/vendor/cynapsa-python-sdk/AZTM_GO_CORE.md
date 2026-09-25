# AZTM Go Core Contract

Status: current

The Go Core is the single owner of transport, authorization policy,
authenticated identity, envelope construction, carrier choice, retries,
deduplication, RPC correlation, payload limits, and process-lifetime send state.
Language SDKs adapt application APIs to its versioned boundary and do not
implement a second networking engine.

## Runtime boundary

Public commands and outputs use closed versioned schemas. The boundary adapter
validates every field and constructs fresh public values. Internal structs,
errors, logs, carrier names, credentials, and unknown fields do not pass through
that adapter.

Canonical HTTP responses preserve status, reason, the complete duplicate-preserving
header list, and exact body bytes. Optional application-error metadata is carried as
a separate bounded field with code, safe detail, and JSON-object details; it
never consumes or reserves an application header name.

Server-handshake membership and application-policy denials retain a dedicated closed
failure category through Core composition and are projected as the local,
non-retryable public `authorization_rejected` error at the `policy` stage.
Other rejected commands retain the generic `command_error` projection.

Core issues message IDs, conversation IDs, event IDs, request handles, and all
private carrier identifiers. User input that attempts to supply or override an
opaque identifier is rejected. A request handle grants one reply attempt for
its exact live RPC and cannot be reused as a general address.

## Messaging state

Core owns one bounded carrier-neutral outbox in process memory. It remains live
across XMPP session replacement. Each logical send retains its message ID while
Core attempts Rank 1 and, when required, Rank 2. The SDK holds no envelope copy
and performs no independent retry.

Inbound deduplication uses authenticated mesh, authenticated sender, and
message ID. Integrity, identity, server-authorized peer state,
application-path policy, expiry, and capacity checks run before SDK
publication. The application policy allows all peers and paths by default;
an explicit `policy.set` replaces it. Server-side mesh authorization remains
mandatory regardless of local policy. No complete mesh-member snapshot is
downloaded. Unknown inbound exact senders require an authenticated server
handshake before lane admission.

Each active peer has bounded Core-owned work state under a shared count and
byte budget. A logical revoke closes every installation of that agent; an
installation revoke closes only the matching server-issued session incarnation.
The dedicated server currently has no per-installation offline mailbox;
unacknowledged stanzas can be lost after the XEP-0198 resume window expires.

## RPC and delivery

The RPC default is 30 seconds and every configured timeout is positive. Core
owns and enforces expiry. SDK bindings allow a fixed additional 5 seconds only
for the Core terminal completion to cross the local boundary.

Successful command completion means Core accepted responsibility. It does not
assert remote handler execution. Carrier ambiguity stays private: RPC callers
eventually receive `rpc_timeout`, while one-way `msg` callers receive no later
application signal. Future telemetry for that ambiguity is operator-private.

## Session lifecycle

A fresh Rank 2 session completes TLS, SASL, exact-resource binding, XEP-0198,
and server-time calibration before local authority is published. Each peer
requires a server-authorized Cynapsa handshake before first use.

A successful resume preserves the server session; Core still closes old peer
links on interruption and reauthorizes them before queued work resumes.
A failed resume performs a fresh bind. The server's resumption window is
30 seconds. Both sides probe idle connections every 10 seconds, allowing
10 seconds for a reply. A dedicated bounded control lane action-ACKs a
revoke only after all specified peer cleanup is complete.

Shutdown closes admission first, joins Core-owned workers, resolves waiters
once, clears secrets and retained bytes, and prevents post-close SDK delivery.
