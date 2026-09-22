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

Current-membership and application-policy denials retain a dedicated closed
failure category through Core composition and are projected as the local,
non-retryable public `authorization_rejected` error at the `policy` stage.
Other rejected commands retain the generic `command_error` projection.

Mesh membership is the communication authorization boundary. Core currently
starts with one wildcard application allow rule, so every request already
authorized by the server's current same-mesh membership can reach SDK/ASGI
routing. `policy.set` remains available as a compatibility override that can
narrow this temporary default; the CLI demo uses `--allow '*' '*'` explicitly.

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
message ID. Integrity, identity, current membership, application-path policy,
expiry, and capacity checks fail closed before SDK publication. Current
membership comes from the server authority and has no membership lease.

Each active peer has bounded Core-owned work state under a shared count and
byte budget. Removing either peer's exact resource revokes its authority,
cancels affected work, and purges the server mailbox entries owned by that
sender or recipient resource.

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
authority-feature discovery, server-time calibration, and a complete current
membership snapshot before application traffic is released.

A successful resume performs TLS and SASL but does not bind again, rediscover
features, or fetch another snapshot. After the authority barrier and retained
transport replay complete, Core recalibrates server time, restores authority,
and publishes the session as live. A failed resume performs the full fresh
setup. A dedicated bounded control lane handles authority work; each exact
correlated IQ is acknowledged as processed only after its effect is committed,
otherwise the session closes.

Shutdown closes admission first, joins Core-owned workers, resolves waiters
once, clears secrets and retained bytes, and prevents post-close SDK delivery.
