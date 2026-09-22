# AZTM SDK Boundary Contract

Status: normative

This boundary applies to every SDK function, model, callback, event, error,
status value, capability, diagnostic, native export, and document intended for
application developers.

Forbidden SDK-visible vocabulary includes:

```text
carrier implementation names
signaling protocol names or extension numbers
connectivity-library names
private route tiers
private credential and replay state
```

## Mandatory Architecture

All bindings use one path:

```text
application API
  -> versioned SDK model
  -> versioned native ABI
  -> Core boundary adapter
  -> Core runtime
```

The adapter decodes closed command schemas and projects fresh allowlisted
results. Public types never alias internal types. Raw maps, envelopes, errors,
causes, logs, metrics, credentials, carrier state, and unknown fields never
cross the boundary. Unknown public fields fail closed; unknown internal values
map to a bounded generic public failure.

## Ownership split

The SDK owns language ergonomics, sync/async adaptation, native value
conversion, supported HTTP client hooks, explicit ASGI attachment, local
handler dispatch, and normalized client-library exceptions.

Core owns authenticated identity, policy, transport, route choice, envelopes,
message IDs, conversation IDs, event IDs, request handles, RPC correlation and
expiry, message-ID deduplication, retry, payload limits, and the bounded
process-memory outbox. The SDK stores no retryable envelope.

`login` has no mode argument. Native and monkey behavior is selected only by
the SDK entry point and installed hooks. Each HTTP origin mapping names an
agent and chooses `rpc` or `msg`; Core receives only the normalized
origin-to-agent policy, while the SDK retains the hook choice and preserves URL
path and query in the payload.

## Canonical application models and handler rules

Public application calls normalize to dependency-free `CynapsaRequest` and
`CynapsaResponse` models. They preserve method/status, path/query where
applicable, duplicate-preserving header pairs, and exact owned body bytes. Responses
may also carry bounded, safe canonical application-error metadata. Framework
objects never cross the native boundary or mesh. Automatic advanced streaming
is not a current capability.

The application error is an explicit optional response field containing code,
detail, and JSON-object details. It is not tunneled through a header: the full
duplicate-preserving header list, including any application header with a similar name,
is preserved unchanged. Error metadata is valid only with a 4xx or 5xx status.

### Coordinated pre-release V1 cutover

The application-error field is an explicitly accepted coordinated cutover of
the pre-release V1 ABI and canonical payload schema; it is not a
backward-compatible rolling extension. A current decoder accepts a legacy V1
HTTP response in which the optional error field or canonical key is absent.
An older strict V1 decoder rejects a response that contains the new field or
key. Every paired Core and SDK deployment that may emit or consume application
errors therefore MUST upgrade together before the field is used. Mixed-version
rolling deployment of this extension is unsupported, and neither side may
claim compatibility based only on the unchanged numeric V1 marker.

`Session.on(path, handler)` has no mode and `request.reply()` does not exist.
The sender chooses RPC or one-way semantics. For RPC, every normal return is
normalized once; `None` is successful JSON `null`. `RPCException` creates an
intentional safe error response. Unexpected exceptions and unsupported returns
create a sanitized `500 handler_error` response while raw details stay local.
For one-way calls, returns are discarded and exceptions stay local.

## Identifiers, errors, and outcomes

Opaque Core identifiers may be observed and returned only in the operation for
which Core issued them. Application-supplied injection or mutation is rejected.
Inbound duplicates are suppressed using authenticated mesh, authenticated
sender, and message ID.

Public Core errors use stable codes and boundary-owned messages with no raw cause.
Malformed destination identities are generic command validation failures and
map to `command_error`; they are not authorization evidence. A syntactically
valid destination that is absent from the current authorized membership
snapshot maps to `authorization_rejected`.
An operation denied by the Core's current local membership or application
policy returns the non-retryable local error `authorization_rejected` at the
`policy` stage. Generic command validation and state rejection remain
`command_error`; the two outcomes are not inferred from private error text.
Valid remote application 4xx/5xx results remain canonical responses and never
become Core errors. Native callers may explicitly call `raise_for_error()`.
Transport uncertainty remains private and may only gain private operator
telemetry in the future. A successful send means Core accepted ownership, not
that a remote handler ran.

Command and RPC defaults are 30 seconds. A zero per-call TTL selects the
configured RPC default. Core enforces RPC expiry; the SDK adds only a fixed
5-second completion safety window.
