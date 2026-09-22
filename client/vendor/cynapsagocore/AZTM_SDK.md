# AZTM SDK Contract

Status: current public API contract

The SDK exposes synchronous and asynchronous native sessions plus selective
HTTP Bridge hooks. Core owns policy, identity, retries, IDs, deduplication, RPC
expiry, and send state. Requests and responses use dependency-free canonical
models; framework-specific objects exist only at the originating or receiving
SDK adapter.

## Entry points

`connect()` and `connect_async()` return native `Session` objects. `login()` and
`login_async()` install HTTP hooks and return bridge handles. `login` has no
mode argument.

All four entry points require a mesh ID and support three authentication
paths. Initial enrollment supplies a `cpsa_` enrollment token and an optional
local profile ID, which defaults to `default`. Later starts normally omit the
token and reopen that installed profile. During the compatibility period,
supplying any legacy field selects legacy authentication and requires the
complete mesh-endpoint, username, and password set; legacy inputs cannot be
combined with an enrollment token or non-default profile selection.

Endpoint discovery, installation credentials, session resources, renewal, and
provider tokens remain Core-owned. Token or installed-profile authentication
returns immutable `AztmAuthInfo` metadata, including the Core-issued
installation ID, but never exposes those private credentials or the discovered
mesh endpoint. The enrollment token is bootstrap input rather than retained
Session state.

An HTTP Bridge `address_map` maps each exact origin to:

```python
{"recipient": "agent@example", "mode": "rpc"}  # or "msg"
```

The key is only `scheme://host[:port]`. The SDK preserves the request URL path
and query in the canonical request. ASGI integration may attach a concrete
`asgi_app`, or `cynapsa run` may observe the single in-process app passed to the
supported server runner. Reload, pre-fork, and multiple workers are outside the
current CLI contract.

## Universal request and response models

Every native or hooked call is normalized to an immutable `CynapsaRequest`
containing method, absolute path, query, duplicate-preserving header pairs,
and exact owned body bytes. Native shorthand accepts exact `bytes`, UTF-8
`str`, strict JSON-compatible values, or an explicit `CynapsaRequest`; it uses
`POST`, the selected path, and an empty query.

Every RPC application result is a `CynapsaResponse` containing status, reason,
duplicate-preserving header pairs, exact owned body bytes, and optional safe
`CynapsaApplicationError` metadata. Native request calls return this model
directly. They never raise automatically for a remote 4xx/5xx response;
`raise_for_error()` is explicit.

The optional error is a distinct canonical response field, never a reserved
application header. Its details are a bounded JSON object. A response carrying
error metadata must use a 4xx or 5xx status; 4xx/5xx responses without that
metadata remain valid, including responses produced by HTTP frameworks.

HTTP Bridge preserves the normal calling style and returns the expected
`requests`, `httpx`, `urllib3`, `urllib.request`, or `aiohttp` response object.
The server never needs to know which caller library will receive its response.

Current payloads are bounded, fully materialized bytes, text, JSON, or HTTP
bodies. Automatic advanced streaming is not available.

## Handlers

`Session.on(path, handler)` registers an exact path; `"*"` is the explicit
catch-all. Registration has no mode. One handler serves RPC and one-way calls;
the sender's operation or HTTP-origin mapping chooses the semantics.

For RPC, the handler return is normalized exactly once:

- `dict`, `list`, and JSON scalar values become `200` JSON.
- `None` becomes successful `200` JSON `null`.
- `str` becomes `200` UTF-8 text and `bytes` becomes `200` binary.
- An explicit `CynapsaResponse` preserves its status, headers, body, and error.
- `raise RPCException(...)` creates an intentional safe application error.
- Any unsupported return or unexpected exception becomes a sanitized
  `500 handler_error`; raw details stay in local diagnostics and logs.

`request.reply()` does not exist. For a one-way call the handler still runs,
but its return value is discarded and exceptions remain local.

For an HTTP Bridge `msg`, the hook returns an empty synthetic `200 OK` only
after successful Core acceptance. Failure, timeout, and cancellation propagate
through the calling library and never look successful.

## Timeouts and IDs

Command and RPC defaults are 30 seconds. An omitted or zero request TTL selects
the configured RPC default; an explicit nonzero value must be positive. Core
enforces RPC expiry. The SDK waits up to 5 additional seconds for the Core
terminal completion before raising an SDK safety timeout.

Message IDs, conversation IDs, event IDs, and request handles are opaque
Core-issued values. The SDK rejects application attempts to inject or alter
them. Successful send acceptance is not proof of remote handler execution.

Core/authentication/connectivity/queue/timeout failures are SDK exceptions. A
valid remote application response, including 4xx or 5xx, remains a response.
