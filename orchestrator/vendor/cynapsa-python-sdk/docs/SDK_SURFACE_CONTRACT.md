# Cynapsa Python SDK surface contract

This document is the authoritative Python contract for canonical application
requests and responses. Go Core owns authentication, authorization, routing,
transport selection, retries, correlation, expiry, identifiers, and queueing.
The Python SDK owns conversion at the application boundary.

## Canonical flow

```text
native or HTTP caller
  -> CynapsaRequest
  -> Go Core / mesh
  -> native handler or ASGI app
  -> CynapsaResponse
  -> Go Core / mesh
  -> native response or caller-library response
```

Framework objects never cross the ABI or mesh. A server does not know whether
the caller used the native API, Requests, HTTPX, urllib3, urllib.request, or
aiohttp. The originating adapter alone reconstructs its library's response
object.

## `CynapsaRequest`

`CynapsaRequest` is dependency-free and immutable. It contains an HTTP-style
method, absolute path, query string, ordered duplicate-preserving headers, and
an owned exact `bytes` body.

Inbound native handlers additionally observe read-only Core-issued context:
`message_id`, `conversation_id`, `from_agent_id`, `mesh_id`, and whether the
sender selected `rpc` or `msg`. These values are transport context, not fields
that application headers or bodies can inject into the Core envelope.

`.content`, `.text()`, and `.json()` are explicit accessors. Native shorthand
is normalized as `POST`, with the supplied path, an empty query, the selected
content type, and exact encoded bytes. HTTP hooks normalize after the HTTP
library prepares the request.

## `CynapsaResponse`

`CynapsaResponse` contains status code and reason, ordered
duplicate-preserving headers, an owned exact `bytes` body, and optional
`CynapsaApplicationError` metadata.

Error metadata is a separate canonical response field, not a reserved HTTP
header. All application headers—including a header named `x-cynapsa-error`—are
preserved normally. Metadata contains a bounded code, safe detail, and JSON
object details, and is valid only with a 4xx or 5xx status. Plain 4xx/5xx
framework responses remain valid without metadata.

`.content`, `.text()`, and `.json()` expose the body. A 4xx or 5xx application
response is returned normally. `.raise_for_error()` is an explicit opt-in that
raises `RemoteApplicationError`; remote application errors never raise
automatically.

At a monkeypatched caller, the same response becomes the intercepted library's
normal response type. Core and transport failures remain SDK exceptions for
native calls and become appropriate library errors for monkeypatched calls.

## Handler registration and dispatch

`session.on(path, handler)` registers one exact path. The literal `*` is the
only catch-all. There is no handler `mode` parameter and no `any` mode. One
handler serves both RPC and one-way delivery; the sender chooses the semantics.

```python
@session.on("/reverse")
def reverse(request: cynapsa.CynapsaRequest):
    return {"value": request.json()["value"][::-1]}
```

For RPC, returning drives the response:

| Return or exception | Canonical result |
| --- | --- |
| `dict`, `list`, scalar JSON value | `200` JSON |
| `None` | `200` JSON `null` |
| `str` | `200` UTF-8 text |
| `bytes` | `200` binary |
| `CynapsaResponse` | exact status, headers, body, and error metadata |
| `raise RPCException(...)` | intentional safe canonical application error |
| unsupported return or unexpected exception | sanitized `500 handler_error` |

`request.reply()` does not exist. Sync and async handlers follow the same
contract after an awaitable is resolved.

For msg, the handler runs but its return value is discarded without creating
or transmitting a response. An exception is recorded locally and no remote
response is sent.

Unexpected exceptions and tracebacks remain in local logging/diagnostics. Only
the stable `handler_error` and normalized message cross the mesh.
`RPCException` is different: its status, code, detail, JSON details, and
headers are deliberately declared safe by application code.

## HTTP and ASGI adapters

Requests, HTTPX sync/async, urllib3, urllib.request, and aiohttp hooks convert
their prepared calls to `CynapsaRequest`. RPC responses are reconstructed as
the caller library's normal response object while preserving exact body bytes
and duplicate headers wherever that library has a raw-header representation.

The ASGI bridge converts `CynapsaRequest` into ASGI scope/receive events and
captures ASGI output as `CynapsaResponse`. Mounted applications are routed by
ASGI itself. Framework identity is never added to the envelope.

Each virtual-origin mapping chooses `{recipient, mode}`. That mode is an
outbound caller choice, not a handler registration choice. In msg mode an HTTP
hook returns a synthetic empty `200 OK` only after Go Core accepts complete
ownership. This does not prove remote delivery or handler work.

## Error boundary

```text
authentication, mesh authorization, connectivity, queue, timeout,
invalid/unsupported payload
  -> transport/Core SDK error

valid remote 4xx/5xx application response
  -> CynapsaResponse or caller-library response
```

RPC expiry belongs to Go Core. Omitted or zero `ttl_ms` selects the configured
default, normally 30 seconds. The SDK waits five additional seconds only for a
terminal Core completion; that guard does not extend the wire deadline.

## Encoding, ownership, and concurrency

The SDK accepts documented HTTP-like values: exact bytes, exact strings,
strict JSON-compatible values, or an explicit canonical model. It does not
pickle objects or invoke arbitrary serializers. Applications encode and decode
their own schema exactly as with ordinary HTTP libraries.

Current payloads are fully materialized and bounded by the configured inline
limit. The Python/ABI/Go ownership transition can create temporary copies. A
successful send or synthetic msg response means Core owns everything required,
so the caller may release its original value. Shared memory and automatic
streaming are future work.

Go networking and Python handler execution run on separate workers but share
one process. Normal Python waits do not stop Go networking. Process suspension,
OOM, process death, native code holding the GIL, or severe CPU starvation are
not isolated; strict fault isolation requires a separate Core process.

## Wire and CLI compatibility

The frozen Go ABI still names its wire variants `http_request` and
`http_response`. Those names are internal details. Public Python code uses
`CynapsaRequest` and `CynapsaResponse`. Historical payload class names are not
exported through `cynapsa.__all__`; historical request names resolve only to
the canonical model and therefore expose no reply API.

`cynapsa run` continues to install authentication, HTTP hooks, optional ASGI
discovery, and policy rules before running the target in-process. It does not
support multi-worker, reload, pre-fork, Trio, or native-executable targets.
Its normal authentication inputs are mesh ID, local profile ID, and an optional
one-time enrollment token from a protected source. Omitting the token reopens
the installed profile. Endpoint, username, and password flags select the
temporary legacy compatibility path only when supplied as one complete set.
