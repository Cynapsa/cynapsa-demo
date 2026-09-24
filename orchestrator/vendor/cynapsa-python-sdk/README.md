# Cynapsa Python SDK

The Cynapsa Python SDK provides synchronous and asynchronous native messaging,
RPC-style requests, inbound application routes, and an opt-in HTTP Bridge. It
supports Python 3.10 through 3.14.

This release requires a compatible Cynapsa native core shared library. It does
not bundle credentials, select an authentication provider, or simulate a
successful production login.

## Install

Install the base package for native messaging without optional HTTP libraries:

```console
python -m pip install cynapsa
```

Install client hooks, ASGI support, or both:

```console
python -m pip install "cynapsa[client]"
python -m pip install "cynapsa[server]"
python -m pip install "cynapsa[client,server]"
```

Contributors can use `cynapsa[test]` or `cynapsa[dev]`. The earlier `http`,
`asgi`, and `http-bridge` extra names remain aliases for compatibility.
Importing `cynapsa` does not import Requests, HTTPX, urllib3, Starlette, FastAPI,
or aiohttp.

## Native library discovery

The shared library is resolved in this order:

1. An explicit low-level `library_path`.
2. `CYNAPSA_CORE_LIBRARY`.
3. A platform library next to the installed `cynapsa` package.
4. The operating system library lookup.

Expected filenames are `libcynapsacore.dylib` on macOS,
`libcynapsacore.so` on Linux, and `cynapsacore.dll` or
`libcynapsacore.dll` on Windows. Discovery and ABI compatibility failures raise
`cynapsa.NativeError` with `code` values such as
`native_library_not_found`, `native_library_load_failed`, or
`abi_version_mismatch`.

For local development:

```console
export CYNAPSA_CORE_LIBRARY=/absolute/path/to/libcynapsacore.dylib
```

## Native Sessions

`connect()` authenticates and returns a synchronous `AztmSession`.
`connect_async()` performs the same setup without blocking the event loop and
returns an `AsyncAztmSession`.

Initial enrollment supplies a reusable `cpsa_` bootstrap token and stores the
result in a local Core profile. Normal restarts omit the token:

```python
with cynapsa.connect(mesh_id="mesh-one", profile_id="default") as session:
    print(session.installation_id, session.auth_info.credential_expires_at)
```

The immutable `AztmAuthInfo` exposes safe installation metadata only. Endpoint
discovery, installation credentials, renewal tokens, and the bound transport
resource remain private to Core. The complete legacy
`mesh_endpoint`/`username`/`password` set remains available temporarily as a
mutually exclusive compatibility mode.

```python
import os
import cynapsa

with cynapsa.connect(
    mesh_id="mesh-one",
    profile_id="default",
    # Supply this bootstrap token on first enrollment. Later starts omit it
    # and reopen the Core-owned installed profile.
    enrollment_token=os.environ["CYNAPSA_ENROLLMENT_TOKEN"],
) as session:
    accepted = session.send(
        "worker@example.test", {"ready": True}, path="/events/ready"
    )
    response = session.request(
        "worker@example.test",
        {"a": 2, "b": 3},
        path="/math/add",
        ttl_ms=5_000,
    )
    print(accepted.message_id, response.json())
```

`send()` completes when the local core accepts or rejects responsibility. A
successful `AztmSendResult` is not proof of remote delivery or application
processing. `request()` waits for the remote application response. Its
`ttl_ms` is the RPC lifetime; zero or omission selects the configured RPC
timeout, which defaults to 30 seconds.
The SDK forwards that value unchanged and the Go core alone expires the RPC
and rejects late responses. The SDK waits five additional seconds only for
the Core's terminal completion. A normal expiry raises `NativeError` with
`code == "rpc_timeout"`; if Core publishes no terminal completion during that
safety window, the SDK directly cancels the command and raises the distinct
`SdkSafetyTimeout`. The extra five seconds never extend the envelope TTL.

Every call is normalized to dependency-free `CynapsaRequest`. It contains an
HTTP-style method, path, query, ordered duplicate-preserving headers, and exact
body bytes. Native shorthand accepts exact bytes, strings, and strict
JSON-compatible values; arbitrary Python objects are not serialized.

On success, native `session.request()` returns `CynapsaResponse`, containing
status, reason, ordered duplicate-preserving headers, and exact body bytes.
A remote 4xx/5xx response raises `RemoteNativeError` (a `NativeError`), with
the canonical application error code/detail when available. Its `.response`
retains the complete, dependency-free `CynapsaResponse`—not a library HTTP
object. An intermediate native handler may explicitly `return error.response`
to forward the status, headers, body, and safe error metadata; unhandled errors
still become sanitized `500 handler_error` replies. Monkeypatched HTTP callers
receive the corresponding library-native HTTP outcome instead: `requests` and `httpx`
return 404 responses, while `urllib.request.urlopen()` raises `HTTPError`.
Forwarding is explicit and crosses a trust boundary: filter sensitive headers
(such as `set-cookie` or `authorization`), hop-by-hop headers, and private body
details before returning an upstream response to a different caller. Use a new
`CynapsaResponse` or raise `RPCException` when a sanitized error is appropriate.
The wire response is the same regardless of the caller. Error metadata is a
separate canonical field and never reserves an application header name.

The asynchronous surface has the same semantics:

```python
session = await cynapsa.connect_async(...)
async with session:
    accepted = await session.send("worker@example.test", b"ready", path="/ready")
    response = await session.request(
        "worker@example.test", "ping", path="/ping", ttl_ms=5_000
    )
```

Core assigns every finalized operation a stable `message_id`, and transport
retries preserve it. Repeated delivery of the same identifier is suppressed by
identifier alone; applications do not need to compare payload content to
recognize a retry.

The messaging commands use closed argument and result shapes:

| Command | Arguments | Result |
| --- | --- | --- |
| `message.send` | `to`, canonical `payload` | `message_id`, `conversation_id`, `accepted` |
| `message.request` | `to`, canonical `payload`, `ttl_ms` | `message_id`, `conversation_id`, `from_agent_id`, `mesh_id`, canonical `payload` |
| `message.reply` | opaque `request_handle`, canonical `payload` | the same accepted-send result shape |

Extra or missing fields fail closed at the native boundary.

## Inbound routes and responses

`session.on(path, handler=None)` owns an exact application path. The literal
`"*"` is the only wildcard. One handler serves both RPC and one-way messages;
the sender's request determines the behavior.

```python
@session.on("/math/add")
def add(request: cynapsa.CynapsaRequest):
    values = request.json()
    return {"sum": values["a"] + values["b"]}

@session.on("/events/ready")
def ready(request: cynapsa.CynapsaRequest):
    print(request.json())
```

Handlers always receive `CynapsaRequest`. For RPC, returning a dict/list/scalar
creates JSON, `str` creates text, `bytes` creates binary, `None` creates JSON
`null`, and an explicit `CynapsaResponse` is preserved. `request.reply()` does
not exist.

Raise `RPCException(status_code, code=..., detail=...)` for an intentional safe
application error. Any other exception is logged locally and becomes a
sanitized `500 handler_error`; raw exception text never crosses the mesh. For a
msg, the handler still runs, but every return value is discarded and exceptions
remain local—no response is created or transmitted.

`delivery.accept` is submitted only after strict event decoding, selection of
either a registered route or its missing-route response, and successful
placement in the bounded handler queue, and before
the handler is invoked. It transfers local flow-control responsibility. It is
not a remote receipt, an application acknowledgement, or evidence that the
handler completed. An unmatched native RPC path is accepted and receives a
canonical `404 not_found` response; later handler registration affects only
later deliveries. Unmatched one-way messages have no remote reply. Queue-blocked
deliveries remain unaccepted until admitted.

Use `next_event()`, `next_diagnostic()`, and `next_local_diagnostic()` for the
separate immutable event streams. `session.off()` removes an exact
registration.

## HTTP Bridge

`login()` and `login_async()` select the HTTP Bridge personality and return
`HttpBridgeHandle` and `AsyncHttpBridgeHandle`, respectively—not native
Sessions. Only explicitly mapped origins enter Cynapsa; every unmapped request
follows the original client path.

```python
import os
import requests
import cynapsa

with cynapsa.login(
    mesh_id="mesh-one",
    profile_id="default",
    enrollment_token=os.environ["CYNAPSA_ENROLLMENT_TOKEN"],
    address_map={
        "https://orders.example.test": {
            "recipient": "orders@example.test",
            "mode": "rpc",
        },
        "https://audit.example.test": {
            "recipient": "audit@example.test",
            "mode": "msg",
        },
    },
):
    response = requests.post(
        "https://orders.example.test/orders", json={"sku": "A1"}
    )
```

Every address mapping requires exactly `recipient` and `mode`. The mode is
selected independently for each normalized origin, so one bridge can mix RPC
and one-way message routes. `rpc` reconstructs the remote canonical HTTP
response. `msg` waits only for successful local send acceptance and returns a
synthetic `200 OK` with no headers and an empty body. A rejection, timeout, or
cancellation never returns that synthetic response. Missing or invalid modes,
extra mapping fields, and duplicate spellings of the same normalized origin
are rejected before login.

The normalized origin selects the mapping. The remaining URL path and query
are carried separately in the canonical HTTP request. Only
`virtual_origin -> recipient` is configured in Core; `mode` remains local to
the Python hook and selects `message.request` or `message.send`. There is no
login-level mode.

Native versus monkeypatched operation is never a remote capability or envelope
flag. The sender SDK, its sender-side core, and the envelope do not know or
negotiate which remote application surface will consume the payload. The
receiving SDK adapts only at its local boundary: `session.on()` exposes
Cynapsa-owned models, while an HTTP hook returns the actual response type of
the locally intercepted library for a compatible canonical HTTP response,
regardless of the remote producer's SDK or surface. The receiving core may
still enforce its locally selected native or HTTP Bridge personality. See the
frozen
[`SDK Surface Contract`](docs/SDK_SURFACE_CONTRACT.md).

| Surface | Support | Result for a mapped request |
| --- | --- | --- |
| `requests.Session.send` | Supported | `requests.Response` |
| `httpx.Client.send` | Supported | `httpx.Response` |
| `httpx.AsyncClient.send` | Supported | awaited `httpx.Response` |
| urllib3 pools / `PoolManager` | Supported for buffered bodies | `urllib3.HTTPResponse` |
| `urllib.request.urlopen` | Supported for URL strings and `Request` objects with buffered bodies | urllib-compatible buffered response |
| `aiohttp.ClientSession` | Supported for buffered bodies and `json=` | awaited `aiohttp.ClientResponse` |

For mapped RPC calls, Requests, HTTPX, urllib3, and explicit numeric
`urllib.request.urlopen` timeouts set the Core RPC TTL. A Core `rpc_timeout` is
projected as the corresponding library's timeout exception. The same is true
if Core fails to publish a terminal completion before the SDK's five-second
safety guard: the command is cancelled and the hook raises the client's native
timeout type, without exposing Cynapsa internals. `urlopen` uses
`urllib.error.URLError` with a native timeout reason and the configured/default
Core timeout when its timeout argument is omitted. Native SDK requests retain
the distinct `SdkSafetyTimeout` for this Core-completion failure.

Mapped bodies must be buffered and repeatable. Duplicate response headers are
preserved by the canonical model; Requests may comma-combine them in its usual
mapping, while `response.raw.headers.getlist(name)` retains exact duplicates.
`urllib.request` accepts its ordinary URL-string and `Request` forms, respects
`Request.get_method()`, urllib's normal/unredirected header precedence, its
forced `Connection: close`, and bytes-like `data`. It returns the standard
buffered file-style response interface (`read`, `readinto`, line iteration,
metadata accessors, and context-managed close); `fileno()` is unsupported for
the in-memory body. Canonical 304 and 4xx/5xx results raise
`urllib.error.HTTPError`, whose body and headers remain readable. Standard
redirects are followed only within the same mapped virtual origin (up to ten
hops); cross-origin or unmapped redirects raise `HTTPError` without opening an
ordinary network connection. Streaming, iterable, text, and file-like
mapped bodies fail locally. Userinfo and fragments are invalid on mapped URLs
and fail locally instead of escaping to the network. Unmapped calls delegate
unchanged to the original `urlopen`.

aiohttp interception preserves its normal `request()`/`get()`/`post()` call
shapes, including direct `await` and `async with`. Mapped responses are real
`aiohttp.ClientResponse` instances with buffered `read()`, `text()`, and
`json()` behavior, duplicate-aware `CIMultiDict` headers, request metadata,
and normal `raise_for_status` handling. Per-request timeouts and the session
default become the Core RPC TTL; both Core RPC expiration and the SDK safety
window surface as `asyncio.TimeoutError` without Cynapsa internals.

Mapped aiohttp requests accept bytes, bytearray, memoryview, text, or `json=`
bodies. Forms, multipart bodies, file-like objects, iterables, compression,
chunked uploads, and `100-continue` are rejected locally before submission.
URL query and `params=` are preserved, as are duplicate request headers.
Session/request cookies, HTTP authentication, and skipped automatic headers are
applied during aiohttp request preparation. Proxy options are rejected for
mapped virtual origins because Cynapsa opens no HTTP proxy connection.
Unmapped calls delegate to aiohttp's exact original implementation.

`urlopen` argument binding follows the running Python version: the deprecated
`cafile`, `capath`, and `cadefault` keywords are accepted on versions that
still expose them and rejected after their stdlib removal. TLS context and CA
arguments have no transport effect for a mapped virtual origin because no TLS
socket is opened; `context` still selects urllib's default request headers in
place of a configured global opener. Canonical headers are name/value pairs,
and canonical bodies are exact bytes. Optional HTTP imports remain lazy.

Pass `asgi_app=app` to `login`/`login_async` for explicit inbound ASGI routing.
The app is invoked only after queue placement and local delivery acceptance.
RPC deliveries receive its canonical response; one-way deliveries do not.
Application failures produce a sanitized 500 response. `hook_asgi(app)` is
available only when exactly one live bridge owns the process.

## CLI

Installing the package exposes the `cynapsa` console script. `cynapsa run`
authenticates with the synchronous HTTP Bridge, installs the same in-process
client hooks, and runs a Python target in that same process:

```console
cynapsa run \
  --mesh-id mesh-one \
  --profile-id default \
  --token-env CYNAPSA_ENROLLMENT_TOKEN \
  --map https://orders.example.test orders@example.test rpc \
  --allow orders@example.test /orders \
  -- python app.py
```

Supported target forms are `-- python script.py`, `-- python -m module`,
installed Python console scripts such as `uvicorn app:create_app --factory`,
and the clean shorthand `cynapsa run script.py ...`. In shorthand form,
Cynapsa options may appear after the `.py` target; a later literal `--`
separates any arguments intended for the script:

```console
cynapsa run myagent.py \
  --mesh-id mesh \
  --profile-id default \
  -- --target-option value
```

The target is not launched as a subprocess, so Cynapsa monkeypatches and the
bridge handle stay alive in the application process. Target `sys.argv`,
`SystemExit`, and exit status are preserved as closely as Python allows.
Arbitrary native executables are rejected because in-process hooks cannot
cross a process boundary.

On initial enrollment, choose exactly one protected token source:
`--token-env NAME`, `--token-file PATH`, `--token-stdin`, or
`--token-prompt`. On later starts, omit every token flag; `cynapsa run` reopens
`--profile-id` (default `default`) from Core-owned installation state. Tokens
and passwords are never accepted as plaintext CLI arguments.

Legacy compatibility is selected only when any of `--mesh-endpoint`,
`--username`, or a `--password-*` source is supplied. It requires all three,
with exactly one of `--password-env NAME`, `--password-file PATH`,
`--password-stdin`, or `--password-prompt`, and cannot be combined with a token
source or a non-default profile. The canonical wrapped-command form uses the first `--` to end Cynapsa
option parsing and start the wrapped command; shorthand uses the `.py` target
as the command and a later `--` for script arguments. The other login
configuration flags mirror `login()`: `--mesh-id`, `--profile-id`, repeated
`--map ORIGIN RECIPIENT rpc|msg`, `--command-timeout-ms`, `--rpc-timeout-ms`,
`--queue-limit`, and `--payload-limit`. Repeated `--allow AGENT PATH` options
replace Core's fail-closed application policy with exactly those allow rules
before application code starts. Use the literal `*` for either wildcard; the
CLI translates it to Core's empty selector. Omitting `--allow` leaves the
fail-closed policy unchanged, and a failed policy installation prevents the
target from running.

When the target imports FastAPI, `cynapsa run` installs a CLI-only FastAPI
lifespan hook before user code. On the actual root FastAPI ASGI lifespan call,
the hook invokes `cynapsa.hook_asgi(app)` on that exact app from the running
event loop, including apps created inside factories or local variables. It
attaches once, errors if a second independent FastAPI root appears, and
restores its patch when the target exits. Mounted apps remain routed by the
root FastAPI app. Known reload, multi-worker, lifespan-off, Trio, and pre-fork
server configurations are rejected when detectable from the command line; V1
does not attempt load balancing or cross-worker hook propagation.

This makes a regular FastAPI or Requests program runnable without importing
Cynapsa. For example, `cynapsa run [Cynapsa options] -- uvicorn package:app`
discovers the FastAPI app at lifespan startup, while mapped Requests calls are
intercepted in the same process. Policy remains explicit in both cases.

## Lifecycle

Prefer context managers. `close()` is idempotent and coordinates active work,
logout, callback/worker teardown, native shutdown, and destruction. Concurrent
close calls join the same teardown. After closing begins, new operations fail
with normalized `shutdown_in_progress` behavior. If bounded native teardown
cannot safely finish, ownership is retained for a later close rather than
unloading resources still reachable from callbacks.

## Payload size and deferred handles

This release supports inline application payloads only. Native and HTTP models
enforce the public 256 KiB inline boundary; high-level Session and Bridge paths
also enforce any lower configured limit. They fail closed above the applicable
limit and never submit payload-handle commands as a fallback or serialize a
private core snapshot. Native Session send/request reports normalized
`payload_too_large`; direct model and some HTTP prepared-request validation
remains ordinary local `ValueError`.

Payload handles are intentionally unavailable. The public boundary says a
handle represents an implementation-neutral, immutable snapshot of all
application metadata and body bytes. At the pinned core commit, however, the
raw bytes accepted by `payload_write` are validated by a private core
serializer when `payload_finish` runs. The native ABI provides no public
variant/metadata builder that lets Python stream application bytes without
knowing that private representation.

Upstream must correct the contract before this SDK can expose handles: define a
versioned public, variant-aware builder/materialization API in the public Go
models, native header, and conformance vectors, with the core alone creating
its private snapshot. Binding the existing native signatures does not make
their private byte contract a supported Python API.

The non-normative recommended future shape sends a small
variant-and-metadata descriptor, then supplies the already-encoded body as
bounded byte chunks to an opaque builder handle. The core constructs its
private snapshot. A matching public materializer must expose projected
metadata and bounded exact-body reads without requiring SDKs to decode the
private snapshot. Current V1 does not provide direct views into Go-managed
memory; any future writable-buffer ABI would need explicit C ownership and
lifetime rules. The frozen current behavior and future design note are in the
[`SDK Surface Contract`](docs/SDK_SURFACE_CONTRACT.md#6-large-payloads).

## Deferred scope

Expanded payload-handle support remains separate future work. The current APIs
and limits do not imply that feature.

## Errors and security

Catch `cynapsa.NativeError` for normalized SDK/core failures. Its stable public
fields are `status`, `code`, `message`, and immutable `details`. Ordinary Python
validation errors remain `TypeError` or `ValueError`; local wait expiration is
`TimeoutError`; async task cancellation remains `asyncio.CancelledError`.

Enrollment tokens, passwords, and provider tokens must come from a secret manager or environment,
not source code, command-line arguments, URLs, logs, diagnostics, exceptions,
or address maps. The SDK clears temporary credential containers after setup,
does not retain bootstrap tokens or passwords on Session/Bridge objects, and projects only
allowlisted normalized failure details. Do not place secrets in payload paths,
recipient identifiers, HTTP origins, or headers that application code may log.

No release claim is made for production authentication without exercising the
actual configured providers. A providerless native smoke may correctly fail
with a normalized authentication or connectivity error.

## Examples

Runnable examples are in [`examples`](examples/README.md): synchronous native,
asynchronous native, Requests, HTTPX, and explicit ASGI. The CLI form can wrap
those Python scripts with `cynapsa run`.

## Build and integration provenance

This section describes binding integration, not application semantics.

The complete low-level V1 native signatures remain declared for compatibility,
including payload functions, but there is no high-level payload-handle wrapper.
The vendored header and conformance files are pinned by SHA-256 in
`src/cynapsa/_core_source.json` at Go core commit
`a21e4a23cf0c5b334e2818e2faa8b025d3d1e2b2`.

Verify the installed inputs or compare a Go checkout at the pinned commit:

```console
python scripts/verify_core_provenance.py
python scripts/verify_core_provenance.py --core-checkout /path/to/cynapsagocore
```

See [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md),
[`docs/RELEASE_READINESS.md`](docs/RELEASE_READINESS.md),
[`docs/SDK_SURFACE_CONTRACT.md`](docs/SDK_SURFACE_CONTRACT.md), and
[`CHANGELOG.md`](CHANGELOG.md).
