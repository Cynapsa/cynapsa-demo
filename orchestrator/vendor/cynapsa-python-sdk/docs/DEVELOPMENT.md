# Development and release architecture

## Layers

The public package has three application-facing areas:

- `cynapsa.session` and `cynapsa.messaging`: sync/async native Sessions,
  payload models, sends, requests, replies, status, and capabilities.
- `cynapsa._inbound` and `cynapsa.events`: bounded event consumption,
  delivery acceptance, handler workers, and immutable event models.
- `cynapsa._http_bridge` and `cynapsa.http`: selective HTTP client hooks and
  explicit ASGI routing.
- `cynapsa._cli`: same-process command wrapping, secure enrollment-token and
  legacy-password source parsing, and CLI-only FastAPI lifespan discovery.

`cynapsa.native` is the integration layer. It owns library discovery, exact
native signatures, returned-buffer ownership, command validation, completion
routing, callbacks, and shutdown. Application modules do not inspect runtime
internals or transport state.

## Frozen SDK surface contract

[`SDK_SURFACE_CONTRACT.md`](SDK_SURFACE_CONTRACT.md) is the normative
Cynapsa SDK Surface Contract 1.0. Preserve these architectural boundaries in
all implementation and review work:

- sender SDKs, the Go core, and envelopes never identify or negotiate a remote
  native or monkeypatched surface;
- local native entry points expose only Cynapsa-owned models and never require
  optional HTTP imports;
- each local HTTP hook reconstructs its intercepted library's actual response
  class from a compatible canonical HTTP response, independent of the remote
  producer;
- envelopes contain only closed canonical payload variants and `msg`/`rpc`
  semantics, with ordered duplicate-preserving HTTP headers and exact body
  bytes; and
- encoding is a documented API/application concern. Exact bytes, UTF-8 text,
  strict JSON values on the JSON path, and existing canonical payloads are
  accepted; arbitrary Python objects fail before core admission.

The mirrored `AZTM_*.md` files are awaiting a coordinated Core-owned rewrite.
Until that synchronization lands, they do not override the Python API under
`src/cynapsa` or this surface contract. The vendored native header and
conformance vectors remain authoritative for the native ABI itself.

The prohibition is about the remote target surface. It does not conflict with
the core enforcing the locally selected native or HTTP Bridge authentication
personality, or with a hook knowing which local HTTP library it intercepted.
Initial enrollment uses `auth.token_connect`/`auth.token_login`; subsequent
starts use the installed-profile commands. The old username/password commands
remain a complete, mutually exclusive compatibility mode. Endpoint discovery,
credentials, renewal, resources, and persisted installation state remain
behind the Core boundary.
Native and HTTP calls normalize through `CynapsaRequest`; RPC results normalize
through `CynapsaResponse`. The originating HTTP hook reconstructs its own
library response type. Framework-specific objects never cross the ABI.
Optional application-error metadata uses its own canonical response field;
application headers remain entirely duplicate-preserving and unreserved.
HTTP `Content-Type` semantics are derived from ordered headers and fail closed
on duplicate, empty, malformed, or control-bearing `Content-Type` fields. JSON
media types remain UTF-8 even if a sender supplies a different `charset`.

Requests, HTTPX, and urllib3 interception occurs after the library prepares
the request. Their adapters consume prepared buffered body bytes and MUST NOT
re-serialize the caller's original object. The stdlib `urllib.request.urlopen`
adapter instead preserves URL-string/`Request`, method, bytes-like data,
header, and positional/keyword timeout behavior directly; unmapped calls are
delegated with their exact original arguments. Optional imports remain lazy,
and native APIs must remain usable without Requests, HTTPX, urllib3, Starlette,
FastAPI, or aiohttp. In `msg` mode, construct the fixed local `200 OK`/no-
headers/empty-body response only after successful local core acceptance. In
`rpc` mode, reconstruct the remote canonical HTTP response through the shared
canonical response parser.

`cynapsa run` must remain same-process. Use `runpy` or importable
`console_scripts` entry points for Python targets; do not use subprocesses for
the target command because the HTTP and FastAPI monkeypatches must live in the
application process. The parser must split target selection deterministically:
canonical wrapped commands begin after a literal `--`, while `.py` shorthand
may appear before or among Cynapsa flags and uses a later `--` to delimit
target arguments. Reject native executables and detectable multi-worker,
reload, lifespan-off, Trio, or pre-fork server modes. The CLI-only FastAPI hook
patches the ASGI `FastAPI.__call__` lifespan path before user code, attaches
exactly one root app through `hook_asgi(app)` from the running loop, and
restores the patch transactionally.

CLI application-policy bootstrap is explicit and fail-closed. Parse repeated
`--allow AGENT PATH` values before login, translate only literal `*` values to
Core's empty selectors, and issue one `policy.set` containing exactly those
allow rules before running user code. Do not install policy when no rules were
provided. Policy failure must enter the ordinary bridge-cleanup path without
executing the target.

Native routing is exact except for the literal `"*"` wildcard. Keep
`session.on(path, handler=None)` synchronized across sync and async Sessions.
The sender selects RPC or msg. Handler return is the only RPC response path;
`None` becomes JSON `null`. Msg returns are discarded. `RPCException` is the
safe intentional application-error path; unexpected exceptions are logged
locally and produce a sanitized `500 handler_error` only for RPC.

HTTP Bridge configuration requires exactly `{recipient, mode}` per normalized
origin. Core receives only the origin-to-recipient mapping; Python keeps mode
locally and preserves URL path and query as separate canonical fields. Core
owns RPC expiry, including the configured/default 30-second value selected by
zero. Python waits the effective duration plus a five-second terminal-
completion safety window.

## Contract provenance

The vendored native header and conformance vectors are immutable inputs. Their
source commit and hashes live in `src/cynapsa/_core_source.json`.

```console
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 \
  scripts/verify_core_provenance.py

/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 \
  scripts/verify_core_provenance.py \
  --core-checkout /path/to/cynapsagocore
```

Passing `--commit WORKTREE` compares checkout files instead. Drift is blocking:
update the pin and all vendored inputs together only after reviewing the public
contract. Never regenerate an SDK-local substitute for a missing core schema.

## Payload-handle boundary

High-level payload handles remain disabled. The low-level native signatures are
bound so ABI compatibility checks remain complete, but no dedicated Python
method, type, or fallback invokes them. Do not add a Python encoder for the
core's internal payload snapshot.

Enabling handles requires an upstream public, versioned, variant-aware builder
and materialization contract represented consistently in public Go models, the
native header, and conformance vectors. Until then, inline limits and
fail-closed behavior are authoritative. Native Session send/request reports
oversize input as `payload_too_large`; direct response-model construction,
handler-return normalization, and some HTTP prepared-model validation can
remain local `ValueError`.

The preferred future API opens an opaque builder from a small canonical
variant-and-metadata descriptor, accepts exact body bytes in bounded chunks,
and lets the core construct its private snapshot. A matching public
materializer must expose projected variant/metadata and bounded exact-body
reads; SDKs must never decode private snapshot bytes. Inbound reads copy into
SDK-owned memory. This is a non-normative future direction, not a current API.
Do not expose a direct view into Go-managed memory: V1 handles are
opaque/non-pointer and `BufferRead` is copy-based. A future writable-buffer
optimization would require a separate explicit C-owned buffer ABI with
complete allocation, commit/cancel, thread, and lifetime rules.

## Test commands

Use the requested release interpreter:

```console
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 -m pytest -q
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 -m pytest -q -m stress
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 -m pytest -q \
  tests/test_installed_artifacts.py
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 -m compileall -q \
  src tests scripts examples
```

The AST regression parses every Python file with Python 3.10 grammar. CI runs
the unit suite on Python 3.10-3.14 and runs HTTP adapter, package install, and
pinned native-core jobs separately.

The current peer-authority harness runs real Python applications, Core
processes, the dedicated ejabberd image, STUN, TURN, and isolated VLANs:

```console
./e2e/simple/run-peer-authority.sh
```

The historical `run.sh` deep-fault campaign is not a current release gate.
Both runners require the dedicated ejabberd checkout; the SDK does not own a
server image or server configuration.

For a real native run, set `CYNAPSA_CORE_SMOKE_LIBRARY` to the release-style
shared library. The guarded tests exercise create/start/init, polling,
callbacks, shutdown/destroy, and repeated providerless authentication failure.
Providerless failure is expected and must remain normalized and secret-free.

## Build and inspect

```console
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 -m build
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 -m twine check dist/*
```

The wheel contains only the Python package, typing marker, provenance manifest,
vendored header, and conformance inputs. The sdist additionally contains tests,
scripts, docs, and examples. `tests/test_installed_artifacts.py` builds both,
inspects them, installs the wheel into a clean temporary virtual environment,
imports without optional HTTP packages, verifies assets, and exercises an
actionable native-library discovery failure.

## Release discipline

- Do not add or infer a license field without an approved license artifact.
- Never log credentials, raw provider errors, exception traces, payload bodies,
  or native/private metadata.
- The CLI must never accept a plaintext password argument; accept only an
  environment variable name, file, stdin, or hidden prompt as the selected
  password source.
- Optional HTTP packages must stay lazily imported.
- Patches must be transactional and restored only when Cynapsa still owns the
  patched attribute.
- Teardown must join callback, consumer, handler, ASGI, and drain workers before
  releasing their owners.
- Do not publish, tag, commit, or push as part of local release verification.
- Keep expanded payload support explicitly deferred until its contract and
  tests land.
