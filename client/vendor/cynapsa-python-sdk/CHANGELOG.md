# Changelog

All notable SDK changes are recorded here.

## Unreleased - feature/cli

- Added public transport-neutral `CynapsaRequest`, `CynapsaResponse`, and
  `CynapsaApplicationError` models with exact bytes, ordered
  duplicate-preserving headers, explicit decode helpers, and opt-in
  `raise_for_error()` behavior.
- Carried optional application-error metadata in a dedicated canonical field
  so no application header name is reserved or consumed.
- Unified native and HTTP/ASGI normalization around the canonical models.
- Removed handler registration modes and explicit request replies: one
  `session.on(path)` handler serves RPC and msg; return values drive RPC,
  including `None` as JSON null, while msg returns are discarded.
- Added FastAPI-style `RPCException` for intentional safe application errors;
  unexpected exceptions now return sanitized `500 handler_error` responses
  while retaining details only in local logging/diagnostics.

- Defined the per-origin HTTP Bridge mapping contract with exact
  `{recipient, mode}` values, local mode selection, and separate canonical path
  and query fields.
- Made Core the sole owner of RPC expiration while the SDK waits an additional
  five seconds for terminal completion and raises a distinct safety timeout if
  Core never completes.
- Narrowed public capability and event exposure to the active SDK contract
  while retaining temporary low-level compatibility with the pinned Core ABI.
- Documented stable message identifiers, ID-only duplicate suppression, actual
  lifecycle handles, closed messaging schemas, and the fixed mapped-msg
  synthetic response.
- Added selective `urllib.request.urlopen` interception for URL strings and
  `Request` objects, buffered bytes-like bodies, stdlib-compatible responses
  and timeout errors, exact unmapped delegation, and nested-safe restoration.
- Added selective aiohttp `ClientSession` interception with native buffered
  `ClientResponse` results, await/async-context-manager support, buffered and
  JSON request bodies, timeout projection, raise-for-status behavior, exact
  unmapped delegation, and nested-safe restoration.
- Added `cynapsa run` for same-process Python script, `python -m`, and
  importable console-script wrapping, secure password sources, deterministic
  shorthand parsing, argv/exit preservation, and CLI-only FastAPI lifespan
  discovery. Repeated `--allow AGENT PATH` rules provide explicit,
  fail-closed application-policy bootstrap before target execution.
- Added the real six-agent Core/ejabberd deep harness with isolated LANs,
  TURN-forced and XMPP-only clients, and environment-only long-outage recovery
  campaigns.
- Kept expanded payload support deferred.

## 0.1.0 - 2026-08-26

- Added synchronous and asynchronous native Session lifecycle, messaging, RPC,
  inbound routing, replies, events, and normalized errors.
- Added native-session support for explicit canonical `HTTPRequestPayload`
  send/request payloads and explicit `HTTPResponsePayload` replies for inbound
  HTTP-request RPCs, with fail-closed response variant matching and
  dependency-free native imports.
- Added strict canonical HTTP `Content-Type` handling for native request and
  response accessors; duplicate, empty, and malformed header values fail
  closed instead of being guessed.
- Added selective Requests, HTTPX, and urllib3 HTTP Bridge adapters plus
  explicit ASGI routing.
- Added Python 3.10-3.14 metadata, typed package marker, optional extras,
  reproducible wheel/sdist checks, clean-install smoke, provenance drift
  verification, CI, examples, and lifecycle stress coverage.
- Kept payload handles unavailable because the current native write/finish
  contract requires a private core snapshot representation. Inline oversized
  payloads continue to fail with `payload_too_large`.
- Added release-readiness documentation and pinned native-core verification.
