# Development release-readiness report

SDK branch: `feature/cli`
Core branch: `feature/cli-v2`
Committed Core provenance baseline: `a21e4a23cf0c5b334e2818e2faa8b025d3d1e2b2`

The Core worktree is clean at the pinned commit. Only the SDK currently has
uncommitted changes. The pinned Core commit exists locally but is not yet
reachable from the remote repository; this document does not claim otherwise.

## Release ordering

Push Core branch `feature/cli-v2` first so commit
`a21e4a23cf0c5b334e2818e2faa8b025d3d1e2b2` is reachable from the remote
repository. Only after that push succeeds may the SDK branch be pushed or SDK
CI be run, because SDK CI clones and checks out that exact Core commit. Do not
reverse this order.

## Decision

The SDK is suitable for continued deep integration testing within its current
implemented scope. It is not yet a release candidate: cross-repository
release ordering, provider-backed authentication, platform CI, and legal
approval remain open. The harness records the exact pinned Core source
manifest used by each run.

## Implemented SDK scope

| Area | Current behavior |
| --- | --- |
| Python | 3.10 through 3.14 package metadata and syntax regression coverage |
| Authentication | Initial enrollment token, installed-profile restart, immutable public installation metadata, and legacy credential compatibility across native and HTTP Bridge entry points |
| Native Sessions | Sync/async connect, send, request, universal request/response models, exact routes, events, status, capabilities, and coordinated close |
| Handler rules | One route serves RPC/msg; return drives RPC including `None` as JSON null; msg returns are discarded; `RPCException` is safe and intentional; unexpected failures return sanitized `handler_error` |
| RPC timing | Core owns zero/explicit TTL resolution and expiration; SDK adds only a five-second completion safety window |
| HTTP Bridge mapping | Exactly `{recipient, mode}` per normalized origin; path/query preserved separately; no login-level mode |
| HTTP clients | Requests, HTTPX sync/async, buffered urllib3, buffered `urllib.request.urlopen`, and buffered aiohttp; unmapped requests use the original network path |
| HTTP server | Explicit ASGI app through `asgi_app=` or `hook_asgi`; canonical RPC response and sanitized 500 failure |
| CLI | `cynapsa run` same-process Python script/module/console-script wrapping with secure enrollment-token or legacy-password sources, installed-profile restart, explicit `--allow` policy override, deterministic shorthand parsing, and CLI-only FastAPI lifespan discovery |
| Lifecycle | `login()` returns `HttpBridgeHandle`; `login_async()` returns `AsyncHttpBridgeHandle` |
| Payloads | Current inline behavior only; expanded payload support remains deferred |

Messaging command and result documents are closed. Unknown, missing, or extra
fields fail at the Python/native boundary. Stable Core-assigned message IDs are
preserved across retries, and duplicate application delivery is identified by
message ID alone.

## Deep integration scope

The current `e2e/simple` harness contains:

- four clients: native sync, native async, monkey sync, and monkey async;
- two servers: native `session.on` and FastAPI/ASGI;
- ten RPCs from every client to each server, for an 80-RPC matrix;
- real Core processes, ejabberd, STUN, TURN, and isolated VLAN-backed LANs;
- environment-enforced TURN-only behavior for the native async client;
- environment-enforced XMPP TCP/5222-only behavior for the monkey async client;
- long native-server and monkey-client outages during active RPCs; and
- fresh authentication and snapshot recovery after XEP-0198 resume expiry;
- short monkey-server and monkey-sync-client interruptions during active RPCs;
- successful XEP-0198 resume after exactly one required pre-resume SASL
  reconnect, without a fresh bound session, authority discovery, or snapshot;
  and
- passive evidence that the server emitted the ready resume-authority result
  before Core released the original workload.

This SDK-owned harness remains an explicit legacy-credential compatibility
test. The Management/Enrollment token path and installed-profile cold restart
are cross-repository acceptance responsibilities of `CynapsaTests`; this
branch does not relabel the direct-ejabberd harness as provider-backed
authentication.

## Required release gates still open

- Push Core `feature/cli-v2` before pushing the SDK or running SDK CI, so the
  pinned Core commit is remotely reachable when CI attempts to clone it.
- Extend the exact Core source manifest with SDK source identity and final
  container image digests before a reproducible release claim.
- Run provider-backed production authentication before making a production
  authentication claim.
- Complete platform CI and legal/license approval before publication.

Expanded payload support is explicitly deferred and is not a release claim.
The CLI release claim is limited to
same-process Python targets; it does not include native executable wrapping,
reload execution, multi-worker load balancing, lifespan-off ASGI servers, Trio
servers, or pre-fork process models.

## Verification commands

```console
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 -m pytest -q
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 -m compileall -q \
  src tests scripts examples
python scripts/verify_core_provenance.py
./e2e/simple/run.sh
```

The focused SDK handoff reports the exact full-suite result from the current
working tree. Deep E2E results should be reported through a retained harness
artifact rather than copied into this document without provenance.
