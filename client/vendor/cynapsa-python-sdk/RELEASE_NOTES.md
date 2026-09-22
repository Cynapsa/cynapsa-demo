# Cynapsa Python SDK development release notes

Date: 2026-09-07
Status: development branch; not a published release

The current SDK supports synchronous and asynchronous native Sessions,
native sends and RPC requests, universal request/response models, exact
inbound routes, selective HTTP Bridge clients, and explicit ASGI integration.
One `session.on(path)` handler serves RPC and msg without a mode argument or
explicit reply API. RPC return values become canonical responses, `None`
becomes JSON null, and msg returns are discarded. `RPCException` carries safe
intentional application errors; unexpected failures return sanitized
`handler_error` responses while raw details stay local. It also
includes `cynapsa run`, a same-process CLI wrapper for Python scripts,
`python -m` modules, and importable Python console scripts. The `.py`
shorthand accepts Cynapsa flags after the script target and uses a later
literal `--` to delimit target arguments. Repeated `--allow AGENT PATH`
options install only explicitly requested application-policy allow rules;
omitting them preserves Core's fail-closed policy.

Application-error metadata is a distinct bounded response field rather than a
reserved HTTP header. Exact ordered application headers therefore survive the
native, HTTP, ASGI, ABI, and mesh boundaries unchanged.

The HTTP Bridge requires `{recipient, mode}` for every mapped origin. Mapping
mode stays local to Python, while Core receives only origin-to-recipient data.
Requests, HTTPX sync/async, buffered urllib3, and buffered
`urllib.request.urlopen` and aiohttp calls reconstruct their own compatible response
interfaces. `urlopen` supports URL strings and `Request` objects and delegates
unmapped calls unchanged. Its mapped buffered response includes urllib's
file-style and metadata interfaces, returns canonical HTTP error statuses as
responses, and fails locally for forbidden mapped URLs or streaming bodies.
One-way mapped calls return the fixed local `200 OK` result only after Core
accepts the send.

aiohttp supports normal `ClientSession.request/get/post` await and `async with`
forms for mapped origins. It returns a real buffered `ClientResponse`, honors
query parameters, duplicate headers, buffered/text/JSON bodies, timeout
selection, and raise-for-status policy, and delegates unmapped requests to the
original aiohttp implementation. Streaming and form-style request bodies are
explicitly outside this release's mapped surface.

Core owns RPC expiration. Zero or omission selects the configured timeout,
which defaults to 30 seconds; Python waits that effective duration plus a
five-second terminal-completion safety window.

The repository includes a real six-agent Core/ejabberd harness with isolated
LANs, STUN/TURN, a TURN-forced native client, an XMPP-only monkey client, and
long environment-only outages that require fresh authentication, a new
membership snapshot, and completion of the original workload. Separate short
outages interrupt one server and one client during in-flight RPCs and require
successful XEP-0198 resumption after the required pre-resume SCRAM reconnect,
with no new bound session, authority discovery, or snapshot. Passive server counters prove the exact
resume hook and ready resume-authority result, while the unchanged validator
still requires the original 80 requests and 80 dispatches exactly once.

Each run records an exact digest manifest for the pinned Core source snapshot.
Remaining work before a release claim includes publishing the paired Core
commit before SDK CI, SDK/container provenance, provider-backed authentication,
platform CI, and legal approval. Expanded payload support remains deferred;
current high-level behavior is unchanged. The CLI V1 boundary excludes arbitrary
native executables, reload execution, multi-worker load balancing, lifespan-off
ASGI execution, Trio servers, and pre-fork server models.
