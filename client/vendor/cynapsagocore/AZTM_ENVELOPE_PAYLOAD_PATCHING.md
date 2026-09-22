# AZTM Envelope, Payload, and HTTP Hook Contract

Status: current

## Envelope

Core creates and validates the private envelope. Its application identity is a
Core-issued message ID, conversation ID, authenticated sender and recipient,
mesh scope, message kind (`rpc`, `msg`, or response), expiry, and one canonical
payload. Applications and SDK hooks cannot inject envelope identifiers or
private carrier metadata.

Inbound envelopes pass integrity, identity, membership, authorization, size,
expiry, and message-ID deduplication checks before SDK delivery. The dedupe key
is authenticated mesh, authenticated sender, and message ID. Rank 1 and Rank 2
copies of one logical message therefore converge on one application delivery.

## Canonical application payloads

Current application payloads are fully materialized and bounded:

- Every call becomes an exact `CynapsaRequest` containing method, path, query,
  duplicate-preserving header pairs, and owned body bytes.
- Native shorthand accepts exact `bytes`, UTF-8 `str`, strict JSON-compatible
  values, or an explicit canonical request. JSON values are `null`, booleans,
  finite numbers, strings, lists, and string-keyed objects.
- Every RPC application result becomes `CynapsaResponse`, containing status,
  reason, duplicate-preserving header pairs, owned body bytes, and optional safe
  application-error metadata.

Application-error metadata is an explicit optional response sidecar with a
bounded code, safe detail, and JSON-object details. It is not encoded into an
HTTP header. Every application header name and duplicate remains available for
ordinary use and is preserved byte-for-byte as UTF-8 text.

The SDK wire retains the closed `http_request` and `http_response` variant
names, but those are private encoding details. Automatic advanced large-payload
streaming and adaptive size policy are not implemented.

## HTTP Bridge mapping

Each mapping key is only `scheme://host[:port]`. Its value names an agent and
chooses `rpc` or `msg` for the SDK hook. Matching is by normalized origin.
Fragments are rejected; the complete URL path and query are copied without
being folded into the origin map.

For example:

```text
https://agent.example/work/items?state=open
origin    -> https://agent.example
agent     -> the mapped recipient
path      -> /work/items
query     -> state=open
```

Supported hooks return the calling library's normal response object:
`requests.Response`, `httpx.Response`, a `urllib.request`-compatible response,
`urllib3.HTTPResponse`, or `aiohttp.ClientResponse`. Native `Session.request`
returns `CynapsaResponse`. ASGI integration may use an explicit app or the
single in-process app observed by `cynapsa run` through the supported runner.

An `rpc` hook sends a canonical request and converts the canonical response to
the originating library's response class. A `msg` hook waits for successful
Core acceptance and only then returns the synthetic `200 OK` response with no
headers and an empty body. Rejection, timeout, or cancellation never returns
that synthetic success.

HTTP client timeouts are translated to the client's native timeout exception.
Core still owns RPC expiry; the SDK's internal completion wait includes the
fixed 5-second safety window.
