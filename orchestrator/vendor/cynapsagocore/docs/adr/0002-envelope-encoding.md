# ADR 0002: Internal Envelope Encoding

- Status: Accepted
- Scope: Private peer-to-peer envelope representation
- Current wire version: V2

## Context

Internal messages require deterministic encoding, strict size limits, stable
identifiers, bounded duplicate detection, integrity validation, and fail-closed
version handling. This representation is private and never crosses the SDK or
native ABI boundary.

V2 is a clean private wire cut. Earlier wire versions are rejected and
reserved keys are not interpreted.

## Current V2 decision

V2 uses RFC 8949 Core Deterministic CBOR with a definite-length unsigned-key
map:

| Key | V2 field | CBOR type | Required |
| ---: | --- | --- | --- |
| 0 | version, exactly `2` | unsigned integer | yes |
| 1 | message ID | text | yes |
| 2 | conversation ID | text | yes |
| 3 | sender identity | text | yes |
| 4 | recipient identity | text | yes |
| 5 | mesh ID | text | yes |
| 6 | reserved | none | forbidden |
| 7 | interaction mode | unsigned integer | yes |
| 8 | correlation ID | text | request/response only |
| 9 | reply-to message ID | text | response only |
| 10 | creation time | signed epoch milliseconds | yes |
| 11 | payload descriptor | map below | yes |
| 12 | reserved | none | forbidden |
| 13 | absolute expiry time | signed epoch milliseconds | request only |
| 14 | calibrated-clock uncertainty | unsigned integer microseconds | yes |
| 15 | reserved | none | forbidden |

Mode values are `0=msg`, `1=rpc`, and `2=rpc_response`. Message mode forbids
keys 8, 9, and 13. Request mode requires keys 8 and 13 and forbids key 9.
Response mode requires keys 8 and 9 and forbids key 13.

The payload descriptor remains:

| Key | Field | Rule |
| ---: | --- | --- |
| 0 | kind | `0=inline`, `1=object_reference`, `2=transfer_reference` |
| 1 | profile | required text |
| 2 | inline canonical bytes | inline only |
| 3 | private reference | reference kinds only |
| 4 | canonical size | required unsigned integer |
| 5 | canonical SHA-256 digest | required 32-byte string |
| 6 | private encryption reference | optional for reference kinds |

## Canonical and semantic validation

The codec rejects indefinite values, tags, floats, arrays, duplicate or unknown
keys, invalid UTF-8, non-minimal integers, trailing data, malformed values,
unsupported versions, and any byte representation that differs from its
deterministic re-encoding. Inputs are bounded to 1 MiB before decoding.

Typed identifiers retain their exact canonical forms: `msg_` and `cor_` plus
22 unpadded base64url characters for 128 bits, and `conv_` plus 43 characters
for a SHA-256 conversation identity. Creation and request-expiry times use UTC
millisecond precision. Request expiry must be later than creation. Clock
uncertainty is a nonzero whole-microsecond value no greater than one second.

RPC is matched by correlation and reply identity. Stable message identity
enables bounded deduplication across retries and carrier changes. Logical and
envelope digests protect integrity and carrier convergence; they are not
duplicate identity and are not compared when an already-seen scoped message ID
is replayed.

## Integrity and carrier authentication

`CanonicalIntegrityBytes` is the complete deterministic V2 envelope encoding,
and `EnvelopeIntegrityDigest` is SHA-256 over those bytes. It covers version,
message and conversation identity, sender, recipient, mesh, mode, correlation,
reply identity, creation, request expiry, clock uncertainty, and the complete
payload descriptor.

`LogicalMessageDigest` is a separate deterministic V2 form containing the
stable envelope fields plus payload profile, canonical size, and canonical
digest. It excludes carrier provenance, descriptor kind, inline bytes, private
references, and encryption references so fallback attempts converge on one
logical message.

Carrier authentication is out of band. Rank 2 binds the authenticated XMPP
sender and exact session resource while mesh authority is verified separately;
Rank 1 binds the authenticated peer session.
Current membership is checked at admission and again at the final publication
or success boundary. No per-envelope password-derived proof exists.

## Compatibility and evidence

V2 accepts only version 2. Keys 6, 12, and 15 are rejected, as are all other
unknown fields. There is no downgrade negotiation or mixed V1/V2
interpretation. Golden vectors, truncation tests, strict-field tests, semantic
maxima, randomized round trips, and fuzz coverage live with
`internal/protocol` and must change atomically with the codec.
