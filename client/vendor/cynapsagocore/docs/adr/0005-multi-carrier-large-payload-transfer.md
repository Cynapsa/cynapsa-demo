# ADR 0005: Multi-Carrier Payload Transfer

- Status: Accepted private Core design
- Scope: Carrier-neutral transfer of canonical payload bytes

Confidentiality terminology is governed by the
[`V1 security model`](../security/V1_SECURITY_MODEL.md): Rank 1 is protected by
DTLS; V1 XEP-0363 objects and XMPP fallback chunks are protected in transit but
visible to the serving infrastructure.

## Context

Applications submit native, HTTP-request, or HTTP-response payloads. The SDK
does not select or inspect a carrier. Core materializes a bounded canonical
payload, binds it to one logical message, and may use more than one private
carrier attempt when completion is ambiguous or a carrier fails.

## Decision

The private carrier plan is:

1. use bounded binary chunks over a healthy WebRTC data channel;
2. otherwise prepare an XEP-0363 upload and require the recipient to confirm
   reachability of the exact object authority before publication;
3. if the object path has a fallback-eligible failure, use bounded text-safe
   chunks over authenticated XMPP TLS; and
4. return one normalized transfer failure if no permitted carrier completes
   within the operation bounds.

Core may skip a carrier known to be unavailable. Policy may disable a carrier
or reduce its limits. Public SDK input cannot choose a carrier.

## Canonical payload

The serializer accepts only closed internal payload variants. It does not
accept arbitrary Go values and never serializes a local payload-handle token.
The canonical form uses deterministic CBOR with definite lengths, shortest
encodings, duplicate- and unknown-key rejection, strict UTF-8 and semantic
validation, and exact re-encoding checks.

The supported canonical variants are:

- native content type, optional path, and exact body bytes;
- HTTP method, path, query, duplicate-preserving headers, and exact body bytes;
- HTTP status, reason, duplicate-preserving headers, exact body bytes, and an
  optional bounded application-error sidecar. The sidecar is not encoded in or
  inferred from application headers.

### Accepted pre-release V1 schema cutover

Adding the application-error sidecar is an intentional coordinated cutover of
the pre-release V1 native ABI and canonical HTTP-response schema. It does not
establish an append-only extension rule for V1. Current decoders accept the
legacy response representation when the optional ABI field or canonical CBOR
key is absent. Older strict V1 decoders reject the extended representation
when that field or key is present.

Consequently, a Core and its paired language SDK, and all participants that
may exchange an application-error response, must be upgraded together before
the sidecar is emitted or consumed. Mixed-version rolling deployment for this
extension is unsupported. The unchanged numeric V1 marker is not evidence
that an older strict decoder accepts the extended shape. A future requirement
for rolling mixed-version evolution requires an explicit version or capability
negotiation design; this decision does not imply one.

Selection between inline and private transfer uses the final canonical byte
size. Every configured count, frame, payload, worker, and byte limit is checked
with overflow-safe accounting before allocation or ownership transfer.
The exact current canonical payload ceiling is 134,217,696 bytes; deployments
may configure a smaller limit.

## Stable transfer binding

All attempts for one logical message retain the same:

```text
message ID
conversation ID
authenticated mesh, sender, and recipient
interaction mode and RPC identity
transfer ID
payload profile
canonical size and SHA-256 digest
creation and expiry data
```

Current membership is checked at admission and again before publication or
successful completion. Removing a peer cancels and clears that peer's pending
transfer, object, completion, retry, and reassembly ownership.

Inline bytes, object references, chunk frames, and private carrier descriptors
may differ between attempts. Receiver duplicate suppression remains keyed by
authenticated mesh, authenticated sender, and message ID. Transfer digests
authenticate materialization; they do not redefine duplicate identity.

## Manifest and chunk framing

A private manifest binds at least:

```text
transfer ID and message ID
authenticated mesh, sender, and recipient
canonical size and digest
transferred size and digest
chunk size and count
expiry
optional private encryption descriptor
```

Each chunk carries the transfer ID, zero-based chunk index, byte offset, bytes,
and chunk digest. Direct carriers receive deterministic bounded CBOR frames.
XMPP receives those complete frames in a canonical text-safe encoding. Frame
limits include CBOR, text, and XML overhead.

Chunks are private transfer records. They cannot invoke an SDK handler, create
an RPC correlation, or become an application envelope. Duplicate indexes are
idempotent only when their bytes agree; conflicting duplicates fail closed.
Indexed geometry allows independently arriving chunks to converge on one
bounded reassembly entry.

## Direct carrier

The WebRTC path uses explicit chunk and in-flight limits, authenticated route
binding, cancellation, expiry, digest verification, and authenticated
completion evidence. A write failure before trusted completion is
fallback-eligible. An ambiguous completion preserves the original logical
identity for any later attempt.

## XEP-0363 object carrier

Core obtains slots from the authenticated server authority. URL handling
enforces HTTPS, permitted hosts and ports, DNS and IP policy, redirect checks,
header restrictions, response-body limits, exact content length where known,
streamed byte limits, and deadlines at every hop.

Upload acceptance is not delivery evidence. The recipient must authenticate
the manifest, download and verify the exact bytes, and return materialization
evidence. Object references, URLs, headers, and dependency errors are private
and never become SDK output.

Every non-nil prepared-upload handle transfers to the coordinator even when
slot preparation returns it together with an error. The coordinator invokes `Abort` exactly once after commit or on every failure, cancellation, and
timeout path. A panic from `Abort` is contained so it cannot replace the original carrier result or panic across the coordinator boundary.

## XMPP chunk carrier

The final carrier sends bounded text-safe frames over the authenticated exact
XMPP session in its separately verified mesh. Server visibility is explicit. Stanza limits are
applied after framing overhead. Completion still requires authenticated digest
evidence from the recipient.

## Reassembly and ownership

Reassembly validates the authenticated route, stable transfer binding,
authorized carrier attempt, geometry, expiry, and byte limits before reserving
capacity. Per-peer and global transfer counts and bytes are bounded. Different
authorized carriers for the same transfer converge on one quota entry and one
verified canonical completion.

Verified bytes remain owned until the matching logical envelope consumes them
or a terminal cleanup path destroys them. Success, failure, timeout,
cancellation, membership removal, and shutdown release each owned resource
exactly once. Temporary plaintext, private references, partial frames, and
future-seam key material are cleared where Go ownership permits it.

## Encryption boundary

Default V1 transfer relies on carrier transport protection. The private
`PayloadCipher`, `KeySealer`, and `KeyResolver` interfaces are an inactive seam
for a future reviewed identity-key design. Passwords are never payload keys.
The default composition does not claim end-to-end encryption for server-carried
payloads.

## Process lifetime

Transfer state and the sender outbox are process-local. XMPP connection or
session replacement does not recreate the outbox, so fallback keeps the same
logical message. Process termination clears local pending state. Persistent
sender storage and automatic advanced SDK streaming remain separate future
work.

## Acceptance evidence

Tests cover:

- canonical serialization, strict decoding, size boundaries, and exact byte
  accounting;
- carrier selection, fallback-eligible and terminal failures, cancellation,
  and ambiguous completion;
- manifest/frame validation, duplicate and conflicting chunks, missing chunks,
  geometry, expiry, digest mismatch, and quota pressure;
- current-membership fences at admission and completion;
- HTTPS URL, DNS, redirect, header, response, timeout, and cleanup policy;
- cross-carrier convergence on one transfer and one application invocation;
- handle, worker, reassembly, object-ingress, and shutdown races; and
- private-data and dependency-text leakage scans.
