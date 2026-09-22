# Pod 5: Payload System

Status: current implementation and qualification scope

Parent plan: [Cynapsa Go Core Development, Validation, and QA Orchestration](PROJECT_DEVELOPMENT_VALIDATION_QA.md)

## Mission

Implement deterministic canonical payloads, bounded payload handles,
multi-carrier transfer, integrity verification, secure object handling,
reassembly, and exact cleanup without exposing private storage or carrier data
to SDKs.

## Owned implementation areas

```text
internal/payload/**
internal/xep0363/**
```

The payload system consumes private envelope and carrier interfaces. It does
not own public API models, transport implementations, membership authority, or
the carrier-neutral message outbox.

## Required behavior

### Canonical payloads

- Serialize only closed native, HTTP-request, and HTTP-response variants.
- Use deterministic bounded encoding and strict semantic validation.
- Preserve exact body bytes and duplicate-preserving HTTP headers.
- Resolve local handles before wire serialization; never send a handle token.
- Choose inline or private transfer from the final canonical byte size.

### Handles and ownership

- Handles are opaque, unpredictable, Core-scoped, bounded, and stateful.
- Writes and reads enforce exact byte limits and overflow-safe accounting.
- Completion is immutable; retain/release and shutdown have one cleanup owner.
- Invalid, stale, consumed, cross-Core, and wrong-class handles fail closed.

### Transfer and materialization

- The carrier plan and stable binding follow ADR 0005.
- Manifests bind transfer/message identity, authenticated route, profile,
  canonical geometry, digest, and expiry.
- Reassembly uses per-peer and global count/byte limits.
- Duplicate frames are idempotent only when their bytes agree; conflicts fail
  closed.
- Completion requires exact authenticated transfer, message, and digest
  evidence before canonical bytes become available to the logical envelope.
- Current membership is checked at admission and before successful completion.

### Object security

- XEP-0363 slots come from the authenticated session authority.
- Every request and redirect validates scheme, host, port, credentials, DNS,
  resolved IPs, headers, content length, actual bytes read, and deadline.
- Upload acceptance alone is not recipient materialization evidence.
- URLs, headers, references, dependency text, and temporary paths remain
  private.

### Confidentiality

- Rank 1 uses DTLS transport protection.
- V1 XEP-0363 and XMPP transfer content is protected in transit but visible to
  the serving infrastructure.
- The private encryption interfaces are inactive future seams; passwords are
  never payload keys.

## Resource and cleanup rules

- Payload, transfer, frame, reassembly, worker, queue, object, and handle state
  has explicit count and byte bounds.
- Primary work and cleanup use separate bounded contexts.
- Success, failure, timeout, cancellation, peer removal, and shutdown release
  each owned resource exactly once.
- Partial bytes and sensitive temporary values are cleared where Go ownership
  permits it.

Narrow V1 exception (2026-08-24): worker and dependency panic containment and
resource release remain active except for a panic from exactly `payload.ObjectStore.Download`, both at synchronous materialization and inside
the asynchronous object-ingress flight. Ordinary download errors, context precedence, owned-result cleanup, cancellation, timeout, close, and non-panic
flight finalization remain active. All transfer workers and every other payload dependency boundary remain subject to containment and resource-release rules.
See
[`AZTM_FUTURE_ISSUES.md#11-private-xep-0363-and-object-dependency-panic-containment`](../../AZTM_FUTURE_ISSUES.md#11-private-xep-0363-and-object-dependency-panic-containment).

## Builder tests

Tests cover:

- canonical vectors, malformed data, strict re-encoding, and size boundaries;
- handle states, invalid use, capacity, concurrent reads/releases, and
  shutdown;
- digest tamper, truncation, extension, wrong size, and wrong identity;
- carrier selection and all fallback-eligible and terminal outcomes;
- manifest/frame geometry, duplicates, conflicts, missing frames, expiry,
  authentication, and quota pressure;
- object URL, DNS, redirect, header, response, timeout, cancellation, and
  cleanup policy;
- current-membership removal during every transfer phase;
- ambiguous and duplicate carrier completion convergence;
- Transfer-worker capacity, shutdown, job cancellation, panic containment, and resource release.
- worker saturation, cancellation storms, secret canaries, and resource leaks.

## Independent validation and QA

Validation reviews deterministic encoding, integrity, authenticated route
binding, SSRF defenses, resource ownership, limits, zeroization, and public
abstraction containment.

Independent QA fuzzes decoders, manifests, frame indexes, URL policy, redirects,
and size arithmetic; runs adversarial local HTTP servers; stresses concurrent
handle and reassembly operations; and scans every externally reachable output
for private data.

## Completion gates

- ADR 0005 and the V1 security model match implementation.
- Normal and race-enabled payload/XEP-0363 tests pass.
- Cross-carrier attempts converge on one verified canonical payload and one
  logical message identity.
- All failure paths are bounded and release resources.
- Public-boundary scans reveal no private reference, endpoint, credential,
  transfer identifier, dependency error, or partial payload.
