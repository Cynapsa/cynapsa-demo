# Pod 3: Messaging Semantics

Status: current implementation and qualification scope

Parent plan: [Cynapsa Go Core Development, Validation, and QA Orchestration](PROJECT_DEVELOPMENT_VALIDATION_QA.md)

## Mission

Implement the carrier-neutral message model shared by every delivery path:
deterministic private envelopes, Core-owned identity, authenticated binding,
bounded duplicate suppression, RPC correlation, current-membership policy,
peer lanes, and the process-lifetime outbox.

## Owned implementation areas

```text
internal/protocol/**
internal/conversation/**
internal/rpc/**
internal/policy/**
internal/mesh/**
internal/outbox/**
```

The pod owns private protocol types used by connectivity and payload code. It
must not expose carrier types through `api/v1`, the native ABI, generated
bindings, diagnostics, or SDK-facing errors.

## Normative decisions

### Envelope and identity

- Private envelope V2 uses deterministic CBOR and strict bounded decoding.
- Core creates message IDs, conversation IDs, correlation IDs, reply handles,
  and carrier metadata.
- The authenticated mesh, sender, recipient, and derived conversation must
  match before stateful receive processing.
- User input cannot override Core-owned identity fields.

### Membership and peer lanes

- One complete server snapshot is the authority for the authenticated logical
  session.
- One lazy bounded actor lane owns the active work for each peer.
- All lanes share aggregate count and byte budgets.
- Loss of server authority pauses publication and carrier I/O until authority
  is restored.
- Outbound resolution and final topology admission wait within the caller's
  existing context while authority is paused. They continue only against the
  restored current snapshot; a removed peer is rejected and an RPC whose
  lifetime expires while waiting completes as `rpc_timeout`.
- A snapshot that omits a peer fences and terminally clears that peer's lane.

### Duplicate suppression

- The receiver key is authenticated mesh, authenticated sender, and message ID.
- A repeated key is dropped without comparing content.
- In-flight and terminal evidence has dedicated per-peer and global capacity.
- Terminal evidence remains for 25 hours to cover the server mailbox window.

### RPC

- Requests and responses use unpredictable correlation identity and exact reply
  identity.
- Tables are bounded; reply handles are scoped, expiring, and single use.
- Timeout, cancellation, response, membership removal, and shutdown share one
  terminal-winner boundary.
- Late or mismatched responses cannot complete another request.

### Outbox and replay

- Core owns one bounded carrier-neutral outbox in process memory.
- A logical message keeps its identity while carrier ownership changes.
- Rank 1 completion requires exact authenticated receipt evidence.
- Rank 2 ownership and XEP-0198 acknowledgement remain private transport state.
- XMPP session replacement preserves pending entries; process termination does
  not.

### Policy

- Inbound authorization uses authenticated identity, current membership,
  integrity, interaction mode, and application path.
- Outbound preflight is not inbound authorization.
- Dependency text, credentials, private identifiers, and carrier details cannot
  cross the public boundary.

## Required implementation properties

1. Deterministic private encode/decode with strict version, field, and size
   validation.
2. Collision-resistant identifiers with bounded retry and no embedded secrets.
3. Trusted conversation derivation and envelope binding.
4. Bounded message-ID duplicate suppression before handler invocation.
5. Bounded RPC lifecycle with exact correlation and terminal ownership.
6. Current-membership authority with batch peer reconciliation and immediate
   removal fences.
7. Lazy peer actors with bounded quarantine while authority is paused.
8. One bounded process-memory outbox with atomic carrier ownership transitions.
9. Identity-preserving carrier replay and at-most-once handler invocation after
   duplicate convergence.
10. Cancellation-aware cleanup and zeroization of owned payload bytes.

Narrow V1 exception (2026-08-24): fail-closed policy enforcement and policy
dependency containment remain active except for panic containment at exactly `policy.GateConfig.Now`,
`policy.MembershipVerifier.RequireAll`,
`policy.ApplicationPathExtractor.ExtractApplicationPath`, and
`policy.RuleAuthorizer.Allows`. Their ordinary result and error handling, bounded error mapping, clock and path validation, membership checks, denial
semantics, authenticated identity, fail-closed authorization, caller ownership,
snapshot cleanup, cancellation, and shutdown requirements remain active. All other policy and messaging dependencies, callbacks, goroutines, cleanup paths,
and security invariants remain subject to containment and resource-release
requirements. See
[`AZTM_FUTURE_ISSUES.md#12-private-policy-dependency-panic-containment`](../../AZTM_FUTURE_ISSUES.md#12-private-policy-dependency-panic-containment).

- An inbound policy gate that verifies integrity, authenticated identity, mesh scope, membership, and application authorization in a fail-closed order.

Any nondeterministic codec, unbounded decode, authorization bypass, replay identity change, duplicate handler invocation,
secret exposure, or late-response confusion is blocking.

## Builder tests

Builder tests cover:

- envelope golden vectors and malformed, duplicate, unknown, truncated, and
  unsupported input;
- identifier validation, collision handling, and conversation derivation;
- authenticated binding before dedupe admission;
- duplicate replay, same-ID changed-content replay, retention, and capacity;
- RPC success, error, timeout, cancellation, late response, duplicate response,
  handle reuse, and table pressure;
- current snapshot installation, paused quarantine, recovery, batch removal,
  and absent-peer authorization failure;
- policy allow/deny and malformed or unauthenticated input;
- outbox admission, ownership races, fallback replay, terminal removal,
  process-lifetime recovery, capacity, and zeroization; and
- carrier-attempt convergence on one logical message and one handler call.

## Independent validation and QA

Validation must verify that public surfaces remain transport-opaque, all state
is bounded, authenticated values are used for authorization, stale callbacks
cannot publish, and cleanup has one owner.

Independent QA adds fuzzing, randomized RPC lifecycle tests, duplicate and
carrier-race tests, capacity tests, current-membership race tests, secret-canary
scans, and process-lifetime recovery simulations. Production code corrections
return to the builder and the affected validation scopes rerun.

## Completion gates

- ADRs 0002, 0003, 0006, and 0008 match production behavior.
- Builder, independent validation, and independent QA have no unresolved
  finding.
- Normal and race-enabled package tests pass.
- Cross-pod tests prove identical behavior through Rank 1, Rank 2 live
  delivery, exact-resource mailbox replay, and carrier fallback.
- Public-boundary scans find no private identity, credential, dependency, or
  carrier leakage.
