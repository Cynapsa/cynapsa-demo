# Pod 1: Public Contract and SDK Boundary

Parent plan: [Cynapsa Go Core V1 Development, Validation, and QA Orchestration](PROJECT_DEVELOPMENT_VALIDATION_QA.md)

## Mission

Own the complete application-facing contract and the only translation path between language SDKs and private Go implementation. This pod defines stable `api/v1` schemas, private boundary-facing models, strict ABI encoding, normalized outputs, opaque handle ownership, and the root Go facade.

This pod is the primary enforcement point for the non-negotiable SDK abstraction boundary. It must assume that every private type, dependency error, serialized field, diagnostic attribute, and future internal enum is unsafe for SDK exposure until explicitly mapped into an allowlisted public value.

## Required subagents

The root orchestrator must create three independent agents for this pod:

1. `pod-01-builder`: owns production implementation and builder unit tests.
2. `pod-01-validator`: performs a read-only architecture, ABI, security, and compatibility review of the builder commits.
3. `pod-01-qa`: creates independent acceptance, negative, fuzz, and conformance tests without modifying production code.

Pod 1 runs twice:

- Stage A freezes the public contract, internal boundary model, and process-lifetime ADR before other pods implement against them.
- Stage B completes mappings, facade behavior, and the native ABI after Pods 2–5 stabilize private behavior.

The Stage B validator and QA pass must review the integrated candidate, not only the earlier Stage A commit.

## Owned production files and folders

### Root facade

```text
abi.go
config.go
core.go
doc.go
errors.go
payload.go
status.go
```

### Versioned public contract

```text
api/v1/address.go
api/v1/auth.go
api/v1/capabilities.go
api/v1/command.go
api/v1/completion.go
api/v1/config.go
api/v1/conversation.go
api/v1/delivery.go
api/v1/diagnostics.go
api/v1/doc.go
api/v1/error.go
api/v1/event.go
api/v1/event_payloads.go
api/v1/identifiers.go
api/v1/lifecycle.go
api/v1/lifecycle_commands.go
api/v1/mesh.go
api/v1/message.go
api/v1/payload.go
api/v1/policy.go
api/v1/status.go
```

### Private boundary-facing model

```text
internal/model/command.go
internal/model/config.go
internal/model/error.go
internal/model/event.go
internal/model/result.go
internal/model/status.go
```

### Sole public/private adapter

```text
internal/sdkboundary/abi_codec.go
internal/sdkboundary/adapter.go
internal/sdkboundary/capabilities_mapper.go
internal/sdkboundary/command_decoder.go
internal/sdkboundary/completion_mapper.go
internal/sdkboundary/config_mapper.go
internal/sdkboundary/diagnostics_mapper.go
internal/sdkboundary/error_mapper.go
internal/sdkboundary/event_mapper.go
internal/sdkboundary/payload_mapper.go
internal/sdkboundary/public_allowlist.go
internal/sdkboundary/status_mapper.go
```

### Native shared-library wrapper

```text
cmd/cynapsacore-shared/callbacks.go
cmd/cynapsacore-shared/exports.go
cmd/cynapsacore-shared/main.go
```

Pod 1 may add narrowly scoped files inside these folders, such as explicit schema codecs, handle registries, ABI buffer ownership, architecture tests, and generated-binding conformance vectors. New public files require the same validation as existing `api/v1` files.

## Owned documentation

```text
AGENTS.md
AZTM_SDK.md
AZTM_SDK_BOUNDARY.md
README.md
SECURITY.md
docs/adr/0004-v1-process-lifetime.md
```

Changes to `AZTM_GO_CORE.md`, `AZTM_FUTURE_ISSUES.md`, `go.mod`, or `go.sum` require root-orchestrator coordination.

## Explicit exclusions

Pod 1 must not implement or edit:

- Runtime, command queue, worker, delivery, or diagnostics internals owned by Pod 2.
- Envelope, stable identity, dedupe, RPC, policy, current membership, peer-lane,
  or outbox behavior owned by Pod 3.
- Connectivity, peer, or handshake behavior owned by Pod 4.
- Private payload and oversized-payload behavior owned by Pod 5.
- `integration/**` or `internal/testkit/**`, which are owned by Pod 6.
- Python or TypeScript SDK repositories unless the invoking user separately places them in scope.

When a private output is insufficient, Pod 1 must request a typed internal-model change through the owning pod. It must not inspect concrete private packages, use reflection, or serialize arbitrary values.

## Stage A: Contract and lifetime freeze

The builder must complete these tasks before other pods treat the boundary as stable:

1. Resolve ADR 0004 and change it to Accepted only after exact ownership and test obligations are documented.
2. Define process creation, startup, shutdown, destruction, cancellation, callback quiescence, library unload, and failure cleanup ordering.
3. Define numeric core handles, opaque payload handles, stale-handle rejection, non-reuse expectations, reference counting, buffer ownership, and idempotent release behavior.
4. Freeze the V1 public command, admission, completion, result, event, error, status, capability, diagnostics, configuration, identifier, and payload schemas.
5. Freeze the private `internal/model` variants required by the adapter without importing concrete connectivity or storage dependencies.
6. Define strict version negotiation and compatibility behavior for native ABI input and output.
7. Specify timeout representation and distinguish timeout, cancellation, shutdown, invalid handle, unsupported version, malformed input, and internal failure.
8. Ensure public identifier types are opaque and cannot encode private addressing or implementation semantics.
9. Add architecture tests proving `api/v1` imports no private packages and the shared wrapper has no direct private implementation imports.
10. Add a machine-enforced public-vocabulary scan that can later include generated Python and TypeScript artifacts.

The Stage A result must be reviewed by the Pod 1 validator and exercised by the Pod 1 QA agent before the root orchestrator records the contract freeze.

## Stage B: Adapter, facade, and ABI implementation

After Pods 2–5 are integrated, the builder must:

1. Implement `SDKBoundaryAdapter` as the only public/private translation component.
2. Decode public configuration and commands with exact schemas, explicit discriminators, bounded lengths, duplicate-field rejection, and unknown-field rejection.
3. Copy public values into private models field by field; never alias caller-owned mutable buffers.
4. Map every known private completion and event into a fresh typed public value.
5. Define total mappings for private status, errors, diagnostics, and capability data. Unknown variants must map to a safe public fallback or fail closed without stringifying private values.
6. Map every direct-chunk, XEP-0363, and XMPP-chunk internal transfer outcome into the same bounded public payload success or normalized transfer failure without exposing the selected mechanism.
7. Use boundary-owned public error messages and codes. Never attach raw causes, formatted dependency text, stack traces, or arbitrary fields.
8. Implement root facade lifecycle, submission, completion, event, status, shutdown, destruction, and payload-handle methods.
9. Implement the native shared-library exports without exporting Go pointers or Go-managed buffer addresses as durable handles.
10. Implement wrapper-owned buffer allocation and freeing, callback registration and clearing, concurrent callback teardown, timeout behavior, and stale-handle rejection.
11. Make all callbacks and polling paths consume the same already-normalized boundary values.
12. Produce deterministic ABI conformance vectors usable by both Python and TypeScript.
13. Remove Pod 1 scaffold sentinel returns only when the corresponding behavior and tests are complete.

## Builder test requirements

Builder-owned unit tests must cover at least:

- Every public command discriminator and exact schema.
- Missing, duplicate, unknown, oversized, malformed, and unsupported-version input.
- Every completion result and event payload mapping.
- Every public status, capability, error stage, error code, and lifecycle mapping.
- Default behavior for unknown private variants.
- Mutable input and output aliasing attempts.
- Core and payload handle creation, invalidation, non-reuse, double release, cross-core use, and concurrent access.
- Callback registration, callback removal, callback reentrancy policy, and callback teardown during shutdown.
- Buffer allocation limits, freeing, double freeing, and use-after-free prevention.
- Repeated create/start/shutdown/destroy and failed-initialization cleanup.
- ABI version compatibility and deterministic serialized vectors.

## Validator assignment

The validator must inspect the actual diff and produce severity-ranked findings. It must verify:

- Every exported identifier and serialized field is application-level and transport opaque.
- `api/v1` has no private imports, aliases, embedded types, catch-all metadata, or reflection-based encoding.
- The wrapper imports only the root facade and approved public packages directly.
- The adapter is the only producer of SDK-visible completions, events, errors, status, capabilities, diagnostics, and payload metadata.
- Every mapping is explicit and total; new private variants cannot leak automatically.
- Raw private errors, logs, metrics, paths, identifiers, external references, and dependency values cannot reach output.
- Unknown input fails closed before runtime admission.
- Go pointers and object addresses never cross or become ABI handles.
- Handle tables resist stale reuse, cross-runtime confusion, overflow, double release, and concurrent destruction.
- Callback and buffer ownership remains safe across garbage collection, cancellation, shutdown, and library unload.
- Public schema and ABI changes are versioned and accompanied by compatibility evidence.
- Tests assert semantic output, not only successful encoding.

Any unexplained public vocabulary match, generic pass-through, unsafe pointer lifetime, missing variant mapping, or unsupported ABI ownership case is blocking.

## QA assignment

The QA agent must create tests without changing production files. Its matrix must include:

- Black-box public lifecycle tests using only root-package and ABI surfaces.
- Golden public schema and ABI vectors.
- Mutation testing of command fields, lengths, enum values, identifiers, and version headers.
- Fuzzing of public configuration, command decoding, buffer handles, and malformed serialized input.
- Injection of representative private failures followed by scans of every public output channel.
- Successful and failed large-payload transfers through each private carrier while asserting identical public shapes and no carrier, chunk, object-reference, or reassembly metadata.
- Concurrent polling and callback consumers, including shutdown races.
- Repeated handle allocation and destruction to detect stale-handle reuse.
- Cross-core and cross-session handle misuse.
- Python/TypeScript parity-vector execution when those binding repositories are available.
- Exported-symbol and generated-header review.
- Public documentation and example scanning.

QA tests belong in `*_acceptance_test.go` and `*_fuzz_test.go` files under Pod 1-owned packages. Repository-wide integration files remain owned by Pod 6.

## Pod 1 completion gates

Pod 1 is complete only when:

- ADR 0004 is Accepted and matches implementation.
- All Pod 1 production functions required for V1 are implemented.
- No public command, completion, event, error, status, capability, diagnostic, payload value, callback, or buffer bypasses the adapter.
- Builder unit tests pass.
- Validator has no unresolved blocking finding.
- Independent QA tests pass, including fuzz and concurrency cases.
- Public-boundary and dependency scans pass.
- Native ABI builds successfully on all available supported CI targets.
- ABI conformance vectors are committed and deterministic.
- The builder, validator, and QA evidence is returned to the root orchestrator in the format required by the parent plan.
