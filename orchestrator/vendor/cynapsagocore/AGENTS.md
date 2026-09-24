# Cynapsa Go Core Agent Rules

This repository implements the Go core library. Python and TypeScript packages are bindings over its versioned public contract; they are not alternative implementations.

## Non-negotiable SDK boundary

- SDK-facing source, symbols, serialized values, documentation, errors, events, logs, metrics, examples, generated bindings, and configuration must remain transport opaque.
- Do not expose underlying signaling, connectivity, negotiation, addressing, fallback-tier, or protocol-extension terminology through any public path.
- `api/v1` owns all SDK-visible types. It must not import `internal/*`.
- `internal/sdkboundary.Adapter` is the only public/internal translator. Map values field by field; never pass through internal structures, arbitrary maps, raw errors, or raw diagnostic attributes.
- Unknown public input fails closed. Unknown internal state maps to a bounded public fallback.
- `cmd/cynapsacore-shared` imports the root facade and `api/v1` only; it must not import internal packages directly or export Go pointers.
- Add or update boundary scans, dependency tests, and Python/TypeScript parity vectors whenever the public contract changes.

## Scaffold implementation rule

Every unfinished function must retain a precise `TODO` describing validation, ownership, state transitions, error normalization, and expected outputs. Replace `ErrNotImplemented` only when the behavior and its tests are implemented together.

## V1 implementation orchestration

When asked to execute the complete V1 development effort, start with `docs/development/PROJECT_DEVELOPMENT_VALIDATION_QA.md`. Read every pod document it references before dispatching work, preserve the file-ownership boundaries, and require independent validator and QA evidence for every pod.

Production completion also requires `docs/development/PRODUCTION_READINESS_NEXT_STEP.md` and Pod 7. Do not describe the project as production qualified after functional E2E alone.

For that execution plan, Pod 6 is authorized to create and configure disposable local ejabberd containers for integration and E2E testing. Follow the isolation, image-pinning, test-credential, resource-limit, prohibited-mount, and unconditional-teardown rules in the master and Pod 6 documents. This does not authorize access to production or shared servers.

Pod 7 has the similarly bounded authorization documented in the master and Pod 7 documents for disposable Coturn, fault-injection, SDK-build, and qualification containers. External publication remains prohibited without explicit user authorization.

## CynapsaGuard implementation gate

Guard work starts at [`docs/guard/00-agent-start-here.md`](docs/guard/00-agent-start-here.md). Read that file and the rest of `docs/guard/` in numeric order before changing Guard-related code or tests.

`CynapsaGuardDesign` is the contract authority. Guard implementation is prohibited while [`contracts.lock.json`](contracts.lock.json) is `UNPINNED`, while its pinned Design publication is not an immutable `implementation_candidate`, or while `implementation_allowed` is not `true`. Before coding, verify the exact Design commit, external release-manifest SHA-256, and every consumed artifact SHA-256 from a remote readback. Machine-readable Design contracts take precedence over prose. A conflict, missing field, or ambiguous state is a Design defect, not permission to invent a local contract.

The current V1 boundaries are non-negotiable:

- Logical message version remains 2. A new strict private wire version 3 carries the unchanged logical-V2 material plus `ms1` and optional `ga1` proofs.
- Current V2 deliberately omits and rejects reserved map key 12. Never repurpose `CredentialProof` or make V2 proof-bearing.
- `ms1` is the Design-defined Ed25519 signature over the domain-separated logical digest. `ga1` is the Guard compact JWS that supplies the exact live-session, installation, policy, and authority binding for enforced traffic.
- Guard-off enrollment, cached `e2` login, renewal, Rank 1, TURN, Rank 2, object delivery, and `continue`/`disconnect` behavior remain unchanged and make zero Guard/provider calls.
- The deferred `SenderTransferProofV1`, independent `signed_required` policy, and `stable_resource` mailbox proposal are not V1 and must not be implemented.
- One sender transfer creates one Guard transaction. A normal same-domain receiver makes zero Guard/provider calls. Ordinary send/receive does not make a per-message Management call.
- Never hold global, profile, mesh, transport, peer, outbox, or delivery locks across Guard or other network I/O. Immediately before publication, run the final current-authority/session/policy fence defined by Design.

Use the repository's pinned toolchain and existing commands. At minimum run applicable unit, race, integration, E2E, fault, Coturn, shared-library, doc-contract, and release checks described in [`docs/guard/08-testing-and-acceptance.md`](docs/guard/08-testing-and-acceptance.md). Do not claim production, live-provider, deployed, HA, or qualification success from local fixtures.

Every Guard handoff must report the repository commit, pinned Design commit and checksums, completed requirement IDs, files changed, commands and exact results, evidence paths and digests, live dependencies exercised, limitations, security/compatibility notes, and rollback compatibility. If implementation discovers a contract change, stop affected work, update and publish Design first, then update this repository's lock before resuming.
