# GoCore Guard implementation plan

This plan is ready for assignment but blocked by the unpinned contract lock. It specifies sequence and acceptance, not code already delivered.

## Phase 0: candidate and baseline gate

- Replace `contracts.lock.json` with the final remote-readback Design commit, external manifest SHA-256, consumed artifact SHA-256 values, versions, and `implementation_allowed: true`.
- Add executable lock/contract verification to CI.
- Re-run the complete current Guard-off baseline, doc-contract, shared-library, integration, E2E, fault, Coturn, and release suites as authorized.
- Inventory current lock ownership, payload copies, replay/dedupe keys, carrier frames, budgets, SDK mappings, and profile migration behavior.
- Stop on any Design conflict; never invent a local field/state.

Exit: immutable inputs are verified and current behavior has reproducible evidence.

## Phase 1: key and authority foundations

- Implement versioned Ed25519 installation message-key generation, encrypted atomic persistence, proof of possession, Enrollment registration/rotation, authoritative cache, revocation, and redaction.
- Implement authenticated finite-fresh authority/key/policy/Guard-key/session views without per-message Management calls.
- Implement g1 and lease state as separate bounded credentials, with Guard-off zero-fetch behavior.

Exit: key lifecycle and state-machine tests pass, including crash, conflict, rotation, revocation, stale restore, and off-mode outage.

## Phase 2: immutable sender and Guard client

- Freeze canonical payload/logical fields and retain existing logical-V2 digest bytes.
- Implement exact `ms1` construction and vectors.
- Implement strict bounded Guard client, g1 scope/refresh, exact retry/event identity, one transaction with eligible bindings, response/JWS parsing, and no locks over I/O.
- Implement the final current-authority fence immediately before publication.

Exit: valid/mutation vectors and concurrency races prove one sender call, no post-fence mutation, no publish on stale/changed authority, and unchanged Guard-off traffic.

## Phase 3: strict V3 and carrier propagation

- Introduce a separate V3 protocol type/codec. Preserve logical key 0 as 2.
- Keep V2 key 12 rejected and do not reuse `CredentialProof`.
- Add negotiated capability/protocol floor and proof-stripping/downgrade rejection.
- Propagate identical validated proof material across Rank 1 direct/TURN, Rank 2 inline/object, chunks, retry/resume, outbox, RPC, and eligible fallback.
- Update memory/budget/clone/clear/cancel/shutdown ownership.

Exit: every carrier and fallback passes byte-preservation, budget, fuzz, race, cleanup, and no-V2-downgrade tests.

## Phase 4: receiver verification and replay

- Strictly decode V3, materialize/hash payload, reconstruct logical V2, verify authoritative sender key and `ms1`.
- Resolve effective policy locally, verify required `ga1` header/signature/claims/time/exact live-session bindings, and enforce current authority freshness.
- Add durable replay admission before application delivery, then retain existing deterministic application authorization.
- Ensure zero receiver Guard/provider and per-message Management calls.

Exit: full mutation, reconnect-generation, replay/fallback, stale authority, zero-call, and delivery-order tests pass.

## Phase 5: private runtime and public boundary

- Compose lifecycle, health, errors, metrics, and redacted diagnostics behind the private runtime.
- Map only approved bounded outcomes through `api/v1` and `internal/sdkboundary.Adapter`.
- Update native ABI only if the pinned public contract requires it; preserve exact header/schema/export allowlist and language parity.
- Keep provider, carrier, credential, proof, and authority internals out of public surfaces.

Exit: architecture scans, ABI/native smoke, parity vectors, leakage tests, and existing API compatibility pass.

## Phase 6: coordinated validation

- Run repository unit, race, fuzz, contract, integration, E2E, fault, Coturn/NAT, regression, shared-library, and release gates.
- Coordinate real revision-pinned acceptance with Guard, Management, Enrollment/issuer, ejabberd, provider, infrastructure, SDK, and CynapsaTests owners.
- Exercise off/observe/enforce canary, failure, region loss, load, chaos, soak, staged rollback, and cleanup.
- Return the exact handoff report required by Design.

Exit: local evidence is complete and accurately labeled. Production status is granted only by the separate coordinated qualification record.

## Dependency order

The agreed cross-repository order is Design candidate, Management authority models/fixtures, Guard and Jev provider, Enrollment/issuer and ejabberd authority, then GoCore, SDKs, infrastructure, and CynapsaTests qualification. GoCore may prepare investigation/tests earlier, but it must not implement against drafts or assume unfinished producer behavior.
