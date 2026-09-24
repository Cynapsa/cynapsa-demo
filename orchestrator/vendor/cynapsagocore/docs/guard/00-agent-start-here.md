# GoCore Guard agent start here

## Status

This folder is a documentation-only handoff for a future coding agent. It does not assert that Guard integration exists, is deployed, or is qualified. [`../../contracts.lock.json`](../../contracts.lock.json) now pins a remotely verified Design `implementation_candidate`; independently verify the commit, manifest, and artifact hashes before coding.

## Mandatory read order

1. Root [`AGENTS.md`](../../AGENTS.md), including the existing V1 orchestration and SDK-boundary rules.
2. [`../../contracts.lock.json`](../../contracts.lock.json). Stop if it is unpinned, the publication is not `implementation_candidate`, or `implementation_allowed` is not `true`.
3. The pinned `CynapsaGuardDesign/contracts/release-manifest.json`, every consumed machine-readable artifact, and all checksums named by the lock.
4. The pinned Design normative specs, especially crypto/wire, message binding/evaluation, runtime modes/dependencies, authority snapshots, policy semantics, limits/errors, and component contracts.
5. The pinned Design `integration-requirements/cynapsagocore/` README, implementation plan, and acceptance tests. Its end-to-end integrity proposal is explicitly deferred and nonnormative.
6. This folder in numeric order from `01` through `10`.
7. Existing repository architecture and qualification material under `docs/development/`, `docs/security/`, `docs/adr/`, and `docs/release/` before editing an owned subsystem.

Machine-readable Design contracts win over explanatory prose. Do not resolve a conflict locally. Record the exact paths and values, return the defect to the Design owner, wait for a new published candidate, then update the lock.

## Outcome and ownership

GoCore owns the client-side Guard integration:

- installation message-key lifecycle through Enrollment;
- deterministic logical-V2 digest and `ms1` construction;
- one sender Guard evaluation transaction;
- strict wire V3 creation, negotiation, propagation, and downgrade rejection;
- final current-authority fence before publication;
- receiver payload, sender-proof, attestation, authority, replay, and application-authorization verification;
- unchanged Guard-off behavior and stable public SDK boundary.

GoCore does not own provider integration, final policy definitions, destination-policy authority, Cloud KMS, Management state, g1 issuance, or ejabberd's authoritative session-generation source.

## Hard V1 distinctions

| Item | Current V1 meaning |
| --- | --- |
| logical version | Existing logical envelope version 2 and current deterministic CBOR mapping |
| wire version | New strict private version 3 for proof-carrying delivery |
| `ms1` | Ed25519 signature over the Design domain-separated 32-byte logical digest |
| `ga1` | Compact Guard JWS that binds the exact live destination, both installations and session generations, policy, authority, provider/evidence, and validity |
| V2 key 12 | Reserved, never emitted, and rejected if present, including empty bytes |
| Guard off | Existing runtime behavior with zero Guard/provider traffic |
| deferred proposal | `SenderTransferProofV1`, `signed_required`, and `stable_resource`; not V1 |

The V1 `ms1` is not the deferred exact-transfer proof. Exact live-session authorization comes from `ga1` for enforced traffic.

## Stop conditions

Stop and report rather than improvise if:

- the lock is missing a required checksum or references a mutable branch/tag;
- Design schemas, examples, vectors, and prose disagree;
- g1, authority, installation-key, session-generation, or wire negotiation inputs are unavailable;
- an implementation would require V2 key 12, a per-message Management call, receiver semantic evaluation, `stable_resource`, or proof-free enforced fallback;
- current repository behavior contradicts the pinned candidate;
- a test would need a production/shared service not explicitly authorized by repository instructions.

## Completion report

Use the Design handoff format and include: repository/commit, exact candidate pins, requirement IDs, changed files, behavior, commands/results, evidence paths and SHA-256, live dependencies, limitations, security compatibility, rollback revision, and unresolved ownership. Local fixtures prove only local behavior.
