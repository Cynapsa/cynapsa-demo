# Current baseline and ownership

## Reviewed repository baseline

The documentation baseline was prepared against GoCore commit `8ce2d3f197afde7125609bcb3e8d6af4996732e1`. Recheck the current commit and source before implementation. The repository currently has:

- a private `internal/protocol.Envelope` fixed to `Version2`;
- deterministic RFC 8949 CBOR from `fxamacker/cbor`;
- `LogicalMessageDigest` over the carrier-invariant logical-V2 shape;
- strict unknown-field and noncanonical-decoding rejection;
- a reserved wire key 12 represented by the transitional in-memory `CredentialProof` field but deliberately omitted by the V2 encoder;
- inbound messaging rejection of nonempty transitional proof material;
- existing Rank 1 direct/TURN and Rank 2 XMPP/object paths;
- strict SDK/public-boundary tests and a C shared-library build;
- Docker-backed integration, E2E, fault, Coturn/NAT, and release qualification packages.

This is not a claim that wire V3, `ms1`, `ga1`, g1, Guard leases, Guard calls, or Guard receiver verification are implemented.

## Current source anchors

| Concern | Existing anchor to inspect first |
| --- | --- |
| logical/wire envelope | `internal/protocol/envelope.go`, `internal/protocol/codec.go`, `internal/protocol/validation.go` |
| outbound immutability and policy | `internal/mesh/messaging_outbound.go`, `internal/policy/` |
| inbound admission/delivery | `internal/mesh/messaging_inbound.go` |
| carrier manager/fallback | `internal/transport/manager.go` |
| Rank 1 propagation | `internal/transport/rank1webrtc/` |
| Rank 2 propagation | `internal/transport/rank2xmpp/` |
| durable pending/retry state | `internal/outbox/`, `internal/rpc/` |
| enrollment/profile state | `internal/enrollment/` |
| runtime composition | `internal/runtime/` and root facade |
| public API | `api/v1/` and `internal/sdkboundary/` |
| native ABI | `cmd/cynapsacore-shared/` |
| system/qualification | `integration/`, `test/e2e/`, `test/production/`, `test/regression/` |

These anchors guide investigation, not file ownership for a future parallel plan. Read `docs/development/PROJECT_DEVELOPMENT_VALIDATION_QA.md` before splitting implementation work.

## Contract ownership

GoCore may implement only what the pinned candidate assigns to CORE-001, CORE-002, and CORE-003. In particular:

- Management owns policy, installation-key records, Guard verification keys, authority epochs/sequences/snapshots, and stale-state rules.
- Enrollment is GoCore's versioned path for key registration/rotation and Guard bootstrap. GoCore does not call Management directly.
- The runtime issuer owns short-lived g1 issuance and exact claims.
- ejabberd owns accepted-session generation, applied routing authority, protocol floor, and admission enforcement.
- Guard verifies/evaluates and signs allow attestations. A provider adapter supplies evidence only.
- GoCore verifies sender/Guard proofs and applies current deterministic application authorization before delivery.

## Availability boundary

Normal sender and receiver messaging must use cached, authenticated, finite-freshness authority. It must not call Management for every message. An enforced sender calls Guard once for the logical transaction. A normal same-trust-domain receiver calls neither Guard nor a provider. Guard-off traffic must not resolve or contact Guard/provider endpoints at all.

## Public-boundary constraint

Guard endpoints, g1 tokens, installation private keys, wire proofs, leases, authority internals, provider names, and attestation details remain private implementation state. Public SDK surfaces may expose only pinned, bounded, transport-opaque outcomes through `api/v1`. Update architecture scans, ABI contracts, and SDK parity vectors if the approved public contract changes.
