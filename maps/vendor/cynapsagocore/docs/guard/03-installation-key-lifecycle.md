# Installation message-key lifecycle

## Key purpose and scope

Each installation needs a dedicated Ed25519 message-signing key for `ms1`. It is not an Auth0/JWT key, `e2`, g1, enrollment secret, DPoP key, DTLS certificate key, XMPP credential, or Guard attestation key. The private key never leaves GoCore or enters the public SDK ABI.

Authority binds the public key and key state to the exact trust domain, logical agent, installation ID, installation epoch, key ID, key epoch, purpose, status, and finite validity. The coding agent must use the pinned authority schema and not design a parallel local registry.

## Creation and persistence

- Generate with the operating-system CSPRNG.
- Persist atomically with the encrypted installation profile before advertising the key.
- Version the profile migration and preserve crash recovery, rollback, inter-process lock, permissions, and defensive-copy rules already enforced by `internal/enrollment/`.
- Never log, emit through diagnostics, serialize into application-visible output, or include private bytes in test artifacts.
- Clear transient private material where Go ownership permits. Do not claim hardware or process isolation: the SDK loads GoCore in the application process.

If storage migration fails, do not partially advertise or use a key. Existing Guard-off profiles must remain usable without generating or registering a Guard key unless authoritative configuration requires Guard-capable bootstrap.

## Registration path

GoCore uses Enrollment's pinned versioned endpoint. It does not call Management directly. Registration and rotation require proof of possession and exact installation/epoch scope. The authoritative result supplies key ID, key epoch, status, validity, and revision. A lost response is recovered using the contract's idempotent lookup/retry semantics, never by inventing another active identity.

Key registration is control-plane activity, not a per-message dependency. Cache only authenticated key/authority material with an absolute freshness deadline. A 304, local retry, lease, or old snapshot must not extend freshness.

## Rotation and revocation

- Rotation increments an authoritative monotonic key epoch.
- Sign only with the single currently authorized signing key.
- Accept an older `retiring` verification key only for the exact bounded overlap published by authority.
- Unknown, expired, revoked, wrong-purpose, wrong-installation, or epoch-mismatched keys fail closed when proof is required.
- Installation revocation invalidates every message key for that installation epoch.
- A restored profile must reconcile authority and must not resurrect a revoked or superseded key.

## Runtime separation

Maintain separate state for:

- local private-key ownership;
- authoritative public-key record and revision;
- g1 credential and expiry;
- Guard lease and expiry;
- authority snapshot epoch/sequence/freshness;
- current source session generation.

No one item extends another. In particular, a live g1 does not prove key registration, a lease does not extend snapshot freshness, and a cached `e2` does not authorize enforced messaging.

## Required future tests

Cover first creation, crash between persistence and registration, exact retry, key conflict, proof-of-possession failure, rotation overlap boundaries, revocation, installation-epoch replacement, corrupted profile, concurrent profile writers, restore from stale backup, redaction, and Guard-off profiles with all Guard/control endpoints unavailable.
