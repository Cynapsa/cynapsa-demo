# Guard client and g1

## Credential boundary

g1 is a short-lived Guard-specific bearer credential issued by the authorized runtime issuer. It is separate from `e2`, enrollment secrets, installation signing keys, provider credentials, and Guard attestation keys. Its pinned claims must bind exact audience, trust domain/organization, logical agent, installation ID/epoch, mesh, profile/protocol scope, and authority constraints.

GoCore obtains g1 through the approved Enrollment/issuer bootstrap path. It does not mint g1, reuse `e2`, accept caller-supplied Guard URLs, or expose g1 through SDK values, logs, events, errors, dumps, or traces. Guard-off mode does not request, refresh, store, resolve, or transmit g1.

## Client configuration

Use fixed, authenticated configuration from the pinned contract. Enforce HTTPS, exact audience/host policy, request/response size bounds, deadlines, connection limits, strict JSON, canonical base64url, and redacted errors. Do not follow redirects to caller-controlled hosts. Public GoCore-to-Guard requests use `Authorization: Bearer <g1>`; they do not use Cloud Run service-account identity headers.

Refresh g1 before expiry with bounded jitter/backoff and single-flight ownership. An expired or wrong-scope token cannot be used for enforced evaluation. Token refresh must not hold global/profile/mesh/transport/peer/outbox locks or block Guard-off work.

## One sender transaction

For one logical transfer, GoCore:

1. freezes canonical application payload bytes and immutable logical fields;
2. computes the logical-V2 digest and `ms1`;
3. resolves one fresh authenticated authority view without a per-message Management call;
4. creates the exact pinned evaluation request with all batch-equivalent live destination bindings;
5. makes one `/v1/evaluate` Guard transaction;
6. strictly validates the decision and, for ALLOW, exactly one matching `ga1` per binding;
7. runs the final current-authority fence;
8. publishes through eligible carriers.

Replies are new logical transfers. Bindings that differ outside Design's exact batch-equivalence set require separate transactions. A timeout, malformed response, ambiguous provider execution, `INDETERMINATE`, `DENY`, stale authority, missing attestation, or mismatched attestation produces no enforced publication.

## Locking and cancellation

Build an immutable transfer snapshot under the minimum existing locks, then release them before token refresh, DNS, TLS, Guard HTTP, or other network I/O. Completion reacquires narrowly scoped ownership, confirms the transfer is still current, and applies the final fence. Cancellation must cancel the HTTP attempt, ignore late results through generation/fence checks, clear sensitive buffers, and release capacity exactly once.

Do not use a lock to prevent duplicate external requests. Use explicit transfer ownership, stable event ID, exact retry fingerprint, and the Guard idempotency contract. Conflicting event reuse is terminal. An exact client retry must not change payload, binding, authority, policy, or event identity.

## Receiver traffic

The normal receiver never calls Guard or a provider. It verifies the carried proofs and current cached authority locally. It also does not call Management per message. Missing or stale inputs fail closed for enforced delivery rather than triggering an unbounded synchronous control-plane lookup.
