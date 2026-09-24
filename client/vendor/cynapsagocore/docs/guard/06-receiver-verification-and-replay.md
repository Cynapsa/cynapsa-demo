# Receiver verification and replay

## Verification order

Before application delivery, a V3 receiver performs bounded checks in an order that avoids trusting unauthenticated claims:

1. Strictly decode canonical V3 and reject unknown, duplicate, oversized, noncanonical, or invalidly versioned data.
2. Materialize the complete canonical application payload and verify exact size and SHA-256.
3. Reconstruct the logical-V2 preimage and recompute `LogicalMessageDigest`.
4. Resolve the sender key from authenticated finite-freshness authority, not an embedded key or caller URL.
5. Validate sender installation/key purpose, status, epochs, and validity, then verify `ms1` over the exact Design message.
6. Determine current effective Guard requirement from authenticated authority.
7. When required, parse and verify `ga1` protected header/signature/JCS claims against an approved current Guard key.
8. Compare every exact claim to the received transfer and current authority, including both resources, installations, session generations, digest/proof, policies, authority, profiles/evidence, and time.
9. Apply durable replay admission.
10. Apply existing current deterministic application authorization and only then deliver.

No failure is repaired by calling Guard, a provider, or Management. The normal receiver performs zero Guard/provider calls and no per-message Management call.

## Exact live-session binding

Current V1 requires live source and destination session generations. A reconnect that reuses the same full JID/resource receives a new generation, so an old `ga1` fails. `stable_resource` and offline mailbox acceptance without a destination session are deferred and must not be implemented.

## Time and freshness

Use the pinned integer-time rules and conservative receiver allowance. Expiry equality is rejected. Attestation lifetime cannot extend logical expiry, policy validity, evidence validity, authority freshness, key validity, session eligibility, or local revocation knowledge. A cached snapshot, 304, g1, lease, or system-clock reset cannot extend authority.

## Replay state

The replay key and retention interval must come from the pinned Design candidate. State must bind the complete logical transfer and exact attested binding rather than a carrier packet. Duplicate transport frames, Rank 1/Rank 2 fallback, XEP-0198 replay, chunk retry, and object refetch may be transport duplicates, but only one application delivery may win the replay admission owned by GoCore.

A reused identifier with different canonical content/proof is a conflict, not a duplicate. A reply has a distinct logical transfer. Application side effects still require application-level idempotency; GoCore must not claim transactional exactly-once business effects.

## Downgrade behavior

If effective authority requires Guard/V3, reject V2, missing `ms1`, missing/mismatched `ga1`, unsupported proof version, unknown key, or proof stripping on any carrier. Guard-off scopes retain current V2 behavior and are not globally forced to V3 because another tenant enabled Guard.

## Required mutation coverage

Mutate every payload byte and logical field; source/destination identity/resource; installation ID/epoch; session generation; key ID/epoch/signature; authority epoch/sequence/digest; source/destination policy; provider/profile/evidence digest; JWS algorithm/type/header/payload/signature; issue/expiry; replay identifier; wire version; and proof presence. Each negative case must fail before application delivery with the pinned bounded error and without leaking proof/token/content material.
