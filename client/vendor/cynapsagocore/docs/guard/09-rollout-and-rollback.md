# Rollout and rollback

## Preconditions

Do not enable Guard code paths until all participating repositories pin the same immutable Design implementation candidate. Production enforcement additionally requires a separate qualified-release record naming exact GoCore, Guard, Management, Enrollment/issuer, ejabberd, provider, infrastructure, and test revisions.

Before each rollout scope, prove:

- authoritative source/destination policies and keys are available and fresh;
- g1 issuance and Guard lease paths work;
- ejabberd assigns and enforces session generation and protocol floor;
- both endpoints advertise compatible V3 capability;
- all carriers preserve proofs;
- Guard/provider failure modes and final fencing pass;
- current Guard-off regression and rollback behavior pass;
- observability is content/token/proof safe.

## Staged order

1. Land dormant code and strict decoders with Guard-off behavior unchanged.
2. Publish capability without authorizing V3/enforcement.
3. Roll out Management/Enrollment/issuer/ejabberd authority producers and verify monotonic state.
4. Exercise observe mode with bounded best-effort evaluation; its result never authorizes enforced delivery.
5. Canary enforce only for exact compatible source/destination installations and live sessions.
6. Expand by trust domain/mesh/policy after error, latency, stale-authority, provider, call-count, and carrier evidence passes.
7. Run coordinated qualification, region/fault/load/soak, and rollback exercises before broader release.

Guard-off tenants must not receive g1, leases, V3 requirements, Guard DNS traffic, inspection, or availability impact merely because another tenant is enabled.

## Rollback rules

Rollback selects an exact immutable revision and compatible contract set, never a mutable branch or image tag. It may disable a not-yet-enforced canary through a committed authoritative policy transition. It may not silently reinterpret a missing policy as off.

Once a scope is enforced, rollback must not:

- select proof-free V2;
- reuse V2 key 12;
- accept cached-`e2`-only admission as Guard authority;
- omit `ms1`/required `ga1` verification;
- accept an old session generation, key epoch, policy, or authority snapshot;
- use deferred `stable_resource` to rescue delivery;
- fail open or downgrade to deterministic-only delivery;
- restore stale authority without fencing/reconciliation.

If no qualified compatible rollback exists, stop/isolate affected enforced transfers and recover forward. Existing Guard-off traffic remains on its independently validated path.

## Rollout evidence

Retain exact policy transition, capability census, Design/implementation revisions, image/library checksums, before/after call counts, errors/latency, proof-preservation traces, final-fence race results, rollback revision, and cleanup results. Coordinate final acceptance in `CynapsaTests`; do not rewrite the published Design candidate as qualification evidence.
