# ADR 0003: Conversation Identity, Deduplication, and Replay

- Status: Accepted
- Scope: Stable logical-message identity, authenticated binding, duplicate
  suppression, and carrier replay
- Accepted for: private envelope V2 and the current-membership peer-lane model

## Context

A logical message may move from Rank 1 to Rank 2, survive an XMPP connection
replacement in the sender's process, or arrive more than once after ambiguous
carrier completion. Those attempts must converge on one application message
without trusting identifiers supplied by an SDK or by unauthenticated wire
data.

## Identifiers

Core creates message IDs and private RPC correlation IDs from 128 bits of
`crypto/rand` entropy. Their canonical forms are `msg_` or `cor_` followed by
exactly 22 unpadded base64url characters. Invalid forms are rejected. An owning
registry may provide a collision predicate; creation is bounded to eight
attempts and fails closed on entropy failure or collision exhaustion.

Identifiers contain no participant, mesh, time, credential, carrier, or other
secret-bearing metadata. A replay reuses the original message ID and, when
applicable, the original correlation ID and reply identity.

## Conversation identity

Conversation identity is deterministic for one mesh and canonical participant
pair:

```text
first, second = bytewise-lexicographically sorted canonical participant UTF-8
input = "CYNAPSA-CONVERSATION-ID-V3\0"
        || uint32be(len(mesh_id)) || mesh_id
        || uint32be(len(first))   || first
        || uint32be(len(second))  || second
conversation_id = "conv_" || base64url_no_padding(SHA-256(input))
```

The mesh and identity layers supply the exact authenticated mesh, sender, and
local recipient. The conversation package validates those trusted values,
derives the expected `conv_` identifier, and requires the envelope to match.
An envelope cannot establish its own mesh, sender, recipient, conversation, or
membership authority.

Structural validity is not authorization. Current membership and policy are
checked independently before publication or outbound success.

## Bounded duplicate suppression

One receiving process owns a bounded dedupe table. The complete duplicate key
is:

```text
authenticated mesh + authenticated sender + canonical message ID
```

The table classifies a key as new, already in flight, or terminal. Once a key
exists, duplicate classification does not compare payload bytes, metadata, or
digests. Digests remain integrity and materialization evidence.

The default limits are 4,096 entries per peer and 65,536 globally. Terminal
entries remain for 25 hours, one hour beyond the Mesh Server's maximum 24-hour
exact-resource mailbox retention. In-flight entries remain until trusted
terminal handling or explicit local abandonment. Capacity exhaustion fails
closed instead of evicting live or unexpired evidence.

Removing a peer retires that peer's process-local dedupe state. A later
authenticated snapshot may authorize the identity again as a current member.

## Replay and carrier convergence

Every carrier attempt for one logical message preserves:

```text
message ID
conversation ID
authenticated sender and recipient
mesh ID
interaction mode
correlation ID and reply identity when applicable
creation and expiry data
payload profile, canonical size, and canonical digest
```

Carrier representation may change. Inline bytes, transfer references, object
references, and private carrier metadata are not duplicate identity. A valid
completion must match the canonical size and digest. A later authenticated copy
with the same scoped message ID cannot invoke the application again.

Private transfer records are not application envelopes, do not create RPC
correlations, and cannot invoke application handlers independently.

## Process lifetime

The dedupe table and carrier-neutral outbox are process-local. XMPP session
replacement does not recreate the outbox, so pending entries retain their
logical identity. Process termination clears this state; persistent sender
storage is deferred.

## Acceptance evidence

Tests must cover:

- canonical identifier validation, entropy failure, collision retry, and
  collision exhaustion;
- conversation derivation symmetry and separation by mesh and participant;
- authenticated envelope binding before state admission;
- new, in-flight duplicate, terminal duplicate, expiry, peer retirement, and
  per-peer/global pressure;
- same-ID replay with changed content being suppressed without a content check;
- carrier fallback and ambiguous completion converging on one application
  invocation; and
- process-lifetime outbox replay preserving the original logical identity.

## Consequences

- Carrier fallback cannot manufacture a second logical message.
- Duplicate suppression is independent of payload integrity verification.
- SDKs and applications cannot inject Core-owned identity fields.
- Cross-process sender persistence remains future work.
