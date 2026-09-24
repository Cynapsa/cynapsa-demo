# Carrier preservation

## One logical transfer across carriers

V3 is above carrier selection. The exact logical-V2 fields, `ms1`, and required `ga1` travel unchanged through:

- Rank 1 direct WebRTC data channel;
- Rank 1 TURN-relayed WebRTC data channel;
- Rank 2 XMPP inline delivery;
- Rank 2 object/reference delivery;
- chunking and reassembly;
- retry, resume, replay, and eligible carrier fallback.

TURN relays do not create a distinct semantic transfer. Rank 2 wrappers treat the strict GoCore frame as opaque. No carrier may decode and reconstruct proof fields through a looser representation.

## Implementation rules

- Define V3 as a separate strict protocol type and codec, not an extension hidden inside V2 `CredentialProof`.
- Keep logical-V2 digest bytes independent of carrier representation.
- Preserve exact proof bytes or reconstruct an identical already-validated typed structure under the pinned canonical rules.
- Include proof sizes in all inbound/outbound memory, queue, RPC, outbox, callback, and carrier budgets.
- Preserve clone ownership, zeroing, cancellation, shutdown, and single-release invariants for proof buffers.
- Reject a carrier that cannot negotiate/preserve the required version before content publication.
- A fallback may reuse the same authorization only if its exact live binding remains current and every claim still matches.

## Path-specific review targets

Rank 1 work must review data-channel framing, managed payload transfer, transfer admission, buffering, health/fallback, and TURN behavior. Rank 2 work must review private Core framing, XMPP stream-management replay, inline payloads, object upload/readiness/retrieval, chunks, mailbox/reconcile behavior, and server wrapper opacity. Shared work must review transport manager, mesh outbound/inbound, outbox, RPC borrowing, root adapters, delivery bridge, and native callbacks.

The current code frequently clones and clears `CredentialProof` as transitional private memory. That is not permission to reuse the field. A coding agent must replace or isolate ownership with the explicit V3 proof type while retaining the tested memory-safety properties.

## Assertions for every carrier

Future acceptance must capture sender, carrier, and receiver evidence proving:

- one sender Guard transaction per same-domain logical transfer;
- zero receiver Guard/provider calls;
- byte-identical logical digest, `ms1`, and `ga1` at send and receive;
- no proof content in server/provider/public logs;
- no V2/key-12 downgrade;
- no duplicate application delivery across retry/fallback;
- final fence occurred before first publication;
- proof buffers and owned payload memory are released/cleared on success, failure, cancellation, panic containment, and shutdown.

Mocks can prove serialization and call counts. Real disposable carrier environments are still required for integration, E2E, fault, and Coturn qualification.
