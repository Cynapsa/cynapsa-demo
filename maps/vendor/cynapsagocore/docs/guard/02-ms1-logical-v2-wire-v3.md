# `ms1`, logical V2, and private wire V3

## Three separate version axes

Do not conflate:

1. Logical digest profile: version 2, unchanged.
2. Private carrier envelope: current V2 versus new proof-carrying V3.
3. Proof objects: `ms1` sender proof and optional `ga1` Guard attestation.

Wire V3 transports the unchanged logical-V2 fields plus a strict proof container. It does not substitute 3 into logical CBOR key 0. The receiver recomputes the same logical-V2 digest used by the sender and Guard.

## Existing logical-V2 digest

The current `LogicalMessageDigest` maps deterministic CBOR integer keys to version, message/conversation IDs, logical sender/recipient, mesh, mode, correlation/reply metadata, created time, payload profile/size/SHA-256, optional logical expiry, and clock uncertainty. The exact mapping and omission rules come from the pinned Design candidate and existing `internal/protocol/codec.go`.

The logical digest excludes carrier kind, inline bytes, object/transfer references, encryption references, carrier provenance, `ms1`, and `ga1`. Payload bytes remain bound through exact size and SHA-256.

## V1 sender proof

For digest `D`, exactly 32 bytes, `ms1` signs the exact 56-byte message:

```text
UTF8("CYNAPSA-SENDER-PROOF") || 0x00 || UTF8("v1") || 0x00 || D
```

Use ordinary Ed25519 with no prehash and no alternate JSON/CBOR preimage. The proof object contains only the pinned schema fields, including its approved key ID and raw 64-byte signature encoded as canonical unpadded base64url. `sender_proof_sha256` uses the separate Design-defined proof-object domain and length-prefixed key ID.

`ms1` proves that the authoritative installation key signed the logical message digest. It does not independently bind exact resources, installations, sessions, policies, or authority. In current enforced V1, `ga1` supplies those exact live-session bindings.

## Guard attestation

`ga1` is the attached compact JWS specified by Design. It uses an exact protected header, RFC 8785 claims, Ed25519/`EdDSA`, a pinned `kid`, and the exact raw ASCII signing input. GoCore verifies the claims against the received transfer and current authenticated authority, not merely the signature.

The verifier rejects alternate algorithms/types, unprotected or remote-key headers, noncanonical base64url/JCS, claim additions/omissions, wrong purpose/issuer/key status, stale validity, and any exact-binding mismatch.

## V2 stays proof-free

Current V2 deliberately reserves key 12 and never emits it. Strict decode rejects key 12 whether its value is empty or nonempty. Preserve that behavior. Do not serialize `CredentialProof`, reinterpret it as `ms1`/`ga1`, or let a V3 value enter the V2 decoder.

V3 must be a new strict type and codec path with bounded fields, deterministic encoding, duplicate/unknown rejection, canonical re-encoding checks, clone/clear ownership, and fuzz coverage. Its proof container must survive all carriers without changing logical bytes or proof values.

## Deferred design that is not V1

Do not implement the Design document titled end-to-end message integrity as current behavior. Its `SenderTransferProofV1` adds an exact-transfer preimage, independent `signed_required` policy, and `stable_resource` offline binding. Current V1 instead:

- signs only the domain-separated logical digest in `ms1`;
- requires live source and destination session generations for enforced `ga1`;
- does not offer `stable_resource`; and
- leaves Guard-off messaging unchanged and proof-free.

Any future adoption requires a new published Design candidate and coordinated sender, receiver, Management, ejabberd, and acceptance changes.
