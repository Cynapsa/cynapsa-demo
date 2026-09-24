# Sender flow and final current-authority fence

## Immutable preparation

The sender must freeze one transfer object before Guard I/O. It owns exact payload bytes/descriptor, logical fields, event ID, source/destination identities, both installation IDs/epochs, both session generations, authority epoch/sequence/digest/freshness, policy revisions/validity, inspection/profile inputs, logical digest, sender key ID/epoch, and `ms1`.

Any mutation after digest/signature creation invalidates the operation. Retry and carrier fallback reuse the validated immutable logical transfer and proofs; they do not regenerate message IDs, event IDs, payload encodings, `ms1`, or `ga1` opportunistically.

## Ordered sender path

1. Apply existing deterministic sender authorization and payload validation.
2. Determine effective Guard requirement from authenticated source-egress and destination-ingress authority. Missing configuration is not an off transition for a previously enforced installation.
3. If effectively off, enter the unchanged current flow with zero Guard/provider activity.
4. If enforce/observe requires evaluation, verify finite-fresh authority, capability, negotiated V3 support, installation-key state, session generations, and g1 scope.
5. Freeze canonical application bytes and logical-V2 envelope.
6. Compute `LogicalMessageDigest`, construct/sign `ms1`, and create one exact Guard request.
7. Release all unrelated locks and call Guard with a bounded absolute deadline.
8. Strictly parse the stored decision response. Enforce requires ALLOW and one exact valid `ga1` for every binding. Observe evidence never authorizes enforced delivery.
9. Re-enter the owning transfer lane and run the final current-authority fence.
10. Publish only if the fence succeeds, then preserve identical V3 material over every allowed retry/carrier path.

## Final fence

Immediately before first publication, compare the frozen inputs to current authenticated state. At minimum reject if any of these changed or expired:

- source or destination membership/eligibility;
- source or destination installation ID/epoch/key status;
- source or destination full resource/session generation;
- authority epoch, committed/applied sequence constraints, snapshot digest, or freshness deadline;
- source-egress or destination-ingress policy revision/validity/effective mode;
- negotiated protocol floor/capability;
- payload/logical digest, sender proof, decision, or attestation validity;
- local transfer cancellation, shutdown, peer replacement, or ownership generation.

The fence is a comparison against current authenticated local/transport authority, not a synchronous Management call. If current authority cannot prove the frozen decision still applies, fail closed and restart only as a new contract-permitted evaluation. Never publish first and fence later.

## Carrier selection and fallback

Carrier selection happens after semantic authorization but may prepare noncontent route state earlier. A fallback may reuse an attestation only when the logical and exact binding claims remain identical and the new carrier preserves V3 proofs. Reconnect, new destination session generation, installation replacement, policy revision, authority change, or expiry requires a new transaction.

Enforce never falls back to proof-free V2 or deterministic-only delivery. If no proof-capable route exists, return the pinned bounded error.

## Concurrency acceptance

Future tests must race Guard completion against membership removal, key revocation, session reconnect, policy change, authority expiry, transfer cancellation, shutdown, outbox retry, and fallback. They must prove no publication after a failed fence, no duplicate publication, no lock held across Guard I/O, no capacity leak, and one terminal result.
