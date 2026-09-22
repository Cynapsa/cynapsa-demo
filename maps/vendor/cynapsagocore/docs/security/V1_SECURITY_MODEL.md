# Cynapsa private V2 security model

This document is the authoritative confidentiality and trust-boundary statement for the
Cynapsa Go Core. Architecture notes, tests, release material, and SDK
documentation must use the same model.

## Accepted current architecture

Current membership in the server's complete freshly installed mesh snapshot
is the sole application communication authority. Client-supplied revision,
watermark, timestamp, and historical-incarnation claims are not authority.

Every authenticated carrier enters one bounded actor lane for the exact peer.
A transient loss of the authority server suspends new Rank 1 establishment,
restart, discovery, and Rank 2 publication. An already published Rank 1 link
retains its exact membership and channel-binding capability and may continue
carrying application traffic until a hard authority fence. The mesh scope is
verified separately and never inferred from its resource. A fresh complete
snapshot reconciles current peers and terminally removes absent peers,
including their messages, RPC state, payload transfers, references, receipts,
retries, and transport ownership.

## Confidentiality boundaries

| Path | Protection | Infrastructure visibility |
| --- | --- | --- |
| Rank 1 WebRTC RTCDataChannel | Authenticated peer-to-peer DTLS bound to the exact authenticated Jingle/DTLS transcript | Signaling, STUN, and TURN cannot recover RTCDataChannel plaintext; they observe metadata and DTLS ciphertext |
| Rank 2 ordinary XMPP messages and controls | XMPP over mandatory TLS plus authenticated exact-resource routing | The XMPP/ejabberd service can see plaintext and routing metadata; this is not end-to-end encryption |
| Rank 2 XEP-0363 object transfer | HTTPS transport protection and an unguessable bounded reference | The HTTP upload/download service can see payload plaintext; references remain private Core data |
| Rank 2 XMPP fallback chunks | XMPP over mandatory TLS plus authenticated exact-resource routing | The XMPP/ejabberd service can see payload plaintext and routing metadata |
| SDK/Core command boundary | Process-local typed commands and owned buffers | Private carrier details never enter the SDK schema or native ABI |

Rank 1 is therefore **infrastructure-confidential** in V1.
Rank 2 is **transport protected but server visible** in V1.
The stored V1 object is payload plaintext protected in transit by HTTPS.
The V1 chunk body contains payload plaintext.
`IdentityKeyProvider` and production payload E2EE are future work.
The public V1 `large_payloads` capability means bounded multi-carrier,
not end-to-end encrypted large-payload delivery.

Login passwords are used only by the XMPP authentication path. They are never
used as payload keys, per-envelope proofs, policy inputs, logs, diagnostics, or
SDK values.

## Enrollment-token authentication

The additive token commands accept only an exact bounded
`cpsa_e1.<uuid>.<secret>` token and mesh ID. Core strips exactly the `cpsa_`
prefix at its public boundary and uses the canonical body with a freshly
generated installation ID and installation secret. Its private v2 client sends
the exact full token to the fixed HTTPS enrollment service. The service response
is strict, bounded, non-cacheable, and must bind the requested mesh, generated
installation, authenticated bare identity, and server-issued session resource.

The returned endpoint, username, access-token JWT, wrapper, and display
metadata remain private. Display has no authentication meaning. Token sessions
present the access token through SASL PLAIN only after verified TLS 1.3;
password sessions retain their unchanged SCRAM profile. Exact resource binding
and complete current membership remain mandatory. See
[private integration details](../development/ENROLLMENT_TOKEN_LOGIN.md).

## Membership and carrier authentication

For an e2 token session, the server binds only the registered full identity
`agent@domain/r2.<installation-uuid>.<nonce>` and separately maps that resource
to its authorized mesh attachment. Legacy password sessions use the mesh ID as
their resource. Removing a member terminates the affected exact resources,
rejects a later unauthorized bind, and purges that member's mesh-scoped stored
messages, RPC traffic, resumable delivery state, temporary file work, and
references. It does not log out unrelated resources for the same account or
resources attached to another mesh.

After authentication or recovery, application traffic stays blocked until the
Core atomically installs and publishes one complete current snapshot from that
exact server session. Installed authority has no local lease and is not
periodically refreshed while the logical session remains healthy. Membership
controls are session-serialized and excluded from application offline storage;
unacknowledged controls remain XEP-0198 stream traffic and replay only when the
same logical session resumes. A stale-session callback carries only a
process-local capability and cannot publish into the new session.

Control readiness and retained data authority are separate. Transport loss is
not authenticated revocation evidence. During that partition, Core cannot
learn a new server-side removal, so continuing an existing Rank 1 link accepts
revocation-detection delay. An authenticated membership-change control,
fresh-snapshot removal, changed local identity, logout, shutdown, or the
configured expiry behavior hard-fences retained links. A successful fresh
bind also fences the old snapshot before installing its replacement. New links
cannot be published while control is suspended.

Rank 2 provenance is the authenticated outer XMPP sender, exact recipient
session, and separately verified mesh. Rank 1 provenance is the authenticated same-mesh signaling
transcript plus the DTLS channel binding. Envelope-supplied identity never
authenticates itself.

## Admission and removal rules

- Inbound work is checked against current membership immediately before SDK
  publication. An absent sender is dropped and owned bytes are cleared.
- Outbound work is checked at admission and again before success. An absent
  destination returns `authorization_rejected`.
- While control authority is suspended, new Rank 1 setup, Rank 2 traffic, and
  authority updates remain blocked. Existing exact Rank 1 links may use the
  retained data authority. During hard fencing or snapshot synchronization,
  authenticated work may use only bounded per-peer quarantine plus the
  Core-wide count/byte budget. Deadlines continue; no SDK publication or
  carrier success is reported.
- Removing a peer sets an external fence before its lane command, cancels
  pending work, invalidates request/reply handles, replaces unconsumed RPC
  success with authorization rejection, zeroes owned payloads, and joins one
  exact transport-close owner.
- Stable message IDs scoped by authenticated mesh and sender provide bounded
  duplicate detection across Rank 1, Rank 2, retries, and ambiguous completion.
  Duplicate classification does not compare content. Logical and payload
  digests remain independent integrity checks.

## Private V2 envelope and payload integrity

The deterministic private V2 envelope contains stable message/conversation
identity, sender, recipient, mesh, mode, correlation/reply identity, creation,
request expiry, clock uncertainty, and a bounded payload descriptor. Legacy
reserved key positions are forbidden. Earlier and unknown versions fail
closed.

Payload manifests bind stable transfer and message identity, exact route,
profile, canonical size, digest, and expiry. Reassembly is bounded by per-peer
and global counts/bytes. Completion requires authenticated remote
materialization evidence matching the exact transfer, message, and digest.
Removing a peer cancels and zeroes its retained transfer state.

## SDK and operational boundary

SDK-visible results remain transport opaque. They never expose full private
resource identities, carrier/rank selection, Jingle/ICE/DTLS data, XMPP stanza
details, XEP-0363 URLs, session capabilities, private references, or internal
queue ownership. Logs and diagnostics use bounded normalized counters and
non-secret identifiers only.
