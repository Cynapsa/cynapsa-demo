# Pod 4: Connectivity

Status: current implementation and qualification scope

Parent plan: [Cynapsa Go Core Development, Validation, and QA Orchestration](PROJECT_DEVELOPMENT_VALIDATION_QA.md)

## Mission

Implement private authenticated connectivity, preferred-path establishment,
health evaluation, fallback, XMPP recovery, exact ownership, and deterministic
fault injection while keeping every networking detail outside public surfaces.

## Owned implementation areas

```text
internal/transport/**
internal/peer/**
internal/handshake/**
```

Payload serialization and transfer state belong to Pod 5. Envelope identity,
dedupe, RPC, membership policy, peer lanes, and the outbox belong to Pod 3.
Connectivity consumes those private interfaces and cannot add SDK-visible
carrier state.

## Required behavior

### Carrier selection and replay

- Rank 1 is a WebRTC data channel authenticated through same-mesh signaling
  and DTLS channel binding.
- Rank 2 is the authenticated exact-resource XMPP session, including live
  delivery and server mailbox custody.
- Core prefers a healthy Rank 1 path and uses Rank 2 after a hard failure or
  when no usable direct path exists.
- An ambiguous attempt retains the original logical message in the shared
  outbox and reuses its identity on fallback.
- Rank 1 success requires exact authenticated receiver-Core receipt evidence.

### Peer and control ownership

- One peer actor owns peer-scoped path decisions and delegates blocking I/O
  under cancellable capabilities.
- Authority controls use a dedicated bounded session lane independent of peer
  state and peer backpressure.
- Delayed callbacks must match the exact live owner before changing state or
  publishing data.
- Peer removal, logout, transport failure, and shutdown converge on one close
  owner.

### XMPP lifecycle

- Fresh sessions perform TLS, SASL, exact-resource binding, XEP-0198 setup,
  authority-feature discovery, complete snapshot installation, external
  service discovery, and server-time calibration before traffic is released.
- A transport break pauses peer publication and outbound carrier success.
- Successful XEP-0198 resume keeps the logical session only after the strict
  authority barrier and retained stream replay complete.
- Resume rejection, expiry, malformed authority evidence, or setup failure
  uses a clean session and a fresh complete snapshot.
- XEP-0198 handled counters and wire records are private transport mechanics.

### Rank 1 health

- Establishment is single-flight per peer with deterministic glare handling,
  deadlines, stale-attempt rejection, and bounded cooldown.
- Health uses progress evidence, suspicion thresholds, immediate hard-failure
  signals, and bounded recovery.
- Temporary lack of progress alone does not manufacture a hard failure.
- ICE configuration comes only from authenticated XEP-0215 discovery.

### Private payload carriers

- Healthy Rank 1 may carry bounded binary transfer frames.
- Rank 2 may carry bounded text-safe transfer frames below the configured XML
  stanza budget.
- Transfer frames remain private and enter Pod 5 reassembly only after exact
  sender, mesh, route, framing, and limit validation.

## Security and resource rules

- Concrete carrier names, states, endpoints, credentials, stanza data, ICE,
  Jingle, DTLS, and dependency errors cannot cross public models.
- Network operations have explicit cancellation and deadlines.
- Reconnect, retry, cooldown, peer, link, frame, queue, timer, and worker state
  is bounded.
- TLS, endpoint, DNS, redirect, credential, and secret-handling rules follow
  the accepted networking ADRs and security model.
- Test fault injection is unreachable in production composition.

Narrow V1 exception (2026-08-24): dependency panic and error containment
remains active except for panic containment at exactly
`xep0363.SlotRequester.RequestSlot` and
`xep0363.Resolver.LookupIPAddr`. Their ordinary error normalization, context precedence, result ownership, validation, cancellation, and shutdown
requirements remain active. All
other connectivity dependency callbacks and goroutines remain subject to
panic containment, bounded errors, and resource release. See
[`AZTM_FUTURE_ISSUES.md#11-private-xep-0363-and-object-dependency-panic-containment`](../../AZTM_FUTURE_ISSUES.md#11-private-xep-0363-and-object-dependency-panic-containment).

## Builder tests

Tests cover:

- manager start, selection, send, receive, transition, and close;
- establishment, simultaneous initiation, stale attempts, cooldown, and
  replacement;
- progress, temporary interruption, hard failure, fallback, and recovery;
- ambiguous Rank 1 send followed by Rank 2 replay with unchanged identity;
- exact receipts and forged, late, duplicate, or stale receipt rejection;
- XMPP authentication, reconnect, successful resume, rejected resume, mailbox
  catch-up, and clean-session recovery;
- strict authority barrier ownership and XEP-0198 accounting;
- direct and XMPP transfer framing, duplicates, missing frames, conflicts,
  cancellation, expiry, and backpressure;
- blocked dependency calls during removal and shutdown;
- Dependency panic and error containment.
- Repeated creation, reconnect, replacement, and teardown under the race
- repeated creation, reconnect, replacement, and teardown under the race
- repeated creation, reconnect, replacement, and teardown under the race
  detector.

## Independent validation and QA

Validation reviews state ownership, authenticated provenance, replay identity,
resource bounds, security policy, and public abstraction containment.

Independent QA uses randomized peer-state events, network loss and
duplication, delayed packets, partial writes, blocked reads, resets, resume
rejection, relay-only routing, XMPP-only routing, long reconnect loops, and
secret-canary scans. Packet/frame arrival tests concern transport mechanics and
must not introduce new application fields.

## Completion gates

- Normal and race-enabled connectivity tests pass.
- Real ejabberd and Coturn tests cover fresh login, resume, mailbox, direct,
  relay, XMPP-only, failure, and recovery paths.
- No stale callback can revive retired authority or transport ownership.
- Fallback preserves logical identity and receiver dedupe prevents duplicate
  application invocation.
- Public-boundary scans contain no private connectivity data.
