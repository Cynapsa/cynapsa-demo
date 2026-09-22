# ADR 0008: Current-Membership Peer Lanes

- Status: Accepted
- Implementation status: Implemented by the private V2 wire migration and
  current-membership peer-lane authority model
- Scope: Mesh membership recovery, peer lifecycle, buffering, RPC outcomes,
  duplicate suppression, and application delivery
- Supersedes: the historical membership-continuity model in ADRs 0003 and 0006

## Context

The product rule is deliberately current-state based:

> Two authenticated users may communicate exactly when both are current members
> of the same mesh. Otherwise they may not communicate.

The Core does not assign meaning to the time at which a membership was created,
does not preserve historical membership continuity, and does not distinguish a
member that remained present from the same identity being removed and re-added
while another Core was offline. A freshly authenticated complete server
snapshot is authoritative at the instant it is installed.

RPC requests and responses are matched by their unpredictable correlation
identity. Duplicate suppression uses the authenticated mesh, authenticated
sender, and canonical message ID. Once a message passes independent validation
and integrity checks, a later copy with that key is dropped without comparing
content or digests. Trusted terminal identity remains process-local for 25
hours, one hour longer than the Mesh Server's maximum 24-hour exact-resource
mailbox replay window. Dedicated per-peer and global admission limits bound the
table independently of ordinary queue capacity; live and unexpired entries are
not evicted to make room.

## Decision

### Current membership is the only communication authority

The Mesh Server owns the complete current member set for one mesh. The Core
retains that set only while its exact server session and separately verified mesh are
authoritative. A complete snapshot from that session is the sole membership
capability used by the application plane.

On every fresh bind or new logical session, the server advertises the exact
versioned Cynapsa authority feature through root XEP-0030 service discovery.
After exact resource binding and XEP-0198 enablement, Core sends one bounded,
strictly correlated `disco#info` query to the authenticated bare server domain.
Only a response containing exactly one `urn:cynapsa:mesh-authority:1` feature
permits Core to request the complete current snapshot. The server keeps the
exact session unroutable until it has served the terminal page and Core does
not enable application delivery until the snapshot is installed. If Core
missed one or many mutations, it reconciles directly:

```text
removed = locally known or active peers - current server snapshot
allowed = current server snapshot
```

One update may add or remove any bounded number of members. The Core fences all
removed peers as one snapshot transition before it resumes any allowed lane.
If an identity was removed and re-added while the Core was offline, and is
present in the current snapshot, it is authorized now. Paused work for that
identity may resume; the design intentionally does not preserve the historical
removal boundary.

### No stale update after a fresh snapshot

Membership-change notices are live control wakeups, not durable messages.
They are authority controls, not application messages.
They are excluded from the exact-resource application mailbox, MAM, and the
Core application outbox. Within one resumable XEP-0198 logical session they
remain acknowledged stream traffic and are replayed after a transport break.
They never transfer to a fresh bind or a different logical session.

Core routes authenticated server authority controls to one dedicated bounded
session lane, independent of all peer lanes. Recognition of a valid
invalidating control atomically fences every peer before the control lane
performs snapshot I/O. Peer failure or backpressure cannot delay this lane;
control-lane overload fails closed.

Snapshot publication and live-update subscription are serialized at the
server. For one authenticated session the server installs the subscription and
queues the complete snapshot before it can queue a later live notice. The Core
processes those controls serially. An unexpected transport break suspends new
link establishment, restart, and Rank 2 publication but does not itself revoke
an already published Rank 1 link or destroy a resumable logical session. On
successful XEP-0198 resume, Core holds control-plane and Rank 2 work until
ejabberd's post-resume `<r/>` separates the retained server FIFO from one strict
current `resume-authority` result. Replayed results before `<r/>` are stale and ignored;
exactly one server-originated result after the separator is processed on the
control lane.
Only `ready` proves that preceding authority controls have been processed.
If none invalidated authority, the same membership capability resumes without
another discovery query or snapshot. If a replayed notice requires a snapshot, or if resumption
fails and a fresh bind is created, the old capability remains fenced and
complete synchronization is mandatory. A fresh session accepts no notice from
the retired logical session. These rules replace update revision numbers; if
the server cannot uphold them, the revision-free model is invalid.

### Membership authority lifecycle

Control authority and retained data authority have related states:

```text
Synchronizing: authenticated or recovering, complete snapshot not installed
Ready:         complete current snapshot installed; setup and data permitted
Suspended:     authority transport lost; new setup and Rank 2 blocked
DataReady:     prior exact snapshot may serve already published Rank 1 links
Fenced:        retained data and setup both blocked
```

Loss of the authoritative transport changes control from `Ready` to
`Suspended` before any new link publication. Existing Rank 1 links retain
`DataReady`; endpoint membership, the exact installed adapter, and its channel
binding are all rechecked for each use. Successful XEP-0198 resume may restore
control only after its strict authority result. Missing, duplicate, malformed,
stale-session, or `not-ready` results force a clean bind and complete snapshot;
the fresh synchronization hard-fences the old data capability before
replacement. An authenticated membership-change control enters
`Synchronizing` immediately and also hard-fences retained data. Atomic
installation of the complete snapshot changes it to `Ready`.

Ready or retained data authority is owned by the authenticated logical XMPP session and has no
local membership lease or wall-clock expiration. Core does not poll or renew a
healthy session's snapshot. Authority changes only through complete snapshot
replacement, setup-only transport-loss suspension, hard invalidation, or an
authenticated membership-change control. Message, transfer, mailbox,
deduplication, XEP-0198, and operational deadlines remain independent and
continue to apply.

Rank 2a, Rank 2b, and Rank 2c remain XMPP message-custody observations. They do
not represent these authority states and do not drive peer-lane lifecycle.

### One actor lane per active peer

Every active peer has one lazy, bounded logical lane, independent of carrier:

```text
peer lane
  inbound messages and quarantine
  outbound pending messages
  outbound RPC requests
  inbound RPC requests and reply handles
  payload/file/control work
  current Rank 1 and Rank 2 attempt ownership
```

Rank 1, Rank 2 live delivery, Rank 2 mailbox delivery, and replay all enter the
same lane. A lane is created only for a peer with active work; installing a
large membership snapshot does not create a goroutine or channel for every
member. Its key is the exact canonical mesh plus peer identity.

One goroutine owns each active lane's mutable state and serializes ordinary
lane commands. Its mailbox is count- and byte-bounded. All lanes also reserve
from one Core-wide count and byte budget, so many peers cannot multiply the
configured memory ceiling. Network or injected dependency I/O must not block
the lane owner indefinitely; admitted external work runs under cancellable
ownership and returns through a stale-safe capability.

Lane state is:

```text
Active -> Paused -> Active
Active -> Removed -> Closed
Paused -> Removed -> Closed
```

The membership owner installs an immediate authority fence outside the lane
mailbox before enqueueing a remove command. Therefore ordinary work already in
the mailbox cannot publish after removal merely because it precedes the remove
command in FIFO order.

### Paused authority buffers but does not publish

While membership authority is `Paused` or `Synchronizing`, an already
transport-authenticated peer message may enter that peer's bounded quarantine.
It is not delivered to the SDK and cannot complete an outbound operation.
Unknown or unauthenticated identities, invalid exact resources, malformed data,
and work exceeding per-peer or global capacity still fail immediately.

Locally submitted outbound messages, requests, replies, and payload work may
also enter a bounded paused lane for a canonical peer identity, but they perform
no carrier I/O and cannot report success until the complete snapshot resolves
that identity. Caller cancellation, operation expiry, and configured delivery
or RPC deadlines continue while the lane is paused.

Pending RPC deadlines continue to run while paused. An RPC that reaches its
caller-configured deadline returns `rpc_timeout`. After snapshot installation:

- work for a current member resumes;
- work for an absent member is destroyed;
- each pending outbound RPC to an absent member completes exactly once with
  `authorization_rejected`;
- each inbound request and reply handle owned by an absent member is invalidated;
- a later SDK reply to that member returns `authorization_rejected` without I/O.

### Removal is peer-lane terminal cleanup

For every peer absent from an installed snapshot or removed by a live update,
the Core first fences inbound publication, outbound admission, and successful
completion. It then terminally resolves RPCs, zeroes and releases every owned
message, payload, correlation, handle, receipt, retry, and file/control value,
and closes the peer's transport through one exact close owner. Concurrent
transport failure, removal, logout, and process shutdown join that owner and do
not close the peer twice.

A complete snapshot does not close a healthy Rank 1 transport for a peer that
is still present. The global authority fence remains immediate; recovery then
cancels and joins that retained adapter's old receive loop, discards any
adapter-owned old-epoch ingress, and installs a fresh receive loop behind the
exact unpublished authority epoch. Publication occurs only after every retained
adapter has completed that bounded rebind. A noncooperative receive leaves
authority fail-closed when the recovery deadline expires.

An inbound message is checked immediately before SDK publication. When
authority is `Ready`, a sender absent from the current member set is silently
dropped and its owned bytes are cleared. An outbound message, request, reply,
or file operation is checked at admission and again before success; an absent
destination returns `authorization_rejected`.

If a successful RPC response is queued but not consumed when removal wins, the
Core clears it and replaces it with `authorization_rejected`. A result already
consumed by the SDK cannot be retracted. Removal, response completion,
cancellation, and timeout share one terminal-winner boundary.

### Logical-agent replicas and exact-session removal

One logical agent may have multiple current exact sessions in the same mesh.
The topology groups those sessions by the agent's bare identity and keeps each
opaque resource as an independent peer lane. One-way `message.send` reserves
local capacity for the eligible batch atomically and attempts every current
healthy session, even when an earlier session fails. It reports success only
after all selected sessions accept local responsibility. Each session has its
own private message and conversation identifiers; the stable first session's
identifiers form the one public receipt after complete success.

Cross-session publication is not a distributed transaction. If one session
accepts and another fails, the command returns an aggregate failure after a
partial external effect. That is an uncertain outcome, and blindly replaying
the semantic action can duplicate work. The failed session's conversation
remains fenced, while later logical sends still attempt healthy
siblings and continue returning aggregate failure until current authority
removes or replaces the fenced session. Automatic exactly-once failover needs
an application idempotency key plus a distributed claim protocol and is outside
V1.

`message.request` selects the lexicographically first current session for a new
request. A later new request selects the next session after authoritative
removal. Core never replays an ambiguously delivered request to a sibling,
because doing so could duplicate side effects. Replies target the exact session
that issued the request.

### Exact server-session removal

Removing a member terminates only resources attached to that mesh. A legacy
password session has this full identity:

```text
agent-a@mesh.example/<mesh-id>
```

An e2 token session instead has a registered installation resource:

```text
agent-a@mesh.example/r2.<installation-uuid>.<nonce>
```

It does not globally log out the account or terminate sessions for other
meshes. The server commits membership removal before later routing, rejects a
future bind of a resource attached to the removed mesh, and removes that member's
mesh-scoped offline messages, pending RPC traffic, resumable delivery state,
temporary file transfers, and references. Shared content owned by others is
deleted only when no authorized reference remains.

## Implementation requirements

The coordinated implementation:

1. preserve stable message IDs, correlation IDs, expiry, integrity, bounded
   dedupe, and exact-once terminal ownership;
2. use the bounded lazy peer-lane actor and global aggregate budget;
3. implement server snapshot-before-traffic recovery and non-durable serialized
   live membership controls;
4. implement batch peer reconciliation, paused quarantine, RPC timeout while
   paused, authorization failure on absence, and exact-once peer teardown;
5. update golden vectors, schemas, fixtures, docs, and E2E tests together.

## Acceptance evidence

Required deterministic and race-enabled tests include:

- one update removing multiple active peers atomically;
- reconnect after missing multiple updates, with direct reconciliation against
  one complete current snapshot;
- remove and re-add while another Core is offline, proving current-membership
  semantics resume the now-authorized identity;
- proof that membership controls are not stored or replayed across sessions and
  that snapshot subscription cannot leave an update gap;
- paused inbound quarantine, bounded per-peer/global pressure, and resume or
  zeroizing removal after the snapshot;
- pending RPC survival until its configured timeout, authorized resume, absent
  peer authorization failure, reply-handle invalidation, and one terminal winner;
- late Rank 1 and Rank 2 input dropped after the external authority fence even
  when ordinary lane commands were already queued;
- one active-lane goroutine per active peer, no eager goroutine per snapshot
  member, fair progress, idle retirement, and joined shutdown;
- exact-once peer transport close across removal, failure, logout, and Core close;
- snapshot removal closing each absent Rank 1 transport once while retained
  transports remain open, reject old-epoch ingress, and resume receive only
  after exact-epoch publication; a noncooperative receive must keep the fence
  closed without delaying the initial fence;
- duplicate carrier attempts invoking the SDK at most once.

## Consequences

- Authorization is simple and strict: current same-mesh membership permits
  communication; absence denies it.
- Re-addition intentionally restores authorization for paused work associated
  with the same identity; historical membership discontinuity is irrelevant.
- Temporary server loss pauses bounded work rather than prematurely declaring a
  peer unauthorized.
- The actor lane makes peer removal and cleanup local and efficient while the
  Core-wide budget prevents per-peer resource multiplication.
