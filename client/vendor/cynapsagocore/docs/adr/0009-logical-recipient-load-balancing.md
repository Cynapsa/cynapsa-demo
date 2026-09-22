# ADR 0009: Logical-Recipient Load Balancing

- Status: Proposed
- Implementation status: Not implemented
- Scope: Logical-agent routing, installation load balancing, Rank 1 and Rank 2
  assignment, mesh control messages, offline work, and lane lifecycle
- Supersedes on implementation: the complete-snapshot authority model and the
  replica fanout/deterministic-selection rules in ADR 0008

## Purpose

Cynapsa users address a logical agent, not one of its installations. A user sends
to an identity such as:

```text
agent-b@connect.example
```

The user does not need to know whether Agent-B currently runs as B1, B2, B3, or
any other installation. Core and the Mesh Server distribute work among those
installations.

This document defines that target model. It is intentionally explicit enough
for both human implementers and AI coding agents. It does not describe the
current implementation. Until this ADR is implemented, ADR 0008 and the current
code remain authoritative.

## Example deployment

Assume one mesh contains four logical agents:

```text
Agent-A: A1, A2, A3
Agent-B: B1, B2, B3
Agent-C: C1
Agent-D: D1, D2
```

`Agent-A` is the logical identity. `A1`, `A2`, and `A3` are independently
running installations of Agent-A. Each installation has its own exact,
server-authorized address, for example:

```text
agent-a@connect.example/r2.<installation-uuid>.<nonce>
```

Applications send to the bare logical identity:

```text
agent-a@connect.example
```

Exact installation addresses remain private to Core and the Mesh Server.

## Terms

### Logical agent

The user-visible bare agent identity, such as `agent-b@connect.example`. Mesh
membership is defined for this identity.

### Installation

One independently running Core instance for a logical agent. An installation
has one exact server-authorized endpoint while its authenticated session is
active.

### Macro-lane

One local `RecipientLane` for a logical destination. Agent-A creates a
RecipientLane for Agent-B only when A needs to communicate with B or receives a
relevant authenticated control for an already-existing B lane.

The macro-lane owns the real operation storage, load-balancing state, and the
set of active installation micro-lanes.

### Micro-lane

One `EndpointLane` for one exact installation, such as B2. A micro-lane owns
endpoint-specific execution and transport state. It references operations in
the macro-lane; it does not own copied payload objects.

### Proposed installation

The installation selected by the source macro-lane for an operation. Rank 2
must continue preferring this installation while the Mesh Server still regards
its authenticated session as active.

### Fully offline

An installation is fully offline only when its authoritative XMPP session has
been closed, expired, replaced, revoked, or forcibly terminated after failing
the control-acknowledgement contract. P2P failure, missing Rank 1, handler
latency, or an ordinary stall does not make an installation fully offline.

An installation waiting to apply a control is `ControlPending`, not fully
offline. The server does not give it new Rank 2 assignments or new resolution
results until it proves that it applied every required control. Existing
proposed work remains reserved for it unless it becomes fully offline.

### Message identity

Every application MSG or RPC request has one stable message ID that survives
Rank 1 retries, Rank 2 retries, endpoint reassignment, and requester reconnect.
The effective deduplication key is the authenticated mesh, authenticated source
installation, and message ID. A retry must reuse the same ID; it must never
create a new ID merely because a transport or XMPP session changed.

Control messages use a separate unpredictable `control_id`. Application
message IDs and control IDs are different namespaces with different
acknowledgement rules.

## Core lane structure

The conceptual ownership model is:

```text
RecipientRegistry
  Agent-B RecipientLane
    operation storage
    ready operation references
    B1 EndpointLane
    B2 EndpointLane
    B3 EndpointLane
```

A possible Go shape is:

```go
type RecipientKey struct {
    MeshID  string
    AgentID string
}

type RecipientLane struct {
    operations map[MessageID]*Operation
    rpcWaiters map[MessageID]*RPCWaiter
    ready      deque[MessageID]
    endpoints  map[EndpointKey]*EndpointLane
}

type EndpointKey struct {
    FullIdentity       string
    SessionIncarnation string
}

type EndpointLane struct {
    assigned deque[MessageID]
}
```

The exact implementation may use different types, but it must preserve the
ownership boundary:

```text
inline payload bytes, or a large-payload descriptor and object lease,
live in RecipientLane operation storage

EndpointLane holds only operation IDs or owned references
```

Assigning an operation to B2 must not clone the application payload into B2's
lane. Internal transport serialization may require bounded buffers, but lane
assignment itself is a metadata change.

The first implementation gives every micro-lane one active assignment slot. An
idle micro-lane asks its macro-lane for work only when its bounded local
pipeline can accept another operation. The representation may allow more slots
later without changing operation ownership.

RPC waiters belong to the macro-lane. They must not be stored in the selected
micro-lane because an operation may move from B1 to B2 without changing its
logical request or the SDK call waiting for the result.

## Operation storage and assignment

An operation is admitted from the SDK in installation-neutral form. Core must:

1. copy or freeze SDK-owned input;
2. validate and canonicalize the supported payload;
3. reserve bounded Core-owned count and byte capacity;
4. assign a stable logical operation identity and applicable deadline; and
5. place the operation in the destination macro-lane.

The operation does not receive an exact destination installation until the
selector assigns it to a micro-lane.

A conceptual operation record is:

```go
type Operation struct {
    MessageID           MessageID
    LogicalRecipient    string
    Mode                Mode
    Payload             OwnedCanonicalPayload
    ApplicationPath     string
    Deadline            time.Time
    ProposedEndpoint    EndpointKey
    TransportAttemptID  string
    State               OperationState
}
```

Useful internal states include:

```text
Ready
Assigned
ServerAccepted
DestinationAccepted
Completed
Expired
```

An unknown final delivery observation is not an SDK error or a required routing
state. Future telemetry may record that an expired operation's final delivery
is unknown, but RPC still returns its ordinary timeout and routing state is
released normally.

An assignment transfers exclusive attempt authority to one micro-lane, but the
macro-lane remains the operation's storage owner until ownership is explicitly
transferred to the Mesh Server or the operation terminates.

If B2 becomes fully offline, locally owned operations assigned to B2 have their
assignment cleared and their references are moved to the front of Agent-B's
macro-lane ready queue. Moving them to the front is an internal scheduling
choice. Application-level sequencing lies outside this routing ADR.

The selector must avoid immediately returning a failed operation to the same
failed endpoint. A flapping installation must not monopolize the queue.

## Macro-level RPC waiters

Suppose A1 sends an RPC to Agent-B:

```text
A1 Agent-B macro-lane creates waiter op-123
  -> selector proposes B1
  -> B1 becomes fully offline
  -> operation op-123 is assigned to B2
  -> B2 responds
  -> the same Agent-B macro-lane waiter completes
```

The stable logical operation ID, correlation ID, and caller deadline survive
every endpoint reassignment. Each Rank 1 or Rank 2 delivery attempt has a
separate private attempt or lease identity. A micro-lane owns only its current
attempt; it never owns RPC completion.

The macro-lane accepts a response only when the response proves a legitimate
attempt for that operation:

- for Rank 1, the exact authenticated endpoint and attempt must be in the
  macro-lane's bounded attempt ledger; or
- for Rank 2, the Mesh Server must have authenticated the current or terminal
  assignment lease before proxying the response.

Membership in Agent-B alone is not enough to answer any Agent-B waiter. This
prevents an arbitrary B installation from injecting a response into an
operation it was never assigned.

The first valid terminal result wins. The macro-lane removes the waiter, ends
remaining attempts where possible, releases operation storage, and silently
drops later responses with optional future telemetry. The original RPC timeout
starts at SDK admission and continues through local queueing, Rank 1 attempts,
Rank 2 server ownership, and endpoint reassignment.

Destination failover and requester failover are different:

```text
B1 -> B2 destination failover: supported by A1's same macro-level waiter
A1 -> A2 requester failover:   not supported by process-local SDK state
```

An RPC initiated by A1 must still complete at A1. A2 cannot resolve A1's local
SDK future unless a separate future design introduces shared requester state.

## Lazy recipient resolution

Core does not preload a lane for every member or installation in the mesh.

When A1 wants to send to Agent-B and does not have an active Agent-B macro-lane,
the registry atomically creates or returns one empty Agent-B macro-lane in
`Resolving` state. Creating this provisional macro-lane records that Agent-B is
now relevant before any server request can race with a control. Concurrent SDK
operations join the same lane and the same single in-flight resolution; they
must not create competing lanes or duplicate resolution requests. Core then
asks the authenticated Mesh Server to resolve:

```text
resolve_recipient(agent-b@connect.example)
```

The server derives the mesh from A1's authenticated session. It must not trust
a caller-supplied mesh identifier. It returns one of:

```text
authorized, endpoints = [B1, B2, B3]
authorized, endpoints = []
authorization_rejected
```

The outcomes initialize or close that provisional macro-lane:

- `[B1, B2, B3]`: initialize the existing provisional macro-lane with those
  micro-lanes, then apply newer buffered controls;
- `[]`: Agent-B is a member but has no active installation; keep the macro-lane
  empty and offer bounded work to Agent-B's Rank 2 logical mailbox as
  unassigned work;
- `authorization_rejected`: Agent-A and Agent-B are not currently allowed to
  communicate; close the provisional lane and fail every joined operation with
  the canonical authorization error.

A temporary resolution transport failure is not authorization rejection. Core
retains bounded, unexpired joined operations while restoring control authority
and retries one resolution. Their original deadlines continue to run.

Core must not attempt Rank 1 establishment with an endpoint that has not been
returned by the authenticated server or admitted by an authenticated control.

### Race-free first resolution

Resolution is part of macro-lane creation, not an action that happens before
macro-lane creation. The following is a required invariant:

```text
create provisional RecipientLane(Agent-B, Resolving)
    -> send resolve_recipient(Agent-B)
    -> initialize from the authoritative result
    -> apply buffered controls newer than that result
    -> publish RecipientLane(Agent-B, Ready)
```

As soon as the provisional lane exists, Agent-B is relevant to C1. Therefore C1
must authenticate, retain, and acknowledge controls for Agent-B while the
resolve operation is in flight. It must never send `resolve_recipient` first and
wait until the response arrives to create the lane.

The resolve result must carry the Mesh Server authority revision at which its
endpoint set was read. Relevant controls carry a revision from the same ordered
authority stream. A revision consists of:

```text
revision_cycle_id + recipient_revision
```

`revision_cycle_id` is a fixed-size server-generated value. A 64-bit random
value is sufficient for the first implementation. `recipient_revision` is an
unsigned counter for one logical recipient within that cycle. The server starts
a new cycle and resets counters to zero after a server restart or before a
counter wraps.

Within one cycle, a Core with an existing or resolving recipient lane handles
revisions as follows:

- the next contiguous revision is applied;
- the same or an older revision is an acknowledged duplicate or stale event;
- a future revision with a gap fences that recipient lane until an
  authenticated resolution repairs its state; and
- a different cycle fences affected cached state and requires authenticated
  resolution before publication continues.

The resolving lane buffers controls and, when the result arrives, installs the
result and then applies contiguous buffered controls newer than the result. A
gap must not be guessed across. This prevents both an `installation_added` from
being omitted and an `installation_offline` from being undone by a stale
resolve result.

For example, an installation can become fully offline after the server reads
the endpoint set but before Core receives the resolve result:

```text
server reads Agent-B endpoints [B1, B2] at revision 10
B2 becomes fully offline
server publishes installation_offline(B2) at revision 11
C1 receives the revision-11 control while Agent-B is Resolving and buffers it
C1 receives resolve result [B1, B2] at revision 10
C1 installs [B1, B2], then applies revision 11
final active endpoint set: [B1]
```

Without the revisions and buffered replay, the older resolve result could
incorrectly recreate B2 after C1 had already learned that B2 was offline. The
same rule handles the opposite race, where an installation is added after the
server reads the endpoint set.

The valid outcomes are:

- a control applied before the resolve response is reflected by the response or
  retained and applied during publication;
- a control sent after the response is processed after publication; and
- `member_removed` always wins over a concurrent or late successful resolve and
  prevents that result from recreating the macro-lane.

Core acknowledges a control only after the resolving lane owns its effect. This
does not require a complete mesh snapshot. It requires only the ordered
authority revision used by recipient resolution and control publication, plus
the per-recipient resolution barrier described above.

Removal and later re-addition do not require a separate historical membership
epoch. `member_removed` closes the macro-lane and discards its buffered controls.
Re-addition restores only current authorization; a later send creates a fresh
provisional lane and resolves current state. Controls from an old XMPP/control
session are never accepted by a new session.

## Load-balancing selection

For each new SDK `send` or `request` to Agent-B, Agent-A's macro-lane selects one
available B installation. One operation is handled by one installation attempt
at a time. `send` does not fan out to every installation.

Selection should be completion-driven:

```text
B1 becomes available -> asks Agent-B macro-lane for work
B2 becomes available -> asks Agent-B macro-lane for work
B3 becomes available -> asks Agent-B macro-lane for work
```

Faster installations naturally request more work. When multiple installations
are simultaneously available, the selector must use a rotating, randomized,
or operation-ID-based starting point. It must not always select the
lexicographically first endpoint, because independent A1, A2, and A3 senders
would otherwise concentrate work on B1.

The selected installation becomes the proposed installation for the operation.

The source selector and the server selector both use completion-driven pulls,
but they own different queues. Source selection proposes an installation.
Server selection later chooses only among operations eligible for the pulling
installation, as defined by the Rank 2 queue model below.

## Rank 1

If A1 selects B2 and an authorized Rank 1 link to B2 is usable, A1 sends the
operation directly to B2.

```text
A1 Agent-B macro-lane
  -> proposes B2
  -> B2 micro-lane
  -> Rank 1 direct transport
  -> B2
```

The exact endpoint remains subject to current server-session authority. A local
P2P failure does not remove B2. It causes transport fallback while B2 remains
the proposed installation.

The XMPP server permits Rank 1 signaling only between installations mapped to
the same authenticated mesh. Rank 1 provenance is then bound to that authorized
signaling transcript and the resulting DTLS channel, not to sender fields in an
application envelope. Once established, Rank 1 application traffic bypasses
the server. Consequently, `member_removed(Agent-A)` must fence and close
existing inbound and outbound Agent-A Rank 1 state at peers; server routing
policy alone cannot revoke an already-established direct channel.

## Rank 2 and the logical server mailbox

The Mesh Server is a transport hop and process-lifetime owner, not the application
recipient. For flexible offline routing, Rank 2 cannot use a permanently fixed
per-resource mailbox as its only storage model. It needs one logical mailbox
per organization, mesh, and logical recipient:

```text
ServerRecipientMailbox
  key = organization + mesh + Agent-B
  operation storage
  unassigned references
  reserved[B1] references
  reserved[B2] references
  reserved[B3] references
  at most one active lease per installation
  next-queue selector state per installation
```

Payloads are stored once in logical operation storage. The unassigned,
reserved, and leased collections contain only message-ID references. Moving an
operation between them must not copy its payload.

Each `reserved[Bn]` collection is the server's internal installation mailbox
for that active installation. It is not an XMPP per-resource offline mailbox
and it disappears into `Unassigned` when that installation becomes fully
offline.

Cynapsa operations must be stored directly in this logical mailbox. They must
not first be committed to ordinary per-resource XMPP offline storage for B1,
B2, or B3. Raw per-resource offline stanzas cannot be redistributed safely
without duplicating ownership, changing authenticated routing metadata, or racing
a reconnecting installation. Standard XMPP offline storage may continue serving
unrelated traffic, but it is not the source of truth for Cynapsa operations.

When A1 falls back to Rank 2 for an operation proposed to B2, it sends:

```text
logical recipient = Agent-B
proposed installation = B2
source installation = A1
stable message ID and bounded operation data
```

The server follows this rule:

> Route to B2 while B2 is not fully offline. Select another B installation only
> after B2 is fully offline.

Therefore:

- B2 has no P2P connectivity but its XMPP session is active: route through
  Rank 2 to B2;
- B2 is slow or an RPC handler is stalled: retain B2 as the target until the
  operation expires or B2 becomes fully offline;
- B2's XMPP session closes: return unfinished B2 leases to Agent-B's logical
  unassigned queue and let B1 or B3 pull them;
- B1, B2, and B3 are all offline: retain the operations unassigned in Agent-B's
  logical mailbox;
- B4 later becomes ready: B4 asks for work and begins draining Agent-B's
  mailbox one assignment at a time.

### Reserved, unassigned, and leased work

The Rank 2 routing states are:

```text
Reserved(B2) -> waiting specifically for B2
Unassigned   -> any valid active B installation may pull it
Leased(B2)   -> B2 owns the one active server attempt
```

If proposed B2 is still active, an admitted operation enters `Reserved(B2)`.
This remains true while B2 is temporarily busy or `ControlPending`; those
states do not make B2 fully offline. B1 and B3 may not take B2-reserved work.

If the proposed installation is already fully offline or no installation was
proposed, the operation enters `Unassigned`. This includes work accepted while
Agent-B has no active installation.

An installation pulls work only when all of the following are true:

- its exact session is `Valid`, not `ControlPending` or fully offline;
- it has no active Rank 2 lease; and
- its bounded local Core/SDK pipeline can accept another operation.

The first implementation has one active lease per installation. It uses this
alternating selector whenever an installation asks for work:

1. start by preferring that installation's own reserved queue;
2. on the next successful pull, prefer the global unassigned queue;
3. continue alternating between those two classes;
4. if the preferred class is empty, immediately try the other class; and
5. after success, next prefer the class opposite the one actually served.

For example, B2 normally pulls:

```text
B2 reserved -> unassigned -> B2 reserved -> unassigned
```

Only B2 may pull `Reserved(B2)`. Every valid B installation may pull
`Unassigned`, so faster installations naturally drain more unassigned work.
The server, not the installation, atomically selects the exact message and
creates its lease; two installations can never successfully lease the same
reference.

This is a pull protocol. An installation announces Rank 2 readiness; the
server sends at most one assignment; and the installation does not announce
readiness again until its bounded local pipeline can accept another operation.
XMPP presence alone never means readiness.

When B2 becomes fully offline, the server atomically removes B2 from the active
consumer set and moves every `Reserved(B2)` reference plus B2's unfinished
`Leased(B2)` reference to the front of Agent-B's unassigned queue. B1, B3, and
any other valid installation then pull those operations through the same
alternating selector:

```text
op-1 Reserved(B2) -> front(Unassigned) -> B1
op-2 Leased(B2)   -> front(Unassigned) -> B3
op-3 Reserved(B2) -> front(Unassigned) -> next available B installation
```

This is dynamic draining, not a one-time static partition. An installation that
finishes sooner receives the next available operation. If every B installation
is offline, all operations remain unassigned until an installation becomes
available. Moving recovered work to the front is a scheduling choice;
application-level sequencing lies outside this routing ADR.

Logout does not delete mailbox work. It makes that exact installation fully
offline, returns its references to `Unassigned`, and leaves all unexpired
operations in Agent-B's logical mailbox. A later B installation, including a
fresh session for the same installation UUID, may pull whatever remains.

### Rank 2 ownership transfer

Payload ownership moves across processes only after a bounded Rank 2
server-acceptance transaction:

```text
source RecipientLane owns payload
  -> server validates and stores logical operation in RAM
  -> server returns server_accepted(message_id)
  -> source releases payload bytes
  -> server mailbox owns dispatch responsibility
```

The XEP-0198 packet acknowledgement proves only that the XMPP stream handled
the stanza. `server_accepted(message_id)` is the application acknowledgement
that proves the logical mailbox accepted it. Authorization failure, expiration,
or a full server mailbox returns an error instead, and the source retains
ownership.

Rank 2 admission is atomically deduplicated by the stable application message
ID scoped to its authenticated mesh and source installation. If the same
message ID is retried because `server_accepted` was not observed, the server
does not compare payload contents and does not insert another operation. It
discards the duplicate body and returns the existing operation's acceptance or
terminal status. Dedupe evidence remains until the operation deadline plus the
five-second retry safety window.

For RPC, the source retains only the bounded waiter metadata needed for the
response, timeout, and exact requesting SDK instance. The server owns the
payload after server acceptance.

Source-owned work is requeued by the source macro-lane. Server-owned work is
requeued by the server mailbox. Both sides must never independently requeue the
same payload after the server-acceptance boundary.

Server mailbox, lease, response, and dedupe state are RAM-only in the first
implementation. Logout does not clear them, but an ejabberd process restart may
lose accepted Rank 2 work. `server_accepted` therefore means accepted by the
current server process, not persisted across a server crash. Durable storage
and multi-node shared mailbox state are future work.

A large operation stores either inline bytes or the descriptor and object lease
defined by [ADR 0005](0005-multi-carrier-large-payload-transfer.md). Server
acceptance transfers responsibility for keeping that object usable through
completion or expiration. Reassignment reuses the descriptor and must not
upload or copy the file again.

### Rank 2 response routing

The server may replace B2 with B1 after B2 becomes fully offline. Consequently,
the Rank 2 RPC response cannot be required to come from the source's original
proposed exact endpoint.

The server validates the destination lease and proxies the response to the
exact requesting installation:

```text
B1 completes operation originally proposed to B2
  -> Mesh Server validates operation and assignment
  -> Mesh Server returns response to A1
  -> A1 resolves its local RPC waiter
```

A response to an RPC started by A1 must return to A1. It cannot be delivered to
A2 or A3 because the SDK call and waiter are local to A1. New requests to
Agent-A may be load-balanced among A1, A2, and A3; an existing response retains
requester affinity. Within A1, the response is published to the Agent-B
macro-lane waiter rather than to the B1 or B2 micro-lane that happened to carry
an attempt.

Requester logout does not delete an unexpired request or completed response.
The server retains it in RAM until the original RPC deadline. If the same A1
installation authenticates again before expiration, the server may deliver the
response to that installation. If A1's local waiter no longer exists, Core
silently drops the response. The server never redirects that response to A2.

Local SDK cancellation does not send remote cancellation in the first
implementation. It removes the local waiter, while server-owned work remains
eligible to execute until its absolute deadline. Any later result is silently
dropped at A1 and the server deletes the operation at expiration.

## Completion boundaries

The following observations are different:

```text
XEP-0198 packet ACK: server/client stream handled the stanza
server acceptance:   server stored the Rank 2 operation in its RAM mailbox
destination acceptance: destination Core owns the delivered MSG operation
RPC response:        destination handler produced a terminal response
```

An XEP-0198 acknowledgement alone is not an application completion signal.

For one-way MSG delivery, destination Core's automatic acceptance completes the
server lease after its bounded local pipeline owns the operation; receiving an
XMPP packet is not enough. For RPC, the lease remains active until response,
error, expiration, installation loss, or another terminal outcome. If the
assigned installation becomes fully offline first, the server may reassign
according to this ADR. An installation asks for another operation only when its
local pipeline is ready.

Each destination Core keeps bounded message-ID dedupe state across Rank 1,
Rank 2, and retries from the same authenticated sender. Duplicate
classification does not compare content. That local dedupe state cannot prevent
the at-least-once case where B2 executed work, its completion was lost, and the
server later assigns the same stable message ID to B1.

Automatic reassignment provides at-least-once execution. If B2 completed an
operation but became unreachable before its completion reached the server, B1
may execute it again. Exactly-once application side effects require application
idempotency or a future distributed claim/transaction contract; this ADR does
not claim to provide them.

## Mesh-wide control messages

Every control message is broadcast to every ready installation in the affected
mesh. Every receiver must authenticate, process, and acknowledge it. A Core
allocates new recipient-lane state only when the control is relevant to state
it already owns.

The control set is:

```text
member_removed
installation_added
installation_offline
```

Control messages belong to the dedicated high-priority control lane. Peer-lane,
SDK, payload, or handler congestion must not delay them.

Controls are session-scoped authority traffic. They are not written to the
logical application mailbox or ordinary XMPP offline storage. A pending control
may replay only through the same XEP-0198 resumable session and remains subject
to the 30-second action-ACK deadline. A fresh authenticated session resolves
relevant current state instead of consuming controls from an old session.

### Lazy relevance

Suppose Agent-A adds A3. The server broadcasts:

```text
installation_added(Agent-A, A3)
```

to B1, B2, B3, C1, D1, D2, and every other ready installation in the mesh.

- B1 currently has an Agent-A macro-lane, so B1 adds an A3 micro-lane.
- C1 does not communicate with Agent-A and has no Agent-A macro-lane, so C1
  acknowledges the control but creates no lane or goroutine.
- D1 also has no Agent-A macro-lane, so it acknowledges and ignores the local
  mutation.
- If C1 later sends to Agent-A, it calls `resolve_recipient` and receives the
  complete active endpoint set for Agent-A at that time.

The same relevance rule applies to all controls:

- `installation_added`: add the micro-lane only if the logical macro-lane
  already exists;
- `installation_offline`: remove the exact micro-lane only if it exists;
- `member_removed`: allocate no new macro-lane, but fence and destroy every
  existing local state involving that logical agent, including outbound lanes,
  inbound Rank 1 channels, reply authority, dedupe state, and retained
  operations.

Ignoring an irrelevant local mutation never means ignoring the control
protocol: the Core must still acknowledge the control.

## Two-level control acknowledgement and forced logout

Every control has two different acknowledgements:

```text
XEP-0198 packet ACK
  proves only that the exact XMPP stream handled the stanza

control_applied(control_id, revision_cycle_id, recipient_revision)
  proves that Go Core completed the required control operation
```

The packet ACK never makes a Core control-current. Each control has a bounded,
unpredictable `control_id` and requires the second, application-level action ACK
from the exact session. Core sends that ACK only after the local mutation is
complete, not merely after placing the control on an internal queue.

Both acknowledgements are required:

```text
Valid
  -> server emits control
ControlPending
  -> XEP-0198 packet ACK observed: remain ControlPending
  -> matching action ACK observed after application: Valid
  -> 30 seconds without the required action ACK: FullyOffline
```

If transport scheduling makes the action ACK observable before the separate
packet ACK, the server may record it but does not restore `Valid` until both
acknowledgements for that control are present.

For example:

```text
server -> C1:
  installation_offline(control_id, cycle, revision,
                       Agent-B, B2, session-incarnation)

C1:
  remove B2 if C1 has an Agent-B macro-lane
  requeue C1-owned B2 assignments
  record the contiguous applied revision
  send control_applied(control_id, cycle, revision)
```

For an irrelevant control, completion means authenticating it, validating its
control metadata, determining that no local state is relevant, and then sending
the action ACK. Core does not retain that recipient's revision or allocate a
lane or goroutine. A later first send obtains a current revision through
`resolve_recipient`.

When the server sends a control to C1, C1 enters `ControlPending`. During that
state the server withholds new Rank 2 assignments and new resolution results
from C1. XEP packet acknowledgement leaves C1 `ControlPending`. The server owns
a bounded pending-control ledger for that exact session. C1 returns to `Valid`
only after it action-acknowledges every outstanding required control. Existing
proposed work remains reserved; the server does not reassign it merely because
C1 is applying a control.

The action-acknowledgement window is exactly 30 seconds from server emission.
If C1 does not complete the required action ACK within that window, the server
must:

1. atomically remove C1 from the active installation registry;
2. requeue C1's unfinished server-owned assignments;
3. force C1's XMPP session closed;
4. broadcast `installation_offline(Agent-C, C1, incarnation)` to the mesh; and
5. require C1 to authenticate again before it can communicate.

A late packet or action acknowledgement from the retired session is ignored.
Pending control state is bounded by count, bytes, and the 30-second window so an
unresponsive Core cannot consume unlimited server memory.

This timeout must not be inferred from missing P2P or a slow application
handler. It applies only to the dedicated server control protocol. If a
control is pending during XEP-0198 suspension, it must still be action-acked
inside the same 30-second wall-clock window or the resumable session is forced
terminal. If no control is pending, ordinary XMPP/XEP-0198 liveness determines
when the installation becomes fully offline.

## Session-scoped authority without complete snapshots

This model does not install a complete mesh snapshot in every Core. Instead:

- `resolve_recipient` authoritatively initializes one relevant macro-lane;
- acknowledged mesh-wide controls keep active, relevant lanes current; and
- the endpoint cache is valid only while the exact control session remains
  authoritative.

If a resumable XMPP session preserves the complete control stream and all
required control acknowledgements, its cache may continue. If resumption fails,
the server forces a fresh authentication, or control continuity is otherwise
lost, Core must:

1. fence all communication immediately;
2. remove all micro-lanes and endpoint-specific transport state;
3. retain only bounded Core-owned logical operations that are still valid;
4. authenticate again; and
5. resolve each relevant logical recipient on demand before communication
   resumes.

An existing macro-lane with queued work is relevant and should be resolved
immediately after authentication. An idle macro-lane may wait until its next
operation. If an agent was removed while Core was disconnected,
`resolve_recipient` returns `authorization_rejected` and the macro-lane is shut.

## Control lifecycle rules

### Installation added

An installation is not advertised merely because its TCP/XMPP connection
exists. The sequence is:

1. authenticate and bind the exact installation session;
2. establish the dedicated control lane;
3. complete local readiness for receiving operations;
4. atomically mark the installation active, `Valid`, and assignment-eligible in
   the server registry; and
5. broadcast `installation_added` to every ready installation in the mesh.

Publication and assignment eligibility are one transition. This prevents
resolution from exposing an installation that has not finished startup or
cannot yet accept an assignment. If the new installation receives its own
mesh-wide broadcast, it treats it as an irrelevant local mutation and sends the
normal action ACK.

### Installation fully offline

When B2 becomes fully offline, the server first removes B2 from routing and
returns all of its reserved references and its unfinished lease to the front of
the logical unassigned queue. Only then does it broadcast
`installation_offline`. A stale Core may still propose B2 during propagation,
but the authoritative server records that work as unassigned and lets B1 or B3
pull it.

Each Core receiving the control removes B2's exact micro-lane and returns
locally owned references to the front of Agent-B's macro-lane queue.

### Installation reconnect

A reconnect is a new session incarnation even when the installation UUID and
resource remain the same. The old incarnation is terminal. Controls name the
incarnation they affect so a delayed old `installation_offline` cannot delete a
new lane.

The server publishes the replacement only after the new session is ready and
then broadcasts `installation_added` for the new incarnation. Core constructs a
fresh micro-lane as if it had never seen that installation before.

### Installation revoked

Revocation removes only the revoked installation, prevents its credential from
binding again, moves its reserved references and unfinished lease to the front
of the logical unassigned queue, and broadcasts `installation_offline`. Other
installations of the same logical agent remain eligible.

### Logical member removed

When an administrator removes Agent-B from the mesh, the server must atomically
commit authority removal before later routing. It then:

1. rejects future resolution or Rank 2 work involving Agent-B in that mesh;
2. closes B1, B2, B3, and every other Agent-B installation in that mesh;
3. purges every server operation where Agent-B is either source or recipient,
   including operations stored in another agent's mailbox, retained RPC
   responses, leases, dedupe evidence, large-payload object leases, resumable
   state, and mesh-scoped temporary data;
4. broadcasts `member_removed(Agent-B)` to the remaining mesh sessions; and
5. requires new current authorization before any Agent-B installation can
   communicate again.

Every Core fences all existing state involving Agent-B. This includes the
complete Agent-B macro-lane and micro-lanes, inbound and outbound Rank 1
channels, reply handles, dedupe state, retained operations, and pending
attempts. Later sends return `authorization_rejected`.

If Agent-B was the destination of a server-owned RPC and its requester remains
authorized and reachable, the server returns the canonical
`authorization_rejected` terminal response. If Agent-B was the removed source,
the server purges its outstanding requests and never publishes them to another
agent. One-way MSG operations are purged with only optional future telemetry.

The removed installations are closed directly; they do not remain trusted
merely because they did not receive or acknowledge the broadcast.

### Logical member added or re-added

No mesh-wide `member_added` control is required. An interested Core discovers
the agent through `resolve_recipient`, and installations become visible through
`installation_added` after they are ready.

Re-adding Agent-B creates current authorization only. It does not restore the
old macro-lane, old server mailbox, old operations, or revoked installation
sessions. The server's recipient revision continues within the current
revision cycle; a later server cycle starts from zero with a different cycle
ID. Old buffered controls were destroyed with the removed lane.

## Idle macro-lane retirement

A macro-lane may retire after five continuous minutes without activity, but
only when it owns no:

- queued or assigned operation;
- RPC waiter;
- active transport attempt;
- unresolved recipient resolution; or
- buffered or unapplied control.

Operation admission, assignment, completion, response handling, relevant
control processing, or resolution resets the five-minute timer. Retirement
closes the lane's micro-lanes and Rank 1 transport state. The next SDK operation
creates a new provisional lane and follows the race-free resolution path.

## Three-by-three example

Agent-A and Agent-B each run three installations:

```text
Agent-A: A1, A2, A3
Agent-B: B1, B2, B3
```

Each A installation has its own local Agent-B macro-lane only if that A
installation communicates with Agent-B. Those local selectors are independent:

```text
A1 operation 1 -> proposes B2
A2 operation 2 -> proposes B3
A3 operation 3 -> proposes B1
```

If B2 cannot use P2P but remains connected to the Mesh Server:

```text
A1 operation 1 -> Rank 2 -> Mesh Server -> B2
```

The server must not redirect it merely because Rank 1 was unavailable.

If B2 becomes fully offline before completing the operation:

```text
Mesh Server removes B2
Mesh Server moves operation 1 to front(Unassigned)
B1 or B3 pulls operation 1 through its alternating selector
Mesh Server broadcasts installation_offline(B2)
```

If all B installations are offline, the server retains accepted Rank 2 work in
Agent-B's logical mailbox. When B4 becomes ready, the server broadcasts
`installation_added(B4)` and B4 pulls one operation at a time. On successive
pulls, B4 alternates between work reserved for B4 and globally unassigned work,
falling back immediately when one class is empty.

If B1 starts a separate RPC to Agent-A, Agent-A's A1, A2, and A3 installations
are independently eligible destinations. The same rules apply in the opposite
direction. A response to an RPC originally issued by B1 still returns to B1.

This design therefore supports two logical agents with any bounded number of
installations on both sides. It does not require a shared in-process queue among
A1, A2, and A3; the Mesh Server coordinates only work that has crossed the Rank
2 server-acceptance boundary.

## Security and race requirements

The implementation must satisfy all of the following:

- derive the mesh and source installation from the authenticated server
  session, never from untrusted operation fields;
- validate logical-recipient membership during resolution, Rank 2 enqueue,
  assignment, completion, and reply;
- bind Rank 1 identity to authenticated same-mesh signaling and the direct
  channel rather than trusting application envelope identity;
- bind every micro-lane and assignment lease to an exact session incarnation;
- create the provisional macro-lane before issuing recipient resolution, then
  initialize it from the revisioned result and apply newer buffered controls;
- scope revisions by cycle, reject old-session controls, and fence revision
  gaps until authenticated resolution repairs them;
- atomically deduplicate Rank 2 admission and destination delivery by stable
  message ID without comparing payload contents;
- make completion versus offline/requeue one atomic terminal-winner race;
- reject late controls, acknowledgements, replies, and lease completions from a
  retired incarnation;
- fence communication immediately when local control authority is lost;
- keep control processing independent of peer and SDK backpressure;
- distinguish XEP-0198 packet acknowledgement, Rank 2 server acceptance, and
  control action acknowledgement;
- bound local macro queues, endpoint references, server mailboxes, control
  ledgers, RPC waiters, and retained bytes;
- apply operation deadlines while work is queued, assigned, offline, or
  awaiting RPC completion; and
- purge every source-side and destination-side operation and authority object
  involving a logical agent after committed mesh removal.

## Frozen first-implementation choices

The following operational choices are part of this ADR rather than open design
questions:

1. Each valid installation pulls at most one active Rank 2 lease and alternates
   successful pulls between its reserved queue and the global unassigned queue.
2. Local SDK cancellation does not remotely cancel server-owned work; absolute
   expiration removes it.
3. Requester logout does not delete unexpired work or responses. A response
   remains bound to the exact requester installation until its deadline.
4. Rank 2 server state is bounded but RAM-only. Server crash survival and
   multi-node shared state are future work and are not promised.
5. An empty macro-lane retires after five continuous inactive minutes.
6. The control action-ACK deadline is 30 wall-clock seconds.
7. Dedupe evidence remains through the operation deadline plus five seconds.

## Implementation impact

This ADR requires coordinated changes in Go Core and the dedicated ejabberd
repository.

Go Core needs:

- a `RecipientRegistry` and lazy `RecipientLane` aggregate above exact peer
  lanes;
- installation-neutral operation storage;
- macro-level RPC waiters with endpoint-independent deadlines and correlation;
- reference-only micro-lane assignment;
- one-of-N selection for both MSG and RPC;
- Rank 2 logical-recipient handoff with proposed-installation metadata;
- per-recipient resolution;
- revision-cycle and recipient-revision handling;
- direct handling of the three mesh controls;
- separate packet and application-level control acknowledgements;
- a 30-second `ControlPending` validity gate;
- bounded message-ID dedupe across transport retries;
- five-minute safe macro-lane retirement; and
- complete endpoint-cache fencing on control-session loss.

The Mesh Server needs:

- authenticated per-recipient resolution;
- one logical Rank 2 mailbox per organization, mesh, and agent;
- direct Cynapsa logical-mailbox storage instead of per-resource XMPP offline
  storage;
- active installation and session-incarnation tracking;
- proposed-installation preference;
- per-installation reserved references, one active lease, a global unassigned
  queue, and alternating pull selection;
- atomic message-ID dedupe and `server_accepted` responses;
- exact requester-affinity response proxying;
- mesh-wide control broadcast with packet/action acknowledgement tracking;
- forced logout after a 30-second missed action acknowledgement;
- atomic membership removal, mailbox purge, and session closure; and
- bounded RAM-only storage and expiry for accepted Rank 2 work.

The existing implementation does not meet this ADR. In particular, it uses a
complete authority snapshot, broadcasts one-way MSG work to all replicas,
selects the first endpoint for RPC, routes Rank 2 to exact resources, and emits
membership controls without this installation lifecycle and
application-acknowledgement contract.

## Required acceptance scenarios

The implementation is not complete until tests prove at least:

1. Agent-A sends concurrent MSG and RPC work across B1, B2, and B3 without
   inline-payload or large-payload copies caused by lane assignment.
2. B2 loses only P2P and continues receiving its proposed work through Rank 2.
3. B2 stalls while its XMPP session remains authoritative and is not silently
   replaced merely because it is slow.
4. B2 closes definitively; source-owned references and server-owned reserved or
   leased references are requeued exactly once and B1/B3 continue.
5. All B installations are offline; accepted server work waits and a newly
   ready B4 pulls it one operation at a time.
6. `installation_added(B4)` reaches every ready mesh installation, while only
   Cores with an existing Agent-B macro-lane allocate a B4 micro-lane.
7. An irrelevant Core action-acknowledges every control after authentication
   without retaining that recipient's revision or allocating recipient state.
8. A Core receives a packet-acknowledged control but misses the 30-second action
   ACK deadline; it is removed from routing, has server work requeued, is
   announced offline, and cannot communicate until reauthentication.
9. A stale offline control for an old B2 incarnation cannot remove a reconnected
   B2 micro-lane.
10. Administrative removal of Agent-B closes every B installation, purges all
    server work where B is source or recipient, fences inbound and outbound B
    state at peers, and makes later resolution return
    `authorization_rejected`.
11. Agent-B is later re-added without resurrecting its old mailbox, lanes, or
    operations.
12. A1, A2, and A3 all send to B1, B2, and B3 without deterministic
    first-endpoint concentration.
13. RPC responses return to the exact requesting installation even when the
    destination installation changes during Rank 1 or Rank 2 execution, and
    complete the macro-level waiter rather than an endpoint lane.
14. Completion, timeout, local cancellation, installation-offline, member
    removal, and server acceptance each have one terminal winner under race
    testing. Local cancellation does not remove server-owned work.
15. Loss of the local control session immediately fences communication; after
    fresh authentication, relevant recipients are resolved lazily before work
    resumes.
16. `installation_added(B2)` or `installation_offline(B2)` racing C1's first
    Agent-B resolution cannot be lost or undone, regardless of whether the
    control or resolve response reaches Core first.
17. B2 with several reserved references and one unfinished lease becomes fully
    offline; the server atomically moves all of them to the front of Agent-B's
    unassigned queue and B1, B3, and a later B4 pull them without per-resource
    payload copies.
18. Concurrent first sends to Agent-B join one provisional macro-lane and one
    resolution request; authorization rejection fails all joined work, while a
    temporary transport failure retains bounded work until deadline.
19. Duplicate, stale, skipped, and new-cycle authority revisions are handled as
    specified; a gap or cycle change fences the recipient until authenticated
    resolution repairs it.
20. An XEP-0198 packet ACK without `control_applied` never returns a
    `ControlPending` installation to `Valid`.
21. The server accepts a Rank 2 operation but its `server_accepted` response is
    not observed; retrying the same stable message ID returns the existing
    status without a second stored operation or payload comparison.
22. B1, B2, and B3 alternate their own reserved work with global unassigned
    work, fall back when one class is empty, and never pull work reserved for a
    different online installation.
23. B2 logs out with unexpired reserved and leased work; the server retains and
    redistributes it, and a later B2 session may pull whatever remains before
    expiration.
24. A1 logs out while an RPC response is retained; the response remains bound
    to A1 until deadline, may reach a reauthenticated A1, and never moves to A2.
25. A controlled ejabberd process restart demonstrates the explicitly accepted
    first-version limitation that RAM-only mailbox, response, lease, and dedupe
    state may be lost.
26. An otherwise empty macro-lane retires after five inactive minutes, while a
    lane with any queued work, waiter, attempt, resolution, or unapplied control
    does not retire.
27. A large Rank 2 payload transfers descriptor/object-lease ownership at
    server acceptance and is reassigned between installations without re-upload
    or payload copying.

## Summary

Users address logical agents. Core stores each logical recipient's work once in
a macro-lane, keeps RPC waiters at that logical level, and assigns references to
exact installation micro-lanes. Rank 1 uses the proposed installation directly.
Rank 2 transfers process-lifetime ownership to a RAM-only logical server
mailbox after message-ID-deduplicated server acceptance. Work remains reserved
for its proposed installation until that installation is fully offline.
Available installations pull one lease at a time and alternate their own
reserved work with globally unassigned work. Fully offline installation work
returns to the front of the unassigned queue.

Mesh-wide controls use XEP-0198 packet acknowledgements and separate
post-application action acknowledgements. A Core remains `ControlPending` until
it has applied the required contiguous revision, and a missed 30-second action
ACK forces logout and reauthentication. Logout retains unexpired application
work, while logical membership removal purges every source-side and
destination-side operation involving that agent. Empty macro-lanes retire after
five inactive minutes; large-payload descriptors follow the same ownership and
reassignment rules without copying payload data.
