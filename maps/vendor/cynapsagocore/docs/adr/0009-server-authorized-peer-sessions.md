# Server-authorized peer sessions without mesh membership snapshots

- Status: implementation in progress on `remove-snapshot`.
- Scope: the current mesh-member snapshot IQ and its client-side admission
  cache. This does not change the separately pinned future Guard authority/key
  snapshot contract.
- Supersedes: the complete-membership synchronization and recovery rules in
  ADR 0008 and the older snapshot portions of ADRs 0003 and 0006. Their
  envelope, conversation, payload, and peer-lane rules remain in force where
  they do not depend on a complete mesh-member snapshot.

## Authority and admission

The server authenticates each installation into exactly one mesh. On the first
attempt to contact a logical peer, Go Core asks the server to authorize that
peer for its current exact session. The server checks both parties' current
mesh membership and returns an active exact peer endpoint, its installation
identifier, and a server-issued session generation. A peer without an active
endpoint fails `unavailable` before an SDK `send` or `request` is accepted.
Go Core never derives membership from a roster, snapshot, resource string,
envelope, or SDK mapping. It admits only the peer/session tuple returned by
the authenticated server handshake. The server remains the routing authority.
An inbound message from a previously unknown peer uses its transport-
authenticated exact sender in the same server handshake before a peer lane
is opened. A bare-name lookup is not sufficient to authorize a different
installation of the same logical agent.

Mellium can hand an authorization IQ result to the general stanza reader if
the initiating caller is canceled before the result arrives. Go Core retains
a bounded ledger of exact locally issued authorization IDs across successful
stream resumption. Such a late reply must match the server and local bound
JIDs and validate its full body; a successful result must also name the
original requested peer. It is then accounted for and discarded; it never
grants peer authority or reopens a lane. Unknown IDs and malformed replies
still fail closed. A clean bind or terminal close clears the ledger.

`send` is fire-and-forget after handshake authorization: acceptance does not
assert delivery. An RPC waits for a response until its timeout. An unknown
last-hop outcome is not a post-acceptance SDK error. For now, an unacknowledged
stanza from an expired XEP-0198 session is not rerouted to another
installation. The current dedicated ejabberd profile has no per-installation
offline mailbox, so an unacknowledged stanza can be lost after the resume
window. This is a best-effort delivery boundary, not an end-to-end receipt.
If ejabberd later rejects an application message, Go Core consumes its bounded,
server-originated error stanza without treating it as peer traffic or closing
the XMPP session. A message ID that matches a pending RPC completes that waiter
with the mapped routing error, and the rejected outbox entry is retired without
affecting other messages. A late error cannot retract an RPC response already
consumed by its waiter. A completed fire-and-forget `send` cannot retroactively
raise an exception; its return value remains an acceptance result, not a
delivery guarantee. The error alone does not prove delivery of any other
message.

## Disconnect, resumption, and revocation

Go Core and ejabberd independently probe idle XMPP connectivity. The initial
target is a 10-second idle interval and 10-second reply deadline. Detection of
transport loss pauses Go Core application traffic and closes its peer state.
ejabberd permits XEP-0198 resumption for 30 seconds after it detects the loss.
A successful resume continues the server session and does not emit a revoke,
but Go Core still repeats Cynapsa peer handshakes before application traffic.
Once resumption definitively expires, the server revokes the old installation
session. Clean logout and explicit membership removal need not wait for the
resume period.

Locally queued outbound messages remain locally owned across a transient
disconnect. When transport recovers, Go Core revalidates the exact peer
sessions before releasing queued work; definitive revocation or failed
authorization retires the affected peer's pending work. This phase does not
move an unacknowledged message to a different installation.
If the server rejects the local installation's clean re-login, recovery ends
terminally: Go Core fences peers, clears replay/outbox state, and subsequent
SDK send/request/reply operations report `authentication_failed` rather than
retrying as a temporary connectivity outage.

The server's current c2s resume hook also has a narrow PID-switch window:
the resource may point to the new process before the mesh module has moved its
session record. A concurrently routed message can fail the exact-session
guard in that interval. Atomic authority handoff or a bounded handoff buffer
is required before claiming loss-free admission through that transition.

An installation revoke identifies the installation and the exact server-issued
session generation. It closes peer state only if that generation is still
current; a late old-session revoke cannot close a newly established session.
A logical-agent revoke closes all local state for that agent. Control events
have opaque `action-id` values and are idempotent by target. Go Core promptly
returns a packet-level IQ result for each incoming revoke, then closes the
specified peer state on its independent control lane. Once cleanup finishes,
it sends a separate `<applied control-id='action-id'/>` IQ set and waits for
the server's result. Neither an XMPP stanza acknowledgment nor the initial
packet IQ result is the action acknowledgment. If the action acknowledgment
does not arrive within 30 seconds, the server closes the nonresponding
installation's session and revokes it to other mesh members. A resumed session
receives outstanding controls again with new action IDs; old acknowledgments
cannot complete those reissued controls.

The control path is independent of application peer state. Membership removal
immediately denies new handshakes and server-routed traffic, then broadcasts a
logical revoke to the remaining mesh members. The local Go Core must not
continue to use a previously authorized peer after its own server connection
is interrupted; it re-establishes authorization on reconnection.

## Compatibility and tests

The public SDK command shape is unchanged unless a newly required failure
cannot be represented by its current bounded error vocabulary. Verify: idle
silent failure, clean close, successful resume, resume expiry, stale revoke,
logical removal, action-ACK timeout, unavailable target, handshake denial,
and independent operation of the control path under busy application work.

The older Core-embedded ejabberd Docker fixtures still speak the prior
snapshot protocol. They must be migrated to the dedicated server's new
peer-authority IQs before their end-to-end results can qualify this branch;
their handshake failure is an expected incompatibility, not evidence of a
passing end-to-end test.
