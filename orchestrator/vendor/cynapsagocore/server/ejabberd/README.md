# Cynapsa mesh authority for ejabberd 26.04

`mod_cynapsa_mesh` is the V1 Mesh Server authorization component. It is
version-pinned to ejabberd 26.04 and intentionally fails closed unless all of
the following are true:

- exactly one ejabberd database node is running;
- the module is loaded for one dedicated authority vhost;
- `mod_shared_roster` uses its Mnesia backend;
- resource-conflict handling preserves the requested resource (`setresource`
  is rejected); and
- the shared-roster, offline-mailbox, and MAM stores use their pinned Mnesia
  backends and are readable on the local node.

Clustered authority is not supported in V1. Direct `srg_*` changes to Cynapsa
groups are not an authoritative administration path. The module deliberately
does not subscribe to raw Mnesia table events; only the `cynapsa_mesh_*`
commands below provide the serialized membership/snapshot/revocation contract.

## Build and install

Run:

```sh
server/ejabberd/build.sh
```

The script has no network access and compiles/tests against the pinned image
`ghcr.io/processone/ejabberd@sha256:68482e33ff11934e73ef2881cf455ccefc1e03b31203f3ab651f4b2b1152b4a8`.
Its only artifact is:

```text
server/ejabberd/mod_cynapsa_mesh/ebin/mod_cynapsa_mesh.beam
```

Mount that file read-only at:

```text
/opt/ejabberd/.ejabberd-modules/mod_cynapsa_mesh/ebin/mod_cynapsa_mesh.beam
```

Then enable the fragment in `ejabberd.yml.example`. The module registers its
bind hook before ejabberd opens a session, a global pre-routing filter, the
exact-session authority-sync IQ, and the restricted administration commands.

## Authority commands

All commands use canonical binary values and are `restricted`. `host` is the
one configured authority vhost. `user` is the localpart only. `mesh` is the
exact case-preserving resource value (maximum 256 bytes). Internally the SRG
identifier is exactly `cynapsa_mesh_` plus `mesh`.

```text
ejabberdctl cynapsa_mesh_ready HOST
ejabberdctl cynapsa_mesh_create MESH HOST
ejabberdctl cynapsa_mesh_add USER HOST MESH
ejabberdctl cynapsa_mesh_remove USER HOST MESH
ejabberdctl cynapsa_mesh_remove_many USER1,USER2 HOST MESH
ejabberdctl cynapsa_mesh_snapshot MESH HOST
```

`ready` returns the tuple `ready, HOST, single_node_mnesia`; a non-ready
authority returns `not_ready, HOST, fail_closed`. Create/add/remove exit zero
only for success or the documented idempotent case. `remove_many` validates and
commits every effective removal in one Mnesia transaction and emits one
constant-size live snapshot-required wakeup. Snapshot prints the canonical
sorted current bare-JID member list. Unknown, invalid, capacity, or storage
states fail closed. Every effective mutation invalidates ready sessions before
later cleanup or notification. Removed exact mesh resources are kicked;
retained sessions remain connected, receive a live wakeup, and install a fresh
complete snapshot before application routing resumes.

The authoritative group contains at most 65,536 members; a node contains at
most 65,536 Cynapsa mesh groups.

## Bind and route policy

SASL authenticates a bare local JID. The client must request the exact mesh ID
as its resource. Before session open, the module requires current membership
of that bare JID in `cynapsa_mesh_` plus the requested resource. It does not
cache allow decisions.

Before a user session can submit any agent-directed message, IQ, or presence,
the authenticated ingress hook requires the stanza sender to be the exact
canonical bound full JID. The recipient must be a canonical full local JID
whose opaque, case-sensitive resource is byte-for-byte equal to the sender's
mesh resource. Bare, missing, foreign-host, cross-resource, normalization
alias, and near-match addresses fail before MAM or offline hooks can retain
the stanza. Undirected/bare-user presence therefore cannot become roster
fan-out; directed same-resource presence remains available.

The ingress hook also rejects carbon sent/received, forwarded, MAM-result, and
nested `jabber:client`/`jabber:server` stanza containers. Prefixed element names
or namespace declarations are rejected rather than trusted as aliases; default
namespace inheritance is resolved from the outer `jabber:client` stanza.
Ordinary non-prefixed extension payloads in their own default namespace remain
available. XEP-0280 carbon delivery is intentionally unsupported on the agent
plane because its purpose is delivery to another resource; server metadata is
not an authorization capability. Carbon enable/disable and other legitimate
local service IQs can still be processed, but no resulting wrapped copy can
cross the same-resource boundary. A global pre-router check repeats the rule
for message, IQ, and presence, and the recipient c2s hook rechecks it
immediately before online delivery. Consequently rejected traffic cannot
bypass the policy through offline storage, archive storage, carbon wrapping,
or final resource replacement.

Denied server/internal wrappers, including carbon and MAM/forwarded messages,
are silently dropped: they are never reflected as stanza errors. A denied
packet receives a policy error only when the ingress hook stamped an exact,
still-current authenticated c2s PID/session epoch and canonical bound full JID.
That error contains no original body, wrapper, or extension payload and is
addressed only from the local bare server to that exact bound full session;
the router and final recipient hook validate the structured internal marker.

Before a user session can submit admitted application traffic, its exact c2s
PID/session epoch must install one complete current snapshot. Both full JIDs
must belong to the authority vhost, use the same
exact mesh resource, and remain current members. Durable application
messages may target an offline current member. Jingle/Rank-1 IQ establishment
requires both endpoint sessions live and currently synchronized. Final online
delivery independently checks the recipient's exact ready session;
it does not require the original sender still to be connected. A rejection is
dropped before application delivery; only a denied exact authenticated client
submission can receive one sanitized server policy error.

The server/service exemption is the exact canonical bare domain configured as
this module's local authority host. Its user and resource parts must be empty,
its original and normalized domain fields must both equal that configured host,
and its wire encoding must be exact. Foreign domains, subdomains, suffix or
prefix lookalikes, case or Unicode aliases, punycode alternatives, trailing
dots, ports, resources, localparts, and bare-user destinations do not classify
as services. The other endpoint must be that same authority domain or a
canonical full local client JID. Even on that route, only the private group and
authority IQs, XEP-0202 time, disco info/items, XEP-0215 extdisco, ping, carbon
enable/disable, correlated result/error shapes, and directed presence are
allowed. Arbitrary server-addressed IQ payloads and ordinary messages fail
closed. TLS, SASL, resource bind, session setup, and stream management occur
outside these routed stanza hooks and are unchanged.

The only auxiliary service identity is the exact canonical bare upload domain
`upload.` plus that authority host. This derivation intentionally matches the
pinned `mod_http_upload` default and Core's authenticated-domain derivation; it
does not admit other subdomains. The client-to-upload direction permits only a
strict XEP-0363 IQ `get` request from an exact current synchronized local full
session. The reverse direction permits only the correlated strict slot result
or echoed-request stanza error to that same exact current session. Correlation
uses a 256-entry exact-session ID set carrying a monotonic 30-second expiry,
with no per-request timer. Session loss or mesh mutation clears it; full
capacity rejects a new request; and duplicate, late, malformed, or
replacement-session results are dropped.
The module retains only the validated local slot path and slot identifier needed
to delete an accepted upload if its owner is removed. It never retains PUT
headers or credential values, and rejects paths outside the configured upload
document root.

On the dedicated authority vhost, configure the HTTP upload request route as
`/upload: mod_cynapsa_mesh`, not `mod_http_upload`. The module delegates valid
requests to the pinned upload implementation while holding an exact-path lock.
The same lock fences membership removal, so a PUT that already consumed its
temporary slot either completes before removal deletes it or is rejected after
the membership commit. GET and HEAD also require the durable path owner to be a
current member of that exact mesh. Reusing an XMPP IQ ID cannot replace an
earlier ownership record because durable ownership is keyed by the
server-generated validated object path.
An issued-but-unused path is a pending object owned by the current server
instance and expires with the pinned five-hour HTTP-upload slot lifetime. A
minute sweeper and every new admission prune expired paths under that same
lock before deleting their ownership rows. A successful PUT atomically marks
the row completed. A failed or interrupted PUT first becomes a non-readable
cleanup tombstone. Its row is deleted only after physical cleanup succeeds;
otherwise it remains quota-charged and the sweeper or exact-owner removal
retries cleanup under the same path lock. Rejected duplicate or conflicting
slot responses likewise preserve any durable owner already recorded for that
exact path. Before a sweep touches the filesystem, it revalidates the persisted
host, path, and slot against the configured upload document root. A malformed
row remains quota-charged and returns a cleanup error without deleting any file
or ownership record. The durable table admits at most 4,096 objects and 4 GiB
of aggregate declared content globally, and at most 256 objects and
1,073,741,824 bytes for each exact
`{user, host, mesh}` owner. Both pending and completed rows count, so one member
cannot monopolize upload authority and unused slots or retained files cannot
grow authority state or removal work without bound. Admission takes a table
write lock while checking both quotas. Capacity failure cancels the newly
issued slot and returns no usable URL.
Completed objects remain charged to both quotas while their owner remains in
the mesh; exact-owner removal deletes both their files and ownership rows under
the same path fence. Removal re-reads the current row after acquiring that
fence, so a PUT that completed first is cleaned and its completed ownership row
is deleted rather than leaving stale quota. Physical cleanup failure retains
the current row for an idempotent retry, and a row now owned by another mesh is
preserved.
The server accepts at most 134,217,760 transferred bytes per object: the V1
134,217,696-byte public payload ceiling plus its exact 64-byte authenticated
transfer allowance. The HTTP-upload module's `max_size` must use the same
134,217,760-byte ceiling in production.

The dedicated authority vhost configures MAM `default: never` and
`request_activates_archiving: false`; startup/reload rejects any mismatch and
transactionally migrates legacy per-user `archive_prefs` to `never` before the
authority becomes ready. Stock `mod_offline` may retain ordinary account
traffic, but its bare-account spool is never allowed to own a Cynapsa frame.

Eligible Rank-2 data is exactly one `<message type='chat'>` child of the form
`<frame xmlns='urn:cynapsa:aztm:1' v='1'>RAW-BASE64</frame>`, with no body,
subject, thread, sibling, nested element, extra attribute, padding, or
whitespace. The server validates the wrapper and canonical raw-Base64 terminal
bits, but never decodes or classifies the opaque Core frame body. A canonical
outer `msg_` ID selects durable exact-resource mailbox handling. A missing or
noncanonical outer ID selects transient live routing; the server removes any
library-generated noncanonical ID before delivery so Core can enforce the
protocol rule that non-envelope frames have no outer ID. Transient transfer and
object-control frames are excluded from both this mailbox and stock bare-JID
offline storage. Malformed Cynapsa-looking wrappers fail closed. Presence, IQ,
Jingle, authority controls, errors, MAM/carbon wrappers, and arbitrary messages
never enter the Cynapsa mailbox.

Every eligible frame is synchronously committed before the sender's routing
operation returns. The durable owner is exact `{destination user, host, mesh
resource}` plus a server-local mailbox arrival ordinal; the row contains no c2s PID, SID,
resume ID, or other session identity. This interception occurs before
`ejabberd_sm` full-JID routing, so an online resource belonging to the same bare
account can never receive or suppress storage for a different addressed mesh
resource. Per-resource defaults are 1,024 rows, 64 MiB, and 24 hours. Message
and byte limits are positive bounded module options; retention is additionally
capped at 24 hours so Core's 25-hour terminal message-ID dedupe horizon always
outlives possible mailbox replay. Capacity or storage failure produces a deterministic
`resource-constraint` stanza error and never reports successful custody.
After and only after the mailbox transaction commits, the module sends one
strict `urn:cynapsa:mesh-custody:1` acceptance from the exact bare authority
domain to the exact authenticated sender session that submitted the committed
packet, correlated by the canonical outer message ID. A replacement session
cannot inherit that capability; the old receipt is instead lost and replay
obtains a fresh acceptance. Replays with the same destination resource, sender
resource, and canonical message ID reuse the original FIFO row and quota charge;
the server does not compare content. Client-originated copies cannot pass the internal
metadata and exact-session delivery checks. XEP-0198 handled counts alone are not custody
evidence; a lost acceptance causes the still-owned Core Outbox entry to be
replayed on a fresh authenticated session.

If the exact destination session is synchronized, the committed row is leased
to that session and queued by XEP-0198. A short outage remains in the resumable
SM queue. The mailbox row is deleted only when the destination's cumulative SM
acknowledgement covers it. If resumption expires, ejabberd's rerouted copy is
dropped because the stable row still exists. A fresh bind completes the exact
authority result first and then drains only its resource mailbox in server FIFO
order. New admissions are serialized behind that drain, and an interrupted
drain leaves the same rows available to the next session. Non-SM diagnostic
clients use successful c2s write as their terminal lease boundary; production
Cynapsa sessions always negotiate XEP-0198.

`mailbox_audit_log: true` enables metadata-only `cynapsa_mailbox` INFO events
for committed admission, immediate dispatch outcome, custody emission, and the
exact mailbox keys deleted by a destination SM acknowledgement. Payload bytes
are never logged. The option defaults to `false`; the integration and deep E2E
profiles enable it so a final empty table cannot hide the distinction between
"never admitted" and "admitted, dispatched, acknowledged, and deleted."

## Exact-session authority synchronization

Every fresh bind starts unsynchronized. `mod_cynapsa_mesh` advertises exactly
`urn:cynapsa:mesh-authority:1` through its host-scoped root XEP-0030 feature
hook. After exact resource binding and XEP-0198 enablement, the client sends
one bounded, correlated `disco#info` IQ to the authenticated bare authority
domain. Missing, duplicate, wrong-version, malformed, error, or incorrectly
correlated results fail the fresh session closed. Snapshot synchronization may
start only after discovery succeeds.

The client then sends a correlated IQ `get` to the bare authority domain:

```xml
<sync xmlns='urn:cynapsa:mesh-authority:1' nonce='R' cursor='C'/>
```

The server infers caller, host, and mesh solely from exact authenticated c2s
session metadata. It rejects caller-supplied authority data, unknown or
replaced sessions, unexpected cursors, and oversized input. On cursor zero the
serialized owner freezes one immutable sorted full-JID vector for that exact
session and nonce. A page has at most 256 members and 256 KiB. Later pages come
only from that vector. Pending vectors have direct aggregate member/byte
counters and a fixed monotonic five-second deadline that page requests cannot
refresh. A stalled sync is removed and its exact unsynchronized c2s resource is
closed, releasing the bounded capacity for another session. A membership
mutation invalidates incomplete vectors and ready sessions before publishing a
live wakeup, so snapshots never mix.

Only the terminal page installs exact `{c2s PID, session epoch, mesh}`
readiness. PID exit, failed/expired resumption, replacement by a fresh bind, or
membership mutation invalidates it. Successful XEP-0198 resumption preserves
the same logical session's readiness: the `c2s_copy_session` hook reserves the
old exact ready epoch using a server-generated one-use handoff, and
`c2s_session_resumed` installs it on the replacement PID/SID only after
ejabberd has accepted the resume. It then sends one strict server-originated
`resume-authority` IQ result with status `ready` or `not-ready` from the bare
authority domain to the exact bound resource. Failure or exception while
completing the handoff produces `not-ready`. Both phases revalidate
server-owned session identity and current membership. No readiness or identity
claim supplied by the client is trusted.

Successful XEP-0198 resumption does not repeat discovery or request another
snapshot; it proceeds through the strict `resume-authority` barrier.

The complete snapshot authorizes the authenticated logical session without a
client-side membership lease or periodic renewal. ejabberd membership changes
are therefore explicit authority edges: the reliable control transaction is
processed or the exact session is closed. The five-second pending snapshot
assembly deadline and the configurable control-acknowledgment timeout are
bounded protocol-operation deadlines, not membership-authority expiration.

The handoff is fail-closed. A membership mutation removes old readiness and
every in-flight handoff for that mesh in the serialized authority process
before routing its notification. A racing old-PID `DOWN`, failed replacement,
wrong SID, wrong resource, stale ticket, or missing current membership cannot
make the replacement ready. The server emits `not-ready`; the client abandons
continuity and uses a clean bind plus a complete current snapshot.

Live `membership-changed` IQs are constant-size snapshot-required wakeups;
they contain no removed identity list, target only a currently connected exact
resource, and are never offline-mailbox, MAM, or application replay data.
Router acceptance is not delivery evidence. The authority process tracks each
control by its server-generated IQ ID and exact c2s PID, SID, and bound full
JID. The client result is consumed at authenticated c2s ingress and must match
all four values. A route/enqueue exception, replaced exact session, pending
capacity failure, malformed or mismatched result, or missing result after the
configurable `control_ack_timeout_seconds` bound closes that exact session. The
default bound is five seconds; `control_pending_max` bounds tracked controls.
During XEP-0198 handoff, ownership moves to the replacement PID before the old
PID can exit, remains unacknowledgeable under the old SID, and receives the new
SID only after successful resume completion.
ejabberd's XEP-0198 replay queue remains one indivisible FIFO: this module does
not delete, reorder, or renumber selected entries and does not alter `h`
accounting. ejabberd sends its post-resume `<r/>` after the retained FIFO and
before the current `c2s_session_resumed` result. The client ignores canonical
`resume-authority` results before that post-resume `<r/>` because an unacked
result from an older reconnect may replay there. It then accepts exactly one
strict current result after `<r/>` for the resumed logical session. Missing,
duplicate, malformed, stale-session, or `not-ready` results fail closed and
force the clean-bind/full-snapshot path. The result is completed on the
dedicated control lane, after every preceding replayed authority item, before
any retained outbound application stanza is replayed. No client IQ is sent,
because it could overtake retained outbound stanzas and corrupt cumulative
XEP-0198 acknowledgement ordering.

The former diagnostic group/topology IQ is not registered or implemented.

## Removal and re-add

Removal atomically commits current membership deletion together with every
Cynapsa mailbox row whose exact sender or recipient is the removed
`user@host/mesh`. It then invalidates ready sessions, purges matching legacy
full-resource offline and raw/decoded MAM records, deletes locally
owned completed XEP-0363 objects and cancels their temporary slots, clears
exact-session upload leases, and invalidates pending authority-resume handoffs.
Removed resources are still kicked and their exact mailbox/offline state is
purged. Retained resources are not disconnected: their unmodified XEP-0198
stream can replay the queued snapshot-required control after a short outage.
Other account resources and objects owned in other meshes remain untouched.
Remaining live resources receive one wakeup and install a fresh complete
snapshot. A future removed-resource bind is rejected.

Re-add has current-membership semantics. If the identity is present in the
latest snapshot it is authorized now; older membership history is not
consulted. Removal physically deletes the exact
resource rows, so re-add cannot resurrect them; rows for other resources and
meshes are untouched. Cleanup retries are idempotent.
