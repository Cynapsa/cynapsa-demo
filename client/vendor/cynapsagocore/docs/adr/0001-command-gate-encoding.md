# ADR 0001: Command Gate Encoding and Ownership

- Status: Accepted
- Scope: In-process command admission, dispatch, cancellation, completion, and shutdown
- Decision date: 2026-08-11

## Context

The core needs a bounded, deterministic mechanism for accepting typed private
commands without allowing SDK callbacks, public types, serialization, or
connectivity work to spread into runtime workers. Every locally admitted
command must produce at most one terminal completion even when execution,
cancellation, panic, and shutdown race.

The command gate is an in-process coordination boundary. Native ABI encoding
and public completion projection remain owned by `internal/sdkboundary` and are
not part of this ADR.

## Decision

### Queue and capacity model

`commandgate.New(capacity)` requires one explicit positive capacity. Zero and
negative capacities are configuration errors.

The same bound is used for:

- the buffered command queue;
- live registry entries;
- the buffered completion queue; and
- the terminal command-ID and command-handle history.

The live registry entry is retained until its terminal completion is consumed.
Consequently, every admitted command reserves exactly one possible completion
slot for its whole lifetime. A full registry rejects new admission immediately;
it does not wait, spawn a goroutine, grow memory, or silently discard a command.
Dispatching a command does not release capacity. Consuming its completion does.

Count capacity is paired with an exact retained-memory charge. One frozen
command may retain at most 4 MiB. The aggregate command-snapshot budget is
`min(capacity * 4 MiB, 256 MiB)`. The charge includes the command and closed
argument structs, pointer targets, slice backing, strings, credentials, and
payload bytes. A command that would exceed either bound is rejected before
admission with normalized capacity pressure and no caller-memory mutation.

The gate owns both channels. They are never closed: shutdown is signalled by
dedicated state channels, avoiding send-on-closed-channel races. Finalization
drains channel contents and then closes only the gate's `closedCh` signal.

There is no ordering guarantee between completions for different commands.
Each completion is identified by its command ID.

### Typed in-process encoding

Admission receives `model.Command`; completion emits `model.Result`. The gate
does not serialize either value and never accepts an untyped public map. The
single SDK boundary adapter remains responsible for strict decoding, public
normalization, and serialized output.

Admission and completion are distinct:

- successful `Submit` means the local gate accepted ownership and queued the
  command;
- it does not mean the command executed or any remote operation occurred; and
- the eventual terminal `model.Result` is read separately from the bounded
  completion queue.

Successful admission also returns an opaque `CommandHandle`. It is the sole
authority for local cancellation; `CommandID` is application identity and is
never accepted as cancellation authority. `SDKBoundaryAdapter` maps the private
handle to the same-named public `CommandHandle`, which SDK futures retain
privately.

### Immutable ownership handoff

Before admission the caller owns the command. `Submit` independently freezes
every value in the closed command catalog, including all strings, slices,
pointer targets, credentials, and payload bytes. A successful call transfers
that frozen snapshot to the gate; caller memory remains caller-owned and may be
reused after `Submit` returns. Rejection neither consumes nor mutates it.

The SDK boundary performs the same freeze once, then moves its unforgeable
one-shot ownership token into the gate after all admission checks. This avoids
a second persistent boundary-to-gate clone without creating a bypass for
direct internal `Submit` callers. The returned `model.Command` is an opaque
facade: all visible fields are empty and only its private token retains the
canonical frozen graph. Immediately before reservation and again at the atomic
move, the gate verifies that the facade is still empty and nonallocatingly
remeasures the token's complete closed graph against its exact charge and
process-keyed fingerprint. A visible field, type, pointee, payload, or
typed-nil injection rejects without consuming the token or changing live
accounting. A copied facade exposes no canonical string, slice, or pointee
backing, so pre- or post-transfer mutation cannot alter the gate-owned graph.
Only that one canonical backing is retained and transferred.

The retained charge covers queued, dispatching, and in-flight commands. A
terminal transition releases queued snapshots immediately. A dispatched
snapshot is released only after both the terminal winner and dispatcher return
are known, so cancellation or shutdown cannot clear bytes under a running
handler. Release drops all string/slice references and explicitly zeroes
password and payload byte backing exactly once. Internal authentication uses
Core-owned clearable password bytes; public Go and C ABI shapes remain strings.
The native submit path removes password and enrollment-token fields from its generic JSON working
copy, decodes the original token with a bounded clearable-byte decoder, and
clears both raw copies, so an ABI credential is never materialized as an
unzeroizable internal Go string.

If a dispatcher still borrows a command when a bounded `Shutdown` deadline
expires, the gate is nevertheless closed to admission and new dispatch, but
the borrowed owner remains quarantined and charged in `Stats.OwnedBytes` until
that dispatcher returns a terminal attempt. This is bounded by the configured
command count and byte limits and prevents clearing memory through a live
borrowed view. Native unload must still join all in-flight calls; a
non-cooperative internal handler can therefore delay full memory release beyond
the shutdown deadline.

### Registry state machine

The live registry has three states:

```text
admitted -> dispatched -> terminal
    |                       ^
    +-----------------------+
```

Cancellation, completion, panic normalization, and shutdown all compete for
the one transition into `terminal`. The transition is protected by the
registry mutex. Only the winner publishes a completion.

Terminal entries remain live until completion consumption. Consumption removes
the live entry, releases its capacity, and records the ID in a bounded FIFO
terminal history as a process-keyed fixed-size HMAC-SHA-256 digest. The live
registry clears its full ID reference before releasing the charged command
owner, so neither live terminal state nor history can retain uncharged
caller-sized ID backing. Operations on a retained terminal ID return
`already_terminal`. When the history reaches its capacity, the oldest ID is
evicted and may be reused; terminal history therefore cannot grow without
bound. Every admission mints a fresh opaque `CompletionCapability`, stored in
the registry entry and returned only with its `Dispatch`. An old worker cannot
complete a new admission that reused the same command ID: its stale capability
does not match the new entry. Final shutdown removes both live entries and
terminal history.

### Cancellation

Cancellation is local only and never claims to revoke work already handed to a
remote or durable subsystem.

Every admission mints a core-scoped handle from a random 128-bit core scope, a
monotonic 64-bit admission generation, and an independent random 128-bit nonce.
The registry stores the handle-to-admission binding. This makes handles
unguessable, non-reusable within a core lifetime, and invalid in another core.
Handle generation failure or generation exhaustion rejects admission before
ownership transfer.

- A cancelled admission context rejects before ownership transfer and creates
  no registry entry.
- Cancelling an empty handle returns `empty_command_handle`.
- Cancelling an unknown, stale, or cross-core handle returns `not_found` and creates
  no tombstone.
- Cancelling the handle for an admitted or dispatched command competes for the terminal
  transition, cancels the command context, and publishes the command's single
  cancellation completion when it wins.
- Cancelling a terminal handle retained by the registry or bounded terminal history
  returns `already_terminal`.
- After final shutdown has released registry history, operations return the
  closed-gate result rather than preserving unbounded historical state.

If execution continues after cancellation, its later completion attempt sees
`already_terminal` and cannot publish a second result.

### Completion and overload behavior

`Complete` requires the `CompletionCapability` returned with the dispatch. It
is nonblocking. It returns `not_found` for an unknown active-gate ID,
`already_terminal` when another terminal transition already won,
`invalid_completion_capability` for an absent or command-mismatched capability,
and `stale_completion_capability` when the command ID has been readmitted with
a newer capability.

Before successful completion the caller owns the `model.Result` and all memory
reachable from `Result.Value` and `Result.Err`. A successful `Complete`
transfers exclusive ownership to the gate. A rejected `Complete` leaves
ownership with the caller and does not mutate the result. The gate treats an
accepted result as immutable; `NextCompletion` transfers that ownership to its
caller.

Completion-queue saturation is prevented structurally by the one-slot-per-live-
entry reservation. A full completion queue also means the registry is full, so
new commands are rejected until a consumer drains terminal completions. A
terminal transition and its queue publication are serialized, so the reserved
slot makes the completion send provably bounded. There is no error path after
the gate accepts completion-result ownership.

There is one logical completion consumer. `NextCompletion` polling and
`ClearCompletions` share a gate-owned mutex across dequeue and registry cleanup,
so one cannot observe an empty channel while another has dequeued a result but
has not released its terminal registry entry. Final shutdown uses the same
ownership lock before abandoning output. If a caller retains a completion lease
past the shutdown deadline, `Shutdown` returns on that deadline and one
lifecycle-owned finalizer waits for the exact commit or rollback; admission and
new dispatch remain closed, and later shutdown calls join the same cleanup.
`ClearCompletions` is valid only after shutdown has started; before shutdown it
returns `gate_not_closing`.

Event delivery is separate from command completion. Later Pod 2 event queues
must also be explicitly bounded, must fail closed or emit a bounded aggregate
overflow indication, and must not borrow command completion capacity. Events
cannot replace command completions.

Wave 2 uses one event queue with the configured runtime `QueueLimit`. Producers
push typed `model.Event` values and optional polling consumes that same queue;
there is no second polling queue and no SDK callback in the runtime. A
successful push transfers exclusive event ownership. Saturation is
nonblocking, retains caller ownership, and returns `event queue full`.
Runtime-owned lifecycle publication counts rejected events in one
fixed-cardinality aggregate metric so observability pressure cannot create an
unbounded stream of overflow events.

The queue is bounded independently by count and by dispatcher-owned dynamic
bytes. Its immutable ordinary byte budget is
`min(256 MiB - 1 KiB, QueueLimit * (PayloadLimit + 256 KiB))`, using checked
arithmetic. The metadata allowance covers bounded identifiers, headers, and
closed private error fields; count remains the bound for fixed-size typed
records. Admission sizes the closed event/payload variants before ownership
transfer. It charges caller slice backing capacity to reject tiny aliases of
large allocations, then clones all retained strings and slices, canonicalizes
timestamps to UTC, drops opaque error causes, and records the exact frozen
backing size. Accepted pointer variants are normalized to independently owned
value variants so an interior pointer cannot retain unrelated caller memory;
rejection does not mutate caller ownership. Queue, rolled-back
front, and active lease are ownership states of the same record and therefore
carry the byte charge exactly once. Commit, retirement, and shutdown release it
exactly once; abandoned payload bytes are cleared.

An active lease has already lent its event view to the sole consumer. If final
shutdown wins while that consumer is mapping or encoding, the dispatcher
invalidates the lease, releases its accounting, and drops its own reference,
but does not clear backing memory through the borrowed view. The stale lease
cannot commit or roll back afterward. Undelivered queue, front, and lifecycle
records remain zeroed on abandonment.

Shutdown owns a separate fixed two-event lifecycle reserve for exactly the
ordered `closing` and `closed` observations. It is unavailable to ordinary
event producers and does not enlarge configured application-event capacity.
The reserve owns at most 1 KiB total and at most 512 bytes per event, keeping
the dispatcher aggregate at or below 256 MiB even at the ordinary byte cap.
Beginning shutdown atomically stages `closing` and stops ordinary event
admission; cleanup stages `closed` into the remaining slot. Consumers drain a
rolled-back head and existing ordinary events before this reserve. Terminal
lifecycle publication is therefore nonblocking and bounded even when a
restored delivery occupies the final configured queue slot. The owned cleanup
deadline may abandon the reserve under the same local-output rule as the event
queue.

### Context, clock, and goroutine ownership

The caller owns the admission, dispatch-wait, execution-worker, completion-wait,
and shutdown deadline contexts it supplies. The gate checks admission context
cancellation before the atomic ownership handoff.

The gate owns one lifetime context and one child cancellation context per live
command. Local cancellation and shutdown cancel those child contexts. The
dispatcher must use the context returned in `Dispatch` for command work and the
matching capability for completion.

The gate creates no goroutines. Runtime code owns worker goroutines and calls
`ExecuteNext`; every worker therefore has a caller-owned termination context.
No blocking gate send creates an uncancellable wait.

The V1 runtime owns exactly one dispatcher worker. Its private, immutable
dependency table maps only allowlisted command names to typed handlers.
Handlers must perform prompt handoff; slow peer, RPC, connectivity, and payload
work belongs to the injected bounded subsystems. Unregistered and unknown
commands fail closed. Runtime lifecycle components start in declared order and
shut down or roll back in reverse order. Component and handler panics are
normalized without retaining recovered values.

`Runtime.Start` starts the kernel worker and injected lifecycle components but
leaves the session lifecycle at `created`. Credentials and personality arrive
later through `auth.login` or `auth.connect`. Authentication uses
`CommitAuthenticated`, not a series of separately visible transitions. Runtime
serializes the publisher callback against shutdown and every lifecycle change.
The callback stages the authenticated service graph and immutable personality
under its own publication lock, then calls the one supplied `commitReady`
function. Success atomically makes that graph and the final `ready` state
visible. Callback failure, panic, cancellation, shutdown, or a failed
`commitReady` leaves Runtime at `created` and requires the publisher to restore
its staged values before unlocking.

The commit admits the ordered lifecycle observation path `created ->
authenticating -> mesh_connected -> durable_ready -> peer_link_building ->
ready` through the existing bounded nonblocking event queue. Lifecycle event
retention is not part of graph/readiness atomicity: saturation retains the
ordered prefix that fits, rejects the remainder, and increments the bounded
rejection metric. Authentication never blocks waiting for an SDK event
consumer, including when the configured queue capacity is one. After
`commitReady` succeeds, the transaction is committed and cannot report a later
publisher failure.

A successful `auth.logout` is terminal for that Runtime. The gate publishes
the successful `EmptyResult` into the authoritative completion queue and then,
under the same admission lock, changes the gate to closing before any later
submission can be admitted. Shutdown completions for other live commands are
published after the logout completion. Runtime begins its ordinary exactly-once
background cleanup only after this gate transaction returns. A typed command
failure, handler error, contained panic, cancellation/shutdown win, or any
non-`EmptyResult` does not initiate terminal logout.

Process-local control commands retain the one-queue rules. Completion-channel
and event-sink registration identify the sole logical consumers of the existing
completion and event queues; registration never mirrors output, creates an
alternate queue, or installs or invokes an SDK callback. Clearing a registration
does not abandon gate/runtime-owned output; bounded output abandonment remains
part of shutdown. The command-side `delivery.next` seam is a nonblocking poll
of the same event queue so Runtime's sole dispatcher worker cannot wait for an
SDK consumer. Pausing command-side delivery retains events in that bounded
queue and therefore preserves its existing immediate saturation behavior.
Command-side polling permits one consumed event pending local acceptance. A
later `delivery.next` fails promptly until `delivery.accept` names that exact
event ID. Wrong, stale, and repeated IDs fail closed. Acceptance means only
that the SDK safely took ownership of or enqueued the event; it is not an
application-processing or network acknowledgment. Direct polling and callback
consumption compete for the same authoritative queue and do not create copies.
Language wrappers should automatically accept a command-polled event after
placing it safely into their own bounded delivery machinery.

`delivery.next` uses a private head-event lease rather than removing an event
at handler return. The gate resolves that lease inside the terminal transition:
a winning successful completion commits ownership before publishing the
completion, while cancellation, shutdown, handler failure, or any other losing
completion restores the event at the authoritative queue head before publishing
the competing completion. The lease retains queue capacity and exclusive
consumer ownership, so rollback preserves order without duplicate or lost
delivery. An owned shutdown deadline may abandon the lease only with the same
bounded local-output abandonment permitted for the event queue as a whole.
Final abandonment commits the dispatcher `closed` state before releasing an
active lease's consumer ownership. Every blocking, nonblocking, and leasing
consumer rechecks that state under the dispatcher lock before inspecting
buffered data, so no consumer can overtake finalization or create a new lease
in the lock handoff window.

Only application command and RPC timeouts are mutable after construction.
Presence of `QueueLimit` or `PayloadLimit` in a configuration update is an
explicit immutable-field rejection, including when the supplied value equals
the current value. Diagnostic log subscription gates only the closed,
support-safe `diagnostics.log` event variant and never forwards raw dependency
logs.

The accepted internal transition table is:

```text
created -> authenticating | closing | failed
authenticating -> mesh_connected | closing | failed
mesh_connected -> durable_ready | degraded | closing | failed
durable_ready -> peer_link_building | ready | degraded | closing | failed
peer_link_building -> ready | degraded | closing | failed
ready <-> degraded
ready | degraded -> closing | failed
failed -> closing
closing -> closed
closed -> terminal
```

Same-state requests are idempotent. Every other transition fails closed.
Injected handlers may request authenticated/health transitions but cannot
request `closing` or `closed`; only runtime shutdown owns those transitions.

Stage A uses no wall clock or timer. Deadlines are expressed by caller contexts.
Later runtime timers must use Pod 2's clock abstraction; command-gate code must
not introduce direct wall-clock calls.

### Panic containment

`ExecuteNext` contains panics at the private command-handler boundary. It emits
a typed private `command_handler_panic` failure without retaining the recovered
panic value, payload, stack, or other potentially sensitive detail. Handler
errors similarly become typed private `command_handler_failed` results.

If cancellation or shutdown reached terminal state while the handler was
returning or panicking, that earlier terminal result remains the only
completion.

The gate does not start goroutines, so higher-level runtime goroutine entry
points remain responsible for their own outer panic containment.

### Shutdown and drain

Shutdown is idempotent and follows this order:

1. Atomically move from open to closing and stop new admission.
2. Under terminal-publication ownership, move every nonterminal command to
   terminal and cancel its child context.
3. Cancel the gate lifetime context so dispatch waiters also wake.
4. Enqueue one shutdown completion for each terminal winner while retaining
   terminal-publication ownership.
5. Allow completion consumers to drain until the supplied shutdown context
   deadline.
6. When all live terminal entries are consumed, drain queued cancelled commands,
   release registry history, and enter closed.
7. If the deadline expires first, return to that caller and continue one
   lifecycle-owned finalizer. It abandons undrained local command and completion
   output, clears registry/history, and enters closed once any already-borrowed
   completion lease has committed or rolled back.

Deadline abandonment does not claim remote revocation and does not define the
durable networking outbox policy. That policy belongs to the runtime and
messaging/transport owners.

Concurrent shutdown callers are safe. Any caller whose deadline expires may
force final local cleanup; other callers observe the common closed state.

The implementation claims every live command's terminal transition under the
terminal-publication lock before cancelling the gate lifetime context. A
running handler awakened by cancellation therefore cannot race ahead with a
handler-failure result; shutdown remains the sole terminal completion.

Both the gate and runtime expose an idempotent nonblocking `BeginShutdown` in
addition to blocking `Shutdown`. Runtime begin-shutdown moves to `closing`,
stops gate admission immediately, cancels its worker context, and starts exactly
one background cleanup operation. That operation uses a Runtime-owned context
with a positive private cleanup timeout supplied by trusted composition. The
approved V1 composition value is 30 seconds; it is not SDK configuration and
Runtime has no hidden default.

Each `Runtime.Shutdown(ctx)` context bounds only that caller's join. A caller
deadline or cancellation returns the typed runtime shutdown-timeout error joined
with the context cause, leaves lifecycle at `closing`, and does not cancel or
poison background cleanup. Concurrent and later callers join the same cleanup;
a later caller may succeed after an earlier caller timed out. The shared cleanup
result contains only actual cleanup failures, never a previous caller's wait
timeout.

The owned cleanup waits for startup cancellation and worker exit, invokes every
started component exactly once in reverse order, and drains or abandons command
output under its own deadline. After component, worker, and gate cleanup is
complete, Runtime atomically sets status to `closed` and admits the final
ordered lifecycle event. It then performs bounded post-cleanup event delivery
drain or abandonment before completing `shutdownDone`. Therefore an observer
that receives `closed` also reads closed status, while successful Shutdown join
still means every Runtime-owned output has received its final disposition. The
owned deadline may abandon only undrained local output; it does not claim
remote revocation.

An in-flight component lifecycle call is ownership, not output, and cannot be
forcibly revoked by Go. If a component `Start` call fails to return before the
owned deadline, Runtime records the cleanup timeout but remains `closing` and
waits for startup to finish its reverse rollback. Runtime never snapshots or
stops the same component concurrently with that rollback, never stops a
component twice, and never publishes `closed` while a late startup component
can still become active. The expired owned context bounds the remaining
component and output cleanup calls once startup releases ownership.

The Runtime lifetime context is the direct parent of the startup context, so
`BeginShutdown` cancellation is synchronously visible without waiting for a
callback goroutine. Startup also checks caller cancellation, root cancellation,
and runtime shutdown state while atomically registering each component start
under the same Runtime lock used by `BeginShutdown`. That registration is the
component invocation's lifecycle linearization point. Shutdown may return with
one already-registered call classified as in flight, but no later call can be
registered after shutdown claims the lock. Startup retains exclusive cleanup
ownership for that call through its return and any reverse rollback. It also
checks cancellation immediately after every successful return and atomically
before publishing worker success. A component that ignores cancellation and
returns success after shutdown is added to startup's owned rollback set,
stopped exactly once in reverse order, and can never permit a later component
to start.

Runtime begin-shutdown has one serialized initiation owner. Concurrent callers
cannot start component/output cleanup until that owner has stopped admission,
cancelled the worker, and published the `closing` lifecycle event. This prevents
the shared event dispatcher from closing ahead of the closing event.

The same lifecycle-transition owner covers state mutation and lifecycle-event
admission for normal, failed, closing, and closed transitions. A transition
whose event publication stalls therefore cannot be overtaken by shutdown;
lifecycle events remain admitted in state-transition order.

## Rejected alternatives

- Unbounded channels or per-submission goroutines: resource use is not bounded.
- Closing command/completion channels: concurrent publishers could panic.
- Releasing capacity at dispatch: completion backpressure could then lose an
  already accepted command's terminal result.
- Permanent tombstones: identifier history grows without bound.
- Command-ID cancellation authority: a stale cancellation could affect a later
  admission that reused the same command ID.
- ID-only completion: a delayed worker could complete a later admission that
  reused the same command ID.
- Returning `model.Command` without its context: runtime work could bypass
  gate-owned local cancellation.
- Generic reflection-based copying of `Command.Args`: violates the typed package
  boundary and cannot define ownership for arbitrary reachable values.
- Treating admission as completion: overstates local execution and remote
  delivery.
- Cancellation as remote revoke: cannot be guaranteed after local handoff.

## Acceptance evidence

`internal/commandgate/gate_unit_test.go` covers:

- invalid, capacity-one, normal, saturated, drained, and recovered queues;
- immutable ownership transfer;
- empty, duplicate, unknown, cancelled, dispatched, and terminal IDs;
- bounded terminal cleanup and ID reuse after eviction;
- cancellation before admission, before dispatch, during execution, and after
  completion;
- high-entropy unique command handles, cross-core rejection, and stale
  cancellation rejection after command-ID reuse;
- concurrent completion attempts and completion/cancellation races;
- stale completion rejection after command-ID reuse;
- successful and rejected completion-result ownership;
- handler error and panic normalization;
- completion backpressure and reserved terminal slots;
- serialized concurrent completion polling and shutdown clearing;
- graceful shutdown, deadline abandonment, repeated/concurrent shutdown, and
  shutdown/completion races; and
- concurrent registry/status access.

The ADR may remain Accepted only while these tests pass normally and under the
Go race detector.

Wave 2 builder evidence additionally covers:

- deterministic fake-clock deadlines, equal-deadline ordering, cancellation,
  backward-movement rejection, and large advances;
- event capacity validation, ownership transfer, saturation, recovery, shared
  push/pull consumption, graceful drain, and deadline abandonment;
- allowlisted runtime handler injection and closed argument validation;
- one dispatcher worker, prompt sequential dispatch, handler panic
  normalization, and shutdown cancellation races;
- complete legal/illegal lifecycle transition-table coverage;
- deterministic component startup, failure at each injected component stage,
  reverse rollback/shutdown, and component panic normalization; and
- fixed-cardinality metrics, bounded resource snapshots, recursive redaction,
  and concurrent race-detector execution.
