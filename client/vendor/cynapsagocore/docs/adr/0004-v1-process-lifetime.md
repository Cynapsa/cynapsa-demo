# ADR 0004: Version 1 Process Lifetime

- Status: Accepted
- Scope: Core, native handle, callback, payload, buffer, shutdown, and library ownership

## Context

Python and TypeScript runtimes need predictable ownership across garbage collection, asynchronous callbacks, cancellation, host shutdown, and library unload. V1 deliberately has no process-restart continuity: a new process creates a new Core, handle namespace, dedupe state, request state, and SDK session.

## Decision

### Creation and authentication

`CoreCreate` strictly decodes a versioned public configuration containing process-local limits only. It creates the boundary adapter and local runtime resources, then publishes a core handle only after creation succeeds completely. Failed creation releases every partial allocation and never publishes a handle.

Authentication is a later command. Exactly one authentication command may select the Core's SDK personality. `auth.login` and `auth.connect` carry `mesh_endpoint`, `username`, `password`, and `mesh_id`. The additive `auth.token_login` and `auth.token_connect` alternatives carry only `token` and `mesh_id`; the private enrollment service supplies the endpoint, username, credential, and installation identity within the same bounded authentication operation. Once one personality is selected, another authentication command is rejected for that Core. Authentication secrets are input-only.

The private connectivity implementation authenticates its account identity,
binds the exact resource supplied by an e2 enrollment bundle or the mesh
resource used by a legacy password session, verifies the server-returned full
session identity, and keeps that full identity private. Public agent identifiers
remain opaque and transport-neutral. The installation-centric persistence and
resource model added after this ADR is defined in
[`ENROLLMENT_TOKEN_LOGIN.md`](../development/ENROLLMENT_TOKEN_LOGIN.md).

### Core lifecycle

Creation, startup, shutdown, and destruction follow this order:

```text
create -> start -> authenticate -> operate -> shutdown -> destroy
```

Creation does not start networking. Startup initializes bounded local runtime resources. Shutdown stops new admission immediately and begins ordered cleanup. Destruction permanently retires the native core handle after shutdown reaches `closed`.

If shutdown's caller deadline expires, the call returns normalized `shutdown_timeout`, the Core stays in `closing`, and cleanup continues. The caller must later observe `closed` or make a successful shutdown call before destruction or library unload. Timeout never abandons live callbacks or background cleanup.

Every successful admission returns a high-entropy opaque `command_handle` in addition to the application-visible `command_id`. Its exact shape is `cmdh_` followed by the unpadded base64url encoding of 40 bytes laid out as a 16-byte random Core scope, an 8-byte unsigned big-endian admission generation, and a 16-byte random nonce. The complete handle identifies exactly one admitted local wait; SDKs never parse it. `Core.Cancel` and `command.cancel` consume that handle, never `command_id`. SDK futures and request state retain the handle internally; applications normally do not manage it. Unknown, malformed, wrong-class, stale, cross-Core, and already-consumed command handles fail with normalized `invalid_handle`. Cancellation does not promise remote revocation of work already admitted by the Core.

### Numeric core and buffer handles

Core handles and returned-buffer handles are opaque nonzero `uint64` values in one coordinated, class-partitioned process-wide namespace. The two high bits carry an internal class tag: `01` for Core handles, `10` for reusable returned-buffer slots, and `11` for the one immutable emergency capacity descriptor described below. Core handles retain 62 random bits and reject zero payloads, live collisions, and retired collisions. A core handle issued once is never reused during the library process lifetime.

Returned buffers use the buffer class tag, a 12-bit slot index, and a 50-bit nonzero generation. There are exactly 4,096 reusable process-wide slots. Releasing a live buffer advances neither backwards nor through zero; the next use of its slot receives the next generation, so every older descriptor is stale. The final nonzero generation is valid, but after it is released that slot is permanently exhausted. Generation exhaustion never wraps and allocation fails closed.

The core registry retains a process-lifetime retired tombstone and permits at most 1,048,576 total core-handle issuances per process. The ceiling bounds core tombstone memory and fails further core allocation closed. Transient returned buffers do not create process-lifetime tombstones.

No Go pointer, object address, or allocator address is a durable ABI handle. Handle lookup is synchronized. Unknown handles, cross-class handles, and operations on retired handles fail with normalized `invalid_handle`, except for the explicitly idempotent operations below.

`CoreDestroy` is idempotent only for a handle that was previously issued and retired. An arbitrary never-issued handle is invalid. Destruction atomically removes the live Core before releasing its resources, so concurrent new operations cannot acquire it.

### Command, payload, and request handles

Request handles are exactly `reqh_` plus unpadded base64url encoding of 32 random bytes. Payload handles are exactly `payh_` plus unpadded base64url encoding of 32 random bytes. Command handles use the separately frozen scope/generation/nonce layout above. These class-distinct values are scoped to one Core/session and one handle class. They contain no pointers, paths, storage references, connectivity terminology, or reusable credentials. Their owning Core registry rejects unknown, malformed, cross-Core, stale, consumed, and wrong-class use. SDKs treat the complete strings as opaque despite the boundary-owned command layout.

Command handles are capability identifiers for local cancellation only. They are distinct from application-visible command IDs and private message correlation. A command handle is retired when its admission can no longer be cancelled; retired handles are never reassigned within that Core's process lifetime.

Payload handles identify an immutable snapshot of the complete canonical payload, including its variant metadata and exact body. Payload references are counted atomically. `PayloadRetain` adds one ownership reference. `PayloadRelease` consumes exactly one reference; releasing without an owned reference is `invalid_handle`. Final release destroys the payload's local resources. `PayloadCancel` is idempotent while the handle record still exists, but it does not create an extra ownership release. Request handles are expiring and single-use for one inbound application reply.

`payload.close` is exactly one `PayloadRelease` expressed through the command
surface. It has identical invalid-handle and double-release behavior and does
not add a second lifecycle operation.

### Returned buffers

Serialized ABI operations return a pointer-free descriptor containing `buffer_handle uint64` and `byte_length uint64`. `BufferRead(buffer_handle, offset, host_destination)` copies a bounded range into caller-owned memory. No Go-managed pointer, slice descriptor, allocator address, or direct byte view is returned to the host; host-supplied input and destination memory is copied during the call. The host reads or decodes the copied bytes, then calls `FreeBuffer`; ordinary descriptors must be freed exactly once, while the exceptional process-owned descriptor follows the idempotent rule below.

`FreeBuffer` removes and clears an ordinary wrapper allocation. Double free, unknown handle, and use after free return `invalid_handle`. Host allocators never free wrapper memory, and wrapper allocators never free host memory. Every exported ABI function zeroes each non-null scalar or descriptor output at function entry, before validating any argument (including another required output) and before installing recoverable panic handling. A failed call therefore cannot leave stale caller sentinels in an output. `BufferRead` applies the same rule to both `out_copied` and the complete error descriptor.

Buffer allocation reserves one slot and the complete encoded byte count before cloning. At most 4,096 returned buffers and 256 MiB of their bytes may be live process-wide, and one encoded result may not exceed 64 MiB. Rejection changes neither live accounting nor caller-owned bytes and maps to normalized local capacity. Final free removes the generation, releases its count and bytes, and clears its owned clone exactly once while excluding concurrent reads.

Error reporting must remain available when normal error encoding succeeds but its returned-buffer allocation cannot reserve a slot or bytes. Exactly one closed process-owned descriptor exists for that condition. Its fixed handle is `0xc000000000000001` (class bits `11`, reserved payload value `1`), its length is 141 bytes, and its immutable canonical contents are:

```json
{"abi_version":1,"error":{"code":"queue_full","message":"The local queue is full","retryable":false,"stage":"sdk","local_or_remote":"local"}}
```

This descriptor is outside the 4,096 live-slot and 256 MiB live-byte accounting, is never cloned, mutated, cleared, retired, or generation-reused, and supports the same bounded offset reads as an ordinary descriptor. `FreeBuffer` recognizes only that exact reserved handle and is an idempotent no-op for it. Every other class-`11` value is invalid. Only failure to allocate an already normalized error document may select this descriptor; result, completion, event, status, payload, callback, and arbitrary data allocation cannot use it to bypass capacity. Multiple callers may read and free the descriptor concurrently. Its fixed document reports `queue_full` because capacity to return the original error itself has been exhausted; it never contains the displaced error or dependency text.

The byte-heavy ordinary result is a payload-read document containing one 1 MiB chunk. Its deterministic JSON size is the fixed payload-read framing plus RFC 4648 base64 expansion; completion, event, status, and error documents remain below the 64 MiB result ceiling. A pathological valid unpaginated list whose JSON would exceed 64 MiB fails closed. Pagination or streaming for those list commands is future contract work and is not added to the frozen V1 ABI here.

### Callbacks and polling

Polling and callbacks consume the same already-normalized public completion/event values. Registering callbacks does not give internal packages an SDK callback reference.

Native polling and callback delivery reserve the authoritative queue head, encode it, and obtain an ordinary quota-accounted ABI buffer before committing queue ownership. Buffer count or byte pressure restores the exact head and waits on a cancellation-aware capacity generation; a concurrent free cannot be missed. Deterministic encoding, item-size, clone, teardown, or dispatch-admission failure restores the head and frees any untransferred descriptor exactly once. Polling may then win the same single-consumer competition. No callback result uses the emergency error descriptor or a second data queue.

The private typed-event dispatcher has its own aggregate 256 MiB ceiling: an
ordinary derived byte budget plus a fixed 1 KiB two-lifecycle-event reserve.
Its queue/front/active-lease accounting follows typed event ownership and is
separate from the process-global 4,096-buffer/256 MiB native descriptor quota.
Accepted internal pointer variants are converted to independent value
snapshots before queue publication, so neither the queue nor a later native
descriptor can retain an uncharged caller allocation through an interior
pointer. Final shutdown may invalidate an active event lease while encoding is
in progress; it releases dispatcher accounting and references without clearing
through the consumer's borrowed view, and that stale lease cannot commit or
roll back. Undelivered records are still cleared on abandonment.
While a callback or poll encodes a leased event, both ownership domains may
legitimately coexist; each accounts its own allocation exactly once, so neither
quota bypasses or double-counts the other. Descriptor allocation failure rolls
the still-charged event lease back rather than consuming it.

Tracked `message.received` callback ownership is one-at-a-time. After dispatching
one tracked event, the event pump waits for its exact mandatory
`delivery.accept` before leasing another tracked event. Ordinary completion and
event queues remain separate ownership domains.

Concurrent callback registration has exactly one winner. A candidate that loses publication is disposed promptly without starting queue consumers or waiting for the callback-teardown timeout.

`ClearCallbacks` first prevents new callback dispatch, rolls back every prepared but uncommitted delivery, frees its descriptor, and waits for every in-flight callback to return before releasing callback state. Shutdown performs the same bounded quiescence step. A binding must not call `ClearCallbacks`, shutdown, destroy, or unload reentrantly from the callback currently being cleared; detected reentrancy is rejected with a normalized local state error rather than self-deadlocking.

Callbacks never run after successful callback clearing, closed shutdown, or destruction. Slow callbacks cannot block networking workers; only the bounded boundary-owned callback dispatcher may wait on them.

### Idempotency summary

```text
CoreDestroy(issued-and-retired handle)       idempotent success
PayloadCancel(existing handle)               idempotent cancellation
PayloadRelease                               one call per owned reference
FreeBuffer(ordinary descriptor)              exactly once
FreeBuffer(emergency capacity descriptor)    idempotent no-op
all arbitrary unknown handles                invalid_handle
all stale non-destroy operations              invalid_handle
```

### Library unload and process exit

Orderly library unload is allowed only after:

1. every Core is closed and destroyed;
2. callbacks are cleared and quiescent;
3. every ordinary returned buffer is freed (the process-owned emergency descriptor has no releasable allocation);
4. every SDK-owned payload reference is released; and
5. no ABI call is in flight.

Bindings must register process-exit cleanup as a best-effort safety net, but process exit is not a substitute for explicit shutdown. The library never persists V1 handle, callback, RPC, dedupe, or process-lifetime state for reuse by a later process.

### ABI version and timeout representation

Every serialized native input and output declares `abi_version`. V1 uses strict deterministic JSON. Unknown or duplicate fields, unknown command/result/event discriminators, malformed values, trailing data, oversized input, and unsupported versions fail closed before runtime admission.

Cross-language timeouts are signed integer milliseconds. Zero selects the configured default. Negative values are invalid. Poll and shutdown functions use bounded nonnegative millisecond arguments; an expired wait is distinct from command cancellation, shutdown, invalid handle, malformed input, unsupported version, and internal failure.

ABI capacities and chunk limits are fixed-width unsigned integers. Core `queue_limit`, command-channel and event-sink registrations, completion-channel `max_in_flight`, and queue-pressure event capacities are always in the inclusive range 1..65,536; zero is not a capacity default. V1 accepts 256 KiB in one inline canonical payload, 1 MiB in one payload read/write chunk, and 134,217,696 bytes in one complete canonical payload snapshot. Invalid or larger values fail during Core creation, before authenticated runtime construction, allocation, admission, projection, or serialization. Timeout zero-default behavior remains unchanged.

## Consequences

- Handles cannot accidentally become Go pointers or be silently reused after destruction.
- Retired core tombstones consume bounded process memory proportional to issued core handles; reusable generation-bound buffers consume only their fixed slot table and live owned bytes.
- Shutdown timeout does not make immediate unload safe; the host must wait for eventual closure.
- Buffer and payload ownership is explicit across Python, TypeScript, Go garbage collection, and host allocators.
- Bindings must prevent or surface forbidden teardown reentrancy from callbacks.
- A process restart begins a clean V1 runtime with no continuity assumptions.

## Acceptance evidence

Stage A unit and architecture tests cover strict schema versioning, duplicate/unknown-field rejection, exact-one canonical payload variants, integer-millisecond and fixed-width size validation, strong opaque handle formats, the class-partitioned numeric namespace and core issuance ceiling, class and collision rejection, retired core-handle non-reuse, idempotent issued-handle destruction, generation-bound buffer reuse and exhaustion, pointer-free buffer descriptors, exact count/byte/result limits, copy-only buffer reads, invalid double buffer free, callback clearing, and callback quiescence.

Stage B and system qualification must additionally cover repeated create/start/shutdown/destroy, failed-initialization cleanup, cross-Core payload use, concurrent retain/release, forced shutdown timeout with continuing cleanup, callback teardown races and forbidden reentrancy, concurrent handle destruction, native allocator integration, sanitizer-enabled native tests, and clean library unload. ADR acceptance does not waive those later implementation gates.
