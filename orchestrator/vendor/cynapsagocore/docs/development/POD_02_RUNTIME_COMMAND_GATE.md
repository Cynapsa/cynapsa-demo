# Pod 2: Runtime and Command Gate

Parent plan: [Cynapsa Go Core V1 Development, Validation, and QA Orchestration](PROJECT_DEVELOPMENT_VALIDATION_QA.md)

## Mission

Implement the bounded, race-safe execution kernel that owns command admission, cancellation, dispatch, completion, lifecycle, delivery queues, time, and private diagnostics. The runtime must remain independent of SDK callbacks and concrete connectivity implementations.

Pod 2 is responsible for exactly-once local command completion, deterministic shutdown, explicit backpressure, bounded resource ownership, and private event production for later projection by Pod 1.

## Required subagents

The root orchestrator must create:

1. `pod-02-builder`: implements Pod 2 production code and builder unit tests.
2. `pod-02-validator`: independently reviews concurrency, state machines, ownership, and failure semantics.
3. `pod-02-qa`: creates independent acceptance, race, stress, and timing tests without modifying production code.

The pod has an early Stage A decision checkpoint for ADR 0001, followed by full implementation after the Wave 1 contracts are frozen.

## Owned production files and folders

### Clock abstraction

```text
internal/clock/clock.go
internal/clock/fake.go
```

### Command admission and completion

```text
internal/commandgate/admission.go
internal/commandgate/cancellation.go
internal/commandgate/channels.go
internal/commandgate/gate.go
internal/commandgate/registry.go
```

### Internal delivery coordination

```text
internal/delivery/dispatcher.go
internal/delivery/pull.go
internal/delivery/push.go
```

### Private diagnostics and telemetry state

```text
internal/diagnostics/events.go
internal/diagnostics/metrics.go
internal/diagnostics/redaction.go
internal/diagnostics/snapshot.go
```

### Runtime lifecycle and dispatch

```text
internal/runtime/config.go
internal/runtime/dispatcher.go
internal/runtime/events.go
internal/runtime/lifecycle.go
internal/runtime/runtime.go
internal/runtime/shutdown.go
internal/runtime/startup.go
```

Pod 2 may add focused files inside these directories for queue implementations, state-transition tables, worker supervision, panic containment, resource accounting, and package-local tests.

## Owned documentation

```text
AZTM_COMMAND_GATE.md
docs/adr/0001-command-gate-encoding.md
```

Changes to the overall runtime description in `AZTM_GO_CORE.md` require root-orchestrator approval.

## Explicit exclusions

Pod 2 must not edit:

- Public schemas, `internal/model`, the SDK adapter, root facade, or native ABI owned by Pod 1.
- Private envelope, RPC, policy, identity, peer-lane, dedupe, or outbox
  semantics owned by Pod 3.
- Connectivity, peer, or handshake packages owned by Pod 4.
- Payload storage or transfer packages owned by Pod 5.
- Shared integration fixtures and `integration/**` owned by Pod 6.

The runtime dispatches typed private commands and results. It must not import SDK binding packages, invoke SDK callbacks, format SDK errors, or inspect concrete private connectivity types.

## Stage A: Command-gate decision

Before dependent runtime work is considered stable, the builder must resolve ADR 0001 and document:

- The concrete bounded queue design and capacities.
- Command ownership and immutable handoff rules.
- Admission meaning versus eventual completion.
- Registry state transitions and terminal cleanup.
- Cancellation before admission, after admission, during execution, and after completion.
- Exactly-once completion under cancellation, shutdown, panic, and concurrent completion attempts.
- Overload and backpressure behavior for command, completion, and event queues.
- Shutdown drain behavior and what can be abandoned after a deadline.
- Context and clock ownership.
- Panic containment and conversion into typed private failures.

The ADR can become Accepted only after race-enabled tests demonstrate these decisions.

## Builder implementation assignment

The builder must implement:

1. A bounded command registry with explicit states, capacity accounting, unique identifiers, terminal cleanup, and no unbounded tombstone growth.
2. Bounded admission and completion queues whose ownership and close order are unambiguous.
3. Local cancellation that wakes waiters promptly without claiming to revoke work already accepted elsewhere.
4. Exactly-once completion even when cancellation, execution, timeout, panic, and shutdown race.
5. A lifecycle state machine with an explicit transition table and safe behavior for repeated lifecycle calls.
6. Startup that allocates all resources before publishing readiness and rolls back partial initialization in reverse ownership order.
7. Shutdown that stops admission, cancels workers, drains or terminates outputs according to ADR 0001, and completes within its context deadline.
8. Runtime dispatch using typed private commands and typed private results without raw SDK values.
9. Internal event delivery with bounded push and pull semantics and a documented overflow policy.
10. Private diagnostics, metrics, snapshots, and redaction that remain internal until Pod 1 maps allowlisted values.
11. A real clock and deterministic fake clock suitable for race-free timing tests.
12. Panic containment at goroutine and handler boundaries, with cleanup and typed internal failure reporting.
13. Resource accounting for queues, registries, timers, worker lifetimes, and pending events.

Every goroutine must have an owner, termination condition, and testable shutdown path. Every blocking send or receive must be cancellable or provably bounded.

## Builder test requirements

Builder-owned tests must cover:

- Queue capacity zero, one, normal capacity, saturation, and recovery.
- Duplicate, empty, expired, cancelled, and terminal command identifiers.
- Cancellation at every command lifecycle point.
- Two or more concurrent completion attempts.
- Command panic, handler panic, context cancellation, and shutdown races.
- Completion and event backpressure.
- Startup failure at every allocation or worker-start stage with reverse cleanup.
- Repeated and concurrent Start, Status, Shutdown, and Destroy-facing runtime calls.
- Legal and illegal lifecycle transitions.
- Deadline expiration during graceful shutdown.
- Fake-clock timer ordering, equal deadlines, cancellation, and large time advances.
- Diagnostics redaction and bounded snapshot behavior.
- Race-detector execution and deterministic repeated runs.

## Validator assignment

The validator must verify:

- ADR 0001 precisely matches the code and tests.
- There is a single owner for each queue, channel close, registry entry, context cancellation function, timer, and goroutine.
- No send-on-closed-channel, double close, goroutine leak, timer leak, map race, or lock-order cycle is possible.
- Exactly-once completion holds under all race combinations.
- Backpressure cannot turn into unbounded goroutine creation or memory growth.
- Cancellation semantics do not overpromise remote revocation.
- Partial startup releases every resource acquired before failure. Timed-out shutdown releases queued and otherwise unborrowed resources; a non-cooperative dispatched borrow remains bounded, quarantined, and charged past the caller deadline until the dispatcher returns, then is zeroed and released exactly once. An already-borrowed completion lease similarly delays only the one lifecycle-owned finalizer until exact commit or rollback; the caller deadline still returns. The closed gate admits and dispatches no new work, and native unload requires joining the dispatcher and any retained completion lease.
- The runtime never calls an SDK callback or creates public serialized output.
- Private diagnostics do not retain secrets, payload content, or unbounded arbitrary attributes.
- Unknown commands and illegal state transitions fail closed.
- Tests use deterministic synchronization instead of timing sleeps wherever possible.
- Race-sensitive tests are repeated enough to expose probabilistic failures.

Any potential deadlock, lost completion, duplicate completion, resource leak, unbounded queue or registry, unsafe close, or alternate SDK-output path is blocking.

## QA assignment

The QA agent must add independent tests for:

- Saturation with many concurrent producers and slow consumers.
- Cancellation racing admission, dispatch, completion, and shutdown.
- Repeated startup and shutdown loops under the race detector.
- Injected panics and failures in every runtime stage.
- Consumers disappearing while queues are full.
- Deadlines firing at exact transition boundaries using the fake clock.
- Event and completion ordering under load.
- Registry reuse, identifier collisions, capacity release, and terminal cleanup.
- Memory and goroutine growth across long repeated runs.
- Diagnostics snapshots during concurrent state changes.
- Fuzzing of lifecycle operation sequences and command-registry transitions.

QA writes `*_acceptance_test.go` and `*_fuzz_test.go` only within Pod 2 directories. It must not edit production code or the shared integration directory.

## Pod 2 completion gates

Pod 2 is complete only when:

- ADR 0001 is Accepted and demonstrated by tests.
- All V1 functions in Pod 2-owned production files are implemented.
- No Pod 2 path returns a scaffold sentinel.
- Builder tests pass normally and with the race detector.
- Concurrency-sensitive tests pass repeatedly without flakes.
- Validator has no unresolved blocking finding.
- Independent QA tests pass under race and stress runs.
- Queue, registry, timer, goroutine, and shutdown resource bounds are proven by tests.
- The pod returns exact commits, commands, results, findings disposition, and limitations to the root orchestrator.
