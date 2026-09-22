# Pod 6: System Integration and Release QA

Status: current system-integration and release-QA plan

Parent plan: [Cynapsa Go Core V1 Development, Validation, and QA Orchestration](PROJECT_DEVELOPMENT_VALIDATION_QA.md)

Next mandatory stage: [Pod 7: Production Qualification](POD_07_PRODUCTION_QUALIFICATION.md)

## Mission

Independently prove that the integrated Cynapsa Go Core behaves correctly as one system. Pod 6 owns shared test infrastructure, a disposable real ejabberd environment, cross-pod integration specifications, a full external black-box end-to-end tester, native-build verification, fault injection, release test orchestration, and the final evidence matrix.

Pod 6 does not own production behavior. When a system test fails, it identifies the owning pod and sends a reproducible failure to that builder. It must not patch production code, relax assertions, bypass the SDK boundary, or mark required tests skipped.

Pod 6 completion establishes functional E2E readiness. It does not by itself establish production qualification; the exact Pod 6-approved candidate must proceed through Pod 7.

## Required subagents

The root orchestrator must create four independent agents:

1. `pod-06-integration-builder`: implements only shared test infrastructure, integration tests, CI test runners, and release evidence tooling.
2. `pod-06-validator`: reviews whether the tests are independent, adversarial, deterministic, complete, and capable of detecting real defects.
3. `pod-06-release-qa`: executes the full release matrix from a clean candidate, investigates failures, and produces the final QA disposition without modifying production code.
4. `pod-06-e2e-tester`: operates only through public Go or native SDK boundaries against real compiled cores and the disposable ejabberd environment; it owns the positive/negative feature traceability matrix.

The agents must start from the integrated, individually approved outputs of Pods 1–5.

## Owned files and folders

### System integration specifications

```text
integration/binding_parity_test.go
integration/file_transfer_fallback_test.go
integration/loopback_test.go
integration/offload_test.go
integration/rank2_test.go
integration/rank_transition_test.go
integration/rpc_test.go
integration/sdk_boundary_test.go
```

### Shared private test fixtures

```text
internal/testkit/envelope.go
internal/testkit/peer.go
internal/testkit/transport.go
```

### End-to-end environment and black-box suites

```text
test/e2e/**
test/e2e/ejabberd/**
test/e2e/fixtures/**
test/e2e/runner/**
```

Pod 6 may add:

```text
integration/*_test.go
internal/testkit/**
test/e2e/**
scripts/e2e-*.sh
scripts/test-*.sh
scripts/check-*.sh
.github/workflows/*test*.yml
.github/workflows/*e2e*.yml
Makefile test targets, if the repository adopts a Makefile
testdata/** containing non-sensitive deterministic fixtures
```

Repository-level runner and workflow changes require root-orchestrator review because they affect every pod. Pod 6 must not add secrets or production endpoints to fixtures, CI, or documentation.

## Explicit ejabberd container authorization

The Pod 6 integration builder, release-QA agent, and E2E tester are authorized to create, configure, start, stop, inspect, and remove disposable local ejabberd containers required by this test plan. They may create dedicated test networks, generated test-only accounts, ephemeral certificates, temporary configuration, and temporary volumes.

The environment must:

- Pin the ejabberd image by immutable digest for release evidence.
- Bind host ports only to loopback when host access is required; otherwise use an isolated container network.
- Use generated test-only credentials and synthetic agent identities.
- Enable and configure the features required for authentication, discovery, establishment signaling, reliable fallback, stream resume, offline delivery, and XEP-0363 HTTP upload.
- Provide separate profiles with XEP-0363 enabled, disabled, unavailable, quota-rejected, upload-failing, and download-failing behavior.
- Expose a readiness check that proves the required service behavior, not merely that the container process is running.
- Impose CPU, memory, storage, upload-size, stanza-size, and log-size limits.
- Capture sanitized logs and configuration as test artifacts.
- Tear down containers, networks, temporary files, and volumes on success, failure, cancellation, and timeout.

The environment must not use privileged mode, host networking, the host Docker socket inside a container, production credentials, public production endpoints, or broad host filesystem mounts. Container authorization is limited to disposable test infrastructure for this repository.

## Explicit exclusions

Pod 6 must not modify production files owned by Pods 1–5. It must not:

- Implement missing production functions.
- Add test-only behavior to normal production builds.
- Expose private packages through a public API for easier testing.
- Replace real boundary behavior with direct internal calls in public integration tests.
- weaken size, timeout, integrity, policy, identity, or ownership rules.
- convert a failing required test into a skip.

Package-private test hooks are acceptable only when designed by the owning pod, unreachable in production, and reviewed by that pod's validator.

## Integration-builder assignment

The integration builder must replace every scaffolded integration skip with real assertions and build reusable test infrastructure for:

1. Multiple isolated core instances in one process.
2. Deterministic loopback delivery with injected transport-frame delay,
   duplication, loss, reordering, partial failure, and disconnect.
3. Disposable local reliable-fallback infrastructure with test-only credentials.
4. A pinned and configurable ejabberd container with enabled and failing service profiles.
5. Preferred-path establishment and forced path transitions.
6. DTLS direct file chunks, HTTPS XEP-0363 object transfer, and V1 plaintext XMPP chunk fallback over TLS.
7. Local oversized-payload object transfer with adversarial server modes.
8. Native shared-library build, load, symbol lookup, call, buffer free, callback, and unload behavior.
9. Serialized ABI vectors shared across language binding conformance tests.
10. Multi-process black-box agent runners that use only supported public boundaries.
11. Leak observation for goroutines, file descriptors, temporary files, handles, timers, connections, buffers, containers, networks, and volumes.
12. Deterministic time and seeded randomness where reproducibility is required.
13. Sanitized logs and artifacts that do not expose secrets or private payload contents.

Test infrastructure must fail loudly when a prerequisite is missing. It must never fall back to a production endpoint or silently reduce the test scope.

## Mandatory integration scenarios

### Public lifecycle and loopback

Implement `integration/loopback_test.go` to verify:

- Create, start, authenticate or initialize, become ready, submit, complete, receive, status, shutdown, and destroy.
- Two or more isolated cores in one process.
- Repeated lifecycle loops.
- Sync polling and callback consumption where both are supported.
- Cancellation, timeout, queue saturation, and shutdown with outstanding work.
- No private terminology or structures in any public output.

### Reliable fallback connectivity

Implement `integration/rank2_test.go` against disposable infrastructure:

- Authentication success and failure.
- Direct delivery, offline delivery, reconnect, resume, resume failure, and catch-up.
- Credential removal and shutdown.
- No production credentials, endpoints, or data.
- Real ejabberd service discovery, authentication, stream resume, offline mailbox, and XEP-0363 capability behavior rather than only simulated adapters.

The filename is private test terminology and is not an SDK surface.

### Path transition and replay

Implement `integration/rank_transition_test.go` to verify:

- Preferred-path establishment.
- Interruption before, during, and after send.
- Ambiguous-send replay through fallback with the same logical identity.
- Receiver duplicate suppression across all carrier attempts.
- Recovery without duplicate application invocation or identity replacement.
- Flapping, cooldown, and simultaneous establishment.

### Request-response lifecycle

Implement `integration/rpc_test.go` to verify:

- Request success, remote application error, timeout, local cancellation, and shutdown.
- Single-use inbound reply handles.
- Duplicate and late response rejection.
- No response loops.
- Correlation cleanup and capacity recovery.

### Large-payload transfer

Implement `integration/offload_test.go` to verify:

- Inline boundary and each ADR 0005 carrier.
- Healthy live-link direct bounded binary chunks.
- Live-link loss before transfer, mid-transfer, after receiver completion, and before sender completion evidence.
- XEP-0363 success when no usable live link exists.
- XEP-0363 unsupported, disabled, slot-rejected, quota-rejected, upload-failing, timed-out, corrupt-download, download-failing, and unavailable profiles, including receiver failure that triggers XMPP chunk fallback after upload acceptance.
- Automatic V1 plaintext XMPP chunk fallback over TLS when XEP-0363 fails and no live link is usable.
- XMPP transfer-frame loss, duplication, reordering, conflicting duplicate,
  sender mismatch, stanza rejection, expiry, and reassembly quota exhaustion.
- Canonical bytes, transferred and canonical integrity, V1 plaintext
  reassembly/materialization, rejection of unsupported encryption references,
  the isolated future encryption seam, and exactly-once public delivery.
- Preservation of one logical message identity across all carrier attempts.
- Terminal normalized failure when direct, XEP-0363, and XMPP chunk delivery are all unavailable or exhausted.
- Tamper, truncation, redirect, blocked address, excessive size, timeout, cancellation, and cleanup.
- No partial payload, chunk event, carrier name, transfer identifier, private URL, key, storage descriptor, or dependency error in SDK output.

### Full black-box end-to-end feature matrix

The dedicated E2E tester must run compiled core instances as separate agent processes and interact only through the public Go facade or native ABI. Direct imports of `internal/**` are forbidden in `test/e2e/**`.

For every public V1 feature, maintain a traceability row with:

```text
feature or requirement identifier
positive scenario
required rejection or failure scenario
test name
environment profile
expected public result or error
observed result
artifact reference
```

The matrix must include at least:

- Core create, start, status, shutdown, destroy, repeated lifecycle, and invalid lifecycle calls.
- Authentication success, bad credentials, unavailable server, revoked or missing credentials, and reconnect.
- Mesh credential put/remove, membership refresh/list, allowed membership, and denied membership.
- Address mapping put/remove/list/resolve, malformed origins, unknown destination, and conflicting mapping.
- One-way send, request, reply, response error, timeout, cancellation, duplicate request, late response, and reply-handle reuse.
- Delivery pull/push behavior, acknowledgement or control behavior, queue saturation, and consumer disappearance.
- Independent peer progress, scoped message-ID duplicates (including
  changed-content reuse), carrier replay, and dedupe-capacity pressure.
  Duplicate classification never compares content.
- Policy allow, deny, malformed rule, missing identity, wrong mesh, unauthorized action, and stale membership.
- Preferred live-link establishment, simultaneous establishment, failure, fallback, recovery, flapping, and clean shutdown.
- Reliable fallback direct delivery, offline delivery, resume, resume rejection, mailbox catch-up, and duplicate suppression after recovery.
- Small inline payloads and the complete large-payload matrix above.
- Payload-handle open/write/finish/read/retain/release/cancel plus invalid, stale, cross-core, over-limit, and double-release cases.
- Status, capabilities, diagnostics, events, error normalization, unknown internal variants, and public-boundary leakage scans.
- ABI version mismatch, malformed serialized input, invalid numeric handles, buffer double-free, callback teardown, and library unload.

Every feature needs both a success assertion and the failures required by its contract. “The call returned” is not sufficient: tests must verify exact public state, identity, payload bytes, normalized errors, side effects, cleanup, and absence of forbidden output.

### SDK abstraction boundary

Implement `integration/sdk_boundary_test.go` to verify:

- Unknown, malformed, oversized, duplicate, and unsupported public input fails closed.
- Representative private errors, panics, states, events, diagnostics, and payload descriptors map to bounded public values.
- Every public output surface passes the forbidden-vocabulary and raw-private-data scanners.
- Dependency upgrades or new internal variants cannot automatically change public output.

### Binding parity

Implement `integration/binding_parity_test.go` and shared vectors to verify:

- Native ABI version and schema vectors are deterministic.
- Equivalent Python and TypeScript calls produce equivalent admissions, completions, events, errors, status, and payload-handle behavior.
- Both bindings free buffers, release handles, stop callbacks, and shut down correctly.
- Unsupported versions fail predictably.

If binding repositories are unavailable, the Go ABI vector generation and native verification must still be completed. Cross-language execution is then reported as an explicit external blocker, not a pass.

## Integration validator assignment

The validator must inspect tests and test infrastructure for false confidence. It must verify:

- Tests exercise supported boundaries instead of duplicating implementation logic.
- Assertions validate identity, payload bytes, errors, lifecycle, cleanup, and
  absence of leaks, not only lack of exceptions.
- Fault injection reaches the intended internal transition and cannot be active in production.
- Disposable infrastructure matches the protocol behaviors needed by the test.
- The ejabberd container is pinned, isolated, resource-bounded, configured for the claimed features, proven ready at the protocol level, and always torn down.
- XEP-0363-disabled and failing profiles genuinely force V1 plaintext XMPP chunk fallback over TLS rather than a mock shortcut.
- Black-box E2E tests import no private package and cannot alter private state directly.
- Every public V1 feature has at least one positive and one required negative traceability row.
- Tests fail when the targeted defect is intentionally introduced or the relevant implementation is disabled.
- Timeouts distinguish a product failure from a hung test and produce useful diagnostics.
- Randomized tests log their seed and are reproducible.
- Secret and forbidden-vocabulary scans examine all output channels and generated artifacts.
- Skips and environment conditions are explicit, narrow, and excluded from required release behavior.
- CI commands actually execute race, fuzz-smoke, integration, and native-build jobs rather than reporting only compilation.

Any tautological test, production bypass, silent infrastructure fallback, required skip, assertion-free path, or unexamined output channel is blocking.

## Release-QA assignment

The release-QA agent must operate from a clean integrated candidate and run:

1. Formatting and diff checks.
2. All unit, acceptance, and integration tests without cache.
3. The full race-detector suite.
4. Repeated concurrency-sensitive packages and integration scenarios.
5. Required fuzz targets for the agreed smoke and extended durations.
6. Native shared-library builds and ABI smoke tests on every available supported platform.
7. Disposable fallback and object-transfer infrastructure scenarios.
8. Real ejabberd profiles for normal, offline, resume, XEP-0363-disabled, and XEP-0363-failing behavior.
9. Full black-box E2E positive and negative feature matrix.
10. Public dependency and forbidden-vocabulary architecture tests.
11. Vulnerability and dependency policy checks using pinned tools.
12. Leak, shutdown, stale-handle, callback-teardown, repeated-lifecycle, and container-teardown tests.
13. Python and TypeScript conformance suites when their repositories are available.

The QA agent records exact commands, environment, tool versions, test counts, fuzz duration and seeds, skipped tests, failures, reruns, and artifact locations. A rerun that passes after an unexplained failure remains a flake and blocks release until diagnosed.

## Defect routing

Pod 6 routes failures by ownership:

| Failure area | Owning pod |
| --- | --- |
| Public schema, ABI, mapping, handle, callback, or SDK leakage | Pod 1 |
| Runtime lifecycle, queue, cancellation, delivery, or diagnostics | Pod 2 |
| Codec, identity, dedupe, RPC, policy, mesh, peer lanes, or outbox | Pod 3 |
| Peer, establishment, health, private path, reconnect, direct chunk carrier, XMPP chunk carrier, or transition | Pod 4 |
| Serialization, payload handle internals, transfer selection, manifest, reassembly, integrity, encryption, XEP-0363, or cleanup | Pod 5 |
| Test harness or false assertion | Pod 6 integration builder |

After an owning builder fixes a defect, that pod's validator and affected QA scope rerun before Pod 6 accepts the new candidate.

## Required final commands

The exact CI matrix may expand this list, but it may not omit these repository-wide gates:

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
git diff --check
```

Pod 6 must also execute the committed boundary scanner, architecture dependency tests, native-build tests, required fuzz targets, real ejabberd container profiles, full E2E traceability suite, and disposable-service suites. Commands that require platform-specific tooling must be documented and run on the corresponding CI worker.

## Pod 6 completion gates

Pod 6 is complete only when:

- Every required integration stub is replaced with executable assertions.
- No completed V1 behavior remains skipped.
- Shared fixtures are deterministic, bounded, non-sensitive, and production-inaccessible.
- The disposable ejabberd environment covers required normal and failure profiles and proves teardown after failed tests.
- The integration validator has no unresolved blocking finding.
- Release QA passes the full clean matrix without unexplained flakes.
- The independent E2E tester has executed every positive and negative feature row through public boundaries and found no unexplained mismatch.
- Every discovered production defect was routed to its owning pod and passed that pod's validator and QA after correction.
- Native ABI, lifecycle, path transition, RPC, all three large-payload carriers, automatic fallback, real ejabberd, boundary, shutdown, cleanup, and leak scenarios pass.
- Cross-language parity passes or unavailable binding repositories are recorded as precise external blockers.
- A final evidence report with exact commits, commands, versions, results, limitations, and artifacts is delivered to the root orchestrator.
- The exact approved candidate commit and Pod 6 artifacts are handed to Pod 7 for production qualification.
