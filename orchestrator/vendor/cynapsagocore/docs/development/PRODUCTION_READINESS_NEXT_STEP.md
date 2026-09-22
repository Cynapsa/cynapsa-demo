# Production Readiness: Missing Work and Next Execution Step

Status: current production-readiness gap register

Parent plan: [Cynapsa Go Core V1 Development, Validation, and QA Orchestration](PROJECT_DEVELOPMENT_VALIDATION_QA.md)

Execution owner: [Pod 7: Production Qualification](POD_07_PRODUCTION_QUALIFICATION.md)

## Purpose

The current architecture, pod plan, and scaffold are sufficient to begin implementation, but they are not sufficient evidence for a production release. This document records the missing production work and turns it into mandatory outcomes for the Codex harness.

When the master plan is executed, Codex must complete this document after functional implementation and Pod 6 system E2E testing. It must not reinterpret these gaps as future ideas, optional hardening, or post-release work.

Production readiness means that the actual compiled core and connected language SDKs have demonstrated correct behavior under real networking, failure, concurrency, security, resource, compatibility, packaging, and long-duration conditions.

## Current evidence boundary

The repository currently contains:

- Normative architecture and SDK-boundary rules.
- Production package and ABI scaffolding.
- Pod-specific development, validation, and QA instructions.
- A functional E2E design using disposable ejabberd.
- An accepted multi-carrier large-payload transfer decision.
- Skipped integration and E2E test specifications.

The repository does not yet contain completed production implementations, real language-binding E2E execution, a real traversal/relay matrix, network-fault infrastructure, approved performance objectives, long-duration results, compatibility evidence, or releasable native artifacts.

Compilation of stubs is not production evidence.

## Gap 1: Real WebRTC traversal and relay

### Missing

Ejabberd verifies signaling and reliable fallback behavior, but it does not prove that agents can establish and maintain WebRTC across real traversal and relay conditions.

### Required work

- Add a disposable, pinned Coturn environment providing test-only STUN and TURN services.
- Exercise direct host candidates where available.
- Exercise server-reflexive traversal where the test environment supports it.
- Force TURN solely through test network/router restrictions while Core keeps
  its ordinary `all` ICE policy, and prove application traffic uses the relayed
  path.
- Test TCP and UDP relay modes that are declared supported.
- Exercise authentication failure, expired credentials, unreachable relay, allocation rejection, permission failure, channel failure, and relay restart.
- Exercise in-place ICE restart and complete peer-link replacement.
- Prove that public SDK output remains transport opaque in all cases.

### Exit evidence

- Reproducible container configuration pinned by digest.
- Protocol-level readiness checks.
- Black-box successful direct and forced-relay transfers.
- Failure and recovery results with exact seeds and artifacts.
- Clean teardown with no remaining container, network, volume, process, or credential file.

### Current Go-core evidence

The repository now contains the pinned disposable ejabberd, STUN, TURN, and
per-agent NAT environment. Ejabberd XEP-0215 is the sole endpoint and
short-lived credential authority, and readiness performs a real allocation
with the authenticated discovery result. The environment proves
server-reflexive direct traversal, server-authorized UDP relay, TCP relay with
UDP blocked, unavailable/restarted TURN, exact unsupported-relay allocation
rejection, exact permission rejection, established-channel loss, in-place ICE
restart, and bounded fallback to complete make-before-break peer-link
replacement. The
31-second channel interruption leaves XMPP available, proves durable
request/reply during demotion, then refreshes the existing authenticated
link through structured Jingle before a post-recovery request/reply. A real
Pion pair proves the same data channel carries traffic before and after that
restart. Public output remains transport-neutral. This closes the declared
Go-core traversal and relay matrix; broader impairment and soak qualification
remain tracked below.

## Gap 2: Controlled network fault injection

### Implemented qualification slice

`test/production/faults` now provides an immutable Toxiproxy environment for
the production XMPP and XEP-0363 clients. Versioned profiles cover XMPP
latency, half-open sockets, TCP reset, HTTP bandwidth/backpressure, HTTP stall,
HTTP reset, and abrupt proxy restart with bounded failure or recovery. The
environment is bridge-isolated, exposes only random loopback ports, drops all
container capabilities, and verifies complete teardown. UDP packet
impairment, DNS/TLS failure, and individual service-restart profiles remain
open below.

### Missing

Package fakes can verify deterministic state transitions, but they cannot prove behavior under real sockets, TCP backpressure, HTTP stalls, UDP loss, half-open connections, or server restarts.

### Required work

- Add a pinned TCP/HTTP fault proxy for ejabberd and XEP-0363 traffic.
- Add isolated UDP and packet impairment for WebRTC tests using a dedicated network namespace or test container.
- Cover latency, jitter, packet loss, duplication, reordering, bandwidth restriction, connection reset, half-open connections, DNS failure, TLS failure, and abrupt container restart.
- Never impair the host network globally.
- If packet impairment requires `CAP_NET_ADMIN`, grant it only to the dedicated disposable fault container or namespace; privileged mode and host networking remain prohibited.
- Record every fault profile as versioned test configuration.

### Exit evidence

- Each fault profile is reproducible from a clean machine.
- Tests prove expected recovery or normalized failure within bounded time.
- No unexplained flake passes after retry.
- Fault infrastructure cannot be enabled in production builds.

## Gap 3: True multi-process and multi-agent behavior

### Missing

Multiple cores inside one test process do not expose operating-system process boundaries, native-library unload, independent clocks, separate failures, or realistic agent concurrency.

### Required work

- Run at least three separately compiled agent processes.
- Give each process independent configuration, credentials, storage, logs, and lifecycle.
- Exercise simultaneous messaging, RPC, offline delivery, policy changes, peer recovery, and large transfers among all peers.
- Kill and restart individual agents during command execution, callback delivery, direct chunks, XEP-0363 download, XMPP chunk reassembly, and shutdown.
- Verify the documented V1 clear-process model: no unsupported cross-process continuity is claimed, stale handles fail, partial state is cleaned, and peers recover using permitted mechanisms.
- Run multiple independent conversations so one blocked payload cannot block unrelated delivery.

### Exit evidence

- Black-box process orchestration and deterministic test identities.
- Process-exit and restart assertions for every owned resource.
- Exactly-once application behavior where guaranteed and explicit normalized failure where continuity is not guaranteed.

## Gap 4: Real Python and TypeScript SDK conformance

### Missing

Go ABI vectors prove only the producer side. They do not prove foreign-function ownership, language event loops, garbage collection, exception mapping, promise behavior, buffer release, callback lifetime, packaging, or actual user-facing API parity.

### Required work

- Locate the authoritative Python and TypeScript SDK repositories supplied by the user or organization.
- Check them out under the authorized `git` directory and record exact commits.
- Build the same native core candidate for both SDKs.
- Execute identical conformance vectors and real E2E scenarios through each public SDK.
- Test Python sync and async behavior, exceptions, context management, garbage collection, callback teardown, and supported HTTP integrations.
- Test TypeScript promise behavior, event-loop non-blocking behavior, errors, resource disposal, callbacks, and supported runtime versions.
- Prove equivalent admissions, completions, events, status, capabilities, payload bytes, normalized failures, and cleanup.
- Scan exported symbols, generated code, public documentation, examples, errors, logs, and serialized output for SDK-boundary violations.

### External dependency rule

If either SDK repository is unavailable, Codex must complete Go-side vectors and harness adapters but report production qualification as blocked. It must not claim full production readiness from Go-only tests.

### Exit evidence

- Exact SDK repository commits and runtime versions.
- Shared conformance results with no unexplained divergence.
- Native library load/unload and resource ownership evidence in both languages.
- Package installation and minimal user-journey smoke tests from clean environments.

## Gap 5: Machine-readable feature traceability

### Current Go-core evidence

`test/e2e/fixtures/traceability_v1.json` now contains an exact 54-command
inventory. Its checker rejects a missing command and requires positive and
negative scenario references for every entry. The shared-group public runner
executes real authenticated message, request, reply, mandatory acceptance,
peer diagnostics, bounded diagnostic counters, and conversation lifecycle
through two separately constructed public Cores. Python and TypeScript rows
remain pending their SDK repositories and therefore this production gap is not
yet closed.

### Missing

The Pod 6 document defines a feature matrix, but prose alone cannot prove that every public operation and required rejection is executed in CI.

### Required work

- Commit a machine-readable traceability file, such as `test/e2e/feature_matrix.yaml`.
- Give every public command, result, event, lifecycle operation, capability, and error requirement a stable identifier.
- Associate at least one positive test and every contractually required negative test with each identifier.
- Include environment profile, SDK surface, expected outcome, timeout, and artifact location.
- Add a checker that fails CI when a public feature lacks coverage, references a missing test, or is marked skipped.
- Generate a human-readable report from the same source without maintaining a separate manual list.

### Exit evidence

- Zero uncovered required V1 identifiers.
- Zero required skipped rows.
- Python, TypeScript, native ABI, and Go facade coverage recorded separately.

## Gap 6: Security and adversarial qualification

### Missing

Unit validation is necessary but does not demonstrate resistance to cross-component attacks under real protocol and process boundaries.

### Required work

- Attempt forged identity, wrong mesh, stale membership, replay,
  same-scoped-message-ID reuse with changed content, request-handle reuse, and
  cross-core handle use. Verify the same-ID case is
  suppressed without a content comparison or second handler invocation.
- Attempt forged transfer manifests, chunk injection, wrong sender, wrong transfer identifier, overlapping offsets, chunk bombs, decompression or allocation bombs if applicable, V1 plaintext/digest tampering, unsupported future encryption references, and partial materialization.
- Attempt malicious XEP-0363 URLs, redirects, credentials in URLs, private or reserved addresses, DNS rebinding through an injected resolver, excessive headers, incorrect lengths, and slow responses.
- Attempt malformed public ABI input, oversized lengths, duplicate fields, unsupported versions, invalid buffer handles, double free, stale callback, callback reentrancy, and library unload with in-flight work.
- Scan logs, diagnostics, crashes, core dumps where enabled, test artifacts, temporary paths, and SDK errors for secret or private implementation canaries.
- Run dependency vulnerability, license, and provenance checks with pinned trusted tooling.
- Document a threat-model-to-test mapping and unresolved accepted risks.

### Exit evidence

- No unresolved critical or high-severity finding.
- Every threat-model item has prevention, detection, test, and owner.
- Medium or lower accepted risks have explicit owner approval and release notes.

## Gap 7: Performance objectives and resource budgets

### Missing

The architecture requires bounded resources but does not yet define approved throughput, latency, memory, CPU, transfer, startup, or shutdown objectives.

### Required work

- Measure idle memory and goroutines per core.
- Measure incremental memory per peer, queued command, RPC waiter, conversation, outbox entry, payload handle, and active transfer.
- Measure startup, ready, shutdown, send, request-response, fallback, and recovery latency distributions.
- Measure small-message throughput and large-payload throughput for direct, XEP-0363, and XMPP-chunk carriers.
- Measure queue saturation and recovery without unbounded allocation.
- Measure file descriptors, sockets, timers, temporary storage, and container resources.
- Produce proposed release SLOs and hard safety ceilings from repeatable benchmarks.
- Obtain an explicit product-owner decision for externally meaningful SLOs before declaring production readiness.

### Default qualification schedule

Until product-specific durations are approved:

```text
pull request: focused benchmarks and 5-10 minute concurrency smoke
nightly: at least 2 hours of mixed workload and failure cycling
release candidate: at least 24 hours of mixed workload and recovery soak
```

These are minimum qualification durations, not guaranteed service SLOs.

### Exit evidence

- Committed benchmark definitions and machine-readable results.
- Approved budgets and regression thresholds.
- CI failure when a statistically meaningful regression exceeds the approved threshold.
- No unbounded resource trend during release soak.

## Gap 8: Chaos, recovery, and long-duration stability

### Missing

Short functional tests do not reveal slow leaks, retry storms, timer
accumulation, reconnection flapping, or cleanup races.

### Required work

- Cycle ejabberd, XEP-0363, Coturn, fault proxies, SDK processes, and network paths during sustained mixed traffic.
- Maintain concurrent small messages, RPC, offline delivery, direct files, XEP-0363 files, and XMPP fallback files.
- Exercise repeated authentication failure and recovery without credential leakage or retry storms.
- Exercise queue pressure, slow consumers, cancellation storms, callbacks, and shutdown during chaos.
- Track memory, goroutines, file descriptors, sockets, timers, queues, reassembly bytes, temporary files, and container storage over time.
- Fail on monotonic unbounded growth, unexplained data loss, duplicate
  application invocation, or stuck shutdown.

### Exit evidence

- Reproducible workload and fault seeds.
- Time-series artifacts and leak analysis.
- Successful 24-hour release-candidate soak after the final production-code change.

## Gap 9: Compatibility and support matrix

### Missing

The repository does not yet declare which operating systems, architectures, Go versions, Python versions, Node.js runtimes, native library formats, or server versions are supported.

### Required work

- Create an explicit support matrix before release.
- Build and test every claimed Go target and shared-library format.
- Test every claimed Python and TypeScript runtime.
- Test supported ejabberd, Coturn, and object-upload behavior combinations.
- Test ABI version mismatch and at least the promised backward-compatible upgrade path.
- Test clean installation on hosts without a developer toolchain where binary packages are claimed.
- Do not claim support for an untested platform.

### Exit evidence

- Committed support matrix with CI job links.
- Reproducible build instructions and checksums for every artifact.
- Explicit unsupported-platform behavior and documentation.

## Gap 10: Release engineering and supply chain

### Missing

Passing source tests does not produce auditable, reproducible, distributable native libraries or SDK packages.

### Required work

- Use pinned build images and dependencies.
- Generate checksums and a software bill of materials for release artifacts.
- Run vulnerability, license, provenance, and secret scans.
- Produce deterministic version metadata and ABI version reporting.
- Build native libraries and headers for every supported target.
- Package Python and TypeScript SDKs from clean environments.
- Smoke-install published-form artifacts into clean disposable environments without actually publishing unless separately authorized.
- Define signing and publication steps; execute external publication only with explicit user authorization.
- Create rollback and artifact-revocation instructions.

### Exit evidence

- Reproducible release candidate artifacts.
- SBOM, checksums, scan reports, and provenance metadata.
- Clean-install smoke results.
- No publication performed merely because the test plan says how.

## Gap 11: Operational diagnostics and supportability

### Current Go-core evidence

The fixed V1 diagnostic snapshot now receives live, bounded contributions from
the authenticated graph: the complete current-authority remote-member set,
excluding the authenticated local member (not an all-account server roster,
live-link, or reachability count), messaging plus durable replay queues,
inbound and outbound RPC state, and inbound and outbound large payload
transfers. Status uses the same queued-message observation. Session shutdown
unregisters the contribution before destroying its owned graph, and all
projections remain transport opaque. Operator runbooks, long-duration
cardinality evidence, and SDK-level support testing remain open below.

### Missing

The core can be functionally correct but still impossible to operate safely when failures occur.

### Required work

- Verify stable public diagnostic identifiers and completely transport-opaque SDK output.
- Verify internal operator diagnostics can distinguish failure stages without secrets or payload contents.
- Correlate commands, logical messages, transfers, and peer lifecycle internally while keeping identifiers private.
- Prove rate limiting and bounded cardinality for logs, metrics, events, and failure storms.
- Produce failure artifacts sufficient to diagnose a failed E2E run without enabling unsafe verbose output.
- Add operator runbooks for authentication failure, connectivity degradation, XEP-0363 outage, XMPP chunk fallback saturation, queue saturation, and shutdown timeout.

### Exit evidence

- Redaction and cardinality tests.
- Diagnostic canary scans.
- Reviewed runbooks linked to tested failure scenarios.

## Mandatory qualification workflow

The Codex harness must execute production qualification in this order:

1. Pods 1–5 complete implementation, validation, and QA.
2. Pod 6 completes functional integration and the real-ejabberd feature matrix.
3. Pod 7 builds traversal, relay, fault, SDK, security, performance, soak, compatibility, and release qualification.
4. Failures return to the owning production pod and rerun that pod's validator and QA.
5. Pod 6 reruns affected functional E2E scopes.
6. Pod 7 reruns affected production qualification scopes.
7. The root orchestrator performs the final clean release audit.

Production qualification cannot be based on an earlier code commit than the final functional E2E candidate.

## Production release decision

Codex may report “implementation complete” when production code and pod gates are complete. It may report “production qualified” only when:

- Every gap in this document has exit evidence.
- Pod 7 has no unresolved blocking finding.
- The approved support and performance matrices are satisfied.
- The 24-hour release soak passed on the exact candidate commit.
- Real Python and TypeScript SDK conformance passed, unless the user explicitly narrows the release scope.
- All critical and high-severity security findings are closed.
- Release artifacts are reproducible and pass clean-install smoke tests.
- No required test is skipped or replaced by a simulator where real infrastructure is required.

If a required repository, platform, credential, or product SLO decision is unavailable, Codex must finish all unaffected work and report production qualification as blocked with the exact missing input. It must not silently downgrade the release standard.
