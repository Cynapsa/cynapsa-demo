# Pod 7: Production Qualification

Status: current production-qualification plan

Parent plan: [Cynapsa Go Core V1 Development, Validation, and QA Orchestration](PROJECT_DEVELOPMENT_VALIDATION_QA.md)

Mandatory gap register: [Production Readiness: Missing Work and Next Execution Step](PRODUCTION_READINESS_NEXT_STEP.md)

## Mission

Turn the functionally complete and Pod 6-tested core into a production-qualified release candidate. Pod 7 owns real traversal and relay infrastructure, controlled network impairment, language-SDK conformance, security and chaos qualification, performance and soak testing, compatibility evidence, release-candidate artifacts, and the final production evidence bundle.

Pod 7 does not own application production behavior. It builds test and release infrastructure, finds defects, and routes every production correction to the owning Pod 1–5 builder. After a correction, the affected validator, pod QA, Pod 6 E2E scope, and Pod 7 qualification scope must rerun.

## Required subagents

The root orchestrator must create five independent Pod 7 agents:

1. `pod-07-production-builder`: creates pinned production-test infrastructure, runners, support matrices, benchmark harnesses, release-candidate build tooling, and machine-readable evidence collection.
2. `pod-07-validator`: independently reviews the test infrastructure, qualification claims, thresholds, artifacts, and release evidence for completeness and false confidence.
3. `pod-07-sdk-compat-qa`: tests Python, TypeScript, native ABI, platform, installation, version, and compatibility behavior from clean environments.
4. `pod-07-security-chaos-qa`: performs adversarial protocol, boundary, fault-injection, recovery, and threat-model testing.
5. `pod-07-performance-soak-qa`: executes benchmarks, resource-limit tests, nightly-duration stress, and the release-candidate soak.

The agents may run in waves when concurrency is limited. The SDK, security, and performance QA agents must remain independent from the production builder and from one another. The validator reviews the final combined candidate after all three QA agents report.

## Entry gate

Pod 7 may begin infrastructure preparation earlier, but it may not produce final qualification evidence until:

- Pods 1–5 are individually complete.
- Pod 6 has passed the full functional E2E matrix against real ejabberd.
- The candidate commit is recorded and immutable for the qualification run.
- Every required native artifact can be built from that commit.

Any production-code change invalidates affected Pod 7 evidence and requires a new candidate commit.

## Owned files and folders

Pod 7 owns production-qualification infrastructure and evidence only:

```text
test/production/**
test/production/coturn/**
test/production/faults/**
test/production/multiprocess/**
test/production/sdk/**
test/production/security/**
test/production/performance/**
test/production/soak/**
test/production/compatibility/**
test/production/release/**
testdata/production/**
scripts/production-*.sh
scripts/benchmark-*.sh
scripts/soak-*.sh
scripts/release-candidate-*.sh
.github/workflows/production-*.yml
.github/workflows/nightly-*.yml
.github/workflows/release-candidate-*.yml
docs/release/**
```

Pod 7 must create a machine-readable feature/support/evidence index or extend the Pod 6 feature matrix without duplicating it. Pod 6 remains owner of `test/e2e/**`, `integration/**`, and the ejabberd environment. Pod 7 consumes those artifacts and submits any required changes to Pod 6.

## Required durable artifacts

Pod 7 must produce at least:

```text
test/production/qualification.yaml
docs/release/SUPPORT_MATRIX.md
docs/release/PERFORMANCE_BUDGETS.md
docs/release/THREAT_MODEL.md
docs/release/RELEASE_CHECKLIST.md
docs/release/PRODUCTION_EVIDENCE.md
docs/release/OPERATOR_RUNBOOK.md
```

`qualification.yaml` is the machine-readable source linking each production requirement to candidate commit, environment, test, threshold, result, and artifact. The Markdown reports are generated or verified against that source so the release record cannot drift from executed evidence.

## Explicit exclusions

Pod 7 must not:

- Modify production files owned by Pods 1–5.
- Fix a failing implementation inside a test-infrastructure commit.
- Weaken Pod 6 functional assertions or SDK-boundary rules.
- Use production credentials, endpoints, user data, or shared servers.
- Use privileged containers, host networking, Docker socket mounts, or broad host filesystem mounts.
- Publish artifacts, packages, containers, or releases without explicit user authorization.
- Claim a platform, runtime, server, performance level, or SDK as supported without executed evidence.
- turn an unexplained failure into a retry-only pass.

## Authorized disposable infrastructure

Pod 7 is authorized to create, configure, run, inspect, and remove local disposable infrastructure needed by this qualification plan:

- Coturn for test-only STUN/TURN traversal and forced relay.
- TCP/HTTP fault proxies for ejabberd and XEP-0363 paths.
- Dedicated packet-impairment containers or network namespaces for UDP and WebRTC.
- Clean SDK build and installation containers.
- Cross-architecture build containers where supported by the host and CI.
- Metrics and resource collectors scoped to the test environment.

All images must be pinned by immutable digest for release evidence. Networks, ports, credentials, certificates, volumes, limits, readiness probes, log capture, and teardown must be explicit.

Packet impairment may use `CAP_NET_ADMIN` only inside a dedicated disposable fault container or namespace connected solely to the test network. Privileged mode, host networking, modification of host-global network rules, and host credential mounts remain prohibited.

## Production-builder assignment

The builder must implement:

1. A pinned Coturn environment with test-only credentials, direct, relay, failure, restart, and readiness profiles.
2. TCP/HTTP and UDP fault orchestration with versioned profiles and deterministic seeds.
3. A multi-process runner for at least three independent compiled agents.
4. A machine-readable production qualification manifest linking requirement, test, environment, threshold, candidate commit, and artifact.
5. Python and TypeScript SDK harness adapters that use authoritative SDK repositories without copying SDK implementation into this repository.
6. Clean environment builders for every claimed platform and language runtime.
7. Benchmark and resource instrumentation with stable output formats.
8. Nightly mixed-workload and release-candidate soak runners.
9. Threat-model and adversarial test orchestration.
10. Reproducible native shared-library and header builds for every claimed target.
11. Checksum, SBOM, provenance, vulnerability, license, and secret-scan generation.
12. Clean-install smoke environments for native, Python, and TypeScript artifacts.
13. Artifact collection and sanitization that preserves diagnostic value without credentials, private payloads, or internal canaries.
14. Unconditional teardown for processes, containers, networks, namespaces, volumes, temporary files, credentials, certificates, and collectors.

The builder must pin external tools and image digests in committed configuration. Floating tags are not valid release evidence.

## SDK and compatibility QA assignment

The `pod-07-sdk-compat-qa` agent must independently verify:

- The authoritative Python and TypeScript repositories and exact tested commits.
- Sync and async Python APIs, event-loop behavior, exception mapping, callbacks, context management, garbage collection, resource release, and supported HTTP integrations.
- TypeScript promises, event-loop behavior, normalized errors, callbacks, resource disposal, and every supported runtime.
- ABI buffer, callback, timeout, handle, and library-unload rules through both languages.
- Identical conformance vectors and equivalent semantic outcomes across SDKs.
- Clean package installation into environments without the repository checkout or developer toolchain where prebuilt artifacts are claimed.
- Every operating system, architecture, Go version, Python version, Node.js version, shared-library format, and server combination in the proposed support matrix.
- Supported upgrades and ABI version mismatch behavior.
- Generated code, exports, docs, examples, errors, logs, and serialized values pass SDK-boundary scans.

If an SDK repository or required platform is unavailable, this agent must report the exact blocker and cannot approve production qualification for that scope.

## Security and chaos QA assignment

The `pod-07-security-chaos-qa` agent must independently execute:

- Authentication, identity, mesh, membership, policy, replay, dedupe,
  request-handle, and opaque-handle attacks.
- Malformed ABI, protocol, transfer manifest, chunk, envelope, object reference, redirect, header, length, and version inputs.
- Cross-agent and cross-core injection attempts.
- XEP-0363 SSRF, redirect, DNS, content-length, timeout, corruption, and credential attacks.
- Direct and XMPP chunk forgery, conflict, replay, overlap, quota exhaustion,
  expiry, plaintext/digest tampering, and rejection of unsupported future
  encryption references. V1 Rank 2 does not claim ciphertext delivery.
- Ejabberd, Coturn, object service, fault proxy, agent process, and network restarts under load.
- Latency, jitter, packet loss, duplication, reordering, bandwidth restriction, TCP reset, half-open connection, DNS failure, and TLS failure.
- Retry storm, reconnect storm, cancellation storm, slow consumer, queue saturation, and shutdown chaos.
- Canaries across public output, logs, diagnostics, metrics, crash artifacts, temporary files, and release artifacts.
- Pinned vulnerability, license, provenance, and secret scans.

The agent must map findings to a threat model and severity. Critical and high findings block qualification. Accepted lower findings require owner, rationale, mitigation, and release-note treatment.

## Performance and soak QA assignment

The `pod-07-performance-soak-qa` agent must independently measure:

- Core startup, ready, idle, shutdown, and destruction.
- Small-message send and request-response latency distributions and throughput.
- Direct, XEP-0363, and XMPP-chunk large-payload throughput and memory use.
- Recovery and fallback time under each supported failure profile.
- CPU, memory, goroutines, threads, file descriptors, sockets, timers, queue depth, pending correlations, outbox entries, reassembly bytes, temporary files, and container storage.
- Incremental resources per peer, conversation, queued command, RPC waiter, payload handle, and transfer.
- Saturation behavior and recovery at every configured bound.
- Mixed workload with at least three agent processes and concurrent bidirectional traffic.

The agent must run, at minimum:

```text
pull-request qualification: 5-10 minute focused concurrency smoke
nightly qualification: at least 2 hours mixed traffic and failure cycling
release-candidate qualification: at least 24 hours mixed traffic, recovery, and resource observation
```

The exact release SLOs and regression thresholds require explicit product-owner approval. The agent must produce measurements and proposed thresholds, but it cannot invent a customer-facing guarantee.

Any monotonic unbounded growth, data loss, duplicate application invocation,
security-boundary leak, stuck shutdown, unexplained flake, or retry storm blocks
qualification regardless of average performance.

## Validator assignment

The `pod-07-validator` must review the final candidate and all Pod 7 artifacts. It must verify:

- Tests ran against the exact final candidate commit.
- Real infrastructure was used where required and readiness proved behavior rather than process liveness.
- Environment-forced TURN tests kept Core's ordinary `all` ICE policy and
  actually used relay candidates because network/router restrictions made
  direct candidates unreachable.
- Network faults reached the intended traffic without affecting the host globally.
- Multi-process tests did not import or mutate private implementation state.
- Python and TypeScript tests used authoritative SDK code and identical logical scenarios.
- The feature and support matrices contain no untested claimed row.
- Performance statistics, baselines, thresholds, and regressions are computed reproducibly.
- The release soak ran for the required duration after the final production change.
- Resource graphs show no unexplained unbounded trend.
- Security findings are complete, severity-ranked, and resolved or explicitly accepted at the permitted level.
- Release artifacts are reproducible, checksummed, scanned, and clean-install tested.
- Logs and evidence are sanitized without hiding test failures.
- Every container and process teardown was verified.
- No required test is skipped, silently retried to green, or replaced by a simulator.

Any unsupported claim, stale evidence, unpinned infrastructure, missing SDK, missing support row, unexplained flake, incomplete teardown, critical/high finding, failed soak, or unapproved SLO is blocking.

## Defect routing

| Failure area | Owning pod |
| --- | --- |
| Public API, ABI, binding projection, handles, callbacks, or SDK leakage | Pod 1 |
| Runtime lifecycle, command gate, cancellation, queues, delivery, or diagnostics | Pod 2 |
| Protocol, identity, dedupe, RPC, policy, mesh, peer lanes, or outbox | Pod 3 |
| WebRTC, XMPP, peer state, establishment, TURN use, reconnect, or carrier framing | Pod 4 |
| Payload selection, manifests, reassembly, integrity, encryption, XEP-0363, or cleanup | Pod 5 |
| Functional E2E harness, ejabberd profiles, or traceability matrix | Pod 6 |
| Coturn, fault infrastructure, SDK harness, benchmarks, soak, compatibility, or release tooling | Pod 7 production builder |

Production-code failures return to the owning builder. After correction, the owner validator and QA rerun, Pod 6 reruns affected functional E2E tests, and Pod 7 reruns all invalidated qualification evidence.

## Support and performance decision gates

The harness must discover and document, but cannot silently decide, externally meaningful release promises:

- Supported operating systems and architectures.
- Supported Python and Node.js runtime versions.
- Required direct and relay modes.
- Maximum payload and fallback sizes.
- Latency, throughput, startup, recovery, and shutdown SLOs.
- Release benchmark regression tolerances.
- Artifact signing and publication destinations.

Codex should prepare evidence-backed recommendations and continue all work independent of these decisions. Production qualification remains blocked only for the exact undecided claims.

## Pod 7 completion gates

Pod 7 is complete only when:

- Every gap and exit criterion in `PRODUCTION_READINESS_NEXT_STEP.md` has evidence or a precise external blocker.
- Coturn direct/relay and all declared failure profiles pass.
- Controlled TCP, HTTP, UDP, DNS, TLS, restart, and process fault profiles pass or produce the required bounded normalized failure.
- At least three independent compiled agent processes pass concurrent mixed-workload scenarios.
- Python and TypeScript SDK conformance passes for every claimed runtime.
- The machine-readable feature and support matrices have no uncovered claimed row.
- Security and chaos QA has no unresolved critical or high finding.
- Approved performance and resource thresholds pass.
- The 24-hour soak passes on the exact final candidate without unexplained growth or flakes.
- Native and SDK artifacts are reproducible, checksummed, SBOM-generated, scanned, and clean-install tested.
- The validator has no unresolved blocking finding.
- No production or shared infrastructure was used.
- No artifact was externally published without explicit user authorization.
- Exact commits, tool and image digests, commands, environments, seeds, durations, results, findings, graphs, artifacts, teardown evidence, limitations, and blockers are returned to the root orchestrator.

Only after these gates may the root orchestrator describe the candidate as production qualified.
