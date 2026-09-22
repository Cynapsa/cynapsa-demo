# Cynapsa Go Core V1 Development, Validation, and QA Orchestration

Status: current orchestration contract; accepted ADRs govern implementation

Status: executable development plan  
Baseline branch: `Elad`  
Baseline rule: use the current `origin/Elad` head that contains this document and record its exact commit before dispatch

Scaffold ancestor: `6ce8a31fb3b5b0e68b5f4f1dbe8086fd3e409113`
Goal: replace the scaffold with a secure, tested, transport-opaque, production-qualified Go core and native SDK boundary

## Execution directive for Codex

This is the entry document for the complete Cynapsa Go Core V1 implementation effort. When a user asks Codex to execute this plan, Codex must act as the root orchestrator and continue through the development, independent validation, independent QA, integration, and final evidence gates. Producing another plan is not completion.

Before changing code, the root orchestrator must read this document, `AGENTS.md`, every normative architecture document referenced below, and every pod document linked in the next section. It must create the required builder, validator, and QA subagents, isolate write-capable agents in separate Git worktrees, and track every acceptance gate until it passes or a genuine user decision is required.

The orchestrator must not declare success because code compiles, a builder reports completion, or a parent agent finishes. Completion requires the validator, QA, and repository-wide gates defined here.

## Pod execution documents

Each document is mandatory. The first five pods own production implementation. Pod 6 owns independent functional system integration, the disposable ejabberd environment, and full black-box feature E2E testing. Pod 7 owns production qualification across real traversal and relay, faults, language SDKs, security, performance, soak, compatibility, and release artifacts.

1. [Pod 1: Public Contract and SDK Boundary](POD_01_CONTRACT_SDK_BOUNDARY.md)
2. [Pod 2: Runtime and Command Gate](POD_02_RUNTIME_COMMAND_GATE.md)
3. [Pod 3: Messaging Semantics](POD_03_MESSAGING_SEMANTICS.md)
4. [Pod 4: Connectivity](POD_04_CONNECTIVITY.md)
5. [Pod 5: Payload System](POD_05_PAYLOAD_SYSTEM.md)
6. [Pod 6: System Integration and Release QA](POD_06_SYSTEM_INTEGRATION_QA.md)
7. [Pod 7: Production Qualification](POD_07_PRODUCTION_QUALIFICATION.md)

Mandatory production gap register: [Production Readiness: Missing Work and Next Execution Step](PRODUCTION_READINESS_NEXT_STEP.md)

## Normative source order

When repository documents disagree, stop the affected implementation and resolve the conflict in the highest applicable source. Do not silently choose whichever statement is easiest to implement.

1. The invoking user's explicit requirements.
2. `AGENTS.md`.
3. `AZTM_SDK_BOUNDARY.md` for every SDK-visible surface.
4. Accepted architecture decision records in `docs/adr/`.
5. `AZTM_GO_CORE.md`, `AZTM_COMMAND_GATE.md`, `AZTM_ENVELOPE_PAYLOAD_PATCHING.md`, `AZTM_NETWORKING.md`, and `AZTM_SDK.md` in their respective scopes.
6. This orchestration document and the individual pod documents.
7. Existing stubs and TODO comments.

An unresolved proposed ADR is not permission to invent incompatible behavior. The owning pod must make the decision explicit, validate it, and change the ADR to Accepted before dependent implementation is merged.

## Non-negotiable product boundary

The public Go API, native ABI, generated bindings, serialized values, errors, events, status, capabilities, diagnostics, logs, metrics, examples, and SDK documentation must remain transport opaque.

All language SDK traffic must follow this path:

```text
Python or TypeScript SDK
        -> versioned native ABI and api/v1 contract
        -> internal/sdkboundary.Adapter
        -> private runtime packages
```

There is no alternative callback, serialization, diagnostic, or error path around the boundary adapter. Public models are explicit allowlists. Raw internal structures, arbitrary metadata maps, dependency errors, private identifiers, storage references, and connectivity details must not cross the boundary.

Large files and other oversized canonical payloads follow ADR 0005 and the authoritative V1 security model: bounded binary chunks over a DTLS-protected, infrastructure-confidential live link; HTTPS-protected server-visible plaintext in XEP-0363 when no usable live link completes; and XMPP-TLS-protected server-visible plaintext chunks when XEP-0363 fails and no live link is usable. All attempts remain one logical message. The SDK observes only the complete canonical payload or a normalized failure. Production payload E2EE and `IdentityKeyProvider` are future work.

## Pod ownership map

Only the owning pod may modify production files in its area. A different pod must submit a contract-change request to the root orchestrator and the owner. This is required even when the change looks trivial.

| Pod | Production ownership | Test ownership during pod QA |
| --- | --- | --- |
| Pod 1 | `api/v1/**`, root facade Go files, `internal/model/**`, `internal/sdkboundary/**`, `cmd/cynapsacore-shared/**`, public-boundary documentation | QA-specific tests under owned packages using `*_acceptance_test.go` and `*_fuzz_test.go` |
| Pod 2 | `internal/clock/**`, `internal/commandgate/**`, `internal/delivery/**`, `internal/diagnostics/**`, `internal/runtime/**` | QA-specific tests under those directories |
| Pod 3 | `internal/protocol/**`, `internal/conversation/**`, `internal/rpc/**`, `internal/policy/**`, `internal/mesh/**`, `internal/outbox/**` | QA-specific tests under those directories |
| Pod 4 | `internal/transport/**`, `internal/peer/**`, `internal/handshake/**` | QA-specific tests under those directories |
| Pod 5 | `internal/payload/**`, `internal/xep0363/**` | QA-specific tests under those directories |
| Pod 6 | `integration/**`, `internal/testkit/**`, `test/e2e/**`, repository-wide test runners, and CI verification | All system, ABI, cross-pod, real-ejabberd, black-box E2E, fault-injection, and release tests |
| Pod 7 | `test/production/**`, production qualification scripts/workflows, `testdata/production/**`, and `docs/release/**` | Real traversal/relay, fault, SDK, security, performance, soak, compatibility, and release-candidate qualification |
| Root orchestrator | `internal/sessionkernel/**` cross-pod private composition | Package-local builder tests; independent acceptance/fuzz tests are assigned explicitly by the root orchestrator. |

The exact file inventory and exclusions are defined in each pod document.

## Orchestrator-owned and controlled files

The following files can affect multiple pods and therefore require serialized changes by the root orchestrator:

- `go.mod` and `go.sum`.
- `internal/sessionkernel/**`, because it composes private services owned by
  Pods 2–5 and must not become an alternate implementation of any pod.
- `AZTM_GO_CORE.md` and `AZTM_FUTURE_ISSUES.md`.
- This `docs/development/**` orchestration package.
- Repository-level CI files, except when delegated to Pod 6 or Pod 7 within their named workflow ownership.
- Removal of `internal/stub/**` after every production reference has been eliminated.

A pod may propose a module dependency change, but the orchestrator must verify necessity, version, maintenance status, license compatibility, security posture, and transitive impact before applying or merging it. Dependency updates must be isolated in a clearly identified commit.

No agent may invent or select a repository license. License selection requires an explicit owner decision.

## Required subagent topology

Pods 1–5 must use three independent roles. Pod 6 uses four independent roles, including a separate black-box E2E tester. Pod 7 uses five independent production-qualification roles defined in its pod document.

### Builder

The builder implements only pod-owned production code and builder-owned unit tests. It must provide a commit, a function-by-function completion inventory, commands run, and known limitations.

### Validator

The validator is independent and initially read-only. It receives the pod document, normative sources, builder commit range, and test output. It reviews correctness, architecture, concurrency, security, and negative cases. Findings must include severity, exact file and line, evidence, consequence, and required correction. The validator does not weaken requirements or repair the code silently.

### QA agent

The QA agent is independent from both builder and validator. It writes only QA-owned tests and fixtures, preferably on a branch and worktree created from the builder candidate. It tests behavior through stable package or public boundaries and must not modify production code to make tests pass.

When a validator or QA agent finds a defect, the root orchestrator sends the finding back to the builder. After the builder fixes it, the same validator and QA scopes must be rerun against the new commit. This loop continues until all blocking findings are closed.

If agent concurrency is limited, roles run sequentially. Independence is more important than simultaneous execution.

### Subagent lifecycle protocol

The root orchestrator must actively manage every dispatched agent:

1. Give the agent one pod document, the exact branch and worktree, the exact base commit, its permitted files, and its required return format.
2. Track the agent until it reaches a terminal result or requests a decision. Do not dispatch agents and then end the parent task.
3. Inspect the agent's actual diff, commit, commands, test output, and unresolved notes; do not accept a completion statement without evidence.
4. Send actionable validator and QA findings back to the builder and wait for the corrected commit.
5. Rerun the relevant validator and QA scopes after every correction.
6. Record the final terminal status of the builder, validator, and QA agent separately.
7. For Pod 6, track the integration builder, validator, release-QA agent, and E2E tester separately.
8. For Pod 7, track the production builder, validator, SDK/compatibility QA, security/chaos QA, and performance/soak QA separately.
9. Release or close completed agent resources only after their evidence is captured in the task ledger.

A pod lead may create its own builder, validator, and QA agents when the harness supports nested delegation, but the root orchestrator still owns the evidence check and completion decision. No pod is complete while any required child is still running, failed, blocked without disposition, or has unresolved findings.

## Git and worktree isolation

Keep `Elad` as the immutable scaffold and architecture baseline. Create an integration branch from the pinned baseline, for example:

```text
develop/core-v1
```

Use one branch and one separate Git worktree for each write-capable role:

```text
pod/01-contract-boundary
pod/02-runtime-commandgate
pod/03-messaging
pod/04-connectivity
pod/05-payload
pod/06-system-integration
pod/07-production-qualification

qa/01-contract-boundary
qa/02-runtime-commandgate
qa/03-messaging
qa/04-connectivity
qa/05-payload
qa/06-system-integration
qa/07-sdk-compat
qa/07-security-chaos
qa/07-performance-soak
```

Validators may operate from a detached read-only worktree. Two agents must never edit the same branch or worktree concurrently. The orchestrator alone merges or cherry-picks validated commits into the integration branch.

Before each merge, the orchestrator must verify the exact base and head commits, working-tree cleanliness, changed-file ownership, and test evidence. It must never use destructive Git commands to resolve conflicts.

## Execution waves and dependency gates

### Wave 0: Preflight

The root orchestrator must:

1. Fetch and verify the current local and remote `Elad` head containing this document, then record that exact immutable commit as the base for every pod worktree.
2. Confirm a clean worktree and inspect existing changes before creating worktrees.
3. Run `go test ./...`, `go vet ./...`, and `git diff --check` to establish the scaffold baseline.
4. Create a durable task ledger containing pod branch, builder commit, validator disposition, QA disposition, and merge commit.
5. Read all proposed ADRs and route them to their owners.

### Wave 1: Contract and internal foundation freeze

Pod 1 completes its Stage A contract work:

- Accept ADR 0004 with exact process-lifetime decisions and tests.
- Freeze `api/v1` schemas and `internal/model` interfaces.
- Define ABI versioning, ownership, and strict serialization behavior.

Pod 2 completes its Stage A command-gate work:

- Accept ADR 0001 with exact queue, admission, completion, cancellation, and shutdown decisions.
- Freeze the typed command-gate behavior consumed by the runtime and root facade.

Pod 3 completes its Stage A protocol work:

- Accept ADR 0002 and ADR 0003 with exact decisions and tests.
- Freeze internal envelope, payload-descriptor, conversation, message identity,
  and correlation semantics.

All three checkpoints require builder, validator, and QA approval before dependent packages assume the interfaces are stable.

### Wave 2: Parallel implementation

After Wave 1 is merged, Pods 2, 3, 4, and 5 may execute concurrently in isolated worktrees. Pod 1 may prepare mapping tests but must not finalize mappings against unstable private result types.

Cross-pod interface changes are serialized through the owning pod and rerun through its validator and QA. Consumers must not copy or fork shared types to avoid coordination.

### Wave 3: Boundary completion

After Pods 2–5 pass their pod gates, Pod 1 completes Stage B:

- Field-by-field public/internal mapping.
- Strict native ABI codecs.
- Root facade lifecycle and payload methods.
- Numeric handle and buffer ownership.
- Stable public errors, events, diagnostics, status, and capabilities.
- ABI compatibility and generated-binding conformance vectors.

Pod 1 validation and QA then rerun against the integrated private implementations.

### Wave 4: System integration and release QA

Pod 6 receives the candidate produced by Pods 1–5. It implements and runs all system-level tests, creates and configures an isolated disposable ejabberd container, exercises real native builds and a complete black-box positive/negative feature matrix, and routes every failure back to the owning builder. Pod 6 cannot fix production code itself.

### Wave 5: Production qualification

Pod 7 receives the exact Pod 6-approved candidate. It executes `PRODUCTION_READINESS_NEXT_STEP.md`: disposable Coturn and controlled network faults, three-or-more-process scenarios, real Python and TypeScript SDK conformance, security and chaos testing, performance budgets, nightly stress, the 24-hour release soak, compatibility and support matrices, reproducible artifacts, clean installation, SBOM, vulnerability checks, and production evidence.

Pod 7 cannot fix production behavior. Every production defect returns to its owning Pod 1–5 builder, then reruns affected owner validation/QA, Pod 6 functional E2E, and Pod 7 qualification. Pod 7 evidence is valid only for the final candidate commit.

### Wave 6: Final audit and handoff

The root orchestrator performs the repository-wide gates, verifies every subagent's terminal result and evidence, removes obsolete scaffold sentinels only when safe, and produces separate implementation-complete and production-qualified dispositions with the final report.

## Cross-pod change protocol

When a pod needs a change outside its ownership:

1. Stop editing at the ownership boundary.
2. Describe the required behavior, affected interface, reason, compatibility impact, and tests that demonstrate the need.
3. Send the request to the root orchestrator and owning pod.
4. The owner implements the smallest compatible change.
5. The owner's validator and QA rerun the affected gates.
6. The orchestrator merges the owner change before rebasing the consumer.

No pod may use reflection, untyped maps, duplicated structs, exported aliases, or raw byte pass-through as a shortcut around this protocol.

## Coding and test ownership conventions

Use these suffixes to avoid builder and QA collisions:

```text
*_unit_test.go          builder-owned focused unit tests
*_acceptance_test.go    independent pod-QA behavioral tests
*_fuzz_test.go          independent pod-QA fuzz targets
```

Pod 6 owns files under `integration/`, `test/e2e/`, and shared fixtures under `internal/testkit/`. Pod 7 owns `test/production/` and production qualification artifacts. Package QA agents should use package-local fixtures until the owning system-test pod consolidates reusable infrastructure.

Tests must not claim success by skipping required behavior. A skip is allowed only for a documented external prerequisite that is outside the current release gate, and the release report must list it. No test corresponding to completed V1 behavior may remain skipped.

## Global quality gates

Every candidate merge must run the relevant subset; the final candidate must run the complete set.

```bash
gofmt -w <changed Go files>
go test ./...
go test -race ./...
go vet ./...
git diff --check
```

Additional mandatory gates:

- Deterministic repeated tests for concurrency-sensitive packages.
- Fuzzing for public decoders, private codecs, identifiers, policy input, payload serialization, and reference validation.
- Architecture tests proving `api/v1` imports no private package and the native wrapper imports no private implementation package directly.
- A forbidden-vocabulary scanner over exported Go symbols, public schemas, ABI headers, serialized fixtures, generated bindings, public errors, events, diagnostics, examples, and SDK documentation.
- A scanner proving no raw internal error, structure, log attribute, metric label, storage descriptor, or private identifier is serialized to SDK output.
- Resource-bound tests for queues, registries, payloads, buffers, workers,
  timers, correlation tables, peer lanes, dedupe tables, and the outbox.
- Leak and shutdown tests, including repeated create/start/shutdown/destroy cycles.
- Native shared-library builds for every supported target available in CI.
- Python and TypeScript conformance vectors once their binding repositories are connected.
- Real ejabberd container tests for authentication, discovery, establishment signaling, reliable fallback, resume, offline delivery, XEP-0363, and disabled or failing XEP-0363 profiles.
- End-to-end transfer tests for DTLS direct chunks, HTTPS XEP-0363, automatic XMPP TLS chunk fallback, ambiguous completion, integrity failure, and exhaustion of every carrier.
- A public-feature traceability matrix in which every supported success and every required rejection has a black-box E2E assertion.
- Real Coturn direct and forced-relay tests, controlled TCP/HTTP/UDP network fault profiles, and at least three independent agent processes.
- Real Python and TypeScript SDK conformance from clean environments for every claimed runtime.
- Security/chaos qualification mapped to a threat model with no unresolved critical or high finding.
- Approved performance and resource ceilings, nightly stress, and a 24-hour release-candidate soak on the exact final candidate.
- An explicit tested support matrix and reproducible release-candidate artifacts with checksums, SBOM, provenance, vulnerability, license, secret, and clean-install evidence.
- A dependency vulnerability scan using a pinned, trusted tool in CI.

Coverage percentages are supporting evidence, not a substitute for behavior. State-transition tables, error branches, race windows, malformed inputs, and security boundaries require explicit tests even when line coverage is high.

## Security and safety rules

- Never use production credentials, private endpoints, or live user data in tests.
- Use only disposable local or explicitly approved test infrastructure.
- Pod 6 and its E2E tester are explicitly authorized to create, configure, start, stop, inspect, and remove disposable local ejabberd containers and their dedicated test networks and temporary volumes. This authorization does not include production or shared servers.
- Pin container images by immutable digest for release evidence. Bind service ports to loopback or an isolated container network, use generated test-only credentials, impose CPU/memory/storage limits, and tear down containers, networks, and temporary volumes even after failure.
- Test containers must not use privileged mode, host networking, Docker socket mounts, host credential mounts, or broad host filesystem mounts.
- Pod 7 is authorized to create disposable local Coturn, TCP/HTTP fault-proxy, packet-impairment, clean SDK-build, and resource-observation containers and networks. All remain test-only, pinned, isolated, bounded, sanitized, and unconditionally removed.
- A dedicated disposable packet-impairment container or namespace may receive only the minimum `CAP_NET_ADMIN` capability needed for its isolated test network. It must not modify host-global networking, use host networking, or run privileged.
- Do not print credential material, payload plaintext, private identifiers, or internal negotiation data in logs or agent output.
- Validate sizes before allocation and bound every queue, map, timer, worker pool, buffer, and retry loop.
- Treat all inbound bytes, URLs, redirects, identifiers, serialized commands, callbacks, and handles as hostile.
- Use context cancellation and explicit deadlines for blocking operations.
- Make destruction, shutdown, cancellation, callback clearing, and payload release idempotent where the contract requires it.
- Preserve message identity and deduplication semantics across ambiguous delivery outcomes.
- Fail closed on unknown public input, unknown internal variants at the boundary, invalid policy, integrity failure, stale handles, and incomplete authentication.
- Do not add hidden test hooks to production builds.
- Do not deploy services, publish packages, or contact external systems unless the invoking user separately authorizes it.
- Production qualification may build and smoke-install publication-form artifacts locally, but external publication, signing-service use, release creation, or registry upload still requires explicit user authorization.

## Pod completion record

For each pod, the orchestrator must record:

```text
pod name
base commit
builder agent and commit
changed files
unit-test commands and results
validator agent and findings disposition
QA agent and test commit
QA commands and results
E2E tester and traceability result, for Pod 6
SDK/compatibility QA result, for Pod 7
security/chaos QA result, for Pod 7
performance/soak QA result, for Pod 7
fuzz/race/security evidence
remaining limitations
integration merge commit
```

An agent's statement that work is complete is not evidence by itself. The orchestrator must inspect the commit, test output, and unresolved findings.

## Final definition of done

The project is complete only when all of the following are true:

- All seven pod documents and the production-readiness gap register have been executed.
- Every production function required for V1 is implemented; no reachable V1 path returns a scaffold sentinel.
- All proposed V1 ADRs are Accepted and match the implementation.
- Every builder has passed independent validation and independent QA.
- Pod 6's separate E2E tester has completed the public positive/negative feature traceability matrix.
- Pod 7's SDK/compatibility, security/chaos, and performance/soak agents have completed independently, and its validator has accepted the combined evidence.
- All pod and system tests pass without hiding required behavior behind skips.
- Race, fuzz, malformed-input, backpressure, shutdown, and leak tests have passed at the agreed durations.
- Public contracts and generated artifacts pass the SDK abstraction-boundary audits.
- The native ABI builds and obeys its version, ownership, timeout, and buffer rules.
- The disposable ejabberd environment proves authentication, signaling, reliable fallback, resume, offline delivery, XEP-0363, XEP-0363 failure, and V1 plaintext XMPP chunk fallback over TLS, and is fully removed after testing.
- Direct live-link chunks, XEP-0363 transfer, automatic XMPP chunk fallback, ambiguous completion, integrity rejection, and all-carriers-failed behavior pass end to end with one logical application delivery or one normalized terminal failure.
- Real direct and forced-relay WebRTC, controlled network faults, and three-or-more-process mixed workloads pass.
- Approved performance and resource limits pass, and the exact final candidate completes the required 24-hour soak without unexplained growth or flakes.
- Every claimed platform and language runtime is present in the tested support matrix.
- Reproducible native and SDK release-candidate artifacts pass checksum, SBOM, provenance, vulnerability, license, secret, and clean-install gates.
- Python and TypeScript consume equivalent public schemas and conformance vectors, or the absence of connected binding repositories is explicitly reported as an external blocker rather than silently treated as passed.
- The worktree is clean and the integration branch contains only reviewed, attributable commits.
- The final report identifies exact commits, commands, results, accepted limitations, and any external work still required.

The root orchestrator must distinguish “implementation complete” from “production qualified.” It may use the second phrase only after every Pod 7 gate and every exit criterion in `PRODUCTION_READINESS_NEXT_STEP.md` passes on the exact final candidate.

If a required condition cannot be met without a product decision, credential, unavailable external repository, or unsupported build platform, the orchestrator must complete every unaffected item and report the precise blocker. It must not replace missing evidence with an assumption.
