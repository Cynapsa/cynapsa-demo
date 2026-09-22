# Cynapsa Go Core Agent Rules

This repository implements the Go core library. Python and TypeScript packages are bindings over its versioned public contract; they are not alternative implementations.

## Non-negotiable SDK boundary

- SDK-facing source, symbols, serialized values, documentation, errors, events, logs, metrics, examples, generated bindings, and configuration must remain transport opaque.
- Do not expose underlying signaling, connectivity, negotiation, addressing, fallback-tier, or protocol-extension terminology through any public path.
- `api/v1` owns all SDK-visible types. It must not import `internal/*`.
- `internal/sdkboundary.Adapter` is the only public/internal translator. Map values field by field; never pass through internal structures, arbitrary maps, raw errors, or raw diagnostic attributes.
- Unknown public input fails closed. Unknown internal state maps to a bounded public fallback.
- `cmd/cynapsacore-shared` imports the root facade and `api/v1` only; it must not import internal packages directly or export Go pointers.
- Add or update boundary scans, dependency tests, and Python/TypeScript parity vectors whenever the public contract changes.

## Scaffold implementation rule

Every unfinished function must retain a precise `TODO` describing validation, ownership, state transitions, error normalization, and expected outputs. Replace `ErrNotImplemented` only when the behavior and its tests are implemented together.

## V1 implementation orchestration

When asked to execute the complete V1 development effort, start with `docs/development/PROJECT_DEVELOPMENT_VALIDATION_QA.md`. Read every pod document it references before dispatching work, preserve the file-ownership boundaries, and require independent validator and QA evidence for every pod.

Production completion also requires `docs/development/PRODUCTION_READINESS_NEXT_STEP.md` and Pod 7. Do not describe the project as production qualified after functional E2E alone.

For that execution plan, Pod 6 is authorized to create and configure disposable local ejabberd containers for integration and E2E testing. Follow the isolation, image-pinning, test-credential, resource-limit, prohibited-mount, and unconditional-teardown rules in the master and Pod 6 documents. This does not authorize access to production or shared servers.

Pod 7 has the similarly bounded authorization documented in the master and Pod 7 documents for disposable Coturn, fault-injection, SDK-build, and qualification containers. External publication remains prohibited without explicit user authorization.
