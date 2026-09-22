# cynapsagocore

`cynapsagocore` is the Go runtime for Agentic Zero Trust Mesh (AZTM).

This repository owns the Go core, its language-neutral ABI, and the versioned public contract consumed by language SDKs. Python and TypeScript SDK implementations live outside this repository.

The V1 public facade, strict SDK boundary, and C99 shared-library ABI are
implemented. Private session providers are composed behind that boundary and
fail closed when a required production provider is unavailable.

The non-negotiable public boundary is defined in `AZTM_SDK_BOUNDARY.md`. Concrete connectivity technologies, protocols, ranks, configuration, errors, events, diagnostics, and storage descriptors must remain internal.

The authoritative V1 confidentiality and trust-boundary contract is
[`docs/security/V1_SECURITY_MODEL.md`](docs/security/V1_SECURITY_MODEL.md).
In particular, the preferred direct carrier is infrastructure-confidential,
while V1 server-mediated fallback carriers expose payload plaintext to their
terminating service. Production payload E2EE is deferred.

## Package boundary

```text
api/v1                       public transport-opaque contract
root package                 small Go facade
internal/sdkboundary         only public/internal translator
internal/*                   private runtime implementation
cmd/cynapsacore-shared       language-neutral ABI wrapper
```

## Token authentication

The additive `auth.token_login` and `auth.token_connect` commands accept only
a `cpsa_e1.<uuid>.<secret>` enrollment token, a mesh ID, and an optional local
profile ID. Separate profiles create separate installations, even with the
same token and mesh. One profile keeps independent JWT caches for every mesh
attached to that installation. After enrollment, `auth.installation_login`
and `auth.installation_connect` reopen a profile for a mesh without the
bootstrap token. Core calls the private enrollment service only when a cache
is absent, expired, or needs background reserve preparation.
Existing username/password commands remain available. The language integration
scaffolds include token helpers; authentication success is delivered through
the existing asynchronous completion contract. See
[`docs/development/ENROLLMENT_TOKEN_LOGIN.md`](docs/development/ENROLLMENT_TOKEN_LOGIN.md).

Token authentication persists its private installation and mesh credentials
by default under the operating system user configuration directory at
`Cynapsa/Core/auth-v1`. Container deployments must set
`CYNAPSA_STATE_DIRECTORY` to a persistent, mode-0700 directory or volume. The
v2 profile is encrypted independently of `cpsa_`, committed atomically under
an inter-process writer lock, and read back before bootstrap data is discarded.
The current file master key is stored mode 0600 beside the ciphertext. This
protects an isolated copied profile file and prevents token-derived recovery;
it does not protect theft of the complete state directory. An OS keystore is
the planned stronger at-rest boundary. Independent replicas should use
separate profile IDs or separate state roots rather than copying a profile.

An e2 credential supplies a distinct server-issued session resource in the
`r2.<installation UUID>.<nonce>` form while the mesh ID remains the separate
authorization scope. Installations created from one token share a logical bare
identity, but each binds a distinct full connected identity. Valid cached
credentials connect without Enrollment. An insufficient offline reserve
connects first and renews in the background with jitter, bounded backoff, and
one renewal per profile and mesh.
The default `continue` policy leaves an established server session running
after JWT expiry but blocks a new connection, reconnect, or resume without a
currently valid cached credential. The advanced `disconnect` policy closes
the established session at its authenticated deadline.

The complete internal identity hierarchy, enrollment sequence, persistent
profile model, renewal behavior, revocation boundaries, replica deployment
rules, and current limitations are documented in
[`docs/development/ENROLLMENT_TOKEN_LOGIN.md`](docs/development/ENROLLMENT_TOKEN_LOGIN.md).

## Verification

```bash
go test ./...
go build -buildmode=c-shared ./cmd/cynapsacore-shared
```

CI jobs and the dedicated Docker qualification-runner contract are documented
in [`docs/development/CI.md`](docs/development/CI.md).
The executable issue #2–#13 traceability manifest and focused independent-NAT
stage are documented in
[`test/regression/README.md`](test/regression/README.md).
For a faster non-container issue-regression pass, run
`test/regression/run.sh fast`; it does not replace the full suite above.

## Full V1 development execution

The entrypoint for coordinated implementation, independent validation, QA, and production qualification is [`docs/development/PROJECT_DEVELOPMENT_VALIDATION_QA.md`](docs/development/PROJECT_DEVELOPMENT_VALIDATION_QA.md). It defines all implementation pods, exact file ownership, required subagents, execution waves, merge gates, and final evidence requirements.

The mandatory gap register for the post-functional production stage is [`docs/development/PRODUCTION_READINESS_NEXT_STEP.md`](docs/development/PRODUCTION_READINESS_NEXT_STEP.md).
