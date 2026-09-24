# Testing and acceptance

## Test layers to add during implementation

1. Unit/crypto: pinned logical CBOR, digest, `ms1`, proof-object digest, JWS/JCS, key lifecycle, strict decode, time, replay, redaction, and stable errors.
2. Contract: verify `contracts.lock.json`, Design candidate state, external manifest checksum, every consumed artifact checksum, schemas, examples, and positive/negative vectors.
3. Package/race/fuzz: immutable transfer ownership, no locks over Guard I/O, cancellation, shutdown, clone/clear/budgets, and strict V3 mutations.
4. Integration: fake Guard, authority, Enrollment/issuer, and ejabberd fixtures with exact call counts and failure matrices.
5. E2E: separate compiled agents through the public Go facade/native ABI over real disposable ejabberd, Rank 1, Rank 2, object, chunk, and fallback paths.
6. Production qualification: disposable TCP/HTTP faults, Coturn/NAT, reproducible shared library/release, canary/rollback fixtures, and coordinated CynapsaTests evidence.

## Core acceptance scenarios

- Valid ALLOW with exactly one matching `ga1` per binding delivers once.
- DENY, INDETERMINATE, timeout, malformed response, ambiguous external outcome, stale authority, missing/mismatched proof, or failed final fence never publishes enforced content.
- One same-domain transfer generates one sender Guard transaction and zero receiver Guard/provider calls.
- Fan-out shares semantic work only for the pinned equivalence set and still verifies exact per-binding attestations.
- No per-message Management call exists on sender or receiver.
- Guard-off startup and messaging remain unchanged with Guard/provider DNS/routes blackholed and zero attempted calls or new payload copies.
- Current V2 rejects reserved key 12, while strict V3 preserves proofs on all carriers.
- Reconnect under the same full resource yields a different session generation and invalidates the old attestation.
- No lock is held across Guard I/O, and authority/key/session races are caught by the final fence.
- The public Go/ABI/SDK boundary exposes no g1, keys, proofs, leases, Guard/provider endpoint, or transport-specific detail.

## Existing local commands

Use the pinned toolchain explicitly even if the ambient `go` resolves correctly:

```sh
GOTOOLCHAIN=go1.26.6 go build ./...
GOTOOLCHAIN=go1.26.6 go test -count=1 ./...
GOTOOLCHAIN=go1.26.6 go test -race -count=1 ./...
GOTOOLCHAIN=go1.26.6 go vet ./...
GOTOOLCHAIN=go1.26.6 go mod verify
GOTOOLCHAIN=go1.26.6 go build -buildmode=c-shared ./cmd/cynapsacore-shared
test/regression/run.sh fast
```

The repository currently declares `toolchain go1.26.6`. At documentation preparation time, both `go version` and `GOTOOLCHAIN=go1.26.6 go version` reported `go1.26.6 linux/amd64`, so no local mismatch was observed. Recheck on the coding agent's host.

## Trusted serialized qualification commands

These Docker-backed suites share the production lock and must run serially on the authorized disposable qualification environment described in `docs/development/CI.md`:

```sh
GOTOOLCHAIN=go1.26.6 go test -v -count=1 ./integration
GOTOOLCHAIN=go1.26.6 go test -v -count=1 -timeout=20m ./test/e2e
GOTOOLCHAIN=go1.26.6 go test -v -count=1 ./test/production/faults
GOTOOLCHAIN=go1.26.6 go test -v -count=1 ./test/production/coturn -run '^TestRemoteMeshSmoke$'
GOTOOLCHAIN=go1.26.6 go test -v -count=1 -timeout=30m ./test/production/coturn
GOTOOLCHAIN=go1.26.6 go test -v -count=1 ./test/production/release
```

Build a local unpublished release bundle only from a clean committed candidate:

```sh
./scripts/release-candidate-v1.sh ./dist 0.1.0-rc.1
```

Do not publish packages or use production/shared services without separate authorization.

## Evidence standard

Record exact commit/candidate pins, command, environment, timestamps, pass/fail/skip, logs/artifacts outside tracked source where required, and SHA-256. Separate fixture-only, local integration, deployed Guard, live provider, failover, load, chaos, soak, and coordinated qualification claims. A passing local suite is not a production, HA, provider, or deployment acceptance claim.
