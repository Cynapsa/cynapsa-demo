# Continuous integration

The hosted fast workflow is [`.github/workflows/ci.yml`](../../.github/workflows/ci.yml),
and the separately trusted infrastructure workflow is
[`.github/workflows/qualification.yml`](../../.github/workflows/qualification.yml).
Every job uses the module's pinned Go 1.26.6 toolchain. Third-party actions are
referenced by immutable commit SHA, checkout credentials are not persisted, and
both workflows grant only `contents: read`.

## Fast Linux jobs

Pull requests, manual fast checks, and ordinary branch pushes use only
GitHub-hosted Ubuntu 24.04 runners. They perform:

- `go build ./...` and every package test that does not acquire the shared
  production/Docker lock;
- `-race` testing of that same complete, dynamically derived non-Docker package
  set rather than a manually maintained subset;
- `go mod verify`, `go vet ./...`, and a clean `go mod tidy` diff;
- `govulncheck` pinned to `golang.org/x/vuln` v1.7.0; and
- the V1 security-document contract, native C smoke test, and ABI
  schema/header/export-allowlist agreement; and
- the executable GitHub issue #2–#13 regression manifest, including exact
  anchored selectors and source/package existence.

The ordinary and race test jobs exclude the Docker-backed `integration`
package and the four long-running qualification packages. Their helper and
probe packages still build and run normally. The excluded packages are never
converted to skips; the trusted serialized qualification workflow runs each
one explicitly. The small ABI allowlist test in the release package remains a
hosted check because it does not start the release harness.

No pull-request event, including a fork head, can schedule a job on the
self-hosted runner. The repository must not add `pull_request_target` or a
pull-request checkout path to the trusted qualification workflow.

## Docker qualification runner

Integration, E2E, TCP/HTTP faults, Coturn/NAT, and reproducible release tests
share the repository production lock and can run for hours. They execute
sequentially in one job with a job-level concurrency group. This prevents two
workflow runs from competing for service names, routes, and the host production
lock.

The trusted workflow has no pull-request trigger. It runs only for a push to
the protected `main` branch in `Cynapsa/cynapsagocore`, an explicit maintainer
`workflow_dispatch`, or the repository's Sunday `17 2 * * 0` schedule. The
canonical-repository job guard covers every allowed event. The job also uses the protected
`docker-qualification` GitHub environment; configure that environment with
required reviewers and restrict deployment branches to protected `main`.
Manual dispatch is authorization to qualify the selected repository commit and
must be approved only after reviewing that exact ref. The job checks out
`github.sha`, never a pull-request head or an untrusted ref supplied by payload
data.

The job requires a runner with all labels below:

```text
self-hosted
linux
x64
cynapsa-docker-qualification
```

The label may be assigned only to a dedicated, ephemeral Linux x86-64 runner.
Provision a clean VM for one approved job, register its runner with the
one-job/ephemeral option, and destroy the VM and its Docker state after the job
regardless of result. The runner must have:

- Docker Engine available to the runner account and support for bridge
  networks, tmpfs, bind mounts, read-only containers, dropped capabilities,
  and narrowly granted `NET_ADMIN` inside the disposable NAT containers;
- no production credentials, workloads, Docker resources, or user data;
- loopback port binding, outbound access to the immutable image digests already
  pinned by the repository fixtures, and enough disk/memory for the complete
  four-suite run;
- Bash, Git, a C11 compiler/linker, OpenSSL, and ordinary POSIX utilities; and
- permission for the fixtures to create and remove only their uniquely named
  containers, networks, volumes, state directories, credentials, routes, and
  test artifacts.

The runner preflight fails if the OS, architecture, Docker daemon, compiler,
OpenSSL, or exact Go toolchain is unavailable. A missing compatible ephemeral
runner leaves the required job queued; the workflow does not skip, mock, or
report Docker qualification success on an incompatible GitHub-hosted runner.

The executed sequence is exactly:

```sh
GOTOOLCHAIN=go1.26.6 go test -v -count=1 ./integration
GOTOOLCHAIN=go1.26.6 go test -v -count=1 -timeout=20m ./test/e2e
GOTOOLCHAIN=go1.26.6 go test -v -count=1 ./test/production/faults
GOTOOLCHAIN=go1.26.6 go test -v -count=1 ./test/production/coturn -run '^TestRemoteMeshSmoke$'
GOTOOLCHAIN=go1.26.6 go test -v -count=1 -timeout=30m ./test/production/coturn
GOTOOLCHAIN=go1.26.6 go test -v -count=1 ./test/production/release
```

Each fixture owns unconditional exact-resource teardown and asserts that its
containers, networks, volumes, credentials, and private state were removed.
CI must not add broad cleanup commands that could touch unrelated Docker
resources on the runner.

The focused smoke reuses the full Coturn package's XEP-0215 authority, NAT
gateway, public multiprocess agent, and shared production lock. It is a narrow
early regression signal, not a replacement for the following comprehensive
Coturn matrix. See [`test/regression/README.md`](../../test/regression/README.md).
