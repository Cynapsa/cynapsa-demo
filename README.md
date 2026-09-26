# Cynapsa Demo

This repository contains three independent native Cynapsa identities:

- [`client/`](client/) — an interactive laptop client.
- [`orchestrator/`](orchestrator/) — routes user questions and delegates map work.
- [`maps/`](maps/) — answers place questions with the LiteLLM gateway and Google Places.

AI agents and operators should follow [`AGENTS.md`](AGENTS.md) for the complete
startup order, runtime variables, state-volume rules, and verification signals.

```text
client --Cynapsa RPC /ask--> orchestrator --Cynapsa RPC /maps--> maps
                                                                   |
                                                                   +--> Google Places API
```

Every directory is independently runnable. It has its own application source,
configuration, Dockerfile, entrypoint, dependencies, documentation, `run.sh`,
GitHub-backed Python SDK and Go Core builds. The runner builds the
container entirely from that directory when its local image is missing. No
credentials, enrollment tokens, or Cynapsa installation profiles are committed
to this repository.

Each build fetches the current `remove-snapshot` branches of the SDK and
Go Core directly from GitHub. These builds
require an ejabberd authority with the matching peer-handshake protocol; they
are not compatible with a server still running the old snapshot protocol.

“Current” above means the remote heads fetched at build time, not uncommitted
or unpushed local SDK/Core edits. At the 2026-09-26 audit, the latest local-policy
removal was not yet pushed in the SDK/Core; a GitHub
build must not be described as containing that removal until the remote source
is verified and rebuilt. This demo's working-tree apps/scripts contain no local
policy override, but that does not prove the fetched SDK exposes no allowance
API. No remote head, image build, cloud deployment, or live RPC was verified by
this documentation audit.

## Run one identity

Clone the repository, enter the identity directory, and create its ignored
environment file. For example, to run the client:

```sh
git clone --branch remove-snapshot-local-demo https://github.com/Cynapsa/cynapsa-demo.git
cd cynapsa-demo/client
cp .env.example .env
# Edit .env and replace the placeholders.
./run.sh
```

The first run builds the container image for the local Docker architecture.
Add `CYNAPSA_GITHUB_TOKEN` to that entity's ignored `.env` for image builds.
It is passed to BuildKit as a secret and is not sent to the running container.
The token needs read access to both Cynapsa repositories. Each later
`./run.sh --build` refetches both branch heads; the image records their
commit IDs at `/opt/cynapsa/source-revisions`.
Every identity requires `DEMO_MESH_ID`; the client and
orchestrator also require their destination agent ID. `CYNAPSA_TOKEN` enrolls a
new installation only when its state volume has no saved profile. The encrypted
profile is retained in that identity's Docker volume, so the token can be
removed from `.env` afterward.

If an agent is removed from its mesh, its installations are revoked. After
re-adding it, put a still-valid enrollment token back in its `.env` and run
`./run.sh --force-enroll` from that identity's directory. The built-in
`cynapsa run --force-enroll` CLI replaces the installation in the same selected
Docker volume. Its short session closes before the native app reconnects from
the new profile. Subsequent
`./run.sh` runs use that same volume.

The fetched SDK includes force-enroll and native remote-error handling. Existing
containers keep the image they started with, so stop and restart them after an
image rebuild. Use `./run.sh --build` to pick up later SDK/Core commits;
subsequent runs reuse the rebuilt image.

An entity's own agent identity is always resolved by Enrollment and stored by
Go Core. It is never accepted as local configuration.

The Dockerfiles contain no enrollment credential. On a new state volume,
`run.sh` passes `CYNAPSA_TOKEN` from that directory's ignored `.env` (or the
shell when `.env` does not define it) into the container. For maps and
orchestrator, `.env` takes precedence over shell variables for every configured
field. Its entrypoint writes
the token to a private runtime file, uses it once for enrollment, and retains
the encrypted profile in the named volume. A saved profile takes precedence
over a later token on ordinary runs; use `--force-enroll` to replace the
installation or a different volume to test another installation.

For an all-local run, start [`maps/`](maps/) first, then
[`orchestrator/`](orchestrator/), then [`client/`](client/). Use fresh enrollment
grants with available installation capacity on empty state volumes for the two
agents and the client. The two GCP
demo worker pools were previously observed at zero instances; that is not a
verified current cloud status. Start both agents locally for this runbook, or
independently verify the intended deployed workers before targeting them.
Their `.env.example` files list the API credentials they require on every run.
For this local demo, the model key can instead be placed in each agent's ignored
`.private/litellm_api_key` file (mode `0600`); `run.sh` mounts it read-only.
The default model is `gpt-5.6-terra-high` at `https://litellm.eladrave.com`.

## Independent directories

Copying only one entity directory to another machine is sufficient to build
and run it with `./run.sh`, provided Docker can reach GitHub and the
`CYNAPSA_GITHUB_TOKEN` grants read access. No repository-root files,
sibling directories, SDK checkout, or Go Core checkout are required.
The resulting image contains no credentials or installation state; those are
supplied when the container starts.

Use a different named state volume to run the same image with another
enrollment token. This keeps the installations isolated instead of deleting or
overwriting the first one. Each identity's README contains an example.

## Rebuilds

Use `./run.sh --build` to fetch fresh `remove-snapshot` branch heads and
rebuild. `CYNAPSA_DEMO_PLATFORM` remains available for an explicit Docker
target platform. The default image tag distinguishes these GitHub-backed
builds from old vendored images. If you set a custom image tag, run
`./run.sh --build` once. `--force-enroll` builds only when the selected
image is missing.

See each directory's `SOURCE_DEPENDENCIES.md` for build provenance and
source-versus-local limits. A direct `./build.sh` defaults to a `:local` tag,
whereas `run.sh` defaults to `:github-remove-snapshot-<Docker-server-arch>`;
use `run.sh --build` or set the same image override when building separately.

## Runtime limits and errors

Saved Core profiles take precedence over enrollment tokens on ordinary runs.
That does not make cached credentials indefinite authority: login/renewal and
server attachment/epoch checks can still reject them. Mesh removal revokes the
affected installations, not otherwise valid grants. After rejoin, stop the old
container, then force-enroll a replacement in the same selected volume; expiry,
independent grant revocation, or capacity can still prevent enrollment.

The maps app permits three model rounds with at most ten tool calls per round,
requires a first-round tool call, and accepts details only for an ID from a
prior search. The orchestrator accepts zero or exactly one `ask_maps_agent`
call, followed by a final completion; neither app promises a complete answer
within arbitrary tool chains. HTTP client timeouts are 25 seconds for the model
and 10 seconds for Places, not whole-request wall-clock guarantees.

The client prints canonical remote response errors or native/safety errors and
continues its prompt. Orchestrator validation, timeout, and model failures have
distinct public 400/504/503 codes; downstream native/canonical remote and other
internal failures become generic 502 `orchestrator_failed`. Its terminal logs
retain downstream status/details and exception traces: treat them as private,
not guaranteed content-free diagnostics. See the entity READMEs for specifics.

Current server peer authorization selects an exact active session in the same
mesh/organization; no active target is unavailable. Applied revoke controls
have a separate 30-second ACK deadline. Pending-session replay is best effort,
not an offline installation mailbox or application delivery receipt.

## Security

- `.env` files are ignored by Git and must never be committed.
- The GitHub token is used only during the clone step; it is excluded from
  the runtime environment and image layers.
- Encrypted Core state is held in a separate Docker volume for each identity.
- Images contain no API keys, enrollment tokens, or installation state.
- Environment variables are convenient for demos; production platforms should
  inject the same values from their managed secret store.
- Revoke unused installations and rotate credentials that may have been
  exposed outside their intended environment.
