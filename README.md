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
vendored Python SDK source, and vendored Go Core source. The runner builds the
container entirely from that directory when its local image is missing. No
credentials, enrollment tokens, or Cynapsa installation profiles are committed
to this repository.

The `remove-snapshot-local-demo` branch bundles the matching `remove-snapshot`
SDK and Go Core worktree snapshots in all three directories. These builds
require an ejabberd authority with the matching peer-handshake protocol; they
are not compatible with a server still running the old snapshot protocol.

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
Every identity requires `DEMO_MESH_ID`; the client and
orchestrator also require their destination agent ID. `CYNAPSA_TOKEN` enrolls a
new installation only when its state volume has no saved profile. The encrypted
profile is retained in that identity's Docker volume, so the token can be
removed from `.env` afterward.

If an agent is removed from its mesh, its installations are revoked. After
re-adding it, put a still-valid enrollment token back in its `.env` and run
`./run.sh --force-enroll` from that identity's directory. The bundled
`cynapsa run --force-enroll` CLI replaces the installation in the same selected
Docker volume. Its short session closes before the native app reconnects from
the new profile. Subsequent
`./run.sh` runs use that same volume.

The bundled SDK includes force-enroll and native remote-error handling. Existing
containers keep the image they started with, so stop and restart them after an
image rebuild. Use `./run.sh --build` after any later SDK/Core vendor sync;
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
demo worker pools are intentionally scaled to zero; no server-agent instance
will answer until you start its local container.
Their `.env.example` files list the API credentials they require on every run.
For this local demo, the model key can instead be placed in each agent's ignored
`.private/litellm_api_key` file (mode `0600`); `run.sh` mounts it read-only.
The default model is `gpt-5.6-terra-high` at `https://litellm.eladrave.com`.

## Self-contained directories

Each entity directory contains the exact SDK and Go Core source used by its
Docker build under `vendor/`. Copying only one entity directory to another
machine is sufficient to build and run it with `./run.sh`; no repository-root
files, sibling directories, SDK checkout, or Go Core checkout are required.
The resulting image contains no credentials or installation state; those are
supplied when the container starts.

Use a different named state volume to run the same image with another
enrollment token. This keeps the installations isolated instead of deleting or
overwriting the first one. Each identity's README contains an example.

## Rebuilds

Use `./run.sh --build` to force a rebuild from the source bundled in that
directory. `CYNAPSA_DEMO_PLATFORM` remains available for an explicit Docker
target platform. The default image tag changed for CLI force-enrollment,
so an old workaround image is not reused. If you set a custom image tag,
run `./run.sh --build` once after the vendor sync. `--force-enroll` builds only
when the selected image is missing.

## Security

- `.env` files are ignored by Git and must never be committed.
- Encrypted Core state is held in a separate Docker volume for each identity.
- Images contain no API keys, enrollment tokens, or installation state.
- Environment variables are convenient for demos; production platforms should
  inject the same values from their managed secret store.
- Revoke unused installations and rotate credentials that may have been
  exposed outside their intended environment.
