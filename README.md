# Cynapsa Demo

This repository contains three independent native Cynapsa identities:

- [`client/`](client/) — an interactive laptop client.
- [`orchestrator/`](orchestrator/) — routes user questions and delegates map work.
- [`maps/`](maps/) — answers place questions with Gemini and Google Places.

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

## Run one identity

Clone the repository, enter the identity directory, and create its ignored
environment file. For example, to run the client:

```sh
git clone https://github.com/Cynapsa/cynapsa-demo.git
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

An entity's own agent identity is always resolved by Enrollment and stored by
Go Core. It is never accepted as local configuration.

Use the same flow in [`orchestrator/`](orchestrator/) and [`maps/`](maps/).
Their `.env.example` files list the API credentials they require on every run.

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
target platform.

## Security

- `.env` files are ignored by Git and must never be committed.
- Encrypted Core state is held in a separate Docker volume for each identity.
- Images contain no API keys, enrollment tokens, or installation state.
- Environment variables are convenient for demos; production platforms should
  inject the same values from their managed secret store.
- Revoke unused installations and rotate credentials that may have been
  exposed outside their intended environment.
