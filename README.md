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
configuration, Dockerfile, entrypoint, dependencies, documentation, and
`run.sh`. The runner downloads a prebuilt ARM64 or AMD64 container image, so a
user does not need the SDK or Go Core source. No credentials, enrollment tokens,
or Cynapsa installation profiles are committed to this repository.

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

The first run downloads and verifies the correct container image for the local
Docker architecture. Every identity requires `DEMO_MESH_ID`; the client and
orchestrator also require their destination agent ID. `CYNAPSA_TOKEN` enrolls a
new installation only when its state volume has no saved profile. The encrypted
profile is retained in that identity's Docker volume, so the token can be
removed from `.env` afterward.

An entity's own agent identity is always resolved by Enrollment and stored by
Go Core. It is never accepted as local configuration.

Use the same flow in [`orchestrator/`](orchestrator/) and [`maps/`](maps/).
Their `.env.example` files list the API credentials they require on every run.

## Portable images

The GitHub release contains separate ARM64 and AMD64 images for every identity,
including the compatible Python SDK and Go Core. Each `run.sh` downloads the
right archive automatically. The image itself contains no credentials or
installation state; those are supplied when the container starts.

Use a different named state volume to run the same image with another
enrollment token. This keeps the installations isolated instead of deleting or
overwriting the first one. Each identity's README contains an example.

## Maintainer builds

Maintainers can use `./run.sh --build` or `./build.sh` with local SDK and Go
Core checkouts. Override `CYNAPSA_SDK_ROOT`, `CYNAPSA_CORE_ROOT`, and
`CYNAPSA_DEMO_PLATFORM` when needed.

## Security

- `.env` files are ignored by Git and must never be committed.
- Encrypted Core state is held in a separate Docker volume for each identity.
- Images contain no API keys, enrollment tokens, or installation state.
- Environment variables are convenient for demos; production platforms should
  inject the same values from their managed secret store.
- Revoke unused installations and rotate credentials that may have been
  exposed outside their intended environment.
