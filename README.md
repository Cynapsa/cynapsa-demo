# Cynapsa Demo

This repository contains three independent native Cynapsa identities:

- [`client/`](client/) — an interactive laptop client.
- [`orchestrator/`](orchestrator/) — routes user questions and delegates map work.
- [`maps/`](maps/) — answers place questions with Gemini and Google Places.

```text
client --Cynapsa RPC /ask--> orchestrator --Cynapsa RPC /maps--> maps
                                                                   |
                                                                   +--> Google Places API
```

Every directory is self-contained. It has its own application source,
configuration, Dockerfile, entrypoint, dependencies, documentation, and
`run.sh`. No credentials, enrollment tokens, or Cynapsa installation profiles
are committed to this repository.

## Run only the laptop client

Install Docker, then clone just the client directory with Git sparse checkout:

```sh
git clone --filter=blob:none --sparse https://github.com/Cynapsa/cynapsa-demo.git
cd cynapsa-demo
git sparse-checkout set client
cd client
./run.sh
```

The first run downloads a public, secret-free client image archive from this
repository's release and securely prompts for the enrollment
token of a client identity in the `cynapsa-demo` mesh. The token is mounted for
that enrollment only and its temporary file is removed when the process exits.
The encrypted Cynapsa installation profile remains in a dedicated Docker volume,
so later runs are simply:

```sh
cd cynapsa-demo/client
./run.sh
```

The checked-in client configuration already targets the deployed demo
orchestrator. See [`client/README.md`](client/README.md) for configuration and
reset instructions.

## Run an agent locally

The maps and orchestrator agents are currently deployed as GCP Cloud Run worker
pools. They can also be run locally from their own directories:

```sh
cd maps && ./run.sh
cd orchestrator && ./run.sh
```

Each runner prompts for any missing API keys and for a Cynapsa enrollment token
when its persistent Docker volume has no installation profile. Do not run a
local copy with the same installation profile while its cloud copy is active.

## Source versions

The release contains ARM64 and AMD64 client images with the compatible SDK and
Go Core but no secret or installation state, so client users do not need access
to the private source repositories or a container registry. Maintainers can use
each directory's `build.sh` with local SDK
and Go Core checkouts. Override `CYNAPSA_SDK_ROOT` and `CYNAPSA_CORE_ROOT` when
those checkouts are not below the default paths under `$HOME/git`.

## Security

- Local secrets are written only below each identity's ignored `.private/`
  directory with mode `0600`.
- Encrypted Core state is held in a separate Docker volume for each identity.
- Images contain no API keys, tokens, or installation state.
- Enrollment tokens are never passed as Docker command-line environment values.
- Revoke unused installations and rotate any credential that may have been
  exposed outside its intended environment.
