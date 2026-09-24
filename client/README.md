# Demo client

This directory is an independently runnable interactive client for the local
Cynapsa demo. Start the maps and orchestrator containers first; their GCP
worker pools are disabled. Docker is the only runtime prerequisite. This directory contains
the application plus the complete Python SDK and Go Core source needed to build
its container.

For complete setup and usage instructions, see [RUN.md](RUN.md).

## Run

```sh
cp .env.example .env
# Replace the CYNAPSA_TOKEN placeholder in .env.
./run.sh
```

The runner builds the image from this directory when it is missing. The token
is passed as a runtime environment variable and enrolls the client only when
the `cynapsa-demo-client-state` Docker volume has no saved profile. Remove the
token from `.env` after enrollment; later runs reuse the encrypted profile.

You can export `CYNAPSA_TOKEN` instead of using `.env`. An exported value takes
precedence over the value in `.env`.

## Commands

```sh
./run.sh          # build if needed, then run
./run.sh --build  # force a rebuild from this directory
```

`DEMO_MESH_ID` and `DEMO_ORCHESTRATOR_AGENT_ID` are required. This keeps the
same image reusable for any mesh and orchestrator identity.
