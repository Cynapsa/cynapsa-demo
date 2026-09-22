# Demo client

This directory is an independently runnable interactive client for the deployed
Cynapsa demo. Docker is the only runtime prerequisite; the published container
already includes the application, Python SDK, and Go Core.

For complete setup and usage instructions, see [RUN.md](RUN.md).

## Run

```sh
cp .env.example .env
# Replace the CYNAPSA_TOKEN placeholder in .env.
./run.sh
```

The runner downloads and verifies the correct ARM64 or AMD64 image. The token
is passed as a runtime environment variable and enrolls the client only when
the `cynapsa-demo-client-state` Docker volume has no saved profile. Remove the
token from `.env` after enrollment; later runs reuse the encrypted profile.

You can export `CYNAPSA_TOKEN` instead of using `.env`. An exported value takes
precedence over the value in `.env`.

## Commands

```sh
./run.sh          # download if needed, then run
./run.sh --pull   # replace the local image from the GitHub release
./run.sh --build  # maintainer build using local SDK/Core checkouts
```

`DEMO_MESH_ID` and `DEMO_ORCHESTRATOR_AGENT_ID` are required. This keeps the
same image reusable for any mesh and orchestrator identity.
