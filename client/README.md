# Demo client

This directory is a standalone interactive client for the deployed Cynapsa
demo. Docker is the only local build/runtime prerequisite.

For complete setup and usage instructions, see [RUN.md](RUN.md).

## First run

Create or select a client agent in the `cynapsa-demo` mesh and generate an
enrollment token in the Cynapsa portal. Then run:

```sh
./run.sh
```

The runner downloads the correct public, secret-free image archive from the
GitHub release for Intel/AMD64 or ARM64, prompts for the token without echoing it,
enrolls the client, and stores the encrypted installation profile in the
`cynapsa-demo-client-state` Docker volume. The temporary token file is deleted
when the client exits. The runner verifies the release archive against a pinned
SHA-256 digest before loading it. Later runs use the saved profile and do not prompt.

You can also export `CYNAPSA_TOKEN` before the first run; the runner consumes it
without passing it as a Docker environment variable.

## Commands

```sh
./run.sh          # download if needed, then run
./run.sh --pull   # replace the local image from the GitHub release, then run
./run.sh --build  # maintainer build using local private SDK/Core checkouts
```

To discard this laptop installation and enroll again:

```sh
docker volume rm cynapsa-demo-client-state
./run.sh
```

The checked-in `config.json` targets the deployed demo orchestrator. Override
its values without editing the file by exporting `DEMO_MESH_ID` and
`DEMO_ORCHESTRATOR_AGENT_ID`; pass those variables into Docker by customizing
`run.sh` if needed for another mesh.
